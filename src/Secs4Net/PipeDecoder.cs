using CommunityToolkit.HighPerformance;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Secs4Net;

public sealed class PipeDecoder
{
    private const int MessageHeaderLength = 10;
    private const int DecodeErrorDataPreviewLength = 4096;

    private readonly PipeReader _reader;
    private readonly Action? _startInterChunkTimer;
    private readonly Action? _stopInterChunkTimer;
    public PipeWriter Input { get; }

    /// <summary>
    /// Raised when an HSMS data message has a complete frame but its SECS-II payload is malformed.
    /// The malformed frame is skipped and decoding continues with the next HSMS frame.
    /// </summary>
    public event EventHandler<DataMessageDecodeErrorEventArgs>? DataMessageDecodeError;

    internal event Action? IncrementalDataMessageDecodingStarted;

    private readonly Channel<MessageHeader> _controlMessageChannel = Channel
        .CreateUnbounded<MessageHeader>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = true,
        });

    private readonly Channel<(MessageHeader header, Item? rootItem)> _dataMessageChannel = Channel
        .CreateUnbounded<(MessageHeader header, Item? rootItem)>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false,
        });

    public PipeDecoder(PipeReader reader, PipeWriter input)
        : this(reader, input, startInterChunkTimer: null, stopInterChunkTimer: null)
    {
    }

    internal PipeDecoder(
        PipeReader reader,
        PipeWriter input,
        Action? startInterChunkTimer,
        Action? stopInterChunkTimer)
    {
        _reader = reader;
        Input = input;
        _startInterChunkTimer = startInterChunkTimer;
        _stopInterChunkTimer = stopInterChunkTimer;
    }

    internal IAsyncEnumerable<MessageHeader> GetControlMessages(CancellationToken cancellation)
        => _controlMessageChannel.Reader.ReadAllAsync(cancellation);

    public IAsyncEnumerable<(MessageHeader header, Item? rootItem)> GetDataMessages(CancellationToken cancellation)
        => _dataMessageChannel.Reader.ReadAllAsync(cancellation);

    public Task StartAsync(CancellationToken cancellation)
        => DecodeLoopAsync(_controlMessageChannel.Writer, _dataMessageChannel.Writer, _reader, cancellation);

    private async Task DecodeLoopAsync(
        ChannelWriter<MessageHeader> controlMessageWriter,
        ChannelWriter<(MessageHeader header, Item? rootItem)> dataMessageWriter,
        PipeReader reader,
        CancellationToken cancellation)
    {
        var totalLengthBytes = new byte[4];
        var messageHeaderBytes = new byte[MessageHeaderLength];
        var buffer = await PipeReadAsync(reader, required: 4, messageStarted: false, cancellation).ConfigureAwait(false);

        while (!cancellation.IsCancellationRequested)
        {
            if (IsBufferInsufficient(reader, ref buffer, required: 4))
            {
                buffer = await PipeReadAsync(
                    reader,
                    required: 4,
                    messageStarted: !buffer.IsEmpty,
                    cancellation).ConfigureAwait(false);
            }

            var totalLengthSequence = buffer.Slice(buffer.Start, 4);
            totalLengthSequence.CopyTo(totalLengthBytes);
            var messageLength = BinaryPrimitives.ReadUInt32BigEndian(totalLengthBytes);
            buffer = buffer.Slice(totalLengthSequence.End);

            if (messageLength < MessageHeaderLength || messageLength > int.MaxValue)
            {
                // A corrupt HSMS length loses the frame boundary, so reconnecting is safer than
                // trying to find the next frame in an untrusted byte stream.
                throw new InvalidDataException($"Invalid HSMS message length: {messageLength}.");
            }

            if (IsBufferInsufficient(reader, ref buffer, required: MessageHeaderLength))
            {
                buffer = await PipeReadAsync(reader, required: MessageHeaderLength, messageStarted: true, cancellation).ConfigureAwait(false);
            }

            var messageHeaderSequence = buffer.Slice(buffer.Start, MessageHeaderLength);
            messageHeaderSequence.CopyTo(messageHeaderBytes);
            MessageHeader.Decode(messageHeaderBytes, out var header);
            buffer = buffer.Slice(messageHeaderSequence.End);

            var dataLength = (int)messageLength - MessageHeaderLength;
            ValidateHsmsHeader(messageHeaderBytes, header, dataLength);
            if (dataLength == 0)
            {
                if (header.MessageType == MessageType.DataMessage)
                {
                    await dataMessageWriter.WriteAsync((header, rootItem: null), cancellation).ConfigureAwait(false);
                }
                else
                {
                    await controlMessageWriter.WriteAsync(header, cancellation).ConfigureAwait(false);
                }

                continue;
            }

            if (buffer.Length >= dataLength)
            {
                // Decode from a sequence bounded to this HSMS frame. A malformed List can no
                // longer consume the length prefix or header of the following frame.
                var encodedData = buffer.Slice(buffer.Start, dataLength);
                buffer = buffer.Slice(encodedData.End);

                try
                {
                    var remainedData = encodedData;
                    var rootItem = Item.DecodeFromFullBuffer(ref remainedData);
                    if (!remainedData.IsEmpty)
                    {
                        rootItem.Dispose();
                        throw CreateTrailingDataException(remainedData.Length);
                    }

                    await dataMessageWriter.WriteAsync((header, rootItem), cancellation).ConfigureAwait(false);
                }
                catch (Exception ex) when (IsMalformedSecsDataException(ex))
                {
                    var errorPreviewLength = Math.Min(dataLength, DecodeErrorDataPreviewLength);
                    RaiseDataMessageDecodeError(
                        header,
                        encodedData.Slice(encodedData.Start, errorPreviewLength).ToArray(),
                        dataIsTruncated: errorPreviewLength < dataLength,
                        ex);
                }

                continue;
            }

            // Keep streaming large or fragmented messages so PipeWriter backpressure cannot
            // deadlock while the decoder waits for the entire HSMS frame to be buffered.
            IncrementalDataMessageDecodingStarted?.Invoke();
            var stack = new Stack<ItemList>(capacity: 8);
            var remainedDataLength = dataLength;
            var dataPreview = new byte[Math.Min(dataLength, DecodeErrorDataPreviewLength)];
            var previewLength = 0;
            Item? root = null;

            try
            {
                while (root is null)
                {
                    if (remainedDataLength < 1)
                    {
                        throw new InvalidDataException("The HSMS data ended before all declared SECS-II list items were received.");
                    }

                    if (IsBufferInsufficient(reader, ref buffer, required: 1))
                    {
                        buffer = await PipeReadAsync(reader, required: 1, messageStarted: true, cancellation).ConfigureAwait(false);
                    }

                    var formatSequence = buffer.Slice(buffer.Start, 1);
                    CopyToPreview(formatSequence, dataPreview, ref previewLength);
                    Item.DecodeFormatAndLengthByteCount(formatSequence, out var itemFormat, out var lengthByteCount);
                    buffer = buffer.Slice(formatSequence.End);
                    remainedDataLength--;

                    if (lengthByteCount == 0)
                    {
                        throw new InvalidDataException("A SECS-II item must use between one and three length bytes.");
                    }

                    if (remainedDataLength < lengthByteCount)
                    {
                        throw new InvalidDataException("The HSMS data ended inside a SECS-II item length field.");
                    }

                    if (IsBufferInsufficient(reader, ref buffer, required: lengthByteCount))
                    {
                        buffer = await PipeReadAsync(reader, required: lengthByteCount, messageStarted: true, cancellation).ConfigureAwait(false);
                    }

                    var lengthSequence = buffer.Slice(buffer.Start, lengthByteCount);
                    CopyToPreview(lengthSequence, dataPreview, ref previewLength);
                    var itemContentLength = Item.DecodeDataLength(lengthSequence);
                    buffer = buffer.Slice(lengthSequence.End);
                    remainedDataLength -= lengthByteCount;

                    Item item;
                    if (itemFormat == SecsFormat.List)
                    {
                        if (itemContentLength == 0)
                        {
                            item = Item.L();
                        }
                        else
                        {
                            Item.ValidateListItemCount(itemContentLength, remainedDataLength);
                            if (stack.Count >= Item.MaximumListNestingDepth)
                            {
                                throw new InvalidDataException(
                                    $"SECS-II List nesting exceeds the maximum depth of {Item.MaximumListNestingDepth}.");
                            }

                            stack.Push(new ItemList(itemContentLength));
                            continue;
                        }
                    }
                    else
                    {
                        if (itemContentLength > remainedDataLength)
                        {
                            throw new InvalidDataException(
                                $"SECS-II item length {itemContentLength} exceeds the {remainedDataLength} byte(s) left in the HSMS data.");
                        }

                        if (buffer.Length >= itemContentLength)
                        {
                            var itemDataSequence = buffer.Slice(buffer.Start, itemContentLength);
                            CopyToPreview(itemDataSequence, dataPreview, ref previewLength);
                            buffer = buffer.Slice(itemDataSequence.End);
                            remainedDataLength -= itemContentLength;
                            item = Item.DecodeDataItem(itemFormat, itemDataSequence);
                        }
                        else
                        {
                            // A single Binary/string/numeric item may be larger than the Pipe's
                            // pause threshold. Copy it incrementally while advancing the reader so
                            // the socket producer can continue flushing the remainder.
                            var rentedItemData = ArrayPool<byte>.Shared.Rent(itemContentLength);
                            try
                            {
                                var copiedLength = 0;
                                while (copiedLength < itemContentLength)
                                {
                                    if (IsBufferInsufficient(reader, ref buffer, required: 1))
                                    {
                                        buffer = await PipeReadAsync(reader, required: 1, messageStarted: true, cancellation).ConfigureAwait(false);
                                    }

                                    var copyLength = (int)Math.Min(buffer.Length, itemContentLength - copiedLength);
                                    var itemDataChunk = buffer.Slice(buffer.Start, copyLength);
                                    itemDataChunk.CopyTo(rentedItemData.AsSpan(copiedLength, copyLength));
                                    CopyToPreview(itemDataChunk, dataPreview, ref previewLength);
                                    buffer = buffer.Slice(itemDataChunk.End);
                                    remainedDataLength -= copyLength;
                                    copiedLength += copyLength;
                                }

                                var itemDataSequence = new ReadOnlySequence<byte>(
                                    rentedItemData.AsMemory(0, itemContentLength));
                                item = Item.DecodeDataItem(itemFormat, itemDataSequence);
                            }
                            finally
                            {
                                ArrayPool<byte>.Shared.Return(rentedItemData);
                            }
                        }
                    }

                    if (stack.Count == 0)
                    {
                        root = item;
                        break;
                    }

                    var list = stack.Peek();
                    list.Add(item);
                    while (list.IsFull)
                    {
                        item = Item.L(stack.Pop().Items);
                        if (stack.Count == 0)
                        {
                            root = item;
                            break;
                        }

                        list = stack.Peek();
                        list.Add(item);
                    }
                }

                if (remainedDataLength != 0)
                {
                    root.Dispose();
                    root = null;
                    throw CreateTrailingDataException(remainedDataLength);
                }

                await dataMessageWriter.WriteAsync((header, root), cancellation).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsMalformedSecsDataException(ex))
            {
                root?.Dispose();
                foreach (var itemList in stack)
                {
                    itemList.DisposeItems();
                }

                // The HSMS length is still trustworthy. Discard exactly the rest of this frame,
                // then resume at the next four-byte HSMS length prefix.
                while (remainedDataLength > 0)
                {
                    if (IsBufferInsufficient(reader, ref buffer, required: 1))
                    {
                        buffer = await PipeReadAsync(reader, required: 1, messageStarted: true, cancellation).ConfigureAwait(false);
                    }

                    var discardLength = (int)Math.Min(buffer.Length, remainedDataLength);
                    var discarded = buffer.Slice(buffer.Start, discardLength);
                    CopyToPreview(discarded, dataPreview, ref previewLength);
                    buffer = buffer.Slice(discarded.End);
                    remainedDataLength -= discardLength;
                }

                RaiseDataMessageDecodeError(
                    header,
                    dataPreview.AsMemory(0, previewLength),
                    dataIsTruncated: previewLength < dataLength,
                    ex);
            }
        }
    }

    private void RaiseDataMessageDecodeError(
        MessageHeader header,
        ReadOnlyMemory<byte> encodedData,
        bool dataIsTruncated,
        Exception exception)
    {
        var handlers = DataMessageDecodeError;
        if (handlers is null)
        {
            return;
        }

        var eventArgs = new DataMessageDecodeErrorEventArgs(header, encodedData, dataIsTruncated, exception);
        foreach (EventHandler<DataMessageDecodeErrorEventArgs> handler in handlers.GetInvocationList())
        {
            try
            {
                handler(this, eventArgs);
            }
            catch (Exception subscriberException)
            {
                Trace.TraceError($"Unhandled exception in {nameof(DataMessageDecodeError)} subscriber: {subscriberException}");
            }
        }
    }

    private static InvalidDataException CreateTrailingDataException(long trailingByteCount)
        => new($"The SECS-II root item left {trailingByteCount} trailing byte(s) in the HSMS data message.");

    private static void ValidateHsmsHeader(ReadOnlySpan<byte> encodedHeader, MessageHeader header, int dataLength)
    {
        var pType = encodedHeader[4];
        if (pType != 0)
        {
            throw new InvalidDataException(
                $"Invalid HSMS header PType 0x{pType:X2}; only PType 0 is defined. Header: {FormatHeader(encodedHeader)}");
        }

        if (!IsDefinedMessageType(header.MessageType))
        {
            throw new InvalidDataException(
                $"Invalid HSMS header SType 0x{(byte)header.MessageType:X2}. Header: {FormatHeader(encodedHeader)}");
        }

        if (header.MessageType != MessageType.DataMessage && dataLength != 0)
        {
            throw new InvalidDataException(
                $"Invalid HSMS control message {header.MessageType}: control messages cannot contain a {dataLength}-byte data field. " +
                $"Header: {FormatHeader(encodedHeader)}");
        }
    }

    private static string FormatHeader(ReadOnlySpan<byte> encodedHeader)
        => BitConverter.ToString(encodedHeader.ToArray()).Replace("-", " ");

    private static bool IsDefinedMessageType(MessageType messageType)
        => messageType is MessageType.DataMessage
            or MessageType.SelectRequest
            or MessageType.SelectResponse
            or MessageType.Deselect_req
            or MessageType.Deselect_rsp
            or MessageType.LinkTestRequest
            or MessageType.LinkTestResponse
            or MessageType.Reject_req
            or MessageType.SeparateRequest;

    private static bool IsMalformedSecsDataException(Exception exception)
        => exception is ArgumentException
            or IndexOutOfRangeException
            or InvalidDataException;

    private static void CopyToPreview(
        in ReadOnlySequence<byte> source,
        Span<byte> preview,
        ref int previewLength)
    {
        var copyLength = (int)Math.Min(source.Length, preview.Length - previewLength);
        if (copyLength <= 0)
        {
            return;
        }

        source.Slice(source.Start, copyLength).CopyTo(preview[previewLength..]);
        previewLength += copyLength;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsBufferInsufficient(PipeReader reader, ref ReadOnlySequence<byte> remainedBuffer, int required)
    {
        if (remainedBuffer.Length >= required)
        {
            return false;
        }

        reader.AdvanceTo(remainedBuffer.Start);
        return !PipeTryRead(reader, required, ref remainedBuffer);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool PipeTryRead(PipeReader reader, int required, ref ReadOnlySequence<byte> buffer)
    {
        if (reader.TryRead(out var result))
        {
            buffer = result.Buffer;
            if (buffer.Length >= required)
            {
                return true;
            }

            reader.AdvanceTo(consumed: buffer.Start, examined: buffer.End);
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    private ValueTask<ReadOnlySequence<byte>> PipeReadAsync(
        PipeReader reader,
        int required,
        bool messageStarted,
        CancellationToken cancellation)
    {
        ReadOnlySequence<byte> buffer = ReadOnlySequence<byte>.Empty;
        if (PipeTryRead(reader, required, ref buffer))
        {
            return new(buffer);
        }

        // Do not run T8 while the connection is idle between HSMS messages. Once any bytes of a
        // new message have arrived (or its length prefix was already consumed), T8 covers only
        // the wait for the next chunk and is stopped as soon as that wait completes.
        return SlowPipeReadAsync(
            reader,
            required,
            messageStarted || !buffer.IsEmpty,
            _startInterChunkTimer,
            _stopInterChunkTimer,
            cancellation);

        [MethodImpl(MethodImplOptions.NoInlining)]
#if NET
        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
#endif
        static async ValueTask<ReadOnlySequence<byte>> SlowPipeReadAsync(
            PipeReader reader,
            int required,
            bool messageStarted,
            Action? startInterChunkTimer,
            Action? stopInterChunkTimer,
            CancellationToken cancellation)
        {
            while (true)
            {
                var timerStarted = false;
                try
                {
                    if (messageStarted)
                    {
                        startInterChunkTimer?.Invoke();
                        timerStarted = true;
                    }

                    var result = await reader.ReadAsync(cancellation).ConfigureAwait(false);
                    var buffer = result.Buffer;

                    if (buffer.Length >= required)
                    {
                        return buffer;
                    }

                    if (result.IsCompleted)
                    {
                        throw new EndOfStreamException(
                            $"The HSMS byte stream ended with {buffer.Length} of {required} required byte(s) available.");
                    }

                    messageStarted |= !buffer.IsEmpty;
                    reader.AdvanceTo(consumed: buffer.Start, examined: buffer.End);
                }
                finally
                {
                    if (timerStarted)
                    {
                        stopInterChunkTimer?.Invoke();
                    }
                }
            }
        }
    }

    private sealed class ItemList(int size)
    {
        private readonly Item[] _items = new Item[size];
        private int _current;

        public bool IsFull => _current == _items.Length;
        public void Add(Item item) => _items[_current++] = item;
        public Item[] Items => _items;

        public void DisposeItems()
        {
            for (var i = 0; i < _current; i++)
            {
                _items[i].Dispose();
            }
        }
    }
}
