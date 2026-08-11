using CommunityToolkit.HighPerformance.Buffers;
using FluentAssertions;
using System;
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Secs4Net.UnitTests;

public class PipeDecoderUnitTests
{
    private readonly SecsMessage message = new(s: 1, f: 2, replyExpected: false)
    {
        SecsItem =
        L(
            L(),
            U1(122, 34),
            U2(34531, 23123),
            U4(2123513, 52451141),
            F4(23123.21323f, 2324.221f),
            A("A string"),
            J("sdsad"),
            F8(231.00002321d, 0.2913212312d),
            L(
                U1(122, 34),
                U2(34531, 23123),
                U4(2123513, 52451141),
                F4(23123.21323f, 2324.221f),
                Boolean(true, false, false, true),
                B(0x1C, 0x01, 0xFF),
                L(
                    A("A string"),
                    J("sdsad"),
                    Boolean(true, false, false, true),
                    B(0x1C, 0x01, 0xFF)),
                F8(231.00002321d, 0.2913212312d)))
    };

    [Fact]
    public void Message_Equals_Should_Be_True()
    {
        var subject = new SecsMessage(s: 1, f: 2, replyExpected: false)
        {
            SecsItem =
                L(
                    L(),
                    U1(122, 34),
                    U2(34531, 23123),
                    U4(2123513, 52451141),
                    F4(23123.21323f, 2324.221f),
                    A("A string"),
                    J("sdsad"),
                    F8(231.00002321d, 0.2913212312d),
                    L(
                        U1(122, 34),
                        U2(34531, 23123),
                        U4(2123513, 52451141),
                        F4(23123.21323f, 2324.221f),
                        Boolean(true, false, false, true),
                        B(0x1C, 0x01, 0xFF),
                        L(
                            A("A string"),
                            J("sdsad"),
                            Boolean(true, false, false, true),
                            B(0x1C, 0x01, 0xFF)),
                        F8(231.00002321d, 0.2913212312d)))
        };

        subject.Should().BeEquivalentTo(message);
    }

    [Fact]
    public async Task Message_Can_Decode_From_Full_Buffer()
    {
        var messageIds = Enumerable.Range(start: 10001, count: 4).ToArray();
        using var buffer = new ArrayPoolBufferWriter<byte>();

        foreach (var id in messageIds)
        {
            SecsGem.EncodeMessage(message, id, deviceId: 0, buffer);
        }
        var encodedBytes = buffer.WrittenMemory;

        var pipe = new Pipe();
        var decoder = new PipeDecoder(pipe.Reader, pipe.Writer);
        using var cancellationSource = new CancellationTokenSource();
        var decoderTask = decoder.StartAsync(cancellationSource.Token);

        await decoder.Input.WriteAsync(encodedBytes);

        var decodeMessages = await decoder.GetDataMessages(default)
            .Take(messageIds.Length)
            .Select(m => new
            {
                m.header.Id,
                Message = new SecsMessage(m.header.S, m.header.F, m.header.ReplyExpected)
                {
                    SecsItem = m.rootItem,
                },
            })
            .ToListAsync();

        foreach (var (id, index) in messageIds.Select((a, index) => (a, index)))
        {
            decodeMessages[index].Id.Should().Be(id);
            decodeMessages[index].Message.Should().BeEquivalentTo(message);
        }

        cancellationSource.Cancel();
        await FluentActions.Awaiting(() => decoderTask).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Message_Can_Decode_From_Chunked_Sequence()
    {
        var messageIds = Enumerable.Range(start: 10001, count: 4).ToArray();
        using var buffer = new ArrayPoolBufferWriter<byte>();

        foreach (var id in messageIds)
        {
            SecsGem.EncodeMessage(message, id, deviceId: 0, buffer);
        }
        var encodedBytes = buffer.WrittenMemory;

        var pipe = new Pipe();
        var decoder = new PipeDecoder(pipe.Reader, pipe.Writer);
        using var cancellationSource = new CancellationTokenSource();
        var decoderTask = decoder.StartAsync(cancellationSource.Token);

        var writerTask = Task.Run(async () =>
        {
#if NET
            var random = Random.Shared;
#else
            var random = new Random(13); 
#endif
            foreach (var chunk in new ChunkedReadOnlyMemory<byte>(encodedBytes, size: 23))
            {
                await Task.Delay(200); //simulate a slow connection

                if (random.Next() % 2 == 0)
                {
                    await decoder.Input.WriteAsync(ReadOnlyMemory<byte>.Empty);
                }

                await decoder.Input.WriteAsync(chunk);
            }
        });

        var decodeMessages = await decoder.GetDataMessages(default)
            .Take(messageIds.Length)
            .Select(m => new
            {
                m.header.Id,
                Message = new SecsMessage(m.header.S, m.header.F, m.header.ReplyExpected)
                {
                    SecsItem = m.rootItem,
                },
            })
            .ToListAsync();

        foreach (var (id, index) in messageIds.Select((a, index) => (a, index)))
        {
            decodeMessages[index].Id.Should().Be(id);
            decodeMessages[index].Message.Should().BeEquivalentTo(message);
        }

        await writerTask;
        cancellationSource.Cancel();
        await FluentActions.Awaiting(() => decoderTask).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Malformed_Data_Message_Is_Skipped_Without_Stopping_Decoder()
    {
        byte[] malformedMessage =
        [
            0x00, 0x00, 0x00, 0x1B,
            0x7F, 0xFF, 0x86, 0x0B, 0x00, 0x00, 0x06, 0x0B, 0x00, 0xD6,
            0x01, 0x03, 0xA5, 0x01, 0x3C, 0xA9, 0x02, 0x00, 0x16,
            0x01, 0x01, 0x01, 0x02, 0xA9, 0x02, 0x22, 0xB8,
        ];

        const int validMessageId = 10001;
        using var validMessageBuffer = new ArrayPoolBufferWriter<byte>();
        SecsGem.EncodeMessage(message, validMessageId, deviceId: 0, validMessageBuffer);

        var input = malformedMessage
            .Concat(validMessageBuffer.WrittenMemory.ToArray())
            .ToArray();

        var pipe = new Pipe();
        var decoder = new PipeDecoder(pipe.Reader, pipe.Writer);
        var decodeErrorSource = new TaskCompletionSource<DataMessageDecodeErrorEventArgs>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        decoder.DataMessageDecodeError += (_, error) => decodeErrorSource.TrySetResult(error);

        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var decoderTask = decoder.StartAsync(cancellationSource.Token);

        // Put the malformed frame and the next valid frame in the same Pipe write. The malformed
        // List must not consume the valid frame while looking for its missing second child.
        await decoder.Input.WriteAsync(input, cancellationSource.Token);

        var decodedMessage = await decoder.GetDataMessages(cancellationSource.Token).FirstAsync();
        var decodeError = await decodeErrorSource.Task;

        decodeError.Header.S.Should().Be(6);
        decodeError.Header.F.Should().Be(11);
        decodeError.Header.Id.Should().Be(0x060B00D6);
        decodeError.EncodedData.ToArray().Should().Equal(malformedMessage.Skip(14));
        decodeError.DataIsTruncated.Should().BeFalse();
        decodeError.Exception.Should().BeOfType<InvalidDataException>(decodeError.Exception.ToString());

        decodedMessage.header.Id.Should().Be(validMessageId);
        new SecsMessage(decodedMessage.header.S, decodedMessage.header.F, decodedMessage.header.ReplyExpected)
        {
            SecsItem = decodedMessage.rootItem,
        }.Should().BeEquivalentTo(message);
        decoderTask.IsCompleted.Should().BeFalse();

        cancellationSource.Cancel();
        await FluentActions.Awaiting(() => decoderTask).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Chunked_Malformed_Data_Message_Is_Skipped_At_Its_Frame_Boundary()
    {
        byte[] malformedMessage =
        [
            0x00, 0x00, 0x00, 0x1B,
            0x7F, 0xFF, 0x86, 0x0B, 0x00, 0x00, 0x06, 0x0B, 0x00, 0xD6,
            0x01, 0x03, 0xA5, 0x01, 0x3C, 0xA9, 0x02, 0x00, 0x16,
            0x01, 0x01, 0x01, 0x02, 0xA9, 0x02, 0x22, 0xB8,
        ];

        const int validMessageId = 10002;
        using var validMessageBuffer = new ArrayPoolBufferWriter<byte>();
        SecsGem.EncodeMessage(message, validMessageId, deviceId: 0, validMessageBuffer);

        var pipe = new Pipe();
        var decoder = new PipeDecoder(pipe.Reader, pipe.Writer);
        var decodeErrorSource = new TaskCompletionSource<DataMessageDecodeErrorEventArgs>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var incrementalDecodingSource = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        decoder.DataMessageDecodeError += (_, error) => decodeErrorSource.TrySetResult(error);
        decoder.IncrementalDataMessageDecodingStarted += () => incrementalDecodingSource.TrySetResult(true);

        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var cancellationRegistration = cancellationSource.Token.Register(
            () => incrementalDecodingSource.TrySetCanceled());
        var decoderTask = decoder.StartAsync(cancellationSource.Token);

        // Write only the length and header first to force the incremental decoder path.
        await decoder.Input.WriteAsync(malformedMessage.AsMemory(0, 14), cancellationSource.Token);
        await incrementalDecodingSource.Task;

        var remainingInput = malformedMessage
            .Skip(14)
            .Concat(validMessageBuffer.WrittenMemory.ToArray())
            .ToArray();
        await decoder.Input.WriteAsync(remainingInput, cancellationSource.Token);

        var decodedMessage = await decoder.GetDataMessages(cancellationSource.Token).FirstAsync();
        var decodeError = await decodeErrorSource.Task;

        decodeError.Header.Id.Should().Be(0x060B00D6);
        decodeError.EncodedData.ToArray().Should().Equal(malformedMessage.Skip(14));
        decodeError.DataIsTruncated.Should().BeFalse();
        decodedMessage.header.Id.Should().Be(validMessageId);
        decoderTask.IsCompleted.Should().BeFalse();

        cancellationSource.Cancel();
        await FluentActions.Awaiting(() => decoderTask).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task Large_Item_Can_Decode_Without_Pipe_Backpressure_Deadlock()
    {
        var largeBinary = Enumerable.Range(0, 128 * 1024)
            .Select(index => (byte)index)
            .ToArray();
        var largeMessage = new SecsMessage(s: 7, f: 3, replyExpected: false)
        {
            SecsItem = B(largeBinary),
        };

        using var encodedBuffer = new ArrayPoolBufferWriter<byte>();
        SecsGem.EncodeMessage(largeMessage, id: 10003, deviceId: 0, encodedBuffer);

        var pipe = new Pipe();
        var decoder = new PipeDecoder(pipe.Reader, pipe.Writer);
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var decoderTask = decoder.StartAsync(cancellationSource.Token);

        var writerTask = Task.Run(async () =>
        {
            foreach (var chunk in new ChunkedReadOnlyMemory<byte>(encodedBuffer.WrittenMemory, size: 8192))
            {
                await decoder.Input.WriteAsync(chunk, cancellationSource.Token);
            }
        });

        var decodedMessage = await decoder.GetDataMessages(cancellationSource.Token).FirstAsync();
        await writerTask;

        decodedMessage.header.Id.Should().Be(10003);
        decodedMessage.rootItem.Should().NotBeNull();
        decodedMessage.rootItem!.GetMemory<byte>().ToArray().Should().Equal(largeBinary);

        cancellationSource.Cancel();
        await FluentActions.Awaiting(() => decoderTask).Should().ThrowAsync<OperationCanceledException>();
    }
}
