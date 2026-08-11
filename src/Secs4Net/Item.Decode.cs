using CommunityToolkit.HighPerformance;
using CommunityToolkit.HighPerformance.Buffers;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;

namespace Secs4Net;

public partial class Item
{
    internal const int MaximumListNestingDepth = 100;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void DecodeFormatAndLengthByteCount(in ReadOnlySequence<byte> sourceBytes, out SecsFormat format, out byte lengthByteCount)
    {
#if NET
        var formatSeqFirstSpan = sourceBytes.FirstSpan;
#else
        var formatSeqFirstSpan = sourceBytes.First.Span;
#endif

#if DEBUG
        byte formatAndLengthByte = formatSeqFirstSpan[0];
#else
        byte formatAndLengthByte = formatSeqFirstSpan.DangerousGetReference();
#endif
        format = (SecsFormat)(formatAndLengthByte >> 2);
        lengthByteCount = (byte)(formatAndLengthByte & 0b00000011);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    internal static int DecodeDataLength(in ReadOnlySequence<byte> sourceBytes)
    {
        var dataLength = 0;
        var lengthBytes = dataLength.AsBytes();
        sourceBytes.CopyTo(lengthBytes);
        lengthBytes[..(int)sourceBytes.Length].Reverse();
        return dataLength;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    [SkipLocalsInit]
    public static Item DecodeFromFullBuffer(ref ReadOnlySequence<byte> bytes)
        => DecodeFromFullBuffer(ref bytes, nestingDepth: 0);

    private static Item DecodeFromFullBuffer(ref ReadOnlySequence<byte> bytes, int nestingDepth)
    {
        if (bytes.IsEmpty)
        {
            throw new InvalidDataException("The SECS-II data ended before an item format byte was received.");
        }

        var formatSeq = bytes.Slice(0, 1);
        DecodeFormatAndLengthByteCount(formatSeq, out var format, out var lengthByteCount);

        if (lengthByteCount == 0)
        {
            throw new InvalidDataException("A SECS-II item must use between one and three length bytes.");
        }

        if (bytes.Length - 1 < lengthByteCount)
        {
            throw new InvalidDataException("The SECS-II data ended inside an item length field.");
        }

        var dataLengthSeq = bytes.Slice(formatSeq.End, lengthByteCount);
        var dataLength = DecodeDataLength(dataLengthSeq);
        bytes = bytes.Slice(dataLengthSeq.End);

        if (format == SecsFormat.List)
        {
            if (dataLength == 0)
            {
                return L();
            }

            ValidateListItemCount(dataLength, bytes.Length);
            if (nestingDepth >= MaximumListNestingDepth)
            {
                throw new InvalidDataException($"SECS-II List nesting exceeds the maximum depth of {MaximumListNestingDepth}.");
            }

            var items = new Item[dataLength];
            var decodedItemCount = 0;
            try
            {
                foreach (ref var subItem in items.AsSpan())
                {
                    subItem = DecodeFromFullBuffer(ref bytes, nestingDepth + 1);
                    decodedItemCount++;
                }
            }
            catch
            {
                for (var i = 0; i < decodedItemCount; i++)
                {
                    items[i].Dispose();
                }

                throw;
            }

            return L(items);
        }

        if (dataLength > bytes.Length)
        {
            throw new InvalidDataException(
                $"SECS-II item length {dataLength} exceeds the {bytes.Length} byte(s) left in the buffer.");
        }

        var dataItemBytes = bytes.Slice(0, dataLength);
        var item = DecodeDataItem(format, dataItemBytes);
        bytes = bytes.Slice(dataItemBytes.End);
        return item;
    }

    internal static Item DecodeDataItem(SecsFormat format, in ReadOnlySequence<byte> bytes)
    {
        var length = (int)bytes.Length;
        ValidateDataItemLength(format, length);
        return (format, length) switch
        {
            (SecsFormat.ASCII, 0) => A(),
            (SecsFormat.ASCII, >= 256) => A(ASCIIEncoding.GetString(bytes)),
            (SecsFormat.ASCII, _) => A(DecodePooledString(length, bytes, ASCIIEncoding)),

            (SecsFormat.JIS8, 0) => J(),
            (SecsFormat.JIS8, >= 256) => J(JIS8Encoding.GetString(bytes)),
            (SecsFormat.JIS8, _) => J(DecodePooledString(length, bytes, JIS8Encoding)),

            (SecsFormat.Binary, 0) => B(),
            (SecsFormat.Binary, >= 1024) => B(DecodeMemoryOwner<byte>(length, bytes)),
            (SecsFormat.Binary, _) => B(DecodeMemory<byte>(length, bytes)),

            (SecsFormat.Boolean, 0) => Boolean(),
            (SecsFormat.Boolean, >= 1024) => Boolean(DecodeMemoryOwner<bool>(length, bytes)),
            (SecsFormat.Boolean, _) => Boolean(DecodeMemory<bool>(length, bytes)),

            (SecsFormat.I8, 0) => I8(),
            (SecsFormat.I8, >= 1024) => I8(DecodeMemoryOwner<long>(length, bytes)),
            (SecsFormat.I8, _) => I8(DecodeMemory<long>(length, bytes)),

            (SecsFormat.I1, 0) => I1(),
            (SecsFormat.I1, >= 1024) => I1(DecodeMemoryOwner<sbyte>(length, bytes)),
            (SecsFormat.I1, _) => I1(DecodeMemory<sbyte>(length, bytes)),

            (SecsFormat.I2, 0) => I2(),
            (SecsFormat.I2, >= 1024) => I2(DecodeMemoryOwner<short>(length, bytes)),
            (SecsFormat.I2, _) => I2(DecodeMemory<short>(length, bytes)),

            (SecsFormat.I4, 0) => I4(),
            (SecsFormat.I4, >= 1024) => I4(DecodeMemoryOwner<int>(length, bytes)),
            (SecsFormat.I4, _) => I4(DecodeMemory<int>(length, bytes)),

            (SecsFormat.F8, 0) => F8(),
            (SecsFormat.F8, >= 1024) => F8(DecodeMemoryOwner<double>(length, bytes)),
            (SecsFormat.F8, _) => F8(DecodeMemory<double>(length, bytes)),

            (SecsFormat.F4, 0) => F4(),
            (SecsFormat.F4, >= 1024) => F4(DecodeMemoryOwner<float>(length, bytes)),
            (SecsFormat.F4, _) => F4(DecodeMemory<float>(length, bytes)),

            (SecsFormat.U8, 0) => U8(),
            (SecsFormat.U8, >= 1024) => U8(DecodeMemoryOwner<ulong>(length, bytes)),
            (SecsFormat.U8, _) => U8(DecodeMemory<ulong>(length, bytes)),

            (SecsFormat.U1, 0) => U1(),
            (SecsFormat.U1, >= 1024) => U1(DecodeMemoryOwner<byte>(length, bytes)),
            (SecsFormat.U1, _) => U1(DecodeMemory<byte>(length, bytes)),

            (SecsFormat.U2, 0) => U2(),
            (SecsFormat.U2, >= 1024) => U2(DecodeMemoryOwner<ushort>(length, bytes)),
            (SecsFormat.U2, _) => U2(DecodeMemory<ushort>(length, bytes)),

            (SecsFormat.U4, 0) => U4(),
            (SecsFormat.U4, >= 1024) => U4(DecodeMemoryOwner<uint>(length, bytes)),
            (SecsFormat.U4, _) => U4(DecodeMemory<uint>(length, bytes)),
            _ => ThrowHelper(),
        };

        [SkipLocalsInit]
        static string DecodePooledString(int length, in ReadOnlySequence<byte> bytes, Encoding encoding)
        {
            using var spanOwner = SpanOwner<byte>.Allocate(length);
            var span = spanOwner.Span;
            bytes.CopyTo(span);
            return StringPool.Shared.GetOrAdd(span, encoding);
        }

        [SkipLocalsInit]
        static unsafe Memory<T> DecodeMemory<T>(int length, in ReadOnlySequence<byte> bytes) where T : unmanaged, IEquatable<T>
        {
            var memory = new T[length / sizeof(T)];
            var span = memory.AsSpan();
            bytes.CopyTo(span.AsBytes());
            ReverseEndiannessHelper<T>.Reverse(span);
            return memory;
        }

        [SkipLocalsInit]
        static unsafe IMemoryOwner<T> DecodeMemoryOwner<T>(int length, in ReadOnlySequence<byte> bytes) where T : unmanaged, IEquatable<T>
        {
            var owner = MemoryOwner<T>.Allocate(length / sizeof(T));
            try
            {
                var span = owner.Span;
                bytes.CopyTo(span.AsBytes());
                ReverseEndiannessHelper<T>.Reverse(span);
                return owner;
            }
            catch
            {
                owner.Dispose();
                throw;
            }
        }

        [DoesNotReturn]
        static Item ThrowHelper() => throw new ArgumentOutOfRangeException();
    }

    internal static void ValidateListItemCount(int itemCount, long remainingByteCount)
    {
        // Every child requires at least a format byte and one length byte. Check this before
        // allocating the Item[] so a tiny malformed frame cannot request a huge allocation.
        if (itemCount > remainingByteCount / 2)
        {
            throw new InvalidDataException(
                $"SECS-II List declares {itemCount} item(s), but at most {remainingByteCount / 2} can fit in the {remainingByteCount} remaining byte(s).");
        }
    }

    private static void ValidateDataItemLength(SecsFormat format, int byteLength)
    {
        var elementSize = format switch
        {
            SecsFormat.I2 or SecsFormat.U2 => 2,
            SecsFormat.I4 or SecsFormat.F4 or SecsFormat.U4 => 4,
            SecsFormat.I8 or SecsFormat.F8 or SecsFormat.U8 => 8,
            _ => 1,
        };

        if (byteLength % elementSize != 0)
        {
            throw new InvalidDataException(
                $"SECS-II {format} item length {byteLength} is not divisible by its {elementSize}-byte element size.");
        }
    }
}
