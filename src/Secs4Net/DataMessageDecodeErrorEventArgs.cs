namespace Secs4Net;

public sealed class DataMessageDecodeErrorEventArgs : EventArgs
{
    public MessageHeader Header { get; }
    public ReadOnlyMemory<byte> EncodedData { get; }
    public bool DataIsTruncated { get; }
    public Exception Exception { get; }

    internal DataMessageDecodeErrorEventArgs(
        MessageHeader header,
        ReadOnlyMemory<byte> encodedData,
        bool dataIsTruncated,
        Exception exception)
    {
        Header = header;
        EncodedData = encodedData;
        DataIsTruncated = dataIsTruncated;
        Exception = exception;
    }
}
