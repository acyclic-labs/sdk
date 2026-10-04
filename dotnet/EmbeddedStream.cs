using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Google.Protobuf;

namespace Acyclic.Sdk.Embedded;

public sealed record EmbeddedRecord(ulong Sequence, byte[] Value);

public enum EmbeddedStatus : uint
{
    Ok = 0,
    End = 1,
    Pending = 2,
    Cancelled = 3,
    InvalidArgument = 4,
    ProviderError = 5,
    Capacity = 6,
    Panic = 7,
}

public sealed class EmbeddedStreamException : Exception
{
    public EmbeddedStreamException(EmbeddedStatus status, string message)
        : base($"Rust embedded Stream operation failed with {status}: {message}") => Status = status;

    public EmbeddedStatus Status { get; }
}

/// Thin ownership and cancellation facade over the canonical Rust C ABI.
public sealed class EmbeddedStreamEngine : IDisposable
{
    public static IReadOnlyList<string> CanonicalOperations { get; } =
        new[] { "inspect_idempotency", "append", "tail", "fork", "read", "follow", "children", "children_page", "commit", "read_commit" };

    private readonly ulong _handle;
    private int _disposed;

    public EmbeddedStreamEngine()
    {
        if (Rust.acyclic_embedded_abi_version() != 1)
            throw new PlatformNotSupportedException("Unsupported Rust embedded ABI version");
        _handle = Rust.acyclic_embedded_engine_open();
        if (_handle == 0) throw new InvalidOperationException("Rust embedded engine could not be opened");
    }

    public AppendReceipt Append(string path, ReadOnlySpan<byte> value)
    {
        ThrowIfDisposed();
        var pathBytes = Encoding.UTF8.GetBytes(path ?? throw new ArgumentNullException(nameof(path)));
        unsafe
        {
            fixed (byte* pathPtr = pathBytes)
            fixed (byte* valuePtr = value)
            {
                var result = Rust.acyclic_embedded_engine_append(_handle, pathPtr, (nuint)pathBytes.Length, valuePtr, (nuint)value.Length);
                try
                {
                    ThrowOnFailure(result.Status, result.Message);
                    return new AppendReceipt(result.Start, result.End, result.Tail);
                }
                finally { Rust.acyclic_append_result_release(result); }
            }
        }
    }

    /// Executes one unary generated Stream protobuf operation through Rust.
    ///
    /// The request and response are the generated acyclic.stream.v2 messages. This method keeps
    /// the protobuf model in the language SDK while Rust remains the implementation authority.
    public byte[] CallWire(string operation, ReadOnlySpan<byte> request)
    {
        ThrowIfDisposed();
        var operationBytes = Encoding.UTF8.GetBytes(operation ?? throw new ArgumentNullException(nameof(operation)));
        unsafe
        {
            fixed (byte* operationPtr = operationBytes)
            fixed (byte* requestPtr = request)
            {
                var result = Rust.acyclic_embedded_engine_wire_call(
                    _handle,
                    operationPtr,
                    (nuint)operationBytes.Length,
                    requestPtr,
                    (nuint)request.Length);
                try
                {
                    ThrowOnFailure(result.Status, result.Message);
                    return Copy(result.Response);
                }
                finally { Rust.acyclic_wire_result_release(result); }
            }
        }
    }

    /// Decodes the generated protobuf response while keeping the operation implementation in Rust.
    public TResponse CallWire<TResponse>(string operation, IMessage request, MessageParser<TResponse> parser)
        where TResponse : class, IMessage<TResponse> =>
        parser.ParseFrom(CallWire(operation, request.ToByteArray()));

    // These named methods are the generated operation surface. The request and response
    // bytes are the corresponding acyclic.stream.v2 protobuf messages; Rust owns encoding
    // validation and execution while the facade owns only the byte lifetime.
    public byte[] InspectIdempotency(ReadOnlySpan<byte> request) => CallWire("inspect_idempotency", request);
    public byte[] AppendWire(ReadOnlySpan<byte> request) => CallWire("append", request);
    public byte[] Tail(ReadOnlySpan<byte> request) => CallWire("tail", request);
    public byte[] Fork(ReadOnlySpan<byte> request) => CallWire("fork", request);
    public byte[] ChildrenPage(ReadOnlySpan<byte> request) => CallWire("children_page", request);
    public byte[] Commit(ReadOnlySpan<byte> request) => CallWire("commit", request);
    public byte[] ReadCommit(ReadOnlySpan<byte> request) => CallWire("read_commit", request);

    public TResponse InspectIdempotency<TResponse>(IMessage request, MessageParser<TResponse> parser)
        where TResponse : class, IMessage<TResponse> => CallWire("inspect_idempotency", request, parser);
    public TResponse AppendWire<TResponse>(IMessage request, MessageParser<TResponse> parser)
        where TResponse : class, IMessage<TResponse> => CallWire("append", request, parser);
    public TResponse Tail<TResponse>(IMessage request, MessageParser<TResponse> parser)
        where TResponse : class, IMessage<TResponse> => CallWire("tail", request, parser);
    public TResponse Fork<TResponse>(IMessage request, MessageParser<TResponse> parser)
        where TResponse : class, IMessage<TResponse> => CallWire("fork", request, parser);
    public TResponse ChildrenPage<TResponse>(IMessage request, MessageParser<TResponse> parser)
        where TResponse : class, IMessage<TResponse> => CallWire("children_page", request, parser);
    public TResponse Commit<TResponse>(IMessage request, MessageParser<TResponse> parser)
        where TResponse : class, IMessage<TResponse> => CallWire("commit", request, parser);
    public TResponse ReadCommit<TResponse>(IMessage request, MessageParser<TResponse> parser)
        where TResponse : class, IMessage<TResponse> => CallWire("read_commit", request, parser);

    public IAsyncEnumerable<EmbeddedRecord> ReadAsync(string path, ulong from, uint limit, CancellationToken cancellationToken = default) =>
        EnumerateAsync(path, from, limit, follow: false, cancellationToken);

    public IAsyncEnumerable<EmbeddedRecord> FollowAsync(string path, ulong from, CancellationToken cancellationToken = default) =>
        EnumerateAsync(path, from, 0, follow: true, cancellationToken);

    private async IAsyncEnumerable<EmbeddedRecord> EnumerateAsync(
        string path,
        ulong from,
        uint limit,
        bool follow,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        var pathBytes = Encoding.UTF8.GetBytes(path ?? throw new ArgumentNullException(nameof(path)));
        var reader = OpenReader(pathBytes, from, limit, follow);
        using var cancellation = cancellationToken.Register(static state => Rust.acyclic_embedded_reader_cancel((ulong)state!), reader);
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var next = await Task.Run(() => Rust.acyclic_embedded_reader_next(reader), cancellationToken).ConfigureAwait(false);
                try
                {
                    if (next.Status is EmbeddedStatus.End or EmbeddedStatus.Cancelled) yield break;
                    ThrowOnFailure(next.Status, next.Message);
                    yield return new EmbeddedRecord(next.Sequence, Copy(next.Value));
                }
                finally { Rust.acyclic_next_result_release(next); }
            }
        }
        finally { Rust.acyclic_embedded_reader_close(reader); }
    }

    private unsafe ulong OpenReader(byte[] path, ulong from, uint limit, bool follow)
    {
        fixed (byte* pathPtr = path)
        {
            var opened = Rust.acyclic_embedded_reader_open(_handle, pathPtr, (nuint)path.Length, from, limit, follow ? 1u : 0u);
            try
            {
                ThrowOnFailure(opened.Status, opened.Message);
                var reader = Rust.acyclic_open_result_take_reader(ref opened);
                if (reader == 0)
                    throw new EmbeddedStreamException(EmbeddedStatus.Panic, "Rust returned no reader handle");
                return reader;
            }
            finally { Rust.acyclic_open_result_release(opened); }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            Rust.acyclic_embedded_engine_close(_handle);
        GC.SuppressFinalize(this);
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(EmbeddedStreamEngine));
    }

    private static byte[] Copy(RustBuffer buffer)
    {
        if (buffer.Length == 0) return Array.Empty<byte>();
        if (buffer.Pointer == IntPtr.Zero) throw new EmbeddedStreamException(EmbeddedStatus.Panic, "Rust returned a null nonempty buffer");
        var bytes = new byte[checked((int)buffer.Length)];
        Marshal.Copy(buffer.Pointer, bytes, 0, bytes.Length);
        return bytes;
    }

    private static void ThrowOnFailure(EmbeddedStatus status, RustBuffer message)
    {
        if (status is EmbeddedStatus.Ok or EmbeddedStatus.End or EmbeddedStatus.Cancelled) return;
        var text = message.Length == 0 ? "no diagnostic" : Encoding.UTF8.GetString(Copy(message));
        throw new EmbeddedStreamException(status, text);
    }

    public readonly record struct AppendReceipt(ulong Start, ulong End, ulong Tail);

    [StructLayout(LayoutKind.Sequential)]
    private struct RustBuffer { public ulong Id; public IntPtr Pointer; public nuint Length; public nuint Capacity; }
    [StructLayout(LayoutKind.Sequential)]
    private struct RustAppendResult { public EmbeddedStatus Status; public ulong Start; public ulong End; public ulong Tail; public RustBuffer Message; }
    [StructLayout(LayoutKind.Sequential)]
    private struct RustOpenResult { public EmbeddedStatus Status; public ulong Reader; public RustBuffer Message; }
    [StructLayout(LayoutKind.Sequential)]
    private struct RustNextResult { public EmbeddedStatus Status; public ulong Sequence; public RustBuffer Value; public RustBuffer Message; }
    [StructLayout(LayoutKind.Sequential)]
    private struct RustWireResult { public EmbeddedStatus Status; public RustBuffer Response; public RustBuffer Message; }

    private static class Rust
    {
        private const string Library = "acyclic_sdk_embedded_prototype";
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern uint acyclic_embedded_abi_version();
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern ulong acyclic_embedded_engine_open();
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void acyclic_embedded_engine_close(ulong engine);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern unsafe RustAppendResult acyclic_embedded_engine_append(ulong engine, byte* path, nuint pathLength, byte* value, nuint valueLength);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern unsafe RustWireResult acyclic_embedded_engine_wire_call(ulong engine, byte* operation, nuint operationLength, byte* request, nuint requestLength);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern unsafe RustOpenResult acyclic_embedded_reader_open(ulong engine, byte* path, nuint pathLength, ulong from, uint limit, uint mode);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern unsafe ulong acyclic_open_result_take_reader(ref RustOpenResult result);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void acyclic_open_result_release(RustOpenResult result);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern RustNextResult acyclic_embedded_reader_next(ulong reader);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void acyclic_embedded_reader_cancel(ulong reader);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void acyclic_embedded_reader_close(ulong reader);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void acyclic_append_result_release(RustAppendResult result);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void acyclic_next_result_release(RustNextResult result);
        [DllImport(Library, CallingConvention = CallingConvention.Cdecl)] internal static extern void acyclic_wire_result_release(RustWireResult result);
    }
}
