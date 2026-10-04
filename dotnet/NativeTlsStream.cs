using System.Net;
using System.Runtime.InteropServices;

namespace Acyclic.Sdk.Transport;

/// Stream adapter that keeps HTTP/2 and protobuf in Grpc.Net.Client while Rust
/// owns the native TLS handshake and private-key boundary.
internal sealed class NativeTlsStream : System.IO.Stream
{
    private IntPtr _handle;
    private bool _disposed;

    private NativeTlsStream(IntPtr handle) => _handle = handle;

    internal static async ValueTask<System.IO.Stream> ConnectAsync(
        Uri destination,
        RemoteClientFactory.TlsCredentials credentials,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return await Task.Run(() => Connect(destination, credentials, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    private static NativeTlsStream Connect(
        Uri destination,
        RemoteClientFactory.TlsCredentials credentials,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var port = destination.IsDefaultPort ? 443 : destination.Port;
        var endpoint = $"https://{destination.Host}:{port}";
        var ca = System.Text.Encoding.UTF8.GetBytes(credentials.CaCertificatePem);
        var certificate = System.Text.Encoding.UTF8.GetBytes(credentials.ClientCertificatePem ?? string.Empty);
        var privateKey = System.Text.Encoding.UTF8.GetBytes(credentials.PrivateKeyPem ?? string.Empty);
        var handle = acyclic_dotnet_tls_open(endpoint, ca, (nuint)ca.Length, certificate, (nuint)certificate.Length, privateKey, (nuint)privateKey.Length);
        if (handle == IntPtr.Zero)
            throw new System.IO.IOException(GetLastError());
        return new NativeTlsStream(handle);
    }

    private static string GetLastError()
    {
        var pointer = acyclic_dotnet_tls_last_error();
        return pointer == IntPtr.Zero ? "Rust native TLS transport failed" : Marshal.PtrToStringAnsi(pointer) ?? "Rust native TLS transport failed";
    }

    [DllImport("sdk_dotnet_transport", CallingConvention = CallingConvention.Cdecl, EntryPoint = "acyclic_dotnet_tls_open")]
    private static extern IntPtr acyclic_dotnet_tls_open(string endpoint, byte[] ca, nuint caLength, byte[] certificate, nuint certificateLength, byte[] privateKey, nuint privateKeyLength);

    [DllImport("sdk_dotnet_transport", CallingConvention = CallingConvention.Cdecl, EntryPoint = "acyclic_dotnet_tls_read")]
    private static extern nint acyclic_dotnet_tls_read(IntPtr handle, byte[] buffer, nuint length);

    [DllImport("sdk_dotnet_transport", CallingConvention = CallingConvention.Cdecl, EntryPoint = "acyclic_dotnet_tls_write")]
    private static extern nint acyclic_dotnet_tls_write(IntPtr handle, byte[] buffer, nuint length);

    [DllImport("sdk_dotnet_transport", CallingConvention = CallingConvention.Cdecl, EntryPoint = "acyclic_dotnet_tls_close")]
    private static extern void acyclic_dotnet_tls_close(IntPtr handle);

    [DllImport("sdk_dotnet_transport", CallingConvention = CallingConvention.Cdecl, EntryPoint = "acyclic_dotnet_tls_last_error")]
    private static extern IntPtr acyclic_dotnet_tls_last_error();

    public override bool CanRead => !_disposed;
    public override bool CanSeek => false;
    public override bool CanWrite => !_disposed;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    public override int Read(byte[] buffer, int offset, int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var slice = buffer.AsSpan(offset, count).ToArray();
        var result = acyclic_dotnet_tls_read(_handle, slice, (nuint)slice.Length);
        if (result < 0) throw new System.IO.IOException(GetLastError());
        slice.AsSpan(0, checked((int)result)).CopyTo(buffer.AsSpan(offset));
        return checked((int)result);
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var slice = buffer.AsSpan(offset, count).ToArray();
        var written = 0;
        while (written < slice.Length)
        {
            var result = acyclic_dotnet_tls_write(_handle, slice.AsSpan(written).ToArray(), (nuint)(slice.Length - written));
            if (result < 0) throw new System.IO.IOException(GetLastError());
            if (result == 0) throw new System.IO.IOException("Rust native TLS transport made no write progress");
            written += checked((int)result);
        }
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var temporary = new byte[buffer.Length];
        var count = await Task.Run(() => Read(temporary, 0, temporary.Length), cancellationToken).ConfigureAwait(false);
        temporary.AsMemory(0, count).CopyTo(buffer);
        return count;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        new(Task.Run(() => Write(buffer.ToArray(), 0, buffer.Length), cancellationToken));

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
            if (_handle != IntPtr.Zero)
            {
                acyclic_dotnet_tls_close(_handle);
                _handle = IntPtr.Zero;
            }
        }
        base.Dispose(disposing);
    }
}



