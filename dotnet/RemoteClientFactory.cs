#pragma warning disable CS1591
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Grpc.Net.Client;

namespace Acyclic.Sdk.Transport;

/// Thin .NET plumbing over the Rust-emitted policy snapshot.
public sealed class RemoteClient : IDisposable
{
    private readonly HttpMessageHandler _handler;
    internal RemoteClient(GrpcChannel channel, GeneratedRemotePolicy.Transport transport, Metadata headers, HttpMessageHandler handler)
    { Channel = channel; Transport = transport; Headers = headers; _handler = handler; }
    public GrpcChannel Channel { get; }
    public GeneratedRemotePolicy.Transport Transport { get; }
    public Metadata Headers { get; }
    public void Dispose() { Channel.Dispose(); _handler.Dispose(); }
}

public static class RemoteClientFactory
{
    public sealed record TlsCredentials(string CaCertificatePem, string? ClientCertificatePem = null, string? PrivateKeyPem = null);

    public static RemoteClient Create(string family) =>
        Create(family, streaming: false, GeneratedRemotePolicy.Defaults.FromEnvironment(), null, null);

    public static RemoteClient Create(string family, bool streaming, GeneratedRemotePolicy.Defaults defaults, GeneratedRemotePolicy.Transport? overrideTransport, TlsCredentials? tls = null)
    {
        var requiresBearer = GeneratedRemotePolicy.RequiresBearer(family, GeneratedRemotePolicy.Runtime.Native);
        var transport = GeneratedRemotePolicy.Select(family, GeneratedRemotePolicy.Runtime.Native, streaming, bearerAuth: requiresBearer, GeneratedRemotePolicy.Availability.GrpcOnly, GeneratedRemotePolicy.Availability.All, overrideTransport);
        if (transport != GeneratedRemotePolicy.Transport.Grpc)
            throw new NotSupportedException($"the installed .NET package has no {transport} adapter");
        var handler = CreateHttpHandler(tls);
        try
        {
            // NativeTlsStream has already completed the TLS handshake.  An HTTPS
            // channel would make SocketsHttpHandler wrap it in Schannel a second
            // time, so use the handler's HTTP/2 cleartext mode internally while
            // preserving the caller's endpoint and Rustls policy at the boundary.
            var channelEndpoint = defaults.Endpoint;
            if (tls?.ClientCertificatePem is not null)
            {
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
                var originalEndpoint = new Uri(defaults.Endpoint, UriKind.Absolute);
                var endpoint = new UriBuilder(originalEndpoint)
                {
                    Scheme = "http",
                    // UriBuilder otherwise changes an implicit HTTPS 443 to
                    // HTTP 80 when the internal scheme is rewritten.
                    Port = originalEndpoint.Port,
                };
                channelEndpoint = endpoint.Uri.ToString();
            }
            var channel = GrpcChannel.ForAddress(channelEndpoint, new GrpcChannelOptions { HttpHandler = handler });
            var headers = new Metadata();
            if (requiresBearer)
            {
                var token = GeneratedRemotePolicy.ValidateBearer(defaults.BearerToken);
                headers.Add("authorization", $"Bearer {token}");
            }
            return new RemoteClient(channel, transport, headers, handler);
        }
        catch { handler.Dispose(); throw; }
    }

    private static HttpMessageHandler CreateHttpHandler(TlsCredentials? tls)
    {
        if (tls is null) return new SocketsHttpHandler();
        if (string.IsNullOrWhiteSpace(tls.CaCertificatePem))
            throw new ArgumentException("CA certificate PEM is required", nameof(tls));
        if ((tls.ClientCertificatePem is null) != (tls.PrivateKeyPem is null))
            throw new ArgumentException("client certificate and private key must be supplied together", nameof(tls));

        // Rust owns the native TLS boundary whenever mutual TLS is requested.
        // HTTP/2 and generated protobuf remain in Grpc.Net.Client, so every
        // generated RPC keeps the same CallInvoker and cancellation semantics.
        if (tls.ClientCertificatePem is not null)
        {
            var handler = new SocketsHttpHandler { ConnectCallback = (context, cancellationToken) => NativeTlsStream.ConnectAsync(context.InitialRequestMessage.RequestUri ?? throw new InvalidOperationException("gRPC request has no destination URI"), tls, cancellationToken) };
            return handler;
        }

        var ca = X509Certificate2.CreateFromPem(tls.CaCertificatePem);
        var caOnly = new SocketsHttpHandler();
        caOnly.SslOptions = new System.Net.Security.SslClientAuthenticationOptions
        {
            RemoteCertificateValidationCallback = (_, certificate, _, _) =>
            {
                if (certificate is null) return false;
                using var server = certificate as X509Certificate2 ?? new X509Certificate2(certificate);
                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.Add(ca);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                return chain.Build(server);
            },
        };
        return caOnly;
    }
}

