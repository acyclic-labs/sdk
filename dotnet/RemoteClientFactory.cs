#pragma warning disable CS1591
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Grpc.Core;
using Grpc.Net.Client;

namespace Acyclic.Sdk.Transport;

/// Thin .NET plumbing over the Rust-emitted policy snapshot.
public sealed class RemoteClient : IDisposable
{
    internal RemoteClient(GrpcChannel channel, GeneratedRemotePolicy.Transport transport, Metadata headers)
    {
        Channel = channel;
        Transport = transport;
        Headers = headers;
    }

    public GrpcChannel Channel { get; }
    public GeneratedRemotePolicy.Transport Transport { get; }
    public Metadata Headers { get; }

    public void Dispose() => Channel.Dispose();
}

public static class RemoteClientFactory
{
    public sealed record TlsCredentials(
        string CaCertificatePem,
        string? ClientCertificatePem = null,
        string? PrivateKeyPem = null);

    public static RemoteClient Create(string family) =>
        Create(family, streaming: false, GeneratedRemotePolicy.Defaults.FromEnvironment(), null, null);

    public static RemoteClient Create(
        string family,
        bool streaming,
        GeneratedRemotePolicy.Defaults defaults,
        GeneratedRemotePolicy.Transport? overrideTransport,
        TlsCredentials? tls = null)
    {
        var requiresBearer = GeneratedRemotePolicy.RequiresBearer(
            family, GeneratedRemotePolicy.Runtime.Native);
        var transport = GeneratedRemotePolicy.Select(
            family,
            GeneratedRemotePolicy.Runtime.Native,
            streaming,
            bearerAuth: requiresBearer,
            GeneratedRemotePolicy.Availability.GrpcOnly,
            GeneratedRemotePolicy.Availability.All,
            overrideTransport);
        if (transport != GeneratedRemotePolicy.Transport.Grpc)
            throw new NotSupportedException($"the installed .NET package has no {transport} adapter");

        var channel = GrpcChannel.ForAddress(defaults.Endpoint, new GrpcChannelOptions
        {
            HttpHandler = CreateHttpHandler(tls),
        });
        var headers = new Metadata();
        if (requiresBearer)
        {
            var token = GeneratedRemotePolicy.ValidateBearer(defaults.BearerToken);
            headers.Add("authorization", $"Bearer {token}");
        }
        return new RemoteClient(channel, transport, headers);
    }

    private static HttpMessageHandler CreateHttpHandler(TlsCredentials? tls)
    {
        if (tls is null) return new SocketsHttpHandler();
        if (string.IsNullOrWhiteSpace(tls.CaCertificatePem))
            throw new ArgumentException("CA certificate PEM is required", nameof(tls));
        if ((tls.ClientCertificatePem is null) != (tls.PrivateKeyPem is null))
            throw new ArgumentException("client certificate and private key must be supplied together", nameof(tls));

        var ca = X509Certificate2.CreateFromPem(tls.CaCertificatePem);
        var client = tls.ClientCertificatePem is null
            ? null
            : ImportClientCertificate(tls.ClientCertificatePem, tls.PrivateKeyPem!);
        var handler = new SocketsHttpHandler();
        handler.SslOptions = new SslClientAuthenticationOptions
        {
            ClientCertificates = client is null
                ? null
                : new X509CertificateCollection { client },
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
        return handler;
    }

    private static X509Certificate2 ImportClientCertificate(string certificatePem, string privateKeyPem)
    {
        using var certificate = X509Certificate2.CreateFromPem(certificatePem);
        try
        {
            using var ecdsa = System.Security.Cryptography.ECDsa.Create();
            ecdsa.ImportFromPem(privateKeyPem);
            return certificate.CopyWithPrivateKey(ecdsa);
        }
        catch (System.Security.Cryptography.CryptographicException)
        {
            using var rsa = System.Security.Cryptography.RSA.Create();
            rsa.ImportFromPem(privateKeyPem);
            return certificate.CopyWithPrivateKey(rsa);
        }
    }
}
