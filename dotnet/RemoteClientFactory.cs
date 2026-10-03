#pragma warning disable CS1591
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
    public static RemoteClient Create(string family) =>
        Create(family, streaming: false, GeneratedRemotePolicy.Defaults.FromEnvironment(), null);

    public static RemoteClient Create(
        string family,
        bool streaming,
        GeneratedRemotePolicy.Defaults defaults,
        GeneratedRemotePolicy.Transport? overrideTransport)
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

        var channel = GrpcChannel.ForAddress(defaults.Endpoint);
        var headers = new Metadata();
        if (requiresBearer)
        {
            var token = GeneratedRemotePolicy.ValidateBearer(defaults.BearerToken);
            headers.Add("authorization", $"Bearer {token}");
        }
        return new RemoteClient(channel, transport, headers);
    }
}
