package dev.acyclic.transport;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

/** Thin JVM plumbing over the Rust-emitted policy snapshot. */
public final class RemoteClientFactory {
  private RemoteClientFactory() {}

  public record Client(
      ManagedChannel managedChannel,
      Channel channel,
      GeneratedRemotePolicy.Transport transport,
      Metadata headers) implements AutoCloseable {
    @Override
    public void close() {
      managedChannel.shutdownNow();
    }
  }

  public static Client create(String family) {
    return create(family, false, GeneratedRemotePolicy.Defaults.fromEnvironment(), null);
  }

  public static Client create(
      String family,
      boolean streaming,
      GeneratedRemotePolicy.Defaults defaults,
      GeneratedRemotePolicy.Transport override) {
    var requiresBearer = GeneratedRemotePolicy.requiresBearer(
        family, GeneratedRemotePolicy.Runtime.NATIVE);
    var transport = GeneratedRemotePolicy.select(
        family,
        GeneratedRemotePolicy.Runtime.NATIVE,
        streaming,
        requiresBearer,
        GeneratedRemotePolicy.Availability.GRPC_ONLY,
        GeneratedRemotePolicy.Availability.ALL,
        override);
    if (transport != GeneratedRemotePolicy.Transport.GRPC) {
      throw new UnsupportedOperationException("the installed JVM package has no " + transport + " adapter");
    }
    var managed = channelBuilder(defaults.endpoint()).build();
    var headers = new Metadata();
    if (requiresBearer) {
      var token = GeneratedRemotePolicy.validateBearer(defaults.bearerToken());
      headers.put(
          Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
          "Bearer " + token);
    }
    var intercepted = ClientInterceptors.intercept(managed, MetadataUtils.newAttachHeadersInterceptor(headers));
    return new Client(managed, intercepted, transport, headers);
  }

  private static ManagedChannelBuilder<?> channelBuilder(String endpoint) {
    var target = endpoint.replaceFirst("^https?://", "");
    var builder = ManagedChannelBuilder.forTarget(target);
    if (endpoint.startsWith("http://")) builder.usePlaintext();
    return builder;
  }
}
