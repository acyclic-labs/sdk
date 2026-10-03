package dev.acyclic.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class RemoteClientFactoryTest {
  @Test
  void rustPolicyChoosesGrpcByDefaultAndFactoryCarriesBearerMetadata() {
    var defaults = new GeneratedRemotePolicy.Defaults("http://127.0.0.1:1", "test-token");
    try (var client = RemoteClientFactory.create("actors", false, defaults, null)) {
      assertEquals(GeneratedRemotePolicy.Transport.GRPC, client.transport());
      assertEquals(
          "Bearer test-token",
          client.headers().get(
              io.grpc.Metadata.Key.of("authorization", io.grpc.Metadata.ASCII_STRING_MARSHALLER)));
    }
  }

  @Test
  void invalidBearerAndUnavailableOverrideFailBeforeAnyCall() {
    var defaults = new GeneratedRemotePolicy.Defaults("http://127.0.0.1:1", "bad\ntoken");
    assertThrows(
        IllegalArgumentException.class,
        () -> RemoteClientFactory.create("actors", false, defaults, null));
    var valid = new GeneratedRemotePolicy.Defaults("http://127.0.0.1:1", "test-token");
    assertThrows(
        IllegalArgumentException.class,
        () -> RemoteClientFactory.create(
            "actors", false, valid, GeneratedRemotePolicy.Transport.HTTP_JSON));
  }
}
