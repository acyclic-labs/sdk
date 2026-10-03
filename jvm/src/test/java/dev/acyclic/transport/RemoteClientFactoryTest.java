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

  @Test
  void everyNativeFamilyUsesRustGrpcPolicyWithoutFallback() {
    var families = new String[] {
      "actors", "workers", "objects", "stream", "inference",
      "machines", "filesystem", "harness"
    };
    for (var family : families) {
      var client = RemoteClientFactory.create(
          family,
          false,
          new GeneratedRemotePolicy.Defaults("http://127.0.0.1:1", "test-token"),
          null);
      try {
        assertEquals(GeneratedRemotePolicy.Transport.GRPC, client.transport(), family);
        if (family.equals("machines")) {
          assertEquals(0, client.headers().keys().size(), "Machines uses native mTLS metadata");
        }
      } finally {
        client.close();
      }
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> GeneratedRemotePolicy.select(
            "machines",
            GeneratedRemotePolicy.Runtime.NATIVE,
            false,
            false,
            GeneratedRemotePolicy.Availability.GRPC_ONLY,
            new GeneratedRemotePolicy.Availability(false, false, true),
            null));
    var watch = GeneratedRemotePolicy.OPERATIONS
        .get("machines")
        .get("acyclic.machines.v1.MachinesService/WatchOperation");
    assertEquals(true, watch.serverStreaming());
    assertEquals("call", watch.cancellation());
    assertEquals(false, watch.bearerAuth());
  }
}
