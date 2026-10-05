package dev.acyclic.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import acyclic.actors.v1.Actors;
import acyclic.actors.v1.ActorsServiceGrpc;
import java.net.URI;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Verifies that the consumer tests execute against the installed JAR. */
final class InstalledJarConsumerTest {
  @Test
  void loadsGeneratedTypesAndServicesFromInstalledArtifact() throws Exception {
    URI location = Actors.class.getProtectionDomain().getCodeSource().getLocation().toURI();
    Path artifact = Path.of(location);
    assertTrue(artifact.getFileName().toString().endsWith("acyclic-sdk-jvm-transport-0.2.0-SNAPSHOT.jar"),
        "generated types were loaded from " + artifact);
    assertEquals(artifact, Path.of(ActorsServiceGrpc.class.getProtectionDomain().getCodeSource().getLocation().toURI()));

    var request = Actors.CreateActorRequest.newBuilder()
        .setHomeRegion("installed-consumer")
        .setIdempotencyKey("installed-consumer")
        .build();
    assertEquals(request, Actors.CreateActorRequest.parseFrom(request.toByteArray()));
  }
}
