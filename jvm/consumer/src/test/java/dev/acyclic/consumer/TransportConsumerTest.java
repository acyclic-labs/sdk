package dev.acyclic.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import acyclic.actors.v1.Actors;
import acyclic.stream.v2.Stream;
import org.junit.jupiter.api.Test;

class TransportConsumerTest {
  @Test
  void cleanConsumerCanUseGeneratedActorsAndStreamTypes() {
    var limits = Actors.ActorLimits.newBuilder().setMemoryBytes(Long.MAX_VALUE).build();
    var request = Actors.CreateActorRequest.newBuilder().setLimits(limits).build();
    assertEquals(Long.MAX_VALUE, request.getLimits().getMemoryBytes());
    var read = Stream.ReadRequest.newBuilder().setFrom(Long.MAX_VALUE).build();
    assertEquals(Long.MAX_VALUE, read.getFrom());
    assertTrue(read.isInitialized());
  }
}
