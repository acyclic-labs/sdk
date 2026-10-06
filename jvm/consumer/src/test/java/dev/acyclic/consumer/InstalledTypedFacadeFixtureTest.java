package dev.acyclic.consumer;

import acyclic.actors.v1.Actors;
import acyclic.actors.v1.ActorsServiceGrpc;
import acyclic.stream.v2.Stream;
import acyclic.stream.v2.StreamServiceGrpc;
import com.google.protobuf.ByteString;
import dev.acyclic.transport.RustTypedClients;
import dev.acyclic.transport.RustTypedRequests;
import dev.acyclic.transport.RustTypedResponses;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Runs the installed Java artifact against the Rust fixture server.
 * The public calls use RustTypedRequests/RustTypedClients/RustTypedResponses;
 * raw stubs are used only for cancellation, which is a transport concern.
 */
final class InstalledTypedFacadeFixtureTest {
  @Test
  void installedJarExercisesRustTypedFacadeAgainstFixture() throws Exception {
    String endpoint = System.getenv("ACYCLIC_FIXTURE_ENDPOINT");
    if (endpoint == null || endpoint.isBlank()) {
      return;
    }
    String target = endpoint.replaceFirst("^https?://", "");
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    try {
      ActorsServiceGrpc.ActorsServiceBlockingStub actors = ActorsServiceGrpc.newBlockingStub(channel);
      RustTypedRequests.ActorsActorsCreateActorRequest actorRequest =
          new RustTypedRequests.ActorsActorsCreateActorRequest(
              Actors.CreateActorRequest.newBuilder()
                  .setHomeRegion("fixture")
                  .setIdempotencyKey("java-typed-facade-create")
                  .setCodeSha256(ByteString.copyFrom(new byte[32]))
                  .setLimits(Actors.ActorLimits.newBuilder()
                      .setHandlerTimeoutMillis(1_000)
                      .setMemoryBytes(1_048_576)
                      .setCheckpointBytes(4_096))
                  .build());
      RustTypedResponses.ActorsActorsCreateActorResponse actorResponse =
          RustTypedClients.actorsActorsCreateActor(actors, actorRequest);
      assertTrue(actorResponse.hasActor());
      assertEquals("fixture-actor", actorResponse.actorActorId().value());
      assertEquals("fixture", actorResponse.actor().homeRegion());

      StreamServiceGrpc.StreamServiceBlockingStub stream = StreamServiceGrpc.newBlockingStub(channel);
      Stream.AppendRequest appendWire = Stream.AppendRequest.newBuilder()
          .setPath("fixture/java-typed-facade")
          .setIfTail(0)
          .setIdempotencyKey(ByteString.copyFromUtf8("java-typed-facade-append"))
          .addRecords(ByteString.copyFromUtf8("typed-first"))
          .build();
      RustTypedRequests.StreamStreamAppendRequest appendRequest =
          new RustTypedRequests.StreamStreamAppendRequest(appendWire);
      assertEquals("fixture/java-typed-facade", appendRequest.path().value());
      assertTrue(appendRequest.hasIfTail());
      assertEquals("java-typed-facade-append", appendRequest.idempotencyKey().value().toStringUtf8());

      RustTypedResponses.StreamStreamAppendResponse committed =
          RustTypedClients.streamStreamAppend(stream, appendRequest);
      assertTrue(committed.hasCommitted());
      assertEquals(1L, committed.committed().tail().value());
      assertEquals(32, committed.committedCommitId().value().size());

      RustTypedResponses.StreamStreamAppendResponse replay =
          RustTypedClients.streamStreamAppend(stream, appendRequest);
      assertEquals(committed.toWire(), replay.toWire());

      Stream.AppendRequest mismatch = appendWire.toBuilder()
          .clearRecords().addRecords(ByteString.copyFromUtf8("typed-different"))
          .build();
      try {
        RustTypedClients.streamStreamAppend(
            stream, new RustTypedRequests.StreamStreamAppendRequest(mismatch));
        fail("Rust fixture accepted an idempotency mismatch");
      } catch (io.grpc.StatusRuntimeException error) {
        assertEquals(Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
      }

      cancelRead(channel, "fixture/java-typed-facade", false);
      cancelRead(channel, "fixture/java-typed-facade", true);

      Stream.AppendRequest recoveryWire = appendWire.toBuilder()
          .setIfTail(1)
          .clearRecords().addRecords(ByteString.copyFromUtf8("typed-second"))
          .setIdempotencyKey(ByteString.copyFromUtf8("java-typed-facade-recovery"))
          .build();
      RustTypedResponses.StreamStreamAppendResponse resumed =
          RustTypedClients.streamStreamAppend(
              stream, new RustTypedRequests.StreamStreamAppendRequest(recoveryWire));
      assertTrue(resumed.hasCommitted());
      assertEquals(2L, resumed.committed().tail().value());

      RustTypedRequests.StreamStreamReadRequest readRequest =
          new RustTypedRequests.StreamStreamReadRequest(Stream.ReadRequest.newBuilder()
              .setPath("fixture/java-typed-facade")
              .setFrom(1)
              .setLimit(1)
              .build());
      Iterator<RustTypedResponses.StreamStreamReadResponse> read =
          RustTypedClients.streamStreamRead(stream, readRequest);
      assertTrue(read.hasNext());
      assertEquals(1L, read.next().record().sequence());
    } finally {
      channel.shutdownNow();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static void cancelRead(ManagedChannel channel, String path, boolean follow) throws Exception {
    CountDownLatch first = new CountDownLatch(1);
    CountDownLatch terminal = new CountDownLatch(1);
    AtomicReference<ClientCallStreamObserver<?>> call = new AtomicReference<>();
    if (follow) {
      ClientResponseObserver<Stream.FollowRequest, Stream.ReadResponse> observer =
          observer(call, first, terminal);
      StreamServiceGrpc.newStub(channel).follow(
          Stream.FollowRequest.newBuilder().setPath(path).setFrom(0).build(), observer);
    } else {
      ClientResponseObserver<Stream.ReadRequest, Stream.ReadResponse> observer =
          observer(call, first, terminal);
      StreamServiceGrpc.newStub(channel).read(
          Stream.ReadRequest.newBuilder().setPath(path).setFrom(0).setLimit(16).build(), observer);
    }
    assertTrue(first.await(5, TimeUnit.SECONDS), "Rust fixture did not produce a stream record");
    ClientCallStreamObserver<?> streamCall = call.get();
    assertNotNull(streamCall);
    streamCall.cancel("installed Java fixture cancellation", null);
    assertTrue(terminal.await(5, TimeUnit.SECONDS), "stream cancellation had no terminal callback");
  }

  private static <T> ClientResponseObserver<T, Stream.ReadResponse> observer(
      AtomicReference<ClientCallStreamObserver<?>> call,
      CountDownLatch first,
      CountDownLatch terminal) {
    return new ClientResponseObserver<>() {
      @Override
      public void beforeStart(ClientCallStreamObserver<T> value) {
        call.set(value);
      }

      @Override
      public void onNext(Stream.ReadResponse value) {
        first.countDown();
      }

      @Override
      public void onError(Throwable error) {
        terminal.countDown();
      }

      @Override
      public void onCompleted() {
        terminal.countDown();
      }
    };
  }
}
