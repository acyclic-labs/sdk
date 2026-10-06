package dev.acyclic.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import acyclic.actors.v1.Actors;
import acyclic.harness.v2.Harness;
import acyclic.stream.v2.Stream;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class GeneratedTransportTest {
  @Test
  void preservesUint64AndOneofValues() throws Exception {
    var request = Actors.CreateActorRequest.newBuilder()
        .setLimits(Actors.ActorLimits.newBuilder().setMemoryBytes(Long.MAX_VALUE))
        .addSubscriptions(Actors.SubscriptionSpec.newBuilder()
            .setStart(Actors.SubscriptionStart.newBuilder().setCursor(Long.MAX_VALUE)))
        .build();
    var decoded = Actors.CreateActorRequest.parseFrom(request.toByteArray());
    assertEquals(Long.MAX_VALUE, decoded.getLimits().getMemoryBytes());
    assertTrue(decoded.getSubscriptions(0).getStart().hasCursor());
    assertEquals(Long.MAX_VALUE, decoded.getSubscriptions(0).getStart().getCursor());
  }

  @Test
  void preservesRustPresenceAndOneofAsNativeJavaTypes() {
    var view = inference.customer.v1.Inference.ContextView.newBuilder()
        .setParent(ByteString.copyFromUtf8("parent"))
        .build();
    assertTrue(view.hasParent(), "optional Rust presence must remain observable in Java");
    assertEquals(ByteString.copyFromUtf8("parent"), view.getParent());

    var provenance = inference.customer.v1.Inference.ContextProvenance.newBuilder()
        .setCreated(inference.customer.v1.Inference.Empty.getDefaultInstance())
        .build();
    assertEquals(
        inference.customer.v1.Inference.ContextProvenance.OriginCase.CREATED,
        provenance.getOriginCase(),
        "Rust oneof must remain a typed Java case, not an untyped map");
  }

  @Test
  void adaptsHarnessDescriptorAccessorWithoutChangingWireIdentity() throws Exception {
    var descriptor = Harness.FileDescriptor.newBuilder()
        .setSha256(ByteString.copyFrom(new byte[] {1, 2, 3}))
        .setByteLength(3)
        .setMediaType("application/octet-stream")
        .build();
    var file = Harness.FileRef.newBuilder()
        .setFileDescriptor(descriptor)
        .setDisplayName("fixture")
        .build();
    var field = file.getDescriptorForType().findFieldByName("descriptor");
    assertNotNull(field);
    assertEquals(4, field.getNumber());
    assertEquals("descriptor", field.getName());
    assertEquals("descriptor", field.getJsonName());
    assertEquals(field, Harness.FileRef.getDescriptor().findFieldByName("descriptor"));
    assertEquals(descriptor, file.getFileDescriptor());
    assertEquals(descriptor, file.getAllFields().get(field));
    var parsed = Harness.FileRef.parseFrom(file.toByteArray());
    assertEquals(descriptor, parsed.getFileDescriptor());
    var reflected = Harness.FileRef.newBuilder().setField(field, descriptor).build();
    assertEquals(descriptor, reflected.getFileDescriptor());
  }

  @Test
  void roundTripsRustGoldenBytesForEveryAuthorityFamily() throws Exception {
    String fixture;
    try (var input = getClass().getResourceAsStream("/golden/cross-language-family-fixtures.json")) {
      assertNotNull(input);
      fixture = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
    Pattern entry = Pattern.compile(
        "\\{\\s*\\\"family\\\":\\s*\\\"([^\\\"]+)\\\".*?"
            + "\\\"bytes_b64\\\":\\s*\\\"([^\\\"]*)\\\".*?"
            + "\\\"sha256\\\":\\s*\\\"([^\\\"]+)\\\"\\s*\\}",
        Pattern.DOTALL);
    Matcher matcher = entry.matcher(fixture);
    int count = 0;
    while (matcher.find()) {
      String family = matcher.group(1);
      byte[] bytes = Base64.getDecoder().decode(matcher.group(2));
      assertEquals(matcher.group(3), sha256(bytes), family + " golden hash");
      byte[] roundTrip = switch (family) {
        case "actors" -> Actors.CreateActorRequest.parseFrom(bytes).toByteArray();
        case "stream" -> Stream.AppendRequest.parseFrom(bytes).toByteArray();
        case "objects" -> acyclic.objects.v2.Objects.PutObjectHeader.parseFrom(bytes).toByteArray();
        case "workers" -> acyclic.workers.v1.Workers.PublishVersionRequest.parseFrom(bytes).toByteArray();
        case "filesystem" -> acyclic.filesystem.v2.Filesystem.HandshakeRequest.parseFrom(bytes).toByteArray();
        case "harness" -> Harness.CommandEnvelope.parseFrom(bytes).toByteArray();
        case "machines" -> acyclic.machines.v1.Machines.CreateMachineRequest.parseFrom(bytes).toByteArray();
        case "inference" -> inference.customer.v1.Inference.ListModelsRequest.parseFrom(bytes).toByteArray();
        case "protocol" -> acyclic.protocol.v1.Protocol.HandshakeRequest.parseFrom(bytes).toByteArray();
        default -> throw new AssertionError("unknown golden family: " + family);
      };
      assertArrayEquals(bytes, roundTrip, family + " protobuf bytes");
      count++;
    }
    assertEquals(9, count, "one golden entry per authority family");
  }

  private static String sha256(byte[] bytes) throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
    StringBuilder output = new StringBuilder(64);
    for (byte value : digest) output.append(String.format("%02x", value));
    return output.toString();
  }

  @Test
  void cancelsGeneratedServerStream() throws Exception {
    String name = InProcessServerBuilder.generateName();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch cancelled = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicBoolean observed = new AtomicBoolean();
    Server server = InProcessServerBuilder.forName(name).directExecutor()
        .addService(new acyclic.stream.v2.StreamServiceGrpc.StreamServiceImplBase() {
          @Override
          public void read(Stream.ReadRequest request, StreamObserver<Stream.ReadResponse> response) {
            var serverObserver = (ServerCallStreamObserver<Stream.ReadResponse>) response;
            serverObserver.setOnCancelHandler(cancelled::countDown);
            started.countDown();
            response.onNext(Stream.ReadResponse.newBuilder()
                .setRecord(Stream.Record.newBuilder().setSequence(0)).build());
            new Thread(() -> {
              try {
                release.await(5, TimeUnit.SECONDS);
                for (long sequence = 1; sequence < 10_000 && !serverObserver.isCancelled(); sequence++) {
                  response.onNext(Stream.ReadResponse.newBuilder()
                      .setRecord(Stream.Record.newBuilder().setSequence(sequence)).build());
                }
                if (!serverObserver.isCancelled()) response.onCompleted();
              } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                response.onError(error);
              }
            }).start();
          }
        }).build().start();
    ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    AtomicReference<ClientCallStreamObserver<Stream.ReadRequest>> call = new AtomicReference<>();
    CountDownLatch done = new CountDownLatch(1);
    ClientResponseObserver<Stream.ReadRequest, Stream.ReadResponse> observer = new ClientResponseObserver<>() {
      @Override public void beforeStart(ClientCallStreamObserver<Stream.ReadRequest> stream) { call.set(stream); }
      @Override public void onNext(Stream.ReadResponse value) { observed.set(true); }
      @Override public void onError(Throwable error) { done.countDown(); }
      @Override public void onCompleted() { done.countDown(); }
    };
    acyclic.stream.v2.StreamServiceGrpc.newStub(channel).read(
        Stream.ReadRequest.newBuilder().setFrom(Long.MAX_VALUE).build(), observer);
    assertTrue(started.await(5, TimeUnit.SECONDS));
    assertNotNull(call.get());
    call.get().cancel("consumer cancellation", null);
    assertTrue(cancelled.await(5, TimeUnit.SECONDS));
    release.countDown();
    assertTrue(done.await(5, TimeUnit.SECONDS));
    assertTrue(observed.get());
    channel.shutdownNow();
    server.shutdownNow();
  }

  @Test
  void canExerciseRustFixtureWhenConfigured() throws Exception {
    String endpoint = System.getenv("ACYCLIC_FIXTURE_ENDPOINT");
    if (endpoint == null || endpoint.isBlank()) {
      return;
    }

    String target = endpoint.replaceFirst("^https?://", "");
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    try {
      var request = Actors.CreateActorRequest.newBuilder()
          .setCodeSha256(ByteString.copyFrom(new byte[] {
              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
          }))
          .setHomeRegion("fixture")
          .setIdempotencyKey("jvm-fixture")
          .setLimits(Actors.ActorLimits.newBuilder()
              .setHandlerTimeoutMillis(1_000)
              .setMemoryBytes(Long.MAX_VALUE)
              .setCheckpointBytes(4_096))
          .addSubscriptions(Actors.SubscriptionSpec.newBuilder()
              .setSubscriptionId("fixture")
              .setStreamPath("fixture/events")
              .setStart(Actors.SubscriptionStart.newBuilder().setCursor(0)))
          .build();
      var actor = acyclic.actors.v1.ActorsServiceGrpc.newBlockingStub(channel).createActor(request);
      assertNotNull(actor);

      var streamBlocking = acyclic.stream.v2.StreamServiceGrpc.newBlockingStub(channel);
      var recoveryKey = ByteString.copyFromUtf8("jvm-recovery-key");
      var firstAppend = Stream.AppendRequest.newBuilder()
          .setPath("fixture/recovery")
          .addRecords(ByteString.copyFromUtf8("first"))
          .setIfTail(0)
          .setIdempotencyKey(recoveryKey)
          .build();
      var committed = streamBlocking.append(firstAppend);
      assertTrue(committed.hasCommitted());
      assertEquals(committed, streamBlocking.append(firstAppend));
      var observed = streamBlocking.inspectIdempotency(
          Stream.InspectIdempotencyRequest.newBuilder().setIdempotencyKey(recoveryKey).build());
      assertTrue(observed.hasObservation());
      assertTrue(observed.getObservation().hasAppend());
      assertEquals(committed, observed.getObservation().getAppend());
      var mismatchedAppend = firstAppend.toBuilder()
          .clearRecords()
          .addRecords(ByteString.copyFromUtf8("different"))
          .build();
      try {
        streamBlocking.append(mismatchedAppend);
        throw new AssertionError("fixture accepted an idempotency-key mismatch");
      } catch (io.grpc.StatusRuntimeException error) {
        assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, error.getStatus().getCode());
        assertEquals("idempotency_mismatch", error.getStatus().getDescription());
      }

      CountDownLatch firstRecord = new CountDownLatch(1);
      CountDownLatch terminal = new CountDownLatch(1);
      AtomicReference<ClientCallStreamObserver<Stream.ReadRequest>> call = new AtomicReference<>();
      var observer = new ClientResponseObserver<Stream.ReadRequest, Stream.ReadResponse>() {
        @Override public void beforeStart(ClientCallStreamObserver<Stream.ReadRequest> stream) {
          call.set(stream);
        }
        @Override public void onNext(Stream.ReadResponse value) { firstRecord.countDown(); }
        @Override public void onError(Throwable error) { terminal.countDown(); }
        @Override public void onCompleted() { terminal.countDown(); }
      };
      acyclic.stream.v2.StreamServiceGrpc.newStub(channel).read(
          Stream.ReadRequest.newBuilder().setPath("fixture/recovery").setLimit(16).build(), observer);
      assertTrue(firstRecord.await(5, TimeUnit.SECONDS));
      assertNotNull(call.get());
      call.get().cancel("fixture cancellation", null);
      assertTrue(terminal.await(5, TimeUnit.SECONDS));

      CountDownLatch followFirst = new CountDownLatch(1);
      CountDownLatch followTerminal = new CountDownLatch(1);
      AtomicReference<ClientCallStreamObserver<Stream.FollowRequest>> followCall = new AtomicReference<>();
      var followObserver = new ClientResponseObserver<Stream.FollowRequest, Stream.ReadResponse>() {
        @Override public void beforeStart(ClientCallStreamObserver<Stream.FollowRequest> stream) {
          followCall.set(stream);
        }
        @Override public void onNext(Stream.ReadResponse value) { followFirst.countDown(); }
        @Override public void onError(Throwable error) { followTerminal.countDown(); }
        @Override public void onCompleted() { followTerminal.countDown(); }
      };
      acyclic.stream.v2.StreamServiceGrpc.newStub(channel).follow(
          Stream.FollowRequest.newBuilder().setPath("fixture/recovery").setFrom(0).build(), followObserver);
      assertTrue(followFirst.await(5, TimeUnit.SECONDS));
      assertNotNull(followCall.get());
      followCall.get().cancel("fixture follow cancellation", null);
      assertTrue(followTerminal.await(5, TimeUnit.SECONDS));

      var secondAppend = firstAppend.toBuilder()
          .clearRecords()
          .addRecords(ByteString.copyFromUtf8("second"))
          .setIfTail(1)
          .setIdempotencyKey(ByteString.copyFromUtf8("jvm-recovery-key-2"))
          .build();
      var second = streamBlocking.append(secondAppend);
      assertTrue(second.hasCommitted());
      var resumed = streamBlocking.read(Stream.ReadRequest.newBuilder()
          .setPath("fixture/recovery").setFrom(1).setLimit(1).build());
      assertTrue(resumed.hasNext());
      assertEquals(1, resumed.next().getRecord().getSequence());
      System.out.println("Rust fixture idempotency replay, mismatch, follow cancellation, and resume checks passed.");
    } finally {
      channel.shutdownNow();
    }
  }
}
