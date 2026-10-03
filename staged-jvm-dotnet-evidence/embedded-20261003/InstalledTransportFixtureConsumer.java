import acyclic.actors.v1.Actors;
import acyclic.actors.v1.ActorsServiceGrpc;
import acyclic.stream.v2.Stream;
import acyclic.stream.v2.StreamServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Consumer compiled only against the staged JVM generated transport package. */
public final class InstalledTransportFixtureConsumer {
  public static void main(String[] args) throws Exception {
    String endpoint = args.length == 0 ? "127.0.0.1:54506" : args[0];
    ManagedChannel channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
    try {
      ActorsServiceGrpc.newBlockingStub(channel).createActor(Actors.CreateActorRequest.newBuilder()
          .setHomeRegion("fixture").setIdempotencyKey("installed-jvm")
          .setCodeSha256(ByteString.copyFrom(new byte[] {
              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
              1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }))
          .setLimits(Actors.ActorLimits.newBuilder()
              .setHandlerTimeoutMillis(1_000).setMemoryBytes(1_048_576).setCheckpointBytes(4_096))
          .addSubscriptions(Actors.SubscriptionSpec.newBuilder()
              .setSubscriptionId("installed-jvm-sub").setStreamPath("fixture/recovery-jvm")
              .setStart(Actors.SubscriptionStart.newBuilder().setCursor(0)))
          .build());
      var stream = StreamServiceGrpc.newBlockingStub(channel);
      ByteString key = ByteString.copyFromUtf8("installed-jvm-recovery");
      Stream.AppendRequest first = Stream.AppendRequest.newBuilder().setPath("fixture/recovery-jvm")
          .setIfTail(0).setIdempotencyKey(key).addRecords(ByteString.copyFromUtf8("first")).build();
      Stream.AppendResponse committed = stream.append(first);
      if (!committed.hasCommitted() || !stream.append(first).equals(committed)) throw new AssertionError("replay");
      var observation = stream.inspectIdempotency(Stream.InspectIdempotencyRequest.newBuilder()
          .setIdempotencyKey(key).build());
      if (!observation.hasObservation() || !observation.getObservation().hasAppend()) throw new AssertionError("inspect");
      try {
        stream.append(first.toBuilder().clearRecords().addRecords(ByteString.copyFromUtf8("different")).build());
        throw new AssertionError("mismatch accepted");
      } catch (io.grpc.StatusRuntimeException error) {
        if (error.getStatus().getCode() != Status.Code.FAILED_PRECONDITION
            || !"idempotency_mismatch".equals(error.getStatus().getDescription())) throw error;
      }
      cancelRead(channel, "fixture/recovery-jvm", false);
      cancelRead(channel, "fixture/recovery-jvm", true);
      Stream.AppendResponse second = stream.append(first.toBuilder().clearRecords()
          .addRecords(ByteString.copyFromUtf8("second")).setIfTail(1)
          .setIdempotencyKey(ByteString.copyFromUtf8("installed-jvm-recovery-2")).build());
      if (!second.hasCommitted()) throw new AssertionError("resume append");
      var resumed = stream.read(Stream.ReadRequest.newBuilder().setPath("fixture/recovery-jvm")
          .setFrom(1).setLimit(1).build());
      if (!resumed.hasNext() || resumed.next().getRecord().getSequence() != 1) throw new AssertionError("resume read");
      System.out.println("installed JVM transport fixture checks passed: Actors/Stream replay/mismatch/cancel/recovery");
    } finally { channel.shutdownNow(); }
  }

  private static void cancelRead(ManagedChannel channel, String path, boolean follow) throws Exception {
    CountDownLatch first = new CountDownLatch(1), terminal = new CountDownLatch(1);
    AtomicReference<ClientCallStreamObserver<?>> call = new AtomicReference<>();
    if (follow) {
      ClientResponseObserver<Stream.FollowRequest, Stream.ReadResponse> observer = observer(call, first, terminal);
      StreamServiceGrpc.newStub(channel).follow(Stream.FollowRequest.newBuilder().setPath(path).setFrom(0).build(), observer);
    } else {
      ClientResponseObserver<Stream.ReadRequest, Stream.ReadResponse> observer = observer(call, first, terminal);
      StreamServiceGrpc.newStub(channel).read(Stream.ReadRequest.newBuilder().setPath(path).setFrom(0).setLimit(16).build(), observer);
    }
    if (!first.await(5, TimeUnit.SECONDS)) throw new AssertionError("stream first record");
    call.get().cancel("installed fixture cancellation", null);
    if (!terminal.await(5, TimeUnit.SECONDS)) throw new AssertionError("stream cancellation terminal");
  }

  private static <T> ClientResponseObserver<T, Stream.ReadResponse> observer(
      AtomicReference<ClientCallStreamObserver<?>> call, CountDownLatch first, CountDownLatch terminal) {
    return new ClientResponseObserver<>() {
      public void beforeStart(ClientCallStreamObserver<T> value) { call.set(value); }
      public void onNext(Stream.ReadResponse value) { first.countDown(); }
      public void onError(Throwable error) { terminal.countDown(); }
      public void onCompleted() { terminal.countDown(); }
    };
  }
}
