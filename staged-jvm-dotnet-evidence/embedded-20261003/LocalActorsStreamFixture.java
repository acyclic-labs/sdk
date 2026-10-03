import acyclic.actors.v1.Actors;
import acyclic.actors.v1.ActorsServiceGrpc;
import acyclic.stream.v2.Stream;
import acyclic.stream.v2.StreamServiceGrpc;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Contained loopback fixture for installed JVM/.NET Actors and Stream consumers. */
public final class LocalActorsStreamFixture {
  private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();
  private static final Map<String, Long> TAILS = new ConcurrentHashMap<>();

  public static void main(String[] args) throws Exception {
    Server server = ServerBuilder.forPort(0)
        .addService(new ActorsService())
        .addService(new StreamService())
        .build().start();
    System.out.println("FIXTURE_ENDPOINT=127.0.0.1:" + server.getPort());
    System.out.flush();
    Runtime.getRuntime().addShutdownHook(new Thread(server::shutdownNow));
    server.awaitTermination();
  }

  private static final class ActorsService extends ActorsServiceGrpc.ActorsServiceImplBase {
    @Override public void createActor(Actors.CreateActorRequest request,
        StreamObserver<Actors.CreateActorResponse> response) {
      response.onNext(Actors.CreateActorResponse.newBuilder().build());
      response.onCompleted();
    }
  }

  private static final class StreamService extends StreamServiceGrpc.StreamServiceImplBase {
    @Override public void append(Stream.AppendRequest request,
        StreamObserver<Stream.AppendResponse> response) {
      String key = request.getIdempotencyKey().toStringUtf8();
      String path = request.getPath();
      byte[] digest = request.toByteArray();
      Entry previous = ENTRIES.get(key);
      if (previous != null) {
        if (!Arrays.equals(previous.request, digest)) {
          response.onError(Status.FAILED_PRECONDITION.withDescription("idempotency_mismatch")
              .asRuntimeException());
        } else {
          response.onNext(previous.response);
          response.onCompleted();
        }
        return;
      }
      long start = TAILS.getOrDefault(path, 0L);
      long end = start + request.getRecordsCount();
      Stream.AppendResponse receipt = Stream.AppendResponse.newBuilder()
          .setCommitted(Stream.AppendReceipt.newBuilder().setStart(start).setEnd(end).setTail(end)
              .setCommitId(ByteString.copyFromUtf8("fixture-commit-" + end)))
          .build();
      TAILS.put(path, end);
      ENTRIES.put(key, new Entry(digest, receipt));
      response.onNext(receipt);
      response.onCompleted();
    }

    @Override public void inspectIdempotency(Stream.InspectIdempotencyRequest request,
        StreamObserver<Stream.InspectIdempotencyResponse> response) {
      Entry entry = ENTRIES.get(request.getIdempotencyKey().toStringUtf8());
      Stream.InspectIdempotencyResponse.Builder result = Stream.InspectIdempotencyResponse.newBuilder();
      if (entry != null) {
        result.setObservation(Stream.IdempotencyObservation.newBuilder()
            .setIdempotencyKey(request.getIdempotencyKey()).setAppend(entry.response));
      }
      response.onNext(result.build());
      response.onCompleted();
    }

    @Override public void read(Stream.ReadRequest request, StreamObserver<Stream.ReadResponse> response) {
      stream(request.getPath(), request.getFrom(), request.getLimit(), response);
    }

    @Override public void follow(Stream.FollowRequest request, StreamObserver<Stream.ReadResponse> response) {
      stream(request.getPath(), request.getFrom(), 16, response);
    }

    private void stream(String path, long from, long limit, StreamObserver<Stream.ReadResponse> response) {
      ServerCallStreamObserver<Stream.ReadResponse> server = (ServerCallStreamObserver<Stream.ReadResponse>) response;
      long count = Math.max(1L, Math.min(limit == 0 ? 16 : limit, 16));
      long tail = TAILS.getOrDefault(path, 0L);
      long end = Math.max(tail, from + 1);
      for (long sequence = from; sequence < end && sequence < from + count; sequence++) {
        if (server.isCancelled()) return;
        response.onNext(Stream.ReadResponse.newBuilder().setRecord(Stream.Record.newBuilder()
            .setSequence(sequence).setValue(ByteString.copyFromUtf8("fixture-record-" + sequence))).build());
      }
      response.onCompleted();
    }
  }

  private record Entry(byte[] request, Stream.AppendResponse response) {}
}
