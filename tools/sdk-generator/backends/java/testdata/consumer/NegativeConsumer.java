import acyclic.actors.v1.Actors;
import acyclic.workers.v1.Workers;
import acyclic.stream.v2.Stream;

final class NegativeConsumer {
  void invalid() {
    Actors.CreateActorRequest.newBuilder().setCodeSha256("not bytes");
    Workers.PublishVersionRequest.newBuilder().setJavascriptModule("not bytes");
    Stream.AppendRequest.newBuilder().setIfTail("not an integer");
  }
}
