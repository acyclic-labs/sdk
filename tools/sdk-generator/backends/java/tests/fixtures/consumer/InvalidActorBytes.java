import acyclic.actors.v1.Actors;
final class InvalidActorBytes {
  void invalid() { Actors.CreateActorRequest.newBuilder().setCodeSha256("not bytes"); }
}
