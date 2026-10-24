import acyclic.actors.v1.Actors;
final class InvalidActorBytes {
  void invalid() { Actors.DeploymentRequest.newBuilder().setCodeSha256("not bytes"); }
}
