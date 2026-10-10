import acyclic.stream.v1.Stream;
final class InvalidOptionalInteger {
  void invalid() { Stream.AppendRequest.newBuilder().setIfTail("not an integer"); }
}
