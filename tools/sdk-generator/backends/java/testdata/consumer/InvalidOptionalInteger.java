import acyclic.stream.v2.Stream;
final class InvalidOptionalInteger {
  void invalid() { Stream.AppendRequest.newBuilder().setIfTail("not an integer"); }
}
