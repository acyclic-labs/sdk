import acyclic.workers.v1.Workers;
final class InvalidWorkerBytes {
  void invalid() { Workers.PublishVersionRequest.newBuilder().setJavascriptModule("not bytes"); }
}
