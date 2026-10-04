package acyclic.installed

import acyclic.actors.v1.actors.{ActorLimits, ActorsServiceGrpc, CreateActorRequest}
import acyclic.stream.v2.stream.{AppendRequest, StreamServiceGrpc}
import com.google.protobuf.ByteString
import io.grpc.stub.{ClientResponseObserver, StreamObserver}
import io.grpc.{ClientInterceptors, Context, ManagedChannelBuilder, Metadata}
import io.grpc.stub.MetadataUtils
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.nio.file.{Files, Path}

/** Consumes the locally published ScalaPB artifact through an isolated Ivy resolver. */
object InstalledScalaGrpcConsumer extends App {
  val address = sys.env.get("FIXTURE_GRPC_ADDRESS").orElse(sys.env.get("ACYCLIC_FIXTURE_ENDPOINT"))
    .getOrElse(throw new IllegalStateException("FIXTURE_GRPC_ADDRESS or ACYCLIC_FIXTURE_ENDPOINT is required"))
  val fixtureRootValue = sys.env.getOrElse("ACYCLIC_RUST_FIXTURE_ROOT", throw new IllegalStateException("ACYCLIC_RUST_FIXTURE_ROOT is required"))
  val fixtureRoot = {
    val path = Path.of(fixtureRootValue)
    if (Files.isRegularFile(path)) path.getParent else path
  }
  def rustRequest(relative: String): Array[Byte] = Files.readAllBytes(fixtureRoot.resolve(relative))
  val uri = new java.net.URI(address)
  val channel = ManagedChannelBuilder.forAddress(uri.getHost, uri.getPort).usePlaintext().build()
  try {
    val headers = new Metadata()
    headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer scala-installed")
    val authenticated = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers))
    val actors = ActorsServiceGrpc.blockingStub(authenticated)
    val actorBytes = rustRequest("fixtures/actors-create-unary-v1/request.bin")
    val actorRequest = CreateActorRequest.parseFrom(actorBytes)
    require(actorRequest.toByteArray.sameElements(actorBytes), "Rust actor request changed during typed decode")
    val actor = actors.createActor(actorRequest)
    require(actor.actor.exists(_.codeSha256.toByteArray.sameElements(actorRequest.codeSha256.toByteArray)), "installed Actors bytes mismatch")
    val stream = StreamServiceGrpc.blockingStub(authenticated)
    val appendBytes = rustRequest("fixtures/stream-append-read-v2/append-request.bin")
    val appendRequest = AppendRequest.parseFrom(appendBytes)
    require(appendRequest.toByteArray.sameElements(appendBytes), "Rust append request changed during typed decode")
    val readBytes = rustRequest("fixtures/stream-append-read-v2/read-request.bin")
    val readRequest = acyclic.stream.v2.stream.ReadRequest.parseFrom(readBytes)
    require(readRequest.toByteArray.sameElements(readBytes), "Rust read request changed during typed decode")
    val appended = stream.append(appendRequest)
    val replayed = stream.append(appendRequest)
    require(replayed == appended, "installed Stream idempotency replay mismatch")
    require(appended.outcome.committed.nonEmpty, "installed append was not committed")
    val records = stream.read(readRequest).toVector
    require(records.nonEmpty, "installed Stream response mismatch")

    val cancelled = new CountDownLatch(1)
    val cancellation = Context.current().withCancellation()
    val previous = cancellation.attach()
    try {
      StreamServiceGrpc.stub(authenticated).read(
        acyclic.stream.v2.stream.ReadRequest(path = "actors/events", from = Long.MaxValue, limit = 1),
        new StreamObserver[acyclic.stream.v2.stream.ReadResponse] {
          override def onNext(value: acyclic.stream.v2.stream.ReadResponse): Unit = ()
          override def onError(error: Throwable): Unit = cancelled.countDown()
          override def onCompleted(): Unit = cancelled.countDown()
        })
      cancellation.cancel(new RuntimeException("scala qualification cancellation"))
    } finally {
      cancellation.detach(previous)
    }
    require(cancelled.await(5, TimeUnit.SECONDS), "installed Stream cancellation did not complete")
    println("scala installed artifact consumer passed: auth, bytes, uint64, stream, replay, cancellation")
  } finally {
    channel.shutdownNow()
    channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
  }
}
