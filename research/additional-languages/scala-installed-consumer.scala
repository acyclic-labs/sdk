package acyclic.installed

import acyclic.actors.v1.actors.{ActorLimits, ActorsServiceGrpc, CreateActorRequest}
import acyclic.stream.v2.stream.{AppendRequest, StreamServiceGrpc}
import com.google.protobuf.ByteString
import io.grpc.{ClientInterceptors, ManagedChannelBuilder, Metadata}
import io.grpc.stub.MetadataUtils

/** Consumes the locally published ScalaPB artifact through an isolated Ivy resolver. */
object InstalledScalaGrpcConsumer extends App {
  val address = sys.env.getOrElse("FIXTURE_GRPC_ADDRESS", throw new IllegalStateException("FIXTURE_GRPC_ADDRESS is required"))
  val uri = new java.net.URI(address)
  val channel = ManagedChannelBuilder.forAddress(uri.getHost, uri.getPort).usePlaintext().build()
  try {
    val headers = new Metadata()
    headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer scala-installed")
    val authenticated = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers))
    val actors = ActorsServiceGrpc.blockingStub(authenticated)
    val code = Array.tabulate[Byte](32)(index => (index + 1).toByte)
    val actor = actors.createActor(CreateActorRequest(
      codeSha256 = ByteString.copyFrom(code),
      homeRegion = "eu-west",
      limits = Some(ActorLimits(handlerTimeoutMillis = 1_000L, memoryBytes = 9007199254740993L, checkpointBytes = 4_096L)),
      idempotencyKey = "scala-installed"))
    require(actor.actor.exists(_.codeSha256.toByteArray.sameElements(code)), "installed Actors bytes mismatch")
    val stream = StreamServiceGrpc.blockingStub(authenticated)
    val appended = stream.append(AppendRequest(
      path = "actors/events",
      records = Seq(ByteString.copyFromUtf8("scala-installed")),
      idempotencyKey = Some(ByteString.copyFromUtf8("scala-installed-append"))))
    val start = appended.outcome.committed.map(_.start).getOrElse(throw new IllegalStateException("installed append was not committed"))
    val records = stream.read(acyclic.stream.v2.stream.ReadRequest(path = "actors/events", from = start, limit = 8)).toVector
    require(records.size == 1 && records.head.record.exists(_.value.toStringUtf8 == "scala-installed"), "installed Stream response mismatch")
    println("scala installed artifact consumer passed: auth, bytes, uint64, stream")
  } finally {
    channel.shutdownNow()
    channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
  }
}
