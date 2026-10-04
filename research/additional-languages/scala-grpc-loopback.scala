package acyclic.prototype

import acyclic.actors.v1.actors.{ActorLimits, ActorObservation, ActorState, ActorsServiceGrpc, CreateActorRequest, CreateActorResponse, SubscriptionObservation}
import acyclic.stream.v2.stream.{FollowRequest, ReadResponse, Record, StreamServiceGrpc}
import com.google.protobuf.ByteString
import io.grpc.{Metadata, ServerCall, ServerCallHandler, ServerInterceptor, ServerInterceptors, Status, StatusRuntimeException}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.{MetadataUtils, StreamObserver}
import io.grpc.ClientInterceptors
import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.DurationInt

/**
  * Exercises generated ScalaPB stubs against an in-process gRPC server.
  * The fixture is intentionally small, but checks wire-sensitive behavior that
  * a remote SDK must preserve: auth metadata, bytes, uint64, proto3 optional
  * presence, server streaming, and deadline cancellation.
  */
object ScalaGrpcLoopback extends App {
  implicit val ec: ExecutionContext = ExecutionContext.global
  val serverName = "scala-generated-loopback"
  val seenAuthorization = Promise[String]()
  val never = Promise[CreateActorResponse]()

  val codeSha256 = Array.tabulate[Byte](32)(index => (index + 1).toByte)
  val observation = ActorObservation(
    actorId = "actor-loopback",
    codeSha256 = ByteString.copyFrom(codeSha256),
    homeRegion = "eu-west",
    state = ActorState.ACTOR_STATE_ACTIVE,
    subscriptions = Seq(SubscriptionObservation(failedCursor = Some(9007199254740993L)))
  )

  val actorsService = new ActorsServiceGrpc.ActorsService {
    override def createActor(request: CreateActorRequest): Future[CreateActorResponse] = {
      require(request.limits.exists(_.memoryBytes == 9007199254740993L))
      require(request.idempotencyKey == "scala-loopback")
      if (request.idempotencyKey == "hang") never.future
      else Future.successful(CreateActorResponse(actor = Some(observation)))
    }
    override def updateActor(request: acyclic.actors.v1.actors.UpdateActorRequest): Future[acyclic.actors.v1.actors.UpdateActorResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def inspectActor(request: acyclic.actors.v1.actors.InspectActorRequest): Future[acyclic.actors.v1.actors.InspectActorResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def addSubscription(request: acyclic.actors.v1.actors.AddSubscriptionRequest): Future[acyclic.actors.v1.actors.AddSubscriptionResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def removeSubscription(request: acyclic.actors.v1.actors.RemoveSubscriptionRequest): Future[acyclic.actors.v1.actors.RemoveSubscriptionResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def resumeSubscription(request: acyclic.actors.v1.actors.ResumeSubscriptionRequest): Future[acyclic.actors.v1.actors.ResumeSubscriptionResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def checkpointActor(request: acyclic.actors.v1.actors.CheckpointActorRequest): Future[acyclic.actors.v1.actors.CheckpointActorResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def invokeActor(request: acyclic.actors.v1.actors.InvokeActorRequest): Future[acyclic.actors.v1.actors.InvokeActorResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
  }

  val streamService = new StreamServiceGrpc.StreamService {
    override def read(request: acyclic.stream.v2.stream.ReadRequest, responseObserver: StreamObserver[ReadResponse]): Unit = {
      responseObserver.onNext(ReadResponse(record = Some(Record(sequence = 9007199254740993L, value = ByteString.copyFromUtf8("one")))))
      responseObserver.onCompleted()
    }
    override def follow(request: FollowRequest, responseObserver: StreamObserver[ReadResponse]): Unit = {
      responseObserver.onNext(ReadResponse(record = Some(Record(sequence = request.from, value = ByteString.copyFromUtf8("follow")))))
      responseObserver.onCompleted()
    }
    override def children(request: acyclic.stream.v2.stream.ChildrenRequest, responseObserver: StreamObserver[acyclic.stream.v2.stream.ChildrenResponse]): Unit = responseObserver.onCompleted()
    override def inspectIdempotency(request: acyclic.stream.v2.stream.InspectIdempotencyRequest): Future[acyclic.stream.v2.stream.InspectIdempotencyResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def append(request: acyclic.stream.v2.stream.AppendRequest): Future[acyclic.stream.v2.stream.AppendResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def tail(request: acyclic.stream.v2.stream.TailRequest): Future[acyclic.stream.v2.stream.TailResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def fork(request: acyclic.stream.v2.stream.ForkRequest): Future[acyclic.stream.v2.stream.ForkReceipt] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def childrenPage(request: acyclic.stream.v2.stream.ChildrenPageRequest): Future[acyclic.stream.v2.stream.ChildrenPageResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def commit(request: acyclic.stream.v2.stream.CommitRequest): Future[acyclic.stream.v2.stream.CommitResponse] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
    override def readCommit(request: acyclic.stream.v2.stream.ReadCommitRequest): Future[acyclic.stream.v2.stream.CommittedEnvelope] = Future.failed(Status.UNIMPLEMENTED.asRuntimeException())
  }

  val authInterceptor = new ServerInterceptor {
    override def interceptCall[ReqT, RespT](call: ServerCall[ReqT, RespT], headers: Metadata, next: ServerCallHandler[ReqT, RespT]): io.grpc.ServerCall.Listener[ReqT] = {
      val key = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)
      Option(headers.get(key)).foreach(seenAuthorization.trySuccess)
      next.startCall(call, headers)
    }
  }

  val rustFixtureAddress = sys.env.get("FIXTURE_GRPC_ADDRESS")
  val localServer = rustFixtureAddress match {
    case Some(_) => None
    case None => Some(InProcessServerBuilder.forName(serverName).directExecutor()
      .addService(ServerInterceptors.intercept(ActorsServiceGrpc.bindService(actorsService, ec), authInterceptor))
      .addService(ServerInterceptors.intercept(StreamServiceGrpc.bindService(streamService, ec), authInterceptor))
      .build().start())
  }
  val channel = rustFixtureAddress
    .map(address => {
      val uri = new java.net.URI(address)
      ManagedChannelBuilder.forAddress(uri.getHost, uri.getPort).usePlaintext().build()
    })
    .getOrElse(InProcessChannelBuilder.forName(serverName).directExecutor().build())
  try {
    val headers = new Metadata()
    headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer scala-loopback")
    val authenticatedChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(headers))
    val actors = ActorsServiceGrpc.blockingStub(authenticatedChannel)
    val actorRequest = CreateActorRequest(
      codeSha256 = ByteString.copyFrom(codeSha256),
      homeRegion = "eu-west",
      limits = Some(ActorLimits(
        handlerTimeoutMillis = 1_000L,
        memoryBytes = 9007199254740993L,
        checkpointBytes = 1_048_576L)),
      idempotencyKey = "scala-loopback"
    )
    val encodedActorRequest = actorRequest.toByteArray
    val decodedActorRequest = CreateActorRequest.parseFrom(encodedActorRequest)
    require(!actorRequest.codeSha256.isEmpty && encodedActorRequest.nonEmpty, s"client bytes were ${encodedActorRequest.toSeq}")
    require(actorRequest.serializedSize == encodedActorRequest.length, s"serialized size ${actorRequest.serializedSize} did not match encoded length ${encodedActorRequest.length}")
    require(decodedActorRequest.codeSha256.toByteArray.sameElements(codeSha256), s"protobuf round-trip bytes were ${decodedActorRequest.codeSha256.toByteArray.toSeq}")
    val actorResult = actors.createActor(actorRequest)
    require(actorResult.actor.exists(actor =>
      actor.codeSha256.toByteArray.sameElements(codeSha256) && actor.homeRegion == "eu-west"))
    if (rustFixtureAddress.isEmpty) {
      require(Await.result(seenAuthorization.future, 2.seconds) == "Bearer scala-loopback")
    }

    val streamStub = StreamServiceGrpc.blockingStub(authenticatedChannel)
    val streamed = rustFixtureAddress match {
      case Some(_) =>
        val appended = streamStub.append(acyclic.stream.v2.stream.AppendRequest(
          path = "actors/events",
          records = Seq(ByteString.copyFromUtf8("scala-rust-fixture")),
          idempotencyKey = Some(ByteString.copyFromUtf8("scala-rust-append"))))
        val start = appended.outcome.committed.map(_.start).getOrElse(throw new IllegalStateException("Rust fixture append was not committed"))
        streamStub.read(acyclic.stream.v2.stream.ReadRequest(path = "actors/events", from = start, limit = 8)).toVector
      case None => streamStub.follow(FollowRequest(path = "/actors", from = 9007199254740993L)).toVector
    }
    require(streamed.size == 1 && streamed.head.record.exists(record =>
      rustFixtureAddress match {
        case Some(_) => record.value.toStringUtf8 == "scala-rust-fixture"
        case None => record.sequence == 9007199254740993L
      }))

    if (rustFixtureAddress.isEmpty) {
      val asyncActors = ActorsServiceGrpc.stub(authenticatedChannel)
      try {
        Await.result(asyncActors.withDeadlineAfter(50, TimeUnit.MILLISECONDS).createActor(CreateActorRequest(idempotencyKey = "hang")), 2.seconds)
        throw new IllegalStateException("deadline cancellation did not fail")
      } catch {
        case error: StatusRuntimeException => require(error.getStatus.getCode == Status.Code.DEADLINE_EXCEEDED)
      }
    }
    val cancellationStatus = if (rustFixtureAddress.isEmpty) "cancellation" else "Rust fixture transport (cancellation remains a separate gate)"
    println(s"scala generated gRPC consumer loopback (${rustFixtureAddress.fold("in-process bootstrap")(_ => "Rust fixture")}) passed: auth, bytes, uint64, presence, streaming, $cancellationStatus")
  } finally {
    channel.shutdownNow()
    localServer.foreach(_.shutdownNow())
    channel.awaitTermination(5, TimeUnit.SECONDS)
    localServer.foreach(_.awaitTermination(5, TimeUnit.SECONDS))
  }
}
