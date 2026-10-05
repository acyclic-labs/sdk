package dev.acyclic.consumer

import acyclic.actors.v1.Actors
import acyclic.actors.v1.ActorsServiceGrpcKt
import acyclic.actors.v1.ActorsServiceGrpc
import acyclic.stream.v2.Stream
import acyclic.stream.v2.StreamServiceGrpcKt
import acyclic.stream.v2.StreamServiceGrpc
import acyclic.filesystem.v2.Filesystem
import acyclic.harness.v2.Harness
import acyclic.machines.v1.Machines
import acyclic.objects.v2.Objects
import acyclic.protocol.v1.Protocol
import acyclic.workers.v1.Workers
import dev.acyclic.transport.RustTypedClientsKotlin
import dev.acyclic.transport.RustTypedRequestsKotlin
import inference.customer.v1.Inference
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.ManagedChannelBuilder
import io.grpc.Status
import io.grpc.StatusException
import com.google.protobuf.ByteString
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.assertContentEquals
import java.security.MessageDigest
import java.util.Base64
import java.nio.file.Files
import java.nio.file.Path

class KotlinTransportConsumerTest {
  @Test
  fun installedJarRoundTripsRustGoldenAllNineFamilies() {
    val text = javaClass.getResourceAsStream("/golden/cross-language-family-fixtures.json")!!
      .bufferedReader().readText()
    val entries = Regex("""\{\s*"family"\s*:\s*"([^"]+)".*?"bytes_b64"\s*:\s*"([^"]*)".*?"sha256"\s*:\s*"([^"]+)"\s*\}""", RegexOption.DOT_MATCHES_ALL)
      .findAll(text).map { it.groupValues }.toList()
    assertEquals(9, entries.size)
    entries.forEach { fields ->
      val family = fields[1]
      val bytes = Base64.getDecoder().decode(fields[2])
      val hash = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
      assertEquals(fields[3], hash, family)
      val roundTripped = when (family) {
        "actors" -> Actors.CreateActorRequest.parseFrom(bytes).toByteArray()
        "stream" -> Stream.AppendRequest.parseFrom(bytes).toByteArray()
        "objects" -> Objects.PutObjectHeader.parseFrom(bytes).toByteArray()
        "workers" -> Workers.PublishVersionRequest.parseFrom(bytes).toByteArray()
        "filesystem" -> Filesystem.HandshakeRequest.parseFrom(bytes).toByteArray()
        "harness" -> Harness.CommandEnvelope.parseFrom(bytes).toByteArray()
        "machines" -> Machines.CreateMachineRequest.parseFrom(bytes).toByteArray()
        "inference" -> Inference.ListModelsRequest.parseFrom(bytes).toByteArray()
        "protocol" -> Protocol.HandshakeRequest.parseFrom(bytes).toByteArray()
        else -> error("Unexpected family $family")
      }
      assertContentEquals(bytes, roundTripped, family)
    }
  }

  @Test
  fun installedJarExposesUnaryAndServerStreamingCoroutineStubs() {
    val create = Actors.CreateActorRequest.newBuilder()
      .setLimits(Actors.ActorLimits.newBuilder().setMemoryBytes(Long.MAX_VALUE))
      .build()
    assertEquals(Long.MAX_VALUE, create.limits.memoryBytes)

    val channel = InProcessChannelBuilder.forName("kotlin-consumer").directExecutor().build()
    val actors = ActorsServiceGrpcKt.ActorsServiceCoroutineStub(channel)
    val stream = StreamServiceGrpcKt.StreamServiceCoroutineStub(channel)
    assertTrue(actors.javaClass.name.contains("ActorsServiceGrpcKt"))
    val records: Flow<Stream.ReadResponse> = stream.read(
      Stream.ReadRequest.newBuilder().setFrom(Long.MAX_VALUE).build()
    )
    assertTrue(records.javaClass.name.isNotEmpty())
    channel.shutdownNow()
  }

  @Test
  fun installedJarPreservesPresenceOneofAndGrpcErrorStatus() = runBlocking {
    val ifTail = Stream.AppendRequest.getDescriptor().findFieldByName("if_tail")
    val idempotencyKey = Stream.AppendRequest.getDescriptor().findFieldByName("idempotency_key")
    assertTrue(ifTail.hasPresence())
    assertTrue(idempotencyKey.hasPresence())

    val start = Actors.SubscriptionStart.newBuilder()
      .setCursor(Long.MAX_VALUE)
      .build()
    assertEquals(Actors.SubscriptionStart.StartCase.CURSOR, start.startCase)
    assertEquals(Long.MAX_VALUE, start.cursor)

    val name = "kotlin-error-status"
    val service = object : StreamServiceGrpcKt.StreamServiceCoroutineImplBase() {
      override suspend fun append(request: Stream.AppendRequest): Stream.AppendResponse {
        throw StatusException(Status.FAILED_PRECONDITION.withDescription("idempotency_mismatch"))
      }
    }
    val server = InProcessServerBuilder.forName(name).directExecutor()
      .addService(service.bindService()).build().start()
    val channel = InProcessChannelBuilder.forName(name).directExecutor().build()
    try {
      val error = assertFailsWith<StatusException> {
        StreamServiceGrpcKt.StreamServiceCoroutineStub(channel).append(
          Stream.AppendRequest.newBuilder().setPath("presence").build()
        )
      }
      assertEquals(Status.Code.FAILED_PRECONDITION, error.status.code)
      assertEquals("idempotency_mismatch", error.status.description)
    } finally {
      channel.shutdownNow()
      server.shutdownNow()
    }
  }

  @Test
  fun installedJarExercisesInProcessStreamRecoveryAndCancellation() = runBlocking {
    val name = "kotlin-stream-recovery"
    var tail = 0L
    val service = object : StreamServiceGrpcKt.StreamServiceCoroutineImplBase() {
      override suspend fun append(request: Stream.AppendRequest): Stream.AppendResponse {
        if (request.hasIfTail() && request.ifTail != tail) {
          return Stream.AppendResponse.newBuilder()
            .setConflict(Stream.TailConflict.newBuilder().setActualTail(tail))
            .build()
        }
        val start = tail
        tail += request.recordsCount
        return Stream.AppendResponse.newBuilder()
          .setCommitted(Stream.AppendReceipt.newBuilder().setStart(start).setEnd(tail - 1).setTail(tail))
          .build()
      }

      override fun read(request: Stream.ReadRequest): Flow<Stream.ReadResponse> = flow {
        emit(Stream.ReadResponse.newBuilder()
          .setRecord(Stream.Record.newBuilder().setSequence(request.from).build())
          .build())
        awaitCancellation()
      }
    }
    val server = InProcessServerBuilder.forName(name).directExecutor()
      .addService(service.bindService()).build().start()
    val channel = InProcessChannelBuilder.forName(name).directExecutor().build()
    try {
      val stream = StreamServiceGrpcKt.StreamServiceCoroutineStub(channel)
      val first = stream.append(Stream.AppendRequest.newBuilder().setPath("recover")
        .addRecords(com.google.protobuf.ByteString.copyFromUtf8("first")).build())
      assertTrue(first.hasCommitted())
      assertEquals(1L, first.committed.tail)

      val second = stream.append(Stream.AppendRequest.newBuilder().setPath("recover").setIfTail(1)
        .addRecords(com.google.protobuf.ByteString.copyFromUtf8("second")).build())
      assertTrue(second.hasCommitted())
      assertEquals(2L, second.committed.tail)

      val received = CompletableDeferred<Long>()
      val job = launch {
        stream.read(Stream.ReadRequest.newBuilder().setPath("recover").setFrom(1).build())
          .collect { received.complete(it.record.sequence) }
      }
      assertEquals(1L, received.await())
      job.cancelAndJoin()
      assertTrue(job.isCancelled)
    } finally {
      channel.shutdownNow()
      server.shutdownNow()
    }
  }

  @Test
  fun installedJarExercisesRustFixtureUnaryStreamAndCancellation() = runBlocking {
    val endpoint = System.getenv("ACYCLIC_FIXTURE_ENDPOINT")
    val fixtureRootValue = System.getenv("ACYCLIC_RUST_FIXTURE_ROOT")
    if (endpoint.isNullOrBlank() || fixtureRootValue.isNullOrBlank()) return@runBlocking
    val fixtureRoot = Path.of(fixtureRootValue).let { path ->
      if (Files.isRegularFile(path)) path.parent else path
    }
    fun requestBytes(relative: String): ByteArray = Files.readAllBytes(fixtureRoot.resolve(relative))
    val target = endpoint.removePrefix("http://").removePrefix("https://")
    val channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build()
    try {
      val actorBytes = requestBytes("fixtures/actors-create-unary-v1/request.bin")
      val request = Actors.CreateActorRequest.parseFrom(actorBytes)
      assertContentEquals(actorBytes, request.toByteArray(), "Rust actor request changed during typed decode")
      val actors = ActorsServiceGrpcKt.ActorsServiceCoroutineStub(channel)
      val actorResponse = actors.createActor(request)
      assertTrue(actorResponse.hasActor(), "Rust fixture returned no actor")
      assertContentEquals(request.codeSha256.toByteArray(), actorResponse.actor.codeSha256.toByteArray())

      val stream = StreamServiceGrpcKt.StreamServiceCoroutineStub(channel)
      val appendBytes = requestBytes("fixtures/stream-append-read-v2/append-request.bin")
      val appendRequest = Stream.AppendRequest.parseFrom(appendBytes)
      assertContentEquals(appendBytes, appendRequest.toByteArray(), "Rust append request changed during typed decode")
      val readBytes = requestBytes("fixtures/stream-append-read-v2/read-request.bin")
      val readRequest = Stream.ReadRequest.parseFrom(readBytes)
      assertContentEquals(readBytes, readRequest.toByteArray(), "Rust read request changed during typed decode")
      val appended = stream.append(appendRequest)
      val replayed = stream.append(appendRequest)
      assertEquals(appended, replayed, "Rust stream idempotency replay mismatch")
      var received = false
      val first = CompletableDeferred<Unit>()
      val job = launch {
        stream.read(readRequest)
          .collect { received = true; first.complete(Unit) }
      }
      first.await()
      job.cancelAndJoin()
      assertTrue(received, "Rust fixture did not produce a streamed record")
    } finally {
      channel.shutdownNow()
    }
  }

  @Test
  fun installedJarExercisesRustTypedFacadeAgainstFixture() = runBlocking {
    val endpoint = System.getenv("ACYCLIC_FIXTURE_ENDPOINT")
    if (endpoint.isNullOrBlank()) return@runBlocking
    val target = endpoint.removePrefix("http://").removePrefix("https://")
    val channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build()
    try {
      val actors = ActorsServiceGrpc.newBlockingStub(channel)
      val actorRequest = RustTypedRequestsKotlin.ActorsActorsCreateActorRequest(
        Actors.CreateActorRequest.newBuilder()
          .setHomeRegion("fixture")
          .setIdempotencyKey("kotlin-typed-facade-create")
          .setCodeSha256(ByteString.copyFrom(ByteArray(32) { 1 }))
          .setLimits(Actors.ActorLimits.newBuilder()
            .setHandlerTimeoutMillis(1_000)
            .setMemoryBytes(1_048_576)
            .setCheckpointBytes(4_096))
          .build(),
      )
      val actorResponse = RustTypedClientsKotlin.actorsActorsCreateActor(actors, actorRequest)
      assertTrue(actorResponse.hasActor(), "typed Rust response did not expose actor presence")
      assertEquals("fixture-actor", actorResponse.actorActorId().value)
      assertEquals("fixture", actorResponse.actor().homeRegion)

      val stream = StreamServiceGrpc.newBlockingStub(channel)
      val appendWire = Stream.AppendRequest.newBuilder()
        .setPath("fixture/kotlin-typed-facade")
        .setIfTail(0)
        .setIdempotencyKey(ByteString.copyFromUtf8("kotlin-typed-facade-append"))
        .addRecords(ByteString.copyFromUtf8("typed-first"))
        .build()
      val appendRequest = RustTypedRequestsKotlin.StreamStreamAppendRequest(appendWire)
      assertEquals("fixture/kotlin-typed-facade", appendRequest.path().value)
      assertTrue(appendRequest.hasIfTail())
      assertEquals("kotlin-typed-facade-append", appendRequest.idempotencyKey().value.toStringUtf8())
      val committed = RustTypedClientsKotlin.streamStreamAppend(stream, appendRequest)
      assertTrue(committed.hasCommitted(), "typed Rust stream response did not expose committed presence")
      assertEquals(1L, committed.committed().tail)
      assertEquals(32, committed.committedCommitId().value.size(), "commit identity must be a Rust-owned digest")

      val replay = RustTypedClientsKotlin.streamStreamAppend(stream, appendRequest)
      assertEquals(committed.toWire(), replay.toWire(), "Rust typed facade changed idempotent replay")
      val mismatch = appendWire.toBuilder()
        .clearRecords().addRecords(ByteString.copyFromUtf8("typed-different"))
        .build()
      try {
        RustTypedClientsKotlin.streamStreamAppend(
          stream,
          RustTypedRequestsKotlin.StreamStreamAppendRequest(mismatch),
        )
        error("Rust fixture accepted typed idempotency mismatch")
      } catch (error: io.grpc.StatusRuntimeException) {
        assertEquals(Status.Code.FAILED_PRECONDITION, error.status.code)
      }

      val firstFollowRecord = CompletableDeferred<Unit>()
      val followJob = launch {
        StreamServiceGrpcKt.StreamServiceCoroutineStub(channel)
          .follow(Stream.FollowRequest.newBuilder()
            .setPath("fixture/kotlin-typed-facade")
            .setFrom(0)
            .build())
          .collect { firstFollowRecord.complete(Unit) }
      }
      firstFollowRecord.await()
      followJob.cancelAndJoin()
      assertTrue(followJob.isCancelled, "Rust fixture follow stream did not cancel")

      val secondWire = appendWire.toBuilder()
        .setIfTail(1)
        .clearRecords().addRecords(ByteString.copyFromUtf8("typed-second"))
        .setIdempotencyKey(ByteString.copyFromUtf8("kotlin-typed-facade-recovery"))
        .build()
      val resumed = RustTypedClientsKotlin.streamStreamAppend(
        stream,
        RustTypedRequestsKotlin.StreamStreamAppendRequest(secondWire),
      )
      assertTrue(resumed.hasCommitted(), "typed Rust recovery append was not committed")
      assertEquals(2L, resumed.committed().tail)

      val readRequest = RustTypedRequestsKotlin.StreamStreamReadRequest(
        Stream.ReadRequest.newBuilder()
          .setPath("fixture/kotlin-typed-facade")
          .setFrom(1)
          .setLimit(1)
          .build(),
      )
      val read = RustTypedClientsKotlin.streamStreamRead(stream, readRequest)
      assertTrue(read.hasNext(), "typed Rust stream response iterator was empty")
      val record = read.next()
      assertEquals(1L, record.record().sequence)
    } finally {
      channel.shutdownNow()
      channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
    }
  }
}
