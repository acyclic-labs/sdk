package dev.acyclic.consumer

import acyclic.actors.v1.Actors
import acyclic.actors.v1.ActorsServiceGrpcKt
import acyclic.stream.v2.Stream
import acyclic.stream.v2.StreamServiceGrpcKt
import acyclic.filesystem.v2.Filesystem
import acyclic.harness.v2.Harness
import acyclic.machines.v1.Machines
import acyclic.objects.v2.Objects
import acyclic.protocol.v1.Protocol
import acyclic.workers.v1.Workers
import inference.customer.v1.Inference
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.ManagedChannelBuilder
import io.grpc.Status
import io.grpc.StatusException
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
    if (endpoint.isNullOrBlank()) return@runBlocking
    val target = endpoint.removePrefix("http://").removePrefix("https://")
    val channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build()
    try {
      val request = Actors.CreateActorRequest.newBuilder()
        .setCodeSha256(com.google.protobuf.ByteString.copyFrom(ByteArray(32) { (it + 1).toByte() }))
        .setHomeRegion("fixture")
        .setIdempotencyKey("kotlin-fixture")
        .setLimits(Actors.ActorLimits.newBuilder().setMemoryBytes(Long.MAX_VALUE))
        .build()
      val actors = ActorsServiceGrpcKt.ActorsServiceCoroutineStub(channel)
      assertEquals("fixture-actor", actors.createActor(request).actor.actorId)

      val stream = StreamServiceGrpcKt.StreamServiceCoroutineStub(channel)
      var received = false
      val first = CompletableDeferred<Unit>()
      val job = launch {
        stream.read(Stream.ReadRequest.newBuilder().setPath("fixture/events").setLimit(16).build())
          .collect { received = true; first.complete(Unit) }
      }
      first.await()
      job.cancelAndJoin()
      assertTrue(received, "Rust fixture did not produce a streamed record")
    } finally {
      channel.shutdownNow()
    }
  }
}
