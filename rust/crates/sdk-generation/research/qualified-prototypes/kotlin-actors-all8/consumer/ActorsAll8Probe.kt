package probe

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import uniffi.acyclic_actors_uniffi.ActorId
import uniffi.acyclic_actors_uniffi.ActorObservation
import uniffi.acyclic_actors_uniffi.ActorState
import uniffi.acyclic_actors_uniffi.AddSubscriptionRequest
import uniffi.acyclic_actors_uniffi.ActorLimits
import uniffi.acyclic_actors_uniffi.Binding
import uniffi.acyclic_actors_uniffi.CancellationHandle
import uniffi.acyclic_actors_uniffi.CheckpointActorRequest
import uniffi.acyclic_actors_uniffi.CodeSha256
import uniffi.acyclic_actors_uniffi.CreateActorRequest
import uniffi.acyclic_actors_uniffi.Header
import uniffi.acyclic_actors_uniffi.InspectActorRequest
import uniffi.acyclic_actors_uniffi.InvokeActorRequest
import uniffi.acyclic_actors_uniffi.PositiveU64
import uniffi.acyclic_actors_uniffi.RemoveSubscriptionRequest
import uniffi.acyclic_actors_uniffi.ResumeSubscriptionRequest
import uniffi.acyclic_actors_uniffi.SubscriptionSpec
import uniffi.acyclic_actors_uniffi.SubscriptionStart
import uniffi.acyclic_actors_uniffi.UpdateActorRequest
import uniffi.acyclic_actors_uniffi.BindingException
import uniffi.acyclic_actors_uniffi.connectActorsWithCa
import uniffi.acyclic_actors_uniffi.uniffiEnsureInitialized
import java.nio.file.Files
import java.nio.file.Path

private fun requireCheck(condition: Boolean, message: String) {
    check(condition) { message }
}

private suspend inline fun expectCancelled(label: String, crossinline operation: suspend () -> Unit) {
    try {
        operation()
        error("$label unexpectedly succeeded")
    } catch (error: BindingException.Cancelled) {
        println("OBSERVED $label cancelled=$error")
    }
}

/**
 * Prototype-only adapter derived from the Rust facade's optional
 * CancellationHandle parameter. It forwards coroutine cancellation to that
 * Rust-owned handle and contains no transport or operation behavior.
 */
private suspend fun <T> withRustCancellation(
    handle: CancellationHandle,
    operation: suspend () -> T,
): T {
    val job = currentCoroutineContext()[Job] ?: error("a coroutine Job is required")
    val registration = job.invokeOnCompletion { cause ->
        if (cause is CancellationException) {
            handle.cancel()
        }
    }
    try {
        val result = operation()
        registration.dispose()
        return result
    } catch (error: Throwable) {
        if (error !is CancellationException) {
            registration.dispose()
        }
        throw error
    }
}

fun main() = runBlocking {
    uniffiEnsureInitialized()

    val actor = ActorId("actor-a")
    val digest = CodeSha256(ByteArray(32) { 1 })
    val binding = Binding("binding-a", "capability-a", "resource-a")
    val limits = ActorLimits(1000uL, 1024uL, 1024uL)
    val start = SubscriptionStart.CurrentHead(true)
    val subscription = SubscriptionSpec("sub-a", "/stream", start, true)
    val headers = listOf(Header("content-type", "application/json"))
    val operationSubscription = SubscriptionSpec("sub-op", "events/input", SubscriptionStart.Cursor(9007199254740993uL), true)

    requireCheck(actor.value() == "actor-a", "ActorId constructor failed")
    requireCheck(digest.value().size == 32, "CodeSha256 constructor failed")
    requireCheck(PositiveU64(ULong.MAX_VALUE).value() == ULong.MAX_VALUE, "PositiveU64 constructor failed")
    requireCheck(binding.name() == "binding-a", "Binding constructor failed")
    requireCheck(limits.memoryBytes() == 1024uL, "ActorLimits constructor failed")
    requireCheck(subscription.subscriptionId() == "sub-a", "SubscriptionSpec constructor failed")

    val create = CreateActorRequest(digest, "eu", listOf(binding), limits, listOf(subscription), "create-1")
    val update = UpdateActorRequest(actor, digest, listOf(binding), limits, 0uL, "update-1")
    val inspect = InspectActorRequest(actor)
    val add = AddSubscriptionRequest(actor, subscription, "add-1")
    val remove = RemoveSubscriptionRequest(actor, "sub-a", "remove-1")
    val resume = ResumeSubscriptionRequest(actor, "sub-a", "resume-1")
    val checkpoint = CheckpointActorRequest(actor, "checkpoint-a")
    val invoke = InvokeActorRequest(actor, "GET", "https://example.invalid/", byteArrayOf(1, 2, 3), headers)
    val requests = listOf(create, update, inspect, add, remove, resume, checkpoint, invoke)
    requireCheck(requests.size == 8, "all eight request constructors were not exercised")
    println("OBSERVED all8_request_constructors=${requests.size}")

    val taskHandle = CancellationHandle()
    val task = launch {
        withRustCancellation(taskHandle) {
            awaitCancellation()
        }
    }
    yield()
    task.cancelAndJoin()
    requireCheck(taskHandle.isCancelled(), "coroutine cancellation did not reach Rust handle")
    println("OBSERVED task_cancellation_forwarded=${taskHandle.isCancelled()}")

    val endpoint = System.getenv("ACTORS_ENDPOINT") ?: error("ACTORS_ENDPOINT is required")
    val token = System.getenv("ACTORS_TOKEN") ?: error("ACTORS_TOKEN is required")
    val actorId = System.getenv("ACTORS_ACTOR_ID") ?: "actor-a"
    val caPath = System.getenv("ACTORS_CA_FILE") ?: error("ACTORS_CA_FILE is required")
    val caCertificate = Files.readAllBytes(Path.of(caPath))

    val cancelledConnect = CancellationHandle()
    cancelledConnect.cancel()
    expectCancelled("connect_with_ca",) {
        connectActorsWithCa(endpoint, token, caCertificate, cancelledConnect)
    }

    val client = connectActorsWithCa(endpoint, token, caCertificate, null)
    val observation = client.inspectActor(ActorId(actorId), null)
        ?: error("InspectActor unexpectedly returned no observation")
    requireCheck(observation.actorId.value() == actorId, "unexpected observed actor id")
    requireCheck(observation.codeSha256.value().contentEquals(ByteArray(32) { 1 }), "unexpected observed digest")
    requireCheck(observation.homeRegion == "eu", "unexpected observed region")
    requireCheck(observation.state == ActorState.ACTIVE, "unexpected observed state")
    requireCheck(observation.subscriptions.isEmpty(), "unexpected observed subscriptions")
    requireCheck(observation.checkpointUnixMillis == null, "unexpected checkpoint timestamp")
    requireCheck(observation.checkpointEpoch == 9uL, "unexpected checkpoint epoch")
    requireCheck(observation.configurationRevision == 0uL, "unexpected configuration revision")
    println("OBSERVED inspect_actor=${observation.actorId.value()},state=${observation.state},epoch=${observation.checkpointEpoch}")

    fun observe(label: String, value: ActorObservation?) {
        val observed = value ?: error("$label returned no observation")
        requireCheck(observed.actorId.value() == actorId, "$label actor mismatch")
        requireCheck(observed.codeSha256.value().contentEquals(ByteArray(32) { 1 }), "$label digest mismatch")
        requireCheck(observed.homeRegion == "eu", "$label region mismatch")
        requireCheck(observed.state == ActorState.ACTIVE, "$label state mismatch")
        requireCheck(observed.subscriptions.isEmpty(), "$label subscriptions mismatch")
        requireCheck(observed.checkpointEpoch == 9uL, "$label epoch mismatch")
        requireCheck(observed.configurationRevision == 0uL, "$label revision mismatch")
        println("OBSERVED $label state=${observed.state},epoch=${observed.checkpointEpoch}")
    }

    observe("create_actor", client.createActor(create, null))
    observe("update_actor", client.updateActor(update, null))
    observe("add_subscription", client.addSubscription(AddSubscriptionRequest(actor, operationSubscription, "add-op"), null))
    observe("remove_subscription", client.removeSubscription(remove, null))
    observe("resume_subscription", client.resumeSubscription(resume, null))
    observe("checkpoint_actor", client.checkpointActor(checkpoint, null))
    val invokeResponse = client.invokeActor(invoke, null)
    requireCheck(invokeResponse.status == 201u, "invoke_actor status mismatch")
    requireCheck(invokeResponse.body.isEmpty(), "invoke_actor body mismatch")
    requireCheck(invokeResponse.headers.single().name == "location", "invoke_actor header name mismatch")
    requireCheck(invokeResponse.headers.single().value == "/result", "invoke_actor header value mismatch")
    println("OBSERVED invoke_actor status=${invokeResponse.status},headers=${invokeResponse.headers.size}")

    val deniedClient = connectActorsWithCa(endpoint, "wrong-token", caCertificate, null)
    try {
        deniedClient.inspectActor(ActorId(actorId), null)
        error("service error unexpectedly succeeded")
    } catch (error: BindingException.Service) {
        requireCheck(error.grpcCode != 0, "service error lost grpc code")
        println("OBSERVED service_error grpcCode=${error.grpcCode},serviceCode=${error.serviceCode},detailMessage=${error.detailMessage}")
    }

    val cancelledInspect = CancellationHandle()
    cancelledInspect.cancel()
    expectCancelled("inspect_actor") {
        client.inspectActor(ActorId(actorId), cancelledInspect)
    }

    println("KOTLIN_ALL8_IMMUTABLE_PROBE_PASS")
}