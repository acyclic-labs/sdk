package adapter

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import uniffi.acyclic_actors_uniffi.ActorId
import uniffi.acyclic_actors_uniffi.ActorObservation
import uniffi.acyclic_actors_uniffi.ActorsClient
import uniffi.acyclic_actors_uniffi.AddSubscriptionRequest
import uniffi.acyclic_actors_uniffi.CancellationHandle
import uniffi.acyclic_actors_uniffi.CheckpointActorRequest
import uniffi.acyclic_actors_uniffi.CreateActorRequest
import uniffi.acyclic_actors_uniffi.InspectActorRequest
import uniffi.acyclic_actors_uniffi.InvokeActorRequest
import uniffi.acyclic_actors_uniffi.InvokeActorResponse
import uniffi.acyclic_actors_uniffi.RemoveSubscriptionRequest
import uniffi.acyclic_actors_uniffi.ResumeSubscriptionRequest
import uniffi.acyclic_actors_uniffi.UpdateActorRequest
import uniffi.acyclic_actors_uniffi.connectActors as generatedConnectActors
import uniffi.acyclic_actors_uniffi.connectActorsWithCa as generatedConnectActorsWithCa

/**
 * Generated package adapter for Rust async metadata carrying an optional
 * CancellationHandle. Callers never allocate or pass a handle.
 *
 * UniFFI 0.31.0 generated Kotlin has no Job.invokeOnCancellation hook, so
 * this adapter supplies only the cancellation bridge. It does not implement
 * transport, retries, request construction, validation, or response mapping.
 */
suspend fun <T> automaticRustCancellation(
    operation: suspend (CancellationHandle) -> T,
): T {
    check(currentCoroutineContext()[Job] != null) {
        "automatic Rust cancellation requires a coroutine Job"
    }
    val job = currentCoroutineContext()[Job]!!
    val handle = CancellationHandle()
    val completionLock = Any()
    var operationCompleted = false
    val registration = job.invokeOnCompletion { cause ->
        if (cause is CancellationException) {
            synchronized(completionLock) {
                if (!operationCompleted) {
                    handle.cancel()
                }
            }
        }
    }
    try {
        val result = operation(handle)
        synchronized(completionLock) {
            operationCompleted = true
        }
        registration.dispose()
        return result
    } catch (error: Throwable) {
        if (error !is CancellationException) {
            synchronized(completionLock) {
                operationCompleted = true
            }
            registration.dispose()
        }
        throw error
    }
}

suspend fun connectActors(endpoint: String, token: String): ActorsClient =
    automaticRustCancellation { handle -> generatedConnectActors(endpoint, token, handle) }

suspend fun connectActorsWithCa(
    endpoint: String,
    token: String,
    caCertificate: ByteArray?,
): ActorsClient = automaticRustCancellation { handle ->
    generatedConnectActorsWithCa(endpoint, token, caCertificate, handle)
}

suspend fun ActorsClient.addSubscription(request: AddSubscriptionRequest): ActorObservation? =
    automaticRustCancellation { handle -> addSubscription(request, handle) }

suspend fun ActorsClient.checkpointActor(request: CheckpointActorRequest): ActorObservation? =
    automaticRustCancellation { handle -> checkpointActor(request, handle) }

suspend fun ActorsClient.createActor(request: CreateActorRequest): ActorObservation? =
    automaticRustCancellation { handle -> createActor(request, handle) }

suspend fun ActorsClient.inspectActor(actorId: ActorId): ActorObservation? =
    automaticRustCancellation { handle -> inspectActor(actorId, handle) }

suspend fun ActorsClient.inspectActor(request: InspectActorRequest): ActorObservation? =
    automaticRustCancellation { handle -> inspectActorRequest(request, handle) }

suspend fun ActorsClient.invokeActor(request: InvokeActorRequest): InvokeActorResponse =
    automaticRustCancellation { handle -> invokeActor(request, handle) }

suspend fun ActorsClient.removeSubscription(request: RemoveSubscriptionRequest): ActorObservation? =
    automaticRustCancellation { handle -> removeSubscription(request, handle) }

suspend fun ActorsClient.resumeSubscription(request: ResumeSubscriptionRequest): ActorObservation? =
    automaticRustCancellation { handle -> resumeSubscription(request, handle) }

suspend fun ActorsClient.updateActor(request: UpdateActorRequest): ActorObservation? =
    automaticRustCancellation { handle -> updateActor(request, handle) }
