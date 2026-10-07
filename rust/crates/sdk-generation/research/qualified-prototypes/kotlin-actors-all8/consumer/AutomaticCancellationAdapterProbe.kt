package probe

import adapter.automaticRustCancellation
import adapter.connectActorsWithCa
import adapter.inspectActor
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import uniffi.acyclic_actors_uniffi.ActorId
import uniffi.acyclic_actors_uniffi.ActorState
import uniffi.acyclic_actors_uniffi.CancellationHandle
import uniffi.acyclic_actors_uniffi.uniffiEnsureInitialized
import java.nio.file.Files
import java.nio.file.Path

private fun requireCheck(condition: Boolean, message: String) {
    check(condition) { message }
}

fun main() = runBlocking {
    uniffiEnsureInitialized()

    var normalHandle: CancellationHandle? = null
    val normal = automaticRustCancellation { handle ->
        normalHandle = handle
        "normal-completion"
    }
    requireCheck(normal == "normal-completion", "normal operation result mismatch")
    requireCheck(normalHandle?.isCancelled() == false, "normal completion was cancelled")
    println("OBSERVED adapter_normal_completion_cancelled=${normalHandle?.isCancelled()}")

    val cancellationHandleReady = CompletableDeferred<CancellationHandle>()
    val cancellationJob = launch {
        try {
            automaticRustCancellation { handle ->
                cancellationHandleReady.complete(handle)
                awaitCancellation()
            }
        } catch (_: CancellationException) {
            // Expected: the coroutine itself is cancelled after forwarding.
        }
    }
    val cancelledHandle = cancellationHandleReady.await()
    cancellationJob.cancelAndJoin()
    requireCheck(cancelledHandle.isCancelled(), "coroutine cancellation did not reach Rust handle")
    println("OBSERVED adapter_cancellation_forwarded=${cancelledHandle.isCancelled()}")

    var normalWins = 0
    var cancellationWins = 0
    repeat(200) { iteration ->
        val operationReady = CompletableDeferred<CancellationHandle>()
        val permit = CompletableDeferred<Unit>()
        val operationJob = launch {
            try {
                automaticRustCancellation { handle ->
                    operationReady.complete(handle)
                    permit.await()
                    "race-completion"
                }
            } catch (_: CancellationException) {
                // One legal race outcome: cancellation wins before permit release.
            }
        }
        val raceHandle = operationReady.await()
        if (iteration % 2 == 0) {
            permit.complete(Unit)
            operationJob.join()
        } else {
            val canceller = launch { operationJob.cancel() }
            permit.complete(Unit)
            canceller.join()
            operationJob.join()
        }
        if (operationJob.isCancelled) {
            cancellationWins += 1
            requireCheck(raceHandle.isCancelled(), "cancelled race lost Rust cancellation")
        } else {
            normalWins += 1
            requireCheck(!raceHandle.isCancelled(), "completed race was cancelled after completion")
        }
    }
    requireCheck(normalWins > 0, "race never observed normal completion")
    requireCheck(cancellationWins > 0, "race never observed cancellation")
    println("OBSERVED adapter_race normal=$normalWins cancelled=$cancellationWins")

    val endpoint = System.getenv("ACTORS_ENDPOINT") ?: error("ACTORS_ENDPOINT is required")
    val token = System.getenv("ACTORS_TOKEN") ?: error("ACTORS_TOKEN is required")
    val actorId = System.getenv("ACTORS_ACTOR_ID") ?: "actor-a"
    val caPath = System.getenv("ACTORS_CA_FILE") ?: error("ACTORS_CA_FILE is required")
    val caCertificate = Files.readAllBytes(Path.of(caPath))
    val client = connectActorsWithCa(endpoint, token, caCertificate)
    try {
        val observation = client.inspectActor(ActorId(actorId))
            ?: error("adapter inspect returned no observation")
        requireCheck(observation.actorId.value() == actorId, "adapter actor id mismatch")
        requireCheck(observation.state == ActorState.ACTIVE, "adapter state mismatch")
        println("OBSERVED adapter_live_inspect=${observation.actorId.value()},state=${observation.state},epoch=${observation.checkpointEpoch}")
    } finally {
        client.close()
    }

    println("KOTLIN_AUTOMATIC_CANCELLATION_ADAPTER_PASS")
}
