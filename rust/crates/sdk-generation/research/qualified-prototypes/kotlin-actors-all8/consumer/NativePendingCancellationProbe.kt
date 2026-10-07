package probe

import adapter.connectActorsWithCa
import adapter.inspectActor
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import uniffi.acyclic_actors_uniffi.ActorId
import uniffi.acyclic_actors_uniffi.uniffiEnsureInitialized
import java.net.HttpURLConnection
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path

private fun state(controlEndpoint: String): String =
    URL("$controlEndpoint/state").openStream().bufferedReader().use { it.readText() }

private fun release(controlEndpoint: String) {
    val connection = URL("$controlEndpoint/release").openConnection() as HttpURLConnection
    connection.requestMethod = "POST"
    connection.doOutput = true
    connection.outputStream.use { }
    connection.inputStream.use { it.readBytes() }
    connection.disconnect()
}

private suspend fun waitForState(controlEndpoint: String, vararg fragments: String): String {
    var observed = ""
    withTimeout<Unit>(10_000) {
        while (true) {
            observed = state(controlEndpoint)
            if (fragments.all(observed::contains)) break
            delay(20)
        }
    }
    return observed
}

fun main() = runBlocking {
    uniffiEnsureInitialized()
    val endpoint = System.getenv("ACTORS_PENDING_ENDPOINT") ?: error("ACTORS_PENDING_ENDPOINT is required")
    val token = System.getenv("ACTORS_PENDING_TOKEN") ?: "conformance"
    val caPath = System.getenv("ACTORS_PENDING_CA_FILE") ?: error("ACTORS_PENDING_CA_FILE is required")
    val controlEndpoint = System.getenv("ACTORS_PENDING_CONTROL") ?: error("ACTORS_PENDING_CONTROL is required")
    val client = connectActorsWithCa(endpoint, token, Files.readAllBytes(Path.of(caPath)))
    try {
        val pending = launch {
            try {
                client.inspectActor(ActorId("pending-kotlin"))
                error("pending inspect unexpectedly completed")
            } catch (_: CancellationException) {
                println("OBSERVED native_pending coroutine_cancelled=true")
            }
        }
        val started = waitForState(controlEndpoint, "\"started\":2", "\"active\":1")
        println("OBSERVED native_pending request_started=true state=$started")
        pending.cancelAndJoin()
        val aborted = waitForState(controlEndpoint, "\"aborted\":1", "\"active\":0")
        println("OBSERVED native_pending abort_observed=true state=$aborted")
    } finally {
        runCatching { release(controlEndpoint) }
        client.close()
    }
    println("KOTLIN_NATIVE_PENDING_CANCELLATION_PASS")
}