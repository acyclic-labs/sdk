package uniffi.acyclic_actors_uniffi

import adapter.connectActorsWithCa
import adapter.inspectActor
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import uniffi.acyclic_actors_uniffi.ActorId
import java.net.HttpURLConnection
import java.net.URL
import java.nio.file.Files
import java.nio.file.Path
import kotlin.math.max

private fun state(controlEndpoint: String): String =
    URL("$controlEndpoint/state").openStream().bufferedReader().use { it.readText() }

private fun counter(json: String, key: String): Int =
    Regex("\\\"$key\\\":(\\d+)").find(json)?.groupValues?.get(1)?.toInt()
        ?: error("missing $key in $json")

private suspend fun waitForState(controlEndpoint: String, expectedStarted: Int, expectedAborted: Int): String {
    var observed = ""
    withTimeout<Unit>(10_000) {
        while (true) {
            observed = state(controlEndpoint)
            if (counter(observed, "started") >= expectedStarted &&
                counter(observed, "aborted") >= expectedAborted &&
                counter(observed, "active") == 1) break
            delay(20)
        }
    }
    return observed
}

private suspend fun waitForMapSize(expected: Int) {
    withTimeout<Unit>(5_000) {
        while (uniffiContinuationHandleMap.size != expected) delay(20)
    }
}

private fun release(controlEndpoint: String) {
    val connection = URL("$controlEndpoint/release").openConnection() as HttpURLConnection
    connection.requestMethod = "POST"
    connection.doOutput = true
    connection.outputStream.use { }
    connection.inputStream.use { it.readBytes() }
    connection.disconnect()
}

fun main() = runBlocking {
    uniffiEnsureInitialized()
    val endpoint = System.getenv("ACTORS_PENDING_ENDPOINT") ?: error("ACTORS_PENDING_ENDPOINT is required")
    val token = System.getenv("ACTORS_PENDING_TOKEN") ?: "conformance"
    val caPath = System.getenv("ACTORS_PENDING_CA_FILE") ?: error("ACTORS_PENDING_CA_FILE is required")
    val controlEndpoint = System.getenv("ACTORS_PENDING_CONTROL") ?: error("ACTORS_PENDING_CONTROL is required")
    val client = connectActorsWithCa(endpoint, token, Files.readAllBytes(Path.of(caPath)))
    val baseline = uniffiContinuationHandleMap.size
    println("OBSERVED continuation_map baseline=$baseline")
    check(baseline == 0) { "unexpected pre-existing continuation handles: $baseline" }
    try {
        var current = state(controlEndpoint)
        var started = counter(current, "started")
        var aborted = counter(current, "aborted")
        repeat(3) { attempt ->
            val before = uniffiContinuationHandleMap.size
            val pending = launch {
                try {
                    client.inspectActor(ActorId("pending-kotlin-map-$attempt"))
                    error("pending inspect unexpectedly completed")
                } catch (_: CancellationException) {
                    println("OBSERVED continuation_map coroutine_cancelled attempt=${attempt + 1}")
                }
            }
            current = waitForState(controlEndpoint, started + 1, aborted)
            started = max(started + 1, counter(current, "started"))
            val during = uniffiContinuationHandleMap.size
            println("OBSERVED continuation_map during=$during attempt=${attempt + 1} state=$current")
            check(during > before) { "continuation was not registered: before=$before during=$during" }
            pending.cancelAndJoin()
            withTimeout<Unit>(10_000) {
                while (true) {
                    current = state(controlEndpoint)
                    if (counter(current, "aborted") >= aborted + 1 && counter(current, "active") == 0) break
                    delay(20)
                }
            }
            aborted = counter(current, "aborted")
            waitForMapSize(before)
            val after = uniffiContinuationHandleMap.size
            println("OBSERVED continuation_map after=$after attempt=${attempt + 1} state=$current")
            check(after == before) { "continuation map leak: before=$before after=$after" }
        }
    } finally {
        runCatching { release(controlEndpoint) }
        client.close()
    }
    println("KOTLIN_NATIVE_PENDING_CONTINUATION_MAP_PASS")
}
