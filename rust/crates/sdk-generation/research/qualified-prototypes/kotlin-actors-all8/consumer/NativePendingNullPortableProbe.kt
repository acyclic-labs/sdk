package probe

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import uniffi.acyclic_actors.InspectActorRequest
import uniffi.acyclic_actors_uniffi.ActorsClient
import uniffi.acyclic_actors_uniffi.BindingException
import uniffi.acyclic_actors_uniffi.connectActorsWithCa
import uniffi.acyclic_actors_uniffi.uniffiEnsureInitialized

private val http = HttpClient.newHttpClient()
private fun state(control: String): Map<String, Int> {
    val request = HttpRequest.newBuilder(URI.create("$control/state")).GET().build()
    val body = http.send(request, HttpResponse.BodyHandlers.ofString()).body()
    fun field(name: String) = Regex("\\\"$name\\\":(\\d+)").find(body)?.groupValues?.get(1)?.toInt()
        ?: error("missing $name in fixture state: $body")
    return mapOf("started" to field("started"), "aborted" to field("aborted"), "active" to field("active"))
}

private suspend fun waitFor(control: String, predicate: (Map<String, Int>) -> Boolean): Map<String, Int> = withTimeout<Map<String, Int>>(10_000) {
    while (true) {
        val current = state(control)
        if (predicate(current)) return@withTimeout current
        delay(20)
    }
    error("fixture predicate loop unexpectedly exited")
}

fun main() = runBlocking {
    uniffiEnsureInitialized()
    val endpoint = System.getenv("ACTORS_PENDING_ENDPOINT") ?: error("ACTORS_PENDING_ENDPOINT is required")
    val control = System.getenv("ACTORS_PENDING_CONTROL") ?: error("ACTORS_PENDING_CONTROL is required")
    val caPath = System.getenv("ACTORS_PENDING_CA_FILE") ?: error("ACTORS_PENDING_CA_FILE is required")
    val before = state(control)
    check(before["active"] == 0) { "fixture must be exclusive: $before" }
    val client: ActorsClient = connectActorsWithCa(endpoint, "conformance", Files.readAllBytes(Path.of(caPath)), null)
    try {
        val pending = launch {
            try {
                client.inspectActor(InspectActorRequest("pending-kotlin"), null)
                error("pending inspect unexpectedly completed")
            } catch (_: CancellationException) {
                println("OBSERVED generated_null pending coroutine_cancelled=true")
            } catch (_: BindingException.Cancelled) {
                println("OBSERVED generated_null pending rust_cancelled=true")
            }
        }
        waitFor(control) { it["started"] == before["started"]!! + 1 && it["active"] == 1 }
        println("OBSERVED generated_null pending started=true")
        pending.cancel()
        pending.join()
        val after = waitFor(control) { it["aborted"] == before["aborted"]!! + 1 && it["active"] == 0 }
        val aborted = after["aborted"]
        val active = after["active"]
        println("OBSERVED generated_null pending aborted=$aborted,active=$active")
    } finally {
        client.close()
    }
    println("KOTLIN_JNA_STANDARD_RESOURCE_PENDING_CANCELLATION_PASS")
}
