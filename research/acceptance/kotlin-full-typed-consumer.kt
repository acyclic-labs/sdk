package dev.acyclic.consumer

import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Rust schema-v2 consumer. The manifest is the executable Rust fixture output:
 * no per-language method inventory or request payload is authored here.
 */
class KotlinRustTypedManifestConsumerV2Test {
  private data class Method(
    val family: String,
    val rpc: String,
    val pkg: String,
    val service: String,
    val name: String,
    val requestType: String,
    val clientStreaming: Boolean,
    val serverStreaming: Boolean,
    val requestBytes: ByteArray,
  )

  private data class Frame(val sequence: Int, val bytesBase64: String, val sha256: String)

  private class Capture {
    val responseFrames = mutableListOf<Frame>()
    var terminalCode: String? = null
    val done = CountDownLatch(1)

    fun record(value: Any?) {
      if (value == null) return
      val bytes = value.javaClass.getMethod("toByteArray").invoke(value) as ByteArray
      val digest = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
      responseFrames += Frame(responseFrames.size, Base64.getEncoder().encodeToString(bytes), "sha256:$digest")
    }
  }

  private fun field(chunk: String, name: String): String = Regex("(?s)\\\"$name\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"").find(chunk)?.groupValues?.get(1)
    ?: error("manifest record field missing: $name")

  @Test
  fun generatedStubsExerciseRustSchemaV2All106Methods() {
    val endpoint = System.getenv("ACYCLIC_FIXTURE_ENDPOINT")?.takeIf { it.isNotBlank() } ?: return
    val manifestPath = System.getenv("ACYCLIC_TYPED_REQUEST_MANIFEST")?.takeIf { it.isNotBlank() }
      ?: error("ACYCLIC_TYPED_REQUEST_MANIFEST is required")
    val manifest = Files.readString(Path.of(manifestPath))
    val presentContext = inference.customer.v1.Inference.ContextView.newBuilder()
      .setParent(ByteString.copyFromUtf8("parent")).build()
    assertTrue(presentContext.hasParent(), "optional Rust presence must remain observable in Kotlin")
    val createdContext = inference.customer.v1.Inference.ContextProvenance.newBuilder()
      .setCreated(inference.customer.v1.Inference.Empty.getDefaultInstance()).build()
    assertEquals(
      inference.customer.v1.Inference.ContextProvenance.OriginCase.CREATED,
      createdContext.originCase,
      "Rust oneof must remain a typed Kotlin-visible Java case",
    )
    assertTrue(Regex(""""schema_version"\s*:\s*2""").containsMatchIn(manifest), "manifest schema v2 missing")
    val sourceSha = Regex(""""source_git_sha"\s*:\s*"([0-9a-f]{40})"""").find(manifest)?.groupValues?.get(1)
      ?: System.getenv("ACYCLIC_SOURCE_GIT_REVISION")?.takeIf { it.matches(Regex("[0-9a-f]{40}")) }
      ?: error("manifest/source revision missing")
    System.getenv("ACYCLIC_SOURCE_GIT_REVISION")?.takeIf { it.isNotBlank() }?.let { assertEquals(it, sourceSha) }
    val count = Regex(""""record_count"\s*:\s*(\d+)""").find(manifest)?.groupValues?.get(1)?.toInt()
      ?: error("record_count missing")
    val allowPartial = System.getenv("ACYCLIC_ALLOW_PARTIAL_MANIFEST") == "1"
    val expectedCount = System.getenv("ACYCLIC_EXPECTED_RECORD_COUNT")?.toIntOrNull() ?: 106
    assertEquals(expectedCount, count)
    if (!allowPartial) {
      assertEquals(106, count)
      assertTrue(Regex(""""complete"\s*:\s*true""").containsMatchIn(manifest), "Rust fixture manifest is incomplete")
      assertTrue(Regex(""""missing_rpcs"\s*:\s*\[\s*\]""").containsMatchIn(manifest), "Rust fixture manifest has missing RPCs")
    }
    val methods = manifest.split(Regex("(?m)^    \\{")).drop(1).take(count).map { chunk ->
      val rpc = field(chunk, "rpc")
      val slash = rpc.lastIndexOf('/')
      require(slash > 0) { "invalid Rust RPC identity: $rpc" }
      val serviceRpc = rpc.substring(0, slash)
      val serviceDot = serviceRpc.lastIndexOf('.')
      require(serviceDot > 0) { "invalid Rust service identity: $rpc" }
      val pkg = serviceRpc.substring(0, serviceDot)
      val service = serviceRpc.substring(serviceDot + 1)
      val name = rpc.substring(slash + 1)
      val requestType = field(chunk, "request_type").removePrefix(".")
      Method(
        family = field(chunk, "family"), rpc = rpc, pkg = pkg, service = service, name = name,
        requestType = requestType, clientStreaming = false, serverStreaming = false,
        requestBytes = Base64.getDecoder().decode(field(chunk, "request_base64")),
      )
    }.toList()
    assertEquals(count, methods.size)
    assertEquals(count, methods.map { it.rpc }.toSet().size)

    // Streaming shape is read from the Rust record when present. v2 records
    // carry this as a wire field in newer emitters; absent fields are unary.
    val channel = ManagedChannelBuilder.forTarget(endpoint.removePrefix("http://").removePrefix("https://"))
      .usePlaintext().build()
    var attempted = 0
    var statusErrors = 0
    val observations = mutableListOf<String>()
    try {
      methods.forEach { method ->
        val request = generatedRequest(method)
        val encoded = request.javaClass.getMethod("toByteArray").invoke(request) as ByteArray
        assertTrue(encoded.contentEquals(method.requestBytes), "${method.rpc}: Rust request bytes changed")
        val requestDigest = MessageDigest.getInstance("SHA-256").digest(encoded).joinToString("") { "%02x".format(it) }
        var capture = Capture()
        try {
          capture = invokeGeneratedStub(channel, method, request)
        } catch (error: Throwable) {
          val unwrapped = unwrap(error)
          if (unwrapped is StatusRuntimeException) {
            statusErrors++
            capture.terminalCode = unwrapped.status.code.name
          } else throw error
        } finally {
          val terminal = capture.terminalCode ?: "OK"
          observations += observationJson(method.rpc, encoded, requestDigest, capture.responseFrames, terminal)
          attempted++
        }
      }
    } finally {
      channel.shutdownNow()
      channel.awaitTermination(5, TimeUnit.SECONDS)
    }
    assertEquals(methods.size, attempted)
    System.getenv("ACYCLIC_JVM_TYPED_EVIDENCE")?.takeIf { it.isNotBlank() }?.let { output ->
      Files.writeString(Path.of(output), "{\"schema\":\"acyclic.jvm.typed-manifest-live.v2\",\"source_git_sha\":\"$sourceSha\",\"method_count\":$count,\"attempted\":$attempted,\"status_errors\":$statusErrors}\n")
    }
    System.getenv("ACYCLIC_JVM_TYPED_OBSERVATION_FILE")?.takeIf { it.isNotBlank() }?.let { output ->
      Files.writeString(Path.of(output), "{\"schema\":\"acyclic.kotlin.typed-runtime-observation.v1\",\"source_git_sha\":\"$sourceSha\",\"record_count\":$count,\"attempted\":$attempted,\"observations\":[${observations.joinToString(",")}] }\n")
    }
    println("Kotlin schema-v2 generated-stub ${if (allowPartial) "partial reachability" else "reachability"}: $attempted/$count methods from $sourceSha; status observations $statusErrors")
  }

  private fun generatedRequest(method: Method): Any {
    val simple = method.requestType.substringAfterLast('.')
    val requestPkg = method.requestType.substringBeforeLast('.')
    val outer = if (requestPkg.startsWith("inference.")) "Inference" else
      requestPkg.split('.').filterNot { it.matches(Regex("v\\d+")) }.last().replaceFirstChar { it.uppercase() }
    val candidates = mutableListOf("${requestPkg}.${outer}\$$simple")
    if (method.service == "HarnessService" && method.name == "Handshake") candidates += "acyclic.protocol.v1.Protocol\$HandshakeRequest"
    val clazz = candidates.asSequence().mapNotNull { name -> try { Class.forName(name) } catch (_: ClassNotFoundException) { null } }.firstOrNull()
      ?: error("missing generated request type ${method.requestType}")
    return clazz.getMethod("parseFrom", ByteArray::class.java).invoke(null, method.requestBytes)
  }

  private fun invokeGeneratedStub(channel: ManagedChannel, method: Method, request: Any): Capture {
    val capture = Capture()
    val grpc = Class.forName("${method.pkg}.${method.service}Grpc")
    val javaName = method.name.replaceFirstChar { it.lowercase() }
    val javaNames = listOf(javaName, "${javaName}_")
    val normalized = method.name.replace(Regex("[^A-Za-z0-9]"), "").lowercase()
    val descriptorGetter = grpc.methods.firstOrNull { it.name.startsWith("get") && it.name.endsWith("Method") && it.parameterCount == 0 && it.name.removePrefix("get").removeSuffix("Method").replace(Regex("[^A-Za-z0-9]"), "").lowercase() == normalized }
    val descriptor = descriptorGetter?.invoke(null) as? io.grpc.MethodDescriptor<*, *>
    val clientStreaming = descriptor?.type == io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING || descriptor?.type == io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING
    val serverStreaming = descriptor?.type == io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING || descriptor?.type == io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING
    if (!clientStreaming && !serverStreaming) {
      val factory = grpc.methods.firstOrNull { it.name == "newBlockingStub" && it.parameterCount == 1 }
        ?: error("missing generated newBlockingStub for ${method.rpc}")
      val stub = factory.invoke(null, channel)
      val call = stub.javaClass.methods.firstOrNull {
        it.name in javaNames && it.parameterCount == 1 && it.parameterTypes[0].isAssignableFrom(request.javaClass)
      } ?: error("missing generated typed method $javaName for ${method.rpc}")
      capture.record(call.invoke(stub, request))
      capture.terminalCode = "OK"
      capture.done.countDown()
      return capture
    }
    val factory = grpc.methods.firstOrNull { it.name == "newStub" && it.parameterCount == 1 }
      ?: error("missing generated newStub for ${method.rpc}")
    val stub = factory.invoke(null, channel)
    val callback = Proxy.newProxyInstance(StreamObserver::class.java.classLoader, arrayOf(StreamObserver::class.java)) { _, callbackMethod, args ->
      when (callbackMethod?.name) {
        "onNext" -> capture.record(args?.getOrNull(0))
        "onError" -> {
          val error = args?.getOrNull(0)
          capture.terminalCode = if (error is StatusRuntimeException) error.status.code.name else "UNKNOWN"
          capture.done.countDown()
        }
        "onCompleted" -> {
          capture.terminalCode = "OK"
          capture.done.countDown()
        }
      }
      null
    } as StreamObserver<Any>
    val call = stub.javaClass.methods.firstOrNull { it.name in javaNames }
      ?: error("missing generated streaming method $javaName for ${method.rpc}")
    if (serverStreaming && !clientStreaming) call.invoke(stub, request, callback)
    else {
      val requests = call.invoke(stub, callback) as StreamObserver<Any>
      requests.onNext(request)
      requests.onCompleted()
    }
    if (!capture.done.await(5, TimeUnit.SECONDS)) capture.terminalCode = "DEADLINE_EXCEEDED"
    return capture
  }

  private fun observationJson(rpc: String, request: ByteArray, requestSha: String, responseFrames: List<Frame>, terminalCode: String): String {
    val requestBase64 = Base64.getEncoder().encodeToString(request)
    val frames = responseFrames.joinToString(",") { "{\"sequence\":${it.sequence},\"bytes_base64\":\"${it.bytesBase64}\",\"sha256\":\"${it.sha256}\"}" }
    return "{\"rpc\":\"${jsonEscape(rpc)}\",\"request_frames\":[{\"sequence\":0,\"bytes_base64\":\"$requestBase64\",\"sha256\":\"sha256:$requestSha\"}],\"response_frames\":[$frames],\"terminal_status\":{\"code\":\"$terminalCode\"}}"
  }

  private fun jsonEscape(value: String): String = value.replace("\\", "\\\\").replace("\"", "\\\"")

  private fun unwrap(error: Throwable): Throwable = if (error is InvocationTargetException) unwrap(error.targetException) else error
}

