package dev.acyclic.consumer

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Release/manual live consumer driven only by the Rust typed request manifest. */
class KotlinRustTypedManifestConsumerTest {
  private data class Method(val family: String, val pkg: String, val service: String, val name: String,
    val requestType: String, val clientStreaming: Boolean, val serverStreaming: Boolean, val emptyHex: String)
  private val methodPattern = Regex(
    """\{\s*"family"\s*:\s*"([^"]+)".*?"package"\s*:\s*"([^"]+)".*?"service"\s*:\s*"([^"]+)".*?"method"\s*:\s*"([^"]+)".*?"request_type"\s*:\s*"([^"]+)".*?"client_streaming"\s*:\s*(true|false).*?"server_streaming"\s*:\s*(true|false).*?"typed_request"\s*:\s*\{\s*"empty_serialized_hex"\s*:\s*"([^"]*)"""",
    setOf(RegexOption.DOT_MATCHES_ALL))

  @Test fun installedGeneratedStubsExerciseRustTypedManifestAll106Methods() {
    val endpoint = System.getenv("ACYCLIC_FIXTURE_ENDPOINT")?.takeIf { it.isNotBlank() } ?: return
    val manifestPath = System.getenv("ACYCLIC_TYPED_REQUEST_MANIFEST")?.takeIf { it.isNotBlank() } ?: return
    val manifest = Files.readString(Path.of(manifestPath))
    val sourceSha = Regex(""""source_git_sha"\s*:\s*"([0-9a-f]{40})"""").find(manifest)?.groupValues?.get(1) ?: error("manifest source_git_sha missing")
    System.getenv("ACYCLIC_SOURCE_GIT_REVISION")?.takeIf { it.isNotBlank() }?.let { assertEquals(it, sourceSha) }
    val count = Regex(""""method_count"\s*:\s*(\d+)""").find(manifest)?.groupValues?.get(1)?.toInt() ?: error("manifest method_count missing")
    val methods = methodPattern.findAll(manifest).map { m -> Method(m.groupValues[1],m.groupValues[2],m.groupValues[3],m.groupValues[4],m.groupValues[5],m.groupValues[6]=="true",m.groupValues[7]=="true",m.groupValues[8]) }.toList()
    assertEquals(count, methods.size); assertEquals(106, methods.size)
    val channel = ManagedChannelBuilder.forTarget(endpoint.removePrefix("http://").removePrefix("https://")).usePlaintext().build()
    var attempted=0; var accepted=0; var semanticErrors=0
    try { methods.forEach { method ->
      val request=generatedRequest(method); val encoded=request.javaClass.getMethod("toByteArray").invoke(request) as ByteArray
      assertTrue(encoded.contentEquals(hex(method.emptyHex)), "${method.family}/${method.name}: typed bytes changed")
      try { invokeGeneratedStub(channel,method,request); accepted++ } catch(error:Throwable) { if(unwrap(error) is StatusRuntimeException) semanticErrors++ else throw error } finally { attempted++ }
    }} finally { channel.shutdownNow(); channel.awaitTermination(5,TimeUnit.SECONDS) }
    assertEquals(methods.size,attempted)
    System.getenv("ACYCLIC_JVM_TYPED_EVIDENCE")?.takeIf { it.isNotBlank() }?.let { output -> Files.writeString(Path.of(output), "{\"schema\":\"acyclic.jvm.typed-manifest-live.v1\",\"source_git_sha\":\"$sourceSha\",\"method_count\":$count,\"attempted\":$attempted,\"accepted\":$accepted,\"semantic_errors\":$semanticErrors}\n") }
    println("Kotlin generated-stub live consumer passed: $attempted/$count Rust methods attempted from $sourceSha")
  }
  private fun generatedRequest(method:Method):Any { val simple=method.requestType.substringAfterLast('.'); val outer=if(method.pkg.startsWith("inference.")) "Inference" else method.pkg.substringBefore('.').replaceFirstChar { it.uppercase() }; val clazz=Class.forName("${method.pkg}.${outer}$$simple"); return clazz.getMethod("parseFrom",ByteArray::class.java).invoke(null,hex(method.emptyHex)) }
  private fun invokeGeneratedStub(channel:ManagedChannel,method:Method,request:Any) { val grpc=Class.forName("${method.pkg}.${method.service}Grpc"); val factoryName=if(method.clientStreaming||method.serverStreaming) "newStub" else "newBlockingStub"; val factory=grpc.methods.firstOrNull { it.name==factoryName&&it.parameterCount==1 } ?: error("missing generated $factoryName"); val stub=factory.invoke(null,channel); val javaName=method.name.replaceFirstChar { it.lowercase() }; if(!method.clientStreaming) { val call=stub.javaClass.methods.firstOrNull { it.name==javaName&&it.parameterCount==1&&it.parameterTypes[0].isAssignableFrom(request.javaClass) } ?: error("missing generated typed method $javaName"); val response=call.invoke(stub,request); if(response is Iterator<*>) while(response.hasNext()) response.next(); return }; val callback=Proxy.newProxyInstance(StreamObserver::class.java.classLoader,arrayOf(StreamObserver::class.java)) { _,_,_->null } as StreamObserver<Any>; val call=stub.javaClass.methods.firstOrNull { it.name==javaName&&it.parameterCount==1&&StreamObserver::class.java.isAssignableFrom(it.parameterTypes[0]) } ?: error("missing generated streaming method $javaName"); val requests=call.invoke(stub,callback) as StreamObserver<Any>; requests.onNext(request); requests.onCompleted() }
  private fun unwrap(error:Throwable):Throwable=if(error is InvocationTargetException) unwrap(error.targetException) else error
  private fun hex(value:String):ByteArray=if(value.isEmpty()) ByteArray(0) else value.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
}
