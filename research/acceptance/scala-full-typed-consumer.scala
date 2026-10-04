package acyclic.installed

import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import java.nio.file.{Files, Path}
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.TimeUnit
import scala.util.matching.Regex

/** Release/manual ScalaPB consumer. Rust manifest selects generated ScalaPB companions and stubs. */
object ScalaRustTypedManifestConsumer extends App {
  val endpoint = sys.env.getOrElse("ACYCLIC_FIXTURE_ENDPOINT", sys.error("ACYCLIC_FIXTURE_ENDPOINT is required"))
  val manifestPath = sys.env.getOrElse("ACYCLIC_TYPED_REQUEST_MANIFEST", sys.error("ACYCLIC_TYPED_REQUEST_MANIFEST is required"))
  val json = Files.readString(Path.of(manifestPath))
  val sha = """"source_git_sha"\s*:\s*"([0-9a-f]{40})""".r.findFirstMatchIn(json).map(_.group(1)).getOrElse(sys.error("manifest source_git_sha missing"))
  sys.env.get("ACYCLIC_SOURCE_GIT_REVISION").filter(_.nonEmpty).foreach(expected => require(expected == sha, "fixture and manifest revisions differ"))
  val count = """"method_count"\s*:\s*(\d+)""".r.findFirstMatchIn(json).map(_.group(1).toInt).getOrElse(sys.error("manifest method_count missing"))
  val entry: Regex = """\{\s*"family"\s*:\s*"([^"]+)".*?"package"\s*:\s*"([^"]+)".*?"service"\s*:\s*"([^"]+)".*?"method"\s*:\s*"([^"]+)".*?"request_type"\s*:\s*"([^"]+)".*?"client_streaming"\s*:\s*(true|false).*?"server_streaming"\s*:\s*(true|false).*?"typed_request"\s*:\s*\{\s*"empty_serialized_hex"\s*:\s*"([^"]*)""".r
  val methods = entry.findAllMatchIn(json).toVector
  require(methods.size == count && count == 106, s"expected 106 manifest methods, got ${methods.size}/$count")
  val channel = ManagedChannelBuilder.forTarget(endpoint.stripPrefix("http://").stripPrefix("https://")).usePlaintext().build()
  var attempted = 0
  try methods.foreach { m =>
    val pkg = m.group(2); val service = m.group(3); val name = m.group(4); val reqName = m.group(5).split('.').last
    val scalaPkg = pkg + "." + pkg.split('.').lastOption.getOrElse("")
    val companion = Class.forName(s"$scalaPkg.${reqName}$$").getField("MODULE$").get(null)
    val bytes = hex(m.group(8)); val request = companion.getClass.getMethod("parseFrom", classOf[Array[Byte]]).invoke(companion, bytes)
    val encoded = request.getClass.getMethod("toByteArray").invoke(request).asInstanceOf[Array[Byte]]
    require(java.util.Arrays.equals(bytes, encoded), s"$pkg/$name typed bytes changed")
    val grpcModule = Class.forName(s"$scalaPkg.${service}Grpc$$").getField("MODULE$").get(null)
    val stubFactory = grpcModule.getClass.getMethods.find(x => x.getName == "blockingStub" && x.getParameterCount == 1).getOrElse(sys.error(s"missing ScalaPB blockingStub for $service"))
    val stub = stubFactory.invoke(grpcModule, channel)
    val methodName = name.head.toLower + name.tail
    val call = stub.getClass.getMethods.find(x => x.getName == methodName && x.getParameterCount == 1).getOrElse(sys.error(s"missing generated ScalaPB method $methodName"))
    try call.invoke(stub, request) catch { case e: InvocationTargetException if unwrap(e) .isInstanceOf[StatusRuntimeException] => () }
    attempted += 1
  } finally { channel.shutdownNow(); channel.awaitTermination(5, TimeUnit.SECONDS) }
  require(attempted == count, s"only $attempted/$count ScalaPB calls reached transport")
  println(s"ScalaPB generated-stub live consumer passed: $attempted/$count methods from $sha")
  private def hex(value: String): Array[Byte] = if (value.isEmpty) Array.emptyByteArray else value.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
  private def unwrap(error: Throwable): Throwable = error match { case e: InvocationTargetException => unwrap(e.getTargetException); case other => other }
}
