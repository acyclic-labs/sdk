package acyclic.installed

import io.grpc.{ManagedChannelBuilder, StatusRuntimeException}
import com.google.protobuf.ByteString
import dev.acyclic.transport.{RustTypedClientsScala, RustTypedRequestsScala}
import inference.customer.v1.inference.{ContextProvenance, ContextView, Empty}
import java.lang.reflect.InvocationTargetException
import java.nio.file.{Files, Path}
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/** ScalaPB schema-v2 consumer driven entirely by the Rust fixture manifest. */
object ScalaRustTypedManifestConsumerV2 extends App {
  // Compile-time ScalaPB proof: Rust optional presence is Option[T], and Rust
  // oneofs are sealed ADTs with typed cases.
  private val typedPresence: Option[ByteString] = ContextView().parent
  private val typedOneof: ContextProvenance.Origin = ContextProvenance.Origin.Created(Empty())
  private val typedCaseName = typedOneof match {
    case ContextProvenance.Origin.Created(value) => value.getClass.getSimpleName
  }
  require(typedPresence.isEmpty && typedCaseName == "Empty", "ScalaPB Rust type mapping lost presence or oneof typing")
  val endpoint = sys.env.getOrElse("ACYCLIC_FIXTURE_ENDPOINT", sys.error("ACYCLIC_FIXTURE_ENDPOINT is required"))
  val manifestPath = sys.env.getOrElse("ACYCLIC_TYPED_REQUEST_MANIFEST", sys.error("ACYCLIC_TYPED_REQUEST_MANIFEST is required"))
  val json = Files.readString(Path.of(manifestPath))
  require(""""schema_version"\s*:\s*2""".r.findFirstIn(json).nonEmpty, "manifest schema v2 missing")
  val sourceSha = """"source_git_sha"\s*:\s*"([0-9a-f]{40})""".r.findFirstMatchIn(json).map(_.group(1)).orElse(sys.env.get("ACYCLIC_SOURCE_GIT_REVISION")).getOrElse(sys.error("manifest/source revision missing"))
  sys.env.get("ACYCLIC_SOURCE_GIT_REVISION").filter(_.nonEmpty).foreach(expected => require(expected == sourceSha, "fixture and manifest revisions differ"))
  val count = """"record_count"\s*:\s*(\d+)""".r.findFirstMatchIn(json).map(_.group(1).toInt).getOrElse(sys.error("record_count missing"))
  val allowPartial = sys.env.get("ACYCLIC_ALLOW_PARTIAL_MANIFEST").contains("1")
  val expectedCount = sys.env.get("ACYCLIC_EXPECTED_RECORD_COUNT").map(_.toInt).getOrElse(106)
  require(count == expectedCount, s"expected $expectedCount Rust records, got $count")
  if (!allowPartial) {
    require(count == 106, s"expected 106 Rust records, got $count")
    require(""""complete"\s*:\s*true""".r.findFirstIn(json).nonEmpty, "Rust fixture manifest is incomplete")
    require(""""missing_rpcs"\s*:\s*\[\s*\]""".r.findFirstIn(json).nonEmpty, "Rust fixture manifest has missing RPCs")
  }
  def field(chunk: String, name: String): String = ("(?s)\"" + name + "\"\\s*:\\s*\"([^\"]*)\"").r.findFirstMatchIn(chunk).map(_.group(1)).getOrElse(sys.error(s"manifest record field missing: $name"))
  // Rust emits pretty JSON with four-space top-level record indentation. Split
  // at that boundary so nested response-frame objects cannot be mistaken for
  // records, then resolve fields independently of their serialization order.
  val methods = json.split("(?m)^    \\{").drop(1).take(count).map { chunk =>
    val family = field(chunk, "family")
    val rpc = field(chunk, "rpc"); val slash = rpc.lastIndexOf('/'); require(slash > 0, s"invalid RPC identity $rpc")
    val serviceRpc = rpc.substring(0, slash); val dot = serviceRpc.lastIndexOf('.'); require(dot > 0, s"invalid service identity $rpc")
    val pkg = serviceRpc.substring(0, dot); val service = serviceRpc.substring(dot + 1); val name = rpc.substring(slash + 1)
    val requestType = field(chunk, "request_type").stripPrefix(".")
    (family, rpc, pkg, service, name, requestType, false, false, Base64.getDecoder.decode(field(chunk, "request_base64")))
  }.toVector
  require(methods.size == count && methods.map(_._2).distinct.size == count, s"expected $count distinct Rust records, got ${methods.size}")
  val channel = ManagedChannelBuilder.forTarget(endpoint.stripPrefix("http://").stripPrefix("https://")).usePlaintext().build()
  val observations = ArrayBuffer.empty[String]
  val installedFacadeMethods = RustTypedClientsScala.getClass.getMethods
    .count(_.getName.matches("[a-z].*"))
  require(installedFacadeMethods >= 106, s"installed Scala facade exposes only $installedFacadeMethods generated methods")
  // Exercise the installed Rust-owned Scala facade directly before the broad
  // descriptor-driven reachability sweep.  The Java protobuf/gRPC classes and
  // these nominal wrappers come from the installed JVM artifact in lib/.
  val typedActors = acyclic.actors.v1.ActorsServiceGrpc.newBlockingStub(channel)
  val typedActorRequest = RustTypedRequestsScala.ActorsActorsCreateActorRequest(
    acyclic.actors.v1.Actors.CreateActorRequest.newBuilder()
      .setHomeRegion("fixture")
      .setIdempotencyKey("scala-typed-facade-create")
      .setCodeSha256(ByteString.copyFrom(Array.fill[Byte](32)(1)))
      .setLimits(acyclic.actors.v1.Actors.ActorLimits.newBuilder()
        .setHandlerTimeoutMillis(1000).setMemoryBytes(1048576).setCheckpointBytes(4096))
      .build())
  val typedActorResponse = RustTypedClientsScala.actorsActorsCreateActor(typedActors, typedActorRequest)
  require(typedActorResponse.hasActor && typedActorResponse.actorActorId.value == "fixture-actor", "Scala typed actor facade response")
  require(typedActorResponse.actor.getHomeRegion == "fixture", "Scala typed actor projection")

  val typedStream = acyclic.stream.v2.StreamServiceGrpc.newBlockingStub(channel)
  val typedAppend = RustTypedRequestsScala.StreamStreamAppendRequest(
    acyclic.stream.v2.Stream.AppendRequest.newBuilder()
      .setPath("fixture/scala-typed-facade")
      .setIfTail(0)
      .setIdempotencyKey(ByteString.copyFromUtf8("scala-typed-facade-append"))
      .addRecords(ByteString.copyFromUtf8("typed-first"))
      .build())
  require(typedAppend.path.value == "fixture/scala-typed-facade" && typedAppend.hasIfTail, "Scala typed append request")
  require(typedAppend.idempotencyKey.value.toStringUtf8 == "scala-typed-facade-append", "Scala typed idempotency key")
  val typedCommitted = RustTypedClientsScala.streamStreamAppend(typedStream, typedAppend)
  require(typedCommitted.hasCommitted && typedCommitted.committed.getTail == 1L, "Scala typed append response")
  require(typedCommitted.committedCommitId.value.size == 32, "Scala typed commit identity")
  val typedReplay = RustTypedClientsScala.streamStreamAppend(typedStream, typedAppend)
  require(typedReplay.toWire == typedCommitted.toWire, "Scala typed idempotency replay")
  val typedRead = RustTypedClientsScala.streamStreamRead(
    typedStream,
    RustTypedRequestsScala.StreamStreamReadRequest(
      acyclic.stream.v2.Stream.ReadRequest.newBuilder()
        .setPath("fixture/scala-typed-facade").setFrom(0).setLimit(1).build()))
  require(typedRead.hasNext && typedRead.next().hasRecord, "Scala typed read response")
  val typedFacadeObservation = sys.env.get("ACYCLIC_SCALA_TYPED_FACADE_OBSERVATION_FILE")
  typedFacadeObservation.foreach { path =>
    Files.writeString(Path.of(path), s"{\"schema\":\"acyclic.scala.typed-facade.v1\",\"source_git_sha\":\"$sourceSha\",\"actor\":true,\"stream\":true,\"replay\":true}\n")
  }
  def jsonEscape(value: String): String = value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
  def sha256(bytes: Array[Byte]): String = java.security.MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"$b%02x").mkString
  def b64(bytes: Array[Byte]): String = Base64.getEncoder.encodeToString(bytes)
  def recordObservation(rpc: String, request: Array[Byte], responseFrames: Seq[Array[Byte]], status: String): Unit = {
    val requestJson = s"{\"sequence\":0,\"bytes_base64\":\"${b64(request)}\",\"sha256\":\"sha256:${sha256(request)}\"}"
    val framesJson = responseFrames.zipWithIndex.map { case (bytes, index) => s"{\"sequence\":$index,\"bytes_base64\":\"${b64(bytes)}\",\"sha256\":\"sha256:${sha256(bytes)}\"}" }.mkString("[", ",", "]")
    observations += s"{\"rpc\":\"${jsonEscape(rpc)}\",\"request_frames\":[$requestJson],\"response_frames\":$framesJson,\"terminal_status\":{\"code\":\"${jsonEscape(status)}\"}}"
  }
  var attempted = 0
  var statusErrors = 0
  try methods.foreach { m =>
    println(s"ScalaPB invoking ${m._2}")
    val pkg = m._3; val service = m._4; val name = m._5; val reqName = m._6.split('.').last
    val requestPkg = m._6.split('.').dropRight(1).mkString(".")
    val requestOuter = if (requestPkg.startsWith("inference.")) "Inference" else requestPkg.split('.').filterNot(_.matches("v\\d+")).lastOption.map(_.capitalize).getOrElse(sys.error(s"cannot infer outer for $requestPkg"))
    // ScalaPB places messages in <proto package>.<file base package>; the
    // service package follows the same convention.  Keep this derived from
    // the descriptor identity so callers never carry platform package rules.
    def scalaPackage(pkgName: String): String = {
      val suffix = if (pkgName.startsWith("inference.")) "inference" else pkgName.split('.').filterNot(_.matches("v\\d+")).lastOption.getOrElse(sys.error(s"cannot infer package for $pkgName"))
      s"$pkgName.$suffix"
    }
    val requestScalaPkg = scalaPackage(requestPkg)
    val serviceScalaPkg = scalaPackage(pkg)
    val requestCandidates = Vector((requestScalaPkg, reqName)) ++
      (if (service == "HarnessService" && name == "Handshake") Vector(("acyclic.protocol.v1.protocol", "HandshakeRequest")) else Vector.empty)
    val requestCompanionClass = requestCandidates.iterator.map { case (candidatePkg, candidateName) =>
      try Some(Class.forName(s"$candidatePkg.${candidateName}$$")) catch { case _: ClassNotFoundException => None }
    }.collectFirst { case Some(clazz) => clazz }.getOrElse(sys.error(s"missing ScalaPB request type ${m._6}"))
    val requestCompanion = requestCompanionClass.getField("MODULE$").get(null)
    val message = requestCompanion.getClass.getMethod("parseFrom", classOf[Array[Byte]]).invoke(requestCompanion, m._9)
    val encoded = message.getClass.getMethod("toByteArray").invoke(message).asInstanceOf[Array[Byte]]
    require(java.util.Arrays.equals(m._9, encoded), s"${m._2}: Rust request bytes changed")
    val grpcModule = Class.forName(s"$serviceScalaPkg.${service}Grpc$$").getField("MODULE$").get(null)
    val methodName = name.head.toLower + name.tail
    val normalizedMethod = name.replaceAll("[^A-Za-z0-9]", "").toLowerCase
    val methodGetter = grpcModule.getClass.getMethods.find { method =>
      method.getName.startsWith("METHOD_") && method.getParameterCount == 0 &&
        method.getName.stripPrefix("METHOD_").replaceAll("[^A-Za-z0-9]", "").toLowerCase == normalizedMethod
    }.getOrElse(sys.error(s"missing ScalaPB method descriptor for $name"))
    val methodDescriptor = methodGetter.invoke(grpcModule).asInstanceOf[io.grpc.MethodDescriptor[_, _]]
    val methodType = methodDescriptor.getType.toString
    val clientStreaming = methodType == "CLIENT_STREAMING" || methodType == "BIDI_STREAMING"
    val serverStreaming = methodType == "SERVER_STREAMING" || methodType == "BIDI_STREAMING"
    if (!clientStreaming && !serverStreaming) {
      val stubFactory = grpcModule.getClass.getMethods.find(x => x.getName == "blockingStub" && x.getParameterCount == 1).getOrElse(sys.error(s"missing ScalaPB blockingStub for $service"))
      val stub = stubFactory.invoke(grpcModule, channel)
      val call = stub.getClass.getMethods.find(x => x.getName == methodName && x.getParameterCount == 1).getOrElse(sys.error(s"missing generated ScalaPB method $methodName"))
      try {
        val response = call.invoke(stub, message).asInstanceOf[AnyRef]
        recordObservation(m._2, m._9, Seq(response.getClass.getMethod("toByteArray").invoke(response).asInstanceOf[Array[Byte]]), "OK")
      } catch { case e: InvocationTargetException if unwrap(e).isInstanceOf[StatusRuntimeException] =>
        statusErrors += 1
        recordObservation(m._2, m._9, Seq.empty, unwrap(e).asInstanceOf[StatusRuntimeException].getStatus.getCode.name)
      }
    } else {
      val stubFactory = grpcModule.getClass.getMethods.find(x => x.getName == "stub" && x.getParameterCount == 1).getOrElse(sys.error(s"missing ScalaPB async stub for $service"))
      val stub = stubFactory.invoke(grpcModule, channel)
      val responseBytes = ArrayBuffer.empty[Array[Byte]]
      var terminalStatus = "OK"
      val callback = java.lang.reflect.Proxy.newProxyInstance(classOf[io.grpc.stub.StreamObserver[_]].getClassLoader, Array(classOf[io.grpc.stub.StreamObserver[_]]), (_, method, args) => {
        method.getName match {
          case "onNext" if args != null && args.nonEmpty => responseBytes += args(0).asInstanceOf[AnyRef].getClass.getMethod("toByteArray").invoke(args(0)).asInstanceOf[Array[Byte]]
          case "onError" if args != null && args.nonEmpty => unwrap(args(0).asInstanceOf[Throwable]) match { case e: StatusRuntimeException => terminalStatus = e.getStatus.getCode.name; case other => terminalStatus = other.getClass.getSimpleName }
          case _ =>
        }
        null
      }).asInstanceOf[io.grpc.stub.StreamObserver[Any]]
      if (serverStreaming && !clientStreaming) {
        val call = stub.getClass.getMethods.find(x => x.getName == methodName && x.getParameterCount == 2).getOrElse(sys.error(s"missing ScalaPB server streaming method $methodName"))
        try {
          call.invoke(stub, message, callback)
        } catch { case e: InvocationTargetException if unwrap(e).isInstanceOf[StatusRuntimeException] => terminalStatus = unwrap(e).asInstanceOf[StatusRuntimeException].getStatus.getCode.name; statusErrors += 1 }
      } else {
        val call = stub.getClass.getMethods.find(x => x.getName == methodName && x.getParameterCount == 1).getOrElse(sys.error(s"missing ScalaPB client streaming method $methodName"))
        val requests = call.invoke(stub, callback).asInstanceOf[io.grpc.stub.StreamObserver[Any]]
        try { requests.onNext(message); requests.onCompleted() } catch { case e: StatusRuntimeException => terminalStatus = e.getStatus.getCode.name; statusErrors += 1 }
      }
      recordObservation(m._2, m._9, responseBytes.toSeq, terminalStatus)
    }
    attempted += 1
  } finally { channel.shutdownNow(); channel.awaitTermination(5, TimeUnit.SECONDS) }
  require(attempted == count, s"only $attempted/$count ScalaPB calls reached transport")
  val mode = if (allowPartial) "partial reachability" else "reachability"
  sys.env.get("ACYCLIC_SCALA_OBSERVATION_FILE").foreach { path =>
    val body = s"{\"schema\":\"acyclic.scala.typed-runtime-observation.v1\",\"source_git_sha\":\"$sourceSha\",\"record_count\":$count,\"attempted\":$attempted,\"observations\":[${observations.mkString(",")}] }\n"
    Files.writeString(Path.of(path), body)
  }
  println(s"ScalaPB schema-v2 generated-stub $mode: $attempted/$count methods from $sourceSha; status observations $statusErrors")
  private def unwrap(error: Throwable): Throwable = error match { case e: InvocationTargetException => unwrap(e.getTargetException); case other => other }
}
