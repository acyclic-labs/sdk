import acyclic.actors.v1.Actors
import acyclic.workers.v1.Workers
import acyclic.stream.v1.Stream
import com.google.protobuf.ByteString

object InstalledScalaConsumer {
  def main(args: Array[String]): Unit = {
    InstalledConsumer.main(args)
    val bytes = ByteString.copyFrom(Array[Byte](0, -1))
    val actor = Actors.CreateActorRequest.newBuilder().setCodeSha256(bytes).setHomeRegion("test").setIdempotencyKey("test").build()
    assert(Actors.CreateActorRequest.parseFrom(actor.toByteArray()) == actor)
    val worker = Workers.PublishVersionRequest.newBuilder().setJavascriptModule(bytes).setExpectedSha256(bytes).setIdempotencyKey("test").build()
    assert(Workers.PublishVersionRequest.parseFrom(worker.toByteArray()) == worker)
    val append = Stream.AppendRequest.newBuilder().setPath("test/path").setIfTail(-1L).addRecords(bytes).setIdempotencyKey(bytes).build()
    assert(Stream.AppendRequest.parseFrom(append.toByteArray()) == append)
    val read = Stream.ReadRequest.newBuilder().setPath("test/path").setFrom(-1L).setLimit(-1).build()
    assert(Stream.ReadRequest.parseFrom(read.toByteArray()) == read)
    val absent = Stream.AppendRequest.newBuilder().build()
    val explicit = Stream.AppendRequest.newBuilder().setIfTail(0L).build()
    assert(!absent.hasIfTail() && explicit.hasIfTail())
    assert(absent.toByteArray().isEmpty && explicit.toByteArray().nonEmpty)
    val oneof = Workers.JobTarget.newBuilder().setVersionSha256(bytes).build()
    assert(Workers.JobTarget.parseFrom(oneof.toByteArray()) == oneof)
    println("PASS: installed Scala Java-binding interoperability, bytes, integer bits, presence and oneof")
  }
}
