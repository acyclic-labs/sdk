import acyclic.actors.v1.Actors
import acyclic.workers.v1.Workers
import acyclic.stream.v2.Stream
import com.google.protobuf.ByteString

object InstalledKotlinConsumer {
  @JvmStatic fun main(args: Array<String>) {
    InstalledConsumer.main(args)
    val bytes = ByteString.copyFrom(byteArrayOf(0, -1))
    val actor = Actors.CreateActorRequest.newBuilder().setCodeSha256(bytes).setHomeRegion("test").setIdempotencyKey("test").build()
    check(Actors.CreateActorRequest.parseFrom(actor.toByteArray()) == actor)
    val worker = Workers.PublishVersionRequest.newBuilder().setJavascriptModule(bytes).setExpectedSha256(bytes).setIdempotencyKey("test").build()
    check(Workers.PublishVersionRequest.parseFrom(worker.toByteArray()) == worker)
    val append = Stream.AppendRequest.newBuilder().setPath("test/path").setIfTail(-1L).addRecords(bytes).setIdempotencyKey(bytes).build()
    check(Stream.AppendRequest.parseFrom(append.toByteArray()) == append)
    val read = Stream.ReadRequest.newBuilder().setPath("test/path").setFrom(-1L).setLimit(-1).build()
    check(Stream.ReadRequest.parseFrom(read.toByteArray()) == read)
    val absent = Stream.AppendRequest.newBuilder().build()
    val explicit = Stream.AppendRequest.newBuilder().setIfTail(0L).build()
    check(!absent.hasIfTail() && explicit.hasIfTail())
    check(absent.toByteArray().isEmpty() && explicit.toByteArray().isNotEmpty())
    val oneof = Workers.JobTarget.newBuilder().setVersionSha256(bytes).build()
    check(Workers.JobTarget.parseFrom(oneof.toByteArray()) == oneof)
    println("PASS: installed Kotlin Java-binding interoperability, bytes, integer bits, presence and oneof")
  }
}
