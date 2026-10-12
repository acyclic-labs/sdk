import acyclic.actors.v1.Actors
import acyclic.stream.v1.Stream
import com.google.protobuf.ByteString

object InstalledKotlinConsumer {
  @JvmStatic fun main(args: Array<String>) {
    InstalledConsumer.main(args)
    val bytes = ByteString.copyFrom(byteArrayOf(0, -1))
    val actor = Actors.CreateActorRequest.newBuilder().setCodeSha256(bytes).setHomeRegion("test").setIdempotencyKey("test").build()
    check(Actors.CreateActorRequest.parseFrom(actor.toByteArray()) == actor)
    val append = Stream.AppendRequest.newBuilder().setPath("test/path").setIfTail(-1L).addRecords(bytes).setIdempotencyKey(bytes).build()
    check(Stream.AppendRequest.parseFrom(append.toByteArray()) == append)
    val read = Stream.ReadRequest.newBuilder().setPath("test/path").setFrom(-1L).setLimit(-1).build()
    check(Stream.ReadRequest.parseFrom(read.toByteArray()) == read)
    val absent = Stream.AppendRequest.newBuilder().build()
    val explicit = Stream.AppendRequest.newBuilder().setIfTail(0L).build()
    check(!absent.hasIfTail() && explicit.hasIfTail())
    check(absent.toByteArray().isEmpty() && explicit.toByteArray().isNotEmpty())
    println("PASS: installed Kotlin Java-binding interoperability, bytes, integer bits, presence and oneof")
  }
}
