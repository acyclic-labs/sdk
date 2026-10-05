import inference.customer.v1.inference.{ContextProvenance, ContextView, Empty}
import dev.acyclic.transport.{RustSemanticTypesScala, RustTypedRequestsScala}

object InvalidScalaRustTypes {
  val parent: String = ContextView().parent
  val origin: String = ContextProvenance.Origin.Created(Empty())

  // Rust-owned nominal types must remain distinct at the public SDK boundary.
  val actor = RustSemanticTypesScala.ActorId.from("actor-1").toOption.get
  val actorAsText: String = actor
  val digest: RustSemanticTypesScala.Sha256Digest = actor
  val invalidRequest = RustTypedRequestsScala.streamRead(actor)
}
