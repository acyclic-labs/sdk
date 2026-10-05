import inference.customer.v1.inference.{ContextProvenance, ContextView, Empty}

object InvalidScalaRustTypes {
  val parent: String = ContextView().parent
  val origin: String = ContextProvenance.Origin.Created(Empty())
}
