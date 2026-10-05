package dev.acyclic.negative

import com.google.protobuf.ByteString
import dev.acyclic.transport.RustSemanticTypesKotlin
import dev.acyclic.transport.RustTypedRequestsKotlin

class InvalidKotlinRustTypes {
  fun invalid() {
    val view = inference.customer.v1.Inference.ContextView.newBuilder()
        .setParent(ByteString.copyFromUtf8("parent")).build()
    val parent: String = view.parent
    val provenance = inference.customer.v1.Inference.ContextProvenance.newBuilder().build()
    val selected: String = provenance.originCase

    // Rust-owned nominal types must remain distinct at the public SDK boundary.
    val actorAsText: String = RustSemanticTypesKotlin.ActorId.of("actor-1")
    val digest: RustSemanticTypesKotlin.Sha256Digest =
        RustSemanticTypesKotlin.ActorId.of("actor-1")
    val invalidRequest = RustTypedRequestsKotlin.streamRead(
        RustSemanticTypesKotlin.ActorId.of("actor-1"))
  }
}
