package dev.acyclic.negative

import com.google.protobuf.ByteString

class InvalidKotlinRustTypes {
  fun invalid() {
    val view = inference.customer.v1.Inference.ContextView.newBuilder()
        .setParent(ByteString.copyFromUtf8("parent")).build()
    val parent: String = view.parent
    val provenance = inference.customer.v1.Inference.ContextProvenance.newBuilder().build()
    val selected: String = provenance.originCase
  }
}
