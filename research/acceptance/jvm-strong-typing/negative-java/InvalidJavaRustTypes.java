class InvalidJavaRustTypes {
  void invalid() {
    inference.customer.v1.Inference.ContextView view =
        inference.customer.v1.Inference.ContextView.newBuilder().build();
    String parent = view.getParent();
    inference.customer.v1.Inference.ContextProvenance provenance =
        inference.customer.v1.Inference.ContextProvenance.getDefaultInstance();
    provenance.setOrigin(
        inference.customer.v1.Inference.ContextProvenance.OriginCase.CREATED);

    // These assignments must remain compile errors when the Rust-owned
    // package is installed: nominal IDs, bounded digests, and open unions
    // are distinct from their protobuf wire values.
    String mistakenId = dev.acyclic.transport.RustSemanticTypes.ActorId.of("actor-1");
    dev.acyclic.transport.RustSemanticTypes.Sha256Digest mistakenDigest =
        dev.acyclic.transport.RustSemanticTypes.ActorId.of("actor-1");
    String mistakenUnion =
        new dev.acyclic.transport.RustSemanticTypes.Unknown(99, com.google.protobuf.ByteString.EMPTY);

    acyclic.actors.v1.ActorsServiceGrpc.ActorsServiceBlockingStub stub = null;
    dev.acyclic.transport.RustTypedClients.actorsInvokeActor(
        stub,
        dev.acyclic.transport.RustSemanticTypes.MethodName.of("actor-1"),
        dev.acyclic.transport.RustSemanticTypes.MethodName.of("run"));
  }
}
