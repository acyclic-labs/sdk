class InvalidJavaRustTypes {
  void invalid() {
    inference.customer.v1.Inference.ContextView view =
        inference.customer.v1.Inference.ContextView.newBuilder().build();
    String parent = view.getParent();
    inference.customer.v1.Inference.ContextProvenance provenance =
        inference.customer.v1.Inference.ContextProvenance.getDefaultInstance();
    provenance.setOrigin(
        inference.customer.v1.Inference.ContextProvenance.OriginCase.CREATED);
  }
}
