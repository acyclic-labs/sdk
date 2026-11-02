# Filesystem session corpus provenance

`subtree-publication-v1` is the unmodified Changes value emitted by the public filesystem producer in `session::resources::subtree::tests::volume_changes_subtree_retains_commit_revision_replay_and_revocation` at source `f52742e907088362fc304a69fe3f35a73b604e6b`, captured on Linux x86_64, 2026-11-02.

Capture command:

```sh
cargo test --locked -p acyclic-fs --no-default-features --features memory,session --lib volume_changes_subtree -- --nocapture
```

The source uses `Fs`, `StreamAuthorityStore`, `MemoryStream` and `MemoryObjectStore`, publishes through the ordinary workspace API, and checks three actual filesystem forks, replay, resume and policy revocation. Authorization is supplied through the test credential helper. This is a library producer capture, not a served network frame or a deployed credential fixture. The seed retains the complete generation and entry references exactly as emitted; no identities or metadata were substituted.
