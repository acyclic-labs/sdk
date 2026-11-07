# Filesystem session corpus provenance

`subtree-publication-v1` is the unmodified Changes value emitted by the public filesystem producer in `session::resources::subtree::tests::volume_changes_subtree_retains_commit_revision_replay_and_revocation` at source `f52742e907088362fc304a69fe3f35a73b604e6b`, captured on Linux x86_64, 2026-11-02.

Capture command:

```sh
cargo test --locked -p acyclic-fs --no-default-features --features memory,session --lib volume_changes_subtree -- --nocapture
```

The source uses `Fs`, `StreamAuthorityStore`, `MemoryStream` and `MemoryObjectStore`, publishes through the ordinary workspace API, and checks three actual filesystem forks, replay, resume and policy revocation. Authorization is supplied through the test credential helper. This is a library producer capture, not a served network frame or a deployed credential fixture. The seed retains the complete generation and entry references exactly as emitted; no identities or metadata were substituted.

`native_ascii_leaf` seeds are structured Rust library inputs for the retained component and root-name accounting checkers. Each 88-byte input also selects a root NUL position from its first byte. The seeds cover leading, interior and trailing NUL, a high byte, a separator and a colon. They are generated boundary inputs rather than external tool or provider transcripts; their generator source and hashes are recorded in `SOURCES.tsv`.

`fuzz/mutations/native_root_name_full_charge.patch` restores full-bound recording
in the shared native work operation. After applying it from the repository root,
`cargo test --locked -p acyclic-fs --all-features --lib native_host::open::prepared_leaf_tests::native_root_names_exhaust_nul_positions_on_original_work -- --exact`
must fail at the exact examined-prefix assertion; the restored source must pass.
The same checker is called by `native_ascii_leaf`. Static patch applicability
alone does not establish runtime mutation detection or instrumented coverage.
