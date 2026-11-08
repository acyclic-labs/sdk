# Backend publication conformance

`graphcoder_runs_one_scenario_identically_on_every_backend` drives the shipping
binary against the local host, the served host and the same server with injected
seams. After `write`, the model requests `read` with the exact returned path and
opaque version. The observed read must retain that version, contain the complete
requested text and have no continuation. Each backend proves its own content
before backend-specific identities are removed for comparison.

Build and run the original scenario:

```sh
cargo build --locked --offline -p graphcoder --bin graphcoder --features acyclic-native-runtime/test-support
cargo test --locked --offline -p acyclic-conformance --features graphcoder-backends --test graphcoder_backends graphcoder_runs_one_scenario_identically_on_every_backend -- --exact
```

Apply `backend_empty_publication.patch` from the repository root. It changes the
actual producer's write arguments to an empty payload while preserving the path,
valid immutable version and original expected text. The same scenario must fail
with `immutable read differs from the requested write`. Reverse the patch and
run the scenario again. No constructed write receipt substitutes for a real
publication or read.

The additional read is one fixed operation per backend: four model-requested
tools instead of three, with the same single write. It uses the existing tool
completion path and adds no timer, readiness loop or durability barrier. Content
comparison costs O(bytes returned); retained history and agent count do not
affect selection of the original immutable version.
