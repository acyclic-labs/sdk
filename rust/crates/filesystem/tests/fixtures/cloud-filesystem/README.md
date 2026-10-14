# Cloud filesystem contract capture

`source.json` pins Cloud #3230's handler test, source files and SDK revision.
The canonical Filesystem descriptor is byte-identical in that pinned SDK and
this checkout. There is no parallel schema: `golden.json` uses protobuf JSON
from the existing descriptor, including base64 bytes, decimal-text uint64s,
enum names and oneof fields. Cloud's public transport is gRPC.

The capture copies the create/write/read/list/remove/delete sequence and
assertions from `write_read_list_and_delete_through_reconstructed_replica` in
Cloud's `svc/fs/tests/multi_tenant.rs`. It replays those requests through
`FilesystemWireService`, the exact canonical wire engine to which Cloud's
authenticated handler delegates without translating requests or responses.
The recorder also captures handshake and head lookup for hosted-client setup.
Generated workspace/generation identities are frozen in the JSON capture.

This scoped capture exercises the canonical engine. It does **not** run Cloud's
authentication, replica reconstruction, storage adapters, Edge routing or a
deployment. Those checks remain in Cloud's own suite and coordinator qualification.

Both the TypeScript Node gRPC client and Rust hosted/generated clients replay
these shared goldens over real local gRPC. Refresh `source.json` and the goldens
with the same Cloud SDK pin bump after reviewing the new handler tests:

```sh
cargo run -p acyclic-fs --example record_cloud_contract --locked
cargo test -p acyclic-fs --test cloud_contract --locked
node --test typescript/packages/filesystem/test/cloud-contract.test.mjs
```

Build TypeScript before its Node test. On shared lane machines, run each cargo
command through build-slot in a Linux source checkout; run formatting directly.
