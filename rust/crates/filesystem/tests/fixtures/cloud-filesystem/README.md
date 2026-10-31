# Filesystem contract capture

`source.json` retains the original Cloud handler-test commit, source blobs and
SDK pin. Its `sdk_capture` records the SDK revision and canonical descriptor
used by the current `golden.json`. These are separate provenance: refreshing
SDK wire-engine output does not qualify a live Cloud deployment.

The recorder replays the create/write/read/list/remove/delete sequence from
`write_read_list_and_delete_through_reconstructed_replica` through the SDK's
`FilesystemWireService`. It also records handshake and head lookup for hosted
client setup. Generated identities, complete typed generation-root references
and the handshake descriptor are frozen in the JSON capture. Protobuf JSON
uses the existing canonical descriptor; it is not another protocol or endpoint.

Rust hosted/generated clients and the TypeScript Node gRPC client replay the
same capture over real local gRPC. Their request/response comparisons retain
identity relationships and exact bytes. Descriptor changes require a fresh
capture; replay never rewrites the captured handshake.

Regenerate with the existing recorder, record its exact source revision and
current descriptor blob in `source.json`'s `sdk_capture`, then qualify both clients:

```sh
cd rust
cargo run -p acyclic-fs --example record_cloud_contract --features local,test-support --locked
cargo test -p acyclic-fs --test main --features local,test-support --locked
```

After generating and building the TypeScript filesystem package:

```sh
node --test typescript/packages/filesystem/test/cloud-contract.test.mjs
```

This capture does not run Cloud authentication, replica reconstruction, storage
adapters or Edge routing. Those remain qualified by their owning service tests.
