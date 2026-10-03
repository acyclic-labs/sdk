# Go qualification receipts

Each receipt names the family and capability actually exercised. A passing
receipt covers only that row.

| Rust family | Generated binding | Python/Go wire fixture | Local gRPC transport | Rust fixture transport | Status |
| --- | --- | --- | --- | --- | --- |
| Actors v1 | yes | Python encode + Go decode/re-encode | unary metadata and protobuf round trip | create protobuf request/response | pass |
| Stream v2 | yes | Python encode + Go decode/re-encode | server stream and context cancellation | append/read protobuf requests/responses | pass |
| Workers v1 | yes | Python encode + Go decode/re-encode | not run | not run | wire qualified; transport pending |
| Objects v2 | yes | Python encode + Go decode/re-encode | not run | not run | wire qualified; transport pending |
| Filesystem v2 | yes | Python encode + Go decode/re-encode | not run | not run | wire qualified; transport pending |
| Harness v2 | yes | Python encode + Go decode/re-encode | not run | not run | wire qualified; transport pending |
| Machines v1 | yes | Python encode + Go decode/re-encode | not run | not run | wire qualified; transport pending |
| Inference v1 | yes | Python encode + Go decode/re-encode | not run | not run | wire qualified; transport pending |
| Protocol v1 dependency | yes | Python encode + Go decode/re-encode | not run | dependency closure only | wire qualified; transport pending |

The passing Rust fixture receipt used the portable Go 1.27.1 toolchain and
protobuf runtime, marshalled generated Actors and Stream messages, and sent
them to the Rust `sdk-examples` fixture server over HTTP with protobuf
octet-stream bodies. The fixture server does not claim gRPC service
availability, so cancellation is covered by the local Go gRPC test only.

The all-family wire receipt used the installed Python wheel to encode
scenario-shaped messages for Actors, Stream, Objects, Workers, Filesystem,
Harness, Machines, Inference, and Protocol. A portable Go consumer decoded and
re-encoded each byte sequence and verified the source SHA-256, with all nine
families passing. This proves cross-language protobuf compatibility and does
not claim hosted service behavior for the pending families.
