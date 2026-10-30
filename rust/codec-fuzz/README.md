Run these checks on demand. This is an independent Cargo workspace; adding a
target here does not register a scheduled check.

| Target | Production decoders exercised |
| --- | --- |
| `protobuf` | Every declared message in Auth, Actors, Filesystem, Inference, Machines, Objects, Streams and Harness, including session messages, the protocol handshake and nested messages. The build script derives the table from the canonical descriptors and calls each real generated Rust message type. |
| `protobuf_json` | The shared Protobuf JSON decoder over those same descriptors, including writer output, canonical value identity, caller bounds and trailing-input refusal. |
| `session_frames` | Length-delimited and gRPC framing, minimal varints, chunk boundaries, concatenated frames and EOF. Whole-input decoding must agree with chunks of 1, 2, 5, 10 and 127 bytes; each step must consume input. |
| `session_protocol` | Actual session admission and dispatch, including arbitrary first Hello, typed version refusal before credentials, initial interests and request-producer closure. |
| `stream_persistence` | Canonical append/fork/commit commands, journal headers and checksum frames, memory and local snapshot containers, incarnations, retained retry observations, committed envelopes, semantic records, compressed read batches and change records. File and memory frame readers must agree. |
| `object_persistence` | Manifests, stored HEAD envelopes, retained reference batches, incarnations and collection epochs. |
| `ids` | Every ObjectId kind and GenerationId, with both 33-byte plain and 113-byte sealed encodings. |
| `auth` | Canonical blocks, capabilities, caveats, typed grant unions and cryptographic capability verification using the SDK's deterministic test trust. |
| `identities` | Resource identities and Actor references, with exact accepted-byte identity. |
| `actor_codecs` | Durable Actor state, versioned guest manifests/deliveries/transitions, committed invocations, exact artifact checkpoint admission, private reservation request/decision/checkpoint codecs and timer records through their semantic decoders. |
| `filesystem_codecs` | Authority and durable records, plain/sealed distributed state records, workspace context records, JSON join/control projections, funded private core-state JSON/checksum recovery and the portable Windows USN checkpoint codec. |
| `filesystem_pages` | All seven immutable node formats, volume creation, generation publication, retention/deletion, export manifests and publication evidence. Immutable nodes use the production default decode limits. |
| `machine_journal` | Retained Machine observations and operation envelopes through the shared journal decoder, with version admission before the JSON payload. |
| `harness_codecs` | Agent inbox, status, close and conversation records, retained turn effects, compact checkpoints, and the actual MCP JSON/SSE and stdio exchange decoders with chunk invariance and typed EOF. |

The protobuf oracle compares encoded fixed points, rather than Rust equality:
valid floating-point NaNs do not compare equal to themselves. Unknown protobuf
fields may be discarded at a service boundary. Persisted canonical codecs also
check byte equality wherever their contract requires it.

From this directory, capture seeds with the SDK's actual encoders:

```sh
cargo run --example capture
cargo run --example replay
```

Capture compares current encoder output with the shared
`compatibility/frames/golden.hex` baseline. It refuses an unreviewed byte change.
After reviewing an intentional format change, run
`cargo run --example capture -- --accept-goldens` to capture the new golden
rows. Replay checks the existing corpus against that same shared lookup.

The producer writes `corpus/SOURCES.tsv` with each path, length, SHA-256 and
producing call. Protobuf seeds are emitted by each generated message's default
encoder and carry an eight-byte little-endian decoder index. Journal facts and
snapshots come from real MemoryStream append, fork and commit operations.
Objects metadata, manifests, ID references and session envelopes come from
their production encoders. Actor seeds include an actual Store creation
effect; filesystem nodes use the canonical node encoders. JSON seeds project
the typed binary output through the same descriptors. The signed token uses only deterministic test keys;
it contains no account credential. These are SDK output captures, not hosted
service traffic. Regenerate the complete protobuf corpus when schemas change,
because the decoder indices can change.

Capture also writes `corpus/resource-identity-v1.json` by applying the actual
resource encoder to the shared cross-language vector inputs. Copy that output
to `../crates/auth/tests/fixtures/resource-identity-v1.json` when admitting a new
identity format; the shared tests only compare and never rewrite the fixture.

With cargo-fuzz and its supported Rust toolchain installed, choose a target and
an explicit run budget:

```sh
cargo +nightly fuzz run session_frames -- -max_total_time=30
cargo +nightly fuzz run session_protocol -- -max_total_time=30
cargo +nightly fuzz run protobuf -- -max_total_time=30
cargo +nightly fuzz run protobuf_json -- -max_total_time=30
cargo +nightly fuzz run stream_persistence -- -max_total_time=30
cargo +nightly fuzz run object_persistence -- -max_total_time=30
cargo +nightly fuzz run ids -- -max_total_time=30
cargo +nightly fuzz run auth -- -max_total_time=30
cargo +nightly fuzz run identities -- -max_total_time=30
cargo +nightly fuzz run actor_codecs -- -max_total_time=30
cargo +nightly fuzz run filesystem_codecs -- -max_total_time=30
cargo +nightly fuzz run filesystem_pages -- -max_total_time=30
cargo +nightly fuzz run machine_journal -- -max_total_time=30
cargo +nightly fuzz run harness_codecs -- -max_total_time=30
```

The time budget belongs to the fuzz campaign. Targets impose no input-length
cutoff. Increase libFuzzer's `-max_len` to explore larger frames or snapshots.
Replay a crash with `cargo +nightly fuzz run TARGET artifacts/TARGET/crash-HASH`.
Keep a reproducer and fix its production decoder before treating the campaign
as complete.

Bounded proofs of the same ID codecs, incremental framing and receive credit
live in [the proof package](../proofs/id-codecs/README.md). Existing end-to-end
session and follow state-machine targets remain in `../crates/stream/fuzz`;
filesystem object accounting and cancellation oracles remain in
`../crates/filesystem/fuzz`. Those checks exercise different contracts.
Harness transport, bounded JSON and model-request targets remain in
`../crates/harness-fuzz`; provider streaming and projection targets remain in
`../crates/harness-providers/fuzz`. The shared inventory maps these existing
oracles rather than replacing their production parsers.

The private-codec support entry points require `test-support`. The file frame
reader accepts `impl Read`, so fuzz input and recovery call the same statically
dispatched parser. Format admission and golden-frame coverage are tracked in
[the shared inventory](../../compatibility/FORMATS.md). Input decoding is linear in bytes and returned items, independent of
agent population or retained history outside the supplied input. Fixed ID work
is bounded by 113 bytes and allocates nothing. No decoder waits on an event;
EOF is a typed framing result.

The capture and replay executables report a failed step and a recovery command.
Replay checks each captured length and SHA-256 before decoding; it never
regenerates a differing baseline. A target or harness listed here does not
establish that it has run. The inventory records remaining format coverage.
