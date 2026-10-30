Run these checks on demand. This is an independent Cargo workspace; adding a
target here does not register a scheduled check.

| Target | Production decoders exercised |
| --- | --- |
| `protobuf` | Every declared message in Auth, Actors, Filesystem, Inference, Machines, Objects, Streams and Harness, including session messages, the protocol handshake and nested messages. The build script derives the table from the canonical descriptors and calls each real generated Rust message type. Every Auth entry also exercises its semantic admission and semantic decode/encode round trip, including grant, certificate, revocation, caller and attribute validation. |
| `protobuf_json` | The shared Protobuf JSON decoder over those same descriptors, including writer output, canonical value identity, caller bounds and trailing-input refusal. The resulting binary projection reaches the same typed and semantic checks as `protobuf`, including Auth admission. |
| `session_frames` | Length-delimited and gRPC framing, minimal varints, chunk boundaries, concatenated frames and EOF. Whole-input decoding must agree with chunks of 1, 2, 5, 10 and 127 bytes; each step must consume input. |
| `session_protocol` | Actual session admission and dispatch, including arbitrary first Hello, typed version refusal before credentials, initial interests and request-producer closure. |
| `stream_persistence` | Canonical append/fork/commit commands, journal headers and checksum frames, memory and local snapshot containers, incarnations, retained retry observations, committed envelopes, semantic records, compressed read batches and change records. File and memory frame readers must agree. |
| `object_persistence` | Manifests, stored HEAD envelopes, retained reference batches, incarnations and collection epochs. |
| `ids` | Every ObjectId kind and GenerationId, with both 33-byte plain and 113-byte sealed encodings. |
| `auth` | Canonical blocks, capabilities, caveats, typed grant unions and cryptographic capability verification using the SDK's deterministic test trust. |
| `identities` | Resource identities and Actor references, with exact accepted-byte identity. |
| `actor_codecs` | Durable Actor state, versioned guest manifests/deliveries/transitions, committed invocations, exact artifact checkpoint admission, private reservation request/decision/checkpoint codecs and timer records through their semantic decoders. |
| `filesystem_codecs` | Authority and durable records, plain/sealed distributed state records, workspace context records, JSON join/control projections, funded private core-state JSON/checksum recovery, native exchange journals, native witnesses/metadata, native ownership receipts, directory continuations and the portable Windows USN checkpoint codec. Core-state payloads compare ordinary and funded admission and canonical values; success, refusal and reader failure must return live allocation custody to its baseline. Directory continuations exercise all native name profiles through actual funded decode, encode and retirement; complete original cuts must survive and live allocation returns to its starting value. Ownership receipts use the physical adapter's codec and preserve original lock/parent binding; unsupported versions cannot reach hashing. |
| `filesystem_pages` | All seven immutable node formats, volume creation, generation publication, retention/deletion, export manifests and publication evidence. Immutable nodes use the production default decode limits. |
| `machine_journal` | Retained Machine observations and operation envelopes through the shared journal decoder, with version admission before the JSON payload. |
| `harness_codecs` | Agent inbox, status, close and conversation records, retained turn effects, compact checkpoints, and the actual MCP JSON/SSE and stdio exchange decoders with chunk invariance and typed EOF. MCP service parameters/results cover resources, prompts and tools; callback admission checks canonical request identity, pinned policy and protocol refusal. |

The protobuf oracle compares encoded fixed points, rather than Rust equality:
valid floating-point NaNs do not compare equal to themselves. Unknown protobuf
fields may be discarded at a service boundary. Persisted canonical codecs also
check byte equality wherever their contract requires it.

Both protobuf syntax targets additionally call public semantic conversions for
18 Stream message types, the Objects availability cursor, four Actors request
validators and nine Inference message types. Accepted Stream projections use
their actual reverse codec and preserve the complete semantic value, including
record origins under explicit, matching and foreign follow contexts. Accepted
Inference views must refuse a changed caller-selected identity. The one
round-trip helper also serves Auth; no service codec is reimplemented here.
Private service admission and validators requiring related request/specification
facts remain separate inventory work. These source hooks have not been compiled
or executed with the current changes.

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

Four additional Stream semantic seed producers reuse the same captured
record/batch writer outputs, including an origin-elided projection. Each is
captured under both protobuf syntaxes (eight inputs); capture and shared-golden
admission for these additions are pending. The capture process executes the
real Stream producer once and reuses its outputs.

`corpus/EXTERNAL.tsv` separately records exact output from published
`@modelcontextprotocol/server-filesystem@2.1.0` and
`@modelcontextprotocol/server-everything@2.1.0` processes. The stdio discovery
transcripts exercise the finite exchange. The reference server also supplies
all seven resource, prompt and tool result shapes, an actual sampling callback
request followed by the client's explicit refusal, and eight HTTP/SSE bodies
including initialization. No model request runs. HTTP bodies retain their
original event IDs and empty priming events.

Request transcripts, full reference-server stdout and capture metadata,
including installed dependency identities and HTTP status/headers, live in
`captures/`. Each indexed output records its expected codec and method. Replay
requires actual successful transport and service or callback admission for these
positive inputs; equivalent decoder refusals cannot qualify them. The sampling
tool's error result is an admitted protocol result, not a successful model call.

To recapture with Node.js 24 or later, install the published package in a
temporary directory and pass its package directory to the capture tool:

```sh
npm install --prefix /tmp/mcp-capture --ignore-scripts --no-audit --no-fund --save-exact @modelcontextprotocol/server-filesystem@2.1.0
node capture-mcp.mjs /tmp/mcp-capture/node_modules/@modelcontextprotocol/server-filesystem
npm install --prefix /tmp/mcp-services --ignore-scripts --no-audit --no-fund --save-exact @modelcontextprotocol/server-everything@2.1.0
node capture-mcp.mjs /tmp/mcp-services/node_modules/@modelcontextprotocol/server-everything stdio
node capture-mcp.mjs /tmp/mcp-services/node_modules/@modelcontextprotocol/server-everything http
cargo run --example replay
```

Filesystem discovery uses an empty temporary directory. The reference-server
capture reads a document advertised by its resource catalog, gets its static
prompt and calls its echo tool. HTTP uses the published application over an
ephemeral loopback listener. Listening, stdout and EOF are events; early EOF or
producer failure refuses capture. Review changed external bytes against the
installed versions before updating their `external/` golden rows. Event IDs and
HTTP dates can change on recapture; no output is normalized. SDK encoder capture
preserves the external golden rows.

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
