Run these checks on demand from this directory. This is an independent Cargo
workspace; fuzzing and proofs have no CI or scheduled jobs.

The [shared format inventory](../../compatibility/FORMATS.md) tracks decoder
versions, targets, goldens and remaining qualification boundaries. A listed source
hook is not evidence of an executed campaign. Bounded harnesses and their exact
obligations live in [the proof package](../proofs/id-codecs/README.md).

With cargo-fuzz and its supported Rust toolchain installed, choose a target from
the inventory and give the campaign an explicit budget. The explicit fuzz directory
selects this project even when a parent directory contains another fuzz workspace:

```sh
cargo +nightly fuzz run --fuzz-dir . ids -- -max_total_time=30
cargo +nightly fuzz run --fuzz-dir . session_frames -- -max_total_time=30
```

Targets impose no input-length cutoff. Increase libFuzzer's `-max_len` when
exploring larger valid frames or snapshots. Reproduce a crash with
`cargo +nightly fuzz run --fuzz-dir . TARGET artifacts/TARGET/crash-HASH`; retain its bytes
and fix the production decoder before treating the campaign as complete.

Capture and replay SDK writer output:

```sh
cargo run --locked --example capture
cargo run --locked --example replay
```

Capture exercises each emitted input and compares it with the single
`compatibility/frames/golden.hex` baseline. Protobuf messages and their JSON
projections compare values through the original descriptor codecs, since map
and object member order is unspecified. Every other format compares exact bytes.
Ordinary comparison retains the original captured packet and its provenance;
it never replaces them with an equivalent ordering. Controls require a changed
message value or selector to fail. After reviewing an intentional
format change, run `cargo run --locked --example capture -- --accept-goldens`.
The producer records each path, byte count, SHA-256 and actual producing call in
`corpus/SOURCES.tsv`. Regenerate the complete protobuf corpus when descriptors
change: its first eight little-endian bytes select a generated message entry.
SDK captures come from production encoders and memory-backed operations, rather
than hosted traffic; cryptographic producers use deterministic test keys.

Capture also writes `corpus/resource-identity-v1.json` from the actual resource
encoder and cross-language vector inputs. When admitting an identity format,
copy that output to `../crates/auth/tests/fixtures/resource-identity-v1.json`;
the shared tests compare the fixture and never rewrite it.

External MCP captures use published server processes. `corpus/EXTERNAL.tsv`
records exact bytes; `captures/` retains requests, stdout, dependency identities,
HTTP status/headers and each expected codec/method. Positive replay must reach
the actual initialization, service or callback validator. Equivalent refusals
cannot qualify a positive input. The sampling refusal is the server's admitted
tool result after the client refuses its callback; no model request runs.

To recapture with Node.js 24 or later:

```sh
npm install --prefix /tmp/mcp-capture --ignore-scripts --no-audit --no-fund --save-exact @modelcontextprotocol/server-filesystem@2.1.0
node capture-mcp.mjs /tmp/mcp-capture/node_modules/@modelcontextprotocol/server-filesystem
npm install --prefix /tmp/mcp-services --ignore-scripts --no-audit --no-fund --save-exact @modelcontextprotocol/server-everything@2.1.0
node capture-mcp.mjs /tmp/mcp-services/node_modules/@modelcontextprotocol/server-everything stdio
node capture-mcp.mjs /tmp/mcp-services/node_modules/@modelcontextprotocol/server-everything http
cargo run --locked --example replay
```

Filesystem discovery uses an empty temporary directory. Reference-server
capture uses its advertised resource, static prompt and echo tool over stdio or
an ephemeral HTTP listener. Readiness, stdout and EOF use events; producer loss
refuses capture. Review changed output against installed versions before
updating the `external/` golden rows. Event IDs and dates are retained without
normalization; SDK capture preserves those external rows.

Syntax targets join the owning semantic decoder where exposed. Protobuf
normalization compares semantic values when maps change byte order or NaNs
cannot compare directly. Canonical persisted codecs additionally require exact
accepted-byte identity. Contextual oracles retain original caller identities,
protected content and authority; parsing checkpoint or index syntax grants no
signature, digest or selected-step authority.

Fixed ID work is bounded by 113 bytes and allocates nothing. Variable codecs
perform work proportional to supplied bytes and returned items. Framing controls
check input consumption, typed EOF and finite producer loss; funded controls
check refusal and allocation retirement. Private hooks use `test-support` and
call the original parser rather than copying it into a model.

The ID oracle also calls the canonical prefix parser on arbitrary input. It
requires exact identity bytes, an untouched borrowed suffix and strict whole-input
admission only for an empty suffix. Every truncation of an admitted identity
refuses. Concatenated production writer outputs retain the complete second
identity, including its sealed association. These controls supplement the bounded
prefix harnesses; neither establishes that every unrelated consumer uses the codec.

The optional `object_schedule` target runs original `LocalMedium` operations
on temporary real roots through `native_runtime::testing::Schedule`. It selects
a physical fault site from the same operation's successful trace, injects
before/after/partial-write/IO/disk-full faults, then checks actual visibility
and reopen results. Mutation and flush outcomes remain separate. A failed
mutation must have no effect; an ambiguous one may recover either whole value.
The current small seed producers cover create, replace and conditional remove,
both mutation and flush stages, all five fault kinds and plain/sealed complete
generation references. Those references are opaque keys, not evidence of a
consumer's generation authority or protection against an aliasing bug.

```sh
cargo run --features storage-schedules --example capture -- --accept-goldens
cargo +nightly fuzz run --fuzz-dir . --features storage-schedules object_schedule -- -max_total_time=30
cargo run --features storage-schedules --example object_schedules -- corpus/object_schedule/medium-false-0-0-0
```

Capture uses the same provenance and golden mechanism as the syntax targets;
without this feature, it preserves previous physical schedule entries. Replay
of those entries requires the feature. The report groups reached physical
operation/fault/stage/outcome branches and counts actual calls, owned boundaries
and requested bytes. It establishes coverage only for the supplied inputs.
No campaign, compaction/rename coverage, power-loss simulation or complexity
bound follows from adding the target. Larger remove inputs can enter the
adapter's original compaction path; its policy is never copied into the
generator. All execution remains on demand.

Existing session/follow state-machine targets remain in `../crates/stream/fuzz`,
accounting/cancellation targets in `../crates/filesystem/fuzz`, Harness transport
and JSON targets in `../crates/harness-fuzz`, and provider streaming/projection
targets in `../crates/harness-providers/fuzz`. The shared inventory maps their
distinct obligations.
