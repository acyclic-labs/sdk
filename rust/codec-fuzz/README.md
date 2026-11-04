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

Pass a target name to replay only its retained corpus, for example
`cargo run --locked --example replay -- actor_codecs`. The Actors oracle first
requires admission of the production writers shared with capture, including
their original invocation, checkpoint and effect values. The current invocation
wire capture comes from `Store::enqueue_invoke` and the returned ticket's original
record, including provider-assigned sequence, commit, time and origin. Earlier
invocation captures remain historical inputs. Run
`python3 actor-negative-control.py` to prove that the same corpus oracle detects
a production invocation decoder that rejects every marked request. The control
also demonstrates that conditional round trips alone miss that mutation and
restores both source files on exit. Corpus replay does not establish instrumented
fuzz coverage or distributed recovery guarantees.

`python3 actor-negative-control.py --guest-wire` plants refusals in the actual
manifest and delivery decoders. The conditional oracle must miss both, then the
unchanged positive writer oracle must reject each independently. Exact source
restoration replays the retained corpus. This reuses the original manifest and
delivery producers, corpus and provenance; it does not qualify complete branch
coverage or physical guest transport.

The `call_record` target drives `call::model::check` through the original commit
builders, provider and journal. The `actor_codecs` target feeds committed Stream wire envelopes to
`Entry::decode`, `fence` and `Journal::apply`. Its positive records are captured
from actual commits with a fixed provider clock and stable retry identities.
Admission distinguishes cancellation before acceptance from the indeterminate
outcome after acceptance. `python3 actor-negative-control.py --call-records`
shows that admission without the call binding misses loss of acceptance context;
the unchanged expanded oracle must reject that same production defect. Existing
Host producer admission already rejects a call decoder which refuses every
record. This finite provider model does not establish physical
recovery, distributed ownership or complete branch coverage.

Call schedules reuse the original Actors corpus and its provenance without
copying it: `cargo run --locked --example replay -- actor_codecs call_record`
or `cargo +nightly fuzz run --fuzz-dir . call_record corpus/actor_codecs`.
These bytes select arbitrary schedules; they are not a call wire format.
Decoder replay performs no per-input call commits. Keep schedule exploration
in `call_record` so larger codec captures do not expand unrelated histories.

`python3 actor-negative-control.py --alarms` checks the original guest transition
decoder with independent mutations that lose `Set` or `Clear`. It first shows
that the earlier `Keep`-only producer/corpus misses lost `Set`, requires the same
expanded oracle to reject both mutations, then captures and replays the original
writers. Captures include zero, ordinary and maximum unsigned deadlines through
the shared transition encoder. Refresh only these Actors captures with
`cargo run --locked --example capture -- --only-actors --extend-goldens`.
Use `python3 actor-negative-control.py --capture` to capture and replay without
repeating source mutations already qualified on unchanged decoder/oracle paths.
Full and Actors-only capture retain every historical Actors input and its provenance.
Versioned invocation and State writer captures retain the earlier corpus and
provenance, including transitions that contain the earlier State version.

`python3 actor-negative-control.py --persisted-alarms` covers the deployed
State codec after actual Store publications: a committed input sets an alarm,
its firing preserves the deadline while consuming the generation, and a second
committed input clears it. The shared producer checks the expected cursor and
alarm tuple after each publication. The control removes delivery-generation
decoding in production, shows the create-only oracle misses it, and requires the
unchanged expanded oracle to reject it before capturing original writer bytes.
These finite MemoryStream publications cover the persisted codec and Store
transition path; physical restart, membership and runtime scheduling remain
separate obligations.

Refresh only the current agent record and checkpoint writers with:

```sh
cargo run --locked --example capture -- --only-agents --extend-goldens
```

Capture the public agent command, binding, invocation and reply writers with:

```sh
cargo run --locked --example capture -- --only-agent-controls --extend-goldens
cargo +nightly fuzz run --fuzz-dir . agent_controls -- -runs=1000
```

The target preserves canonical invocation framing, command/reply correlation,
Unknown request bytes and readable content. Reply corpus entries prefix their
actual JSON command and a NUL separator. Assembly inputs instead prefix the
original Staged invocation before the whole command. They check the public
assembly boundary's agent, command kind and retained invocation; host storage,
digest verification and multi-frame transport remain separate obligations.
Outer-response inputs prefix the original invocation before the response from
`AgentCall::reply`. The public `PreparedAgentInvocation::send` path admits those
bytes, including refusal and Unknown results, and must reject unknown/duplicate
protobuf fields and preserve a non-200 Actors status. Its byte source is immediate;
this exercises response admission without modeling a network or its failures.
These selectors are not product formats.
Every input also requires admission of the retained public `command-root` writer
capture, so refusing all command frames cannot make the conditional oracles pass.
The kept `negative-controls/agent-command-reject-all.patch` changes only the
production frame decoder's successful return to refusal. Apply it from the
repository root, run the same instrumented target and corpus expecting
`captured writer command refused`, then reverse the patch and require the same
corpus to pass. This control checks positive admission; it does not replace the
malformed-input controls.
The same oracle requires the retained `actor-response-root-0` and
`assembled-cancel` captures to enter their original invocation, response and
assembly paths. Their existing arbitrary-input checks report admission to this
positive witness instead of silently skipping a refused capture. The kept
`agent-invocation-reject-all.patch` and `agent-assembly-reject-all.patch` change
the corresponding production success returns. Each must fail the same target
at `captured writer invocation refused` or `captured writer assembly refused`,
respectively; reverse each patch before requiring the original corpus to pass.
These two fixed captures add bounded validation work per fuzz input and no
shipping work, new parser, transport or fixture producer.
The retained `unknown` capture must enter the same recovery-request property on
every input. It requires the owned Unknown variant, exact re-encoding and
unknown-field refusal. The kept `agent-recovery-reject-all.patch` changes only
the production Unknown success return to refusal; the same target must fail at
`captured writer recovery request refused`, then pass after reversal. The
existing exact-request unit control remains distinct: it compares the decoded
domain request with the original domain value before encoding.
The existing `actor-response-refused` capture also enters the same public send
oracle unconditionally. Its typed host refusal must remain an admitted exchange.
The kept `agent-refusal-reject-all.patch` removes the production Refused success
return and must fail at `captured writer response refused`; restoration must pass
the original corpus. Together with the success and Unknown captures, this covers
positive admission for all three result envelope variants.
Malformed base64 is derived from admitted content while retaining its original
encoded length and range. Writer inputs include content encoding to JSON field
names, so wire mutation must preserve those fields. Quiet Usage and Events
snapshots must refuse a live wait without source progress or settlement.
These are SDK writer captures, not hosted traffic or
evidence of authentication, dispatch, restart or distributed progress.

Capture cache facts, summary references and epoch snapshots through their original
writers with `cargo run --locked --example capture -- --only-resources --extend-goldens`.
The existing `harness_codecs` target checks their public decoders, round trips,
unsupported versions, mandatory portable text, native provenance and snapshot
source/range/preparation identity. Its genesis envelope comes from a real
`MemoryStream` commit/read; fuzz mutations replace only the value. These internal
writer captures do not qualify stream authentication, hosted transport, provider
cache attribution or physical producer retirement.
Five additional inputs use serde's positional SnapshotWire fields derived from
the original writer payloads. Each must pass the production decoder first.
The oracle builds mutations from the successfully re-encoded snapshot, so all
accepted field spellings use the writer's canonical object shape. A protective
writer refusal still leaves source and settled-prefix checks exercised; the
writer-dependent mutation checks apply only when encoding succeeds.

Run the retained production mutations with
`python3 mutations/resource-codecs.py`. It first captures and checks the writer
inputs, then removes each production admission guard while keeping the fuzz
oracle unchanged. Rejection requires that guard's semantic diagnostic at the
resource oracle's source location; an unrelated assertion, compiler or tool
failure does not count.
Use `--only snapshot-generation` (or another listed guard) to qualify a changed
oracle without repeating unrelated mutation controls. The pending-generation
challenge changes only the pending field, reaching `State::restore` through
`Snapshot::decode` while leaving the earlier epoch parity fence satisfied.
The two pending-range challenges recompute the content address with the original
`SummaryKey::digest` writer, so the later identity fence cannot mask an omitted
range check. They require the original typed range refusal at the covered cursor
and beyond the source head. The settled-epoch prefix check remains separate;
pending-state range checks run only through these typed oracles. Select both with
`--only snapshot-range-start snapshot-range-end`; one original writer replay
precedes only those two mutations.
Corpus capture, executed coverage and mutation results must be recorded separately;
the presence of a target does not establish complete format or state coverage.

The residency harness in `harness-core/src/executor/model_cache.rs` calls the
production reducer under `cfg(kani)`. It checks every initial access followed by
every retained access, with independent explicit/managed lifetimes, full-width
deadline arithmetic, field preservation, source-order and foreign-key/clock
refusals. This is a two-transition scope from an empty entry, not induction over
arbitrary histories or arbitrary restored entries. Neither this proof nor the
codec oracle establishes original provider attribution, funding, physical release
or whole-system constant cost. Running the codec target does not run Kani.

Agent capture names include the shared record version. Previous captures and
their provenance stay unchanged; waiting-status and checkpoint seeds retain
both a retained action binding and an explicit absent binding for external hosts.

After a reviewed protobuf schema change, refresh only the named protobuf
producers and their table selectors:

```sh
cargo run --locked --example capture -- --only-protobuf --extend-goldens
```

Existing payloads must still compare equal through the current descriptor.
Only their selector changes; new message writers get new captures, and entries
for deleted message types are removed. Other targets and their goldens stay
unchanged. The ordinary changed-value and changed-selector controls still run.

Capture exercises each emitted input and compares it with the single
`compatibility/frames/golden.hex` baseline. Protobuf messages and their JSON
projections compare values through the original descriptor codecs, since map
and object member order is unspecified. Every other format compares exact bytes.
Ordinary comparison retains the original captured packet and its provenance;
it never replaces them with an equivalent ordering. Controls require a changed
message value or selector to fail. After reviewing an intentional
format change, run `cargo run --locked --example capture -- --accept-goldens`.
To add successor frames while refusing changes to existing baselines, use
`cargo run --locked --example capture -- --extend-goldens`. Both modes retain
equivalent existing protobuf packets instead of replacing their map order.
The producer records each path, byte count, SHA-256 and actual producing call in
`corpus/SOURCES.tsv`. Regenerate the complete protobuf corpus when descriptors
change: its first eight little-endian bytes select a generated message entry.
SDK captures come from production encoders and memory-backed operations, rather
than hosted traffic; cryptographic producers use deterministic test keys.
Original v1 state-record captures retain their names and bytes as typed-refusal
inputs; current writers use distinct `state-v2-` names for publication provenance.
Objects envelopes likewise retain v1 captures and publish distinct `head-v2-`
captures. Refresh only these original writer seeds with
`cargo run --locked --example capture -- --only-objects --extend-goldens`.
The retired Actor timer capture remains historical input; it has no current
production timer decoder or producer join.

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

The Objects persisted-codec oracle also derives typed values from the fuzz input,
encodes them with the production writers, and requires exact decoded values.
Refusing every input must not satisfy an acceptance-only round trip. The retained
`negative-controls/object_persistence.patch` plants that defect in the production
epoch decoder. In a clean checkout, apply it from the repository root, then run:

```sh
cargo test --locked -p acyclic-objects --no-default-features --features test-support --lib blob::codec_fuzz::tests::stored_writer_corpus_satisfies_the_fuzz_oracle -- --exact --nocapture
```

The mutation must fail at `persisted fuzz oracle`; a compilation error is not a
negative-control result. The test readmits current and historical writer captures
through the same `check` called by `object_persistence`. The libFuzzer target must
also reject a retained corpus input with that mutation. Reverse the patch before
running the positive check or other work. Corpus readmission does not establish
branch coverage, an instrumented campaign, or physical storage recovery.

The per-format `object_manifest_admission.patch`,
`object_envelope_admission.patch`, `object_batch_admission.patch` and
`object_incarnation_admission.patch` refuse valid inputs in their actual
production decoders. Apply each separately: the same corpus control must fail
its unchanged oracle, then pass after restoring the decoder. These controls
qualify positive admission for each format, without a second test or parser.

The same oracle calls `Digest::from_hex` on arbitrary UTF-8 inputs and requires
the storage-key writer to preserve its original digest in lower and uppercase.
Writer-derived truncation, excess width and nonhex spelling must refuse with
`InvalidArgument`. `object_digest_hex_value.patch` replaces the actual decoded
digest with zeroes; the same retained corpus assertion must fail. The existing
writer corpus supplies generator inputs, rather than handwritten key fixtures.

Objects and the shared Protobuf JSON boundary use one complete-input parser.
Objects admits byte capacity before descriptor lookup, then calls
`acyclic_protobuf_json::decode_admitted`; other callers can use the bounded
`decode` entry point. The existing JSON targets reach this same implementation.
`protobuf_json_trailing.patch` removes its actual end-of-input check. The kept
`bytes_use_base64_and_each_refusal_stage_stays_distinct` control in
`acyclic-protobuf-json` must fail at its original `Trailing` assertion.

With `--features storage-schedules`, the same `object_persistence` target also
reaches the native LocalMedium payload decoder beneath its shared checksum frame.
It requires original PUT/REMOVE admission, exact key/value bytes, and refusal of
short lengths, invalid UTF-8 keys, unknown operations and REMOVE records carrying
a value. The `object_local_reject_all.patch` and `object_local_remove_value.patch`
mutate that actual decoder. Each must fail the same corpus control above with
`--features local,test-support`; reverse the patch before the positive check.
The three `object_persistence/local-*` captures came from production
`local::encode`, through the existing seed producer, at source
`4042dd6d3213da1373a5293bdbb08b28f2ef34cc`, without redaction:

```sh
cargo run --locked --features storage-schedules --example capture -- --only-objects --extend-goldens
cargo +nightly fuzz run --fuzz-dir . --features storage-schedules object_persistence
```

The same oracle also calls the production replay routine on original writer
sequences: retain another key, replace the selected key, then remove it. Expected
value offsets, frame sizes and live-byte totals come from the original frames,
with values before the shared frame checksum, independently of the replay index.
Checksummed unknown-operation frames must
reach its `InvalidData` result. `object_local_replay_index.patch` leaves a removed
key indexed; the unchanged corpus control must detect it. Memory input uses the
same replay routine as the buffered file input, without file I/O in the fuzzer.
These controls do not replace physical crash, restart or release qualification.

The optional `object_revisions` target uses the Objects revision metadata writers,
decoders and final-status admission used by both client entry points. It requires
positive writer admission, explicit-minimum preservation and original failures
despite malformed diagnostic metadata. Run it with `--features object-revisions`.
The same oracle mutates the original writer's HTTP header to invalid base64 and
combines original writer tokens with conflicting incarnations. Both must retain
`InvalidArgument`; valid binary metadata constructors cannot reach the former.
The retained `object_revisions/put-revision` input is the original binary metadata
from `BlobsServer::put_blob`, with its command and source in `SOURCES.tsv`; capture
preserves these bytes instead of minting a different authority. Replay this target
with `--features object-revisions` (and `storage-schedules` for the physical corpus).
The retained `negative-controls/object_revision_metadata.patch` makes the shipped
reply decoder refuse valid revisions. After applying it from the repository root,
the following control must fail at `check_revision_metadata`:

```sh
cargo test --locked -p acyclic-objects --lib grpc::tests::revision_transport::admission_preserves_authority_and_original_failure -- --exact --nocapture
```

The same control must fail with `object_revision_minimum.patch`, which makes the
shipping minimum parser silently discard malformed values and lower the floor.
The oracle also sends all six timeout scales through Tonic's original request
writer and the server's `grpc_timeout` parser. Generated values, the writer's
eight-digit boundary and malformed derivatives retain exact duration or refusal.
Arbitrary bytes reach the parser only after the original HTTP ASCII boundary.
The retained `object_timeout_unit.patch` changes hours into minutes; the same
control must fail its original metadata oracle. This covers parser semantics,
not deadline scheduling, remote cancellation or clock progression.
The separate `object_revision_terminal.patch` restores premature success admission
before final gRPC status. The original-server HTTP/2 control
`grpc::tests::revision_transport::a_late_refusal_retains_its_original_status_instead_of_becoming_stale_success`
must fail with that mutation. Reverse each patch before the positive check. A
compiler failure is not a negative-control result. Local transport controls do
not qualify remote membership, restart behavior or instrumented fuzz coverage.

That same terminal control removes the original status or replaces the terminal
with a body error carrying the original provider's refusal. Missing status must
remain `Unknown`, and the body error must retain its code and details. The kept
`object_revision_body_error.patch` flattens the production body error to its
message; the unchanged terminal control must reject that loss.
The `object_revision_owner.patch` replaces the caller's retained frontier with a
new one. The existing generated GET/PUT/DELETE control must reject that mutation
after opening a replacement connection through `RevisionChannel::with_revisions`.

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
Twenty additional `compact-medium-*` producers prepare an obsolete value using
the original compaction chunk and retain a second live value. They require an
actual sync and rename in the successful trace. Post-append maintenance faults
must preserve the original successful mutation and both live/reopened values.
Preparation reserves two payloads plus the original chunk as obsolete bytes,
with checked allocation representability before IO. A named real-adapter
regression covers payloads larger than that chunk, which otherwise need not
trigger compaction and can cause a false fuzz crash.

```sh
cargo run --features storage-schedules --example capture -- --accept-goldens
cargo run --features storage-schedules --example capture -- --only-storage --extend-goldens
cargo +nightly fuzz run --fuzz-dir . --features storage-schedules object_schedule -- -max_total_time=30
cargo run --features storage-schedules --example object_schedules -- corpus/object_schedule/medium-false-0-0-0
cargo run --features storage-schedules --example object_schedules -- --all-sites --seed medium-false-0-1-0
cargo run --features storage-schedules --example object_schedules -- --all-sites --seed compact-medium-false-1-0-1
```

Capture uses the same provenance and golden mechanism as the syntax targets;
`--only-storage` runs only the physical producers and preserves unrelated
provenance and golden rows. `--extend-goldens` admits new names while refusing
changes to an existing captured value.
At source `38a046a401`, all eighty storage inputs passed capture/readmission;
their original bytes, provenance and shared golden rows are retained.
Without this feature, capture preserves previous physical schedule entries. Replay
of those entries requires the feature. The report groups reached physical
operation/fault/stage/outcome branches and counts actual calls, owned boundaries
and requested bytes. It establishes coverage only for the supplied inputs.
`--seed` selects original writer inputs without a saved capture. `--all-sites`
visits each physical site from one successful baseline, streaming each report
without retaining a history or repeating that baseline for every cut.
At source `f3cc43deeb`, all twenty compaction configurations passed 110 cuts
across every physical site in their successful traces, covering 55 distinct
fault outcomes. Post-append write, sync-data, rename and sync-parent faults
preserved successful mutations and whole live/reopened values. Measured maxima
were six physical calls, four owned boundaries and 932 requested bytes.
These controls do not establish power-loss behavior or an asymptotic complexity
bound. The adapter owns compaction policy; all execution remains on demand.

Existing session/follow state-machine targets remain in `../crates/stream/fuzz`,
accounting/cancellation targets in `../crates/filesystem/fuzz`, Harness transport
and JSON targets in `../crates/harness-fuzz`, and provider streaming/projection
targets in `../crates/harness-providers/fuzz`. The shared inventory maps their
distinct obligations.

The shared JSON refusal property also keeps the original parser cause: unknown
fields retain a data error, and writer-derived trailing input retains a syntax
error in the same `protobuf_json` oracle. `protobuf_json_cause.patch` changes the
production error adapter to an IO error while preserving the `Trailing` stage.
The original stage-only property misses this defect; the cause assertion must
reject it. Objects/shared correspondence covers wrapper mappings and quotas;
it is not independent parser evidence after centralisation.

The existing `object_schedule` oracle also reads the original root's `FORMAT`
from the actual `LocalMedium::open` writer. After awaiting its original owned
close, it derives empty, truncated and extended headers and supplies the
arbitrary payload as another header. The same public native open must refuse
foreign bytes and preserve both header and log; restoring the captured header
must reopen the original value. These checks run once outside the measured
mutation, retaining its existing physical counters. The original legacy-layout
control keeps its distinct typed refusal obligation. `object_root_format.patch`
removes the production foreign-format refusal; the unchanged root oracle must
reject that acceptance. This adds no parser, test, target, polling or production
IO, and does not establish a power-loss or full source-branch guarantee.

`object_write_minimum.patch` restores the original server's read-only revision
admission, including the mutation bypass. The existing generated HTTP/2
GET/PUT/DELETE control must reject it at the unchanged physical-storage oracle:
a foreign scope or incarnation cannot change any key through singleton, batch,
manifest, delete, retain, release or collect routes. The same control ends an
ahead-of-provider mutation through the original authority's close event. This
qualifies server admission independently of the client's final-status check;
it does not establish remote replica or full source-branch coverage.

The retained `mutations/machines-call-opening-producer-loss.patch` restores the
original unguarded finite read before an operation watch opens its follow:

```sh
cargo test --locked -p acyclic-machines --no-default-features --features local --lib provider::journal::tests::machines_call_held_read_keeps_admission_and_original_terminal_progress -- --exact --nocapture
```

The existing real-journal control holds its original MemoryStream read response,
drops the actual Publication producer, and requires immediate typed uncertainty
with the caller's original key before releasing IO. The plant must fail at
`a held original read must observe its producer loss`. Reverse the patch and
require the unchanged control to pass. Successful publication and later
admission while a read is held remain covered; no timeout, polling or physical
provider claim is involved.

Reservation producer admission also runs the shipped `Host::wake`, `Package`
and `Store` with original enqueued invocation records and a controlled clock.
The existing Actors target compares Granted/Exhausted/Expired/Released/Stale
results and the populated and released checkpoint against the original owner,
generation, capacity and deadline. Release uses the actual returned grant.
`python3 actor-negative-control.py --reservation-book` shows that empty-only
admission misses a production `Book::validate` which refuses populated state;
the unchanged producer oracle must catch that same defect. Captures are emitted
by the original production writers. This finite MemoryStream path does not
establish physical restart, distributed ownership or instrumented coverage.

The same Actors target admits the actual `SocketOpen::encode`,
`SocketFrame::encode` and `socket_reply` writers before decoding arbitrary input.
It checks all frame variants and close initiators, reply fields, and offered
versus unoffered subprotocols through `socket_verdict`. The documented peer-close
initiator alias is accepted and normalised by the original decoder.
`python3 actor-negative-control.py --sockets` requires this unchanged oracle to
reject production open refusal, lost close codes and skipped subprotocol checks,
then replays the original retained corpus. These codec controls do not cover
the separate inbox `InputWire` or prove physical session/recovery behavior.

The same target also admits retained Runtime-created deliveries at
`SocketPackage::on_record`. The shared physical socket suite independently sends
and checks Open, text, binary and peer/actor/runtime Close events. Their original
guest wire envelopes and event projections are in `captures/socket-deliveries.capture`;
`Delivery::socket` must preserve the connection and every captured event field.
Arbitrary guest deliveries enter that same private input decoder.
`python3 actor-negative-control.py --socket-inputs` requires the unchanged oracle
to reject an actual production `Delivery::socket` returning no events. Error input
has no shipped producer capture yet; these controls do not establish complete
branch coverage, non-owner routing or upgrades.

`python3 actor-negative-control.py --socket-metadata` derives wrong sequence,
alarm trigger, unknown marker version and overflowing offset from those same
captured deliveries. The positive-only oracle misses a production sequence-guard
bypass; the unchanged derived-record oracle must catch actual sequence/version
guard removal. It reuses the original corpus without reconstructing a wire schema.

The same target covers persisted socket epochs and Opened/Closed ledger records.
`captures/socket-ledger.capture` pairs actual committed LocalStream records with
their original writer inputs. Admission checks their frontiers and targets,
unknown fields and versions, and independently computes counter capacity in u128.
`python3 actor-negative-control.py --socket-ledger` checks production refusal,
canonicality bypass and wrapping arithmetic with that unchanged oracle. Recovery
and compaction reserve their required epoch positions before publishing anything.

`python3 actor-negative-control.py --call-schedules` restores the former decoder
schedule drive and requires a production schedule-entry refusal to expose it.
The separated decoder must still replay; the unchanged schedule entry must
reject removal of the actual settlement guard, then replay the original corpus.

Mutation-only controls leave captured inputs and provenance unchanged. Their final
replay still admits production writers. Use `--capture` explicitly when adding
original writer inputs; it remains the single corpus and golden update path.
