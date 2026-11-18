Golden payloads live in `golden.hex`, one `name hexadecimal-bytes` row per
format. Tests use `acyclic_proto_build::golden::frame`; there is one lookup and
one retained byte baseline for all owning crates. Protocol descriptors remain
in `../descriptors`, which describe schemas rather than individual frames.

`objects.manifest`, `objects.envelope`, `objects.retained`,
`objects.incarnation` and `objects.epoch` are the expected encodings in
`objects::blob::tests::persisted_golden_frames_refuse_other_versions`.
`session.admission` is the expected SessionHello in
`stream::session::tests::versions::admission_golden_frame_opens_without_another_request`.
These expected-byte baselines still require execution against their producing
encoders; they are not claimed as captured service traffic.

`stream.changes-v1` preserves the exact retained v1 frame from
`stream/src/changes/golden.rs` at commit
`2344393e026e78807e85edff8a87c880d31d7f7c`. Its protobuf Record encoding remains
readable as raw wire data; semantic admission refuses its old format marker.
It is a refusal fixture, not a migration input or supported reader version.

From the SDK repository root, run `export ACYCLIC_TEST_SOURCE_ROOT="$PWD"`
before using the Bazel commands below. Run
`bazel run //rust/codec-fuzz:capture_example` to produce fuzz seeds.
The `corpus/SOURCES.tsv` records the producing call,
byte length and SHA-256. Run `bazel run //rust/codec-fuzz:replay_example` to
validate those captured bytes without rewriting them. Never replace provider or external-tool
output with manually authored fixtures.

The `corpus/` names in `golden.hex` are exact copies of those captured SDK
outputs, using the same paths as `SOURCES.tsv`. Capture compares each current
encoder output with this shared baseline before saving a seed. Replay compares
the saved seed with both its provenance and this baseline. After reviewing an
intentional format change, `bazel run //rust/codec-fuzz:capture_example -- --accept-goldens`
captures new baseline rows through the production encoders. Ordinary capture
and replay never update the golden file.
Use `--extend-goldens` to add new names while requiring all existing baselines
to keep the same bytes or protobuf value. Equivalent protobuf packets retain
their original bytes in either mode.

The `external/` rows retain real MCP server stdout. Their paths refer to
`rust/codec-fuzz/corpus/EXTERNAL.tsv`, with request transcripts and installed
package provenance under `rust/codec-fuzz/captures/`. The shared replay example
checks their hashes, golden bytes and actual decoder oracles alongside SDK
outputs. SDK encoder capture preserves these external baselines.

To capture protocol descriptors, run this command from the repository root:

```sh
bazel run //compatibility:capture_descriptors_example
```

The existing shared compiler writes every family to
`target/compatibility-descriptors/`, preserving the retained baselines for review.
Compare those files with `compatibility/descriptors/` before adopting an
intentional schema change. Source comments and retained options participate in
descriptor identity. After updating the reviewed descriptors and their manifest
digests, run the owning equality control:

```sh
bazel test //compatibility:acyclic_proto_build_test
```
