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

Fuzz seed captures are produced separately by `cargo run --example capture`
from `rust/codec-fuzz`. Their `corpus/SOURCES.tsv` records the producing call,
byte length and SHA-256. Run `cargo run --example replay` to validate those
captured bytes without rewriting them. Never replace provider or external-tool
output with manually authored fixtures.

The `corpus/` names in `golden.hex` are exact copies of those captured SDK
outputs, using the same paths as `SOURCES.tsv`. Capture compares each current
encoder output with this shared baseline before saving a seed. Replay compares
the saved seed with both its provenance and this baseline. After reviewing an
intentional format change, `cargo run --example capture -- --accept-goldens`
captures new baseline rows through the production encoders. Ordinary capture
and replay never update the golden file.
