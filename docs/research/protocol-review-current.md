# Current wire protocol provenance review

This review records the wire provenance checks for the Rust-owned contract
models. The regression suite is
`rust/crates/sdk-contract-wire/tests/provenance_regressions.rs`; it keeps exact
byte checks separate from semantic descriptor checks. Exact checks cover the
Actors v1, Stream v2, and Objects v2 model outputs. Semantic checks decode
Workers v1, Machines v1, and the primary Filesystem v2 file, remove only
`source_code_info`, and compare the remaining descriptor topology, fields,
options, services, and roles against the pinned fixture. Filesystem dependency
closure remains a strict requirement.

## Fresh immutable fixture hashes

All values below are SHA-256 over the complete descriptor file bytes.

| Fixture | Bytes | SHA-256 |
| --- | ---: | --- |
| `actors-v1.descriptor.bin` | 5234 | `0515dc7e3f38a5648f85ee52f5bda7e639cc31cf83179a208bb5abbd398a04bf` |
| `stream-v2.descriptor.bin` | 5878 | `1d311dd12a56de4f04923e4144071c507c6b59b1789955fd8629d09c990abd0c` |
| `objects-v2.descriptor.bin` | 7554 | `1968e12e89d38f7076b9aee559c815748858c156401a56ba53e615def49fe86f` |
| `workers-v1.descriptor.bin` | 10799 | `851b6cd37b8cb4baa6d3a111efdad655b89936b2e1057ecb74e62825715bd7d8` |
| `filesystem-v2.descriptor.bin` | 54180 | `105e153060d229569836982527c91bd56a69891691007215fbc599115eca2093` |
| `machines-v1.descriptor.bin` | 30060 | `68feb507148fbf798a3e05236a4d93d36d216c260db0a6a339db5919c630e758` |
| `harness-v2.descriptor.bin` | 53908 | `b1d721a657f40f652a560769ff440b7a1ca44739765fd3114269b727de5474e4` |
| archived `objects/v1/objects_descriptor.bin` | 25221 | `4701187ac8ca87325d42aee0f99c7826ebb63ec2be0ae98c4768006d4ff31a51` |

The archived Filesystem handshake identity remains
`blake3:ece4a6bb58779d216707a426a0ebc5375b5a7a99b14178ce2f9d4f9dd781df60`.
The archived Harness source declares
`blake3:8efc8c682b2ba1025b1221dd203685bdf999d04e87568fad3acdf0e428bd84cf`.

## Fresh source and binary hashes

These SHA-256 values identify the reviewed generator and its model inputs.

| Input | Bytes | SHA-256 |
| --- | ---: | --- |
| `sdk-contract-wire.exe` (debug build) | 1631232 | `499fca8085cabba2a33a123c1f2bd53743a23261fd43609484ceb63ba16b9674` |
| `src/bin/sdk-contract-wire.rs` | 12960 | `aaa40d55f1174c9952e41044a1bb8bfa494c0fd57cc70ec2dfeae4cf759339c5` |
| `src/lib.rs` | 77754 | `18b28b4c866c82483a572d01ea086c5541599ea384e667169b9bf3e1372b5d3b` |
| `src/stream.rs` | 27349 | `cdbef21f709fbdc4448baff90f6428a6819626793ed751b746fdd793974daca` |
| `src/objects.rs` | 48966 | `7b45c1bf35d1647ae98dee6909b0a9e700ca30caa7562dff5a9a483b0931b0db` |
| `src/workers.rs` | 30622 | `f50cc9e6e3d96d93b657e7018d754406cd7211baa626237272dbd05aa383f5e3` |
| `src/filesystem.rs` | 11688 | `6b626a8e6f4063cb92cec6b1ef5e5fd01460333fa51ad566c20caea9659862d7` |
| `src/inference.rs` | 48967 | `6818175c21bfbd12cb3c0a5b3ca7ed40a067cfec0f5a13840efd93649118773e` |
| `src/machines.rs` | 26329 | `1ec8d5885b53dea14ecd429fbcaafea2205d153d3a7a3f735be5e406843c142aa` |
| `src/harness.rs` | 13139 | `5479058784d575cabda542d33311d11cd8734edc8a7d78a2c22adac01a460392` |
| `src/protocol.rs` | 9117 | `c25ea3da72df9e267e45d22deda8ba40e1dbec480f5ff880954ac3caa7be3934` |
| `tests/provenance_regressions.rs` | 8111 | `afcb0af4831f647b74c363c9c7625d51b4811c3ab5c5beeea9de8dc74cc61875` |
| `tests/protocol_model.rs` | 5999 | `0d8a1387b483ac10368469f39f44fcf9b815cb4ac8ce4644fdf7e988edc3be9d` |
| `tests/workers_model.rs` | 5435 | `ce323be21640eaf647d6f1070dfffeac81795d052d8a35a6754c95a8fea9e767` |
| `tests/generator_provenance.rs` | 7470 | `17067be7edee2875615359c4fc3f1468e12b16b942a6f84be8e217682975b23a` |
| `tests/family_compatibility.rs` | 6751 | `aecf265c3002315d7b83a8a86aabb8b2f9e7b95e2ec7fcb440c11ef55f75f65a` |
| `tests/build_script_provenance.rs` | 3122 | `6b68bbf347996b51164209383382ecd6701a5a5e82e502efe6d8cd360db33cf7` |
| `sdk-contract-options/src/lib.rs` | 29540 | `fac54598192dc7218ac958f4fdf3b182d5de7bf6179de21fe84ee1f33c5f10db` |
| `sdk-contract-validation/src/lib.rs` | 57058 | `22fdea65707838e53e4c555d7b42cb73cd85e28287f57506bf6f545d9021cd19` |
| `Cargo.toml` (test validator dependency) | 622 | `0739aff9405842f6bd957a929577dcfeebe17b15552189413fc5c5f28564c37e` |

The `machines.rs` value above is recorded from the fresh build input and is
`1ec8d5885b53dea14ecd429fbcaafea2205d153d3a7a3f735be5e406843c142aa`.
The Protocol follow-on source is Rust-owned at `src/protocol.rs`; its model
matches the Protocol file embedded in the immutable Filesystem dependency
fixture after removing only source locations.

The source-info-free Protocol descriptor set hashes to
`ce697a9dede342fa869397ca984dd148d6c6375d33a0a633483615a506398660`.

The immutable upstream `google/protobuf/descriptor.proto` dependency used by
the full validation-options closure hashes to
`46dc93b4614e090fea8f94c2e6a2c1b8ee894e41a6b6710a6826aca42399c0c4` after
source locations are removed and the descriptor set is re-encoded. The test
also verifies that `validation/v1/options.proto` retains that dependency edge.

## Rust binding authority audit

The public Rust crates now have an explicit authority inventory. Machines,
Stream, Harness, Filesystem, Inference, and Inference Contract consume
descriptors emitted by the Rust model. Actors, Objects, and Workers package
generated Rust and descriptor artifacts produced by the same model generation
entrypoint. No public binding build script is permitted to invoke `protoc` over
an authored or mirrored `.proto` tree.

The executable audit in
`rust/crates/sdk-contract-wire/tests/build_script_provenance.rs` records this
inventory and rejects any `AuthoredProto` authority. The central
`sdk-contract-wire` generator emits every active family’s protobuf source,
descriptor, validation options, authority manifest, and family goldens. The
OpenAPI, SDK, policy, snippet, and website stages consume those Rust-bound
outputs. The checked-in `proto/` files are generated compatibility views; they
are never read to discover a contract.

Archived descriptors and protocol fixtures remain immutable compatibility
baselines. A protocol transition adds a new versioned Rust model and an
explicit compatibility gate rather than editing or regenerating an archive.

## Results and current gaps

`cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline
--test provenance_regressions` produced seven passing checks. The Protocol
integration target produced five passing checks, including immutable fixture
equivalence, complete Filesystem/Harness dependency closure, source drift
rejection, and the pinned upstream WKT options dependency.

The Workers model target produced two passing checks. Its typed descriptor
comparison removes only source locations, while the independent raw validator
retains unknown option extensions; a separate check pins the archived fixture
SHA-256 and the seven Workers RPC identities.

The new family compatibility target covers all eight emitted families,
including Machines, and runs the independent raw-option validator so custom
extensions survive beyond `prost-types`' typed view. All four checks pass for
semantic topology, presence/JSON/alias policy, signed `sint64` encoding, and
raw custom options. The build-script audit target passes its source-only checks
and documents zero direct authored-proto authorities.

The full wire integration run uses the raw semantic validator for descriptors
with custom options. This keeps unknown extension bytes intact instead of
round-tripping them through `prost-types`, which intentionally drops extensions
it does not model.

The passing provenance checks cover Actors/Stream/Objects exact bytes; Workers,
Machines, and Filesystem semantic topology including options and service
roles; fixture and archived Objects v1 hashes; the field and file-option
removal test; complete Harness and Protocol family declarations; and the
static check that the generator does not read active proto paths or invoke
`protoc`.

The Protocol model adds explicit tags 1 and 2 for each handshake identity and
capability field, the Protocol `go_package` option, documented handshake
roles, and closure emission for both Filesystem and Harness. Its integration
checks compare the source-info-free Rust descriptor directly with the Protocol
file embedded in the immutable deployed Filesystem fixture.

The clean-generation regression target now passes all three checks. It verifies
the exact generated set, including `validation/v1/options.proto`, binds every
model artifact byte-for-byte, and rejects missing, extra, or edited artifacts.

The complete generator run emits all nine families plus the validation options
source and writes a complete authority manifest. The generated output check
rejects missing, extra, or edited artifacts, and the Rust model mutation checks
cover descriptor, protobuf, OpenAPI, policy, validation, and documentation
projections.

## Current Rust-only propagation verification

The bounded authority audit regenerated a clean isolated output directory from
the current Rust model and immediately checked it. The output contained 21
files: nine versioned protobuf sources, nine descriptor sets, validation
options, `rust-authority.json`, and `rust-family-goldens.json`. The check passed
without consulting the repository `proto/` tree.

The current focused suites passed as follows:

* `sdk-generation` — 46 tests, including authority manifest mutation rejection,
  descriptor-derived RPC identities and stream shapes, OpenAPI stage binding,
  docs/rustdoc source binding, policy receipt checks, and generated artifact
  drift controls.
* `sdk-openapi-prototype` — 26 tests, including Rust model mutation propagation
  into schemas and routes, Rust comment coverage, protobuf presence and oneof
  metadata, and stale-output rejection.
* `family_compatibility` — 10 tests, including operation-policy coverage,
  descriptor/RPC parity, validation option preservation, and archived semantic
  compatibility.
* `protocol_model` — 5 tests, including handshake identity, validation option
  closure, source drift rejection, and immutable dependency checks.

The archive and handshake tests continue to require the pinned historical
bytes. Mutating a generated source, descriptor, authority manifest, RPC shape,
or family metadata fails closed; regenerating from the Rust model restores the
clean output.

## Executed checks

* `cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline --test provenance_regressions` (7 passed)
* `cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline --test protocol_model` (5 passed)
* `cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline --test workers_model` (2 passed)
* `cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline --test generator_provenance` (3 passed)
* `cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline --test build_script_provenance` (2 passed)
* `cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline --test family_compatibility` (10 passed)
* `cargo test --manifest-path rust/crates/sdk-contract-validation/Cargo.toml --offline` (23 passed)
* `cargo test --manifest-path rust/crates/sdk-contract-options/Cargo.toml --offline` (6 passed)
* `cargo test --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline --tests -- --skip inference_model_matches_normalized_compatibility_descriptor` (all targets passed; 17 library tests and all integration targets passed)
* `cargo check --manifest-path rust/crates/sdk-contract-wire/Cargo.toml --offline` (passed)
* `rustfmt --edition 2024 --check` on owned options, validation, Protocol, and acceptance sources (passed)

No acceptance harness, receipt, or deployment file was modified by this
review.
