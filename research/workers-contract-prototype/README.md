# Workers Rust contract prototype

This isolated source-only crate translates `proto/workers/v1/workers.proto` into Rust-owned Protify declarations. It is deliberately outside the SDK worktree and does not alter product/runtime files.

## Pinned inputs

- Protify: `=0.1.4`, `default-features = false`, `features = ["std"]`
- Prost: `=0.14.4`
- Existing Workers proto SHA-256: `8D3904F1645D795678B234E65165E36FEC468D5BF563BADA2F2E2BFDF4751B51`
- Existing Workers descriptor SHA-256: `851B6CD37B8CB4BAA6D3A111EFDAD655B89936B2E1057ECB74E62825715BD7D8`
- Protify crate SHA-256: `E0E4BB2D36634ECD4A2E456706583C039BC095B818D9E11E51E07774F339F6E5`
- Protify proc-macro crate SHA-256: `E6927C26F755F73A592D4CF2E1A287B21BD977A8B837E80AFAD2E3C8E3EFFCF5`

The source follows the maintained Actors migration pattern: a package/file handle, explicit message and enum registration, Rust field tags, Protify oneofs, and a Rust service declaration. The intended generation call is `workers_contract_prototype::render_proto_files(output_dir)`.

## Source layout

- `src/contract_definitions.rs`: complete Workers v1 messages, enums, oneofs, and service.
- `src/contract.rs`: package registration and rendering entrypoint.
- `wire-identity.md`: field-by-field identity map against the current proto.
- `unsupported.md`: features verified in the pinned Protify source and the remaining migration gate.

## Acceptance command for the product migration

From the product worktree, add this declaration set to `rust/crates/workers`, render the proto and descriptor into a clean temporary directory, then compare the normalized descriptor to the immutable Workers baseline. The comparison must preserve field numbers, cardinality, oneof membership, enum values, service method identities, reserved names/numbers, and file options. Only source information and the known Protify declaration ordering normalization may differ.
