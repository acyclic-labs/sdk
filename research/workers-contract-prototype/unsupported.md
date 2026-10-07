# Protify 0.1.4 capability audit

Evidence was read from the pinned local `protify-0.1.4` and `protify-proc-macro-0.1.4` crates. The audit source is retained outside this prototype under `Q:\sdk\work\protify-0.1.4-source-audit-20261007` and `Q:\sdk\work\protify-proc-macro-0.1.4-source-audit-20261007`.

## Supported by the pinned source

- Explicit message, enum, oneof, and service declarations through `#[proto_message]`, `#[proto_enum]`, `#[proto_oneof]`, and `#[proto_service]`.
- Explicit field tags and oneof variant tags.
- `Option<T>` inferred as `ProtoField::Optional`; the proc macro emits `prost(..., optional)` and the renderer emits an `optional` scalar. This preserves the presence of `expected_revision` and `resolved_revision`.
- `Vec<T>` inferred as repeated; explicit `repeated(message)` is available for repeated messages.
- `Bytes` maps directly to protobuf `bytes` and is the required Rust type for byte fields.
- Message presence through `Option<Message>` with `#[proto(message)]`.
- Enum fields with explicit `enum_(Type)` and stable numeric values assigned on Rust enum variants.
- Reusable oneofs with exact tags and generated oneof membership checks.
- Message and enum reserved numbers and names.
- File package/name, file options such as `go_package`, manual package registration, and service method request/response types.
- Service methods with unary or stream markers; Workers uses unary methods.
- Field/message/enum/service names can be explicitly overridden with `#[proto(name = ...)]` where a future wire identity requires it.

## Features not represented by the current Workers proto

No custom field `json_name`, custom field options, extensions, maps, nested messages, or streaming methods are required by Workers v1. The prototype therefore uses the default protobuf JSON name derived from each exact snake_case field identifier.

## Verified gaps requiring a migration gate

- The pinned source has no `json_name` attribute in its field/container parsing or renderer. If an archived contract ever contains an explicit non-default `json_name`, Protify 0.1.4 cannot express it directly; preserve that archived contract with a descriptor/proto compatibility fixture before adopting declarations.
- Protify source documents schema rendering and descriptor support, but this source-only prototype has not run its renderer. Product migration must run the clean render and descriptor comparison before replacing the existing proto.
- Protify renders from Rust metadata and cannot infer service HTTP routes, route templates, transport policy, or Workers semantic validation from this schema alone. Those remain separate Rust-owned metadata inputs in the product migration and must feed the same generation entrypoint.
- Protify's documented limitations require `Bytes` for protobuf bytes and do not support generic or lifetime-bearing proxy types. The prototype uses `Bytes` and direct messages accordingly.
- Generated declarations implement the protobuf model and service schema. They do not provide the remote transport implementation; the product facade must keep the canonical Rust transport and expose language bindings around it.

## Source evidence index

- `src/parsing/field_data.rs:159-168,196` infers `Option<T>` as `ProtoField::Optional`.
- `src/parsing/field_data/field_data_impls.rs:67-75,141` emits prost optional fields and keeps message presence in `Option<Message>`.
- `src/parsing/oneof_info.rs:35-62` requires explicit oneof tags and records them for the generated prost attribute.
- `docs/field_ref.md:48-53,73-80,108-114` documents inferred optional/repeated cardinality, explicit field names, and tags.
- `src/parsing/enum_variant_attributes.rs:25-47` accepts explicit enum value names but rejects unrecognized attributes; numeric values come from Rust discriminants in `enum_proc_macro.rs:32-83`.
- A source search of pinned Protify for `json_name` returned no matches, so the prototype uses explicit field `name` attributes for protobuf field identity and relies on protobuf's default JSON mapping.
- `src/guide/limitations.md` explicitly requires `Bytes` for protobuf bytes and limits proxy structs/enums to non-generic, non-lifetime types.
