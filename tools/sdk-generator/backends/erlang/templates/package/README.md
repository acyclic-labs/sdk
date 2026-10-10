# Acyclic Erlang transport SDK

Generated from the packaged canonical Rust schemas with pinned GPB and grpcbox
generators. `authority/` retains the original schemas, complete Rust descriptor
sets, and their immutable manifest.

Build with Erlang/OTP 29.1.1 and grpcbox 0.18.0. Message maps use binary strings
and bytes; generated message type names have an `acyclic_` prefix. Set a
proto3 optional field to zero to preserve zero presence, and omit its map key
to clear presence. A real oneof uses `{FieldName, Value}` under the group key.

Use the generated `*_service_client` modules with a grpcbox channel. Unary
methods return `{ok, Response, Metadata}`; server-streaming methods return a
stream handle. Inspect trailers before calling the end-of-stream helper,
which may consume them. Set the server listener and client channel explicitly.

GPB is a generation tool and is not a runtime dependency of this package.
The required runtime is grpcbox and its admitted dependency closure.
