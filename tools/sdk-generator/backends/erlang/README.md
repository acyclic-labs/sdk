# Erlang SDK generator

`src/generate.mjs` admits an immutable Rust authority export and invokes unchanged
GPB 4.21.7 and grpcbox_plugin 0.10.0 through Rebar 3.25.1. It emits Erlang message
maps, native gRPC clients, server behaviours, package metadata, and the original
Rust authority files.

The admitted host is Linux x86_64 with OTP 29.1.1. Supply the exact prepared
generator bundle described by `toolchains/generator-files.json` and the OTP-only
runtime inventory matching `toolchains/toolchain.json`. Both complete trees are
checked before and after execution. Downloads and build caches belong outside
this backend.

```sh
node tools/sdk-generator/backends/erlang/src/generate.mjs \
  --source-root /path/to/rust-source \
  --authority /path/to/rust-authority \
  --runtime-root /path/to/prepared-runtime \
  --runtime-inventory /path/to/otp-runtime-inventory.json \
  --tool-home /path/to/prepared-generator-tools \
  --output /path/to/new-package

node --test tools/sdk-generator/backends/erlang/tests/*.test.mjs
```

Generated type names use an `acyclic_` prefix to avoid OTP built-in type
collisions. Byte fields remain binary/iodata, and an absent proto3 optional
field is represented by an omitted map key. Source filenames and compiler-made
synthetic optional-oneof names in GPB's reflection descriptors can differ from
the original Rust descriptors; the original descriptor bytes are preserved in
the package's `authority/` directory.

The generator tool index records the actual tested provider composition:
providers 1.9.0 declares erlware_commons 1.4.0 while grpcbox_plugin 0.10.0 declares
erlware_commons ~> 1.9.1. The admitted composition uses 1.9.1 and passed the
native public generation path. This is evidence for that composition, rather
than a claim that the upstream declarations agree.

Maintained generation is currently separate from the installed qualification
work. Generation alone does not prove installed RPC, current Stream v1,
Rust-backed execution, or a complete domain SDK.
