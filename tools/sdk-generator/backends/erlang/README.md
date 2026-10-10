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

`src/package.mjs` admits a bounded gzip/tar archive against its generation
receipt, current producer source, package templates, and the approved Rust
authority. Only the exact regular-file inventory is accepted.

`src/qualify.mjs` installs that archive with the pinned grpcbox 0.18.0 runtime
and all five dependencies from its upstream lock. It compiles the actual
installed application and consumer with warnings treated as errors, checks
loaded-module paths, and executes 26 populated native TCP RPC pairs (23 unary
and three server-streaming). Request and response wire vectors are computed
independently from the Rust descriptors. Bytes, unsigned maxima, optional zero
and omission, real oneofs, and three invalid-type rejections are checked.

```sh
node tools/sdk-generator/backends/erlang/src/qualify.mjs \
  --package /path/to/package.tar.gz --sha256 <archive-sha256> \
  --receipt /path/to/generation-receipt.json \
  --authority /path/to/rust-authority \
  --runtime-root /path/to/prepared-runtime \
  --runtime-inventory /path/to/otp-runtime-inventory.json \
  --tool-home /path/to/prepared-generator-tools \
  --dependencies /path/to/admitted-grpcbox-runtime-libs \
  --output /path/to/new-qualification
```

The installed controls qualify Actors v1, Workers v1 and Stream v1 from the
canonical source and descriptor bytes at revision `1407b6d9d`. A fresh maintained
generation, archive installation and native run passed 25 populated TCP RPC
pairs (22 unary and three server-streaming), three runtime type rejections,
modeled descriptor comparisons and loaded-module provenance checks. Source,
runtime, tool, dependency, compiled SDK and consumer bytes are checked again
after execution. Earlier Stream v2 foundation evidence remains retained
separately at commit `2dd033b7b`.
The complete modeled file descriptor comparison applies declared protobuf
defaults and default JSON names, sorts unordered message/enum declarations,
and normalizes source basenames and compiler-made synthetic optional-oneof
labels. Field numbers, RPC types, actual oneof names and presence-bearing
indices remain checked; mutation controls verify those comparisons. Unknown
descriptor extensions are outside the GPB decoder's modeled comparison. The
original Rust descriptor bytes remain preserved and hash-checked.

The current control inventory also requires the terminal Actors `DeleteActor`
RPC added in main revision `d86b660146738d507029403756117fa6f102a9c4`:
26 calls, with 23 unary and three server-streaming. Fresh generation and
installed qualification for that revision are pending; the 25-call receipts
above remain scoped to their original source revision.

These controls prove installed Erlang transport behavior for the qualified cohort.
Rust-backed execution, embedded runtimes and complete domain
SDK behavior remain separate work. Routine CI runs affected offline Node
controls without downloading OTP or native dependencies.
