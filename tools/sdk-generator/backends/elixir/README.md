# Elixir transport generator

`src/generate.mjs` consumes checksum-admitted Rust descriptor sets through protoc
28.3 and the unchanged protobuf 0.17.1 Elixir plugin. Generated bindings include
the maintained gRPC service/stub code and message, enum and service descriptors.
The Mix package pins protobuf 0.17.1, grpc_core 1.0.5 and grpc 1.0.5.

```sh
node tools/sdk-generator/backends/elixir/src/generate.mjs \
  --source-root foundation/source --authority foundation/authority \
  --protoc /absolute/path/to/protoc-28.3 \
  --elixir-plugin /absolute/path/to/protoc-gen-elixir \
  --runtime-root /absolute/path/to/admitted-runtime \
  --runtime-inventory /absolute/path/to/runtime-admission.json \
  --output /absolute/path/to/new-package
```

The current admitted host is Linux x86_64. `toolchains/toolchain.json` pins the
plugin and the external full-runtime inventory for OTP 29.1.1 and Elixir 1.20.4.
The runtime inventory records 4,358 files and one symlink in the two SDK roots;
its metadata binds both published archive digests. The prepared OTP installation
contains prefix-specific launcher scripts, so the admitted tree must retain its
qualified installation prefix. Runtime archives, inventories, downloads, package
output and native compilation products remain outside this source directory.

Generation checks the entire admitted runtime before executing tools, gives
Erlang an isolated home and explicit runtime path, limits its scheduler to one,
and rejects inherited Erlang, Elixir, Mix, Hex and dynamic-loader configuration.
It uses descriptor snapshots and creates an absent output directory. Receipts
bind authority, source, compiler/plugin, runtime inventory, templates and every
emitted file. Offline tests use tiny runtime fixtures and never download tools.

The native prototype independently generated identical bindings twice and
produced two identical package archives against foundation
`5f13157414a5f48425007ffd957ea7d09611e5fe`. A fresh archive-installed consumer
passes Rust message/enum/service descriptors, all 25 populated RPC message pairs,
bytes, unsigned bounds, optional presence, both Append outcome branches and
three intended runtime encoding rejections. Its six compiled dependency source
trees match their locked Hex archives, and changed/extra source and substituted
package metadata faults reject. `tests/fixtures/consumer/Consumer.exs` preserves
the actually executed control source.

These are installed message and transport-metadata controls. They do not execute
native RPC calls or qualify network transport, Rust-backed RPC, remaining
families/platforms or the embedded runtime. The maintained producer's real CLI
invocation passes and emits the same 14 payload files as the archive-installed
native prototype. The reusable installed qualifier remains to be qualified.
