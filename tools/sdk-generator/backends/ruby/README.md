# Ruby producer tooling

This backend generates Ruby protobuf messages and gRPC stubs from verified
canonical Rust descriptor sets. Maintained protoc and the grpc-tools plugin
generate every transport definition. It adds no service policy or embedded
runtime implementation. Rust manifest admission is shared in
`../../shared/authority.mjs`.

```text
ruby/
  src/                  generation and installed qualification runners
  tests/                offline admission and runner controls
    fixtures/consumer/  installed descriptor, wire and runtime type controls
  templates/package/    pinned RubyGems specification
  toolchains/           compiler/plugin/runtime and dependency archive pins
```

Generate from an immutable source snapshot containing `LICENSE` and `NOTICE`,
and a Rust authority export attesting every source and descriptor:

```sh
node tools/sdk-generator/backends/ruby/src/generate.mjs \
  --source-root /immutable-source --authority /rust-export \
  --protoc /tools/protoc --grpc-ruby /tools/grpc_ruby_plugin \
  --output /new-package
```

Output must be absent, have an existing parent and be disjoint from protected
inputs. The producer checks all authority and tool bytes before creating output.
It passes only temporary snapshots of verified descriptor sets to protoc, with
no source include root. Every family must emit bindings; duplicate output paths
fail. Partial output remains on generation failure and temporary descriptors
are removed. Inputs and tools must remain exclusively owned during execution.

Protoc 28.3 hashes are shared in `../../shared/protoc.json`.
`toolchains/toolchain.json` pins grpc-tools 1.84.0's official RubyGems archive
and five plugin binaries: Windows/Linux x64 and x86, and macOS x64. Other hosts
need a separately admitted maintained plugin build. The generator downloads no
tools. Receipts record exact Rust source, input, tool, generator and output
identities. Package metadata carries the same immutable Rust authority.

Run seventeen lightweight offline controls from the repository root:

```sh
node --test --test-concurrency=1 tools/sdk-generator/backends/ruby/tests/*.test.mjs
```

Actual installed qualification is currently pinned to portable RubyInstaller
3.4.11-1 Windows x64. Prepare the runtime and the four raw gem dependency
archives named in `toolchains/toolchain.json` from their official URLs. The
runtime pins include Ruby, its DLL, RubyGems entrypoints and its bundled
BigDecimal 3.1.8 extension. The dependencies are grpc 1.84.0, google-protobuf
4.36.2, googleapis-common-protos-types 1.23.0 and rake 13.4.2. Archive SHA-256
pins come from official RubyGems metadata; the runtime archive digest comes
from its official GitHub release.

```sh
node tools/sdk-generator/backends/ruby/src/qualify.mjs \
  --package /new-package --authority /rust-export \
  --ruby-home /rubyinstaller-3.4.11-1-x64 \
  --cache /prepared-gem-archives --output /new-qualification
```

The qualifier verifies package payload, pinned gem specification, authority,
runtime components and dependency archives before building. It snapshots raw
dependencies, installs offline into a fresh cache and builds with the maintained
RubyGems packer using a fixed `SOURCE_DATE_EPOCH`. It then installs the exact
new gem, checks archive and complete payload equality, and runs consumers from
that installed gem. Inherited Ruby/Gem/Bundler settings are cleared and homes
are owned. The prepared cache's extracted gems and old SDK packages are unused.

Positive controls compare the complete three API descriptors, retaining unknown
fields except Buf's file-level image metadata tag 8042 and source comments.
They check binary bytes, unsigned bounds, optional-zero presence, both oneof
branches, switching/clearing and exact gRPC method/message shapes. Loaded source
provenance must match the fresh SDK installation. Three independent consumers
check intended runtime `TypeError` rejection for invalid byte/integer values;
these are dynamic Ruby controls rather than compile-time type checks. Success
requires completion messages and all controls. Failure preserves output without
a success receipt. Receipts hash all runtime/cache files, controls and logs.

The accepted foundation `5f13157414a5f48425007ffd957ea7d09611e5fe` passes
these installed controls. Independent generation runs emit identical Ruby
sources. Two clean official RubyGems builds and the reusable qualifier produce
gem SHA-256 `c4bac636334fbd2662b5eea962d48ad82b36301a4f66d78215bfd98d5c59c197`.
This qualifies Actors, Workers and Stream installed transport bindings on that
runtime. Remaining families/platforms, Rust-backed RPC, TLS/authentication,
cancellation/recovery and embedded runtime remain outstanding. Routine CI runs
only the offline Node controls and downloads no Ruby runtime or gems.
