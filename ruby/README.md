# Acyclic Ruby transport package

This directory is the transport-only Ruby prototype for the Rust-owned Actors
v1 and Stream v2 contracts. The package deliberately contains no handwritten
API model or service behavior. `generate.rb` reads the canonical protobuf
sources from the repository and invokes the pinned upstream gRPC Ruby
generator.

The generated files are build artifacts and are not checked in. Generate them
before using the package:

```powershell
bundle install
bundle exec ruby generate.rb --schema-root ..\\.tmp-rust-authority --manifest ..\\.tmp-rust-authority\\rust-authority.json
bundle exec ruby -Ilib:test test/transport_contract_test.rb
gem build acyclic-sdk.gemspec
```

The generator requires `grpc-tools` and `grpc` from the same upstream release
line. Versions are pinned in `Gemfile` and `generator.lock.json`; the runtime
uses `google-protobuf` 4.33.0 exactly so a fresh Bundler resolution cannot
silently select a different protobuf ABI. Generated provenance records SHA-256
digests for both schema inputs and the optional Rust authority manifest. CI must
regenerate from a clean checkout and fail on a dirty generated tree.

Ruby's gRPC implementation supports unary, server-streaming, client-streaming,
and bidirectional RPCs. Actors is unary; Stream's `Read`, `Follow`, and
`Children` are server streams. The package also exposes
`Acyclic::Remote::Client`, a thin policy-aware invoker facade. It defaults
native callers to Rust-qualified gRPC and accepts an explicit compatible
transport override. The caller injects the wire invoker, so this facade does
not claim a handwritten HTTP encoder or retry policy. Bearer credentials follow
Rust's `bearer-no-crlf` rule.

The package license is Apache-2.0; dependency license evidence is tracked in
`LICENSE-THIRD-PARTY.md`.

`Acyclic::Remote::Client` delegates transport selection and bearer validation to
the Rust-emitted `lib/acyclic_sdk/generated_remote_policy.rb` snapshot. Refresh
that snapshot with the `sdk-contract-wire generate-products` command whenever
the Rust transport policy changes.

Ruby and the gem toolchain were unavailable on the Windows coordinator during
the prototype pass. That is an environment limitation, not a language
feasibility result. The CI job must run the generation and install checks on a
Ruby 3.2+ runner.
