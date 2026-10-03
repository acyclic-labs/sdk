# OpenAPI Generator target qualification

This lane qualifies bounded Rust, PowerShell, Java, Perl, C, and Dart HTTP clients from the Rust-owned
Workers projection at `Q:/sdk/tmp-openapi-integrated2/artifacts/workers.json`.
Neither client defines a second contract. The pinned OpenAPI Generator release
is 7.25.0 (`ef964b0`) with the CLI JAR hash recorded in `receipt.json`.

The Rust package is generated from a metadata-only overlay that adds the
Apache-2.0 license object to the canonical Workers artifact. The overlay does
not alter routes, schemas, operation IDs, protobuf identities, or Rust-owned
extensions. Two generated trees outside the SDK Git checkout were packaged
with `cargo package --allow-dirty --no-verify --offline`; both archives have
SHA-256 `17424d5bc3528dfe9ae9988d88c2da2f51c34901aa2fbc3ca1bdfe5ef0a2fa61`.
The generated `Cargo.toml` declares Apache-2.0, `cargo check` passes offline,
and an isolated consumer completed the canonical Rust fixture request with
protobuf JSON bytes round-tripping as `AQID`.

The PowerShell generator is beta. It is invoked with fixed package name,
version, GUID, and Apache license URI options. The checked-in
`apply-powershell-byte-adaptation.ps1` is a narrow, anchor-checked generated
adaptation for the Workers bytes fields: request `byte[]` values become base64
strings before JSON serialization, and response `body`/`resolvedSha256`
base64 strings become `byte[]`. It does not translate shared algorithms or
define routes. The adapted module imported under PowerShell 7.6.5 and invoked
the canonical fixture with a direct `[byte[]](1,2,3)` caller value. The fixture
received `"body":"AQID"`, and the consumer decoded `b2s=` and `AQID` back to
bytes. A local `AcyclicWorkersHttp-1.0.0-local.zip` package was expanded into
an isolated module directory and imported successfully; its SHA-256 and the
fixture executable hash are recorded in `receipt.json`. A second generation
plus adaptation matched all 94 compared files.

The Java target uses the pinned `okhttp-gson` library and Maven package
`dev.acyclic:acyclic-workers-http:1.0.0`. Maven package and source archives
were produced from the generated POM; its Apache-2.0 name and URL are recorded
in the POM and receipt. A Java 17 consumer sent a direct `byte[] {1,2,3}` and
the Rust fixture received canonical `"body":"AQID"`; the response decoded to
`ok` and `AQID`. The same consumer mapped the Rust fixture's HTTP 409 response
to `ApiException` with status 409 and the canonical error body. Both Java and
Perl consumers also preserve the Workers `resolvedRevision` uint64 decimal
string `18446744073709551615` without numeric coercion.

The Perl target installs generated dependencies into an isolated local CPAN
prefix with Strawberry Perl 5.38.0. Its generated cpanfile had a stale upper
bound on the compatible JSON module; the checked-in, anchor-checked
`apply-perl-runtime-adaptation.ps1` only relaxes that bound. The generated
consumer compiles and completes the canonical fixture and 409 error fixture.
Perl's generated model exposes bytes as base64 strings, so the consumer passes
`AQID` explicitly; this lane does not claim a native Perl byte-array adapter.

The Rust entrypoint now emits the narrow PowerShell adapter with
`--powershell-adaptation`. It derives the Workers bytes and uint64 field names
from `ContractSpec`, keeps OAG package scaffolding separate, and fails closed
when any generated request, response, or API-client anchor drifts. A fresh
pinned OAG package built successfully from the emitted script; a deliberately
modified request anchor exited 1 with the refusal message. OpenAPI Generator
7.25.0's PowerShell options expose package/license metadata but no Workers
byte/base64 serialization option, so this target-specific behavior remains in
the Rust-owned renderer. The package source authority is the `WORKERS` contract
export, and the live fixture evidence is bound to that exact `workers.json`
artifact hash.

Both clients are HTTP projections. This qualification does not cover native
gRPC streaming, SSE, retry policy, recovery, or Rust validation semantics.
The receipt is the source-bound record of tool versions, artifact hashes,
license metadata, consumer commands, and fixture exits.

The unified Rust generation stage carries this qualification boundary into its
generated output as `openapi/stage-receipt.json`. That receipt records the
7.25.0 release, commit, and CLI hash, the Apache-2.0 metadata-overlay scope,
and the SHA-256 and anchor report for the Rust-emitted PowerShell adaptation.
The orchestration manifest hashes the receipt together with every projection,
so its `check` operation detects drift in the pin, license metadata, anchors,
or adaptation bytes.

Java and Perl have completed the bounded HTTP consumer and dependency checks
recorded in `receipt.json`; that evidence does not qualify native gRPC. The C
lane now builds with the pinned official curl development archive and the
working LLVM/Ninja toolchain after one anchor-checked generated local-name
repair. Its consumer passes the bytes, max uint64, and 409 fixture checks.
The Dart lane now uses the official stable Dart 3.13.5 SDK; the Rust-owned
adaptation pins the current cached `test` line, adds package-level
`Apache-2.0` metadata, and emits an AOT package smoke test. Offline dependency
resolution, analysis, and the emitted smoke test pass. With
`frontend_server_client` 4.0.0 selecting the official AOT frontend,
`dart test --concurrency=1` passes all 70 generated tests. The earlier 3.8.3
missing-snapshot failure remains in the receipt as historical evidence. Both
targets carry per-component license manifests and curated portable archives.
These records are scoped to the generated source, cJSON/curl, and resolved
Dart package files; they do not inherit a global Apache claim from another
artifact.

The generated Rust reqwest package now has a separate semantic receipt lane:
the compiled consumer passes base64 bytes, the maximum uint64 decimal string,
and the typed 409 error response against the canonical fixtures.

`c-portable-manifest.json` and `dart-portable-manifest.json` record archive
hashes and file inventories. `c-install-license-manifest.json` and
`dart-license-manifest.json` record the license evidence for each target.

The runnable consumer snippets are `rust-consumer.rs`,
`powershell-consumer.ps1`, `JavaConsumer.java`, `perl-consumer.pl`,
`c-consumer.c`, and `dart-consumer.dart`; they exercise generated packages
without redeclaring the Workers contract.
