# Additional-language qualification audit

Recorded 2026-10-09 from clean base
`6aaed7e3a49a609c8a62790356ce94588d2e44f5`. Remote main was not revalidated:
the local Git query failed with `getaddrinfo() thread failed to start`.
This is a source/evidence audit, not a new generation or execution receipt.

Historical paths below are relative to the parent migration checkout,
`C:/Users/varun/.codex/worktrees/rust-sdk-docs-source/sdk`. They are not present
in this clean base. Its dirty tree must not be imported wholesale. Each accepted
replacement needs an immutable source handoff and matching installed packages.

## Coverage retained for qualification

| Cohort | Targets | Historical evidence and limits |
| --- | --- | --- |
| Python/Go | Python, Go | `research/acceptance/python-go/current-installed-method-matrix-20261004.json` records Stream and Machines coverage; Actors and Inference remain partial. `go/qualification-matrix.md` distinguishes protobuf-over-HTTP fixture probes from local gRPC cancellation. |
| JVM/.NET | Java, Kotlin, Scala, C# | `research/acceptance/jvm-dotnet/current-producer-receipt-20261004.json` records reproducible installed JAR/NuGet artifacts. This does not qualify embedded Rust bindings. Kotlin `qualify-final-producer.ps1` explicitly retains the maintained-binding/type-table cutover gap. |
| Native | Swift, C++, Objective-C | Swift's historical Windows transport guard does not exclude its Linux/macOS target. Embedded consumer sources and historical execution notes require matching accepted runtime artifacts. |
| Remaining core | Ruby, PHP, Dart | `research/acceptance/ruby-php-dart/qualification/current-source-qualification-20261004.receipt.json` and `installed-runtime-availability.receipt.json` contain bounded package/runtime evidence. The retained native shutdown failure and PHP generator availability gap prevent a clean completion claim. |
| Additional gRPC candidates | Elixir, Erlang, Ballerina, OCaml, Common Lisp | Pinned recipes in `research/additional-languages/qualification-manifest.json` and the inventory require fresh maintained-tool and installed-package qualification. Recipe existence is insufficient. |
| Reconsider older exclusions | Haskell, Lua/LuaJIT | Haskell has a newer proto-lens/grapesy prototype; Lua has a Rust C-ABI lane. Neither can be excluded by the older grpc-haskell/lua-protobuf inventory alone. The unrelated pinned lua-grpc repository has a recorded missing-license blocker. |
| HTTP projections | Ada, C, Clojure, Crystal, Elm, GDScript, Julia, Nim, Perl, PowerShell, R, Bash | `research/additional-languages/openapi-targets` contains bounded historical evidence. Preserve HTTP scope; these receipts do not prove gRPC or embedded parity. Every viable unfinished target remains outstanding. |
| Tooling | Documentation and execution generators | Track separately from SDK language packages; snippet execution must use the exact qualified package. |

The inventory also contains Rust and TypeScript, owned by the foundation loop.
The cohort list records observations and work allocation; Rust-owned metadata
remains the generation authority.

## Historical input identities

| Input | SHA-256 read during this audit |
| --- | --- |
| `languages/generation-targets.json` | `241ff8e880588b7f512169aeb710bfa3855e224ed1df8293ea654baea3cdd7cf` |
| `research/additional-languages/qualification-manifest.json` | `2c4e72ed196b1c71b14dc259ceb9538f31d57948d8f31b05be75b7e61cb1236e` |
| `research/additional-languages/haskell-grapesy-prototype/README.md` | `6fc9f9b8797084b7f0a018c48db464eb8ec2b472b26463b99531167eaad18959` |
| `docs/research/embedded-bindings.md` | `6878fc9b54d69cf9b464f05a1c701b27f00f1c64fb653af5980a89c9a109745d` |

Historical embedded notes record executed C/Python consumers, while the audited
test programs themselves are source files. Neither is current qualification of
the pending foundation ABI. UniFFI pins differ between the older embedded
comparison (0.32.2) and the Actors Python typing patch (0.31.0); select the
accepted foundation pin rather than combining their generated surfaces.

## Maintenance reduction gates

Kotlin's duplicate type/operation tables can be removed after Rust metadata
generates the accepted maintained binding. The Python UniFFI typing patch needs
its positive and negative controls retained until an equivalent maintained
replacement passes. Generated Ruby/PHP/Dart policy snapshots are not themselves
authored implementations; audit the emitter and adapters before counting any
deletion. Do not count vendored OSS movement as removal of custom code. Shared
Protify and Rust ABI changes remain foundation-owned.

## Coupled qualification proposal

Await the foundation owner's exact reconstructed source commit and accepted
interface/lock hashes. Prepare one source-attested generation run together with
the docs owner; reuse its canonical exports and native assets across consumers.
Then request finite cohort workloads from the integration coordinator, with
jobs and test threads set to one, per-host disk bounds and owned output paths.
There is currently no build, runtime, fixture or remote-workload admission.

For each cohort, retain deterministic generation and artifact hashes, clean
local installation, positive/negative type controls where supported, and actual
serialization, presence, error, RPC-shape, TLS/authentication, cancellation,
recovery and embedded lifetime results applicable to the surface. Formal claims
must name the production source bindings, assumptions and proved properties;
bounded tests and compile-time method counts cannot establish universal lifetime
or transport guarantees. Unsupported cases need concrete maintained-tool or
platform evidence. Missing admission is pending work, not a target exclusion.

Supply executable snippets and scoped results to the docs owner with exact Rust
source, package version, package hash, snippet source hash, runtime/compiler and
observed output. Preserve the docs owner's TypeScript receipt interface without
claiming its six execution results for another language.
