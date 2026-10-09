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
Run local cohort checks with jobs and test threads set to one, bounded caches
and owned output paths. Local builds, tests and locked dependency installation
are authorized; coordinate contention without a repeated host-approval gate.
Each loop owns its focused PR, affected required CI and protected merge against
the latest `origin/main`. Separate backup-upload rejections remain in force.

For each cohort, retain deterministic generation and artifact hashes, clean
local installation, positive/negative type controls where supported, and actual
serialization, presence, error, RPC-shape, TLS/authentication, cancellation,
recovery and embedded lifetime results applicable to the surface. Formal claims
must name the production source bindings, assumptions and proved properties;
bounded tests and compile-time method counts cannot establish universal lifetime
or transport guarantees. Unsupported cases need concrete maintained-tool or
platform evidence. Missing admission is pending work, not a target exclusion.

### Producer and consumer findings before reuse

The historical Go producer `go/cmd/sdk-go-producer/main.go`, read with SHA-256
`7e3d0ecd856be85d10aaca6e21ea803689d574331f260d07e2d53146feba4feb`,
deletes `outputRoot` at line 120 before tool and input validation. Its request
check establishes argument equality, but does not establish that the output is
disjoint from the source, authority, request, or filesystem root. Do not run
this producer until destination containment and overlap checks are repaired and
verified against adversarial paths. Its `samePath` also compares case-insensitively
on every platform, which cannot establish path identity on case-sensitive hosts.
At lines 146-170, `validation/v1/options.proto` can enter generation by file
existence without the family-input digest checks. The accepted exporter must
attest every transitive generator input, including custom options, before reuse.
These are source findings; no destructive invocation was attempted.

The isolated `tools/sdk-generator/backends/go` tooling repairs this staging boundary
without connecting the historical facade to the new SDK interface. It requires
a fresh disjoint output, validates before creating it, and removes the duplicate
options generation. Focused Windows Go 1.27.2 tests pass for output admission,
input/tool failure, lexical input escape and unattested options. The symlink
escape and request-alias tests are skipped because the sandbox lacks Windows
symlink privilege. Exact tool versions, absolute tool paths, duplicate source
rejection and per-family generated-binding presence also have focused controls.
Review controls additionally stage only digest-approved source snapshots,
exercise a successful run through a subprocess test double and verify output
and receipt hashes. A real protoc 28.3 control rejects an unlisted import and
accepts it once attested. All twelve focused tests pass outside the Windows
sandbox, including the two symlink controls; no generator correctness or SDK
runtime claim follows from the test double.
This evidence covers staging only; plugin generation, installed packages and
the accepted Rust binding interface still need qualification. Local execution
is now authorized; the earlier host-grant hold has been superseded.

The historical JVM consumer
`jvm/src/test/java/dev/acyclic/transport/RpcScenarioEvidenceTest.java`, read with
SHA-256 `a54f1c2149e37f411e4f5b4f3593bb763cd746250964f69378ee10ce2e70add7`,
selects an in-process server when `ACYCLIC_FIXTURE_ENDPOINT` is absent. That
server returns empty generated responses (lines 179-204); this can exercise
dispatch and marshalling but cannot establish canonical Rust service behavior.
The remote branch strips the URL scheme and calls `usePlaintext()` (lines
162-163), so it cannot establish TLS qualification even with an HTTPS URL.
Keep in-process dispatch, Rust-backed remote semantics, and TLS evidence as
separate gates. Reusing its scenario count as complete semantic coverage would
exceed the consumer's actual scope.

Supply executable snippets and scoped results to the docs owner with exact Rust
source, package version, package hash, snippet source hash, runtime/compiler and
observed output. Preserve the docs owner's TypeScript receipt interface without
claiming its six execution results for another language.
