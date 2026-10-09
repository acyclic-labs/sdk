# .NET producer tooling

This backend generates C# messages and gRPC stubs from canonical Rust descriptor
sets with maintained protoc and Grpc.Tools binaries. Rust manifest admission is
shared with Java in `../../shared/authority.mjs`.

```text
dotnet/
  src/                  generation and installed qualification runners
  tests/                offline admission and runner controls
    fixtures/consumer/  installed descriptor, wire and type controls
  templates/package/    project, SDK pin, dependency lock and package README
  toolchains/           compiler, plugin, packer and dependency archive pins
```

From the repository root:

```sh
node tools/sdk-generator/backends/dotnet/src/generate.mjs \
  --source-root /immutable-source --authority /rust-export \
  --protoc /tools/protoc --grpc-csharp /tools/grpc_csharp_plugin \
  --output /new-package
```

The immutable source supplies `LICENSE` and `NOTICE`. The Rust authority manifest
attests every source and supplied descriptor with exact digests. Generation uses
only verified descriptor snapshots, with no source include root. Descriptor sets
must contain the complete import closure, including attested imports without
their own descriptor declaration. Every family must produce C# source and output
collisions fail. The output must be absent and disjoint from protected inputs.
Failed generation retains partial output; temporary snapshots are removed.
Input directories and their parents must not be concurrently replaced.

Protoc must report `libprotoc 28.3` and match the host's SHA-256 in
`../../shared/protoc.json` before execution. Those pins are computed from the
official Maven Central artifacts and checked against their published checksums.
The C# gRPC plugin must match the host pin
from the official Grpc.Tools 2.71.0 package. The receipt records tool, authority,
input, generator and output hashes. The generator downloads nothing.

The generated net8.0 project pins SDK 8.0.425, protobuf 3.31.1 and gRPC 2.71.0.
Its lock file pins transitive dependencies. Explicit path mapping and disabled
ambient Git/SourceLink lookup prevent the generator checkout's revision from
entering generated assembly metadata.

Run lightweight controls without a .NET SDK or downloads:

```sh
node --test --test-concurrency=1 tools/sdk-generator/backends/dotnet/tests/generate.test.mjs tools/sdk-generator/backends/dotnet/tests/qualify.test.mjs
```

Actual installed qualification currently runs on Windows with .NET SDK 8.0.425
and the pinned NuGet.CommandLine 7.9.0 executable. Prepare the owned dependency
cache once with a normal online locked restore:

```sh
dotnet restore /new-package/Acyclic.Sdk.Transport.csproj \
  --locked-mode --disable-parallel --packages /prepared-cache
node tools/sdk-generator/backends/dotnet/src/qualify.mjs \
  --package /new-package --authority /rust-export \
  --dotnet /sdk-8.0.425/dotnet.exe --nuget /nuget-7.9.0/NuGet.exe \
  --cache /prepared-cache --output /new-qualification
```

The qualifier copies only the five pinned dependency archives into a local feed,
checks raw SHA-512 values against the official NuGet catalog pins, then performs
a locked restore into a fresh cache. NuGet also verifies the lock's content hashes;
those differ from raw archive hashes for repository-signed packages. Existing
extracted dependencies and SDK packages in the prepared cache are not reused.
The prepared cache must remain exclusively owned during admission.

The runner builds with one worker, excludes inherited MSBuild settings and uses
an explicit local-only NuGet configuration. NuGet 7.9 packs the generated nuspec
with deterministic mode and a fixed timestamp, as documented in the
[maintained pack command](https://learn.microsoft.com/en-us/nuget/reference/cli-reference/cli-ref-pack).
A fresh consumer restores that exact archive, verifies installed archive and
assembly identities, and checks loaded SDK assembly bytes. Each invalid byte or
integer assignment is compiled independently and must fail with its intended
CS0029 error. A receipt is written only after all controls pass. It records tools,
dependencies, installed artifacts, controls and logs. Partial output is retained
on failure. Routine CI runs neither the .NET SDK nor actual installed qualification.

The installed controls cover Actors, Workers and Stream descriptor equality,
bytes, unsigned integer bounds, optional-zero presence, oneof and exact gRPC
method shapes. Descriptor comparison retains all API fields and other unknown
fields, excluding source comments and Buf file image metadata tag 8042.
Oneof controls exercise both branches, verify that switching clears the previous
branch, check the active branch after serialization and test clearing the target.
These are transport binding checks. Remaining families, other consumer platforms,
Rust-backed remote RPC, TLS/authentication, cancellation/recovery and embedded
runtime qualification remain outstanding.

The accepted foundation source `5f13157414a5f48425007ffd957ea7d09611e5fe`
passes these installed controls on Windows x64. Two clean runs through this
reusable generator and qualifier produce the same NuGet SHA-256:
`e8160d1010c6805166e1974bfef35d3cf3ee080ececc087c52c63bca86fd9da5`.
