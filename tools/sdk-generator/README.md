# SDK generator

Language generation tooling lives here, separately from the installable SDK
packages and the repository's CI and release scripts.

```text
tools/sdk-generator/
  README.md
  shared/               Rust manifest admission and maintained compiler pins
  backends/
    go/                 Go producer, module and focused tests
    java/               Descriptor-based Java producer and installed controls
    dotnet/             C# producer, pinned package and installed controls
```

Each backend consumes the canonical Rust exports through maintained generators.
Add a backend directory when it contains executable tooling; keep generated SDK
source and package artifacts in their language package or requested output path.
Do not check tool downloads, caches or generation output into this directory.

From the repository root:

```sh
go -C tools/sdk-generator/backends/go test -p=1 -parallel=1 ./...
go -C tools/sdk-generator/backends/go run . --help
node --test tools/sdk-generator/backends/java/generate.test.mjs
```

The shared Rust/TypeScript generation entrypoints currently remain in `scripts/`
and `rust/crates/proto-codegen/`. The Go backend is staging tooling; its tests do
not establish generated SDK installation or runtime conformance.
