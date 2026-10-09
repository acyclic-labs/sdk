# SDK generator

Language generation tooling lives here, separately from the installable SDK
packages and the repository's CI and release scripts.

```text
tools/sdk-generator/
  README.md
  shared/               Rust manifest admission and maintained compiler pins
  backends/
    go/                 Go producer, module and focused tests
    java/               Java producer and Java/Kotlin/Scala consumer qualification
    dotnet/             C# producer, pinned package and installed controls
    ruby/               Ruby producer, pinned gem and installed controls
```

Each backend consumes the canonical Rust exports through maintained generators.
Add a backend directory when it contains executable tooling; keep generated SDK
source and package artifacts in their language package or requested output path.
Do not check tool downloads, caches or generation output into this directory.

Java, .NET and Ruby use the same layout within each backend:

```text
<backend>/
  README.md
  src/                  executable generation and qualification code
  tests/                lightweight offline tests
    fixtures/           installed-consumer source controls
  templates/package/    files copied into generated packages
  toolchains/           maintained version and checksum pins
```

Go keeps its Go source and tests together at the module root and consumer
fixtures in `testdata/`, following Go's package conventions. Its Node qualifier
lives in `src/` with offline tests in `tests/`.

Generated packages, qualification receipts, downloads and caches belong in
explicit output directories outside this source tree. Consult each backend's
README for installed qualification commands.

From the repository root:

```sh
go -C tools/sdk-generator/backends/go test -p=1 -parallel=1 ./...
go -C tools/sdk-generator/backends/go run . --help
node --test tools/sdk-generator/backends/java/tests/generate.test.mjs
```

The shared Rust/TypeScript generation entrypoints currently remain in `scripts/`
and `rust/crates/proto-codegen/`.
