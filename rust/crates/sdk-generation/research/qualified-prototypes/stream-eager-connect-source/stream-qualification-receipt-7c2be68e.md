# Stream native qualification receipt: 7c2be68e

- Exact source checkout: `Q:/sdk/work/stream-main-port`
- Exact signed source revision: `7c2be68e75f966f0dccbe0cfef44e79f1195c9fc`
- Native source closure: `sha256:f1cd7e8afe02172fd8dfeb340c5632f1a6e0684ccbbb79c50914eb27a5dcf72d`
- Native target: `x86_64-pc-windows-msvc`
- Bundle/staged/archive-extracted/installed/loaded native artifact: `sha256:c7bc9317bc331b971804f3408316c103dfc6b16638d529a39a64935f049317f8`, 5,358,080 bytes
- Package archive: `Q:/sdk/work/stream-package-archive-7c2be68e/acyclic-labs-stream-0.2.0.tgz`
- Package archive SHA256: `5e8de23b53f61a8450038846a2a81f5a93b0518cab2cf1625517b6150c9dc24e`
- Installed consumer: `Q:/sdk/work/stream-installed-7c2be68e`

The bundle and staged package metadata checks passed. All four native copies matched byte-for-byte. The loaded native module path was the installed package artifact and had the same hash.

The regenerated WASM output is now part of the signed `7c2be68` source freeze. Source, staged package, archive extraction, and installed package all match at `sha256:b2f3c22ba3401dd1e363d8bab17ea758b5870ba25b15bb724abb773fa7772db8` (802,418 bytes).

The corrected installed cancellation harness passed:

```json
{"schema":"acyclic.stream.native.installed-connect-cancellation.v3","defaultEntry":"Stream.fromEnv","nativeGrpcAttempted":true,"stalledTlsAbortSettled":true,"acceptedSocketsClosed":true,"activeSocketCount":0,"elapsedMs":287,"socketClosedByAbort":true,"invalidBearerRejectedBeforeNetwork":true,"unhandledRejections":0}
```

Installed availability/default native selection/browser fallback checks passed. The raw logs are in this directory: `bundle-check.txt`, `package-check.txt`, `hashguard.txt`, `installed-cancel.txt`, `installed-default.txt`, `native-availability.txt`, `native-check.txt`, `browser-fallback.txt`, `wasm-hashguard.txt`, and `metadata.txt`.

The prior 5c package used the same native artifact but carried the freshly regenerated WASM before it was signed into source. That intermediate archive is superseded by this exact 7c package; use only this receipt for final provenance.
