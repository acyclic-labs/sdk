
## Dirty current source receipt (2026-10-07)

Candidate commit: `40bcdfa458d9b61a5568458808025c55698d904d`.

Current `typescript/packages/actors/src/client.ts` is dirty and has Git blob hash `a6b0d9d431fdbe4bfe477cafda4191009afe83bf` (SHA-256 `999FD805403616A73F9D5535244BDE19B702ED4F39BCAB7F557CBAEEE586D137`). It now evicts the shared pending connection before aborting the underlying controller when the final waiter releases. The delayed-old-rejection identity probe passes:

```text
{"connects":2,"second":"resolved","third":"reused-after-old-rejection"}
```

The two-waiter noninterference probe also passes with `connects:1`, first cancelled, second still pending until its own signal aborts. The earlier reconnect failure receipt is therefore stale for this dirty source state.

Generated artifact hashes at this receipt:

- `dist/client.js` Git blob `32ac192e1fbdafc1b5e1b4140f6978140f40b70f`, SHA-256 `8BAF5698F1EEEA185A4D68FDBD4A3CA5CCDE7D07C92F81351E2D5ABE59BBE74D`.
- WASM JS Git blob `2c0253d38b4e5f6e8295a11c41463a8fa96f4346`, SHA-256 `19ABC506688080A83A5837F2F3F99F531D00495C66A43D073F9239394F739DE4`.
- Native loader Git blob `75b90614f1c81af0712ae09b2d1bfbcddbd2d34f`, SHA-256 `ACC55F0DC140554378FAD3AA4CA7AD53796D86F1FF4F9199A17D8AC519428138`.

The source loader now compares the thrown missing-module identity to the exact `generated/native/binding.cjs` URL after path normalization. The generated `dist/client.js` must be rebuilt from this source before publication; its hash is recorded separately above.

## Installed archive receipt after CA/identity rebuild

A fresh archive was created from the current package and extracted into `installed-current-2`.

- Archive SHA-256: `FEC05A28A9A17CF9C6D941EDEC92C5B0192690C1B97B534C794013C7F8E8CF34`
- `dist/client.js`: `435F78915D0CEDCC28E50CCD601B0CAB908D43D32CCF5404942A63CD54198E86`
- WASM JS: `4E0AC74CC33EFD2E7BE247ACBA728A7F4330EBB2972DE91839B5149E7C1DD6C2`
- WASM binary: `19ABC506688080A83A5837F2F3C99F531D00495C66A43D073F9239394F739DE4`
- native loader: `ACC55F0DC140554378FAD3AA4CA7AD53796D86F1FF4F9199A17D8AC519428138`
- Windows native binary: `15D76AEA7FE03BD6557BC0E9310B6F5073D8B3969512679EB4AC9755CEA81D3E`

The installed archive's public `ActorsClient` with `caCertificate` connected to the local TLS gRPC server and completed all eight operations. `client.transport` was `undefined`; the native binding's JS prototype still omits `transport`, so Rust `#[napi]` exposure remains required.

Exact loader identity tests against the installed archive produced:

```text
own-loader: transport=wasm
foreign-binding-same-basename: Cannot find module 'C:\foreign\generated\native\binding.cjs'
foreign-node-same-basename: Cannot find module 'C:\foreign\index.win32-x64-msvc.node'
```

The own-loader miss falls back; same-basename foreign dependencies propagate their errors. No hand-authored semantic predicate was added; validation remains in generated Rust/WASM/native code.

Fresh headless Chrome runs against the same current `dist`/WASM hashes produced:

```text
{"transport":"grpc-web","operations":8,"invalidCurrentHeadRejected":false}
{"rejected":true,"abortReason":"Actors operation cancelled","preRejected":true,"stallRejected":true,"stallReason":"Actors operation cancelled","afterAbort":{"requests":1,"active":0,"aborted":1},"afterPre":{"requests":1,"active":0,"aborted":1}}
```

The all-eight page intentionally reports `invalidCurrentHeadRejected:false` because this focused page omits the invalid call; the abort page independently verifies Rust cancellation and pre-abort no-network behavior.

## Final current-main archive receipt (`84a508449ea247297a0170db67377dd65a1baa41`)

The owner committed metadata-driven native artifact selection after the prior receipt. Current source Git blob is `b4c3364a4b1e1f49dc29999092d596139fbe0ccd`, SHA-256 `9D7F06854B56892069214B8CC930524F37908F0E89B74D5B7B7160B2EEEDBD73`.

Fresh package archive and installed extraction (`installed-current-3`):

- archive SHA-256 `53BA1AF0BFCF435C98630AAA9D952BE2C6EEA2C7012DB2CE8398A4EECC27862C`
- `dist/client.js`: `0F092E1CE74FEC0A810C1E0ED11842C05EAD817279F16E6FE1C6CA56F2C9FE44`
- WASM JS: `4E0AC74CC33EFD2E7BE247ACBA728A7F4330EBB2972DE91839B5149E7C1DD6C2`
- WASM binary: `19ABC506688080A83A5837F2F3C99F531D00495C66A43D073F9239394F739DE4`
- native loader: `ACC55F0DC140554378FAD3AA4CA7AD53796D86F1FF4F9199A17D8AC519428138`
- Windows native binary: `3C65F0544E6E6D7A6A521577225CBEF97802FC1ED32BAC4F9F1D726C4DA32FEB`
- native target metadata: `E9BBE2F92A2BEF617A68D90C7FEC749EBD8EA12ACFF144EC6C19F21C45AB26C3`

The installed archive's normal public `ActorsClient({ caCertificate })` completed all eight native gRPC operations against the trusted local TLS fixture. It reported `transport=undefined`; the generated NAPI prototype still omits JavaScript `transport`, so Rust must expose that method with NAPI metadata.

Exact loader identity tests against this final archive remain correct:

```text
own-loader: transport=wasm
foreign-binding-same-basename: Cannot find module 'C:\foreign\generated\native\binding.cjs'
foreign-node-same-basename: Cannot find module 'C:\foreign\index.win32-x64-msvc.node'
```
Fresh browser runs against final `dist/client.js` `0F092E1C…` and WASM hashes above:

```text
{"transport":"grpc-web","operations":8,"invalidCurrentHeadRejected":false}
{"rejected":true,"abortReason":"Actors operation cancelled","preRejected":true,"stallRejected":true,"stallReason":"Actors operation cancelled","afterAbort":{"requests":1,"active":0,"aborted":1},"afterPre":{"requests":1,"active":0,"aborted":1}}
```

## Final getter and cross-platform receipt (`799214a1ecf8ac67998a3a3de61653081cbbd573`, 2026-10-07)

The NAPI transport getter is now present in source (`rust/crates/actors-napi/src/lib.rs`, blob `665c3ac422dd4ca35a61447274b091499fe82be4`) and generated artifacts. A fresh package archive was built from this tree:

- archive SHA-256: `FE45438B0D5549A5937DEB639A53DD10F3391DC3D34FC84D21968079842C7D2D`
- installed `dist/client.js`: `0F092E1CE74FEC0A810C1E0ED11842C05EAD817279F16E6FE1C6CA56F2C9FE44`
- installed WASM JS: `4E0AC74CC33EFD2E7BE247ACBA728A7F4330EBB2972DE91839B5149E7C1DD6C2`
- installed WASM binary: `B1DA0C5B6F8BF0FAF48695D070B1FD89C9F83EF15A35711C0D748DC06694B5CE`
- installed native loader: `ACC55F0DC140554378FAD3AA4CA7AD53796D86F1FF4F9199A17D8AC519428138`
- installed Windows native binary: `9536544FA8CD887B3C1D38DA11BE206C40DEA5989937DA56CDEF23584A09DA90`
- installed native metadata: `AD34F9AA989F75FBC25239AE4D8C06967F6523946879AAA193EDB75BC6EC02D0`

The installed archive's normal public native client, with the trusted local CA, reported `transport=grpc` and completed all eight operations. The generated NAPI prototype exposes `transport` with a function getter.

Real platform checks used the same final archive without feature flags or injected bindings:

- WSL Ubuntu 2 / Linux: `platform=linux`, then `error=fetch failed`, `cause=not implemented... yet...`; metadata had no Linux artifact, so the wrapper selected shipped WASM before evaluating the native loader.
- `ssh ivar` macOS 15.6 arm64: `platform=darwin`, then the same WASM file-fetch error; metadata had no Darwin artifact, so the wrapper selected WASM before native.

Loader identity adversarial checks:

- own URI-escaped binding URL (`file:///Q:/.../uri%20own/.../binding.cjs`) fell through to `error=fetch failed` (WASM).
- foreign POSIX `/tmp/Foreign/generated/native/binding.cjs` propagated unchanged.

An adversarial metadata fixture containing only `index.linux-x64-musl.node` on real WSL glibc 2.35 entered the native loader and returned `Cannot find native binding`. `hasNativeArtifactForRuntime` currently accepts any `index.linux-${arch}-*.node` suffix without checking libc flavor. A musl-only package on glibc (or gnu-only on musl) can therefore bypass WASM and fail in the generated loader; metadata selection should check the runtime libc/artifact flavor or preserve a loader fallback.

## b32089283d693aed6cb196a50116c1090c77917c final archive and semantic roundtrip receipt (2026-10-07)

The owner snapshot is `b32089283d693aed6cb196a50116c1090c77917c`; current dirty generated artifacts were packed from source client blob `c78ecc4a73e858cc9260ff9aba03947895b3bc46` and NAPI source blob `665c3ac422dd4ca35a61447274b091499fe82be4`.

- archive SHA-256: `A0C0B8E6A1563265F6B0CF5BDF1EB0D16DC395AABFDE46258C7BDA9D4C7C30E9`
- `dist/client.js`: `3AD4513C9F59195AF3904736F3575B993131C6737767E296EEEFF7900F03EBB9`
- Windows native binary: `BCED81A17E326322C1863141DCF7CB1BAE72482FFACF138F52DD18AF036B86A5`
- WASM binary: `B1DA0C5B6F8BF0FAF48695D070B1FD89C9F83EF15A35711C0D748DC06694B5CE`
- native target metadata: `067D23268F213D51290D7A65BF735257DA109EEBEA00C17B4EC8254F88DCA76A5`

Installed final archive results:

- WSL Ubuntu 2/Linux: `transport=grpc-web`; all eight operations passed against a real WSL gRPC-Web server.
- WSL in-flight cancellation: `{"transport":"grpc-web","rejected":true,"reason":"Actors operation cancelled","preRejected":true,"afterAbort":{"requests":9,"active":0,"aborted":1},"afterPre":{"requests":9,"active":0,"aborted":1}}`.
- `ssh ivar` macOS 15.6 arm64: `transport=grpc-web` with the normal public client and no injected binding.
- libc metadata mismatch on real WSL glibc 2.35: metadata containing only `index.linux-x64-musl.node` selected WASM (`transport=grpc-web`); metadata containing matching GNU filename with no file entered native loading and returned `Cannot find native binding` as intended.

Semantic response/request fixture used final installed WASM and Rust-owned wire schemas, with non-empty nested observations and optional presence:

- populated actor response retained 32-byte digest, nested subscription, `failedCursor=99n`, and `checkpointUnixMillis=1234n`;
- second response preserved absent optional scalar fields as `undefined` while retaining nested records;
- third response normalized absent optional actor message to `actor:null`;
- request capture observed both `{case:"cursor", value:"0"}` and `{case:"currentHead", value:true}` oneof variants.

The installed consumer type fixture compiled cleanly with `@ts-expect-error` checks for `currentHead: false` and mutations of recursively readonly response values. Reconnect probes on the current source all pass:

```text
{"connects":2,"second":"still_pending","secondSignalAborted":false}
{"connects":1,"first":"Actors operation cancelled","second":"still_pending","secondSignalAborted":false}
{"connects":2,"second":"resolved","third":"reused-after-old-rejection"}
```

The b320 package includes `dist/generated/actors-service.{js,d.ts}`; `ActorsClient` installs its methods from `ActorsService.methods` via `installActorsMethods`, rather than maintaining a second eight-method facade. The semantic roundtrip and type-negative fixtures above were rerun against `installed-current-5` from the b320 archive after this generated facade was present.

## Latest descriptor-facade commit receipt (`77997805785e8a61f1bf7580ca273e992caee740`)

The final working tree advanced from b320 to commit `77997805785e8a61f1bf7580ca273e992caee740`, which moves the Actors client facade to generated `ActorsService.methods` (`installActorsMethods`). The archive rebuilt from this exact package is byte-identical to the b320 generated package (`A0C0B8E6A1563265F6B0CF5BDF1EB0D16DC395AABFDE46258C7BDA9D4C7C30E9`) and includes `dist/generated/actors-service.{js,d.ts}`. Installed-current-5 reran the Linux transport/all-eight/abort checks, semantic response roundtrip, and type-negative fixture successfully; macOS arm64 again reported `transport=grpc-web`.
