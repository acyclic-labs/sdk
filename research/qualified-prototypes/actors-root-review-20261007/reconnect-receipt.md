
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
