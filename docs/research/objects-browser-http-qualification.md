# Objects browser HTTP/WASM qualification

This report records a source-bound browser consumer check against the Objects Rust/WASM fixture. The check was run from the isolated SDK worktree at revision `fae648874c2b33b19b2892bee51865ac2328f774` on 2026-10-04. The package `dist` and generated WASM files were treated as the installed package inputs; no TypeScript product files were changed by this review.

## Executed checks

The existing Rust-backed HTTP conformance module passed with Node:

```text
node --test test/v2-conformance.mjs
✔ Objects v2 Rust WASM provider covers every operation and atomic retry semantics
✔ Objects v2 HTTP covers every operation, frame boundaries, errors and authentication
✔ Objects v2 rejects invalid requests before HTTP and bounds response allocations
✔ Objects v2 HTTP cancels oversized downloads at the header before pulling the body
✔ Objects v2 validates remote metadata, ranges, framing and terminal errors
5 passed, 0 failed
```

The package’s configured Bun tests also passed six transport and policy tests:

```text
bun test test
6 pass, 0 fail, 21 expect() calls
```

A disposable consumer imported `dist/browser.js` by file URL and called the browser entrypoint:

```text
node C:\Users\varun\AppData\Local\Temp\objects-browser-installed-consumer.mjs
{"fromEnv":"browser-http","rustFixture":true,"lifecycle":"create-put-get","grpcRejected":true,"callerCancellation":true,"endpoint":"http://127.0.0.1:<ephemeral-port>"}
```

The disposable server was the same Rust-owned boundary used by the repository fixture: `ObjectsV2Memory`, `objects_v2_http_type`, `decode_objects_v2_json`, `encode_objects_v2_json`, and the generated Objects protobuf schemas. It accepted the package’s POST framing, invoked Rust for `buckets/create`, `objects/put`, and `objects/get`, and returned the Rust-encoded responses. The consumer therefore exercised the packaged `browser.js` → `fromEnv` → `ensureObjectsWasm` → `HttpObjectsV2` path against a Rust implementation rather than a Node-only memory provider.

The consumer additionally verified that browser `fromEnv({ transport: "grpc" })` fails closed, and that an abort signal supplied to `get` reaches the HTTP fetch operation. The repository fixture independently verifies bounded response cancellation after a GET header. No retry or endpoint recovery was claimed: the browser policy exposes one HTTP endpoint, and recovery is not part of this browser transport contract. HTTPS URLs are accepted by the Rust endpoint validator, but private-CA TLS qualification requires a browser trust configuration; the local evidence here uses loopback HTTP as the allowed browser fixture transport.

The extended disposable consumer ran the same packaged entrypoint and Rust memory fixture with a 135,000-byte upload and download, then retried the same mutation identity with different bytes, used a wrong bearer token, and forwarded a caller abort:

```text
node C:\Users\varun\AppData\Local\Temp\objects-browser-installed-consumer-full.mjs
{"fromEnv":"browser-http","rustFixture":true,"uploadBytes":135000,"downloadBytes":135000,"idempotencyConflict":true,"authentication":true,"callerCancellation":true}
```

The temporary consumer was removed after the run. The conflict and authentication assertions used the Rust fixture's encoded `ErrorDetail` responses, so these are installed browser-package observations rather than constructor-only checks.

## Source and package identities

SHA-256 hashes captured before report authoring:

```text
F390F7649D9738C1DD87FF3A1A132339484E852319AFD82C3C5F6B581A5D49E6  typescript/packages/objects/src/v2-client-browser.ts
10387EA67BB3EFC40A029173EA98E15AF3A9130A71704B9D25AA144A111C70C4  typescript/packages/objects/src/v2-http.ts
43B6A11742E8E47C525531B74D9C2E6D7658FA5178C234AF68B87B0089B41987  typescript/packages/objects/dist/browser.js
E47981A1FEEA6D394328BA358DA69A2904B13F4441122982FE22F5CE502AEBC5  typescript/packages/objects/dist/v2-client-browser.js
268E963C52868561B186A7D840BD7799201181BF60A5DD30CA9F4CD0DD2A66B7  typescript/packages/objects/dist/v2-http.js
134A3E262637103B147FBC2141BF9594C770CC20D6528DB73B2544FAD0500E4E  typescript/packages/objects/generated/wasm/acyclic_objects_wasm_bg.wasm
3D728481699AAFDDFA43DD635175A002599A32F72E6DC534851DFC8EE3F7724C  typescript/packages/objects/test/v2-conformance.mjs
3041A10BD4613F8F8508A8271F860F7E265DEC6091E3BD897DD707764F41F41D  rust/crates/objects/src/v2/http_tests.rs
```

The temporary consumer is intentionally outside the repository and was removed after the run. Its output above is retained as the review receipt; the canonical repository conformance source and Rust HTTP fixture remain the source of record.
