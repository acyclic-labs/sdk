
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
