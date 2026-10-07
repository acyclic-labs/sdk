# Actors semantic TypeScript consumer fixture

This isolated fixture is an external-consumer check for the installed
`@acyclic-labs/actors` package. It deliberately imports the Rust-generated
semantic declarations through the package root's `semantic` namespace and exercises all
eight `ActorsClient` operations.

`positive.ts` checks that every operation accepts the generated semantic
request and returns its corresponding generated semantic result. `negative.ts`
uses `@ts-expect-error` for plain strings, raw byte arrays, zero-valued
positive integers, a false or missing `SubscriptionStart.currentHead`, and
mutation of semantic values. Those checks are intentionally compile-time
checks; runtime Rust validation remains a separate concern.

`runtime-roundtrip.mjs` injects only the documented binding seam, encodes all
eight semantic requests through the package's Buf boundary, checks the
current-head oneof and invocation headers/body, and decodes every response.
It is a wire mapping test; it does not replace a Rust native/WASM fixture.

Run these files from a temporary consumer after building and packing the
Actors package:

```powershell
npm install --ignore-scripts
npx tsc -p tsconfig-positive.json --pretty false
npx tsc -p tsconfig-negative.json --pretty false
node runtime-roundtrip.mjs
```

The fixture contains no production source or hand-authored RPC contract.
