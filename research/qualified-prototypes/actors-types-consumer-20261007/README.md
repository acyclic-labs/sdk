# Actors semantic TypeScript consumer fixture

This isolated fixture is an external-consumer check for the installed
`@acyclic-labs/actors` package. It deliberately imports the Rust-generated
semantic declarations through the package `./types` export and exercises all
eight `ActorsClient` operations.

`positive.ts` checks that every operation accepts the generated semantic
request and returns its corresponding generated semantic result. `negative.ts`
uses `@ts-expect-error` for plain strings, raw byte arrays, zero-valued
positive integers, a false or missing `SubscriptionStart.currentHead`, and
mutation of semantic values. Those checks are intentionally compile-time
checks; runtime Rust validation remains a separate concern.

Run these files from a temporary consumer after building and packing the
Actors package:

```powershell
npm install --ignore-scripts
npx tsc -p tsconfig-positive.json --pretty false
npx tsc -p tsconfig-negative.json --pretty false
```

The fixture contains no production source or hand-authored RPC contract.
