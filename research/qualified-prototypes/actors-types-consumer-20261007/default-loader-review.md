# Default loader review

Review target: `rust-source-foundation/sdk`, HEAD `371bb4170e16aca973176b6756a261ee5add7297`.

The Node root facade is wired to the Rust N-API loader: `ActorsClient.connect`
checks for Node and imports `./native.js`; the installed-package checker also
verifies the archived NAPI-RS loader, the `./native` export, the root export,
all eight native result methods, structured failures, bigint precision, and
abort propagation.

The browser facade is wired to the generated WASM module and reports
`grpc-web`. The checked-in browser conformance runner still exercises
`HttpActorsClient` and the old HTTP fixture, so it does not qualify
`ActorsClient.connect`'s browser branch. A browser-context smoke test must
instantiate the root client, assert `transport === "grpc-web"`, and exercise
at least one operation through the generated WASM module.

The current package is not installable as a semantic TypeScript consumer:
`package.json` exports `./types` as `./dist/types.d.ts`, but that file and
`src/types.ts` are absent. `tsc -p typescript/packages/actors --noEmit`
therefore reports the missing `@acyclic-labs/actors/types` module and the
semantic shape assertions cannot pass. The isolated `positive.ts` and
`negative.ts` fixtures are ready to run after the attested Rust generation
bundle is staged.

The generated semantic declarations inspected from the qualified bundle also
use mutable object properties and `Array` collections. The negative fixture
therefore includes mutation expectations so the public immutability policy is
checked rather than inferred from runtime behavior.
