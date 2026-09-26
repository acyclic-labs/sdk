// Lints, with warnings denied, every crate feature set the SDK ships,
// documents, or builds against. The workspace-wide --all-features run covers
// the union; these are the reduced sets it cannot see. Clippy type-checks
// every target it lints, so a set that does not build fails here too.
//
// Libraries are linted in every such set. Their tests are linted wherever the
// set includes the in-memory backend the test suites run on: a Cargo test
// build unifies dev-dependency features, so only a library-only run proves
// that the library itself builds without them.
import { spawnSync } from "node:child_process";

const fs = ["-p", "acyclic-fs"];
const fsOnly = features => [...fs, "--no-default-features", ...(features ? ["--features", features] : [])];

// Library and tests.
const withTests = [
  // The crates.io default, which the plugin and the Node addon build on.
  fs,
  // The browser build and the Filesystem Harness adapter.
  fsOnly("memory"),
  // The local Filesystem Harness adapter.
  fsOnly("memory,local"),
  // The Windows lane's portable test set.
  fsOnly("local,memory,native-watch"),
  // Objects and Stream with and without their default gRPC client, as
  // acyclic-fs and the browser build use them, and with `local`.
  ["-p", "acyclic-objects"],
  ["-p", "acyclic-objects", "--no-default-features"],
  ["-p", "acyclic-objects", "--no-default-features", "--features", "local"],
  ["-p", "acyclic-stream"],
  ["-p", "acyclic-stream", "--no-default-features"],
  ["-p", "acyclic-stream", "--no-default-features", "--features", "local"],
  // Harness host runtime. Adapters are separate workspace crates and covered
  // by the workspace-wide all-features run.
  ["-p", "acyclic-harness"],
  // The conformance runner and its native-mount qualification binaries.
  ["-p", "acyclic-conformance"],
  ["-p", "acyclic-conformance", "--features", "local-runner"],
];

// Library only: each acyclic-fs capability on its own.
const libraryOnly = [
  fsOnly(),
  fsOnly("memory"),
  fsOnly("local"),
  fsOnly("native-watch"),
  fsOnly("native-mount"),
  fsOnly("s3-http"),
];

function clippy(args) {
  const command = ["clippy", ...args, "--locked", "--", "-D", "warnings"];
  process.stderr.write(`cargo ${command.join(" ")}\n`);
  const result = spawnSync("cargo", command, { stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}

for (const set of withTests) clippy([...set, "--all-targets"]);
for (const set of libraryOnly) clippy(set);
