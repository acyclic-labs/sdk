// Lints, with warnings denied, every crate feature set the SDK ships,
// documents, or builds against. The workspace-wide --all-features run covers
// the union; these are the reduced sets it cannot see. Clippy type-checks
// every target it lints, so a set that does not build fails here too.
//
// Libraries are linted in every such set. Their tests are linted wherever the
// set includes the in-memory backend the test suites run on: a Cargo test
// build unifies dev-dependency features, so only a library-only run proves
// that the library itself builds without them.
import { spawn } from "node:child_process";

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
  // Inference without its gRPC host, as the browser build uses it.
  ["-p", "acyclic-inference", "--no-default-features"],
  // Harness host runtime alone, then each provider adapter feature alone.
  ["-p", "acyclic-harness"],
  ["-p", "acyclic-harness", "--features", "filesystem"],
  ["-p", "acyclic-harness", "--features", "filesystem-local"],
  ["-p", "acyclic-harness", "--features", "objects"],
  ["-p", "acyclic-harness", "--features", "machines"],
  ["-p", "acyclic-harness", "--features", "grpc"],
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

// Each set is its own Cargo invocation, so `clippy-feature-sets.mjs WORKERS`
// may lint several at once. Every extra worker gets its own target directory
// so Cargo's build lock never serializes them; a shared compiler cache still
// deduplicates their dependencies.
const workers = Number(process.argv[2] ?? 1);
const sets = [...withTests.map(set => [...set, "--all-targets"]), ...libraryOnly];
let failed = 0;

async function worker(index) {
  const target = `${process.env.CARGO_TARGET_DIR ?? "target"}-clippy-${index}`;
  const env = index === 0 ? process.env : { ...process.env, CARGO_TARGET_DIR: target };
  for (let set = sets.shift(); set && !failed; set = sets.shift()) {
    const command = ["clippy", ...set, "--locked", "--", "-D", "warnings"];
    const child = spawn("cargo", command, { env, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    child.stdout.on("data", chunk => (output += chunk));
    child.stderr.on("data", chunk => (output += chunk));
    const status = await new Promise((resolve, reject) => child.on("error", reject).on("close", resolve));
    process.stderr.write(`cargo ${command.join(" ")}\n${output}`);
    if (status !== 0) failed ||= status ?? 1;
  }
}

await Promise.all(Array.from({ length: workers }, (_, index) => worker(index)));
process.exitCode = failed;
