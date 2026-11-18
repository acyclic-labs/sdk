// Each Cargo invocation selects exactly one workspace member, so another
// member's features cannot make its tests compile. Each worker owns a stable
// target directory so Cargo and target-store leases permit actual overlap.
import { repositoryRoot as root } from "./policy-core.mjs";
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { workspaceDefaults } from "./workspace-defaults.mjs";
import { workspaceProfileMetadata } from "./workspace-model.mjs";
import { runCommand } from "./run-command.mjs";


export function workspacePackages(metadata) {
  return workspaceDefaults(metadata).map(pkg => pkg.name);
}

/** Every workspace member that exposes a `wasm` feature, so its browser build is checked too. */
export function wasmPackages(metadata) {
  const members = new Set(metadata.workspace_members);
  return metadata.packages
    .filter(pkg => members.has(pkg.id) && Object.hasOwn(pkg.features, "wasm"))
    .map(pkg => pkg.name)
    .sort();
}

export async function checkCrateTests(packages, workers, target, run, selection = ["--tests"]) {
  if (!Number.isSafeInteger(workers) || workers < 1) throw new Error("workers must be a positive integer");
  const failures = [];
  async function worker(index) {
    const buildDirectory = resolve(target, "crate-test-checks", String(index));
    // Stable assignment keeps a crate in the same warm cache on later runs.
    for (let position = index; position < packages.length; position += workers) {
      const name = packages[position];
      const args = ["check", "--locked", ...selection, "-p", name, "--target-dir", buildDirectory];
      if (await run(args, buildDirectory) !== 0) failures.push(name);
    }
  }
  await Promise.all(Array.from({ length: Math.min(workers, packages.length) }, (_, index) => worker(index)));
  return failures.sort();
}

if (import.meta.main) {
  const metadata = workspaceProfileMetadata(root, {workspaceOnly: true});
  const packages = workspacePackages(metadata);
  const wasm = wasmPackages(metadata);
  const target = process.env.CARGO_TARGET_DIR ?? "target";
  const workers = Number(process.argv[2] ?? 2);
  const run = async (args, buildDirectory) => {
    process.stdout.write(`CARGO_BUILD_BUILD_DIR=${buildDirectory}\ncargo ${args.join(" ")}\n`);
    return runCommand("cargo", args, {
      env: { ...process.env, CARGO_BUILD_BUILD_DIR: buildDirectory },
    });
  };
  const failures = await checkCrateTests(packages, workers, target, run);
  const wasmTarget = "wasm32-unknown-unknown";
  execFileSync("bash", [resolve(root, "scripts/ensure-rust-target.sh"), wasmTarget], { stdio: "inherit" });
  // The wasm members share one dependency graph. One target directory compiles
  // each dependency once; separate worker targets would compile it again per worker.
  const wasmFailures = await checkCrateTests(wasm, 1, target, run,
    ["--target", wasmTarget, "--features", "wasm"]);
  console.log(`Standalone test checks: ${packages.length} crates, ${failures.length} failures`);
  console.log(`Standalone wasm checks: ${wasm.length} crates, ${wasmFailures.length} failures`);
  const failed = [...failures, ...wasmFailures];
  if (failed.length) {
    console.error(`Failed crates: ${failed.join(", ")}`);
    process.exitCode = 1;
  }
}
