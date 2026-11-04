// Each Cargo invocation selects exactly one workspace member, so another
// member's features cannot make its tests compile. Each worker owns a stable
// target directory so Cargo and target-store leases permit actual overlap.
import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { workspaceDefaults } from "./workspace-defaults.mjs";
import { runCommand } from "./run-command.mjs";

export function workspacePackages(metadata) {
  return workspaceDefaults(metadata).map(pkg => pkg.name);
}

export async function checkCrateTests(packages, workers, target, run) {
  if (!Number.isSafeInteger(workers) || workers < 1) throw new Error("workers must be a positive integer");
  const failures = [];
  async function worker(index) {
    const buildDirectory = resolve(target, "crate-test-checks", String(index));
    // Stable assignment keeps a crate in the same warm cache on later runs.
    for (let position = index; position < packages.length; position += workers) {
      const name = packages[position];
      const args = ["check", "--locked", "--tests", "-p", name, "--target-dir", buildDirectory];
      if (await run(args, buildDirectory) !== 0) failures.push(name);
    }
  }
  await Promise.all(Array.from({ length: Math.min(workers, packages.length) }, (_, index) => worker(index)));
  return failures.sort();
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const packages = workspacePackages(JSON.parse(execFileSync("cargo",
    ["metadata", "--locked", "--no-deps", "--format-version", "1"], { encoding: "utf8" })));
  const target = process.env.CARGO_TARGET_DIR ?? "target";
  const failures = await checkCrateTests(packages, Number(process.argv[2] ?? 2), target, async (args, buildDirectory) => {
    process.stdout.write(`CARGO_BUILD_BUILD_DIR=${buildDirectory}\ncargo ${args.join(" ")}\n`);
    return runCommand("cargo", args, {
      env: { ...process.env, CARGO_BUILD_BUILD_DIR: buildDirectory },
    });
  });
  console.log(`Standalone test checks: ${packages.length} crates, ${failures.length} failures`);
  if (failures.length) {
    console.error(`Failed crates: ${failures.join(", ")}`);
    process.exitCode = 1;
  }
}
