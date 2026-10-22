// Each Cargo invocation selects exactly one workspace member, so another
// member's features cannot make its tests compile. Reuse the lane's target
// directory and Cargo's parallel dependency builds; bounded workers queue
// independent checks while Cargo coordinates access to the shared artifacts.
import { spawn, execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function workspacePackages(metadata) {
  const members = new Set(metadata.workspace_members);
  return metadata.packages.filter(pkg => members.has(pkg.id)).map(pkg => pkg.name).sort();
}

export async function checkCrateTests(packages, workers, target, run) {
  if (!Number.isSafeInteger(workers) || workers < 1) throw new Error("workers must be a positive integer");
  let next = 0;
  const failures = [];
  async function worker() {
    while (next < packages.length) {
      const name = packages[next++];
      const args = ["check", "--locked", "--tests", "-p", name, "--target-dir", target];
      if (await run(args) !== 0) failures.push(name);
    }
  }
  await Promise.all(Array.from({ length: Math.min(workers, packages.length) }, worker));
  return failures.sort();
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const packages = workspacePackages(JSON.parse(execFileSync("cargo",
    ["metadata", "--locked", "--no-deps", "--format-version", "1"], { encoding: "utf8" })));
  const target = process.env.CARGO_TARGET_DIR ?? "target";
  const failures = await checkCrateTests(packages, Number(process.argv[2] ?? 2), target, async args => {
    const child = spawn("cargo", args, { windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    child.stdout.on("data", chunk => { output += chunk; });
    child.stderr.on("data", chunk => { output += chunk; });
    const status = await new Promise((resolve, reject) => child.on("error", reject).on("close", resolve));
    process.stdout.write(`cargo ${args.join(" ")}\n${output}`);
    return status ?? 1;
  });
  console.log(`Standalone test checks: ${packages.length} crates, ${failures.length} failures`);
  if (failures.length) {
    console.error(`Failed crates: ${failures.join(", ")}`);
    process.exitCode = 1;
  }
}
