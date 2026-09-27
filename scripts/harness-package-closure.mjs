import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function harnessPackageClosure() {
  const result = spawnSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"], {
    cwd: root,
    encoding: "utf8",
  });
  if (result.status !== 0) throw new Error(result.stderr || "cargo metadata failed");
  const packages = new Map(JSON.parse(result.stdout).packages.map(item => [item.name, item]));
  const order = JSON.parse(readFileSync(join(root, "release", "cargo-crates.json"), "utf8"));
  const publicCrates = new Set(order);
  const closure = new Set();
  const include = name => {
    if (closure.has(name)) return;
    const item = packages.get(name);
    if (!item || !publicCrates.has(name)) throw new Error(`missing public Harness dependency: ${name}`);
    closure.add(name);
    for (const dependency of item.dependencies) {
      if (dependency.kind !== "dev" && publicCrates.has(dependency.name)) include(dependency.name);
    }
  };
  include("acyclic-harness");
  return order.filter(name => closure.has(name)).map(name => ({ name, version: packages.get(name).version }));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  for (const item of harnessPackageClosure()) process.stdout.write(`${item.name}\t${item.version}\n`);
}
