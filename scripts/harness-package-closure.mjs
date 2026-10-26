import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function harnessPackageClosure(selected = "acyclic-harness") {
  const result = spawnSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"], {
    cwd: root,
    encoding: "utf8",
  });
  if (result.status !== 0) throw new Error(result.stderr || "cargo metadata failed");
  const order = JSON.parse(readFileSync(join(root, "release", "cargo-crates.json"), "utf8"));
  return packageClosure(JSON.parse(result.stdout).packages, order, selected);
}

export function packageClosure(metadata, order, selected) {
  const packages = new Map(metadata.map(item => [item.name, item]));
  const publicCrates = new Set(order);
  const closure = new Set();
  const include = name => {
    if (closure.has(name)) return;
    const item = packages.get(name);
    if (!item || !publicCrates.has(name)) throw new Error(`missing public package dependency: ${name}`);
    closure.add(name);
    for (const dependency of item.dependencies) {
      const local = packages.get(dependency.name);
      if (dependency.path && local && (publicCrates.has(dependency.name) ||
          (dependency.kind !== "dev" && local.publish?.length !== 0))) {
        include(dependency.name);
      }
    }
  };
  include(selected);
  return order.filter(name => closure.has(name)).map(name => ({ name, version: packages.get(name).version }));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  for (const item of harnessPackageClosure(process.argv[2])) process.stdout.write(`${item.name}\t${item.version}\n`);
}
