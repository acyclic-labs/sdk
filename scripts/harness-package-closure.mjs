import { workspacePackages } from "./workspace-model.mjs";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function harnessPackageClosure(selected = "acyclic-harness") {
  const order = JSON.parse(readFileSync(join(root, "release", "cargo-crates.json"), "utf8"));
  return packageClosure(workspacePackages(root), order, selected);
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

if (import.meta.main) {
  for (const item of harnessPackageClosure(process.argv[2])) process.stdout.write(`${item.name}\t${item.version}\n`);
}
