import { execFileSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";

// Include every target and optional dependency: no workspace feature selection
// should make a package's test compilation wait for its own normal library.
export function devDependencyCycles(packages) {
  const byPath = new Map(packages.map(pkg => [resolve(pkg.manifest_path, ".."), pkg]));
  const dependency = dep => dep.path && byPath.get(resolve(dep.path));
  function pathBack(pkg, target, visited = new Set()) {
    if (pkg.id === target.id) return [pkg.name];
    if (visited.has(pkg.id)) return null;
    const seen = new Set([...visited, pkg.id]);
    for (const dep of pkg.dependencies.filter(dep => dep.kind !== "dev")) {
      const next = dependency(dep);
      const path = next && pathBack(next, target, seen);
      if (path) return [pkg.name, ...path];
    }
    return null;
  }
  const cycles = new Set();
  for (const pkg of packages) {
    for (const dep of pkg.dependencies.filter(dep => dep.kind === "dev")) {
      const next = dependency(dep);
      const path = next && pathBack(next, pkg);
      if (path) cycles.add([pkg.name, ...path].join(" -> "));
    }
  }
  return [...cycles].sort();
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const metadata = JSON.parse(execFileSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"], { encoding: "utf8" }));
  const members = new Set(metadata.workspace_members);
  const cycles = devDependencyCycles(metadata.packages.filter(pkg => members.has(pkg.id)));
  if (cycles.length) {
    console.error(`Workspace dev-dependency cycles:\n${cycles.join("\n")}`);
    process.exitCode = 1;
  } else {
    console.log(`No dev-dependency cycles in ${members.size} workspace packages.`);
  }
}
