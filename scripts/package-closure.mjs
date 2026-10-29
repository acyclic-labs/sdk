import { globSync, readFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";

// Each dependency appears once, before its consumer; visited also breaks cycles.
export function packageClosure(root, selected) {
  const packages = new Set();
  const visited = new Set();
  const manifests = workspaceManifests(root);
  const names = new Map([...manifests].map(([name, manifest]) => [manifest.name, name]));
  function visit(name) {
    if (visited.has(name)) return;
    if (!/^[a-z][a-z-]*$/.test(name)) throw new Error("invalid package generation target");
    const directory = join(root, "typescript/packages", name);
    const config = JSON.parse(readFileSync(join(directory, "tsconfig.json"), "utf8"));
    visited.add(name);
    const manifest = manifests.get(name);
    for (const dependency of Object.keys({ ...manifest.dependencies, ...manifest.peerDependencies })) {
      if (names.has(dependency)) visit(names.get(dependency));
    }
    for (const reference of config.references ?? []) {
      if (!/^\.\.\/[a-z][a-z-]*$/.test(reference.path)) throw new Error("invalid package reference");
      visit(reference.path.slice(3));
    }
    packages.add(name);
  }
  if (selected) visit(selected);
  else for (const name of ["actors", "auth", "filesystem", "harness", "inference", "machines", "objects", "stream", "workers"]) visit(name);
  return packages;
}

export function workspaceManifests(root) {
  const { workspaces } = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
  return new Map(globSync(workspaces.map(pattern => `${pattern}/package.json`), { cwd: root })
    .map(path => [basename(dirname(path)), JSON.parse(readFileSync(join(root, path), "utf8"))]));
}
