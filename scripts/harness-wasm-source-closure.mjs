import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";

function collectFiles(root, directory) {
  const absolute = resolve(root, directory);
  if (!statSync(absolute).isDirectory()) return [directory.replaceAll("\\", "/")];
  return readdirSync(absolute, { withFileTypes: true }).flatMap(entry => {
    const child = `${directory}/${entry.name}`;
    return entry.isDirectory() ? collectFiles(root, child) : [child.replaceAll("\\", "/")];
  });
}

export function harnessWasmSourceClosure(root) {
  const files = new Set(["Cargo.lock", "Cargo.toml", "rust-toolchain.toml"]);
  const packages = ["rust/crates/harness"];
  const visited = new Set();
  while (packages.length > 0) {
    const directory = packages.pop();
    if (visited.has(directory)) continue;
    visited.add(directory);
    const manifest = join(directory, "Cargo.toml");
    if (!statSync(resolve(root, manifest)).isFile()) continue;
    for (const file of collectFiles(root, directory)) files.add(file);
    const source = readFileSync(resolve(root, manifest), "utf8");
    for (const match of source.matchAll(/path\s*=\s*["']([^"']+)["']/g)) {
      const dependencyDirectory = relative(root, resolve(root, dirname(manifest), match[1])).replaceAll("\\", "/");
      if (!dependencyDirectory.startsWith("rust/")) continue;
      if (statSync(resolve(root, dependencyDirectory), { throwIfNoEntry: false })?.isDirectory()) {
        packages.push(dependencyDirectory);
      }
    }
  }
  return [...files].sort();
}
