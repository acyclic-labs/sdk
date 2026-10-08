import { readFileSync } from "node:fs";
import { join } from "node:path";

// Packages are published separately, so each ships its own observer helper.
// Actors owns the source; every other package receives a generated copy.
const source = "typescript/packages/actors/src/observe.ts";
const packages = ["filesystem", "harness", "inference", "machines", "objects", "stream", "workers"];

export function render(_stdout, root) {
  const content = `// @generated from ${source} by scripts/generate-observe.mjs; do not edit.\n\n${readFileSync(join(root, source), "utf8")}`;
  return Object.fromEntries(packages.map(name => [`typescript/packages/${name}/src/observe.ts`, content]));
}
