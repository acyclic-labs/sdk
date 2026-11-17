import assert from "node:assert/strict";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";

const root = new URL("../", import.meta.url).pathname;
const packages = join(root, "typescript/packages");

const sourceFiles = directory => readdirSync(directory, { recursive: true, withFileTypes: true })
  .filter(entry => entry.isFile() && /\.(?:ts|mjs)$/u.test(entry.name))
  .map(entry => join(entry.parentPath, entry.name));

test("stream code resolves wasm through its package's generated files", () => {
  for (const file of [...sourceFiles(join(packages, "stream/src")), ...sourceFiles(join(packages, "stream/test"))]) {
    assert.doesNotMatch(readFileSync(file, "utf8"), /["']@acyclic\/stream-wasm["']/u, file);
  }
});

test("package exports name committed source files", () => {
  for (const entry of readdirSync(packages, { withFileTypes: true }).filter(entry => entry.isDirectory())) {
    const directory = join(packages, entry.name);
    const manifest = JSON.parse(readFileSync(join(directory, "package.json"), "utf8"));
    const targets = [];
    const collect = value => {
      if (typeof value === "string") targets.push(value);
      else if (value && typeof value === "object") for (const child of Object.values(value)) collect(child);
    };
    collect(manifest.exports);
    for (const target of targets) {
      assert.ok(target.startsWith("./"), `${manifest.name} export ${target} is not relative`);
      if (target.startsWith("./src/") && target.endsWith(".ts")) {
        assert.ok(existsSync(join(directory, target.slice(2))), `${manifest.name} export ${target} does not exist`);
      }
    }
  }
});
