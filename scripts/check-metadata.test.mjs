import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { retiredRuntimeFindings } from "./retired-runtime.mjs";

const root = new URL("../", import.meta.url);
const inventories = ["languages/package-names.json", "release/npm-packages.json", "release/cargo-crates.json"];

test("retired runtime findings enumerate each inventory and remain stable when an earlier finding disappears", () => {
  const documents = new Map(inventories.map(path => [path, JSON.parse(readFileSync(new URL(path, root), "utf8"))]));
  assert.deepEqual(retiredRuntimeFindings(documents), []);
  // Apply every retired spelling to real inventories without changing their scope.
  for (const term of ["python", "pytest", "boto3", "botocore", "fixture.py", "fixture.pyi", "fixture.pyc", "fixture.pyo"]) {
    const retired = new Map([...documents].map(([path, document]) => [path, { document, reference: term.toUpperCase() }]));
    const findings = retiredRuntimeFindings(retired);
    assert.equal(findings.length, inventories.length);
    for (const [index, path] of inventories.entries()) assert.ok(findings[index].startsWith(`step=retired-runtime SDK inventory ${path};`));
    retired.set(inventories[0], documents.get(inventories[0]));
    assert.deepEqual(retiredRuntimeFindings(retired), findings.slice(1));
  }
  documents.set("scripts/tool.py", { runtime: "python" });
  documents.set("provenance/manifest.json", { source: "legacy.py" });
  assert.deepEqual(retiredRuntimeFindings(documents), []);
});
