import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { inventoryHost, inventoryRuntime } from "../src/inventory.mjs";
import { sha256 } from "../../../shared/authority.mjs";
import { fixture } from "./fixtures/qualification.mjs";

test("runtime inventory records every regular file and changes with source bytes", t => {
  const f = fixture(t), first = inventoryRuntime(f.runtimeRoot);
  assert.deepEqual(first, JSON.parse(f.runtimeBytes));
  writeFileSync(join(f.runtimeRoot, "include/runtime.hpp"), "changed");
  assert.notEqual(first.files["include/runtime.hpp"], inventoryRuntime(f.runtimeRoot).files["include/runtime.hpp"]);
});

test("host inventory closes include/library/build-tool roots and rejects unsafe coordinates", t => {
  const f = fixture(t), host = JSON.parse(f.hostBytes), environment = { ...host.environment, tools: Object.fromEntries(Object.entries(host.tools).map(([name, pin]) => [name, pin.path])) };
  const actual = inventoryHost(environment);
  assert.deepEqual(actual.roots, host.roots);
  assert.deepEqual(actual.tools, host.tools);
  assert.equal(actual.tools["cl.exe"].sha256, sha256(readFileSync(f.tools["cl.exe"].path)));
  assert.throws(() => inventoryHost({ ...environment, include: ["../escape"] }), /absolute/);
  assert.throws(() => inventoryHost({ ...environment, tools: { ...environment.tools, extra: "extra" } }), /environment inventory differs/);
});
