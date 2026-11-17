import assert from "node:assert/strict";
import test from "node:test";
import { devDependencyCycles } from "./dev-dependency-cycles.mjs";

const edge = (pkg, kind = null) => ({ pkg, dep_kinds: [{ kind }] });

test("refuses a dev-dependency that reaches the crate itself", () => {
  const nodes = new Map([
    ["root", { deps: [edge("helper", "dev")] }],
    ["helper", { deps: [edge("root")] }],
  ]);
  assert.deepEqual(devDependencyCycles(new Set(["root"]), nodes), [["helper", "root"]]);
});

test("accepts a dev-dependency that stays outside the crate", () => {
  const nodes = new Map([
    ["root", { deps: [edge("helper", "dev")] }],
    ["helper", { deps: [edge("leaf")] }],
    ["leaf", { deps: [] }],
  ]);
  assert.deepEqual(devDependencyCycles(new Set(["root"]), nodes), []);
});

test("checks every platform graph", () => {
  const clean = new Map([
    ["root", { deps: [edge("helper", "dev")] }],
    ["helper", { deps: [] }],
  ]);
  const cyclic = new Map([
    ["root", { deps: [edge("helper", "dev")] }],
    ["helper", { deps: [edge("root")] }],
  ]);
  assert.deepEqual([clean, cyclic].flatMap(graph => devDependencyCycles(new Set(["root"]), graph)), [["helper", "root"]]);
});
