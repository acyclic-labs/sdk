import assert from "node:assert/strict";
import { join, relative, resolve } from "node:path";
import test from "node:test";
import { runInNewContext } from "node:vm";
import { declarationTarget } from "./filesystem-napi-types.mjs";

function check(factory) {
  const root = resolve("fixture source");
  for (const value of [undefined, "target", "target with spaces", resolve("owned-job-a/target"), resolve("owned-job-b/target")]) {
    const environment = value === undefined ? {} : { CARGO_TARGET_DIR: value };
    const owner = resolve(root, value ?? "target");
    assert.equal(relative(owner, factory(root, environment)), "napi", "declarations escaped their target owner");
  }
  assert.notEqual(factory(root, { CARGO_TARGET_DIR: resolve("owned-job-a/target") }),
    factory(root, { CARGO_TARGET_DIR: resolve("owned-job-b/target") }), "independent jobs must not share a declaration target");
}

test("declaration generation stays beneath each actual Cargo target owner", () => {
  check(declarationTarget);
  const production = declarationTarget.toString();
  const boundary = 'return join(resolve(root, environment.CARGO_TARGET_DIR ?? "target"), "napi");';
  assert.ok(production.includes(boundary));
  const former = production.replace(boundary, 'return `${resolve(root, environment.CARGO_TARGET_DIR ?? "target")}-napi`;');
  assert.throws(() => check(runInNewContext(`(${former})`, { join, resolve })), /declarations escaped/);
});
