import assert from "node:assert/strict";
import test from "node:test";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { generationInvocation } from "./rust-sdk-generation.mjs";

const options = {
  repositoryRoot: resolve("/frozen/sdk"), callerDirectory: resolve("/caller"),
  temporaryRoot: resolve("/temporary"), environment: {},
};
const value = (plan, option) => plan.args[plan.args.lastIndexOf(option) + 1];
const contains = (parent, child) => {
  const path = relative(parent, child);
  return path === "" || (!path.startsWith("..") && !isAbsolute(path));
};

test("default artifacts and compiler cache are disjoint from the frozen source", () => {
  const plan = generationInvocation("generate", [], options);
  const source = value(plan, "--source-root");
  for (const destination of [value(plan, "--output"), plan.options.env.CARGO_TARGET_DIR]) {
    assert.equal(contains(source, destination), false);
    assert.equal(contains(destination, source), false);
  }
  assert.equal(plan.options.cwd, source);
  assert.equal(value(plan, "--manifest-path"), join(source, "rust/crates/sdk-generation/Cargo.toml"));
  assert.equal(value(plan, "--bin"), "sdk-generation");
});

test("generate and drift share defaults while independent checkouts have separate artifacts", () => {
  const generated = generationInvocation("generate", [], options);
  const drift = generationInvocation("drift", [], options);
  const other = generationInvocation("generate", ["--source-root", "other"], options);
  assert.equal(value(generated, "--output"), value(drift, "--output"));
  assert.notEqual(value(generated, "--output"), value(other, "--output"));
});

test("explicit caller paths and pinned runtime/cache are preserved", () => {
  const environment = { SDK_CARGO: "pinned-cargo", CARGO_TARGET_DIR: "owned-cache", KEEP: "value" };
  const plan = generationInvocation("check", ["--source-root", "snapshot", "--output", "packages"], { ...options, environment });
  assert.equal(plan.program, "pinned-cargo");
  assert.equal(value(plan, "--source-root"), resolve(options.callerDirectory, "snapshot"));
  assert.equal(value(plan, "--output"), resolve(options.callerDirectory, "packages"));
  assert.equal(plan.options.env.CARGO_TARGET_DIR, "owned-cache");
  assert.equal(plan.options.env.KEEP, "value");
  assert.equal(environment.CARGO_TARGET_DIR, "owned-cache");
});

test("missing path values fail before a compiler or generator starts", () => {
  for (const args of [["--source-root"], ["--output"], ["--source-root", "--output", "x"]]) {
    assert.throws(() => generationInvocation("generate", args, options), /requires a path/);
  }
});
