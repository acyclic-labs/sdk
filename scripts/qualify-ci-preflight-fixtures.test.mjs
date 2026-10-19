import assert from "node:assert/strict";
import { join } from "node:path";
import test from "node:test";
import { artifact, modelResult, verifyModelResults } from "./qualify-ci-preflight-fixtures.mjs";
import { gateQualifications, qualificationGatePlans, qualificationPlan } from "./qualify-mcp-loom.mjs";

test("every production gate step supplies its artifact and complete test selection", () => {
  const before = process.env.RUSTFLAGS;
  const plans = qualificationGatePlans();
  assert.equal(plans.length, gateQualifications.length);
  assert.equal(process.env.RUSTFLAGS, before);
  for (const [index, plan] of plans.entries()) {
    const built = artifact(plans, plan.args, "/fixtures");
    assert.equal(built.target.name, plan.target);
    const firstBuild = plans.findIndex(other => JSON.stringify(other.args) === JSON.stringify(plan.args));
    assert.equal(built.executable, join("/fixtures", `model-${firstBuild}`));
    assert.ok(plan.tests.length > 0);
    for (const selected of plan.tests) {
      assert.match(modelResult(plan, selected.args), /test result: ok\. [1-9][0-9]* passed;/);
    }
  }
  // These two different target shapes were absent from the old fake cargo.
  assert.ok(plans.some(plan => plan.target === "e2e-progress-model" && plan.tests.length === 4));
  assert.ok(plans.some(plan => plan.target === "acyclic_native_runtime" && plan.tests.length === 13));
  assert.match(plans.find(plan => plan.target === "memory-accounting-model").rustflags, /-Dwarnings/);
  const sessionFs = plans.find(plan => plan.target === "session_fs_model");
  assert.equal(sessionFs.tests.length, 7);
  assert.match(sessionFs.rustflags, /--cfg acyclic_stream_loom/);
});

test("omitted or duplicated required model results fail, independent of scheduling order", () => {
  const plans = qualificationGatePlans();
  const results = plans.flatMap(plan => plan.tests.map(selected => [plan.target, selected.args]));
  verifyModelResults(plans, results);
  verifyModelResults(plans, [...results].reverse());
  for (const plan of plans) {
    const withoutBuild = plans.filter(other => JSON.stringify(other.args) !== JSON.stringify(plan.args));
    assert.throws(() => artifact(withoutBuild, plan.args, "/fixtures"), /unplanned model build/);
    assert.throws(() => modelResult(plan, ["unplanned::model", "--exact"]), /unplanned model invocation/);
  }
  for (const index of results.keys()) {
    assert.throws(() => verifyModelResults(plans, results.filter((_, at) => at !== index)));
    assert.throws(() => verifyModelResults(plans, [...results, results[index]]));
  }
});

test("new qualification steps need no fake cargo mapping", () => {
  const plans = qualificationGatePlans();
  // New names and a library owned by another crate use the actual command planner.
  for (const args of [["--e2e-progress", "--all-features"], ["--fs-binding", "--features", "new-feature"]]) {
    const added = qualificationPlan(args);
    const extended = [...plans, added];
    assert.equal(artifact(extended, added.args, "/fixtures").target.name, added.target);
    assert.throws(() => artifact(plans, added.args, "/fixtures"), /unplanned model build/);
  }
});

test("an unknown model step or flag in the production plan fails closed", () => {
  for (const args of [["--new-model"], ["--timer", "--all-features"], ["--guest", "--features", "x"],
    ["--e2e-progress", "--bogus"], ["--fs-binding", "--features"], ["--features", "--all-features"], ["--release"]]) {
    assert.throws(() => qualificationPlan(args), /unknown qualification argument/, JSON.stringify(args));
  }
  for (const step of [{ args: ["--new-model"] }, { args: ["--memory-accounting", "--all-features"] }]) {
    gateQualifications.push(step);
    try { assert.throws(() => qualificationGatePlans(), /unknown qualification argument/); }
    finally { gateQualifications.pop(); }
  }
});

