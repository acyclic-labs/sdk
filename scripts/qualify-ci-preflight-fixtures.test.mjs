import assert from "node:assert/strict";
import { join } from "node:path";
import test from "node:test";
import { artifact, expectedGate, modelResult, verifyGate, verifyModelCalls } from "./qualify-ci-preflight-fixtures.mjs";
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

test("every omitted build or model invocation fails the production-derived oracle", () => {
  const plans = qualificationGatePlans();
  const calls = plans.flatMap(plan => plan.tests.map(selected => [plan.args, selected.args]));
  verifyModelCalls(plans, calls);
  for (const [index, plan] of plans.entries()) {
    const withoutBuild = plans.filter(other => JSON.stringify(other.args) !== JSON.stringify(plan.args));
    assert.throws(() => artifact(withoutBuild, plan.args, "/fixtures"), /unplanned model build/);
    assert.throws(() => artifact(plans, [...plan.args, "--unplanned"], "/fixtures"), /unplanned model build/);
    assert.throws(() => modelResult(plan, ["unplanned::model", "--exact"]), /unplanned model invocation/);
  }
  for (const index of calls.keys()) {
    assert.throws(() => verifyModelCalls(plans, calls.filter((_, at) => at !== index)));
    assert.throws(() => verifyModelCalls(plans, [...calls.slice(0, index), calls[index], ...calls.slice(index)]));
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

test("a new production step is covered and every gate deviation is rejected", () => {
  const plans = qualificationGatePlans();
  const calls = plans.flatMap(plan => plan.tests.map(selected => [plan.args, selected.args]));
  const log = (expected, models = calls) => [expected.map(([command]) => command), expected.map(([, flags]) => flags), models];
  verifyGate(plans, ...log(expectedGate(plans)));
  verifyGate(plans, ...log(expectedGate(plans, "-Cbase")), "-Cbase");
  gateQualifications.push({ args: ["--e2e-progress", "--all-features"] });
  let extended;
  try { extended = qualificationGatePlans(); } finally { gateQualifications.pop(); }
  const extendedCalls = extended.flatMap(plan => plan.tests.map(selected => [plan.args, selected.args]));
  verifyGate(extended, ...log(expectedGate(extended), extendedCalls));
  assert.throws(() => verifyGate(extended, ...log(expectedGate(plans))));
  const expected = expectedGate(plans);
  for (const index of expected.keys()) {
    const flagged = expected.map(([command, flags], at) => [command, at === index ? `${flags} --cfg added`.trim() : flags]);
    assert.throws(() => verifyGate(plans, ...log(flagged)), undefined, `flag on ${expected[index][0]}`);
    const extra = [...expected.slice(0, index), ["check --locked -p added", ""], ...expected.slice(index)];
    assert.throws(() => verifyGate(plans, ...log(extra)), undefined, `command before ${expected[index][0]}`);
    assert.throws(() => verifyGate(plans, ...log(expected.filter((_, at) => at !== index))));
  }
});
