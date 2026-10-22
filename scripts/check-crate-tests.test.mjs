import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { checkCrateTests, workspacePackages } from "./check-crate-tests.mjs";

test("checks every workspace member alone, reusing one target with bounded workers", async () => {
  const packages = workspacePackages({
    workspace_members: ["c", "a", "b"],
    packages: [
      { id: "a", name: "first" }, { id: "external", name: "external" },
      { id: "b", name: "second" }, { id: "c", name: "third" },
    ],
  });
  assert.deepEqual(packages, ["first", "second", "third"]);
  let active = 0, peak = 0;
  const commands = [];
  const failures = await checkCrateTests(packages, 2, "/shared target", async args => {
    commands.push(args);
    peak = Math.max(peak, ++active);
    await new Promise(resolve => setImmediate(resolve));
    --active;
    return args.includes("second") ? 101 : 0;
  });
  assert.equal(peak, 2);
  assert.deepEqual(failures, ["second"]);
  assert.deepEqual(commands, packages.map(name =>
    ["check", "--locked", "--tests", "-p", name, "--target-dir", "/shared target"]));
});

test("feature unification cannot hide an individual failure", async () => {
  const run = async args => args.filter(arg => arg === "-p").length === 1 ? 101 : 0;
  assert.deepEqual(await checkCrateTests(["first", "second"], 2, "target", run), ["first", "second"]);
  // Negative control: combining the roots hides both failures in this fixture.
  assert.equal(await run(["check", "--tests", "-p", "first", "-p", "second"]), 0);
});

test("qualification gate runs the standalone checks and their scheduling control", () => {
  const source = readFileSync(new URL("./qualify-ci.sh", import.meta.url), "utf8").replaceAll("\r\n", "\n");
  const gate = source.slice(source.indexOf("  gate)"), source.indexOf("      ;;", source.indexOf("  gate)")));
  assert.match(gate, /node --test scripts\/check-crate-tests\.test\.mjs/);
  assert.match(gate, /node scripts\/check-crate-tests\.mjs 2/);
});
