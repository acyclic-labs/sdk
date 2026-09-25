import assert from "node:assert/strict";
import test from "node:test";
import { parseNpmView, waitForPublishedExact } from "./publish-npm-packages.mjs";

test("npm view tolerates an unpublished or partially visible field", () => {
  assert.equal(parseNpmView({ status: 0, stdout: "", stderr: "" }, "package"), null);
  assert.equal(parseNpmView({ status: 1, stdout: "", stderr: "npm ERR! E404" }, "package"), null);
  assert.equal(parseNpmView({ status: 0, stdout: '"sha512-exact"', stderr: "" }, "package"), "sha512-exact");
  assert.throws(
    () => parseNpmView({ status: 1, stdout: "", stderr: "npm ERR! E500" }, "package"),
    /could not inspect package/,
  );
});

test("publication waits for exact bytes and latest to propagate", async () => {
  const integrities = [null, "sha512-exact", "sha512-exact"];
  const latest = ["0.1.4", "0.1.5"];
  let pauses = 0;
  await waitForPublishedExact("@acyclic-labs/harness", "0.1.5", "sha512-exact", {
    readIntegrity: () => integrities.shift(),
    readLatest: () => latest.shift(),
    pause: async () => { pauses += 1; },
    now: () => 0,
  });
  assert.equal(pauses, 2);
});

test("publication rejects mismatched bytes and bounded nonvisibility", async () => {
  await assert.rejects(
    waitForPublishedExact("package", "0.1.5", "sha512-exact", {
      readIntegrity: () => "sha512-other",
    }),
    /exists with different bytes/,
  );
  let time = 0;
  await assert.rejects(
    waitForPublishedExact("package", "0.1.5", "sha512-exact", {
      readIntegrity: () => null,
      pause: async () => { time = 10; },
      now: () => time,
      timeoutMs: 10,
    }),
    /did not become visible/,
  );
});
