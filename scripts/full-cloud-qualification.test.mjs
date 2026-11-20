import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("the full-cloud wrapper refuses an absent producer before executing a case", () => {
  const wrapper = fileURLToPath(new URL("./full-cloud-test.sh", import.meta.url));
  const env = { ...process.env };
  delete env.CLOUD_SOURCE;
  const result = spawnSync("bash", [wrapper], { env, encoding: "utf8" });
  assert.ifError(result.error);
  assert.equal(result.status, 2, result.stderr);
  assert.match(result.stderr, /STEP_FAILED admit-full-cloud: CLOUD_SOURCE must name an absolute path; recovery:/);
  assert.match(result.stderr, /bazel test .*\/\/rust\/crates\/filesystem:main_full-cloud/);
});
