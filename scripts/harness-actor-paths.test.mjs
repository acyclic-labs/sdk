import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import test from "node:test";
import { workspacePath } from "../tools/harness-actor-bundle.mjs";

test("owner paths resolve against the workspace rather than the invocation directory", () => {
  const workspace = resolve(tmpdir(), "harness-actor-workspace");
  assert.equal(workspacePath(workspace, "owner.json"), join(workspace, "owner.json"));
  const absolute = join(tmpdir(), "owner.json");
  assert.equal(workspacePath(workspace, absolute), absolute);
  assert.ok(isAbsolute(workspacePath(workspace, "artifact/output")));
});
