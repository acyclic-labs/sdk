import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

const require = createRequire(import.meta.url);

test("installed NAPI generator retains native loading causes with an unpatched control", t => {
  const directory = mkdtempSync(join(tmpdir(), "sdk-napi-loader-causes-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  // Execute the installed producer's real bundled loader in isolation. No
  // native companion or optional platform package exists beside this copy.
  const entry = join(dirname(require.resolve("@napi-rs/cli/package.json")), "dist/index.cjs");
  const source = readFileSync(entry, "utf8");
  const constructor = "new AggregateError(loadErrors,";
  assert.ok(source.includes(constructor), "installed generator lacks the retained-cause patch");
  const probe = `
    const assert = require("node:assert/strict");
    let failure;
    try { require(process.argv[1]); } catch (error) { failure = error; }
    assert.ok(failure instanceof AggregateError, "NAPI loader lost individual causes");
    assert.ok(failure.errors.length > 0, "NAPI loader lost its concrete load failures");
    assert.ok(failure.errors.every(error => error instanceof Error && error.message.length > 0));
  `;
  const env = { ...process.env, NODE_PATH: "" };
  delete env.NAPI_RS_NATIVE_LIBRARY_PATH;
  const run = (name, bytes) => {
    const path = join(directory, name);
    writeFileSync(path, bytes);
    return spawnSync(process.execPath, ["-e", probe, path], {
      cwd: directory, env, encoding: "utf8", windowsHide: true,
    });
  };
  const retained = run("retained.cjs", source);
  assert.equal(retained.status, 0, retained.stderr);
  const omitted = run("unpatched.cjs", source.replaceAll(constructor, "new Error("));
  assert.notEqual(omitted.status, 0, "unpatched loader unexpectedly preserved individual causes");
  assert.match(omitted.stderr, /NAPI loader lost individual causes/);
});
