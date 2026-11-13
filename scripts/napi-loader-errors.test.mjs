import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { writeJsBinding } from "@napi-rs/cli";

test("installed NAPI generator retains native loading causes with an unpatched control", async t => {
  const directory = mkdtempSync(join(tmpdir(), "sdk-napi-loader-causes-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  // Use the installed producer's public API, without compiling a Rust crate.
  // The emitted binding executes real native-load failures in the child.
  await writeJsBinding({
    platform: true, idents: ["probe"], format: "commonjs", jsBinding: "generated.cjs",
    binaryName: "sdk-native-error-probe", packageName: "@acyclic-labs/native-error-probe",
    version: "0.0.0", outputDir: directory,
  });
  const source = readFileSync(join(directory, "generated.cjs"), "utf8");
  const constructor = "new AggregateError(loadErrors,";
  assert.ok(source.includes(constructor), "installed generator lacks the retained-cause patch");
  const probe = `
    const assert = require("node:assert/strict");
    let failure;
    try { require(process.argv[1]); } catch (error) { failure = error; }
    assert.ok(failure instanceof AggregateError, "NAPI loader lost individual causes");
    assert.ok(failure.errors.length > 0, "NAPI loader lost its concrete load failures");
    assert.ok(failure.errors.every(error => error instanceof Error && error.message.length > 0));
    assert.ok(failure.errors.some(error => error.code === "MODULE_NOT_FOUND" && error.message.includes("missing.node")));
  `;
  const env = { ...process.env, NODE_PATH: "", NAPI_RS_NATIVE_LIBRARY_PATH: join(directory, "missing.node") };
  delete env.NAPI_RS_FORCE_WASI;
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
