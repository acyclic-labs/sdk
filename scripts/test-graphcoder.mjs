#!/usr/bin/env node
// Installed headless presentation checks; no durable-runtime qualification.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
import { isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";

const [binary, ...extra] = process.argv.slice(2);
if (!binary || extra.length || !isAbsolute(binary) || !statSync(binary).isFile()) {
  throw new Error("usage: node scripts/test-graphcoder.mjs ABSOLUTE_INSTALLED_BINARY");
}
const digest = path => createHash("sha256").update(readFileSync(path)).digest("hex");
console.log(`binary=${binary}`);
console.log(`binary_sha256=${digest(binary)}`);
console.log(`suite_sha256=${digest(fileURLToPath(import.meta.url))}`);
// The installed binary receives no tool search path or environment credentials.
const env = { PATH: "" };
for (const key of ["SystemRoot", "WINDIR", "TEMP", "TMP"]) {
  if (process.env[key] !== undefined) env[key] = process.env[key];
}
function run(name, args, input, success) {
  const result = spawnSync(binary, args, {
    input, encoding: "utf8", env, windowsHide: true,
    timeout: 5_000, killSignal: "SIGKILL", maxBuffer: 1_048_576,
  });
  console.log(JSON.stringify({ case: name, argv: args, exit: result.status,
    signal: result.signal, stdout: result.stdout, stderr: result.stderr }));
  // Timeouts, launch failures and signal deaths never satisfy a negative case.
  if (result.error) throw result.error;
  assert.equal(result.signal, null, `${name}: terminated by signal`);
  assert.notEqual(result.status, null, `${name}: missing exit status`);
  assert.equal(result.status === 0, success, `${name}: unexpected exit`);
  return result;
}
const unavailableHost = run("default-unavailable", [], "", false);
assert.match(unavailableHost.stderr, /durable terminal host is unavailable/);
const args = ["--fixture", "wire", "--headless"];
const playback = run("explicit-playback", args, "/next\n/next\n/reset\n/next\n/quit\n", true);
assert.match(playback.stdout, /UI playback only, no durable effects/);
assert.equal(playback.stdout.split("activity ").length - 1, 2);
assert.equal(playback.stdout.split("end of fixture activity").length - 1, 1);
assert.match(playback.stdout, /fixture display reset; no session restored/);
assert.doesNotMatch(playback.stdout, /fixture> /);
const unavailableInput = run("input-not-queued", args, "hello\n/next\n", false);
assert.match(unavailableInput.stderr, /fixture input is not queued/);
assert.doesNotMatch(unavailableInput.stdout, /activity /);
const oversized = run("input-bound", args, "x".repeat(65_537) + "\n/next\n", false);
assert.match(oversized.stderr, /terminal input exceeds bounds/);
assert.doesNotMatch(oversized.stdout, /activity /);
