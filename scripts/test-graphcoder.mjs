#!/usr/bin/env node
// Installed CLI contract: native snapshot/follow and request-file JSONL.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { isAbsolute } from "node:path";
import { fixture } from "./graphcoder-fixture.mjs";
const [binary] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary), "absolute installed binary required");
const deadline = AbortSignal.timeout(8000);
const host = await fixture();
async function run(args, input = "") {
  const child = spawn(binary, args, { windowsHide: true, stdio: ["pipe", "pipe", "pipe"], signal: deadline, killSignal: "SIGKILL" });
  let stdout = "", stderr = "";
  child.stdout.on("data", bytes => { stdout += bytes; });
  child.stderr.on("data", bytes => { stderr += bytes; });
  child.stdin.end(input);
  const code = await new Promise((resolve, reject) => { child.on("error", error => reject(new Error(`${error.message}\nstdout: ${stdout}\nstderr: ${stderr}`, { cause: error }))); child.on("exit", (code, signal) => signal ? reject(new Error(`CLI terminated by ${signal}\nstdout: ${stdout}\nstderr: ${stderr}`)) : resolve(code)); });
  return { code, stdout, stderr };
}
let failure;
try {
  const help = await run(["--help"]);
  assert.equal(help.code, 0);
  for (const option of ["--prompt", "--input-format", "--workspace", "--model"]) assert.ok(help.stdout.includes(option), option);
  for (const removed of ["--fixture", "--session", "--prepare-scenario"]) assert.notEqual((await run([removed, "unused"])).code, 0);
  const operation = "00000000-0000-0000-0000-000000000001";
  const prompt = await run([...host.args, "--operation", operation, "-p", "say hello"]);
  assert.equal(prompt.code, 0, prompt.stderr);
  const frames = prompt.stdout.trim().split("\n").map(line => JSON.parse(line));
  function outcomes(frames, operation) {
    assert.equal(frames[0][0].TaskSession.session, operation);
    assert.equal(frames[0][0].TaskSession.task, operation);
    const files = [...frames[0][1].files.map(file => ({ ...file, bytes: file.segments.flat() })),
      ...frames.slice(1).flatMap(change => [change.file, ...change.files].filter(Boolean))];
    return files.filter(file => file.key.Session?.file.ExecutionResult === operation)
      .map(file => JSON.parse(Buffer.from(file.bytes).toString("utf8")));
  }
  assert.equal(outcomes(frames, operation).at(-1).completion, "completed");
  assert.equal(outcomes(frames, operation).at(-1).ok.text, "graphcoder **test answer**");
  const resumed = await run([...host.args, "--input-format", "jsonl", "--operation", operation]);
  assert.equal(resumed.code, 0, resumed.stderr);
  assert.equal(outcomes(resumed.stdout.trim().split("\n").map(JSON.parse), operation).at(-1).completion, "completed");
  assert.equal(host.calls(), 1, "reopen must not dispatch the model again");
  const second = "00000000-0000-0000-0000-000000000002";
  const request = { key: { Session: { session: second, file: { Request: second } } }, channel: null,
    bytes: [...Buffer.from(JSON.stringify({ kind: "prompt", text: "say hello", source: null, mentions: [] }))] };
  const jsonl = await run([...host.args, "--input-format", "jsonl", "--operation", second], `${JSON.stringify(request)}\n`);
  assert.equal(jsonl.code, 0, jsonl.stderr);
  assert.equal(outcomes(jsonl.stdout.trim().split("\n").map(JSON.parse), second).at(-1).completion, "completed");
  assert.equal(host.calls(), 2, "JSONL EOF must finish exactly one admitted operation");
  const malformed = await run([...host.args, "--input-format", "jsonl", "--operation", operation], "not JSON\n");
  assert.notEqual(malformed.code, 0, "malformed request JSON must fail");
  assert.equal(host.calls(), 2, "malformed stdin must not reach the model");
  host.fail();
  assert.notEqual((await run([...host.args, "-p", "provider failure"])).code, 0);
} catch (error) { failure = error; }
finally {
  try { await host.close(); }
  catch (cleanup) { throw failure ? new AggregateError([failure, cleanup], "CLI qualification and cleanup failed") : cleanup; }
}
if (failure) throw failure;
