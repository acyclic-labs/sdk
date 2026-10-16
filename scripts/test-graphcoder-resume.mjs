#!/usr/bin/env node
// Kill an admitted stock turn during model preparation, then replay its request.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { isAbsolute } from "node:path";
import { fixture } from "./graphcoder-fixture.mjs";
const [binary] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary));
const host = await fixture();
host.holdCount("resume after kill");
const operation = "00000000-0000-0000-0000-000000000077";
const args = [...host.args, "--operation", operation, "-p", "resume after kill"];
const children = [];
const start = () => {
  const child = spawn(binary, args, { windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
  children.push(child);
  let stdout = "", stderr = "", pending = "", acceptClaim;
  const claimed = new Promise(resolve => { acceptClaim = resolve; });
  child.stdout.on("data", bytes => {
    stdout += bytes; pending += bytes;
    for (let newline; (newline = pending.indexOf("\n")) !== -1;) {
      const line = pending.slice(0, newline); pending = pending.slice(newline + 1);
      const change = JSON.parse(line);
      if (!Array.isArray(change) && [change.file, ...change.files].filter(Boolean)
          .some(file => file.key.Session?.file.ExecutionClaim === operation)) acceptClaim();
    }
  });
  child.stderr.on("data", bytes => { stderr += bytes; });
  const exit = new Promise((resolve, reject) => { child.on("error", reject); child.on("close", (code, signal) => resolve({ code, signal, stdout, stderr })); });
  return { child, exit, claimed };
};
const timeout = setTimeout(() => { for (const child of children) child.kill("SIGKILL"); }, 8000);
let failure;
try {
  const first = start();
  await Promise.race([Promise.all([host.counting, first.claimed]), first.exit.then(result => { throw new Error(`turn exited before preparation: ${JSON.stringify(result)}`); })]);
  assert.equal(host.calls(), 0, "model must not have been dispatched before kill");
  first.child.kill("SIGKILL");
  const killed = await first.exit;
  assert.ok(killed.signal || killed.code !== 0, "first process was not killed");
  const initial = killed.stdout.trim().split("\n").filter(Boolean).map(JSON.parse);
  assert.ok(initial.slice(1).flatMap(change => [change.file, ...change.files].filter(Boolean)).some(file => file.key.Session?.file.ExecutionClaim === operation), "no durable execution claim before kill");
  host.completeCount();
  const resumed = await start().exit;
  assert.equal(resumed.code, 0, resumed.stderr);
  const records = resumed.stdout.trim().split("\n").map(JSON.parse);
  assert.ok(records.slice(1).flatMap(change => [change.file, ...change.files].filter(Boolean)).some(file => file.key.Session?.file.ExecutionResult === operation && JSON.parse(Buffer.from(file.bytes).toString()).completion === "completed"), "restarted task did not complete");
  assert.equal(host.calls(), 1, "resume must dispatch the retained turn exactly once");
} catch (error) { failure = error; }
finally {
  clearTimeout(timeout);
  for (const child of children) child.kill("SIGKILL");
  try { await host.close(); }
  catch (cleanup) { throw failure ? new AggregateError([failure, cleanup], "resume qualification and cleanup failed") : cleanup; }
}
if (failure) throw failure;
