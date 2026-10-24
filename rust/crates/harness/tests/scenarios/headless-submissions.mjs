#!/usr/bin/env node
// Real CLI submissions over the production provider adapter, with fixture settings.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { isAbsolute, join } from "node:path";
import { fixture } from "../../../../../scripts/graphcoder-fixture.mjs";

const [binary, scenario] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary), "absolute executable required");
assert.ok(["kill-mid-turn", "same-text-twice", "retry-jsonl"].includes(scenario));
const host = await fixture();
const children = [];
const exits = [];
const prompt = "repeat this exact submission text";
const started = performance.now();
const deadline = setTimeout(() => {
  for (const child of children) if (child.exitCode === null) child.kill("SIGKILL");
}, 8000);

function start(extra, input = "") {
  const child = spawn(binary, [...host.args, ...extra], {
    windowsHide: true, stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, CODEX_HOME: host.settings, HOME: host.settings,
      USERPROFILE: host.settings, LOCALAPPDATA: join(host.settings, "AppData", "Local"),
      APPDATA: join(host.settings, "AppData", "Roaming"), XDG_DATA_HOME: host.settings,
      OPENAI_API_KEY: "", GRAPHCODER_MODEL: "" },
  });
  children.push(child);
  let stdout = "", stderr = "";
  child.stdout.on("data", bytes => {
    stdout += bytes;
  });
  child.stderr.on("data", bytes => { stderr += bytes; });
  child.stdin.end(input);
  const exit = new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
  exits.push(exit);
  return { child, exit };
}

function completed(result) {
  assert.equal(result.code, 0, result.stderr + result.stdout);
  const frames = result.stdout.trim().split("\n").map(JSON.parse);
  const operation = frames[0][0].TaskSession.session;
  const files = [...frames[0][1].files,
    ...frames.slice(1).flatMap(change => [change.file, ...change.files].filter(Boolean))];
  const outcomes = files.filter(file => file.key.Session?.file.ExecutionResult === operation);
  assert.equal(JSON.parse(outcomes.at(-1).bytes.text).completion, "completed");
  return operation;
}

try {
  if (scenario === "kill-mid-turn") {
    host.hold();
    const first = start(["-p", prompt]);
    await Promise.race([host.responding, first.exit.then(result => {
      throw new Error(`exited before provider dispatch: ${JSON.stringify(result)}`);
    })]);
    first.child.kill("SIGKILL");
    const killed = await first.exit;
    assert.ok(killed.signal || killed.code !== 0, "first process was killed mid-turn");
    const oldOperation = JSON.parse(killed.stdout.split("\n")[0])[0].TaskSession.session;
    host.complete();
    const newOperation = completed(await start(["-p", prompt]).exit);
    assert.notEqual(newOperation, oldOperation, "a new invocation is a new submission");
    assert.equal(host.calls(), 2, "re-entered text must dispatch a new model turn");
  } else if (scenario === "same-text-twice") {
    const first = completed(await start(["-p", prompt]).exit);
    const second = completed(await start(["-p", prompt]).exit);
    assert.notEqual(first, second, "identical text must have independent identities");
    assert.equal(host.calls(), 2);
  } else {
    const operation = randomUUID();
    const request = JSON.stringify({
      key: { Session: { session: operation, file: { Request: operation } } }, channel: null,
      bytes: { text: JSON.stringify({ kind: "prompt", text: prompt, source: null, mentions: [] }) },
    }) + "\n";
    const args = ["--input-format", "jsonl", "--operation", operation];
    assert.equal(completed(await start(args, request + request).exit), operation);
    assert.equal(completed(await start(args, request).exit), operation);
    assert.equal(completed(await start(args).exit), operation);
    assert.equal(host.calls(), 1, "outbox retransmission and explicit resume must deduplicate");
  }
  console.log(`${scenario}: passed in ${(performance.now() - started).toFixed(0)} ms`);
} finally {
  clearTimeout(deadline);
  for (const child of children) if (child.exitCode === null) child.kill("SIGKILL");
  await Promise.allSettled(exits);
  await host.close();
}
