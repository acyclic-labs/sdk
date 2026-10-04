#!/usr/bin/env node

// Deliberately small PTY fixture for runner failure tests. It is only enabled
// when the runner receives an explicit test entrypoint and allow flag.
import { createInterface } from "node:readline";

const mode = process.env.GRAPHCODER_PTY_FIXTURE_MODE ?? "success";
const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
const success = JSON.stringify({
  ok: true,
  value: {
    selectedSession: { id: "fixture-session" },
    activity: [],
    messages: [],
    approvals: [],
    changes: [],
    changeBody: { unifiedDiff: "fixture" },
    fileBody: { mediaType: "text/plain", bytes: [102] },
    writeback: { applied: true },
    state: "cancelled",
  },
});

process.stdout.write("graphcoder> ");
input.on("line", line => {
  if (line === "quit") {
    if (mode === "bridge-crash") process.exit(17);
    process.stdout.write('{"ok":true,"exited":true}\n');
    process.exit(0);
  }
  if (mode === "bridge-crash") process.exit(17);
  if (mode === "timeout" || mode === "missing") return;
  if (mode === "truncated") {
    process.stdout.write('{"ok":true');
    return;
  }
  if (mode === "wrong") {
    process.stdout.write('{"ok":true,"value":{"unexpected":true}}\n');
  } else {
    process.stdout.write(`${success}\n`);
  }
  process.stdout.write("graphcoder> ");
});
