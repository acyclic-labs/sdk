#!/usr/bin/env node

// Deliberately faulty host bridge used by the installed transport qualification
// driver. It never invokes a model or touches a workspace.

import { createInterface } from "node:readline";
import { spawn } from "node:child_process";

const mode = process.argv[2];
if (!["malformed", "unmatched", "cancel", "descendant"].includes(mode)) process.exit(2);
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
lines.on("line", line => {
  let request;
  try { request = JSON.parse(line); }
  catch { return; }
  if (mode === "malformed") {
    process.stdout.write("not-json\n");
    return;
  }
  if (mode === "descendant") {
    const detached = process.platform === "win32";
    const descendant = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
      detached,
      stdio: ["ignore", "inherit", "inherit"],
      windowsHide: true,
    });
    process.stdout.write(`${JSON.stringify({ request_id: request.request_id, ok: true, result: { descendant_pid: descendant.pid } })}\n`, () => process.exit(0));
    return;
  }
  const response = JSON.stringify({ request_id: mode === "unmatched" ? "wrong-id" : request.request_id, ok: true, result: { items: [] } });
  if (mode === "cancel") setTimeout(() => process.stdout.write(`${response}\n`), 150);
  else process.stdout.write(`${response}\n`);
});
