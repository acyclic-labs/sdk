#!/usr/bin/env node

// Deliberately faulty host bridge used by the installed transport qualification
// driver. It never invokes a model or touches a workspace.

import { createInterface } from "node:readline";

const mode = process.argv[2];
if (!["malformed", "unmatched", "cancel"].includes(mode)) process.exit(2);
const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
lines.on("line", line => {
  let request;
  try { request = JSON.parse(line); }
  catch { return; }
  if (mode === "malformed") {
    process.stdout.write("not-json\n");
    return;
  }
  const response = JSON.stringify({ request_id: mode === "unmatched" ? "wrong-id" : request.request_id, ok: true, result: { items: [] } });
  if (mode === "cancel") setTimeout(() => process.stdout.write(`${response}\n`), 150);
  else process.stdout.write(`${response}\n`);
});
