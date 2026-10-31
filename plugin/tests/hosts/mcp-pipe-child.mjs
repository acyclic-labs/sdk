// A native pipe producer for transport conformance.
// IPC controls only the test schedule; JSON-RPC uses the real original pipes.
import { createInterface } from "node:readline";

const mode = process.argv[2];
let requests = 0;
let completed = false;
createInterface({ input: process.stdin }).on("line", line => {
  if (completed) return;
  const request = JSON.parse(line);
  if (request.method === "initialize") {
    process.stdin.pause();
    process.send({ phase: "initialize", request });
  } else if (request.method === "tools/call") {
    requests++;
    if (mode !== "cancel") {
      process.stdin.pause();
      process.stdout.end(JSON.stringify({ jsonrpc: "2.0", id: request.id,
        result: { received: request.params.arguments.text.length } }) + "\n");
    } else process.send({ phase: "request", id: request.id, received: request.params.arguments.text.length });
  } else if (request.method === "notifications/cancelled") {
    completed = true;
    process.stdin.pause();
    process.send({ phase: "cancelled", id: request.params.requestId, requests });
    process.stdout.end();
  }
});
process.on("message", command => {
  if (command === "initialize") process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: 0,
    result: { protocolVersion: "2025-06-18" } }) + "\n");
  else if (command === "resume") process.stdin.resume();
  else if (command === "malformed") process.stdout.write("{malformed\n");
  else if (command === "eof") process.stdout.end();
  else if (command === "exit") process.exit(0);
});
