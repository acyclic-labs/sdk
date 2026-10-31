// A native pipe producer for transport conformance, not an agent/model host.
// IPC controls only the test schedule; JSON-RPC uses the real original pipes.
import { createInterface } from "node:readline";

createInterface({ input: process.stdin }).on("line", line => {
  const request = JSON.parse(line);
  if (request.method === "initialize") {
    process.stdin.pause();
    process.send({ phase: "initialize", request });
  } else if (request.method === "tools/call") {
    process.stdin.pause();
    process.stdout.end(JSON.stringify({ jsonrpc: "2.0", id: request.id,
      result: { received: request.params.arguments.text.length } }) + "\n");
  }
});
process.on("message", command => {
  if (command === "initialize") process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: 0,
    result: { protocolVersion: "2025-06-18" } }) + "\n");
  else if (command === "resume") process.stdin.resume();
  else if (command === "exit") process.exit(0);
});
