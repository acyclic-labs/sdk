// Minimal stdio MCP backend reached through ordinary user configuration.
import { appendFileSync } from "node:fs";
import { createInterface } from "node:readline";
const [record] = process.argv.slice(2);
for await (const line of createInterface({ input: process.stdin })) {
  const request = JSON.parse(line);
  appendFileSync(record, JSON.stringify(request) + "\n");
  if (request.id === undefined) continue;
  let result;
  if (request.method === "initialize") result = {
    protocolVersion: request.params.protocolVersion, capabilities: { tools: {} },
    serverInfo: { name: "e2e-fixture", version: "1" },
  };
  else if (request.method === "tools/list") result = { tools: [{ name: "echo", description: "Echo the fixture argument", inputSchema: {
    type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false,
  } }] };
  else if (request.method === "tools/call") result = { content: [{ type: "text", text: `MCP_E2E_${request.params.arguments.text}` }], isError: false };
  else if (request.method === "ping") result = {};
  else {
    console.log(JSON.stringify({ jsonrpc: "2.0", id: request.id, error: { code: -32601, message: "unsupported fixture method" } }));
    continue;
  }
  console.log(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }));
}
