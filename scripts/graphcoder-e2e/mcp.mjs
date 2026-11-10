// Minimal stdio MCP backend reached through ordinary user configuration.
import { appendFileSync } from "node:fs";
import { createInterface } from "node:readline";
const [record, optionsJson = "{}"] = process.argv.slice(2);
const { pages = 1, toolsPerPage = 1, descriptionBytes = 0 } = JSON.parse(optionsJson);
for await (const line of createInterface({ input: process.stdin })) {
  const request = JSON.parse(line);
  appendFileSync(record, JSON.stringify(request) + "\n");
  if (request.id === undefined) continue;
  let result;
  if (request.method === "initialize") result = {
    protocolVersion: request.params.protocolVersion, capabilities: { tools: {} },
    serverInfo: { name: "e2e-fixture", version: "1" },
  };
  else if (request.method === "tools/list") {
    const page = request.params?.cursor === undefined ? 0 : Number(request.params.cursor);
    if (!Number.isSafeInteger(page) || page < 0 || page >= pages) throw Error("invalid fixture cursor");
    result = { tools: Array.from({ length: toolsPerPage }, (_, index) => ({
      name: pages === 1 ? "echo" : `echo${page * toolsPerPage + index}`,
      description: "Echo the fixture argument" + "x".repeat(descriptionBytes), inputSchema: {
        type: "object", properties: { text: { type: "string" } }, required: ["text"], additionalProperties: false,
      },
    })), ...(page + 1 < pages ? { nextCursor: String(page + 1) } : {}) };
  }
  else if (request.method === "tools/call") result = { content: [{ type: "text", text: `MCP_E2E_${request.params.arguments.text}` }], isError: false };
  else if (request.method === "ping") result = {};
  else {
    console.log(JSON.stringify({ jsonrpc: "2.0", id: request.id, error: { code: -32601, message: "unsupported fixture method" } }));
    continue;
  }
  console.log(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }));
}
