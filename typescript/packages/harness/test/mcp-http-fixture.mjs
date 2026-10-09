// Test peer only: its counters observe physical requests, not SDK receipts.
// The CLI reuses Filesystem's browser driver and never launches another driver.
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

export async function startMcpFixture() {
  const peers = new Map();
  const sockets = new Set();
  const server = createServer(async (request, response) => {
    response.setHeader("access-control-allow-origin", "*");
    response.setHeader("access-control-allow-headers", "content-type,mcp-session-id,mcp-protocol-version");
    response.setHeader("access-control-allow-methods", "GET,POST,OPTIONS");
    response.setHeader("access-control-expose-headers", "mcp-session-id");
    response.setHeader("connection", "close");
    if (request.method === "OPTIONS") { response.writeHead(204).end(); return; }
    try {
      const url = new URL(request.url, "http://fixture");
      const client = url.searchParams.get("client");
      if (!client || client.length > 128) throw new Error("invalid fixture client");
      if (request.method === "GET" && url.pathname === "/stats") {
        response.setHeader("content-type", "application/json");
        response.end(JSON.stringify(peers.get(client) ?? { requests: 0, calls: 0 }));
        return;
      }
      if (request.method !== "POST" || url.pathname !== "/mcp") throw new Error("invalid fixture route");
      const chunks = [];
      let length = 0;
      for await (const chunk of request) {
        length += chunk.length;
        if (length > 4096) throw new Error("fixture request overflow");
        chunks.push(chunk);
      }
      const input = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      if (input.jsonrpc !== "2.0" || request.headers["mcp-protocol-version"] !== "2025-11-25") {
        throw new Error("invalid protocol header or envelope");
      }
      if (request.headers.cookie || request.headers.authorization) throw new Error("ambient credentials escaped");
      let peer = peers.get(client);
      if (input.method !== "notifications/initialized"
        && (typeof input.id !== "string" || peer?.ids.includes(input.id))) throw new Error("repeated or missing request id");
      let result;
      if (input.method === "initialize") {
        if (peer || request.headers["mcp-session-id"] || Object.keys(input.params.capabilities).length) {
          throw new Error("unexpected initialization or capabilities");
        }
        if (peers.size >= 16) throw new Error("fixture peer allowance exhausted");
        peer = { session: `fixture-${client}`, requests: 0, calls: 0, ready: false, ids: [] };
        peers.set(client, peer);
        response.setHeader("mcp-session-id", peer.session);
        result = { protocolVersion: "2025-11-25", capabilities: { tools: {} },
          serverInfo: { name: "browser-fixture", version: "1" } };
      } else {
        if (!peer || request.headers["mcp-session-id"] !== peer.session) throw new Error("foreign session");
        if (input.method === "notifications/initialized") {
          if (input.id !== undefined || peer.ready) throw new Error("invalid readiness");
          peer.ready = true; peer.requests++;
          response.writeHead(202).end(); return;
        }
        if (!peer.ready) throw new Error("request before readiness");
        if (input.method === "tools/list") {
          if (input.params.cursor !== undefined && input.params.cursor !== "next") throw new Error("foreign cursor");
          result = input.params.cursor === "next"
            ? { tools: [{ name: "other", description: "Find other", inputSchema: { type: "object" } }] }
            : { tools: [{ name: "echo", description: "Find héllo 🦀", inputSchema: { type: "object" } }], nextCursor: "next" };
        } else if (input.method === "tools/call" && input.params.name === "echo") {
          peer.calls++;
          if (input.params.arguments.loseResponse) {
            // The peer applied the request; an incomplete observation cannot
            // serve as a receipt or justify a second POST.
            result = undefined;
          } else {
            result = { content: [{ type: "text", text: "héllo 🦀" }], structuredContent: { value: "FULL_WIDTH" } };
          }
        } else throw new Error("unsupported fixture method");
      }
      peer.ids.push(input.id); peer.requests++;
      const message = result === undefined ? "{" : JSON.stringify({ jsonrpc: "2.0", id: input.id, result })
        .replace('"FULL_WIDTH"', "18446744073709551615");
      const sse = url.searchParams.get("mode") === "sse" && result !== undefined;
      response.setHeader("content-type", sse ? "text/event-stream" : "application/json");
      response.end(sse ? `data: ${message}\r\n\r\n` : message);
    } catch (error) {
      response.writeHead(400, { "content-type": "text/plain" }).end(String(error));
    }
  });
  server.on("connection", socket => { sockets.add(socket); socket.on("close", () => sockets.delete(socket)); });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return { origin: `http://127.0.0.1:${server.address().port}`,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    } };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const fixture = await startMcpFixture();
  try {
    const page = `../harness/test/browser-mcp.html?fixture=${encodeURIComponent(fixture.origin)}`;
    const driver = spawn(process.execPath, [fileURLToPath(new URL("../../filesystem/test/browser-qualify.mjs", import.meta.url)), page],
      { stdio: "inherit", windowsHide: true });
    process.exitCode = await new Promise((resolve, reject) => {
      driver.once("error", reject);
      driver.once("exit", code => resolve(code ?? 1));
    });
  } finally { await fixture.close(); }
}
