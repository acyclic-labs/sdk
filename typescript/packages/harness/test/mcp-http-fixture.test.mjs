import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import init from "../generated/wasm/acyclic_harness_wasm.js";
import { createBrowserMcpHttpProvider } from "../src/mcp-http.ts";
import { startMcpFixture } from "./mcp-http-fixture.mjs";
import { exerciseMcpHttp, verifyReopenedMcpHttp, peerStats } from "./mcp-http-consumer.mjs";

await init({ module_or_path: readFileSync(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)) });

test("real HTTP peer and portable Rust client retain isolated JSON/SSE sessions without reconciliation POSTs", async () => {
  const fixture = await startMcpFixture();
  try {
    const observed = await Promise.all(["json", "sse"].map(mode =>
      exerciseMcpHttp(fixture.origin, crypto.randomUUID(), mode, createBrowserMcpHttpProvider)));
    assert.notEqual(observed[0].session, observed[1].session);
    await Promise.all(observed.map(peer => verifyReopenedMcpHttp(fixture.origin, peer, createBrowserMcpHttpProvider)));
    const peer = observed[0];
    const before = await peerStats(fixture.origin, peer.client);
    // Independently check the peer oracle: disabling its session/identity/
    // credential guards must fail these controls before another apply.
    for (const change of [
      { headers: { "mcp-session-id": observed[1].session } },
      { headers: { "mcp-protocol-version": "unknown" } },
      { headers: { cookie: "fixture=ambient" } },
      { id: peer.lostOperation },
    ]) {
      const response = await fetch(peer.endpoint, { method: "POST", headers: {
        "content-type": "application/json", "mcp-session-id": peer.session,
        "mcp-protocol-version": "2025-11-25", ...change.headers,
      }, body: JSON.stringify({ jsonrpc: "2.0", id: change.id ?? crypto.randomUUID(),
        method: "tools/call", params: { name: "echo", arguments: {} } }) });
      assert.equal(response.status, 400);
      await response.text();
      assert.deepEqual(await peerStats(fixture.origin, peer.client), before);
    }
  } finally { await fixture.close(); }
});
