import assert from "node:assert/strict";
const { Stream } = await import("@acyclic-labs/stream");
const originalProcess = globalThis.process;
const originalFetch = globalThis.fetch;
let calls = 0;
try {
  globalThis.fetch = async (input, init) => {
    calls += 1;
    assert.equal(String(input), "https://browser.test/v1/stream/tail");
    assert.equal(init?.method, "POST");
    assert.equal(init?.headers?.authorization, "Bearer browser-token");
    return new Response('"0"', { status: 200, headers: { "content-type": "application/json" } });
  };
  globalThis.process = undefined;
  const client = Stream.fromEnv({ endpoint: "https://browser.test", token: "browser-token" });
  assert.equal(client.provider.constructor.name, "DefaultStreamProvider");
  const tail = await client.bytes("browser/events").tail();
  assert.equal(tail, 0n);
  assert.equal(calls, 1);
  console.log(JSON.stringify({ schema: "acyclic.stream.browser.fallback.v1", selected: "http-wasm", process: "browser", tail: tail.toString(), requests: calls }));
} finally {
  globalThis.process = originalProcess;
  globalThis.fetch = originalFetch;
}
