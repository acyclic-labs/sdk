// Production adapter over a loopback endpoint; no model implementation in the binary.
import { createServer } from "node:http";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

export async function fixture() {
  const directory = mkdtempSync(join(tmpdir(), "graphcoder-"));
  const root = join(directory, "workspace"), settings = join(directory, "settings");
  mkdirSync(root); mkdirSync(settings);
  let fail = false, calls = 0, release, releaseCount, observeCount, countTarget;
  let countBarrier = Promise.resolve();
  const counting = new Promise(resolve => { observeCount = resolve; });
  let barrier = Promise.resolve();
  const server = createServer(async (request, response) => {
    let body = "";
    for await (const bytes of request) body += bytes;
    const input = JSON.parse(body);
    if (request.url === "/count") {
      if (countTarget && JSON.stringify(input).includes(countTarget)) { observeCount(); await countBarrier; }
      if (response.destroyed) return;
      response.writeHead(200, { "content-type": "application/json" });
      response.end(JSON.stringify({ input_tokens: 64 })); return;
    }
    if (request.url !== "/responses" || !Array.isArray(input.input)) {
      response.writeHead(400); response.end("invalid responses request"); return;
    }
    calls++;
    if (fail) { response.writeHead(400); response.end("scripted provider error"); return; }
    response.writeHead(200, { "content-type": "text/event-stream" });
    for (const delta of ["graphcoder ", "**test answer**"]) {
      response.write(`data: ${JSON.stringify({ type: "response.output_text.delta", delta })}\n\n`);
    }
    await barrier;
    if (response.destroyed) return;
    const completed = `data: ${JSON.stringify({ type: "response.completed", response: { status: "completed", usage: { input_tokens: 64, output_tokens: 2 } } })}\n\n`;
    response.end(completed);
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const catalog = join(directory, "catalog.json");
  writeFileSync(catalog, JSON.stringify({ scripted: {
    adapter: "responses", authentication: "optional", base_url: `http://127.0.0.1:${server.address().port}`,
    models: { test: { capacity: { context_tokens: 8192, output_tokens: 1024 }, features: { tools: true, images: false } } }, max_event_bytes: 65536,
    token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 },
  } }));
  return {
    root, settings, args: ["--workspace", root, "--settings-root", settings, "--model", "scripted/test", "--catalog", catalog],
    counting,
    holdCount: text => { countTarget = text; countBarrier = new Promise(resolve => { releaseCount = resolve; }); },
    completeCount: () => releaseCount?.(),
    hold: () => { barrier = new Promise(resolve => { release = resolve; }); },
    complete: () => { release?.(); },
    calls: () => calls, fail: () => { fail = true; },
    close: async () => { release?.(); releaseCount?.(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); rmSync(directory, { recursive: true, force: true }); },
  };
}
