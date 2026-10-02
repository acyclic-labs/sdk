#!/usr/bin/env node
// Fake OpenAI Responses API for recording `codex exec --json` fixtures.
//
// Usage: fake-responses-server.mjs PORT LOGFILE
// The mode is read per request from $FAKE_MODE_FILE (default /tmp/codex-probe/mode),
// falling back to $FAKE_MODE:
//   ok          assistant message "hi from fake"
//   shell       first call: a shell function_call (tool name taken from the request);
//               once a function_call_output is in the input: a final message
//   patch       first call: apply_patch custom_tool_call adding hello.txt (needs a catalog
//               model, e.g. -m gpt-5.5)
//   reasoning   reasoning summary + message
//   http402 / http500 / http429 / http401   that status with a JSON error body
//   failed      200 SSE with response.failed (generic, retryable)
//   quota429    429 with error.type=insufficient_quota (non-retryable)
//   quotafailed 200 SSE response.failed with code insufficient_quota
// Only POST */responses is served; everything else 404s. Every request is appended to
// LOGFILE as a JSON line (n, t, method, path, headers, mode, body).

import { appendFileSync, readFileSync } from "node:fs";
import { createServer } from "node:http";
import { spawnSync } from "node:child_process";

const [port, log] = process.argv.slice(2);
const modeFile = process.env.FAKE_MODE_FILE ?? "/tmp/codex-probe/mode";
let counter = 0;

const mode = () => {
  try {
    return readFileSync(modeFile, "utf8").trim() || "ok";
  } catch {
    return process.env.FAKE_MODE ?? "ok";
  }
};

const usage = {
  input_tokens: 100,
  input_tokens_details: { cached_tokens: 40 },
  output_tokens: 7,
  output_tokens_details: { reasoning_tokens: 3 },
  total_tokens: 107,
};

const sse = event => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`;

const message = (text, n) => ({
  type: "message",
  id: `msg_${n}`,
  role: "assistant",
  status: "completed",
  content: [{ type: "output_text", text, annotations: [] }],
});

const shellTool = body => {
  const names = (body.tools ?? []).map(tool => tool.name ?? tool.type);
  return ["exec_command", "shell_command", "shell", "local_shell"].find(name => names.includes(name))
    ?? names[0] ?? "shell";
};

const send = (res, code, body) => {
  res.writeHead(code, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
};

createServer((req, res) => {
  const chunks = [];
  req.on("data", chunk => chunks.push(chunk));
  req.on("end", () => {
    let raw = Buffer.concat(chunks);
    if (req.headers["content-encoding"] === "zstd") {
      const out = spawnSync("zstd", ["-dc"], { input: raw });
      if (out.status === 0) raw = out.stdout;
    }
    const n = ++counter;
    const record = { n, t: Date.now() / 1000, method: req.method, path: req.url, headers: req.headers, mode: mode() };
    try {
      record.body = raw.length ? JSON.parse(raw.toString("utf8")) : null;
    } catch {
      record.body_raw = raw.toString("utf8");
    }
    appendFileSync(log, `${JSON.stringify(record)}\n`);

    if (req.method !== "POST" || !req.url.replace(/\/$/, "").endsWith("/responses")) {
      return send(res, 404, '{"error":"not found"}');
    }
    const m = record.mode;
    if (m === "quota429") {
      return send(res, 429, JSON.stringify({ error: { message: "budget exhausted", type: "insufficient_quota", code: "insufficient_quota" } }));
    }
    if (m.startsWith("http")) {
      const code = Number(m.slice(4));
      const error = code === 402
        ? { message: "Payment required: budget exhausted", type: "billing", code: "budget_exhausted" }
        : { message: `fake ${code}`, type: "fake_error", code: `fake_${code}` };
      return send(res, code, JSON.stringify({ error }));
    }
    const body = record.body ?? {};
    const id = `resp_${n}`;
    const events = [{ type: "response.created", response: { id } }];
    if (m === "quotafailed" || m === "failed") {
      const error = m === "failed"
        ? { code: "server_error", message: "fake response.failed" }
        : { code: "insufficient_quota", message: "budget exhausted (sse)" };
      events.push({ type: "response.failed", response: { id, status: "failed", error } });
    } else {
      const items = [];
      if (m === "reasoning") {
        items.push({ type: "reasoning", id: `rs_${n}`, summary: [{ type: "summary_text", text: "**Thinking** about greeting" }] });
      }
      const hasOutput = (body.input ?? []).some(item =>
        item && ["function_call_output", "custom_tool_call_output"].includes(item.type));
      if (m === "patch" && !hasOutput) {
        items.push({
          type: "custom_tool_call", id: `ctc_${n}`, call_id: `call_${n}`, name: "apply_patch", status: "completed",
          input: "*** Begin Patch\n*** Add File: hello.txt\n+hello from patch\n*** End Patch\n",
        });
      } else if (m === "shell" && !hasOutput) {
        const tool = shellTool(body);
        const cmd = "echo probe-output && ls";
        const args = tool === "shell_command" ? { command: cmd }
          : tool === "exec_command" ? { cmd }
          : { cmd, command: ["bash", "-lc", cmd] };
        items.push({ type: "function_call", id: `fc_${n}`, call_id: `call_${n}`, name: tool, arguments: JSON.stringify(args), status: "completed" });
      } else {
        items.push(message("hi from fake", n));
      }
      for (const item of items) {
        events.push({ type: "response.output_item.added", output_index: 0, item });
        events.push({ type: "response.output_item.done", output_index: 0, item });
      }
      events.push({ type: "response.completed", response: { id, status: "completed", usage } });
    }
    res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "close" });
    for (const event of events) res.write(sse(event));
    res.end();
  });
}).listen(Number(port), "127.0.0.1");
