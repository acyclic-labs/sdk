import { appendFileSync } from "node:fs";

// This process is deliberately a hostile protocol peer. It is only used to
// qualify response validation in the package adapter; it is not a Harness or
// swarm implementation.
const mode = process.argv[process.argv.indexOf("--case") + 1] ?? "unsupported";
const logPath = process.env.GRAPH_CODER_HOST_LOG;

function log(request) {
  if (logPath !== undefined) appendFileSync(logPath, `${JSON.stringify({ method: request.method, params: request.params })}\n`);
}

function respond(request, result) {
  process.stdout.write(`${JSON.stringify({ request_id: request.request_id, ok: true, result })}\n`);
}

function fail(request, code, message) {
  process.stdout.write(`${JSON.stringify({ request_id: request.request_id, ok: false, error: { code, message } })}\n`);
}

function summary(id = "session-1") {
  return { id, title: "hostile fixture", state: "running", updated_at: "2026-01-01T00:00:00.000Z", root_agent_id: "agent-1" };
}

function snapshot(id = "session-1") {
  return {
    summary: summary(id),
    agents: [{ id: "agent-1", parent_id: null, task: "qualification", state: "running", depth: 0, children: [] }],
    workspace_generation: "7",
  };
}

function approval(sessionId = "session-1") {
  return {
    id: "approval-1",
    session_id: sessionId,
    agent_id: "agent-1",
    operation_id: "op-7",
    action_digest: "digest-7",
    description: "hostile approval fixture",
    state: "pending",
    created_at: "2026-01-01T00:00:00.000Z",
  };
}

function handle(request) {
  log(request);
  switch (mode) {
    case "wrong-session":
      return respond(request, snapshot("session-2"));
    case "wrong-page-session":
      return respond(request, { session_id: "session-2", items: [] });
    case "wrong-path":
      return respond(request, { path: "other.txt", unified_diff: "@@ hostile @@\n", generation: "8" });
    case "wrong-receipt":
      return respond(request, { operation_id: "other-operation", session_id: "session-2", generation: "8", applied: true });
    case "wrong-approval-session":
      if (request.method === "open_session") return respond(request, snapshot("session-1"));
      return respond(request, approval("session-2"));
    case "missing-approval-session":
      if (request.params?.session_id === undefined) return fail(request, "invalid_input", "session_id is required");
      return respond(request, approval("session-2"));
    case "bad-bytes":
      return respond(request, { path: "README.md", media_type: "text/markdown", bytes: [-1, 256, 1.5], generation: "7" });
    case "malformed-envelope":
      return process.stdout.write(`${JSON.stringify({ request_id: request.request_id, ok: "yes", result: {} })}\n`);
    case "backend-error":
      return fail(request, "denied", "root approval required");
    case "oversized":
      return process.stdout.write(`${"x".repeat(2048)}\n`);
    default:
      return fail(request, "unsupported", `unknown hostile fixture case ${mode}`);
  }
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => {
  buffer += chunk;
  for (;;) {
    const newline = buffer.indexOf("\n");
    if (newline < 0) return;
    const line = buffer.slice(0, newline).replace(/\r$/u, "");
    buffer = buffer.slice(newline + 1);
    if (line.trim() === "") continue;
    try {
      const request = JSON.parse(line);
      if (typeof request?.request_id !== "string" || typeof request?.method !== "string") throw new Error("invalid request envelope");
      handle(request);
    } catch (error) {
      process.stderr.write(`hostile fixture error: ${error instanceof Error ? error.message : String(error)}\n`);
    }
  }
});
