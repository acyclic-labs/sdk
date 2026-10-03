import { appendFileSync } from "node:fs";

const logPath = process.env.GRAPH_CODER_HOST_LOG;
const sessionId = "session-1";
const agentId = "agent-1";
const generation = "7";
let sessionState = "running";
let approvalState = "pending";
let messageNumber = 1;

function log(request) {
  if (logPath === undefined) return;
  appendFileSync(logPath, `${JSON.stringify({ method: request.method, params: request.params })}\n`);
}

if (logPath !== undefined) {
  appendFileSync(logPath, `${JSON.stringify({ kind: "host_identity", executable: process.execPath, argv: process.argv.slice(1) })}\n`);
}

if (logPath !== undefined) {
  appendFileSync(logPath, `${JSON.stringify({ kind: "host_identity", executable: process.execPath, argv: process.argv.slice(1) })}\n`);
}

function respond(request, result) {
  process.stdout.write(`${JSON.stringify({ request_id: request.request_id, ok: true, result })}\n`);
}

function fail(request, code, message) {
  process.stdout.write(`${JSON.stringify({ request_id: request.request_id, ok: false, error: { code, message } })}\n`);
}

function summary() {
  return {
    id: sessionId,
    title: "native protocol fixture",
    state: sessionState,
    updated_at: "2026-01-01T00:00:00.000Z",
    root_agent_id: agentId,
  };
}

function snapshot() {
  return {
    summary: summary(),
    agents: [{ id: agentId, parent_id: null, task: "inspect", state: "running", depth: 0, children: [] }],
    workspace_generation: generation,
  };
}

function approval() {
  return {
    id: "approval-1",
    session_id: sessionId,
    agent_id: agentId,
    operation_id: "op-7",
    action_digest: "digest-7",
    description: "apply the reviewed workspace change",
    state: approvalState,
    created_at: "2026-01-01T00:00:00.000Z",
  };
}

function handle(request) {
  log(request);
  const params = request.params ?? {};
  switch (request.method) {
    case "list_sessions":
      return respond(request, { items: [summary()] });
    case "start_session":
      sessionState = "running";
      return respond(request, snapshot());
    case "open_session":
      if (params.session_id !== sessionId) return fail(request, "not_found", "session was not found");
      return respond(request, snapshot());
    case "resume_session":
      if (params.session_id !== sessionId) return fail(request, "not_found", "session was not found");
      sessionState = "running";
      return respond(request, snapshot());
    case "read_activity":
      return respond(request, {
        session_id: sessionId,
        items: [{ sequence: "1", id: "activity-1", kind: "session", actor_id: null, text: "session opened", at: "2026-01-01T00:00:00.000Z" }],
      });
    case "read_messages":
      return respond(request, {
        session_id: sessionId,
        items: [{ id: "message-1", session_id: sessionId, sender_id: agentId, recipient_id: agentId, body: "native fixture message", delivered_at: null }],
      });
    case "send_message":
      return respond(request, {
        id: `message-${++messageNumber}`,
        session_id: params.session_id,
        sender_id: params.sender_id,
        recipient_id: params.recipient_id,
        body: params.body,
        delivered_at: null,
      });
    case "list_approvals":
      return respond(request, { session_id: sessionId, items: [approval()] });
    case "resolve_approval":
      if (params.approval_id !== "approval-1") return fail(request, "not_found", "approval was not found");
      approvalState = params.approved === true ? "approved" : "declined";
      return respond(request, approval());
    case "cancel_session":
      if (params.session_id !== sessionId) return fail(request, "not_found", "session was not found");
      sessionState = "cancelled";
      return respond(request, snapshot());
    case "list_changes":
      return respond(request, { session_id: sessionId, generation, items: [{ path: "README.md", kind: "modified", additions: 1, deletions: 0 }] });
    case "read_change":
      if (params.path !== "README.md") return fail(request, "not_found", "change was not found");
      return respond(request, { path: params.path, unified_diff: "@@ -1 +1 @@\n-native\n+fixture\n", generation: params.generation });
    case "read_file":
      if (params.path !== "README.md") return fail(request, "not_found", "file was not found");
      return respond(request, { path: params.path, media_type: "text/markdown", bytes: [35, 32, 110, 97, 116, 105, 118, 101, 10], generation: params.generation });
    case "approve_writeback":
      if (params.operation_id !== "op-7" || params.session_id !== sessionId) return fail(request, "denied", "writeback operation is not pending");
      if (params.expected_generation !== generation) return fail(request, "stale", "workspace generation changed");
      if (params.approved !== true || approvalState !== "approved") return fail(request, "denied", "matching approval is required");
      return respond(request, { operation_id: params.operation_id, session_id: sessionId, generation, applied: true });
    default:
      return fail(request, "unsupported", `fixture does not implement ${request.method}`);
  }
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", chunk => {
  buffer += chunk;
  for (;;) {
    const newline = buffer.indexOf("\n");
    if (newline < 0) return;
    const line = buffer.slice(0, newline).replace(/\r$/, "");
    buffer = buffer.slice(newline + 1);
    if (line.trim() === "") continue;
    try {
      const request = JSON.parse(line);
      if (typeof request?.request_id !== "string" || typeof request?.method !== "string") throw new Error("invalid request envelope");
      handle(request);
    } catch (error) {
      process.stderr.write(`fixture error: ${error instanceof Error ? error.message : String(error)}\n`);
    }
  }
});
