import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { test } from "node:test";
import { SwarmClient, SwarmHttpError } from "@acyclic-labs/swarm";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";

async function fixture(t, handler, options = {}) {
  const failures = [];
  const server = createServer(async (request, response) => {
    try { await handler(request, response); }
    catch (error) { failures.push(error); response.writeHead(500).end(); }
  });
  const close = ownFixtureServer(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  const client = new SwarmClient({ endpoint, token: "fixture", org: "org", user: "user", ...options });
  t.after(async () => { await close(); assert.deepEqual(failures, []); });
  return { client, endpoint };
}

const traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
const callOptions = { traceparent };
const createRequest = {
  task: "work", repo: "https://github.com/example/repo", limits: { width: 2, depth: 1, running: 2, web_calls: 3, budget_usd: 4 },
  speculation: { attempts: 2 }, inference: { base_url: "https://inference.example", token: "scoped", budget_micros: 100 },
  mcp_servers: [{ name: "tools", url: "https://tools.example", headers: { "x-scoped": "fixture" } }], skills: [{ content: "# skill" }],
  model: "model", harness: "codex", merge_requires: "npm test", models: { root: "model" }, verify: { command: "npm test", rounds: 2 },
  pull_request: { github_token: "scoped-github-fixture", title: "change", body: "body", draft: true },
};

test("every public Cloud route forwards its method, identity, body and response format", async t => {
  const id = "s/1";
  const prefix = "/v1/swarms/s%2F1";
  const json = { retained: [0, false, null, "text"] };
  const upload = new Uint8Array([31, 139, 0, 255]);
  const cases = [
    ["GET", "/health", undefined, json, client => client.health(callOptions)],
    ["POST", "/v1/swarms", createRequest, { swarm_id: id, harness: "codex" }, client => client.create(createRequest, { ...callOptions, idempotencyKey: "create-original" }), "create-original"],
    ["GET", "/v1/swarms", undefined, { swarms: [json] }, client => client.list(callOptions)],
    ["GET", prefix, undefined, { ...json, agents: [{ box: { sandbox_id: "box" } }], last_seq: 7 }, client => client.detail(id, callOptions)],
    ["GET", `${prefix}/trace`, undefined, { trace_id: "trace", spans: [json] }, client => client.trace(id, callOptions)],
    ["GET", `${prefix}/artifact`, undefined, { artifact_id: "artifact", partial: true }, client => client.artifact(id, callOptions)],
    ["GET", `${prefix}/artifact/files`, undefined, { files: [{ path: "file", status: "modified", insertions: 2, deletions: 1 }] }, client => client.artifactFiles(id, callOptions)],
    ["GET", `${prefix}/artifact/patch`, undefined, "diff --git a/file b/file\n", client => client.artifactPatch(id, callOptions)],
    ["GET", `${prefix}/artifact/tree`, undefined, upload, async client => {
      const response = await client.artifactTree(id, callOptions);
      assert.equal(response.headers.get("content-type"), "application/gzip");
      assert.equal(response.headers.get("x-acyclic-files"), "2");
      assert.equal(response.headers.get("x-acyclic-left-out-count"), "1");
      return new Uint8Array(await response.arrayBuffer());
    }],
    ["GET", `${prefix}/memory`, undefined, json, client => client.memory(id, callOptions)],
    ["POST", `${prefix}/cancel`, undefined, { swarm_id: id, status: "canceling" }, client => client.cancel(id, callOptions)],
    ["POST", `${prefix}/pull-request`, createRequest.pull_request, { url: "https://github.com/example/repo/pull/1", number: 1 }, client => client.openPullRequest(id, createRequest.pull_request, callOptions)],
    ["POST", "/v1/tokens", { org: "org", user: "user", ttl_hours: 1, scopes: ["swarms:read"] }, { token: "fixture-new", token_id: "token", expires_at: "later" }, client => client.createToken({ org: "org", user: "user", ttl_hours: 1, scopes: ["swarms:read"] }, callOptions)],
    ["GET", "/v1/tokens?org=org+%26+one", undefined, { tokens: [json] }, client => client.listTokens("org & one", callOptions)],
    ["DELETE", "/v1/tokens/t%2F1", undefined, undefined, client => client.revokeToken("t/1", callOptions)],
    ["GET", "/v1/whoami", undefined, { org: "org", user: "user", scopes: ["swarms:read"], kind: "token", expires_at: "later", capabilities: ["artifact_tree"] }, client => client.whoami(callOptions)],
    ["GET", "/v1/metrics", undefined, "swarm_requests_total 1\n", client => client.metrics(callOptions)],
    ["POST", "/v1/swarms/uploads", upload, { upload_id: "upl_fixture", files: 1, compressed_bytes: 4, tar_bytes: 3, sha256: "hash", expires_at: "later" }, client => client.upload(upload, { ...callOptions, idempotencyKey: "upload-original" }), "upload-original"],
  ];
  let index = 0;
  const { client } = await fixture(t, async (request, response) => {
    const [method, path, body, result, , idempotencyKey] = cases[index++];
    assert.equal(request.method, method);
    assert.equal(request.url, path);
    assert.equal(request.headers.authorization, "Bearer fixture");
    assert.equal(request.headers["x-acyclic-org"], "org");
    assert.equal(request.headers["x-acyclic-user"], "user");
    assert.equal(request.headers.traceparent, traceparent);
    assert.equal(request.headers["idempotency-key"], idempotencyKey);
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const received = Buffer.concat(chunks);
    if (body instanceof Uint8Array) {
      assert.equal(request.headers["content-type"], "application/gzip");
      assert.deepEqual(new Uint8Array(received), body);
    } else if (body !== undefined) {
      assert.equal(request.headers["content-type"], "application/json");
      assert.deepEqual(JSON.parse(received), body);
    } else assert.equal(received.length, 0);
    if (result === undefined) response.writeHead(204).end();
    else if (result instanceof Uint8Array) response.writeHead(200, { "content-type": "application/gzip", "x-acyclic-files": "2", "x-acyclic-left-out-count": "1" }).end(result);
    else if (typeof result === "string") response.writeHead(200, { "content-type": "text/plain" }).end(result);
    else response.writeHead(method === "POST" ? 201 : 200, { "content-type": "application/json" }).end(JSON.stringify(result));
  });
  for (const [, , , result, run] of cases) assert.deepEqual(await run(client), result);
  assert.equal(index, 18);
});

test("SSE parses split UTF-8, CRLF, multiline data and heartbeat, with exact replay cursors", async t => {
  const cursors = [];
  const event = { seq: 2, type: "log", agent_id: "agent", line: "résumé" };
  const { client } = await fixture(t, (request, response) => {
    cursors.push(request.headers["last-event-id"]);
    assert.equal(request.headers.accept, "text/event-stream");
    assert.equal(request.headers.authorization, "Bearer fixture");
    response.writeHead(200, { "content-type": "text/event-stream; charset=utf-8" });
    const frame = request.url === "/health/sse" ? 'id: 1\ndata: {"seq":1,"ok":true}\n\n' : `: heartbeat\r\n\r\nid: 9007199254740993\r\nevent: message\r\ndata: {"seq":2,"type":"log",\r\ndata: "agent_id":"agent","line":"résumé"}\r\n\r\n`;
    for (const byte of new TextEncoder().encode(frame)) response.write(new Uint8Array([byte]));
    response.end();
  });
  const result = [];
  for await (const frame of client.watch("swarm", { lastEventId: "9007199254740992" })) result.push(frame);
  assert.deepEqual(result, [{ id: "9007199254740993", event: "message", data: event }]);
  for await (const frame of client.watch("swarm", { lastEventId: result[0].id })) assert.deepEqual(frame, result[0]);
  const health = [];
  for await (const frame of client.healthEvents()) health.push(frame);
  assert.deepEqual(health, [{ id: "1", event: "message", data: { seq: 1, ok: true } }]);
  assert.deepEqual(cursors, ["9007199254740992", "9007199254740993", undefined]);
});

test("ending SSE consumption cancels the subscription without reconnecting", async t => {
  let calls = 0;
  let disconnected;
  const closed = new Promise(resolve => { disconnected = resolve; });
  const { client } = await fixture(t, (request, response) => {
    calls++;
    response.on("close", disconnected);
    response.writeHead(200, { "content-type": "text/event-stream" });
    response.write('id: 1\ndata: {"seq":1,"type":"log","agent_id":"a","line":"one"}\n\n');
  });
  for await (const event of client.watch("swarm")) { assert.equal(event.id, "1"); break; }
  await closed;
  assert.equal(calls, 1);
});

test("failed mutations expose status, error payload and trace without retry", async t => {
  let calls = 0;
  const { client } = await fixture(t, (request, response) => {
    calls++;
    assert.equal(request.headers["idempotency-key"], "original");
    response.writeHead(503, { "content-type": "application/json", "x-trace-id": "trace" });
    response.end(JSON.stringify({ error: "unavailable", message: "authority unavailable" }));
  });
  await assert.rejects(client.create({ task: "work" }, { idempotencyKey: "original" }), error => {
    assert.ok(error instanceof SwarmHttpError);
    assert.equal(error.status, 503);
    assert.equal(error.traceId, "trace");
    assert.deepEqual(error.body, { error: "unavailable", message: "authority unavailable" });
    return true;
  });
  assert.equal(calls, 1);
});

test("bounded JSON and SSE reject oversize responses and incomplete events never advance the cursor", async t => {
  const { client } = await fixture(t, (request, response) => {
    if (request.url === "/v1/swarms/partial/events") response.writeHead(200, { "content-type": "text/event-stream" }).end('id: 2\ndata: {"seq":2}');
    else if (request.url.endsWith("/events")) response.writeHead(200, { "content-type": "text/event-stream" }).end(`data: ${"x".repeat(300)}\n\n`);
    else response.writeHead(200, { "content-type": "application/json" }).end(JSON.stringify("x".repeat(300)));
  }, { maximumResponseBytes: 256 });
  await assert.rejects(client.list(), RangeError);
  await assert.rejects(async () => { for await (const frame of client.watch("large")) assert.fail(frame); }, RangeError);
  const frames = [];
  for await (const frame of client.watch("partial")) frames.push(frame);
  assert.deepEqual(frames, []);
});

test("abort, route encoding and redirects preserve the authenticated request boundary", async t => {
  for (const endpoint of ["http://remote.example", "https://user@host", "https://host/path", "https://host?x=1"]) assert.throws(() => new SwarmClient({ endpoint, token: "fixture" }));
  for (const token of ["", "bad\nheader", "x".repeat(12289)]) assert.throws(() => new SwarmClient({ endpoint: "https://host", token }));
  let calls = 0;
  const { client } = await fixture(t, (request, response) => { calls++; response.writeHead(302, { location: "/forwarded" }).end(); });
  await assert.rejects(client.list(), TypeError);
  assert.throws(() => client.detail(".."), TypeError);
  await assert.rejects(client.list({ signal: AbortSignal.abort() }));
  assert.equal(calls, 1);
});
