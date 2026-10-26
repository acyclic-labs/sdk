import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { create } from "@bufbuild/protobuf";
import * as session from "../generated/proto/stream/v1/session_pb.js";
import { RecordSchema } from "../generated/proto/stream/v1/stream_pb.js";
import { sessionService } from "./session-service-fixture.mjs";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { MemoryStreamProvider } from "../dist/memory.js";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
const provider = new MemoryStreamProvider();
const bytes = value => new Uint8Array(Buffer.from(value, "base64"));
function revive(name, value) {
  if (value === null) return value;
  if (["from", "ifTail", "atTail", "deadlineUnixMillis"].includes(name)) return BigInt(value);
  if (["idempotencyKey", "commitId", "hierarchyVersion", "value"].includes(name)) return bytes(value);
  if (name === "values") return value.map(bytes);
  if (Array.isArray(value)) return value.map(item => revive("", item));
  if (typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, revive(key, item)]));
  return value;
}
function values(records = []) {
  return records.map(record => {
    assert.deepEqual(record.keys, []);
    assert.deepEqual(record.attributes, []);
    return record.value;
  });
}
async function dispatch(route, input) {
  switch (route) {
    case "tail": return provider.tail(input.path);
    case "append": return provider.append(input.path, values(input.records), input.options);
    case "fork": return provider.fork(input.source, input.destination, { ...input.options, exclude: input.exclude });
    case "read": { const records = []; for await (const record of provider.read(input.path, { from: input.from, limit: input.limit })) records.push(record); return records; }
    case "children/page": return provider.childrenPage(input);
    case "commit": {
      const request = { ...input.request, mutations: input.request.mutations.map(mutation => {
        const kind = mutation.append ? "append" : "fork";
        const { records, ...fields } = mutation[kind];
        return { [kind]: { ...fields, values: values(records) } };
      }) };
      const result = await provider.commit(request, input.options);
      return result.ok ? { ...result, envelope: await provider.readCommit(result.commitId) } : result;
    }
    case "commits/read": return provider.readCommit(input.commitId);
    case "idempotency/inspect": return (await provider.inspectIdempotency(input.idempotencyKey)) ?? null;
    default: throw new Error(`unknown route ${route}`);
  }
}
const replacer = (_name, value) => typeof value === "bigint" ? value.toString() : value instanceof Uint8Array ? Buffer.from(value).toString("base64") : value;
const directory = mkdtempSync(join(tmpdir(), "stream-http-conformance-"));
const keyFile = join(directory, "key.pem");
const certFile = join(directory, "cert.pem");
execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost", "-keyout", keyFile, "-out", certFile], { stdio: "ignore" });
const adapter = connectNodeAdapter({ routes(router) {
  router.service(session.StreamSessionService, { session: sessionService(session, async function* (subscription, signal) {
    const path = subscription.kind.value.resource.replace(/^streams:\//, "");
    for await (const record of provider.follow(path, { from: subscription.resume.next, signal })) {
      yield { record: create(RecordSchema, record) };
    }
  }) });
} });
const server = createSecureServer({ key: readFileSync(keyFile), cert: readFileSync(certFile), allowHTTP1: true }, async (request, response) => {
  if (!request.url.startsWith("/v1/stream/")) { adapter(request, response); return; }
  response.setHeader("content-type", "application/json");
  const commitOnly = request.headers.authorization === "Bearer commit-only";
  if (request.headers.authorization !== "Bearer conformance" && !(commitOnly && request.url === "/v1/stream/commit")) { response.writeHead(403); response.end('{"code":"access_denied"}'); return; }
  try {
    let text = "";
    for await (const chunk of request) text += chunk;
    const result = await dispatch(request.url.replace("/v1/stream/", ""), revive("", JSON.parse(text)));
    if (process.env.SDK_HTTP_TRACE) console.error(request.url, JSON.stringify(result, replacer)); response.end(JSON.stringify(result, replacer));
  } catch (error) { if (process.env.SDK_HTTP_TRACE) console.error(request.url, error); response.writeHead(400); response.end(JSON.stringify({ code: error.code ?? "unavailable" })); }
});
const closeServer = ownFixtureServer(server);
try {
  await new Promise(resolve => server.listen(0, "localhost", resolve));
  const status = await new Promise((resolve, reject) => {
    const child = spawn("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-stream", "--features", "http", "--example", "http-conformance", "--", `https://localhost:${server.address().port}/`], { cwd: root, stdio: "inherit", env: { ...process.env, ACYCLIC_CONFORMANCE_CA: certFile } });
    child.once("error", reject); child.once("exit", resolve);
  });
  assert.equal(status, 0, "native Stream HTTP conformance failed");
} finally { await closeServer(); rmSync(directory, { recursive: true, force: true }); }
