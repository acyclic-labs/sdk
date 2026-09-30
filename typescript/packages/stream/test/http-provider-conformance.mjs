import assert from "node:assert/strict";
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { MemoryStreamProvider } from "../dist/memory.js";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
const provider = new MemoryStreamProvider();
const bytes = value => new Uint8Array(Buffer.from(value, "base64"));
function revive(name, value) {
  if (value === null) return value;
  if (["from", "ifTail", "atTail", "deadlineUnixMillis"].includes(name)) return BigInt(value);
  if (["idempotencyKey", "commitId", "hierarchyVersion"].includes(name)) return bytes(value);
  if (name === "values") return value.map(bytes);
  if (Array.isArray(value)) return value.map(item => revive("", item));
  if (typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, revive(key, item)]));
  return value;
}
async function dispatch(route, input) {
  switch (route) {
    case "tail": return provider.tail(input.path);
    case "append": return provider.append(input.path, input.values, input.options);
    case "fork": return provider.fork(input.source, input.destination, input.options);
    case "read": { const records = []; for await (const record of provider.read(input.path, { from: input.from, limit: input.limit })) records.push(record); return records; }
    case "children": { const children = []; for await (const child of provider.children(input.parent, input.limit)) children.push(child); return children; }
    case "children/page": return provider.childrenPage(input);
    case "commit": return provider.commit(input.request, input.options);
    case "commits/read": return provider.readCommit(input.commitId);
    case "idempotency/inspect": return (await provider.inspectIdempotency(input.idempotencyKey)) ?? null;
    default: throw new Error(`unknown route ${route}`);
  }
}
const replacer = (_name, value) => typeof value === "bigint" ? value.toString() : value instanceof Uint8Array ? Buffer.from(value).toString("base64") : value;
const server = createServer(async (request, response) => {
  response.setHeader("content-type", "application/json");
  if (request.headers.authorization !== "Bearer conformance") { response.writeHead(403); response.end('{"code":"access_denied"}'); return; }
  try {
    let text = "";
    for await (const chunk of request) text += chunk;
    const result = await dispatch(request.url.replace("/v1/stream/", ""), revive("", JSON.parse(text)));
    if (process.env.SDK_HTTP_TRACE) console.error(request.url, JSON.stringify(result, replacer)); response.end(JSON.stringify(result, replacer));
  } catch (error) { if (process.env.SDK_HTTP_TRACE) console.error(request.url, error); response.writeHead(400); response.end(JSON.stringify({ code: error.code ?? "unavailable" })); }
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
try {
  const status = await new Promise((resolve, reject) => {
    const child = spawn("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-stream", "--features", "http", "--example", "http-conformance", "--", `http://127.0.0.1:${server.address().port}/`], { cwd: root, stdio: "inherit" });
    child.once("error", reject); child.once("exit", resolve);
  });
  assert.equal(status, 0, "native Stream HTTP conformance failed");
} finally { await new Promise(resolve => server.close(resolve)); }
