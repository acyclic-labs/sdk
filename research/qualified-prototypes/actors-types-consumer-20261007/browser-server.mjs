import http from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";
import { create, toBinary } from "@bufbuild/protobuf";
import {
  AddSubscriptionResponseSchema,
  CheckpointActorResponseSchema,
  CreateActorResponseSchema,
  InspectActorResponseSchema,
  InvokeActorResponseSchema,
  RemoveSubscriptionResponseSchema,
  ResumeSubscriptionResponseSchema,
  UpdateActorResponseSchema,
} from "@acyclic-labs/actors/proto";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const responseSchemas = {
  "/v1/actors/create": CreateActorResponseSchema,
  "/v1/actors/update": UpdateActorResponseSchema,
  "/v1/actors/inspect": InspectActorResponseSchema,
  "/v1/actors/subscriptions/add": AddSubscriptionResponseSchema,
  "/v1/actors/subscriptions/remove": RemoveSubscriptionResponseSchema,
  "/v1/actors/subscriptions/resume": ResumeSubscriptionResponseSchema,
  "/v1/actors/checkpoint": CheckpointActorResponseSchema,
  "/v1/actors/invoke": InvokeActorResponseSchema,
};
const grpcSchemas = {
  CreateActor: CreateActorResponseSchema,
  UpdateActor: UpdateActorResponseSchema,
  InspectActor: InspectActorResponseSchema,
  AddSubscription: AddSubscriptionResponseSchema,
  RemoveSubscription: RemoveSubscriptionResponseSchema,
  ResumeSubscription: ResumeSubscriptionResponseSchema,
  CheckpointActor: CheckpointActorResponseSchema,
  InvokeActor: InvokeActorResponseSchema,
};
let requests = 0;
let active = 0;
let aborted = 0;
let holdInvoke = false;

function contentType(path) {
  return { ".js": "text/javascript", ".d.ts": "text/plain", ".wasm": "application/wasm", ".html": "text/html" }[extname(path)] ?? "application/octet-stream";
}

const server = http.createServer(async (request, response) => {
  console.log(request.method, request.url);
  response.setHeader("Access-Control-Allow-Origin", "*");
  response.setHeader("Access-Control-Allow-Headers", "content-type,x-grpc-web,grpc-timeout,x-user-agent,authorization");
  response.setHeader("Access-Control-Expose-Headers", "grpc-status,grpc-message");
  if (request.method === "OPTIONS") { response.writeHead(204); response.end(); return; }
  const pathname = new URL(request.url, "http://127.0.0.1").pathname;
  const query = new URL(request.url, "http://127.0.0.1").searchParams;
  if (request.method === "GET" && pathname === "/set-hold") {
    holdInvoke = query.get("value") === "1";
    response.writeHead(204); response.end(); return;
  }
  const schema = responseSchemas[pathname] ?? grpcSchemas[pathname.split("/").at(-1)];
  if (request.method === "POST" && schema) {
    requests += 1;
    if (holdInvoke && schema === InvokeActorResponseSchema) {
      active += 1;
      let settled = false;
      const release = () => { if (!settled) { settled = true; active -= 1; } };
      request.on("aborted", () => { aborted += 1; release(); });
      response.on("close", release);
      return;
    }
    const message = toBinary(schema, create(schema, {}));
    const frame = Buffer.alloc(5 + message.byteLength);
    frame.writeUInt32BE(message.byteLength, 1);
    Buffer.from(message).copy(frame, 5);
    const trailers = Buffer.from("grpc-status: 0\r\ngrpc-message: \r\n");
    const trailerFrame = Buffer.alloc(5 + trailers.byteLength);
    trailerFrame[0] = 0x80;
    trailerFrame.writeUInt32BE(trailers.byteLength, 1);
    trailers.copy(trailerFrame, 5);
    response.writeHead(200, { "content-type": "application/grpc-web+proto" });
    response.end(Buffer.concat([frame, trailerFrame]));
    return;
  }
  if (request.method === "GET" && pathname === "/browser.html") {
    const body = await readFile(join(root, "research/qualified-prototypes/actors-types-consumer-20261007/browser.html"));
    response.writeHead(200, { "content-type": "text/html" }); response.end(body); return;
  }
  if (request.method === "GET" && pathname.startsWith("/pkg/")) {
    const relative = normalize(pathname.slice("/pkg/".length));
    if (relative.startsWith("..")) { response.writeHead(403); response.end(); return; }
    const file = join(root, "typescript/packages/actors", relative);
    try { const body = await readFile(file); response.writeHead(200, { "content-type": contentType(file) }); response.end(body); }
    catch { response.writeHead(404); response.end(); }
    return;
  }
  if (request.method === "GET" && pathname === "/browser-abort.html") {
    const body = await readFile(join(root, "research/qualified-prototypes/actors-types-consumer-20261007/browser-abort.html"));
    response.writeHead(200, { "content-type": "text/html" }); response.end(body); return;
  }
  if (request.method === "GET" && pathname.startsWith("/buf/")) {
    const relative = normalize(pathname.slice("/buf/".length));
    if (relative.startsWith("..")) { response.writeHead(403); response.end(); return; }
    const file = join(root, "research/qualified-prototypes/actors-types-consumer-20261007/node_modules/@bufbuild/protobuf", relative);
    try { const body = await readFile(file); response.writeHead(200, { "content-type": contentType(file) }); response.end(body); }
    catch { response.writeHead(404); response.end(); }
    return;
  }
  if (request.method === "GET" && pathname === "/metrics") {
    response.writeHead(200, { "content-type": "application/json" }); response.end(JSON.stringify({ requests, active, aborted })); return;
  }
  response.writeHead(404); response.end();
});
server.listen(0, "127.0.0.1", () => {
  console.log(`http://127.0.0.1:${server.address().port}/browser.html`);
});
