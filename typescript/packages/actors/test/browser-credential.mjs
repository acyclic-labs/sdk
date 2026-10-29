// Default browser binding over real loopback gRPC-Web requests and trailers.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { unlink } from "node:fs/promises";
import { createServer } from "node:http";
import { fromBinary } from "@bufbuild/protobuf";
import { InspectActorRequestSchema } from "../generated/proto/actors/v1/actors_pb.js";
import { browserTest } from "../../../../scripts/browser-test.mjs";

try {
if (typeof Bun !== "undefined") {
  const built = await Bun.build({
    entrypoints: [new URL("browser-credential.browser.ts", import.meta.url).pathname],
    outdir: import.meta.dirname, naming: "browser-credential.browser.js", target: "browser",
    plugins: [{ name: "existing-bindings", setup(build) {
      build.onResolve({ filter: /acyclic_actors_wasm\.js$/ }, () => ({ path: "../generated/wasm/acyclic_actors_wasm.js", external: true }));
      build.onResolve({ filter: /generated\/native\/binding\.cjs$/ }, () => ({ path: "../generated/native/binding.cjs", external: true }));
    } }],
  });
  if (!built.success) throw new AggregateError(built.logs, "step=browser-credential-bundle; recovery: bun typescript/packages/actors/test/browser-credential.mjs");
  const child = spawn("node", [new URL(import.meta.url).pathname], { stdio: "inherit", windowsHide: true });
  const code = await new Promise((resolve, reject) => { child.once("error", reject); child.once("exit", resolve); });
  await unlink(new URL("browser-credential.browser.js", import.meta.url));
  if (code !== 0) throw new Error("step=browser-credential-execution; recovery: bun typescript/packages/actors/test/browser-credential.mjs");
} else {
  let requests = 0, failure;
  const server = createServer((request, response) => {
    response.setHeader("access-control-allow-origin", "*");
    response.setHeader("access-control-allow-headers", "authorization,content-type,x-grpc-web,x-user-agent");
    if (request.method === "OPTIONS") { response.writeHead(204).end(); return; }
    const chunks = [];
    request.on("data", chunk => chunks.push(chunk));
    request.on("end", () => {
      try {
        assert.equal(request.url, "/acyclic.actors.v1.ActorsService/InspectActor");
        const body = Buffer.concat(chunks);
        assert.equal(body[0], 0);
        assert.equal(body.readUInt32BE(1), body.length - 5);
        assert.equal(fromBinary(InspectActorRequestSchema, body.subarray(5)).actorId, "credential-control");
        const rejected = requests++ === 0;
        assert.equal(request.headers.authorization, rejected ? "Bearer synthetic-rejected" : "Bearer synthetic-current");
        const trailers = Buffer.from(`grpc-status: ${rejected ? 16 : 7}\r\ngrpc-message: credential-control\r\n`);
        const frame = Buffer.alloc(5 + trailers.length);
        frame[0] = 0x80;
        frame.writeUInt32BE(trailers.length, 1);
        trailers.copy(frame, 5);
        response.writeHead(200, { "content-type": "application/grpc-web+proto" }).end(frame);
      } catch (error) { failure = error; response.destroy(); }
    });
  });
  try {
    await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
    const result = await browserTest("typescript/packages/actors/test/browser-credential.browser.js", `http://127.0.0.1:${server.address().port}`);
    assert.ifError(failure);
    assert.equal(requests, 2);
    assert.equal(result.refreshes, 2);
    assert.equal(result.transport, "grpc-web");
    console.log(JSON.stringify({ status: "passed", ...result }));
  } finally {
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
}
} catch (error) {
  console.error("step=browser-credential; recovery: bun typescript/packages/actors/test/browser-credential.mjs");
  throw error;
}
