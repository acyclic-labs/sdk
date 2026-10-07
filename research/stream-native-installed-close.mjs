import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { join } from "node:path";
const root = "C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk";
const { connectNodeAdapter } = await import(pathToFileURL(join(root, "typescript/packages/stream/node_modules/@connectrpc/connect-node/dist/esm/index.js")));
const { create, toBinary } = await import(pathToFileURL(join(root, "typescript/packages/stream/node_modules/@bufbuild/protobuf/dist/esm/index.js")));
const proto = await import(pathToFileURL(join(root, "typescript/packages/stream/generated/proto/stream/v2/stream_pb.js")));
const requireConsumer = createRequire(pathToFileURL(join(import.meta.dirname, "consumer/index.cjs")));
const binding = requireConsumer("@acyclic-labs/stream-native-qual");
assert.equal(binding.__napiBindingTarget, "native");
assert.equal(typeof binding.NativeStreamClient.connectWithCaResult, "function");
assert.equal(typeof binding.NativeStreamClient.prototype.openFollowResult, "function");
assert.equal(typeof binding.NativeStreamFollow.prototype.nextResult, "function");
assert.equal(typeof binding.NativeStreamFollow.prototype.close, "function");
let started = false;
let dropped = false;
const tls = JSON.parse(execFileSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: root, encoding: "utf8" }));
const adapter = connectNodeAdapter({ routes(router) {
  router.service(proto.StreamService, {
    async *follow(_request, context) {
      started = true;
      await new Promise(resolve => {
        const onAbort = () => { context.signal.removeEventListener("abort", onAbort); resolve(); };
        context.signal.addEventListener("abort", onAbort, { once: true });
      });
      dropped = true;
    },
  });
} });
const server = createSecureServer({ key: tls.key, cert: tls.certificate }, adapter);
await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "localhost", resolve); });
try {
  const port = server.address().port;
  const connected = await binding.NativeStreamClient.connectWithCaResult(`https://localhost:${port}`, "installed-token", Buffer.from(tls.certificate));
  assert.equal(connected.error, undefined, "installed consumer failed to connect");
  const client = connected.client;
  assert.ok(client);
  assert.equal(client.transport(), "grpc");
  const request = create(proto.FollowRequestSchema, { path: "accounts/events", from: 0n });
  const opened = await client.openFollowResult(Buffer.from(toBinary(proto.FollowRequestSchema, request)));
  assert.equal(opened.error, undefined, "installed consumer failed to open follow");
  const follow = opened.follow;
  assert.ok(follow);
  const pending = follow.nextResult();
  const startDeadline = Date.now() + 2000;
  while (!started && Date.now() < startDeadline) await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(started, true, "server follow did not start from installed nextResult");
  await follow.close();
  const closed = await Promise.race([pending, new Promise((_, reject) => setTimeout(() => reject(new Error("pending nextResult did not resolve after close")), 2000))]);
  assert.equal(closed.value, undefined);
  assert.equal(closed.error, undefined);
  const dropDeadline = Date.now() + 2000;
  while (!dropped && Date.now() < dropDeadline) await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(dropped, true, "server stream was not released after installed close");
  const closedAgain = await follow.nextResult();
  assert.equal(closedAgain.value, undefined);
  assert.equal(closedAgain.error, undefined);
  console.log(JSON.stringify({ schema: "acyclic.stream.native.installed-close.v1", transport: client.transport(), binding: binding.NativeStreamClient.version(), pendingNextResolved: true, serverStreamDropped: true, installedPackage: "@acyclic-labs/stream-native-qual" }));
} finally {
  await new Promise(resolve => server.close(resolve));
}


