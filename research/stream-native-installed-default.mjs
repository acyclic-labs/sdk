import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { execFileSync } from "node:child_process";
import { join } from "node:path";
const root = "C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk";
const { connectNodeAdapter } = await import(new URL("./typescript/packages/stream/node_modules/@connectrpc/connect-node/dist/esm/index.js", `file:///${root}/`));
const proto = await import(new URL("./typescript/packages/stream/generated/proto/stream/v2/stream_pb.js", `file:///${root}/`));
const { Stream } = await import("@acyclic-labs/stream");
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
  const client = Stream.fromEnv({ endpoint: `https://localhost:${port}`, token: "default-token", caCertificate: new TextEncoder().encode(tls.certificate) });
  assert.equal(client.provider.constructor.name, "DefaultStreamProvider");
  const controller = new AbortController();
  const pending = (async () => { for await (const _record of client.bytes("accounts/events").follow({ from: 0n, signal: controller.signal })) {} })();
  const startDeadline = Date.now() + 2000;
  while (!started && Date.now() < startDeadline) await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(started, true, "default provider did not select native gRPC transport");
  controller.abort();
  await Promise.race([pending, new Promise((_, reject) => setTimeout(() => reject(new Error("default follow did not close")), 2000))]);
  const dropDeadline = Date.now() + 2000;
  while (!dropped && Date.now() < dropDeadline) await new Promise(resolve => setTimeout(resolve, 10));
  assert.equal(dropped, true, "native default follow did not release server stream");
  console.log(JSON.stringify({ schema: "acyclic.stream.native.installed-default.v1", transport: "grpc", defaultEntry: "Stream.fromEnv", nativeSelected: true, pendingFollowClosed: true, serverStreamDropped: true, installedPackage: "@acyclic-labs/stream" }));
} finally {
  await new Promise(resolve => server.close(resolve));
}
