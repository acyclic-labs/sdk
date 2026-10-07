import assert from "node:assert/strict";
import net from "node:net";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { createRequire } from "node:module";
import { Stream } from "@acyclic-labs/stream";
const require = createRequire(import.meta.url);
const unhandled = [];
const uncaught = [];
process.on("unhandledRejection", reason => { unhandled.push(String(reason)); });
process.on("uncaughtException", error => { uncaught.push(String(error)); });
let accepted = false;
let closed = false;
const sockets = new Set();
const server = net.createServer(socket => {
  accepted = true;
  sockets.add(socket);
  socket.resume();
  socket.on("close", () => { closed = true; sockets.delete(socket); });
});
await new Promise((resolvePromise, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolvePromise); });
try {
  const port = server.address().port;
  const client = Stream.fromEnv({ endpoint: `https://127.0.0.1:${port}`, token: "stalled" });
  const controller = new AbortController();
  const startedAt = Date.now();
  let settled = false;
  const pending = (async () => {
    for await (const _record of client.bytes("accounts/events").follow({ from: 0n, signal: controller.signal })) {}
    settled = true;
  })();
  const acceptDeadline = Date.now() + 5000;
  while (!accepted && Date.now() < acceptDeadline) await new Promise(resolvePromise => setTimeout(resolvePromise, 10));
  assert.equal(accepted, true, "native gRPC open did not reach stalled TLS endpoint");
  controller.abort();
  await Promise.race([pending, new Promise((_, reject) => setTimeout(() => reject(new Error("aborted native gRPC open did not settle")), 5000))]);
  assert.equal(settled, true);
  const closeDeadline = Date.now() + 5000;
  while (sockets.size !== 0 && Date.now() < closeDeadline) await new Promise(resolvePromise => setTimeout(resolvePromise, 10));
  assert.equal(sockets.size, 0, "aborted native gRPC connect left the stalled TLS socket open");
  let invalidAccepted = false;
  const invalidServer = net.createServer(() => { invalidAccepted = true; });
  await new Promise((resolvePromise, reject) => { invalidServer.once("error", reject); invalidServer.listen(0, "127.0.0.1", resolvePromise); });
  try {
    const invalidPort = invalidServer.address().port;
    const invalidClient = Stream.fromEnv({ endpoint: `https://127.0.0.1:${invalidPort}`, token: String.fromCharCode(13, 10) });
    await assert.rejects(invalidClient.bytes("accounts/events").tail());
    await new Promise(resolvePromise => setTimeout(resolvePromise, 200));
    assert.equal(invalidAccepted, false, "invalid bearer reached the endpoint");
  } finally {
    await new Promise(resolvePromise => invalidServer.close(resolvePromise));
  }
  const packageEntry = require.resolve("@acyclic-labs/stream");
  const packageRoot = resolve(dirname(packageEntry), "..");
  const metadataPath = resolve(packageRoot, "generated/native/native-targets.json");
  const metadata = JSON.parse(readFileSync(metadataPath, "utf8"));
  const loadedPath = Object.keys(require.cache).find(path => path.endsWith(".node") && path.startsWith(packageRoot));
  assert.ok(loadedPath, "no native .node entry appeared in require cache");
  const loadedHash = createHash("sha256").update(readFileSync(loadedPath)).digest("hex");
  assert.equal(loadedHash, metadata.artifact.sha256.replace(/^sha256:/, ""), "loaded native bytes differ from manifest artifact");
  assert.equal(metadata.source_revision, "486bd26ac1f9c8c9a88b13a70bdcf49d5637ecb1");
  assert.equal(metadata.source_sha256, "sha256:2b8aa6ad6289370480b7b8a5725f83fa7d84b761535b595af2d0c3eaeffd8312");
  assert.equal(unhandled.length, 0);
  assert.equal(uncaught.length, 0);
  console.log(JSON.stringify({ schema: "acyclic.stream.native.installed-connect-cancellation.hashguard.v1", packageEntry, loadedPath, loadedHash, manifestArtifact: metadata.artifact.sha256, sourceRevision: metadata.source_revision, sourceSha256: metadata.source_sha256, nativeGrpcAttempted: true, stalledTlsAbortSettled: true, acceptedSocketsClosed: true, activeSocketCount: sockets.size, elapsedMs: Date.now() - startedAt, socketClosedByAbort: closed, invalidBearerRejectedBeforeNetwork: !invalidAccepted, unhandledRejectionCount: unhandled.length, uncaughtExceptionCount: uncaught.length }));
} finally {
  for (const socket of sockets) socket.destroy();
  await new Promise(resolvePromise => server.close(resolvePromise));
}
