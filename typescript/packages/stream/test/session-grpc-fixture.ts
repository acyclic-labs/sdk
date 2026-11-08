import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { constants, createSecureServer, type ServerHttp2Session, type ServerHttp2Stream } from "node:http2";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import type { ConnectError } from "@connectrpc/connect";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import { StreamSessionService } from "../generated/proto/stream/v1/session_pb.js";
import { StreamService } from "../generated/proto/stream/v1/stream_pb.js";
import { SessionNode } from "./session-node.js";
import assert from "node:assert/strict";
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { SessionFrameSchema } from "../generated/proto/stream/v1/session_pb.js";

export async function grpcFixture(identity: { key: string; cert: string }, failure?: () => ConnectError | undefined) {
  const { key, cert } = identity;
  const node = new SessionNode();
  const counts = { connections: 0, sessions: 0, legacyFollows: 0 };
  const faults = { missingStatus: false };
  const connections = new Set<ServerHttp2Session>();
  const streams = new Set<ServerHttp2Stream>();
  const adapter = connectNodeAdapter({ routes(router) {
    router.service(StreamService, {
      tail(_request, context) {
        const echo = context.requestHeader.get("x-follow-options") ?? "";
        context.responseHeader.set("x-follow-options", echo);
        context.responseTrailer.set("x-follow-options", echo);
        return { tail: 0n };
      },
      async *follow() { counts.legacyFollows += 1; throw new Error("legacy Follow RPC is forbidden"); },
    });
    router.service(StreamSessionService, { async *session(input, context) {
      assert.equal(context.requestHeader.get("authorization"), "Bearer fixture");
      counts.sessions += 1;
      const rejected = failure?.();
      if (rejected) throw rejected;
      const transport = await node.connect(undefined, context.signal);
      const abort = (): void => transport.close();
      context.signal.addEventListener("abort", abort, { once: true });
      const pump = async (): Promise<void> => { try { for await (const value of input) await transport.send(toBinary(SessionFrameSchema, value)); } finally { transport.close(); } };
      void pump().catch(() => transport.close());
      try { for await (const frame of transport.incoming) yield fromBinary(SessionFrameSchema, frame); }
      finally { context.signal.removeEventListener("abort", abort); transport.close(); }
    } });
  } });
  const server = createSecureServer({ key, cert }, (request, response) => {
    if (faults.missingStatus) {
      assert.equal(request.headers.authorization, "Bearer fixture");
      counts.sessions += 1;
      response.writeHead(200, { "content-type": "application/grpc" });
      response.end();
      return;
    }
    adapter(request, response);
  });
  server.on("stream", stream => {
    streams.add(stream);
    stream.once("close", () => streams.delete(stream));
  });
  server.on("connection", socket => socket.setNoDelay(true));
  server.on("session", session => {
    counts.connections += 1;
    connections.add(session);
    session.once("close", () => connections.delete(session));
  });
  const close = ownFixtureServer(server);
  await new Promise<void>(resolve => server.listen(0, "localhost", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing HTTP/2 address");
  return { node, counts, faults, close, resetRpc() {
    for (const stream of streams) if (!stream.closed && !stream.destroyed) {
      stream.close(constants.NGHTTP2_CANCEL);
    }
  }, disconnect() { for (const session of connections) session.destroy(); },
    options: { endpoint: `https://localhost:${address.port}`, token: "fixture", caCertificate: cert } };
}


export function tlsIdentity() {
  const directory = mkdtempSync(join(tmpdir(), "stream-session-tls-"));
  const keyFile = join(directory, "key.pem");
  const certFile = join(directory, "cert.pem");
  execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost", "-keyout", keyFile, "-out", certFile], { stdio: "ignore" });
  return { key: readFileSync(keyFile, "utf8"), cert: readFileSync(certFile, "utf8"), dispose() { rmSync(directory, { recursive: true, force: true }); } };
}
