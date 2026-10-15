import { afterAll, beforeAll, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createSecureServer } from "node:http2";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import { StreamSessionService, SessionFrameSchema } from "../generated/proto/stream/v1/session_pb.js";
import { StreamService } from "../generated/proto/stream/v1/stream_pb.js";
import { GrpcStreamProvider } from "../src/grpc.js";
import { HttpStreamProvider } from "../src/http.js";
import { SessionNode } from "./session-node.js";
import type { SessionTransport } from "../src/session.js";

let directory: string;
let key: string;
let cert: string;
beforeAll(() => {
  directory = mkdtempSync(join(tmpdir(), "stream-session-tls-"));
  const keyFile = join(directory, "key.pem");
  const certFile = join(directory, "cert.pem");
  execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost", "-keyout", keyFile, "-out", certFile], { stdio: "ignore" });
  key = readFileSync(keyFile, "utf8"); cert = readFileSync(certFile, "utf8");
});
afterAll(() => { if (directory) rmSync(directory, { recursive: true, force: true }); });

test("public gRPC follows and unaries share one physical HTTP/2 session", async () => {
  const node = new SessionNode();
  let connections = 0;
  let sessions = 0;
  let legacyFollows = 0;
  const server = createSecureServer({ key, cert }, connectNodeAdapter({ routes(router) {
    router.service(StreamService, {
      tail() { return { tail: 0n }; },
      async *follow() { legacyFollows += 1; throw new Error("legacy Follow RPC is forbidden"); },
    });
    router.service(StreamSessionService, { async *session(input, context) {
      expect(context.requestHeader.get("authorization")).toBe("Bearer fixture");
      sessions += 1;
      const transport = await node.connect(undefined, context.signal);
      const abort = (): void => transport.close();
      context.signal.addEventListener("abort", abort, { once: true });
      const pump = async (): Promise<void> => { try { for await (const value of input) await transport.send(value); } finally { transport.close(); } };
      void pump().catch(() => transport.close());
      try { yield* transport.incoming; }
      finally { context.signal.removeEventListener("abort", abort); transport.close(); }
    } });
  } }));
  server.on("connection", socket => socket.setNoDelay(true));
  server.on("session", () => { connections += 1; });
  const close = ownFixtureServer(server);
  try {
    await new Promise<void>(resolve => server.listen(0, "localhost", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("missing HTTP/2 address");
    const provider = new GrpcStreamProvider({ endpoint: `https://localhost:${address.port}`, token: "fixture", caCertificate: cert });
    expect(await provider.tail("unary")).toBe(0n);
    const follows = Array.from({ length: 64 }, (_, id) => provider.follow(`agent/${id}`, { from: 0n })[Symbol.asyncIterator]());
    const pending = Promise.all(follows.map(follow => follow.next()));
    await Promise.race([node.until(() => node.active === follows.length), pending.then(() => {})]);
    for (let id = 0; id < follows.length; id += 1) node.append(`agent/${id}`);
    expect((await pending).every(result => result.value?.sequence === 0n)).toBe(true);
    expect(connections).toBe(1);
    expect(sessions).toBe(1);
    expect(legacyFollows).toBe(0);
    expect(node.actorChannels).toBe(0);
    await Promise.all(follows.map(follow => follow.return?.()));
    await node.until(() => node.active === 0);
  } finally { await close(); }
});

test("public browser follows use one authenticated binary WebSocket without fetch polling", async () => {
  const node = new SessionNode();
  let connections = 0;
  let fetches = 0;
  let sockets = 0;
  const server = Bun.serve<{ transport: Promise<SessionTransport> }>({
    port: 0, hostname: "localhost", tls: { key, cert },
    fetch(request, server) {
      expect(new URL(request.url).pathname).toBe("/v1/stream/session");
      const protocols = request.headers.get("sec-websocket-protocol") ?? "";
      expect(protocols).toContain("acyclic.session.v1");
      expect(protocols).toContain(`acyclic.bearer.${Buffer.from("fixture").toString("base64url")}`);
      const transport = node.connect(undefined, new AbortController().signal);
      if (server.upgrade(request, { headers: { "Sec-WebSocket-Protocol": "acyclic.session.v1" }, data: { transport } })) return;
      return new Response("upgrade required", { status: 400 });
    },
    websocket: {
      async open(socket) {
        connections += 1;
        const transport = await socket.data.transport;
        try { for await (const value of transport.incoming) socket.send(toBinary(SessionFrameSchema, value)); }
        catch { socket.close(); }
      },
      async message(socket, input) {
        if (typeof input === "string") throw new Error("session messages must be binary");
        await (await socket.data.transport).send(fromBinary(SessionFrameSchema, input));
      },
      async close(socket) { (await socket.data.transport).close(); },
    },
  });
  try {
    const provider = new HttpStreamProvider({ endpoint: `https://localhost:${server.port}`, token: "fixture",
      fetcher: async () => { fetches += 1; throw new Error("follow issued an HTTP poll"); },
      webSocketFactory: (url, protocols) => { sockets += 1; return new WebSocket(url, { protocols, tls: { ca: cert } }); },
    });
    const follows = Array.from({ length: 64 }, (_, id) => provider.follow(`agent/${id}`, { from: 0n })[Symbol.asyncIterator]());
    const pending = Promise.all(follows.map(follow => follow.next()));
    await Promise.race([node.until(() => node.active === follows.length), pending.then(() => {})]);
    for (let id = 0; id < follows.length; id += 1) node.append(`agent/${id}`);
    expect((await pending).every(result => result.value?.sequence === 0n)).toBe(true);
    expect(connections).toBe(1); expect(sockets).toBe(1); expect(fetches).toBe(0);
    expect(node.actorChannels).toBe(0);
    await Promise.all(follows.map(follow => follow.return?.()));
    await node.until(() => node.active === 0);
  } finally { await server.stop(true); }
});
