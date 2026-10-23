import { afterAll, beforeAll, expect, test } from "bun:test";
import { Code, ConnectError } from "@connectrpc/connect";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { SessionFrameSchema, SessionFollowSchema, SessionDelivery } from "../generated/proto/stream/v1/session_pb.js";
import { GrpcStreamProvider, createStreamGrpcClient } from "../src/grpc.js";
import { HttpStreamProvider } from "../src/http.js";
import { SessionFilterSchema } from "../generated/proto/stream/v1/stream_pb.js";
import { SessionNode } from "./session-node.js";
import { grpcFixture, tlsIdentity } from "./session-grpc-fixture.js";
import type { SessionTransport } from "../src/session.js";

let identity: ReturnType<typeof tlsIdentity>;
let key: string;
let cert: string;
beforeAll(() => { identity = tlsIdentity(); ({ key, cert } = identity); });
afterAll(() => identity?.dispose());

test("public gRPC follows and unaries share one physical HTTP/2 session", async () => {
  const { node, counts, close, options } = await grpcFixture({ key, cert });
  try {
    const provider = new GrpcStreamProvider(options);
    expect(await provider.tail("unary")).toBe(0n);
    await provider.registerCredential(0, new Uint8Array([1, 2, 3]));
    const filter = create(SessionFilterSchema, { keys: [{ start: new Uint8Array([4]), end: new Uint8Array([5]) }] });
    const latest = provider.followData(create(SessionFollowSchema, { resource: "streams:/status", delivery: SessionDelivery.LATEST }), { from: 0n, filter })[Symbol.asyncIterator]();
    const firstStatus = latest.next();
    const follows = Array.from({ length: 64 }, (_, id) => provider.follow(`agent/${id}`, { from: 0n })[Symbol.asyncIterator]());
    const children = provider.children({ parent: "agent", limit: 1 })[Symbol.asyncIterator]();
    const discovered = children.next();
    const pending = Promise.all(follows.map(follow => follow.next()));
    await Promise.race([node.until(() => node.active === follows.length + 2), pending.then(() => {})]);
    for (let id = 0; id < follows.length; id += 1) node.append(`agent/${id}`);
    expect((await pending).every(result => result.value?.sequence === 0n)).toBe(true);
    const hello = node.received.find(frame => frame.body.case === "hello")!;
    expect(hello.body.case === "hello" && hello.body.value.credentials[0]).toEqual(new Uint8Array([1, 2, 3]));
    const interest = node.received.flatMap(frame => frame.body.case === "interest" ? frame.body.value.upsert : [])
      .find(subscription => subscription.kind.case === "follow" && subscription.kind.value.resource === "streams:/status");
    expect(interest?.filter).toEqual(filter);
    node.publishLatest("status", 3n);
    node.publishLatest("status", 9n, true);
    expect((await firstStatus).value?.after?.next).toBe(3n);
    const status = (await latest.next()).value!;
    expect(status.after?.next).toBe(9n);
    expect(status.accepted).toBe(11n);
    expect(status.durable).toBe(8n);
    expect(status.item.case === "latestValue" && status.item.value.attention).toBe(true);
    expect((await discovered).value?.path).toBe("agent/0");
    for (let id = 1; id < follows.length; id++) expect((await children.next()).value?.path).toBe(`agent/${id}`);
    const later = children.next(); node.append("agent/later");
    expect((await later).value?.path).toBe("agent/later");
    expect(counts.connections).toBe(1);
    expect(counts.sessions).toBe(1);
    expect(counts.legacyFollows).toBe(0);
    expect(node.actorChannels).toBe(0);
    await Promise.all(follows.map(follow => follow.return?.()));
    await children.return?.();
    await latest.return?.();
    await node.until(() => node.active === 0);
  } finally { await close(); }
});

test("raw gRPC follow deadlines remove only their logical channel", async () => {
  const { node, counts, close, options } = await grpcFixture({ key, cert });
  const controller = new AbortController();
  try {
    const client = createStreamGrpcClient(options);
    let header = "";
    let trailer = "";
    expect((await client.tail({ path: "unary" }, {
      timeoutMs: 500, headers: { "x-follow-options": "unary" },
      onHeader: headers => { header = headers.get("x-follow-options") ?? ""; },
      onTrailer: trailers => { trailer = trailers.get("x-follow-options") ?? ""; },
    })).tail).toBe(0n);
    expect(header).toBe("unary");
    expect(trailer).toBe("unary");
    const expired = client.follow({ path: "deadline", from: 0n }, { timeoutMs: 100 })[Symbol.asyncIterator]();
    const live = client.follow({ path: "live", from: 0n }, { timeoutMs: 0, signal: controller.signal })[Symbol.asyncIterator]();
    const expiry = expired.next().then(() => { throw new Error("deadline did not reject"); }, error => error);
    const first = live.next();
    await node.until(() => node.active === 2);
    const error = await expiry;
    expect(error).toBeInstanceOf(ConnectError);
    expect(error.code).toBe(Code.DeadlineExceeded);
    await node.until(() => node.active === 1);
    node.append("live");
    expect((await first).done).toBe(false);
    expect(counts.connections).toBe(1);
    expect(counts.sessions).toBe(1);
    expect(counts.legacyFollows).toBe(0);
    controller.abort();
    await live.return?.();
    await node.until(() => node.active === 0);
    expect(node.outstandingFrames).toBe(0);
  } finally { controller.abort(); await close(); }
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
    await provider.registerCredential(0, new Uint8Array([1, 2, 3]));
    const filter = create(SessionFilterSchema, { keys: [{ start: new Uint8Array([4]), end: new Uint8Array([5]) }] });
    const latest = provider.followData(create(SessionFollowSchema, { resource: "streams:/status", delivery: SessionDelivery.LATEST }), { from: 0n, filter })[Symbol.asyncIterator]();
    const firstStatus = latest.next();
    const follows = Array.from({ length: 64 }, (_, id) => provider.follow(`agent/${id}`, { from: 0n })[Symbol.asyncIterator]());
    const children = provider.children({ parent: "agent", limit: 1 })[Symbol.asyncIterator]();
    const discovered = children.next();
    const pending = Promise.all(follows.map(follow => follow.next()));
    await Promise.race([node.until(() => node.active === follows.length + 2), pending.then(() => {})]);
    for (let id = 0; id < follows.length; id += 1) node.append(`agent/${id}`);
    expect((await pending).every(result => result.value?.sequence === 0n)).toBe(true);
    const hello = node.received.find(frame => frame.body.case === "hello")!;
    expect(hello.body.case === "hello" && hello.body.value.credentials[0]).toEqual(new Uint8Array([1, 2, 3]));
    const interest = node.received.flatMap(frame => frame.body.case === "interest" ? frame.body.value.upsert : [])
      .find(subscription => subscription.kind.case === "follow" && subscription.kind.value.resource === "streams:/status");
    expect(interest?.filter).toEqual(filter);
    node.publishLatest("status", 3n);
    node.publishLatest("status", 9n, true);
    expect((await firstStatus).value?.after?.next).toBe(3n);
    const status = (await latest.next()).value!;
    expect(status.after?.next).toBe(9n);
    expect(status.accepted).toBe(11n);
    expect(status.durable).toBe(8n);
    expect(status.item.case === "latestValue" && status.item.value.attention).toBe(true);
    expect((await discovered).value?.path).toBe("agent/0");
    for (let id = 1; id < follows.length; id++) expect((await children.next()).value?.path).toBe(`agent/${id}`);
    const later = children.next(); node.append("agent/later");
    expect((await later).value?.path).toBe("agent/later");
    expect(connections).toBe(1); expect(sockets).toBe(1); expect(fetches).toBe(0);
    expect(node.actorChannels).toBe(0);
    await Promise.all(follows.map(follow => follow.return?.()));
    await children.return?.();
    await latest.return?.();
    await node.until(() => node.active === 0);
  } finally { await server.stop(true); }
});
