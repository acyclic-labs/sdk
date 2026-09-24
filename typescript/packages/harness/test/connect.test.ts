import { afterEach, expect, test } from "bun:test";
import { create, toJsonString } from "@bufbuild/protobuf";
import {
  DeliverySchema,
  HandshakeRequestSchema,
  HandshakeResponseSchema,
  ResumeRequestSchema,
  ServerFrameSchema,
  TransportKind,
} from "../src/proto.js";
import {
  WireError,
  connectHarness,
  type GrpcBridge,
} from "../src/index.js";

const resume = create(ResumeRequestSchema, {});
const negotiation = create(HandshakeRequestSchema, {
  protocol: { version: "1", descriptorDigest: "digest" },
  required: { capabilities: [] },
});
const handshake = (transports: Array<{ kind: TransportKind; url: string }> = []) =>
  create(HandshakeResponseSchema, {
    protocol: negotiation.protocol,
    supported: { capabilities: [] },
    transports,
  });
const handshakeFrame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
  frame: { case: "handshake", value: handshake() },
}))}\n`;
const frame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
  frame: { case: "delivery", value: create(DeliverySchema, { generation: "g" }) },
}))}\n`;

function fakeFetcher(response = handshake(), calls: string[] = []) {
  const fetcher = (async (input: unknown) => {
    const url = String(input);
    calls.push(url);
    if (url.endsWith("/handshake")) {
      return new Response(toJsonString(HandshakeResponseSchema, response));
    }
    return new Response(new ReadableStream<Uint8Array>());
  }) as typeof fetch;
  return fetcher;
}

class FakeSocket extends EventTarget {
  readonly readyState = WebSocket.OPEN;
  sent = 0;
  send() {
    const data = this.sent++ === 0 ? handshakeFrame : frame;
    queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", { data })));
  }
  close() {}
}

const originalWebSocket = globalThis.WebSocket;
afterEach(() => {
  globalThis.WebSocket = originalWebSocket;
  delete process.env.ACYCLIC_TRANSPORT;
});

test("servers that advertise nothing fall back to http-sse", async () => {
  const calls: string[] = [];
  const transport = connectHarness("https://harness.example", {
    negotiation,
    fetcher: fakeFetcher(handshake(), calls),
  });
  await transport.connect(resume);
  expect(transport.selected).toBe("http-sse");
  expect(calls).toContain("https://harness.example/v1/harness/replay");
});

test("auto prefers an advertised websocket when a factory is available", async () => {
  const calls: string[] = [];
  const socket = new FakeSocket();
  let opened = "";
  const transport = connectHarness("https://harness.example", {
    negotiation,
    fetcher: fakeFetcher(handshake([
      { kind: TransportKind.WEBSOCKET, url: "wss://harness.example/v1/harness/ws" },
      { kind: TransportKind.HTTP_SSE, url: "https://harness.example" },
    ]), calls),
    webSocketFactory: url => { opened = url; return socket as unknown as WebSocket; },
  });
  await transport.connect(resume);
  expect(transport.selected).toBe("websocket");
  expect(opened).toBe("wss://harness.example/v1/harness/ws");
});

test("websocket is skipped without a factory or global WebSocket", async () => {
  globalThis.WebSocket = undefined as unknown as typeof WebSocket;
  const calls: string[] = [];
  const transport = connectHarness("https://harness.example", {
    negotiation,
    fetcher: fakeFetcher(handshake([
      { kind: TransportKind.WEBSOCKET, url: "wss://harness.example/v1/harness/ws" },
    ]), calls),
  });
  await transport.connect(resume);
  expect(transport.selected).toBe("http-sse");
  expect(calls).toContain("https://harness.example/v1/harness/replay");
});

test("explicit grpc without a bridge is unsupported", () => {
  const transport = connectHarness("https://harness.example", {
    negotiation,
    transport: "grpc",
    fetcher: fakeFetcher(handshake([{ kind: TransportKind.GRPC, url: "https://harness.example:443" }])),
  });
  return expect(transport.connect(resume)).rejects.toMatchObject({ code: 3 });
});

test("advertised grpc uses the provided bridge", async () => {
  const calls: string[] = [];
  const bridge: GrpcBridge = {
    async handshake() { return handshake(); },
    async connect() {
      return {
        async send() {},
        async observe() { throw new Error("unused"); },
        async cancel() { throw new Error("unused"); },
        close() {},
        async *[Symbol.asyncIterator]() {},
      };
    },
  };
  const transport = connectHarness("https://harness.example", {
    negotiation,
    fetcher: fakeFetcher(handshake([
      { kind: TransportKind.GRPC, url: "https://grpc.example:443" },
    ]), calls),
    grpc: () => bridge,
  });
  await transport.connect(resume);
  expect(transport.selected).toBe("grpc");
});

test("ws:// endpoints skip discovery entirely", async () => {
  const calls: string[] = [];
  const socket = new FakeSocket();
  const transport = connectHarness("ws://harness.example/v1/harness/ws", {
    negotiation,
    fetcher: fakeFetcher(handshake(), calls),
    webSocketFactory: () => socket as unknown as WebSocket,
  });
  await transport.connect(resume);
  expect(transport.selected).toBe("websocket");
  expect(calls).toHaveLength(0);
});

test("ACYCLIC_TRANSPORT beats auto but an explicit option beats the env", async () => {
  const calls: string[] = [];
  process.env.ACYCLIC_TRANSPORT = "http-sse";
  const socket = new FakeSocket();
  const envTransport = connectHarness("https://harness.example", {
    negotiation,
    fetcher: fakeFetcher(handshake([
      { kind: TransportKind.WEBSOCKET, url: "wss://harness.example/v1/harness/ws" },
      { kind: TransportKind.HTTP_SSE, url: "https://harness.example" },
    ]), calls),
    webSocketFactory: () => socket as unknown as WebSocket,
  });
  await envTransport.connect(resume);
  expect(envTransport.selected).toBe("http-sse");

  const explicit = connectHarness("https://harness.example", {
    negotiation,
    transport: "websocket",
    fetcher: fakeFetcher(handshake([
      { kind: TransportKind.WEBSOCKET, url: "wss://harness.example/v1/harness/ws" },
      { kind: TransportKind.HTTP_SSE, url: "https://harness.example" },
    ])),
    webSocketFactory: () => socket as unknown as WebSocket,
  });
  await explicit.connect(resume);
  expect(explicit.selected).toBe("websocket");
});

test("an explicit transport that is not advertised is unsupported", () => {
  const transport = connectHarness("https://harness.example", {
    negotiation,
    transport: "websocket",
    fetcher: fakeFetcher(handshake([{ kind: TransportKind.HTTP_SSE, url: "https://harness.example" }])),
    webSocketFactory: () => new FakeSocket() as unknown as WebSocket,
  });
  return expect(transport.connect(resume)).rejects.toMatchObject({ code: 3 });
});

test("WireError is thrown for unsupported schemes", () => {
  const transport = connectHarness("ftp://harness.example", { negotiation });
  return expect(transport.connect(resume)).rejects.toBeInstanceOf(WireError);
});
