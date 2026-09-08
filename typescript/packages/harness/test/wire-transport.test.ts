import { expect, test } from "bun:test";
import { create, toJsonString } from "@bufbuild/protobuf";
import {
  AdmissionSchema,
  AdmissionState,
  DeliverySchema,
  CommandEnvelopeSchema,
  HandshakeRequestSchema,
  HandshakeResponseSchema,
  ResumeRequestSchema,
  ServerFrameSchema,
} from "../src/proto.js";
import {
  EmbeddedWireTransport,
  GrpcWireTransport,
  HttpSseWireTransport,
  JsonlWireTransport,
  WebSocketWireTransport,
  TerminalAdmissionError,
  WireError,
  type JsonlChannel,
} from "../src/index.js";

const resume = create(ResumeRequestSchema, {});
const negotiation = create(HandshakeRequestSchema, {
  protocol: { version: "1", descriptorDigest: "digest" },
  required: { capabilities: [] },
});
const handshake = create(HandshakeResponseSchema, {
  protocol: negotiation.protocol,
  supported: { capabilities: [] },
});
const delivery = create(DeliverySchema, {
  authority: { kind: 2, id: "conversation-1" },
  generation: "generation-1",
  fromRevision: 0n,
  throughRevision: 0n,
  live: true,
});

test("embedded, JSONL, WebSocket and gRPC bridges expose the same wire delivery", async () => {
  const expected = [delivery];
  const embedded = new EmbeddedWireTransport({
    async handshake() { return handshake; },
    async *replay() { yield* expected; },
    async submit() {},
  }, negotiation);
  expect(await collect(await embedded.connect(resume))).toEqual(expected);

  const handshakeFrame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "handshake", value: handshake },
  }))}\n`;
  const frame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "delivery", value: delivery },
  }))}\n`;
  const lines = () => {
    const writes: string[] = [];
    const channel: JsonlChannel = {
      async write(line) { writes.push(line); },
      close() {},
      async *[Symbol.asyncIterator]() { yield handshakeFrame; yield frame; },
    };
    return { channel, writes };
  };
  const { channel, writes } = lines();
  const jsonl = new JsonlWireTransport(async () => channel, negotiation);
  expect(await collect(await jsonl.connect(resume))).toEqual(expected);
  expect(writes).toHaveLength(2);

  class FakeSocket extends EventTarget {
    readonly readyState = WebSocket.OPEN;
    sent = 0;
    send() {
      const data = this.sent++ === 0 ? handshakeFrame : frame;
      queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", { data })));
    }
    close() {}
  }
  const socket = new FakeSocket();
  const websocket = new WebSocketWireTransport(
    "ws://example.test/v1/harness",
    negotiation,
    () => socket as unknown as WebSocket,
  );
  const websocketConnection = await websocket.connect(resume);
  expect((await websocketConnection[Symbol.asyncIterator]().next()).value).toEqual(delivery);
  expect(socket.sent).toBe(2);

  const grpc = new GrpcWireTransport(negotiation, async () => handshake, async request => embedded.connect(request));
  expect(await collect(await grpc.connect(resume))).toEqual(expected);
});

test("HTTP rejection is terminal only with an authoritative admission", async () => {
  const encoder = new TextEncoder();
  const fetcher: typeof fetch = async input => {
    const url = String(input);
    if (url.endsWith("/handshake")) {
      return new Response(toJsonString(HandshakeResponseSchema, handshake));
    }
    if (url.endsWith("/replay")) {
      return new Response(new ReadableStream({ start(controller) { controller.enqueue(encoder.encode("")); } }));
    }
    return new Response(toJsonString(AdmissionSchema, create(AdmissionSchema, {
      operation: { operationId: "01010101-0101-0101-0101-010101010101", idempotencyKey: "one" },
      state: AdmissionState.REJECTED,
    })), { status: 400 });
  };
  const transport = new HttpSseWireTransport("https://example.test", negotiation, fetcher);
  const connection = await transport.connect(resume);
  const command = create(CommandEnvelopeSchema, {
    operation: { operationId: "01010101-0101-0101-0101-010101010101", idempotencyKey: "one" },
  });
  await expect(connection.send(command)).rejects.toBeInstanceOf(TerminalAdmissionError);
});

test("HTTP admission requires a complete echoed identity", async () => {
  const fetcher: typeof fetch = async input => {
    const url = String(input);
    if (url.endsWith("/handshake")) {
      return new Response(toJsonString(HandshakeResponseSchema, handshake));
    }
    if (url.endsWith("/replay")) return new Response(new ReadableStream());
    return new Response(toJsonString(AdmissionSchema, create(AdmissionSchema, {
      state: AdmissionState.ACCEPTED,
    })));
  };
  const connection = await new HttpSseWireTransport(
    "https://example.test",
    negotiation,
    fetcher,
  ).connect(resume);
  await expect(connection.send(create(CommandEnvelopeSchema, {
    operation: { operationId: "01010101-0101-0101-0101-010101010101", idempotencyKey: "one" },
  }))).rejects.toBeInstanceOf(WireError);
});

test("HTTP throttling remains indeterminate", async () => {
  const fetcher: typeof fetch = async input => {
    const url = String(input);
    if (url.endsWith("/handshake")) {
      return new Response(toJsonString(HandshakeResponseSchema, handshake));
    }
    if (url.endsWith("/replay")) return new Response(new ReadableStream());
    return new Response("retry later", { status: 429 });
  };
  const connection = await new HttpSseWireTransport(
    "https://example.test",
    negotiation,
    fetcher,
  ).connect(resume);
  await expect(connection.send(create(CommandEnvelopeSchema, {
    operation: { operationId: "01010101-0101-0101-0101-010101010101", idempotencyKey: "one" },
  }))).rejects.toBeInstanceOf(WireError);
});

test("clean JSONL EOF makes an unanswered command indeterminate", async () => {
  let finish!: () => void;
  const gate = new Promise<void>(resolve => { finish = resolve; });
  const channel: JsonlChannel = {
    async write() {},
    close() {},
    async *[Symbol.asyncIterator]() {
      yield `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
        frame: { case: "handshake", value: handshake },
      }))}\n`;
      await gate;
    },
  };
  const connection = await new JsonlWireTransport(async () => channel, negotiation).connect(resume);
  const pending = connection.send(create(CommandEnvelopeSchema, {
    operation: { operationId: "01010101-0101-0101-0101-010101010101", idempotencyKey: "one" },
  }));
  finish();
  await expect(pending).rejects.toBeInstanceOf(WireError);
});

async function collect<T>(values: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const value of values) result.push(value);
  return result;
}
