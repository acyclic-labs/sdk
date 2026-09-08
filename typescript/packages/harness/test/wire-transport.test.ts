import { expect, test } from "bun:test";
import { create, toJsonString } from "@bufbuild/protobuf";
import {
  AdmissionSchema,
  AdmissionState,
  CancelRequestSchema,
  CancelResponseSchema,
  CompletionState,
  DeliverySchema,
  CommandEnvelopeSchema,
  ErrorCode,
  ErrorSchema,
  HandshakeRequestSchema,
  HandshakeResponseSchema,
  ObserveRequestSchema,
  OperationStatusSchema,
  ResumeRequestSchema,
  ServerFrameSchema,
} from "../src/proto.js";
import {
  EmbeddedWireTransport,
  GrpcWireTransport,
  HttpSseWireTransport,
  JsonlWireTransport,
  WebSocketWireTransport,
  WireOperationHandle,
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
    async observe(request) {
      return create(OperationStatusSchema, {
        protocol: request.protocol,
        owner: request.owner,
        operation: { operationId: request.operationId },
        state: CompletionState.RUNNING,
      });
    },
    async cancel(request) {
      return create(CancelResponseSchema, {
        operation: {
          operationId: request.operationId,
          idempotencyKey: request.idempotencyKey,
        },
        status: {
          protocol: request.protocol,
          owner: request.owner,
          operation: { operationId: request.operationId },
          state: CompletionState.CANCELLED,
        },
      });
    },
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

test("gRPC control validates echoed operation, owner, protocol, error, and retry identity", async () => {
  const operationId = "01010101-0101-0101-0101-010101010101";
  const owner = { kind: 5, id: "owner" };
  const observe = create(ObserveRequestSchema, { owner, operationId });
  let invalidStatus = create(OperationStatusSchema, {
    protocol: negotiation.protocol,
    owner,
    operation: { operationId: "02020202-0202-0202-0202-020202020202" },
    state: CompletionState.RUNNING,
  });
  let invalidCancel = create(CancelResponseSchema, {
    operation: { operationId, idempotencyKey: "wrong-key" },
    status: {
      protocol: negotiation.protocol,
      owner,
      operation: { operationId },
      state: CompletionState.CANCELLED,
    },
  });
  const grpc = new GrpcWireTransport(negotiation, async () => handshake, async () => ({
    async send() {},
    async observe() { return invalidStatus; },
    async cancel() { return invalidCancel; },
    close() {},
    async *[Symbol.asyncIterator]() {},
  }));
  const connection = await grpc.connect(resume);
  await expect(connection.observe(observe)).rejects.toBeInstanceOf(WireError);
  invalidStatus = create(OperationStatusSchema, {
    protocol: { version: "wrong", descriptorDigest: "wrong" },
    owner: { kind: owner.kind, id: "wrong-owner" },
    operation: { operationId },
    state: CompletionState.INDETERMINATE,
    error: { code: ErrorCode.INDETERMINATE, message: "uncertain", operationId: "wrong-operation" },
  });
  await expect(connection.observe(observe)).rejects.toBeInstanceOf(WireError);
  await expect(connection.cancel(create(CancelRequestSchema, {
    owner,
    operationId,
    idempotencyKey: "cancel-1",
  }))).rejects.toBeInstanceOf(WireError);

  invalidStatus = create(OperationStatusSchema, {
    protocol: negotiation.protocol,
    owner,
    operation: { operationId },
    state: CompletionState.RUNNING,
  });
  invalidCancel = create(CancelResponseSchema, {
    operation: { operationId, idempotencyKey: "cancel-1" },
    status: {
      protocol: negotiation.protocol,
      owner,
      operation: { operationId },
      state: CompletionState.CANCELLED,
    },
  });
  expect((await connection.observe(observe)).operation?.operationId).toBe(operationId);
  expect((await connection.cancel(create(CancelRequestSchema, {
    owner,
    operationId,
    idempotencyKey: "cancel-1",
  }))).operation?.idempotencyKey).toBe("cancel-1");
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

test("HTTP observe and cancel preserve protocol, owner, and retry identity", async () => {
  const operationId = "01010101-0101-0101-0101-010101010101";
  const owner = { kind: 5, id: "owner" };
  const scope = {
    id: "control",
    capabilities: ["operation:observe", "operation:cancel"],
    issuer: "runtime",
    proof: new Uint8Array(32).fill(1),
  };
  const fetcher: typeof fetch = async (input, init) => {
    const url = String(input);
    if (url.endsWith("/handshake")) return new Response(toJsonString(HandshakeResponseSchema, handshake));
    if (url.endsWith("/replay")) return new Response(new ReadableStream());
    if (url.endsWith("/observe")) {
      return new Response(toJsonString(OperationStatusSchema, create(OperationStatusSchema, {
        protocol: negotiation.protocol,
        owner,
        operation: { operationId },
        state: CompletionState.RUNNING,
        revision: 4n,
      })));
    }
    const body = JSON.parse(String(init?.body)) as { idempotencyKey: string };
    return new Response(toJsonString(CancelResponseSchema, create(CancelResponseSchema, {
      operation: { operationId, idempotencyKey: body.idempotencyKey },
      status: {
        protocol: negotiation.protocol,
        owner,
        operation: { operationId },
        state: CompletionState.CANCELLED,
        revision: 5n,
      },
    })));
  };
  const connection = await new HttpSseWireTransport("https://example.test", negotiation, fetcher).connect(resume);
  const control = create(ObserveRequestSchema, { owner, operationId, scope });
  const handle = new WireOperationHandle(connection, control.owner!, operationId, control.scope!);
  const observed = await handle.observe();
  expect(observed.revision).toBe(4n);
  const cancelled = await handle.cancel("cancel-1", true);
  expect(cancelled.operation?.idempotencyKey).toBe("cancel-1");
  expect(cancelled.status?.state).toBe(CompletionState.CANCELLED);
});

test("framed status mismatch rejects instead of stranding observation", async () => {
  const operationId = "01010101-0101-0101-0101-010101010101";
  const mismatched = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "status", value: {
      protocol: negotiation.protocol,
      owner: { kind: 5, id: "wrong-owner" },
      operation: { operationId },
      state: CompletionState.RUNNING,
    } },
  }))}\n`;
  const handshakeFrame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "handshake", value: handshake },
  }))}\n`;
  class FakeSocket extends EventTarget {
    readonly readyState = WebSocket.OPEN;
    sent = 0;
    send() {
      this.sent += 1;
      if (this.sent === 1) queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", { data: handshakeFrame })));
      if (this.sent === 3) queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", { data: mismatched })));
    }
    close() {}
  }
  const connection = await new WebSocketWireTransport(
    "ws://example.test/v1/harness",
    negotiation,
    () => new FakeSocket() as unknown as WebSocket,
  ).connect(resume);
  const pending = connection.observe(create(ObserveRequestSchema, {
    owner: { kind: 5, id: "owner" },
    operationId,
    scope: { id: "control", capabilities: ["operation:observe"], issuer: "runtime", proof: new Uint8Array(32) },
  }));
  await expect(Promise.race([
    pending,
    new Promise((_, reject) => setTimeout(() => reject(new Error("observation timed out")), 100)),
  ])).rejects.toBeInstanceOf(WireError);
});

test("embedded status rejects an error correlated to another operation", async () => {
  const operationId = "01010101-0101-0101-0101-010101010101";
  const transport = new EmbeddedWireTransport({
    async handshake() { return handshake; },
    async *replay() {},
    async submit() {},
    async observe(request) {
      return create(OperationStatusSchema, {
        protocol: request.protocol,
        owner: request.owner,
        operation: { operationId: request.operationId },
        state: CompletionState.INDETERMINATE,
        error: {
          code: ErrorCode.INDETERMINATE,
          message: "uncertain",
          operationId: "02020202-0202-0202-0202-020202020202",
        },
      });
    },
    async cancel() { return create(CancelResponseSchema); },
  }, negotiation);
  const connection = await transport.connect(resume);
  await expect(connection.observe(create(ObserveRequestSchema, {
    owner: { kind: 5, id: "owner" },
    operationId,
    scope: { id: "control", capabilities: ["operation:observe"], issuer: "runtime", proof: new Uint8Array(32) },
  }))).rejects.toBeInstanceOf(WireError);
});

test("framed control serializes observe and cancel for the same operation", async () => {
  const operationId = "01010101-0101-0101-0101-010101010101";
  const handshakeFrame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "handshake", value: handshake },
  }))}\n`;
  const rejectedFrame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "error", value: {
      code: ErrorCode.UNAUTHORIZED,
      message: "scope proof is invalid",
      operationId,
    } },
  }))}\n`;
  class FakeSocket extends EventTarget {
    readonly readyState = WebSocket.OPEN;
    sent = 0;
    send() {
      this.sent += 1;
      if (this.sent === 1) queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", { data: handshakeFrame })));
      if (this.sent === 3) queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", { data: rejectedFrame })));
    }
    close() {}
  }
  const connection = await new WebSocketWireTransport(
    "ws://example.test/v1/harness",
    negotiation,
    () => new FakeSocket() as unknown as WebSocket,
  ).connect(resume);
  const owner = { kind: 5, id: "owner" };
  const scope = { id: "control", capabilities: ["operation:observe", "operation:cancel"], issuer: "runtime", proof: new Uint8Array(32) };
  const observed = connection.observe(create(ObserveRequestSchema, { owner, operationId, scope }));
  await expect(connection.cancel(create(CancelRequestSchema, {
    owner,
    operationId,
    scope,
    idempotencyKey: "cancel-1",
  }))).rejects.toThrow("operation control request is already pending");
  await expect(observed).rejects.toMatchObject({ code: ErrorCode.UNAUTHORIZED });
});

test("correlated framed errors do not abort another operation", async () => {
  const firstId = "01010101-0101-0101-0101-010101010101";
  const secondId = "02020202-0202-0202-0202-020202020202";
  const handshakeFrame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "handshake", value: handshake },
  }))}\n`;
  const rejectedFrame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "error", value: {
      code: ErrorCode.UNAUTHORIZED,
      message: "scope proof is invalid",
      operationId: firstId,
    } },
  }))}\n`;
  const acceptedFrame = `${toJsonString(ServerFrameSchema, create(ServerFrameSchema, {
    frame: { case: "status", value: {
      protocol: negotiation.protocol,
      owner: { kind: 5, id: "owner" },
      operation: { operationId: secondId },
      state: CompletionState.RUNNING,
      revision: 2n,
    } },
  }))}\n`;
  class FakeSocket extends EventTarget {
    readonly readyState = WebSocket.OPEN;
    sent = 0;
    send() {
      this.sent += 1;
      const response = this.sent === 1 ? handshakeFrame : this.sent === 3 ? rejectedFrame : this.sent === 4 ? acceptedFrame : undefined;
      if (response) queueMicrotask(() => this.dispatchEvent(new MessageEvent("message", { data: response })));
    }
    close() {}
  }
  const connection = await new WebSocketWireTransport(
    "ws://example.test/v1/harness",
    negotiation,
    () => new FakeSocket() as unknown as WebSocket,
  ).connect(resume);
  const request = (operationId: string) => create(ObserveRequestSchema, {
    owner: { kind: 5, id: "owner" },
    operationId,
    scope: { id: "control", capabilities: ["operation:observe"], issuer: "runtime", proof: new Uint8Array(32) },
  });
  const rejected = connection.observe(request(firstId));
  const accepted = connection.observe(request(secondId));
  await expect(rejected).rejects.toMatchObject({ code: ErrorCode.UNAUTHORIZED });
  expect((await accepted).operation?.operationId).toBe(secondId);
});

test("HTTP operation-control errors retain their canonical code", async () => {
  const fetcher: typeof fetch = async input => {
    const url = String(input);
    if (url.endsWith("/handshake")) return new Response(toJsonString(HandshakeResponseSchema, handshake));
    if (url.endsWith("/replay")) return new Response(new ReadableStream());
    return new Response(toJsonString(ErrorSchema, create(ErrorSchema, {
      code: ErrorCode.UNAUTHORIZED,
      message: "scope proof is invalid",
    })), { status: 403, headers: { "content-type": "application/json" } });
  };
  const connection = await new HttpSseWireTransport("https://example.test", negotiation, fetcher).connect(resume);
  const observed = connection.observe(create(ObserveRequestSchema, {
    owner: { kind: 5, id: "owner" },
    operationId: "01010101-0101-0101-0101-010101010101",
    scope: { id: "bad", capabilities: ["operation:observe"], issuer: "runtime", proof: new Uint8Array(32) },
  }));
  await expect(observed).rejects.toMatchObject({ code: ErrorCode.UNAUTHORIZED });
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
