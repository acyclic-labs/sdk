import { expect, test } from "bun:test";
import { create, fromBinary, toBinary, type MessageInitShape } from "@bufbuild/protobuf";
import { SessionFrameSchema } from "../../stream/generated/proto/stream/v1/session_pb.js";
import { InvokeActorRequestSchema, InvokeActorResponseSchema } from "../../actors/generated/proto/actors/v1/actors_pb.js";
import { initializeClientQueries } from "../src/client-query.js";
import { AgentGuest, WasmAgentCommand, WasmAgentProviders, WasmAgentServer } from "../generated/wasm/acyclic_harness_wasm.js";
import { agentsFixture } from "./browser-agents-fixture.mjs";
import { localSessionAuthority } from "./session-authority.js";
import { serveGrpcSession } from "../src/session-host.js";

const encoder = new TextEncoder();
function envelope(frame: MessageInitShape<typeof SessionFrameSchema>) {
  const message = toBinary(SessionFrameSchema, create(SessionFrameSchema, frame));
  const bytes = new Uint8Array(message.length + 5);
  new DataView(bytes.buffer).setUint32(1, message.length);
  bytes.set(message, 5);
  return bytes;
}

async function fixture() {
  const id = "00112233-4455-6677-8899-aabbccddeeff";
  const model = await agentsFixture(id, async function* () {}, initializeClientQueries);
  const { authority, credential } = await localSessionAuthority([{ name: "actors:/agents", prefix: false, actions: ["actors/invoke"] }]);
  const providers = WasmAgentProviders.memory(model.options);
  const audience = { kind: "task", id };
  const grants = ["interaction:route", `filesystem:${JSON.stringify({ layers: [[{
    actions: ["list", "read", "create", "write", "append", "delete", "rename", "fork", "merge-into", "read-history", "lock", "admin"],
    resource: { lineage_root: null, volume: null, generations: "all", path: "**" },
  }]], expires_at: null })}`];
  const scope = model.owner.issueScope("session-host", grants, undefined);
  const guest = AgentGuest.open(providers, model.options, model.owner, model.registry, { kind: "admitted", audience, scope });
  const host = WasmAgentServer.open(providers, model.options, model.owner, model.registry,
    { audience, scope, account: [...encoder.encode("test")], namespace: "agents" }, authority,
    { manifest: guest.manifest(), onRecord: (delivery, signal, interrupt) => guest.onRecord(delivery, signal, interrupt),
      policy: () => true, admitted: async () => {} });
  const root = await host.create("session-root");
  return { host, root, credential, free() { host.close(); host.free(); guest.free(); providers.free(); authority.free(); model.free(); credential.fill(0); } };
}

function request(input: ReadableStream<Uint8Array>, signal?: AbortSignal) {
  return new Request("https://deployment.invalid/acyclic.stream.v1.StreamSessionService/Session", {
    method: "POST", headers: { "content-type": "application/grpc+proto", "te": "trailers" }, body: input, signal,
    duplex: "half",
  } as RequestInit);
}

test("guest gRPC body serves canonical ActorCall while upload remains open and closes every wait", async () => {
  const f = await fixture();
  let upload!: ReadableStreamDefaultController<Uint8Array>;
  const input = new ReadableStream<Uint8Array>({ start(controller) { upload = controller; } }, { highWaterMark: 0 });
  const opened = (next: (signal: AbortSignal) => Promise<Uint8Array | null>) => Promise.resolve(f.host.grpcSession(next));
  const response = await serveGrpcSession({ grpcSession: opened }, request(input));
  const command = new WasmAgentCommand(encoder.encode('{"kind":"root"}'));
  const reader = response.body.getReader();
  const read = async () => {
    const part = await reader.read();
    expect(part.done).toBe(false);
    const bytes = part.value!;
    expect(bytes[0]).toBe(0);
    expect(new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(1)).toBe(bytes.length - 5);
    return fromBinary(SessionFrameSchema, bytes.subarray(5));
  };
  try {
    const hello = envelope({ body: { case: "hello", value: { version: 1, clientId: crypto.getRandomValues(new Uint8Array(16)),
      receiveBytes: 1048576n, receiveFrames: 16, credentials: [f.credential] } } });
    // Exercise actual split headers and coalesced input, not a transport fixture.
    upload.enqueue(hello.subarray(0, 2)); upload.enqueue(hello.subarray(2));
    expect((await read()).body.case).toBe("welcome");
    const invocation = fromBinary(InvokeActorRequestSchema, f.host.binding());
    invocation.body = command.bytes();
    upload.enqueue(envelope({ body: { case: "interest", value: { revision: 1n, upsert: [{ channel: { channelId: 1n, generation: 1n },
      resume: { next: 0n }, credential: 0, kind: { case: "actorCall", value: toBinary(InvokeActorRequestSchema, invocation) } }] } } }));
    for (;;) {
      const frame = await read();
      if (frame.body.case === "opened") {
        upload.enqueue(envelope({ channelId: 1n, generation: 1n, body: { case: "window", value: { bytes: 1048576n, frames: 16 } } }));
      } else if (frame.body.case === "data") {
        const data = frame.body.value;
        expect(data.item.case).toBe("actorCall");
        if (data.item.case !== "actorCall") throw new Error("unexpected session data");
        expect(data.after?.next).toBe(1n);
        expect(new DataView(data.after!.opaque.buffer, data.after!.opaque.byteOffset).getBigUint64(0, true)).toBe(BigInt(data.item.value.length));
        const result = fromBinary(InvokeActorResponseSchema, data.item.value);
        expect(result.status).toBe(200);
        expect(command.reply(result.body)).toEqual({ kind: "root", agent: f.root });
        break;
      } else if (frame.body.case === "closed") throw new Error(`call closed: ${frame.body.value.code}`);
    }
    response.close();
    await expect(reader.read()).rejects.toMatchObject({ code: "unavailable" });
    expect((await response.trailers).get("grpc-status")).toBe("14");
  } finally { response.close(); reader.releaseLock(); command.free(); f.free(); }
});

test("guest Session producer failure and owner loss end output with typed trailers", async () => {
  for (const loss of ["input", "owner", "request"]) {
    const f = await fixture();
    const entered = Promise.withResolvers<void>();
    let upload!: ReadableStreamDefaultController<Uint8Array>;
    const input = new ReadableStream<Uint8Array>({ start(controller) { upload = controller; }, pull() { entered.resolve(); } }, { highWaterMark: 0 });
    const abort = new AbortController();
    const response = await serveGrpcSession({ grpcSession: next => Promise.resolve(f.host.grpcSession(next)) }, request(input, abort.signal));
    const reader = response.body.getReader();
    try {
      const pending = reader.read();
      await entered.promise;
      if (loss === "input") upload.error(new Error("original producer ended"));
      else if (loss === "owner") f.host.close();
      else abort.abort();
      await expect(pending).rejects.toMatchObject({ code: "unavailable" });
      expect((await response.trailers).get("grpc-status")).toBe("14");
    } finally { response.close(); reader.releaseLock(); f.free(); }
  }
});

test("guest gRPC Session distinguishes complete upload EOF from truncated envelopes", async () => {
  for (const ending of ["complete", "header", "body"]) {
    const f = await fixture();
    let upload!: ReadableStreamDefaultController<Uint8Array>;
    const input = new ReadableStream<Uint8Array>({ start(controller) { upload = controller; } }, { highWaterMark: 0 });
    const response = await serveGrpcSession({ grpcSession: next => Promise.resolve(f.host.grpcSession(next)) }, request(input));
    const reader = response.body.getReader();
    try {
      upload.enqueue(envelope({ body: { case: "hello", value: { version: 1, clientId: crypto.getRandomValues(new Uint8Array(16)),
        receiveBytes: 1048576n, receiveFrames: 16, credentials: [f.credential] } } }));
      const welcome = await reader.read();
      expect(welcome.done).toBe(false);
      expect(fromBinary(SessionFrameSchema, welcome.value!.subarray(5)).body.case).toBe("welcome");
      const pending = reader.read();
      if (ending === "header") upload.enqueue(new Uint8Array([0, 0]));
      else if (ending === "body") upload.enqueue(new Uint8Array([0, 0, 0, 0, 2, 8]));
      upload.close();
      if (ending === "complete") {
        expect((await pending).done).toBe(true);
        expect((await response.trailers).get("grpc-status")).toBe("0");
      } else {
        await expect(pending).rejects.toMatchObject({ code: "invalid_response" });
        expect((await response.trailers).get("grpc-status")).toBe("15");
      }
    } finally { response.close(); reader.releaseLock(); f.free(); }
  }
});
