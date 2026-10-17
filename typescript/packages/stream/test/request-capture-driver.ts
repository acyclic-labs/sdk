import { plugin } from "bun";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { create, toBinary } from "@bufbuild/protobuf";
import { ChildrenResponseSchema } from "../generated/proto/stream/v1/stream_pb.js";

const providerName = process.env.STREAM_CAPTURE_PROVIDER ?? "memory";
const broken = process.env.STREAM_CAPTURE_BROKEN === "1";
let entered!: () => void;
const started = new Promise<void>(yes => { entered = yes; });
let release!: () => void;
const held = new Promise<void>(yes => { release = yes; });
let captures = 0;
Object.assign(globalThis, { async captureBarrier() { entered(); await held; } });

plugin({ name: "children-request-capture", setup(build) {
  build.onResolve({ filter: /^@acyclic-labs\/actors\/(credential|observe)$/ }, args => ({
    path: fileURLToPath(new URL(`../../actors/src/${args.path.split("/").at(-1)}.ts`, import.meta.url)),
  }));
  build.onLoad({ filter: /[\\/]src[\\/]session\.ts$/ }, () => ({ loader: "ts", contents: `
    export class StreamSessions {
      async *children(request) {
        globalThis.recordInterest(request);
        yield { path: request.parent + "/child" };
        if (!request.signal?.aborted) await new Promise(resolve => request.signal?.addEventListener("abort", resolve, { once: true }));
      }
    }` }));
  build.onLoad({ filter: /[\\/]src[\\/](native|memory|http|grpc)\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    if (args.path.endsWith("native.ts")) source = source.replaceAll("const module = await loadNativeModule();", "const module = (globalThis as any).captureNative as typeof GeneratedNative;");
    const start = source.indexOf("  async *children(request:");
    const end = source.indexOf("  async childrenPage(", start);
    let children = source.slice(start, end);
    const capture = "const { parent, signal, limit } = request;";
    assert(children.includes(capture));
    const barrier = "await (globalThis as any).captureBarrier();";
    // Negative control captures after suspension, allowing caller mutation.
    children = children.replace(capture, broken ? `${barrier} ${capture}` : `${capture} ${barrier}`);
    return { loader: "ts", contents: source.slice(0, start) + children + source.slice(end) };
  });
} });

const original = new AbortController();
const replacement = new AbortController();
let parent = "agents";
let signal = original.signal;
let limit = 1;
const reads = { parent: 0, signal: 0, limit: 0 };
const request = {
  get parent() { reads.parent++; return parent; },
  get signal() { reads.signal++; return signal; },
  get limit() { reads.limit++; return limit; },
};
Object.assign(globalThis, { recordInterest(input: any) {
  captures++;
  assert.equal(input.parent, "agents", "registration must use the captured parent");
  assert.equal(input.limit, 1);
  assert.equal(input.signal, original.signal, "registration must use the captured signal");
} });

let nativeClosed = 0;
let cancel!: () => void;
const cancelled = new Promise<void>(yes => { cancel = yes; });
class NativeClient {
  abandonSessionAttempts() {}
  static async connectResult() { return { client: new NativeClient() }; }
  async openChildrenResult(_input: Uint8Array) {
    let delivered = false;
    return { follow: { async nextResult() {
      if (delivered) { await cancelled; return {}; }
      delivered = true;
      return { value: toBinary(ChildrenResponseSchema, create(ChildrenResponseSchema, { child: { path: "agents/child" } })) };
    }, async close() { nativeClosed++; cancel(); } } };
  }
}
Object.assign(globalThis, { captureNative: { NativeStreamClient: NativeClient, NativeStreamCancellation: class { cancel() { cancel(); } } } });

let provider: any;
if (providerName === "memory") {
  const { MemoryStreamProvider } = await import("../src/memory.js");
  provider = new MemoryStreamProvider();
  await provider.append("agents/child", [new Uint8Array([1])]);
} else if (providerName === "http") {
  const { HttpStreamProvider } = await import("../src/http.js");
  provider = new HttpStreamProvider({ endpoint: "https://fixture.invalid", token: "fixture" });
} else if (providerName === "grpc") {
  const { GrpcStreamProvider } = await import("../src/grpc.js");
  provider = new GrpcStreamProvider({ endpoint: "https://fixture.invalid", token: "fixture" });
} else {
  const { NativeStreamProvider } = await import("../src/native.js");
  provider = await NativeStreamProvider.connect("https://fixture.invalid", "fixture");
}
const cursor = provider.children(request)[Symbol.asyncIterator]();
const first = cursor.next();
await started;
assert.equal(reads.parent, 1, "captured parent must precede suspension");
parent = "other"; signal = replacement.signal; limit = 2;
release();
assert.equal((await first).value?.path, "agents/child", "delivery must validate against the captured parent");
const second = cursor.next();
original.abort();
assert.equal((await second).done, true, "original signal must still cancel discovery");
await cursor.return?.();
assert.deepEqual(reads, { parent: 1, signal: 1, limit: 1 }, "request fields must be read exactly once");
if (providerName === "http" || providerName === "grpc") assert.equal(captures, 1);
if (providerName === "native") assert(nativeClosed > 0);
console.log("captured request", providerName);
