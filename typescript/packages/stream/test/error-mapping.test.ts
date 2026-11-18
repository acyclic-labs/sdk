import { expect, spyOn, test } from "bun:test";
import { WasmStream } from "../generated/wasm/acyclic_stream_wasm.js";
import { ensureStreamWasm } from "../src/contract.js";
import { MemoryStreamProvider } from "../src/memory.js";
import { NativeStreamProvider } from "../src/native.js";
import { commitId, idempotencyKey, StreamError, type StreamProvider } from "../src/types.js";

const refusal = { status: 7, message: "refused", details: new Uint8Array([1, 2]), metadata: [] };
const cases = [
  ["not_found", "tail", "stream_not_found"],
  ["not_found", "read_commit", "commit_not_found"],
  ["already_exists", "tail", "destination_exists"],
  ["capacity", "tail", "capacity_exhausted"],
  ["prefix_not_retained", "commit", "invalid_argument"],
  ["prefix_not_retained", "tail", "prefix_not_retained"],
  ["access_denied", "tail", "access_denied"],
  ["configuration", "tail", "configuration"],
  ["unknown_code", "tail", "unavailable"],
  [undefined, "tail", "unavailable"],
] as const;

test.each(cases)("%s in %s maps to %s through native and WASM", async (code, operation, expected) => {
  await ensureStreamWasm();
  const metadata = code === undefined ? {} : { code, refusal };
  const failure = { message: "original", ...metadata };
  const wasmFailure = Object.assign(new Error("original"), metadata);
  const native = Reflect.construct(NativeStreamProvider, [{
    tailResult: async () => ({ error: failure }),
    readCommitResult: async () => ({ error: failure }),
    commitResult: async () => ({ error: failure }),
  }, {}]) as NativeStreamProvider;
  const wasm = new MemoryStreamProvider();
  const dispatch = spyOn(WasmStream.prototype, "dispatch").mockImplementation(async () => { throw wasmFailure; });
  const call = (provider: StreamProvider) => operation === "read_commit"
    ? provider.readCommit(commitId(new Uint8Array(32)))
    : operation === "commit"
      ? provider.commit({ effects: [{ conditions: [], mutations: [{ append: { path: "events", values: [new Uint8Array([1])] } }], soft: false }] },
        { idempotencyKey: idempotencyKey(new Uint8Array([1])) })
      : provider.tail("events");
  try {
    for (const provider of [native, wasm]) {
      const error = await call(provider).then(() => undefined, caught => caught);
      expect(error).toBeInstanceOf(StreamError);
      expect(error).toMatchObject({ code: expected, message: "original" });
      expect(error.refusal).toBe(code === undefined ? undefined : refusal);
    }
  } finally { dispatch.mockRestore(); }
});
