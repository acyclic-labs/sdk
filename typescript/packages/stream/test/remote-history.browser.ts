import fc from "fast-check";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { ReadRequestSchema, ReadResponseSchema, RecordBatchSchema, TailRequestSchema } from "../generated/proto/stream/v1/stream_pb.js";
import type { WasmStream } from "../generated/wasm/acyclic_stream_wasm.js";

// Advance only the request's timer clock while delivering real server bytes.
// No wall-clock delay, polling, replacement provider or second frame schema.
async function transfer<T>(operation: () => Promise<T>, gap: number, chunk: number, totalDeadline = false) {
  const fetch = globalThis.fetch;
  const setTimeout = globalThis.setTimeout;
  const clearTimeout = globalThis.clearTimeout;
  const timers = new Map<number, { at: number; fire: () => void }>();
  let elapsed = 0, nextTimer = 0, requests = 0, expired = false;
  globalThis.setTimeout = ((handler: TimerHandler, delay = 0, ...args: unknown[]) => {
    if (typeof handler !== "function") throw new Error("test requires a timer callback");
    const id = --nextTimer;
    timers.set(id, { at: elapsed + Number(delay), fire: () => { expired = true; handler(...args); } });
    return id;
  }) as typeof globalThis.setTimeout;
  globalThis.clearTimeout = ((id?: number) => {
    if (id !== undefined && id < 0) timers.delete(id);
    else clearTimeout(id);
  }) as typeof globalThis.clearTimeout;
  globalThis.fetch = async (input, init) => {
    requests++;
    const response = await fetch(input, init);
    const bytes = new Uint8Array(await response.arrayBuffer());
    const signal = init?.signal ?? (input instanceof Request ? input.signal : null);
    const broken = new AbortController();
    if (totalDeadline) globalThis.setTimeout(() => broken.abort(), 1000);
    let offset = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (offset === bytes.length) { controller.close(); return; }
        elapsed += gap;
        for (const [id, timer] of [...timers]) {
          if (timer.at <= elapsed) { timers.delete(id); timer.fire(); }
        }
        if (signal?.aborted || broken.signal.aborted) {
          controller.error(new DOMException("response exceeded total deadline", "AbortError"));
          return;
        }
        const end = Math.min(bytes.length, offset + chunk);
        controller.enqueue(bytes.subarray(offset, end));
        offset = end;
      },
    });
    return new Response(body, { status: response.status, headers: response.headers });
  };
  try {
    const value = await operation();
    return { value, error: undefined, elapsed, expired, requests };
  } catch (error) {
    return { value: undefined, error, elapsed, expired, requests };
  } finally {
    globalThis.fetch = fetch;
    globalThis.setTimeout = setTimeout;
    globalThis.clearTimeout = clearTimeout;
  }
}

export async function verifyHistoryTimeoutPolicy(stream: WasmStream) {
  const read = () => stream.read(toBinary(ReadRequestSchema, create(ReadRequestSchema, {
    path: "browser/events", from: 0n, limit: 2,
  })));
  const received = async (gap: number, chunk: number, broken: boolean) => {
    const result = await transfer(read, gap, chunk, broken);
    const records = result.value?.flatMap(page => fromBinary(RecordBatchSchema, fromBinary(ReadResponseSchema, page).data).records);
    return {
      ...result,
      complete: result.requests === 1 && result.elapsed > 1000 && result.error === undefined && records?.length === 2 &&
        records.every((record, index) => record.sequence === BigInt(index) && record.value[0] === index + 1),
    };
  };
  const schedules = [fc.integer({ min: 501, max: 999 }), fc.integer({ min: 1, max: 16 })] as const;
  // Every valid message must survive arbitrary response boundaries and a total
  // transfer time longer than the unary deadline, without losing its cursor.
  const property = (broken: boolean) => fc.asyncProperty(...schedules, async (gap, chunk) => (await received(gap, chunk, broken)).complete);
  await fc.assert(property(false), { numRuns: 32, seed: 710 });
  const control = await fc.check(property(true), { numRuns: 32, seed: 710 });
  if (!control.failed || !control.counterexample) throw new Error("total-deadline negative control escaped");
  const [gap, chunk] = control.counterexample;
  const failed = await received(gap, chunk, true);
  if (!failed.expired || failed.error === undefined || failed.complete) throw new Error("control did not catch a deadline abort");
  // The unary operation retains the actual production one-second deadline.
  const unary = await transfer(() => stream.dispatch("tail", toBinary(TailRequestSchema, create(TailRequestSchema, { path: "browser/events" }))), 600, 1);
  if (!unary.expired || unary.error === undefined) throw new Error("unary deadline was removed");
  return { schedules: 32, negativeControl: control.counterexample, unaryDeadline: true };
}
