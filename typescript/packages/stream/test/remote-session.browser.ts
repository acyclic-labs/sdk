import { HttpStreamProvider } from "../src/http.js";
import { streamResource } from "../src/contract.js";
import init, { WasmStream } from "../generated/wasm/acyclic_stream_wasm.js";
import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import { AppendRequestSchema, AppendResponseSchema, FollowRequestSchema, ForkRequestSchema, ReadRequestSchema, ReadResponseSchema, RecordBatchSchema, StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";
import { SessionDelivery, SessionFrameSchema, SessionSubscriptionSchema } from "../generated/proto/stream/v1/session_pb.js";
import { verifyHistoryTimeoutPolicy } from "./remote-history.browser.js";

export async function test(endpoint: string) {
  await init();
  let credentials = 0;
  const stream = WasmStream.connect(endpoint, () => { credentials++; return "browser-test"; });
  let mutation = 0;
  const key = () => new TextEncoder().encode(`browser:${++mutation}`);
  const append = async (value: number) => {
    const result = await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, { path: "browser/events", records: [{ value: new Uint8Array([value]) }], idempotencyKey: key() })));
    if (fromBinary(AppendResponseSchema, result).outcome.case !== "committed") throw new Error("append did not commit");
  };
  const sequence = (bytes: Uint8Array | null) => {
    if (bytes === null) throw new Error("follow ended before its delivery");
    const result = fromBinary(ReadResponseSchema, bytes);
    const records = fromBinary(RecordBatchSchema, result.data).records;
    if (records.length !== 1) throw new Error("expected one pushed record");
    return records[0].sequence;
  };
  await append(1);
  const follow = await stream.open_follow(toBinary(FollowRequestSchema, create(FollowRequestSchema, { path: "browser/events", from: 0n })));
  const sibling = await stream.open_follow(toBinary(FollowRequestSchema, create(FollowRequestSchema, { path: "browser/events", from: 1n })));
  if (sequence(await follow.next()) !== 0n) throw new Error("snapshot replay lost its cursor");
  const first = await stream.nextConnection(0n);
  if (await (await fetch(`${endpoint}/fixture/connections`)).text() !== "1") throw new Error("follows did not share one physical session");
  await fetch(`${endpoint}/fixture/recover`, { method: "POST" });
  const second = await stream.nextConnection(first);
  if (second <= first) throw new Error("reconnect without a record did not push its generation");
  await append(2);
  const [a, b] = await Promise.all([follow.next(), sibling.next()]);
  if (sequence(a) !== 1n || sequence(b) !== 1n) throw new Error("reconnect skipped or duplicated a consumed cursor");
  if (credentials < 4) throw new Error("credential resolver was cached across calls");
  const open = (delivery: SessionDelivery) => stream.openData(toBinary(SessionSubscriptionSchema, create(SessionSubscriptionSchema, {
    resume: { next: 0n }, kind: { case: "follow", value: { resource: streamResource("browser/status"), delivery } },
  })));
  const value = new TextEncoder().encode('{"phase":"done"}');
  await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, { path: "browser/status", records: [{ value, keys: [new Uint8Array([1])], attributes: [{ name: "attention", value: new TextEncoder().encode("true") }] }], idempotencyKey: key() })));
  const latest = await open(SessionDelivery.LATEST), all = await open(SessionDelivery.ALL);
  const latestFrame = fromBinary(SessionFrameSchema, (await latest.next())!);
  const allFrame = fromBinary(SessionFrameSchema, (await all.next())!);
  if (latestFrame.body.case !== "data" || latestFrame.body.value.item.case !== "latestValue" || !latestFrame.body.value.item.value.attention) throw new Error("LATEST lost its typed attention value");
  if (allFrame.body.case !== "data" || allFrame.body.value.item.case !== "record") throw new Error("ALL did not preserve the full record");
  if (await (await fetch(`${endpoint}/fixture/connections`)).text() !== "2") throw new Error("ALL and LATEST created another pool");
  // The response page exceeds a unary budget while every message remains bounded.
  const payload = new Uint8Array(65_536).fill(7);
  for (const count of [8, 8, 1]) {
    await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, {
      path: "browser/history", records: Array.from({ length: count }, () => ({ value: payload })), idempotencyKey: key(),
    })));
  }
  const pages = await stream.read(toBinary(ReadRequestSchema, create(ReadRequestSchema, { path: "browser/history", from: 0n, limit: 17 })));
  const records = pages.flatMap(page => fromBinary(RecordBatchSchema, fromBinary(ReadResponseSchema, page).data).records);
  const exact = records.length === 17 && records.every((record, index) => record.sequence === BigInt(index) && record.value.length === payload.length && record.value[0] === 7);
  if (!exact) throw new Error("large history was truncated by a unary response budget");
  // Exercise the public adapter through the actual binary host, with no JSON read route.
  let readObservations = 0;
  const portable = new HttpStreamProvider({ endpoint: "https://example.test", token: "browser-test",
    fetcher: async () => { throw new Error("finite read used the obsolete JSON page collector"); },
    webSocketFactory: (_url, protocols) => new WebSocket(`${endpoint.replace(/^http/, "ws")}/v1/stream/session`, protocols),
    observer: { onOperation(event) { if (event.op === "read") readObservations++; } },
  });
  let cursorReads = 0;
  const options = { get from() { cursorReads++; return 0n; }, limit: 18 };
  const finite = portable.read("browser/history", options)[Symbol.asyncIterator]();
  const firstRecord = await finite.next();
  if (firstRecord.done || firstRecord.value.sequence !== 0n || firstRecord.value.value.length !== payload.length) throw new Error("finite session read lost its first record");
  if (readObservations !== 0) throw new Error("finite read reported completion before its terminal");
  if (cursorReads !== 1) throw new Error("finite read captured its cursor more than once");
  if (firstRecord.value.committedAtMicros !== records[0]!.committedAtMicros || firstRecord.value.commitId.some((byte, index) => byte !== records[0]!.commitId[index])) throw new Error("finite session read changed record identity");
  if (firstRecord.value.origin !== (records[0]!.origin || "browser/history")) throw new Error("finite session read changed its appending stream");
  firstRecord.value.value.fill(99);
  await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, {
    path: "browser/history", records: [{ value: payload }], idempotencyKey: key(),
  })));
  let count = 1;
  for (;;) {
    const item = await finite.next();
    if (item.done) break;
    if (item.value.sequence !== BigInt(count) || item.value.value.length !== payload.length) throw new Error("finite session read changed its cursor or payload");
    count++;
  }
  if (count !== 17) throw new Error("finite session read included an append beyond its pinned end");
  if (readObservations !== 1) throw new Error("finite read observation was lost or counted per record");
  const abandoned = portable.read("browser/history", { from: 0n, limit: 18 })[Symbol.asyncIterator]();
  const retained = await abandoned.next();
  if (retained.done || retained.value.value[0] !== 7) throw new Error("returned bytes mutated retained history");
  await abandoned.return?.();
  if (readObservations !== 2) throw new Error("finite read cleanup did not report one terminal");
  await stream.dispatch("fork", toBinary(ForkRequestSchema, create(ForkRequestSchema, {
    source: "browser/history", destination: "browser/inherited", idempotencyKey: new TextEncoder().encode("browser-origin"),
  })));
  const inherited = portable.read("browser/inherited", { from: 0n, limit: 1 })[Symbol.asyncIterator]();
  try {
    const first = await inherited.next();
    if (first.done || first.value.origin !== "browser/history") throw new Error("finite session read lost the inherited appending stream");
  } finally { await inherited.return?.(); }
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      await portable.read("browser/history", { from: 20n, limit: 1 })[Symbol.asyncIterator]().next();
      throw new Error("finite read admitted an invalid cursor");
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "out_of_range") throw error;
    }
    if (attempt === 0) await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, {
      path: "browser/history", records: [{ value: payload }], idempotencyKey: key(),
    })));
  }
  const oldUnaryBudget = StreamLimit.MAX_COMMAND_BYTES + 16_384 + 10;
  if (records.reduce((sum, record) => sum + record.value.length, 0) <= oldUnaryBudget) throw new Error("large-history negative control did not exceed the former aggregate limit");
  await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, {
    path: "browser/overlap", records: Array.from({ length: 6 }, (_, index) => ({ value: new Uint8Array([index]) })), idempotencyKey: key(),
  })));
  for (const cut of [1n, 2n, 3n]) {
    const overlap = await stream.read(toBinary(ReadRequestSchema, create(ReadRequestSchema, { path: "browser/overlap", from: cut, limit: 3 })));
    const fresh = overlap.flatMap(page => fromBinary(RecordBatchSchema, fromBinary(ReadResponseSchema, page).data).records);
    if (fresh.length !== 3 || fresh.some((record, offset) => record.sequence !== cut + BigInt(offset) || record.value[0] !== Number(cut) + offset)) throw new Error("overlap consumed replayed bytes or skipped a fresh cursor");
  }
  const historyTimeouts = await verifyHistoryTimeoutPolicy(stream);
  follow.close(); sibling.close(); latest.close(); all.close(); stream.free();
  return { follows: 4, reconnects: 1, records: 5, modes: ["ALL", "LATEST"], history: records.length, historyTimeouts };
}
