import init, { WasmStream } from "../generated/wasm/acyclic_stream_wasm.js";
import { create, toBinary, fromBinary } from "@bufbuild/protobuf";
import { AppendRequestSchema, AppendResponseSchema, FollowRequestSchema, ReadRequestSchema, ReadResponseSchema, RecordBatchSchema, StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";
import { SessionDelivery, SessionFrameSchema, SessionSubscriptionSchema, SessionLatestValueSchema } from "../generated/proto/stream/v1/session_pb.js";
import { verifyHistoryTimeoutPolicy } from "./remote-history.browser.js";

export async function test(endpoint: string) {
  await init();
  let credentials = 0;
  const stream = WasmStream.connect(endpoint, () => { credentials++; return "browser-test"; });
  const append = async (value: number) => {
    const result = await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, { path: "browser/events", records: [new Uint8Array([value])] })));
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
    resume: { next: 0n }, kind: { case: "follow", value: { path: "browser/status", delivery } },
  })));
  const value = toBinary(SessionLatestValueSchema, create(SessionLatestValueSchema, { key: new Uint8Array([1]), value: new Uint8Array([7]), attention: true }));
  await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, { path: "browser/status", records: [value] })));
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
      path: "browser/history", records: Array.from({ length: count }, () => payload),
    })));
  }
  const pages = await stream.read(toBinary(ReadRequestSchema, create(ReadRequestSchema, { path: "browser/history", from: 0n, limit: 17 })));
  const records = pages.flatMap(page => fromBinary(RecordBatchSchema, fromBinary(ReadResponseSchema, page).data).records);
  const exact = records.length === 17 && records.every((record, index) => record.sequence === BigInt(index) && record.value.length === payload.length && record.value[0] === 7);
  if (!exact) throw new Error("large history was truncated by a unary response budget");
  const oldUnaryBudget = StreamLimit.MAX_COMMAND_BYTES + 16_384 + 10;
  if (records.reduce((sum, record) => sum + record.value.length, 0) <= oldUnaryBudget) throw new Error("large-history negative control did not exceed the former aggregate limit");
  await stream.dispatch("append", toBinary(AppendRequestSchema, create(AppendRequestSchema, {
    path: "browser/overlap", records: Array.from({ length: 6 }, (_, index) => new Uint8Array([index])),
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
