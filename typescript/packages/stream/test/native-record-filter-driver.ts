import { plugin } from "bun";
import assert from "node:assert/strict";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/stream_pb.js";

const fault = process.env.STREAM_RECORD_FAULT;
plugin({ name: "native-filter-boundary", setup(build) {
  build.onLoad({ filter: /[\\/]src[\\/]native\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    const loader = "const module = await loadNativeModule();";
    assert(source.includes(loader));
    source = source.replaceAll(loader, "const module = (globalThis as any).nativeRecordBinding as typeof GeneratedNative;");
    if (fault === "dropped-filter") {
      const filter = "filter: structuredClone(options.filter)";
      assert(source.includes(filter)); source = source.replace(filter, "filter: undefined");
    }
    if (fault === "consecutive") {
      const check = "checkedRecord(decoded, decoded.sequence)";
      assert(source.includes(check)); source = source.replace(check, "checkedRecord(decoded, next)");
    }
    return { contents: source, loader: "ts" };
  });
  build.onLoad({ filter: /[\\/]src[\\/]record\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    if (fault === "dropped-headers") {
      const headers = ", keys: record.keys, attributes: record.attributes";
      assert(source.includes(headers)); source = source.replace(headers, "");
    }
    return { contents: source, loader: "ts" };
  });
} });
const filter = create(wire.SessionFilterSchema, { keys: [{ start: new Uint8Array([4]), end: new Uint8Array([5]) }] });
let closed = 0;
let registered = false;
class Client {
  static async connectResult() { return { client: new Client() }; }
  registerCredential(slot: number, credential: Uint8Array) {
    assert.equal(slot, 2); assert.deepEqual(new Uint8Array(credential), new Uint8Array([7, 8])); registered = true;
  }
  async openFollowResult(bytes: Uint8Array) {
    const request = fromBinary(wire.FollowRequestSchema, bytes);
    assert.deepEqual(request.filter, filter, "native follow dropped its requested filter");
    let index = 0;
    return { follow: {
      async nextResult() {
        const sequence = [2n, 5n, 5n][index++];
        if (sequence === undefined) return {};
        const record = create(wire.RecordSchema, { sequence, value: new Uint8Array([1]), commitId: new Uint8Array(32).fill(6),
          keys: [new Uint8Array([4])], attributes: [{ name: "owner", value: new Uint8Array([9]) }] });
        const data = toBinary(wire.RecordBatchSchema, create(wire.RecordBatchSchema, { records: [record] }));
        return { value: toBinary(wire.ReadResponseSchema, create(wire.ReadResponseSchema, { codec: wire.Codec.NONE, data, decodedLength: BigInt(data.length) })) };
      },
      async close() { closed++; },
    } };
  }
}
Object.assign(globalThis, { nativeRecordBinding: { NativeStreamClient: Client, NativeStreamCancellation: class { cancel() {} } } });
const { NativeStreamProvider } = await import("../src/native.js");
const provider = await NativeStreamProvider.connect("https://fixture.invalid", "fixture");
await provider.registerCredential(2, new Uint8Array([7, 8])); assert(registered);
const follow = provider.follow("scoped", { from: 0n, filter })[Symbol.asyncIterator]();
for (const sequence of [2n, 5n]) {
  const next = await follow.next();
  assert.equal(next.value?.sequence, sequence, "filtered follow must admit increasing cursor gaps");
  assert.deepEqual(next.value?.keys, [new Uint8Array([4])], "native follow dropped record keys");
  assert.equal(next.value?.attributes[0]?.name, "owner", "native follow dropped record attributes");
}
await assert.rejects(follow.next(), (error: any) => error.code === "invalid_response");
assert.equal(closed, 1, "invalid cursors must release the native follow");
