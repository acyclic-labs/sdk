import assert from "node:assert/strict";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager } from "@connectrpc/connect-node";
import { StreamService } from "../generated/proto/stream/v1/stream_pb.js";
import { MemoryStreamProvider } from "../dist/memory.js";
import { GrpcStreamProvider } from "../dist/grpc.js";
import { readResponseRecords, wireAppendRequest } from "../dist/contract.js";
import { validateAppend } from "../dist/client.js";
import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { AppendRequestSchema, AppendResponseSchema } from "../generated/proto/stream/v1/stream_pb.js";
import { projectMemoryResponse } from "../generated/wasm/acyclic_stream_wasm.js";

const WARM = 16;
const SAMPLES = 128;
export async function measure(provider, name, follow = (path, options) => provider.follow(path, options),
  { runtime = process.versions.bun ? "bun" : "node", warm = WARM, samples = SAMPLES } = {}) {
  const path = `latency/${runtime}/${name}`;
  assert.equal((await provider.append(path, [new Uint8Array([0])], { ifTail: 0n })).ok, true, `initial latency append failed for ${path}`);
  const controller = new AbortController();
  const records = follow(path, { from: 1n, signal: controller.signal })[Symbol.asyncIterator]();
  const elapsedSamples = [];
  try {
    for (let index = 0; index < warm + samples; index++) {
      const next = records.next();
      const began = performance.now();
      // Measure request-to-yield, including append validation, transport and
      // acceptance. No durability acknowledgement is awaited before the yield.
      const append = provider.append(path, [new Uint8Array([7])], { ifTail: BigInt(index + 1) });
      const item = await next;
      const elapsed = (performance.now() - began) * 1_000;
      assert.equal(item.value.sequence, BigInt(index + 1));
      assert.equal((await append).ok, true);
      if (index >= warm) elapsedSamples.push(elapsed);
    }
  } finally { controller.abort(); await records.return?.(); }
  elapsedSamples.sort((a, b) => a - b);
  return { samples, p50_us: elapsedSamples[Math.ceil(samples * .5) - 1], p99_us: elapsedSamples[Math.ceil(samples * .99) - 1] };
}

export async function measureLatency(options) {
  const session = new Http2SessionManager(new URL(options.endpoint), {}, { ca: options.caCertificate });
  const raw = createClient(StreamService, createGrpcTransport({ baseUrl: options.endpoint, sessionManager: session }));
  const legacy = async function* (path, { from, signal }) {
    for await (const response of raw.follow({ path, from }, { signal, headers: { authorization: `Bearer ${options.token}` } })) yield* readResponseRecords(response);
  };
  const legacyProvider = { async append(path, values, appendOptions) {
    const records = values.map(value => value.slice());
    const authored = structuredClone(appendOptions);
    await validateAppend(path, records, authored);
    const response = await raw.append(fromBinary(AppendRequestSchema, wireAppendRequest(path, records, authored)), { headers: { authorization: `Bearer ${options.token}` } });
    return projectMemoryResponse("append", toBinary(AppendResponseSchema, response));
  } };
  try {
    console.log(JSON.stringify({ runtime: process.versions.bun ? "bun" : "node", latency: {
      memory: await measure(new MemoryStreamProvider(), "memory"),
      grpc_before: await measure(legacyProvider, "before", legacy),
      grpc_after: await measure(new GrpcStreamProvider(options), "after"),
    } }));
  } finally { session.abort(); }
}
