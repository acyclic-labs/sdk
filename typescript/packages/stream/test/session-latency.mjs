import { measure } from "./session-latency-measure.mjs";
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
