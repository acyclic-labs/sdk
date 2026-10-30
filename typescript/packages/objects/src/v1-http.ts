import { credentialProvider, resolveCredential, type CredentialOptions, type CredentialProvider, type CredentialRequest } from "@acyclic-labs/actors/credential";
import { fromBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";
import { encode_objects_v1_json, decode_objects_v1_json, objects_v1_http_type } from "../generated/wasm/acyclic_objects_wasm.js";
import { ObjectsV1Error, ObjectsV1Provider, objectsRequest, type ObjectsV1Request } from "./v1.js";
import { ensureObjectsWasm } from "./wasm-runtime.js";
import { observed, resolveObserver, type AcyclicObserver, type OperationSizes } from "@acyclic-labs/actors/observe";

export interface ObjectsV1HttpOptions extends CredentialOptions {
  readonly endpoint: string;
  readonly maximumResponseBytes?: number;
  readonly fetch?: typeof globalThis.fetch;
  readonly observer?: AcyclicObserver;
  /** The caller's deadline for one call, in milliseconds; defaults to 30 s. */
  readonly timeoutMs?: number;
}
const STATUS: Readonly<Record<number, wire.ErrorCode>> = {
  400: wire.ErrorCode.INVALID_ARGUMENT, 401: wire.ErrorCode.ACCESS_DENIED, 403: wire.ErrorCode.ACCESS_DENIED,
  404: wire.ErrorCode.NOT_FOUND, 409: wire.ErrorCode.CONFLICT, 413: wire.ErrorCode.QUOTA_EXCEEDED,
  416: wire.ErrorCode.RANGE_NOT_SATISFIABLE, 429: wire.ErrorCode.QUOTA_EXCEEDED, 501: wire.ErrorCode.UNSUPPORTED,
};

/** Browser-compatible HTTP binding: one canonical JSON message per route, as the Rust HTTP client sends. */
export class HttpObjectsV1 extends ObjectsV1Provider {
  private readonly credential: CredentialProvider;
  private readonly endpoint: URL;
  private readonly maximumResponse: number;
  private readonly fetcher: typeof globalThis.fetch;
  private readonly observer: AcyclicObserver | undefined;
  private readonly timeoutMs: number;
  constructor(options: ObjectsV1HttpOptions) {
    super();
    const endpoint = new URL(options.endpoint);
    if ((endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname))) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("invalid Objects HTTP endpoint");
    this.credential = credentialProvider(options);
    this.maximumResponse = options.maximumResponseBytes ?? 64 * 1024 * 1024;
    if (!Number.isSafeInteger(this.maximumResponse) || this.maximumResponse < 1 || this.maximumResponse > 0xffff_ffff) throw new RangeError("wire limit must be a positive uint32");
    endpoint.pathname = endpoint.pathname.replace(/\/$/, "") + "/v1/objects/";
    this.endpoint = endpoint;
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.observer = resolveObserver(options.observer);
    this.timeoutMs = options.timeoutMs ?? 30000;
    // setTimeout carries at most a signed 32-bit millisecond delay.
    if (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs < 1 || this.timeoutMs > 0x7fff_ffff) throw new RangeError("timeoutMs must be a positive int32");
  }
  open(route: string, input: Uint8Array): ObjectsV1Request {
    return objectsRequest(input, async (bytes, controller) => {
      await ensureObjectsWasm(controller.signal);
      return observed(this.observer, "objects", route,
        sizes => this.send(route, bytes, controller, sizes));
    });
  }
  private async send(route: string, input: Uint8Array, controller: AbortController, sizes?: OperationSizes): Promise<Uint8Array> {
    const request = encode_objects_v1_json(objects_v1_http_type(route, false), input, Number.MAX_SAFE_INTEGER);
    if (sizes) sizes.requestBytes = request.byteLength;
    const timeout = setTimeout(() => controller.abort(new ObjectsV1Error(wire.ErrorCode.DEADLINE_ELAPSED)), this.timeoutMs);
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let admitted: CredentialRequest | undefined;
    try {
      const lease = await resolveCredential(this.credential, controller.signal).catch(() => { throw new ObjectsV1Error(wire.ErrorCode.ACCESS_DENIED); });
      checkCredential(lease);
      const response = await this.fetcher(new URL(route, this.endpoint), { method: "POST", redirect: "error", signal: controller.signal, headers: { authorization: `Bearer ${lease.token()}`, "content-type": "application/json" }, body: request as Uint8Array<ArrayBuffer> });
      if (response.status === 401) lease.reject();
      const declared = Number(response.headers.get("content-length") ?? 0);
      if (declared > this.maximumResponse) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
      const chunks: Uint8Array[] = [];
      let size = 0;
      reader = response.body?.getReader();
      if (reader !== undefined) while (true) {
        const { done, value } = await reader.read();
        if (response.status === 200) checkCredential(lease);
        if (done) break;
        size += value.byteLength;
        if (size > this.maximumResponse) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
        chunks.push(value);
      }
      if (sizes) sizes.responseBytes = size;
      const data = new Uint8Array(size);
      let offset = 0;
      for (const part of chunks) { data.set(part, offset); offset += part.byteLength; }
      if (response.status !== 200) {
        let detail: wire.ErrorDetail | undefined;
        try { detail = fromBinary(wire.ErrorDetailSchema, decode_objects_v1_json("ErrorDetail", data, this.maximumResponse)); } catch { /* the status classifies it */ }
        throw new ObjectsV1Error(detail !== undefined && detail.code in wire.ErrorCode && detail.code !== wire.ErrorCode.UNSPECIFIED ? detail.code : STATUS[response.status] ?? wire.ErrorCode.UNAVAILABLE);
      }
      checkCredential(lease);
      let decoded: Uint8Array;
      try { decoded = decode_objects_v1_json(objects_v1_http_type(route, true), data, this.maximumResponse); }
      catch { throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE); }
      checkCredential(lease);
      admitted = lease;
      return decoded;
    } catch (error) {
      throw controller.signal.aborted ? controller.signal.reason : error;
    } finally {
      clearTimeout(timeout);
      await reader?.cancel().catch(() => {});
      reader?.releaseLock();
      if (admitted) checkCredential(admitted);
    }
  }
}

function checkCredential(lease: CredentialRequest): void {
  try { lease.check(); }
  catch { throw new ObjectsV1Error(wire.ErrorCode.ACCESS_DENIED); }
}
