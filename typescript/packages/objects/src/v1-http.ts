import { deadlineSignal } from "@acyclic-labs/actors/deadline";
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
  readonly maximumRequestBytes?: number;
  readonly fetch?: typeof globalThis.fetch;
  readonly observer?: AcyclicObserver;
  /** Optional caller deadline for one call, in milliseconds. */
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
  private readonly maximumResponse: number | undefined;
  private readonly maximumRequest: number | undefined;
  private readonly fetcher: typeof globalThis.fetch;
  private readonly observer: AcyclicObserver | undefined;
  private readonly timeoutMs: number | undefined;
  constructor(options: ObjectsV1HttpOptions) {
    super();
    const endpoint = new URL(options.endpoint);
    if ((endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname))) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("invalid Objects HTTP endpoint");
    this.credential = credentialProvider(options);
    this.maximumResponse = options.maximumResponseBytes;
    this.maximumRequest = options.maximumRequestBytes;
    for (const maximum of [this.maximumResponse, this.maximumRequest]) if (maximum !== undefined && (!Number.isSafeInteger(maximum) || maximum < 1)) throw new RangeError("wire limit must be a positive safe integer");
    endpoint.pathname = endpoint.pathname.replace(/\/$/, "") + "/v1/objects/";
    this.endpoint = endpoint;
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.observer = resolveObserver(options.observer);
    this.timeoutMs = options.timeoutMs;
    if (this.timeoutMs !== undefined && (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs < 1)) throw new RangeError("timeoutMs must be a positive safe integer");
  }
  open(route: string, input: Uint8Array): ObjectsV1Request {
    return objectsRequest(input, async (bytes, controller) => {
      await ensureObjectsWasm(controller.signal);
      return observed(this.observer, "objects", route,
        sizes => this.send(route, bytes, controller, sizes));
    });
  }
  private async send(route: string, input: Uint8Array, controller: AbortController, sizes?: OperationSizes): Promise<Uint8Array> {
    const request = encode_objects_v1_json(objects_v1_http_type(route, false), input, this.maximumRequest);
    if (this.maximumRequest !== undefined && request.byteLength > this.maximumRequest) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
    if (sizes) sizes.requestBytes = request.byteLength;
    const deadline = deadlineSignal(this.timeoutMs, () => new ObjectsV1Error(wire.ErrorCode.DEADLINE_ELAPSED), controller.signal);
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let admitted: CredentialRequest | undefined;
    try {
      const lease = await resolveCredential(this.credential, deadline.signal).catch(() => { throw new ObjectsV1Error(wire.ErrorCode.ACCESS_DENIED); });
      checkCredential(lease);
      const response = await this.fetcher(new URL(route, this.endpoint), { method: "POST", redirect: "error", signal: deadline.signal, headers: { authorization: `Bearer ${lease.token()}`, "content-type": "application/json" }, body: request as Uint8Array<ArrayBuffer> });
      if (response.status === 401) lease.reject();
      const declared = Number(response.headers.get("content-length") ?? 0);
      if (this.maximumResponse !== undefined && declared > this.maximumResponse) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
      const chunks: Uint8Array[] = [];
      let size = 0;
      reader = response.body?.getReader();
      if (reader !== undefined) while (true) {
        const { done, value } = await reader.read();
        if (response.status === 200) checkCredential(lease);
        if (done) break;
        size += value.byteLength;
        if (this.maximumResponse !== undefined && size > this.maximumResponse) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
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
      // The caller's deadline, whichever layer reported the abort.
      throw deadline.signal.aborted ? deadline.signal.reason : error;
    } finally {
      deadline.cleanup();
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
