import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";
import { encode_objects_v1_json, decode_objects_v1_json, objects_v1_http_type, validate_objects_v1_get_header } from "../generated/wasm/acyclic_objects_wasm.js";
import { MAX_BEARER_TOKEN_BYTES, ObjectsV1Error, ObjectsV1Provider, bodyDecodedLimit, objectsV1Error } from "./v1.js";
import { observed, resolveObserver, type AcyclicObserver, type OperationSizes } from "./observe.js";

export interface ObjectsV1HttpOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly maximumResponseBytes?: number;
  readonly maximumRequestBytes?: number;
  readonly fetch?: typeof globalThis.fetch;
  readonly observer?: AcyclicObserver;
}
/** Browser-compatible HTTP binding with bounded wire responses and decoded reads. */
export class HttpObjectsV1 extends ObjectsV1Provider {
  private readonly endpoint: URL;
  private readonly maximumResponse: number;
  private readonly maximumRequest: number;
  private readonly fetcher: typeof globalThis.fetch;
  private readonly observer: AcyclicObserver | undefined;
  constructor(private readonly options: ObjectsV1HttpOptions) {
    super();
    const endpoint = new URL(options.endpoint);
    if ((endpoint.protocol !== "https:" && !(endpoint.protocol === "http:" && ["localhost", "127.0.0.1", "[::1]"].includes(endpoint.hostname))) || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("invalid Objects HTTP endpoint");
    if (!options.token.trim() || new TextEncoder().encode(options.token).byteLength > MAX_BEARER_TOKEN_BYTES || /[\r\n\0]/.test(options.token)) throw new TypeError("invalid bearer token");
    this.maximumResponse = options.maximumResponseBytes ?? 64 * 1024 * 1024;
    this.maximumRequest = options.maximumRequestBytes ?? 64 * 1024 * 1024;
    for (const maximum of [this.maximumResponse, this.maximumRequest]) if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 0xffff_ffff) throw new RangeError("wire limit must be a positive uint32");
    endpoint.pathname = endpoint.pathname.replace(/\/$/, "") + "/v1/objects/";
    this.endpoint = endpoint;
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.observer = resolveObserver(options.observer);
  }
  protected invoke(route: string, bytes: Uint8Array, body: Uint8Array, maximum: bigint): Promise<readonly Uint8Array[]> {
    return observed(this.observer, "objects", route, sizes => this.send(route, bytes, body, maximum, sizes));
  }
  private async send(route: string, bytes: Uint8Array, body: Uint8Array, maximum: bigint, sizes?: OperationSizes): Promise<readonly Uint8Array[]> {
    const requestType = objects_v1_http_type(route, false);
    const responseType = objects_v1_http_type(route, true);
    const decodeResponse = (data: Uint8Array, maximum: number) => {
      try { return decode_objects_v1_json(responseType, data, maximum); }
      catch (error) { throw new ObjectsV1Error(objectsV1Error(error).code === wire.ErrorCode.QUOTA_EXCEEDED ? wire.ErrorCode.QUOTA_EXCEEDED : wire.ErrorCode.UNAVAILABLE); }
    };
    const streaming = route === "objects/put" || route === "multipart/upload-part";
    const payloads: Uint8Array[] = [];
    let requestSize = 0;
    const add = (value: Uint8Array) => {
      requestSize += value.byteLength + (streaming ? 1 : 0);
      if (requestSize > this.maximumRequest) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
      payloads.push(value);
      if (streaming) payloads.push(new Uint8Array([10]));
    };
    if (streaming) {
      const put = route === "objects/put";
      const header = put
        ? toBinary(wire.PutObjectRequestSchema, create(wire.PutObjectRequestSchema, { frame: { case: "header", value: fromBinary(wire.PutObjectHeaderSchema, bytes) } }))
        : toBinary(wire.UploadPartRequestSchema, create(wire.UploadPartRequestSchema, { frame: { case: "header", value: fromBinary(wire.UploadPartHeaderSchema, bytes) } }));
      add(encode_objects_v1_json(requestType, header, 128 * 1024));
      for (let offset = 0; offset < body.byteLength; offset += 65536) {
        const chunk = body.subarray(offset, offset + 65536);
        const frame = put
          ? toBinary(wire.PutObjectRequestSchema, create(wire.PutObjectRequestSchema, { frame: { case: "body", value: chunk } }))
          : toBinary(wire.UploadPartRequestSchema, create(wire.UploadPartRequestSchema, { frame: { case: "body", value: chunk } }));
        add(encode_objects_v1_json(requestType, frame, 128 * 1024));
      }
      const complete = put
        ? toBinary(wire.PutObjectRequestSchema, create(wire.PutObjectRequestSchema, { frame: { case: "complete", value: true } }))
        : toBinary(wire.UploadPartRequestSchema, create(wire.UploadPartRequestSchema, { frame: { case: "complete", value: true } }));
      add(encode_objects_v1_json(requestType, complete, 128 * 1024));
    } else add(encode_objects_v1_json(requestType, bytes, 16 * 1024 * 1024));
    const request = new Uint8Array(requestSize);
    let offset = 0;
    for (const part of payloads) { request.set(part, offset); offset += part.byteLength; }
    if (sizes) sizes.requestBytes = requestSize;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
      const response = await this.fetcher(new URL(route, this.endpoint), { method: "POST", redirect: "error", signal: controller.signal, headers: { authorization: `Bearer ${this.options.token}`, "content-type": streaming ? "application/x-ndjson" : "application/json" }, body: request });
      if (response.status === 304) throw new ObjectsV1Error(wire.ErrorCode.NOT_MODIFIED);
      if (route === "objects/get" && response.status === 200) {
        if (response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase() !== "application/x-ndjson") throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
        reader = response.body?.getReader();
        const frames: Uint8Array[] = [];
        // One NDJSON line, bounded by the base64 of one compressed 8 MiB body frame.
        const lineLimit = 12 * 1024 * 1024;
        let line = new Uint8Array(64 * 1024);
        let lineLength = 0;
        let wireSize = 0;
        let bodySize = 0n;
        let expected: bigint | undefined;
        while (reader !== undefined) {
          const { done, value } = await reader.read();
          if (done) break;
          wireSize += value.byteLength;
          if (wireSize > this.maximumResponse) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
          for (let start = 0; ;) {
            const newline = value.indexOf(10, start);
            const end = newline < 0 ? value.byteLength : newline;
            const fragment = value.subarray(start, end);
            const needed = lineLength + fragment.byteLength;
            if (needed > lineLimit) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
            if (needed > line.byteLength) {
              const grown = new Uint8Array(Math.min(lineLimit, Math.max(needed, line.byteLength * 2)));
              grown.set(line.subarray(0, lineLength));
              line = grown;
            }
            line.set(fragment, lineLength);
            lineLength += fragment.byteLength;
            if (newline < 0) break;
            start = newline + 1;
            const stop = lineLength > 0 && line[lineLength - 1] === 13 ? lineLength - 1 : lineLength;
            if (stop === 0) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
            const decoded = decodeResponse(line.subarray(0, stop), lineLimit);
            lineLength = 0;
            const { frame } = fromBinary(wire.GetObjectResponseSchema, decoded);
            if (frame.case === "error") throw objectsV1Error({ code: frame.value.code });
            if (expected === undefined) {
              if (frame.case !== "header") throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
              expected = validate_objects_v1_get_header(bytes, toBinary(wire.GetObjectHeaderSchema, frame.value), maximum);
            } else {
              if (frame.case !== "body" || frame.value.decodedLength > bodyDecodedLimit(frame.value)) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
              bodySize += frame.value.decodedLength;
              if (bodySize > maximum) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
              if (bodySize > expected) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
            }
            frames.push(decoded);
          }
        }
        if (lineLength !== 0 || expected === undefined || bodySize !== expected) throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
        if (sizes) sizes.responseBytes = wireSize;
        return frames;
      }
      const chunks: Uint8Array[] = [];
      let size = 0;
      reader = response.body?.getReader();
      if (reader !== undefined) while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > this.maximumResponse) throw new ObjectsV1Error(wire.ErrorCode.QUOTA_EXCEEDED);
        chunks.push(value);
      }
      if (sizes) sizes.responseBytes = size;
      const data = new Uint8Array(size);
      offset = 0;
      for (const part of chunks) { data.set(part, offset); offset += part.byteLength; }
      if (response.status !== 200) {
        try {
          const detail = fromBinary(wire.ErrorDetailSchema, decode_objects_v1_json("ErrorDetail", data, this.maximumResponse));
          if (detail.code >= wire.ErrorCode.INVALID_ARGUMENT && detail.code <= wire.ErrorCode.NOT_MODIFIED) throw new ObjectsV1Error(detail.code);
        } catch (error) { if (error instanceof ObjectsV1Error) throw error; }
        const code = response.status === 400 ? wire.ErrorCode.INVALID_ARGUMENT : response.status === 401 || response.status === 403 ? wire.ErrorCode.ACCESS_DENIED : response.status === 404 ? wire.ErrorCode.NOT_FOUND : response.status === 412 ? wire.ErrorCode.PRECONDITION_FAILED : response.status === 416 ? wire.ErrorCode.RANGE_NOT_SATISFIABLE : response.status === 413 || response.status === 429 ? wire.ErrorCode.QUOTA_EXCEEDED : wire.ErrorCode.UNAVAILABLE;
        throw new ObjectsV1Error(code);
      }
      const media = response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
      if (media !== "application/json") throw new ObjectsV1Error(wire.ErrorCode.UNAVAILABLE);
      return [decodeResponse(data, this.maximumResponse)];
    } finally {
      clearTimeout(timeout);
      await reader?.cancel().catch(() => {});
      reader?.releaseLock();
    }
  }
}
