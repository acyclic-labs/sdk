import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/objects/v2/objects_pb.js";
import { decode_objects_v2_json, encode_objects_v2_json, objects_v2_http_body_frame_bytes, objects_v2_http_error_code, objects_v2_http_json_frame_bytes, objects_v2_http_type, validate_objects_v2_get_body, validate_objects_v2_get_header, validate_objects_v2_http_endpoint, validate_objects_v2_response } from "../generated/wasm/acyclic_objects_wasm.js";
import { OBJECTS_METHODS, validateRustOwnedCredentialPolicy } from "./generated-client.js";
import { ObjectsV2Error, ObjectsV2Provider, objectsV2Error } from "./v2.js";

export interface ObjectsV2HttpOptions {
  readonly endpoint: string;
  readonly token: string;
  readonly maximumResponseBytes?: number;
  readonly maximumRequestBytes?: number;
  readonly fetch?: typeof globalThis.fetch;
}
/** Browser-compatible HTTP binding with bounded wire responses and decoded reads. */
export class HttpObjectsV2 extends ObjectsV2Provider {
  private readonly endpoint: URL;
  private readonly maximumResponse: number;
  private readonly maximumRequest: number;
  private readonly fetcher: typeof globalThis.fetch;
  constructor(private readonly options: ObjectsV2HttpOptions) {
    super();
    let endpoint: URL;
    try {
      endpoint = new URL(options.endpoint);
      validate_objects_v2_http_endpoint(options.endpoint);
    } catch {
      throw new TypeError("invalid Objects HTTP endpoint");
    }
    validateRustOwnedCredentialPolicy(options.token);
    if (new TextEncoder().encode(options.token).byteLength > 8192 || /\0/.test(options.token)) throw new TypeError("invalid bearer token");
    this.maximumResponse = options.maximumResponseBytes ?? 64 * 1024 * 1024;
    this.maximumRequest = options.maximumRequestBytes ?? 64 * 1024 * 1024;
    for (const maximum of [this.maximumResponse, this.maximumRequest]) if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 0xffff_ffff) throw new RangeError("wire limit must be a positive uint32");
    endpoint.pathname = endpoint.pathname.replace(/\/$/, "") + "/v2/objects/";
    this.endpoint = endpoint;
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
  }
  protected async invoke(route: string, bytes: Uint8Array, body: Uint8Array, maximum: bigint, signal?: AbortSignal): Promise<readonly Uint8Array[]> {
    const method = Object.values(OBJECTS_METHODS).find(candidate => candidate.path === `v2/objects/${route}`);
    if (method === undefined) throw new ObjectsV2Error(wire.ErrorCode.INVALID_ARGUMENT);
    const types = [objects_v2_http_type(route, false), objects_v2_http_type(route, true)];
    const jsonFrameBytes = objects_v2_http_json_frame_bytes();
    const bodyFrameBytes = objects_v2_http_body_frame_bytes();
    const decodeResponse = (data: Uint8Array, maximum: number) => {
      try { return decode_objects_v2_json(types[1], data, maximum); }
      catch (error) {
        const code = objectsV2Error(error).code;
        throw new ObjectsV2Error(code === wire.ErrorCode.QUOTA_EXCEEDED ? code : wire.ErrorCode.UNAVAILABLE);
      }
    };
    const streaming = method.clientStreaming;
    const payloads: Uint8Array[] = [];
    let requestSize = 0;
    const add = (value: Uint8Array) => {
      requestSize += value.byteLength + (streaming ? 1 : 0);
      if (requestSize > this.maximumRequest) throw new ObjectsV2Error(wire.ErrorCode.QUOTA_EXCEEDED);
      payloads.push(value);
      if (streaming) payloads.push(new Uint8Array([10]));
    };
    if (streaming) {
      const put = route === "objects/put";
      const header = put
        ? toBinary(wire.PutObjectRequestSchema, create(wire.PutObjectRequestSchema, { frame: { case: "header", value: fromBinary(wire.PutObjectHeaderSchema, bytes) } }))
        : toBinary(wire.UploadPartRequestSchema, create(wire.UploadPartRequestSchema, { frame: { case: "header", value: fromBinary(wire.UploadPartHeaderSchema, bytes) } }));
      add(encode_objects_v2_json(types[0], header, jsonFrameBytes));
      for (let offset = 0; offset < body.byteLength; offset += bodyFrameBytes) {
        const chunk = body.subarray(offset, offset + bodyFrameBytes);
        const frame = put
          ? toBinary(wire.PutObjectRequestSchema, create(wire.PutObjectRequestSchema, { frame: { case: "body", value: chunk } }))
          : toBinary(wire.UploadPartRequestSchema, create(wire.UploadPartRequestSchema, { frame: { case: "body", value: chunk } }));
        add(encode_objects_v2_json(types[0], frame, jsonFrameBytes));
      }
      const complete = put
        ? toBinary(wire.PutObjectRequestSchema, create(wire.PutObjectRequestSchema, { frame: { case: "complete", value: true } }))
        : toBinary(wire.UploadPartRequestSchema, create(wire.UploadPartRequestSchema, { frame: { case: "complete", value: true } }));
      add(encode_objects_v2_json(types[0], complete, jsonFrameBytes));
    } else add(encode_objects_v2_json(types[0], bytes, 16 * 1024 * 1024));
    const request = new Uint8Array(requestSize);
    let offset = 0;
    for (const part of payloads) { request.set(part, offset); offset += part.byteLength; }
    const controller = new AbortController();
    const forwardAbort = () => controller.abort(signal?.reason);
    if (signal?.aborted) forwardAbort();
    else signal?.addEventListener("abort", forwardAbort, { once: true });
    const timeout = setTimeout(() => controller.abort(), 30000);
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    try {
      const response = await this.fetcher(new URL(route, this.endpoint), { method: "POST", redirect: "error", signal: controller.signal, headers: { authorization: `Bearer ${this.options.token}`, "content-type": streaming ? "application/x-ndjson" : "application/json" }, body: request });
      if (response.status === 304) throw new ObjectsV2Error(wire.ErrorCode.NOT_MODIFIED);
      if (method.serverStreaming && response.status === 200) {
        if (response.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase() !== "application/x-ndjson") throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
        reader = response.body?.getReader();
        const frames: Uint8Array[] = [];
        const line = new Uint8Array(jsonFrameBytes);
        let lineLength = 0;
        let wireSize = 0;
        let remaining: bigint | undefined;
        while (reader !== undefined) {
          const { done, value } = await reader.read();
          if (done) break;
          wireSize += value.byteLength;
          if (wireSize > this.maximumResponse) throw new ObjectsV2Error(wire.ErrorCode.QUOTA_EXCEEDED);
          let start = 0;
          for (let end = 0; end <= value.byteLength; end++) {
            if (end < value.byteLength && value[end] !== 10) continue;
            const fragment = value.subarray(start, end);
            if (lineLength + fragment.byteLength > line.byteLength) throw new ObjectsV2Error(wire.ErrorCode.QUOTA_EXCEEDED);
            line.set(fragment, lineLength);
            lineLength += fragment.byteLength;
            if (end === value.byteLength) break;
            start = end + 1;
            const stop = lineLength > 0 && line[lineLength - 1] === 13 ? lineLength - 1 : lineLength;
            if (stop === 0) throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
            const decoded = decodeResponse(line.subarray(0, stop), jsonFrameBytes);
            validate_objects_v2_response("objects/get", bytes, decoded, maximum);
            lineLength = 0;
            const { frame } = fromBinary(wire.GetObjectResponseSchema, decoded);
            if (frame.case === "error") throw objectsV2Error({ code: frame.value.code });
            if (remaining === undefined) {
              if (frame.case !== "header") throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
              remaining = validate_objects_v2_get_header(bytes, toBinary(wire.GetObjectHeaderSchema, frame.value), maximum);
            } else {
              if (frame.case !== "body") throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
              remaining = validate_objects_v2_get_body(BigInt(frame.value.byteLength), remaining!);
            }
            frames.push(decoded);
          }
        }
        if (lineLength !== 0 || remaining === undefined || remaining !== 0n) throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
        return frames;
      }
      const chunks: Uint8Array[] = [];
      let size = 0;
      reader = response.body?.getReader();
      if (reader !== undefined) while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > this.maximumResponse) throw new ObjectsV2Error(wire.ErrorCode.QUOTA_EXCEEDED);
        chunks.push(value);
      }
      const data = new Uint8Array(size);
      offset = 0;
      for (const part of chunks) { data.set(part, offset); offset += part.byteLength; }
      if (response.status !== 200) {
        let detailCode: number | undefined;
        try {
          const detail = fromBinary(wire.ErrorDetailSchema, decode_objects_v2_json("ErrorDetail", data, this.maximumResponse));
          detailCode = detail.code;
        } catch { /* malformed details fall back to the canonical HTTP status mapping */ }
        throw new ObjectsV2Error(objects_v2_http_error_code(response.status, detailCode) as wire.ErrorCode);
      }
      const media = response.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase();
      if (media !== "application/json") throw new ObjectsV2Error(wire.ErrorCode.UNAVAILABLE);
      return [decodeResponse(data, this.maximumResponse)];
    } finally {
      clearTimeout(timeout);
      signal?.removeEventListener("abort", forwardAbort);
      await reader?.cancel().catch(() => {});
      reader?.releaseLock();
    }
  }
}
