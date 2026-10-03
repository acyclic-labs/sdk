/* tslint:disable */
/* eslint-disable */
export type StreamErrorCode = "invalid_path" | "invalid_argument" | "limit_exceeded" | "not_found" | "already_exists" | "prefix_not_retained" | "out_of_range" | "idempotency_mismatch" | "capacity" | "access_denied" | "unavailable" | "hierarchy_changed" | "deadline_elapsed" | "unsupported";


/**
 * One Rust-backed live follow cursor.
 *
 * `next` releases the state lock before awaiting the stream, so `close` can
 * always signal a pending call and promptly release its cursor.
 */
export class WasmFollow {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Cancels the cursor and wakes any pending `next` call.
     */
    close(): void;
    /**
     * Waits for one record. Returns `null` after close or stream termination.
     */
    next(): Promise<Uint8Array | null>;
}

/**
 * Stateful browser provider backed by the canonical Rust memory provider.
 *
 * Unary operations use `dispatch(operation, request_bytes)` and return the
 * corresponding protobuf response bytes. `read` and `children` return arrays
 * of encoded stream response messages because protobuf streams have no single
 * finite response envelope.
 */
export class WasmMemoryStream {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Lists one fixed-snapshot child page, returning encoded `ChildrenResponse` messages.
     */
    children(input: Uint8Array): Promise<Uint8Array[]>;
    /**
     * Executes one finite unary operation over canonical protobuf bytes.
     */
    dispatch(operation: string, input: Uint8Array): Promise<Uint8Array>;
    constructor();
    /**
     * Opens a live follow cursor backed by the canonical provider.
     */
    open_follow(input: Uint8Array): Promise<WasmFollow>;
    /**
     * Reads one bounded page, returning encoded `ReadResponse` messages.
     */
    read(input: Uint8Array): Promise<Uint8Array[]>;
}

/**
 * Type-only bridge for the complete Rust-owned Stream error-code contract.
 */
export function __streamErrorCodeContract(value: StreamErrorCode): StreamErrorCode;

/**
 * Validate and project one hosted HTTP JSON success response into the public
 * JavaScript shape. Rust owns the scalar widths and tagged response schema:
 * decimal uint64 strings become `bigint`, base64 bytes become `Uint8Array`,
 * and token timestamps become `Date` values before the value crosses the
 * browser boundary.
 */
export function decodeHttpResponse(route: string, response_json: string): unknown;

/**
 * Encode one protobuf request into the hosted Stream HTTP JSON shape.
 *
 * Protobuf remains the only request contract crossing from TypeScript into
 * Rust.  Rust owns the conversion of uint64 values and opaque bytes to the
 * decimal and base64 spellings required by the hosted API, keeping the HTTP
 * adapter from maintaining a second scalar conversion table.
 */
export function encodeHttpRequest(route: string, input: Uint8Array): string;

/**
 * Return whether a code can be emitted by this WASM adapter.
 *
 * Keeping this validator beside the Rust error mapping prevents the TypeScript adapter from
 * maintaining a second, potentially stale list of base Stream error codes.
 */
export function is_stream_error_code(value: string): boolean;

/**
 * Validates one hosted read page and returns its canonical follow cursor.
 */
export function nextHttpFollowCursor(response_json: string, from: bigint): bigint;

/**
 * Normalize and encode canonical protobuf bytes for one commit request.
 *
 * The returned bytes use the same deterministic ordering as the in-memory
 * provider. Validation failures are thrown as stable error codes.
 */
export function normalizeCommitRequest(input: Uint8Array): Uint8Array;

/**
 * Decode one unary memory-provider response from canonical protobuf bytes
 * into the public JavaScript result shape. Rust owns the response oneofs,
 * scalar widths, copied byte buffers, and camelCase projection at this
 * boundary; TypeScript keeps only request adaptation and cursor lifecycle.
 */
export function projectMemoryResponse(operation: string, input: Uint8Array): unknown;

/**
 * Project a hosted HTTP error code onto the public Stream error vocabulary.
 *
 * The hosted API may report either the Rust-owned wire code or a public alias.
 * Unknown values and a commit-only alias on another route return no value.
 */
export function publicHttpErrorCode(raw: string, route: string): string | undefined;

/**
 * Validate canonical protobuf bytes for one append request.
 *
 * The empty string means that the request passed the same domain validators as
 * the in-memory provider. Otherwise this returns one stable error code.
 */
export function validateAppendRequest(input: Uint8Array): string;

/**
 * Validates request-relative child-page semantics through the canonical Rust
 * provider rules before a public page reaches a TypeScript caller.
 */
export function validateChildrenPageResponse(request: Uint8Array, response: Uint8Array): void;

/**
 * Validates the endpoint policy shared by native and browser HTTP clients.
 * HTTPS is required for hosted endpoints; HTTP is allowed only for loopback
 * fixture servers. The return value is empty for a valid endpoint.
 */
export function validateHttpEndpoint(endpoint: string): string;

/**
 * Validate a hosted read page against the request cursor captured by the
 * caller. Rust owns record shape and cursor contiguity; the HTTP adapter only
 * supplies the response text and its request-relative starting position.
 */
export function validateHttpReadResponse(response_json: string, from: bigint): void;

/**
 * Validate one hosted HTTP JSON success response using the same path, width,
 * identity, and tagged-union rules as the canonical Stream domain.
 *
 * The HTTP adapter keeps its intentionally simple JSON representation (u64
 * values are decimal strings and opaque bytes are base64). This entry point
 * validates that representation using the same Rust projection used by
 * `decodeHttpResponse`, without crossing a second scalar schema boundary.
 */
export function validateHttpResponse(route: string, response_json: string): void;

/**
 * Validate one canonical Stream path using the same parser used by every
 * provider and wire decoder.
 *
 * The empty string means success; failures use the stable Stream error code
 * consumed by the TypeScript adapter.
 */
export function validatePath(path: string): string;

/**
 * Validate one canonical protobuf request at the browser boundary.
 *
 * `kind` is deliberately a small closed set so callers cannot accidentally
 * select a different validator after adding a new wire message. The empty
 * string means success; failures use the same stable codes as the append and
 * commit entry points.
 */
export function validateRequest(kind: string, input: Uint8Array): string;

/**
 * Validate one JavaScript representation of a canonical Stream sequence.
 *
 * JavaScript passes the decimal spelling of its `bigint`; Rust owns the
 * unsigned 64-bit range accepted by every Stream wire field.
 */
export function validateSequence(value: string): string;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __streamErrorCodeContract: (a: any) => any;
    readonly __wbg_wasmfollow_free: (a: number, b: number) => void;
    readonly __wbg_wasmmemorystream_free: (a: number, b: number) => void;
    readonly decodeHttpResponse: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly encodeHttpRequest: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly is_stream_error_code: (a: number, b: number) => number;
    readonly nextHttpFollowCursor: (a: number, b: number, c: bigint) => [bigint, number, number];
    readonly normalizeCommitRequest: (a: number, b: number) => [number, number, number, number];
    readonly projectMemoryResponse: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly publicHttpErrorCode: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateAppendRequest: (a: number, b: number) => [number, number];
    readonly validateChildrenPageResponse: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateHttpEndpoint: (a: number, b: number) => [number, number];
    readonly validateHttpReadResponse: (a: number, b: number, c: bigint) => [number, number];
    readonly validateHttpResponse: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validatePath: (a: number, b: number) => [number, number];
    readonly validateRequest: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateSequence: (a: number, b: number) => [number, number];
    readonly wasmfollow_close: (a: number) => void;
    readonly wasmfollow_next: (a: number) => any;
    readonly wasmmemorystream_children: (a: number, b: number, c: number) => any;
    readonly wasmmemorystream_dispatch: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmmemorystream_new: () => number;
    readonly wasmmemorystream_open_follow: (a: number, b: number, c: number) => any;
    readonly wasmmemorystream_read: (a: number, b: number, c: number) => any;
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke___wasm_bindgen_2db2d17d2c533688___JsValue__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_2db2d17d2c533688___JsError___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke___js_sys_4adc133f13832d5d___Function_fn_wasm_bindgen_2db2d17d2c533688___JsValue_____wasm_bindgen_2db2d17d2c533688___sys__Undefined___js_sys_4adc133f13832d5d___Function_fn_wasm_bindgen_2db2d17d2c533688___JsValue_____wasm_bindgen_2db2d17d2c533688___sys__Undefined_______true_: (a: number, b: number, c: any, d: any) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
