/* tslint:disable */
/* eslint-disable */
/**
 * The `ReadableStreamType` enum.
 *
 * *This API requires the following crate features to be activated: `ReadableStreamType`*
 */

type ReadableStreamType = "bytes";
export type StreamErrorCode = "invalid_path" | "invalid_argument" | "limit_exceeded" | "not_found" | "already_exists" | "prefix_not_retained" | "out_of_range" | "idempotency_mismatch" | "capacity" | "access_denied" | "unavailable" | "hierarchy_changed" | "deadline_elapsed" | "unsupported";


declare class IntoUnderlyingByteSource {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(): void;
    pull(controller: ReadableByteStreamController): Promise<any>;
    start(controller: ReadableByteStreamController): void;
    readonly autoAllocateChunkSize: number;
    readonly type: ReadableStreamType;
}
export type { IntoUnderlyingByteSource };

declare class IntoUnderlyingSink {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    abort(reason: any): Promise<any>;
    close(): Promise<any>;
    write(chunk: any): Promise<any>;
}
export type { IntoUnderlyingSink };

declare class IntoUnderlyingSource {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    cancel(): void;
    pull(controller: ReadableStreamDefaultController): Promise<any>;
}
export type { IntoUnderlyingSource };

/**
 * One Rust-backed live follow cursor.
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
 * Browser ABI over canonical Rust memory or durable providers.
 */
export class WasmStream {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Lists one fixed-snapshot child page, returning encoded `ChildrenResponse` messages.
     */
    children(input: Uint8Array): Promise<Uint8Array[]>;
    /**
     * Connects canonical platform RPCs and shared push follows. Credentials stay live.
     */
    static connect(endpoint: string, credential: Function): WasmStream;
    /**
     * Executes one finite unary operation over canonical protobuf bytes.
     */
    dispatch(operation: string, input: Uint8Array): Promise<Uint8Array>;
    /**
     * Creates an in-process provider.
     */
    constructor();
    /**
     * Pushes the next successful handshake, including reconnects without further records.
     */
    nextConnection(after: bigint): Promise<bigint>;
    /**
     * Opens the canonical Rust provider on one durable `IndexedDB` journal.
     */
    static openBrowser(name: string, maximum_commands: number, maximum_journal_bytes: bigint): Promise<WasmStream>;
    /**
     * Opens a canonical SessionSubscription, yielding encoded SessionFrame data
     * and watermarks. Rust owns IDs, ALL/LATEST cursors, anchors and bounded credit.
     */
    openData(input: Uint8Array): Promise<WasmFollow>;
    /**
     * Opens live child discovery; each next yields one canonical ChildrenResponse.
     */
    open_children(input: Uint8Array): Promise<WasmFollow>;
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
 */
export function decodeHttpResponse(route: string, response_json: string): unknown;

/**
 * Decode one `ReadResponse` frame's fields to its encoded `RecordBatch`.
 */
export function decodeReadResponse(codec: number, data: Uint8Array, decoded_length: bigint): Uint8Array;

/**
 * Encode one protobuf request into the hosted Stream HTTP JSON shape.
 */
export function encodeHttpRequest(route: string, input: Uint8Array): string;

/**
 * Return whether a code can be emitted by this WASM adapter.
 */
export function is_stream_error_code(value: string): boolean;

/**
 * Normalize and encode canonical protobuf bytes for one commit request.
 */
export function normalizeCommitRequest(input: Uint8Array): Uint8Array;

/**
 * Decode one unary memory-provider response from canonical protobuf bytes
 */
export function projectMemoryResponse(operation: string, input: Uint8Array): unknown;

/**
 * Project a hosted HTTP error code onto the public Stream error vocabulary.
 */
export function publicHttpErrorCode(raw: string, route: string): string | undefined;

/**
 * Validate canonical protobuf bytes for one append request.
 */
export function validateAppendRequest(input: Uint8Array): string;

/**
 * Validate one hosted HTTP JSON success response using the same path, width,
 */
export function validateHttpResponse(route: string, response_json: string): void;

/**
 * Validate one canonical Stream path using the same parser used by every
 */
export function validatePath(path: string): string;

/**
 * Validate one canonical protobuf request at the browser boundary.
 */
export function validateRequest(kind: string, input: Uint8Array): string;

/**
 * Validate one JavaScript representation of a canonical Stream sequence.
 */
export function validateSequence(value: string): string;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __streamErrorCodeContract: (a: any) => any;
    readonly __wbg_wasmfollow_free: (a: number, b: number) => void;
    readonly __wbg_wasmstream_free: (a: number, b: number) => void;
    readonly decodeHttpResponse: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly decodeReadResponse: (a: number, b: number, c: number, d: bigint) => [number, number, number, number];
    readonly encodeHttpRequest: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly is_stream_error_code: (a: number, b: number) => number;
    readonly normalizeCommitRequest: (a: number, b: number) => [number, number, number, number];
    readonly projectMemoryResponse: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly publicHttpErrorCode: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateAppendRequest: (a: number, b: number) => [number, number];
    readonly validateHttpResponse: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validatePath: (a: number, b: number) => [number, number];
    readonly validateRequest: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateSequence: (a: number, b: number) => [number, number];
    readonly wasmfollow_close: (a: number) => void;
    readonly wasmfollow_next: (a: number) => any;
    readonly wasmstream_children: (a: number, b: number, c: number) => any;
    readonly wasmstream_connect: (a: number, b: number, c: any) => [number, number, number];
    readonly wasmstream_dispatch: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmstream_new: () => number;
    readonly wasmstream_nextConnection: (a: number, b: bigint) => any;
    readonly wasmstream_openBrowser: (a: number, b: number, c: number, d: bigint) => any;
    readonly wasmstream_openData: (a: number, b: number, c: number) => any;
    readonly wasmstream_open_children: (a: number, b: number, c: number) => any;
    readonly wasmstream_open_follow: (a: number, b: number, c: number) => any;
    readonly wasmstream_read: (a: number, b: number, c: number) => any;
    readonly __wbg_intounderlyingbytesource_free: (a: number, b: number) => void;
    readonly intounderlyingbytesource_autoAllocateChunkSize: (a: number) => number;
    readonly intounderlyingbytesource_cancel: (a: number) => void;
    readonly intounderlyingbytesource_pull: (a: number, b: any) => any;
    readonly intounderlyingbytesource_start: (a: number, b: any) => void;
    readonly intounderlyingbytesource_type: (a: number) => number;
    readonly __wbg_intounderlyingsource_free: (a: number, b: number) => void;
    readonly intounderlyingsource_cancel: (a: number) => void;
    readonly intounderlyingsource_pull: (a: number, b: any) => any;
    readonly __wbg_intounderlyingsink_free: (a: number, b: number) => void;
    readonly intounderlyingsink_abort: (a: number, b: any) => any;
    readonly intounderlyingsink_close: (a: number) => any;
    readonly intounderlyingsink_write: (a: number, b: any) => any;
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke___wasm_bindgen_15a8f3186e544560___JsValue__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_15a8f3186e544560___JsError___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke___web_sys_8e157a6a32ff42f4___features__gen_IdbVersionChangeEvent__IdbVersionChangeEvent__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_15a8f3186e544560___JsValue___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke___js_sys_67453e9d4ad8eaa4___Function_fn_wasm_bindgen_15a8f3186e544560___JsValue_____wasm_bindgen_15a8f3186e544560___sys__Undefined___js_sys_67453e9d4ad8eaa4___Function_fn_wasm_bindgen_15a8f3186e544560___JsValue_____wasm_bindgen_15a8f3186e544560___sys__Undefined_______true_: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke___wasm_bindgen_15a8f3186e544560___JsValue______true_: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke___web_sys_8e157a6a32ff42f4___features__gen_Event__Event______true_: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke___web_sys_8e157a6a32ff42f4___features__gen_MessageEvent__MessageEvent______true_: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke___bool__true_: (a: number, b: number) => number;
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke_______true__1_: (a: number, b: number) => void;
    readonly wasm_bindgen_15a8f3186e544560___convert__closures_____invoke_______true_: (a: number, b: number) => void;
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
