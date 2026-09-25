/* tslint:disable */
/* eslint-disable */

export class MemoryObjectsBinding {
    free(): void;
    [Symbol.dispose](): void;
    abort_multipart(request: Uint8Array): Promise<Uint8Array>;
    complete_multipart(request: Uint8Array): Promise<Uint8Array>;
    create_bucket(request: Uint8Array): Promise<Uint8Array>;
    create_multipart(request: Uint8Array): Promise<Uint8Array>;
    delete(request: Uint8Array): Promise<Uint8Array>;
    delete_bucket(request: Uint8Array): Promise<Uint8Array>;
    destroy_snapshot(request: Uint8Array): Promise<Uint8Array>;
    fork_bucket(request: Uint8Array): Promise<Uint8Array>;
    fork_snapshot(request: Uint8Array): Promise<Uint8Array>;
    get(request: Uint8Array): Promise<any>;
    head(request: Uint8Array): Promise<Uint8Array>;
    head_bucket(request: Uint8Array): Promise<Uint8Array>;
    list(request: Uint8Array): Promise<Uint8Array>;
    list_parts(request: Uint8Array): Promise<Uint8Array>;
    constructor();
    put(header: Uint8Array, body: Uint8Array): Promise<Uint8Array>;
    snapshot(request: Uint8Array): Promise<Uint8Array>;
    upload_part(header: Uint8Array, body: Uint8Array): Promise<Uint8Array>;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_memoryobjectsbinding_free: (a: number, b: number) => void;
    readonly memoryobjectsbinding_abort_multipart: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_complete_multipart: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_create_bucket: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_create_multipart: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_delete: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_delete_bucket: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_destroy_snapshot: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_fork_bucket: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_fork_snapshot: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_get: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_head: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_head_bucket: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_list: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_list_parts: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_new: () => number;
    readonly memoryobjectsbinding_put: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly memoryobjectsbinding_snapshot: (a: number, b: number, c: number) => any;
    readonly memoryobjectsbinding_upload_part: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasm_bindgen__convert__closures_____invoke__hb8d912187548315f: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h047b78ef43e1ae53: (a: number, b: number, c: any, d: any) => void;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
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
