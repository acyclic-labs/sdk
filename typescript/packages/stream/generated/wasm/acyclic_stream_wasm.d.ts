/* tslint:disable */
/* eslint-disable */

/**
 * One Rust-owned follow cursor. `close` interrupts a pending `next` without polling state in JS.
 */
export class FollowBinding {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    close(): void;
    next(): Promise<Uint8Array | undefined>;
}

/**
 * One independent bounded Rust state machine, exchanged as canonical Stream v2 protobuf bytes.
 */
export class MemoryStreamBinding {
    free(): void;
    [Symbol.dispose](): void;
    append(request: Uint8Array): Promise<Uint8Array>;
    children_page(request: Uint8Array): Promise<Uint8Array>;
    commit(request: Uint8Array): Promise<Uint8Array>;
    delete(request: Uint8Array): Promise<Uint8Array>;
    follow(request: Uint8Array): Promise<FollowBinding>;
    fork(request: Uint8Array): Promise<Uint8Array>;
    inspect_idempotency(request: Uint8Array): Promise<Uint8Array>;
    constructor();
    read(request: Uint8Array): Promise<any>;
    read_commit(request: Uint8Array): Promise<Uint8Array>;
    tail(request: Uint8Array): Promise<Uint8Array>;
    trim(request: Uint8Array): Promise<Uint8Array>;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_followbinding_free: (a: number, b: number) => void;
    readonly __wbg_memorystreambinding_free: (a: number, b: number) => void;
    readonly followbinding_close: (a: number) => void;
    readonly followbinding_next: (a: number) => any;
    readonly memorystreambinding_append: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_children_page: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_commit: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_delete: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_follow: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_fork: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_inspect_idempotency: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_new: () => number;
    readonly memorystreambinding_read: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_read_commit: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_tail: (a: number, b: number, c: number) => any;
    readonly memorystreambinding_trim: (a: number, b: number, c: number) => any;
    readonly wasm_bindgen__convert__closures_____invoke__h52b70b151c954ca8: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h084ada5e0839d1da: (a: number, b: number, c: any, d: any) => void;
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
