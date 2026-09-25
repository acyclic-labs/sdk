/* tslint:disable */
/* eslint-disable */

export class SimulatedMachinesBinding {
    free(): void;
    [Symbol.dispose](): void;
    cancel(id: string): Promise<Uint8Array>;
    checkpoint(id: string, idempotency_key: string): Promise<Uint8Array>;
    create(request: Uint8Array): Promise<Uint8Array>;
    destroy_checkpoint(id: string, idempotency_key: string): Promise<Uint8Array>;
    destroy_machine(id: string, idempotency_key: string): Promise<Uint8Array>;
    events(id: string, after_sequence: bigint | null | undefined, limit: number): Promise<Uint8Array>;
    fork(id: string, count: number, performance: number, idempotency_key: string): Promise<Uint8Array>;
    inspect_checkpoint(id: string): Promise<Uint8Array>;
    inspect_machine(id: string): Promise<Uint8Array>;
    inspect_operation(id: string): Promise<Uint8Array>;
    list_machines(after: string | null | undefined, limit: number): Promise<Uint8Array>;
    constructor();
    qualify_image(image: Uint8Array): Promise<Uint8Array>;
    recover(idempotency_key: string): Promise<Uint8Array>;
    recover_operation(idempotency_key: string): Promise<Uint8Array>;
    set_suspension_policy(id: string, policy: Uint8Array, idempotency_key: string): Promise<Uint8Array>;
    suspend(id: string, idempotency_key: string): Promise<Uint8Array>;
    usage(id: string, start_unix_ms: bigint, end_unix_ms: bigint): Promise<Uint8Array>;
    wake(id: string, idempotency_key: string): Promise<Uint8Array>;
    watch_operation(id: string): Promise<Uint8Array>;
}

export function normalizeIdentityBytes(value: string): Uint8Array;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_simulatedmachinesbinding_free: (a: number, b: number) => void;
    readonly normalizeIdentityBytes: (a: number, b: number) => [number, number, number, number];
    readonly simulatedmachinesbinding_cancel: (a: number, b: number, c: number) => any;
    readonly simulatedmachinesbinding_checkpoint: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly simulatedmachinesbinding_create: (a: number, b: number, c: number) => any;
    readonly simulatedmachinesbinding_destroy_checkpoint: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly simulatedmachinesbinding_destroy_machine: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly simulatedmachinesbinding_events: (a: number, b: number, c: number, d: number, e: bigint, f: number) => any;
    readonly simulatedmachinesbinding_fork: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly simulatedmachinesbinding_inspect_checkpoint: (a: number, b: number, c: number) => any;
    readonly simulatedmachinesbinding_inspect_machine: (a: number, b: number, c: number) => any;
    readonly simulatedmachinesbinding_inspect_operation: (a: number, b: number, c: number) => any;
    readonly simulatedmachinesbinding_list_machines: (a: number, b: number, c: number, d: number) => any;
    readonly simulatedmachinesbinding_new: () => number;
    readonly simulatedmachinesbinding_qualify_image: (a: number, b: number, c: number) => any;
    readonly simulatedmachinesbinding_recover: (a: number, b: number, c: number) => any;
    readonly simulatedmachinesbinding_recover_operation: (a: number, b: number, c: number) => any;
    readonly simulatedmachinesbinding_set_suspension_policy: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly simulatedmachinesbinding_suspend: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly simulatedmachinesbinding_usage: (a: number, b: number, c: number, d: bigint, e: bigint) => any;
    readonly simulatedmachinesbinding_wake: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly simulatedmachinesbinding_watch_operation: (a: number, b: number, c: number) => any;
    readonly wasm_bindgen__convert__closures_____invoke__hb8d912187548315f: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h047b78ef43e1ae53: (a: number, b: number, c: any, d: any) => void;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
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
