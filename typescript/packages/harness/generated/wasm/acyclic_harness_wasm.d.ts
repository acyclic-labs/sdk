/* tslint:disable */
/* eslint-disable */

/**
 * Opaque synchronous reducer hosted in WebAssembly.
 */
export class WasmReducer {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Applies one command through the same deterministic Rust reducer as native hosts.
     */
    apply(command: any): any;
    /**
     * Applies one canonical Protobuf command and returns a Protobuf response.
     */
    applyWire(command: Uint8Array): Uint8Array;
    /**
     * Attenuates a scope without permitting capability expansion.
     */
    attenuate(parent: any, id: string, capabilities: any): any;
    /**
     * Issues a root scope from this host's explicit authority object.
     */
    issueScope(id: string, capabilities: any): any;
    /**
     * Resolves named policy layers and issues only their effective grant.
     */
    issueScopeWithPolicies(id: string, layers: any): any;
    /**
     * Creates an empty reducer with explicit host-managed authority.
     */
    constructor(authority: any, issuer_id: string, issuer_key: Uint8Array, schemas: any);
    /**
     * Returns the exact wire identity used by this compiled core.
     */
    protocolIdentity(): any;
    /**
     * Restores a snapshot under explicit host-managed authority.
     */
    static restore(snapshot: any, issuer_id: string, issuer_key: Uint8Array, schemas: any): WasmReducer;
    /**
     * Returns a versioned integrity-checked restoration snapshot.
     */
    snapshot(): any;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_wasmreducer_free: (a: number, b: number) => void;
    readonly wasmreducer_apply: (a: number, b: any) => [number, number, number];
    readonly wasmreducer_applyWire: (a: number, b: number, c: number) => [number, number, number, number];
    readonly wasmreducer_attenuate: (a: number, b: any, c: number, d: number, e: any) => [number, number, number];
    readonly wasmreducer_issueScope: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmreducer_issueScopeWithPolicies: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmreducer_new: (a: any, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_protocolIdentity: (a: number) => [number, number, number];
    readonly wasmreducer_restore: (a: any, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_snapshot: (a: number) => [number, number, number];
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
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
