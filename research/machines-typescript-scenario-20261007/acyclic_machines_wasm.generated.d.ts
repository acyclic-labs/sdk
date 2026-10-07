/* tslint:disable */
/* eslint-disable */
/**
 * WebAssembly binding enum for CapabilityIn.
 */
export type CapabilityIn = "elastic-cpu" | "elastic-memory" | "live-checkpoint" | "live-fork" | "suspend-resume" | "live-movement" | "disk-fork";

/**
 * WebAssembly binding enum for CompatibilityIn.
 */
export type CompatibilityIn = { kind: "best-effort" } | { kind: "require"; capabilities: readonly CapabilityIn[] };

/**
 * WebAssembly binding enum for CompatibilityOut.
 */
export type CompatibilityOut = { kind: "best-effort" } | { kind: "require"; capabilities: readonly CapabilityIn[] };

/**
 * WebAssembly binding enum for ExpirationIn.
 */
export type ExpirationIn = { kind: "never" } | { kind: "max-age"; milliseconds: number } | { kind: "at"; milliseconds: number } | { kind: "idle"; milliseconds: number };

/**
 * WebAssembly binding enum for FactOut.
 */
export type FactOut = { kind: "state"; state: "starting" | "running" | "suspending" | "suspended" | "waking" | "destroying" | "destroyed" | "failed" | "indeterminate" } | { kind: "pressure"; pressure: "customer-budget" | "machine-limit" | "service-saturation" } | { kind: "capacity-changed" };

/**
 * WebAssembly binding enum for ForkFidelityOut.
 */
export type ForkFidelityOut = "memory-and-disk" | "disk-only";

/**
 * WebAssembly binding enum for ImageIn.
 */
export type ImageIn = { kind: "managed-oci"; digestHex: string } | { kind: "custom"; digestHex: string } | { kind: "checkpoint"; checkpointId: string };

/**
 * WebAssembly binding enum for ImageOut.
 */
export type ImageOut = { kind: "managed-oci"; digestHex: string } | { kind: "custom"; digestHex: string } | { kind: "checkpoint"; checkpointId: string };

/**
 * WebAssembly binding enum for MutationOut.
 */
export type MutationOut = { kind: "created"; machine: ObservationOut } | { kind: "checkpointed"; checkpoint: CheckpointOut } | { kind: "forked"; machines: readonly ObservationOut[] } | { kind: "machine-forked"; source: string; fidelity: ForkFidelityOut; children: readonly ObservationOut[] } | { kind: "suspended"; machineId: string } | { kind: "woken"; machineId: string } | { kind: "suspension-policy-set"; machineId: string; policy: TimedOut } | { kind: "machine-destroyed"; machineId: string } | { kind: "checkpoint-destroyed"; checkpointId: string };

/**
 * WebAssembly binding enum for SuspensionIn.
 */
export type SuspensionIn = { kind: "manual" } | { kind: "after-idle"; milliseconds: number };

/**
 * WebAssembly binding enum for TimedOut.
 */
export type TimedOut = { kind: "manual" } | { kind: "after-idle"; milliseconds: number } | { kind: "never" } | { kind: "max-age"; milliseconds: number } | { kind: "at"; milliseconds: number } | { kind: "idle"; milliseconds: number };

/**
 * WebAssembly binding struct for BudgetsIn.
 */
export interface BudgetsIn {
    spendMicros: bigint;
    concurrency: number;
}

/**
 * WebAssembly binding struct for BudgetsOut.
 */
export interface BudgetsOut {
    spendMicros: bigint;
    concurrency: number;
}

/**
 * WebAssembly binding struct for CheckpointKey.
 */
export interface CheckpointKey {
    checkpointId: string;
    idempotencyKey: string;
}

/**
 * WebAssembly binding struct for CheckpointOut.
 */
export interface CheckpointOut {
    id: string;
    source: string;
    contract: ContractOut;
    forkable: boolean;
    createdAtUnixMs: number;
}

/**
 * WebAssembly binding struct for ContractOut.
 */
export interface ContractOut {
    image: ImageOut;
    capabilities: readonly CapabilityIn[];
    compatibility: CompatibilityOut;
    compatibilityRevisionHex: string;
    suspension: TimedOut;
    expiration: TimedOut;
    networkPolicyDigestHex: string;
    budgets: BudgetsOut;
}

/**
 * WebAssembly binding struct for CreateIn.
 */
export interface CreateIn {
    idempotencyKey: string;
    image: ImageIn;
    compatibility: CompatibilityIn;
    suspension: SuspensionIn;
    expiration: ExpirationIn;
    networkPolicyDigestHex: string;
    budgets: BudgetsIn;
}

/**
 * WebAssembly binding struct for EndpointOut.
 */
export interface EndpointOut {
    name: string;
    uri: string;
}

/**
 * WebAssembly binding struct for EventOut.
 */
export interface EventOut {
    machine: string;
    sequence: number;
    observedAtUnixMs: number;
    fact: FactOut;
}

/**
 * WebAssembly binding struct for EventsIn.
 */
export interface EventsIn {
    machineId: string;
    afterSequence: number | null;
    limit: number;
}

/**
 * WebAssembly binding struct for EventsOut.
 */
export interface EventsOut {
    events: readonly EventOut[];
    nextSequence: number | null;
}

/**
 * WebAssembly binding struct for ForkIn.
 */
export interface ForkIn {
    checkpointId: string;
    count: number;
    idempotencyKey: string;
}

/**
 * WebAssembly binding struct for ListIn.
 */
export interface ListIn {
    after: string | null;
    limit: number;
}

/**
 * WebAssembly binding struct for MachineForkIn.
 */
export interface MachineForkIn {
    machineId: string;
    count: number;
    idempotencyKey: string;
}

/**
 * WebAssembly binding struct for MachineKey.
 */
export interface MachineKey {
    machineId: string;
    idempotencyKey: string;
}

/**
 * WebAssembly binding struct for ObservationOut.
 */
export interface ObservationOut {
    id: string;
    state: "starting" | "running" | "suspending" | "suspended" | "waking" | "destroying" | "destroyed" | "failed" | "indeterminate";
    contract: ContractOut;
    endpoints: readonly EndpointOut[];
    lastCheckpoint: string | null;
    createdAtUnixMs: number;
    changedAtUnixMs: number;
}

/**
 * WebAssembly binding struct for OperationOut.
 */
export interface OperationOut {
    id: string;
    phase: "pending" | "succeeded" | "cancelled" | "indeterminate" | "failed";
}

/**
 * WebAssembly binding struct for PageOut.
 */
export interface PageOut {
    machines: readonly ObservationOut[];
    next: string | null;
}

/**
 * WebAssembly binding struct for PolicyIn.
 */
export interface PolicyIn {
    machineId: string;
    policy: SuspensionIn;
    idempotencyKey: string;
}

/**
 * WebAssembly binding struct for QualificationOut.
 */
export interface QualificationOut {
    image: ImageOut;
    capabilities: readonly CapabilityIn[];
    compatibilityRevisionHex: string;
}

/**
 * WebAssembly binding struct for UsageIn.
 */
export interface UsageIn {
    machineId: string;
    startUnixMs: number;
    endUnixMs: number;
}

/**
 * WebAssembly binding struct for UsageOut.
 */
export interface UsageOut {
    machine: string;
    startUnixMs: number;
    endUnixMs: number;
    elasticCpuNs: bigint;
    dedicatedCpuNs: bigint;
    privateResidentByteSeconds: bigint;
    durablePrivateBytes: bigint;
    lineageReceiptSha256: Uint8Array;
    egressBytes: bigint;
    receipt: Uint8Array;
}

export type MachinesHttpRoute =
| "images/qualify"
| "machines/create"
| "machines/inspect"
| "machines/list"
| "checkpoints/inspect"
| "machines/checkpoint"
| "machines/fork"
| "checkpoints/fork"
| "machines/suspend"
| "machines/wake"
| "machines/suspension-policy"
| "machines/destroy"
| "checkpoints/destroy"
| "machines/events"
| "machines/usage"
| "operations/recover"
| "operations/recover-id"
| "operations/inspect"
| "operations/cancel"
| "operations/watch"
;

export interface MachinesHttpRoutes {
    readonly IMAGES_QUALIFY: "images/qualify";
    readonly MACHINES_CREATE: "machines/create";
    readonly MACHINES_INSPECT: "machines/inspect";
    readonly MACHINES_LIST: "machines/list";
    readonly CHECKPOINTS_INSPECT: "checkpoints/inspect";
    readonly MACHINES_CHECKPOINT: "machines/checkpoint";
    readonly MACHINES_FORK: "machines/fork";
    readonly CHECKPOINTS_FORK: "checkpoints/fork";
    readonly MACHINES_SUSPEND: "machines/suspend";
    readonly MACHINES_WAKE: "machines/wake";
    readonly MACHINES_SUSPENSION_POLICY: "machines/suspension-policy";
    readonly MACHINES_DESTROY: "machines/destroy";
    readonly CHECKPOINTS_DESTROY: "checkpoints/destroy";
    readonly MACHINES_EVENTS: "machines/events";
    readonly MACHINES_USAGE: "machines/usage";
    readonly OPERATIONS_RECOVER: "operations/recover";
    readonly OPERATIONS_RECOVER_ID: "operations/recover-id";
    readonly OPERATIONS_INSPECT: "operations/inspect";
    readonly OPERATIONS_CANCEL: "operations/cancel";
    readonly OPERATIONS_WATCH: "operations/watch";
}

export function httpRoutes(): MachinesHttpRoutes;
export function httpRoute(route: MachinesHttpRoute): MachinesHttpRoute;

export interface MachinesHttpRequestMap {
    "images/qualify": { readonly image: ImageIn };
    "machines/create": CreateIn;
    "machines/inspect": { readonly machineId: string };
    "machines/list": ListIn;
    "checkpoints/inspect": { readonly checkpointId: string };
    "machines/checkpoint": MachineKey;
    "machines/fork": { readonly machineId: string; readonly count: number; readonly idempotencyKey: string };
    "checkpoints/fork": ForkIn;
    "machines/suspend": MachineKey;
    "machines/wake": MachineKey;
    "machines/suspension-policy": PolicyIn;
    "machines/destroy": MachineKey;
    "checkpoints/destroy": CheckpointKey;
    "machines/events": EventsIn;
    "machines/usage": UsageIn;
    "operations/recover": { readonly idempotencyKey: string };
    "operations/recover-id": { readonly idempotencyKey: string };
    "operations/inspect": { readonly operationId: string };
    "operations/cancel": { readonly operationId: string };
    "operations/watch": { readonly operationId: string };
}

export type MachinesHttpRequest<Route extends MachinesHttpRoute> =
MachinesHttpRequestMap[Route];
export type MachinesHttpRequestUnion =
MachinesHttpRequestMap[MachinesHttpRoute];

export interface MachinesHttpResponseMap {
    "images/qualify": QualificationOut;
    "machines/create": MutationOut;
    "machines/inspect": ObservationOut;
    "machines/list": PageOut;
    "checkpoints/inspect": CheckpointOut;
    "machines/checkpoint": MutationOut;
    "machines/fork": MutationOut;
    "checkpoints/fork": MutationOut;
    "machines/suspend": MutationOut;
    "machines/wake": MutationOut;
    "machines/suspension-policy": MutationOut;
    "machines/destroy": MutationOut;
    "checkpoints/destroy": MutationOut;
    "machines/events": EventsOut;
    "machines/usage": UsageOut;
    "operations/recover": MutationOut;
    "operations/recover-id": string;
    "operations/inspect": OperationOut;
    "operations/cancel": OperationOut;
    "operations/watch": readonly OperationOut[];
}

export type MachinesHttpResponse<Route extends MachinesHttpRoute> =
MachinesHttpResponseMap[Route];
export type MachinesHttpResponseUnion =
MachinesHttpResponseMap[MachinesHttpRoute];



/**
 * Stateful adapter around `acyclic_machines::SimulatedMachines`.
 */
export class WasmSimulatedMachines {
    free(): void;
    [Symbol.dispose](): void;
    cancel(operation_id: string): Promise<OperationOut>;
    checkpoint(request: MachineKey): Promise<MutationOut>;
    create(request: CreateIn): Promise<MutationOut>;
    /**
     * Decodes a hosted HTTP response using the Rust-owned scalar wrappers and
     * the same public DTO shape as simulator methods.
     * WebAssembly binding fn for decode_http_response.
     */
    static decodeHttpResponse<Route extends MachinesHttpRoute>(route: Route, response_json: string, expected_json: string): MachinesHttpResponse<Route>;
    destroyCheckpoint(request: CheckpointKey): Promise<MutationOut>;
    destroyMachine(request: MachineKey): Promise<MutationOut>;
    /**
     * Encodes a natural hosted request using Rust-owned bigint and bytes
     * wrappers before it crosses the HTTP boundary.
     */
    static encodeHttpRequest(request: MachinesHttpRequestUnion): string;
    events(request: EventsIn): Promise<EventsOut>;
    fork(request: ForkIn): Promise<MutationOut>;
    forkMachine(request: MachineForkIn): Promise<MutationOut>;
    inspectCheckpoint(checkpoint_id: string): Promise<CheckpointOut>;
    inspectMachine(machine_id: string): Promise<ObservationOut>;
    inspectOperation(operation_id: string): Promise<OperationOut>;
    listMachines(request: ListIn): Promise<PageOut>;
    /**
     * Creates an isolated process-local simulator.
     */
    constructor();
    /**
     * Runs one public operation through its Rust-derived input and output DTO.
     *
     * The operation-specific methods below deliberately keep the route names
     * out of TypeScript.  Each method's ABI is generated from its Tsify DTO,
     * while the shared Rust route table remains the single behavior source.
     */
    qualifyImage(image: ImageIn): Promise<QualificationOut>;
    recover(idempotency_key: string): Promise<MutationOut>;
    recoverOperation(idempotency_key: string): Promise<string>;
    setSuspensionPolicy(request: PolicyIn): Promise<MutationOut>;
    suspend(request: MachineKey): Promise<MutationOut>;
    usage(request: UsageIn): Promise<UsageOut>;
    /**
     * Validates and projects a hosted HTTP response against its request context.
     * WebAssembly binding fn for validate_http_response.
     */
    static validate_http_response(route: MachinesHttpRoute, response_json: string, expected_json: string): void;
    wake(request: MachineKey): Promise<MutationOut>;
    watchOperation(operation_id: string): Promise<readonly OperationOut[]>;
}

/**
 * Canonicalizes a hosted HTTP route through the Rust-owned route table before
 * a client uses it to construct a request URL.
 */
export function httpRoute(route: MachinesHttpRoute): MachinesHttpRoute;

/**
 * Returns the hosted route values emitted from the Rust route contract.
 * WebAssembly binding fn for http_routes.
 */
export function httpRoutes(): MachinesHttpRoutes;

/**
 * Parses and normalizes an immutable OCI image reference using the canonical
 * Machines image constructor.
 */
export function managedOci(reference: string): ImageOut;

/**
 * Validates and normalizes a hosted HTTPS Machines endpoint using the shared
 * Rust transport policy. URL construction for individual routes remains in
 * the TypeScript fetch adapter.
 */
export function normalizeEndpoint(endpoint: string): string;

/**
 * Normalize a caller identity to a retained UUID or a deterministic UUIDv5-like value.
 * Valid non-nil UUID strings are preserved byte-for-byte in canonical form.
 * WebAssembly binding fn for normalize_identity.
 */
export function normalize_identity(kind: string, value: string): string;

/**
 * Validates one opaque bearer credential before the fetch adapter formats it.
 */
export function validateBearerToken(token: string): void;

/**
 * Validates the response bound using the same safe-number rule as DTO inputs.
 */
export function validateMaximumResponseBytes(maximum: number): void;

/**
 * Validates the canonical Machines page-size policy before any provider is
 * called, including custom TypeScript providers.
 */
export function validatePageSize(value: number): void;

/**
 * Validates one JavaScript number before it is used as a Rust u64 input.
 */
export function validateSafeU64(value: any): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_wasmsimulatedmachines_free: (a: number, b: number) => void;
    readonly httpRoute: (a: number, b: number) => [number, number, number, number];
    readonly httpRoutes: () => any;
    readonly managedOci: (a: number, b: number) => [number, number, number];
    readonly normalizeEndpoint: (a: number, b: number) => [number, number, number, number];
    readonly normalize_identity: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly validateBearerToken: (a: number, b: number) => [number, number];
    readonly validateMaximumResponseBytes: (a: any) => [number, number];
    readonly validatePageSize: (a: any) => [number, number];
    readonly validateSafeU64: (a: any) => [number, number];
    readonly wasmsimulatedmachines_cancel: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_checkpoint: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_create: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_decodeHttpResponse: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number];
    readonly wasmsimulatedmachines_destroyCheckpoint: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_destroyMachine: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_encodeHttpRequest: (a: any) => [number, number, number, number];
    readonly wasmsimulatedmachines_events: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_fork: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_forkMachine: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_inspectCheckpoint: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_inspectMachine: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_inspectOperation: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_listMachines: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_new: () => number;
    readonly wasmsimulatedmachines_qualifyImage: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_recover: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_recoverOperation: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_setSuspensionPolicy: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_suspend: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_usage: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_validate_http_response: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number];
    readonly wasmsimulatedmachines_wake: (a: number, b: any) => any;
    readonly wasmsimulatedmachines_watchOperation: (a: number, b: any) => any;
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke___wasm_bindgen_2db2d17d2c533688___JsValue__core_ed718c3d60ebd546___result__Result_____wasm_bindgen_2db2d17d2c533688___JsError___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_2db2d17d2c533688___convert__closures_____invoke___js_sys_a5471da919551fc6___Function_fn_wasm_bindgen_2db2d17d2c533688___JsValue_____wasm_bindgen_2db2d17d2c533688___sys__Undefined___js_sys_a5471da919551fc6___Function_fn_wasm_bindgen_2db2d17d2c533688___JsValue_____wasm_bindgen_2db2d17d2c533688___sys__Undefined_______true_: (a: number, b: number, c: any, d: any) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
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
