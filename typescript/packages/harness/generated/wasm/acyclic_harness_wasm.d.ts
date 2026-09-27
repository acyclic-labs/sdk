/* tslint:disable */
/* eslint-disable */
/**
 * Ordered authority-resolution level from the runtime root to one invocation.
 */
export type AuthorityLevel = "runtime" | "agent" | "conversation" | "session" | "turn" | "task" | "invocation";

/**
 * Stable wire identity used during compatibility handshakes.
 */
export interface ProtocolIdentity {
    /**
     * Semantic protocol version.
     */
    version: string;
    /**
     * Digest of the canonical descriptor set.
     */
    descriptor_digest: string;
}


/**
 * Opaque synchronous reducer hosted in WebAssembly.
 */
export class WasmReducer {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Applies a deterministic command. Fresh extension migrations require the
     * native content-admission host and cannot be synthesized by this reducer.
     */
    apply(command: any): any;
    /**
     * Applies one canonical Protobuf command, excluding fresh migrations
     * that require executable content admission, and returns a response.
     */
    applyWire(command: Uint8Array): Uint8Array;
    /**
     * Attenuates a scope without permitting capability expansion.
     */
    attenuate(parent: any, id: string, capabilities: any): any;
    /**
     * Returns the authoritative conversation projection, never a parallel JS reducer.
     */
    conversationJson(): string;
    /**
     * Returns the same bounded page with Rust-owned JS integer projection:
     * revisions and cursors stay `BigInt`, validated file lengths become Number.
     */
    conversationPage(after_sequence: bigint, limit: number): any;
    /**
     * Returns an immutable, bounded page of canonical conversation records.
     * `after_sequence` is an exclusive cursor in the message sequence, not
     * the potentially larger aggregate event revision.
     */
    conversationPageJson(after_sequence: bigint, limit: number): string;
    /**
     * Resolves a complete canonical attachment manifest under the Rust rules.
     */
    decodeAttachmentManifest(manifest: any, bytes: Uint8Array, item_count: number): any;
    /**
     * Delegates a segment-bounded private directory to a reader without a fork.
     */
    delegatePrivateDirectoryRead(owner_scope: any, reader: string, id: string, volume: any, prefix: string): any;
    /**
     * Delegates one pinned private file from its signed owner to a reader.
     */
    delegatePrivateFileRead(owner_scope: any, reader: string, id: string, file: any): any;
    /**
     * Computes the canonical capability for one private directory boundary.
     */
    directoryReadCapability(volume: any, prefix: string): string;
    /**
     * Derives the canonical SHA-256 descriptor for staged bytes.
     */
    fileDescriptorJson(bytes: Uint8Array, media_type: string): string;
    /**
     * Computes the exact-version read capability without embedding it in a ref.
     */
    fileReadCapability(file: any): string;
    /**
     * Issues a root scope from this host's explicit authority object.
     */
    issueScope(id: string, capabilities: any): any;
    /**
     * Issues a root scope signed for one acting agent.
     */
    issueScopeForAgent(agent: string, id: string, capabilities: any): any;
    /**
     * Resolves named policy layers and issues only their effective grant.
     */
    issueScopeWithPolicies(id: string, layers: any): any;
    /**
     * Resolves hierarchy policy and binds its result to one acting agent.
     */
    issueScopeWithPoliciesForAgent(agent: string, id: string, layers: any): any;
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
    /**
     * Validates one ref-only canonical message under explicit runtime limits.
     */
    validateConversationMessage(message: any, limits: any): void;
    /**
     * Validates an exact content reference using the native Rust contract.
     */
    validateFileRef(file: any): void;
    /**
     * Applies configured runtime bounds to one exact file reference.
     */
    validateFileUnderLimits(file: any, limits: any): void;
    /**
     * Authenticates an owner-issued read grant for this exact immutable file.
     * A caller-supplied capability string alone is never accepted as proof.
     */
    verifyContentRead(scope: any, file: any): void;
    /**
     * Authenticates a writer against the original private-volume owner.
     */
    verifyContentWrite(scope: any, volume: any): void;
    /**
     * Verifies exact bytes against the pinned SHA-256 and byte-length descriptor.
     */
    verifyFileBytes(file: any, bytes: Uint8Array): void;
    /**
     * Authenticates lazy directory and named-path access against the
     * owner's signed, segment-bounded private-volume read grant.
     */
    verifyPrivateDirectoryRead(scope: any, volume: any, granted_prefix: string, path: string): void;
    /**
     * Authenticates the signed scope before provider discovery or routing.
     */
    verifyScope(scope: any): void;
    /**
     * Computes the canonical Rust capability for one validated volume operation.
     */
    volumeCapability(volume: any, operation: string): string;
    /**
     * Stable physical namespace shared by Filesystem and Objects adapters.
     */
    volumeStorageName(volume: any): string;
}

/**
 * Derives the same immutable per-slot operation as Rust durable admission.
 */
export function batchMemberOperationId(group: string, batch: string, index: number): string;

/**
 * Decodes a generated aggregate kind using the native enum mapping.
 */
export function decodeAggregateKind(value: number): any;

/**
 * Decodes and validates a canonical Protobuf apply response using Rust-owned
 * event, authority, scope, digest, and payload rules.
 */
export function decodeApplyResponse(bytes: Uint8Array): any;

/**
 * Decodes a complete canonical attachment list without a reducer instance.
 */
export function decodeAttachmentManifest(manifest: any, bytes: Uint8Array, item_count: number): any;

/**
 * Parses canonical JSON without passing full-width integer literals through
 * JavaScript Number. Large serde integers are returned as `BigInt`.
 */
export function decodeCanonicalJson(bytes: Uint8Array): any;

/**
 * Decodes one canonical event payload using the native event union.
 */
export function decodeEventPayload(event_type: string, canonical_payload_json: Uint8Array): any;

/**
 * Parses external JSON with exact integers but without demanding canonical
 * key order or whitespace. Callers must still apply their schema and numeric
 * range policy before presenting model-authored values to an executor.
 */
export function decodeJson(bytes: Uint8Array): any;

/**
 * Derives a stable child operation/message identity from one admitted operation
 * and a local role label without duplicating UUID bit manipulation in hosts.
 */
export function deriveOperationUuid(operation: string, label: string): string;

/**
 * Hashes the same bounded canonical JSON bytes used by native admissions.
 */
export function digestCanonicalJson(value: any): Uint8Array;

/**
 * Encodes a validated attachment list in the exact typed manifest wire form.
 */
export function encodeAttachmentManifest(items: any): Uint8Array;

/**
 * Serializes a plain JavaScript data value through Rust's canonical JSON
 * representation. Unsafe integer Numbers are rejected before conversion;
 * callers must supply `BigInt` for exact full-width identities and counters.
 */
export function encodeCanonicalJson(value: any): Uint8Array;

/**
 * Stages a descriptor with Rust-owned SHA-256, media-type, and safe
 * byte-length rules, projecting its bounded length as a JS Number.
 */
export function fileDescriptor(bytes: Uint8Array, media_type: string): any;

/**
 * Converts a fully captured report into its canonical publishable child seed.
 */
export function forkSeedFromReport(report: any): any;

/**
 * Derives the same pinned task registration digest used by native admission.
 */
export function taskIdentityDigest(name: string, version: string, input_schema: any, output_schema: any, requirements: any, machine_digest: Uint8Array): Uint8Array;

/**
 * Uses one half of a canonical action digest as a stable local identity.
 * The digest is already SHA-256; the two halves separate approval operations
 * from their interaction tickets without a second hashing convention.
 */
export function uuidFromDigestHalf(digest: Uint8Array, second: boolean): string;

/**
 * Pure v2 contract admission shared by native and JavaScript hosts. The
 * returned object is detached and canonically shaped by Rust serde; context
 * supplies `Limits` for messages and the open ticket for resolutions.
 */
export function validateContract(kind: string, value: any, context: any): any;

/**
 * Returns the one Rust UUID spelling accepted for a conversation identity.
 */
export function validateConversationMessageId(value: string): string;

/**
 * Parses one public Harness identity with the canonical Rust contract and
 * returns its normalized UUID spelling for a branded TypeScript facade.
 */
export function validateIdentity(kind: string, value: string): string;

/**
 * Validates a model-visible tool definition using the native contract.
 */
export function validateToolDefinition(definition: any): void;

/**
 * Validates one tool invocation against its registered definition.
 */
export function validateToolInvocation(definition: any, invocation: any): void;

/**
 * Validates one successful tool result against its registered definition.
 */
export function validateToolResult(definition: any, result: any): void;

/**
 * Applies the same JSON Schema admission used by Rust tool execution before
 * a TypeScript facade turns an untrusted model value into a typed argument.
 */
export function validateToolValue(schema: any, value: any): any;

/**
 * Validates a protobuf admission identity; returns encoded `Error` bytes, or empty on success.
 */
export function validateWireAdmission(command: Uint8Array, admission: Uint8Array): Uint8Array;

/**
 * Validates a protobuf cancel request using the canonical Rust scope rules.
 */
export function validateWireCancel(request: Uint8Array): Uint8Array;

/**
 * Validates a protobuf cancellation identity; returns encoded `Error` bytes, or empty on success.
 */
export function validateWireCancellation(request: Uint8Array, response: Uint8Array): Uint8Array;

/**
 * Validates one complete protobuf command before it crosses a wire adapter.
 */
export function validateWireCommand(command: Uint8Array): Uint8Array;

/**
 * Validates only the protocol identity of a command envelope.
 */
export function validateWireCommandProtocol(command: Uint8Array): Uint8Array;

/**
 * Validates a protobuf handshake; returns encoded `Error` bytes, or empty on success.
 */
export function validateWireHandshake(request: Uint8Array, response: Uint8Array): Uint8Array;

/**
 * Validates a protobuf observe request using the canonical Rust scope rules.
 */
export function validateWireObserve(request: Uint8Array): Uint8Array;

/**
 * Validates a protobuf replay request against the compiled protocol identity.
 */
export function validateWireResume(request: Uint8Array): Uint8Array;

/**
 * Validates a protobuf operation status identity; returns encoded `Error` bytes, or empty on success.
 */
export function validateWireStatus(request: Uint8Array, status: Uint8Array): Uint8Array;

/**
 * Checks immutable file identity without constructing a reducer or issuer.
 */
export function verifyFileBytes(file: any, bytes: Uint8Array): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_wasmreducer_free: (a: number, b: number) => void;
    readonly batchMemberOperationId: (a: number, b: number, c: number, d: number, e: number) => [number, number, number, number];
    readonly decodeAggregateKind: (a: number) => [number, number, number];
    readonly decodeApplyResponse: (a: number, b: number) => [number, number, number];
    readonly decodeAttachmentManifest: (a: any, b: number, c: number, d: number) => [number, number, number];
    readonly decodeCanonicalJson: (a: number, b: number) => [number, number, number];
    readonly decodeEventPayload: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly decodeJson: (a: number, b: number) => [number, number, number];
    readonly deriveOperationUuid: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly digestCanonicalJson: (a: any) => [number, number, number, number];
    readonly encodeAttachmentManifest: (a: any) => [number, number, number, number];
    readonly encodeCanonicalJson: (a: any) => [number, number, number, number];
    readonly fileDescriptor: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly forkSeedFromReport: (a: any) => [number, number, number];
    readonly taskIdentityDigest: (a: number, b: number, c: number, d: number, e: any, f: any, g: any, h: number, i: number) => [number, number, number, number];
    readonly uuidFromDigestHalf: (a: number, b: number, c: number) => [number, number, number, number];
    readonly validateContract: (a: number, b: number, c: any, d: any) => [number, number, number];
    readonly validateConversationMessageId: (a: number, b: number) => [number, number, number, number];
    readonly validateIdentity: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly validateToolDefinition: (a: any) => [number, number];
    readonly validateToolInvocation: (a: any, b: any) => [number, number];
    readonly validateToolResult: (a: any, b: any) => [number, number];
    readonly validateToolValue: (a: any, b: any) => [number, number, number];
    readonly validateWireAdmission: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateWireCancel: (a: number, b: number) => [number, number];
    readonly validateWireCancellation: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateWireCommand: (a: number, b: number) => [number, number];
    readonly validateWireCommandProtocol: (a: number, b: number) => [number, number];
    readonly validateWireHandshake: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateWireObserve: (a: number, b: number) => [number, number];
    readonly validateWireResume: (a: number, b: number) => [number, number];
    readonly validateWireStatus: (a: number, b: number, c: number, d: number) => [number, number];
    readonly verifyFileBytes: (a: any, b: number, c: number) => [number, number];
    readonly wasmreducer_apply: (a: number, b: any) => [number, number, number];
    readonly wasmreducer_applyWire: (a: number, b: number, c: number) => [number, number, number, number];
    readonly wasmreducer_attenuate: (a: number, b: any, c: number, d: number, e: any) => [number, number, number];
    readonly wasmreducer_conversationJson: (a: number) => [number, number, number, number];
    readonly wasmreducer_conversationPage: (a: number, b: bigint, c: number) => [number, number, number];
    readonly wasmreducer_conversationPageJson: (a: number, b: bigint, c: number) => [number, number, number, number];
    readonly wasmreducer_decodeAttachmentManifest: (a: number, b: any, c: number, d: number, e: number) => [number, number, number];
    readonly wasmreducer_delegatePrivateDirectoryRead: (a: number, b: any, c: number, d: number, e: number, f: number, g: any, h: number, i: number) => [number, number, number];
    readonly wasmreducer_delegatePrivateFileRead: (a: number, b: any, c: number, d: number, e: number, f: number, g: any) => [number, number, number];
    readonly wasmreducer_directoryReadCapability: (a: number, b: any, c: number, d: number) => [number, number, number, number];
    readonly wasmreducer_fileDescriptorJson: (a: number, b: number, c: number, d: number, e: number) => [number, number, number, number];
    readonly wasmreducer_fileReadCapability: (a: number, b: any) => [number, number, number, number];
    readonly wasmreducer_issueScope: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmreducer_issueScopeForAgent: (a: number, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_issueScopeWithPolicies: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmreducer_issueScopeWithPoliciesForAgent: (a: number, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_new: (a: any, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_protocolIdentity: (a: number) => [number, number, number];
    readonly wasmreducer_restore: (a: any, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_snapshot: (a: number) => [number, number, number];
    readonly wasmreducer_validateConversationMessage: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_validateFileRef: (a: number, b: any) => [number, number];
    readonly wasmreducer_validateFileUnderLimits: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyContentRead: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyContentWrite: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyFileBytes: (a: number, b: any, c: number, d: number) => [number, number];
    readonly wasmreducer_verifyPrivateDirectoryRead: (a: number, b: any, c: any, d: number, e: number, f: number, g: number) => [number, number];
    readonly wasmreducer_verifyScope: (a: number, b: any) => [number, number];
    readonly wasmreducer_volumeCapability: (a: number, b: any, c: number, d: number) => [number, number, number, number];
    readonly wasmreducer_volumeStorageName: (a: number, b: any) => [number, number, number, number];
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
