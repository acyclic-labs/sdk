import initWasm, { type InitInput } from "../generated/wasm/acyclic_harness_wasm.js";

type HarnessWasm = typeof import("../generated/wasm/acyclic_harness_wasm.js");

let initialization: Promise<void> | undefined;
let loadedInput: InitInput | undefined;
let binding: Promise<HarnessWasm> | undefined;

/** Returns the generated Rust/WASM module used by all Harness host adapters. */
export async function loadHarnessWasm(): Promise<HarnessWasm> {
  binding ??= (async () => {
    const module = await import("../generated/wasm/acyclic_harness_wasm.js");
    await ensureHarnessWasm();
    return module;
  })();
  return binding;
}

/** Rust-owned authenticated browser transport with generated protobuf bytes. */
export type RustHarnessRemoteClient = Awaited<
  ReturnType<HarnessWasm["BrowserHarnessRemoteClient"]["connect"]>
>;

/** Every JS function used by either Harness initialization path. */
export const REQUIRED_HARNESS_WASM_EXPORTS = [
  "validateContract", "verifyFileBytes", "decodeAttachmentManifest",
  "encodeAttachmentManifest", "forkSeedFromReport", "validateToolValue",
  "validateWireHandshake", "validateWireCommand", "validateWireCommandProtocol",
  "validateWireResume", "validateWireObserve", "validateWireCancel",
  "validateWireAdmission", "validateWireStatus", "validateWireCancellation",
  "validateToolDefinition", "validateToolInvocation", "validateToolResult",
  "validateModelContent", "validateModelMessages", "validateUserInput", "admitModelEvent", "selectModelContext",
  "validateModelContextSelection", "validateSelectedModelContext",
  "prepareConversationTurn",
  "validateConversationMessageId", "validateIdentity", "deriveOperationUuid", "batchMemberOperationId", "taskIdentityDigest",
  "taskAdmissionIdentities", "admitTask", "admitBatch", "admitBatchRequest",
  "validateTaskRequirements",
  "validateTaskChildrenPage",
  "fileDescriptor", "uuidFromDigestHalf", "decodeCanonicalJson", "decodeJson",
  "encodeCanonicalJson", "digestCanonicalJson",
  "validateOfflineCommand", "validateReplayDelivery",
  "harnessDefaultResidentBytes", "harnessDefaultResidentFiles",
  "harnessMaxInlineAttachments", "harnessAttachmentManifestMediaType",
] as const satisfies readonly (keyof typeof import("../generated/wasm/acyclic_harness_wasm.js"))[];

// `initWasm()` resolves to the raw instance exports. The generated JS module
// exposes WasmContentStore as a wrapper class, but its methods call these ABI
// symbols on the raw instance.
const REQUIRED_WASM_CONTENT_EXPORTS = [
  "__wbg_wasmcontentstore_free", "wasmcontentstore_generation",
  "wasmcontentstore_has", "wasmcontentstore_list", "wasmcontentstore_new",
  "wasmcontentstore_newDefault",
  "wasmcontentstore_pathConflicts", "wasmcontentstore_read",
  "wasmcontentstore_read_path", "wasmcontentstore_stage",
] as const;

/** Reject a stale binding before runtime code can call a missing validator. */
export function assertHarnessWasmExports(value: unknown): void {
  if (value === null || typeof value !== "object") {
    throw new Error("harness WASM does not provide the required validators");
  }
  const exports = value as Record<string, unknown>;
  const contentStore = typeof exports.WasmContentStore === "function"
    ? true
    : REQUIRED_WASM_CONTENT_EXPORTS.every(name => typeof exports[name] === "function");
  if (!contentStore || REQUIRED_HARNESS_WASM_EXPORTS.some(name => typeof exports[name] !== "function")) {
    throw new Error("harness WASM does not provide the required validators");
  }
}

/** Initializes the one WASM instance shared by the reducer and wire transports. */
export async function ensureHarnessWasm(input?: InitInput): Promise<void> {
  if (initialization !== undefined) {
    await initialization;
    if (input !== undefined && (loadedInput === undefined || !sameInput(input, loadedInput))) {
      throw new Error("harness WASM was already initialized from a different source");
    }
    return;
  }
  let moduleLoaded = false;
  const attempt = (async () => {
    const source = input ?? await packagedWasm();
    const exports = await initWasm({ module_or_path: source });
    moduleLoaded = true;
    loadedInput = source;
    assertHarnessWasmExports(exports);
  })();
  initialization = attempt;
  try {
    await attempt;
  } catch (error) {
    if (!moduleLoaded && initialization === attempt) initialization = undefined;
    throw error;
  }
}

async function packagedWasm(): Promise<InitInput> {
  const url = new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url);
  if (url.protocol !== "file:") return url;
  const { readFile } = await import("node:fs/promises");
  return Uint8Array.from(await readFile(url));
}

function sameInput(left: InitInput, right: InitInput): boolean {
  if (left === right) return true;
  const leftBytes = bytesOf(left);
  const rightBytes = bytesOf(right);
  if (leftBytes !== undefined && rightBytes !== undefined) {
    return leftBytes.length === rightBytes.length && leftBytes.every((byte, index) => byte === rightBytes[index]);
  }
  if (left instanceof URL && right instanceof URL) return left.href === right.href;
  return false;
}

function bytesOf(input: InitInput): Uint8Array | undefined {
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  return undefined;
}
