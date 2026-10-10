import initWasm, { type InitInput } from "../generated/wasm/acyclic_harness_wasm.js";

let initialization: Promise<void> | undefined;

/** Every JS function used by either Harness initialization path. */
export const REQUIRED_HARNESS_WASM_EXPORTS = [
  "validateContract", "validateWorkflowAdmission", "verifyFileBytes", "decodeAttachmentManifest",
  "encodeAttachmentManifest", "forkSeedFromReport", "validateToolValue",
  "validateWireHandshake", "validateWireCommand", "validateWireCommandProtocol",
  "validateWireResume", "validateWireObserve", "validateWireCancel",
  "validateWireAdmission", "validateWireStatus", "validateWireCancellation",
  "validateToolDefinition", "validateToolInvocation", "validateToolResult", "validateToolProjection", "jsonToolProjectionSchema", "modelContentInventory",
  "validateMcpCatalog", "searchMcpCatalog", "mcpModelDefinitions", "validateMcpStdioRequest",
  "validateModelContent", "prepareModelRequest", "encodeModelPrefix", "validateModelMessages", "validateUserInput", "admitModelEvent", "selectModelContext",
  "defaultCompactionPolicy", "validateThresholdCompaction", "validateModelTokenCount", "decodeExecutionEventJson",
  "validateModelContextSelection", "validateSelectedModelContext",
  "validateContextSelection", "applyContextProjection", "parseSkillMetadata", "projectDiscoveredContext",
  "captureDiscoveredContext", "contextForRequest", "readPinnedContextPath",
  "prepareConversationTurn",
  "validateConversationMessageId", "validateIdentity", "deriveOperationUuid", "batchMemberOperationId", "taskIdentityDigest",
  "validateAuthorityPathSegment", "validateComponentLabel",
  "taskAdmissionIdentities", "admitTask", "admitBatch", "admitBatchRequest",
  "validateTaskRequirements",
  "validateTaskChildrenPage",
  "fileDescriptor", "uuidFromDigestHalf", "decodeCanonicalJson", "decodeJson",
  "encodeCanonicalJson", "digestCanonicalJson",
] as const satisfies readonly (keyof typeof import("../generated/wasm/acyclic_harness_wasm.js"))[];

// `initWasm()` resolves to the raw instance exports. The generated JS module
// exposes WasmContentStore as a wrapper class, but its methods call these ABI
// symbols on the raw instance.
const REQUIRED_WASM_CONTENT_EXPORTS = [
  "__wbg_wasmcontentstore_free", "wasmcontentstore_generation",
  "wasmcontentstore_has", "wasmcontentstore_list", "wasmcontentstore_new",
  "wasmcontentstore_pathConflicts", "wasmcontentstore_read",
  "wasmcontentstore_read_path", "wasmcontentstore_stage",
] as const;

const CLIENT_VIEW_METHODS = ["advance", "begin", "discard", "observe", "release", "residency", "status", "view", "free"] as const;

/** Reject a stale binding before runtime code can call a missing validator. */
export function assertHarnessWasmExports(value: unknown): void {
  if (value === null || typeof value !== "object") {
    throw new Error("harness WASM does not provide the required validators");
  }
  const exports = value as Record<string, unknown>;
  const contentStore = typeof exports.WasmContentStore === "function"
    ? true
    : REQUIRED_WASM_CONTENT_EXPORTS.every(name => typeof exports[name] === "function");
  const clientViews = typeof exports.WasmClientViews === "function"
    ? CLIENT_VIEW_METHODS.every(name => typeof (exports.WasmClientViews as { prototype: Record<string, unknown> }).prototype[name] === "function")
    : typeof exports.wasmclientviews_new === "function" && CLIENT_VIEW_METHODS.every(name =>
      typeof exports[name === "free" ? "__wbg_wasmclientviews_free" : `wasmclientviews_${name}`] === "function");
  const prefix = typeof exports.WasmReducer === "function"
    ? typeof (exports.WasmReducer as { prototype: Record<string, unknown> }).prototype.prepareInheritedModelRequest === "function"
    : typeof exports.wasmreducer_prepareInheritedModelRequest === "function";
  const forkPolicy = typeof exports.WasmReducer === "function"
    ? typeof (exports.WasmReducer as { prototype: Record<string, unknown> }).prototype.prepareForkRequest === "function"
    : typeof exports.wasmreducer_prepareForkRequest === "function";
  if (!contentStore || !clientViews || !prefix || !forkPolicy || REQUIRED_HARNESS_WASM_EXPORTS.some(name => typeof exports[name] !== "function")) {
    throw new Error("harness WASM does not provide the required validators");
  }
}

/** Initializes the one WASM instance shared by the reducer and wire transports. */
export async function ensureHarnessWasm(): Promise<void> {
  if (initialization !== undefined) {
    await initialization;
    return;
  }
  let moduleLoaded = false;
  const attempt = (async () => {
    const source = await packagedWasm();
    const exports = await initWasm({ module_or_path: source });
    moduleLoaded = true;
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
