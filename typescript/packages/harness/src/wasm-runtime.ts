import initWasm from "../generated/wasm/acyclic_harness_wasm.js";

let initialization: Promise<void> | undefined;

/** Every JS function used by either Harness initialization path. */
const REQUIRED_HARNESS_WASM_EXPORTS = [
  "validateContract", "validateWorkflowAdmission", "verifyFileBytes", "decodeAttachmentManifest",
  "encodeAttachmentManifest", "forkSeedFromReport", "validateToolValue",
  "validateWireHandshake", "validateWireCommand", "validateWireCommandProtocol",
  "validateWireResume", "validateWireObserve", "validateWireCancel",
  "validateWireAdmission", "validateWireStatus", "validateWireCancellation",
  "validateToolDefinition", "validateToolInvocation", "validateToolResult", "validateToolProjection", "jsonToolProjectionSchema", "modelContentInventory",
  "validateMcpCatalog", "searchMcpCatalog", "mcpModelDefinitions", "validateMcpStdioRequest",
  "validateMcpCallbackRequest", "mcpCallbackOperation",
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

// Check raw WASM symbols used by generated wrapper methods.
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
  const reducer = ["prepareInheritedModelRequest", "prepareForkRequest"].every(name =>
    typeof exports.WasmReducer === "function"
      ? typeof (exports.WasmReducer as { prototype: Record<string, unknown> }).prototype[name] === "function"
      : typeof exports[`wasmreducer_${name}`] === "function");
  if (!contentStore || !clientViews || !reducer || REQUIRED_HARNESS_WASM_EXPORTS.some(name => typeof exports[name] !== "function")) {
    throw new Error("harness WASM does not provide the required validators");
  }
}

/** Initializes the one WASM instance shared by the reducer and wire transports. */
export async function ensureHarnessWasm(): Promise<void> {
  if (initialization !== undefined) return initialization;
  let moduleLoaded = false;
  const attempt = (async () => {
    const url = new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url);
    const source = await (url.protocol === "file:"
      ? (await import("node:fs/promises")).readFile(url).then(bytes => Uint8Array.from(bytes)) : url);
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
