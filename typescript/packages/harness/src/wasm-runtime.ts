import initWasm from "../generated/wasm/acyclic_harness_wasm.js";

let initialization: Promise<Record<string, unknown>> | undefined;

/** Every JS function used by either Harness initialization path. */
const REQUIRED_HARNESS_WASM_EXPORTS = [
  "validateContract", "validateWorkflowAdmission", "verifyFileBytes", "decodeAttachmentManifest",
  "encodeAttachmentManifest", "forkSeedFromReport", "validateToolValue",
  "validateToolDefinition", "validateToolInvocation", "validateToolResult", "validateToolProjection", "jsonToolProjectionSchema", "modelContentInventory",
  "validateMcpCatalog", "searchMcpCatalog", "mcpModelDefinitions", "validateMcpStdioRequest",
  "validateMcpCallbackRequest", "mcpCallbackOperation",
  "validateModelContent", "prepareModelRequest", "encodeModelPrefix", "validateModelMessages", "validateUserInput", "admitModelEvent", "selectModelContext",
  "defaultCompactionPolicy", "validateThresholdCompaction", "validateModelTokenCount", "decodeExecutionEventJson",
  "validateModelContextSelection", "validateSelectedModelContext",
  "validateContextSelection", "applyContextProjection", "parseSkillMetadata", "projectDiscoveredContext",
  "captureDiscoveredContext", "contextForRequest", "readPinnedContextPath", "instructionFrameBytes", "frameInstructions",
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

const HYPOTHESIS_METHODS = ["advance", "begin", "discard", "observe", "release", "residency", "status", "view", "free"] as const;

function hasClass(exports: Record<string, unknown>, name: string, prefix: string, methods: readonly string[], constructed = false): boolean {
  return typeof exports[name] === "function"
    ? methods.every(method => typeof (exports[name] as { prototype: Record<string, unknown> }).prototype[method] === "function")
    : (!constructed || typeof exports[`${prefix}_new`] === "function")
      && methods.every(method => typeof exports[method === "free" ? `__wbg_${prefix}_free` : `${prefix}_${method}`] === "function");
}

function hasStaticMethods(exports: Record<string, unknown>, name: string, prefix: string, methods: readonly string[]): boolean {
  return typeof exports[name] === "function"
    ? methods.every(method => typeof (exports[name] as unknown as Record<string, unknown>)[method] === "function")
    : methods.every(method => typeof exports[`${prefix}_${method}`] === "function");
}

/** Reject a stale binding before runtime code can call a missing validator. */
export function assertHarnessWasmExports(value: unknown): void {
  assertClientWasmExports(value);
  if (value === null || typeof value !== "object") {
    throw new Error("harness WASM does not provide the required validators");
  }
  const exports = value as Record<string, unknown>;
  const contentStore = typeof exports.WasmContentStore === "function"
    ? true
    : REQUIRED_WASM_CONTENT_EXPORTS.every(name => typeof exports[name] === "function");
  const hypotheses = hasClass(exports, "WasmHypothesisClient", "wasmhypothesisclient", HYPOTHESIS_METHODS, true);
  const reducer = hasClass(exports, "WasmReducer", "wasmreducer", ["prepareInheritedModelRequest", "prepareForkRequest"], true);
  const providers = typeof exports.WasmProviderRegistry === "function"
    || typeof exports.wasmproviderregistry_new === "function";
  if (!contentStore || !hypotheses || !reducer || !providers || REQUIRED_HARNESS_WASM_EXPORTS.some(name => typeof exports[name] !== "function")) {
    throw new Error("harness WASM does not provide the required validators");
  }
}

/** Public frontend admission depends only on the shipping client exports. */
export function assertClientWasmExports(value: unknown): void {
  if (value === null || typeof value !== "object") {
    throw new Error("harness WASM does not provide the required client exports");
  }
  const exports = value as Record<string, unknown>;
  const stream = hasClass(exports, "WasmStream", "wasmstream", ["registerCredential", "free"], true);
  const session = hasClass(exports, "WasmWireSession", "wasmwiresession", [
    "setView", "registerCredential", "openStream", "next", "close", "followedAgents",
    "viewRevision", "firstSequence", "recordCount", "record", "free",
  ], true);
  const agents = hasClass(exports, "WasmAgentClient", "wasmagentclient", [
    "setView", "selectedAgents", "agentWindow", "agentRecord", "agentStatus",
    "openResource", "releaseResource", "resourceWindow", "resourceRecord",
    "next", "close", "viewRevision", "firstSequence", "recordCount", "record", "currentStatus", "controls", "free",
  ], true);
  const commands = hasClass(exports, "WasmAgentCommand", "wasmagentcommand", [
    "bytes", "value", "reply", "free",
  ], true) && hasStaticMethods(exports, "WasmAgentCommand", "wasmagentcommand", [
    "maximumCommandBytes", "maximumInvocationBytes", "maximumReplyBytes",
  ]);
  const controls = hasClass(exports, "WasmAgentControls", "wasmagentcontrols", ["prepare", "observe", "free"], true);
  const invocation = hasClass(exports, "WasmAgentInvocation", "wasmagentinvocation", ["bytes", "execute", "free"]);
  const pages = hasClass(exports, "WasmPageWindow", "wasmpagewindow", [
    "reset", "begin", "finish", "items", "hasMore", "retainedBytes", "pageCount", "revision", "free",
  ], true);
  const pageRequest = hasClass(exports, "WasmPageRequest", "wasmpagerequest", ["cursor", "free"]);
  if (typeof exports.decodeClientJson !== "function" || !stream || !session || !agents || !commands || !controls || !invocation || !pages || !pageRequest) {
    throw new Error("harness WASM does not provide the required client exports");
  }
}

/** One physical WASM initialization, shared by client and host facades. */
async function loadHarnessWasm(): Promise<Record<string, unknown>> {
  if (initialization !== undefined) return initialization;
  let moduleLoaded = false;
  const attempt = (async () => {
    const url = new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url);
    const source = await (url.protocol === "file:"
      ? (await import("node:fs/promises")).readFile(url).then(bytes => Uint8Array.from(bytes)) : url);
    const exports = await initWasm({ module_or_path: source });
    moduleLoaded = true;
    return exports as unknown as Record<string, unknown>;
  })();
  initialization = attempt;
  try {
    return await attempt;
  } catch (error) {
    if (!moduleLoaded && initialization === attempt) initialization = undefined;
    throw error;
  }
}

/** Initialize only the public agent/session frontend, without legacy tasks. */
export async function ensureClientWasm(): Promise<void> {
  assertClientWasmExports(await loadHarnessWasm());
}

/** Initialize the reducer/host surface on the same physical WASM instance. */
export async function ensureHarnessWasm(): Promise<void> {
  assertHarnessWasmExports(await loadHarnessWasm());
}
