import initWasm from "../generated/wasm/acyclic_harness_wasm.js";

let initialization: Promise<Record<string, unknown>> | undefined;

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

/** Host composition requires only retained Rust agent-host exports. */
export function assertBrowserWasmExports(value: unknown): void {
  if (value === null || typeof value !== "object") {
    throw new Error("harness WASM does not provide the required browser host exports");
  }
  const exports = value as Record<string, unknown>;
  const authority = hasClass(exports, "WasmAgentAuthority", "wasmagentauthority", ["issueScope", "free"], true);
  const registry = hasClass(exports, "WasmAgentRegistry", "wasmagentregistry", ["registerTool", "configureModel", "bindMachinesWorkspace", "free"], true);
  const agents = hasClass(exports, "WasmAgents", "wasmagents", [
    "create", "send", "cancel", "archive", "pause", "list", "read", "readFile", "free",
  ]) && hasStaticMethods(exports, "WasmAgents", "wasmagents", ["open", "openHosting"]);
  const guest = hasClass(exports, "AgentGuest", "agentguest", ["manifest", "onRecord", "free"])
    && hasStaticMethods(exports, "AgentGuest", "agentguest", ["open", "abiVersion"]);
  if (!authority || !registry || !agents || !guest) {
    throw new Error("harness WASM does not provide the required browser host exports");
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
  const explorer = hasClass(exports, "WasmExplorer", "wasmexplorer", [
    "frame", "history", "health", "setViewport", "scroll", "hover", "focus", "expand", "collapse",
    "open", "close", "seek", "jumpEnd", "networkChanged", "snapshot", "restore", "free",
  ], true);
  if (typeof exports.discoverAgentControls !== "function" || typeof exports.decodeClientJson !== "function" || typeof exports.encodeCanonicalJson !== "function" || !stream || !session || !agents || !commands || !controls || !invocation || !explorer) {
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

/** Configuration admission does not require a server, provider or transport. */
export async function ensureConnectionWasm(): Promise<void> {
  const exports = await loadHarnessWasm();
  if (typeof exports.resolveClientConnection !== "function"
    || !hasClass(exports, "WasmConnectionOptions", "wasmconnectionoptions", ["endpoint", "credential", "free"])) {
    throw new Error("harness WASM does not provide the required connection configuration exports");
  }
}

/** Defaults are data and need neither a server nor provider admission. */
export async function ensurePresetWasm(): Promise<void> {
  const exports = await loadHarnessWasm();
  if (typeof exports.graphcoderPreset !== "function") {
    throw new Error("harness WASM does not provide the required preset export");
  }
}

/** Browser authority and agent hosts share the one physical WASM instance. */
export async function ensureBrowserWasm(): Promise<void> {
  assertBrowserWasmExports(await loadHarnessWasm());
}

/** Provider catalogs initialize without an obsolete workflow admission contract. */
export async function ensureModelWasm(): Promise<void> {
  const exports = await loadHarnessWasm();
  if (typeof exports.WasmProviderRegistry !== "function" && typeof exports.wasmproviderregistry_new !== "function") {
    throw new Error("harness WASM does not provide the required model provider exports");
  }
}

/** Declarative content contracts need neither a provider registry nor a runtime. */
export async function ensureModelContentWasm(): Promise<void> {
  const exports = await loadHarnessWasm();
  if (!["modelContentInventory", "validateModelContent", "validateModelMessages",
    "validateUserInput", "prepareModelRequest", "admitModelEvent"]
    .every(name => typeof exports[name] === "function")) {
    throw new Error("harness WASM does not provide the required model content exports");
  }
}

/** MCP uses its Rust contracts and transport on the same physical module. */
export async function ensureMcpWasm(): Promise<void> {
  const exports = await loadHarnessWasm();
  const functions = ["validateMcpCatalog", "mcpModelDefinitions", "searchMcpCatalog",
    "validateMcpStdioRequest", "validateMcpCallbackRequest", "mcpCallbackOperation",
    "jsonToolProjectionSchema"];
  const transport = hasClass(exports, "WasmMcpHttpTransport", "wasmmcphttptransport", [
    "initializationRequest", "acceptInitialization", "listToolsJson", "serviceJson", "callJson", "reconcileJson", "free",
  ], true);
  const initialization = hasClass(exports, "WasmMcpHttpInitialization", "wasmmcphttpinitialization", [
    "resultJson", "initializedRequest", "intoTransport", "free",
  ]);
  if (!functions.every(name => typeof exports[name] === "function") || !transport || !initialization) {
    throw new Error("harness WASM does not provide the required MCP exports");
  }
}

/** Owner-mediated context reads do not require a retired workflow runtime. */
export async function ensureContextWasm(): Promise<void> {
  const exports = await loadHarnessWasm();
  if (!["captureDiscoveredContext", "contextForRequest", "readPinnedContextPath",
    "projectDiscoveredContext", "applyContextProjection", "parseSkillMetadata", "fileDescriptor"]
    .every(name => typeof exports[name] === "function")) {
    throw new Error("harness WASM does not provide the required context discovery exports");
  }
}

