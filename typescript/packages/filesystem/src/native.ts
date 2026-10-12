import { copyBytes, copyOptionalBytes, requireIdentity } from "./binding-values.js";
import { createRequire } from "node:module";
import { arch, platform } from "node:process";
import type { WasmRawFs, WasmRawWorkspace, EngineCapabilities, FsGeneration, FsWorkspace, NativeBindings, NativeFsEngine, NativeFsWorkspace, NativeFsOptions, NativeRawFs, NativeRawCheckout, NativeRawTransactionOperation, NativeRawMergeConflict, NativeRawSourceResult, NativeRawWorkspace, NativeRawWorkspaceMount, NativeRawWatcher, NativeRawWatchBatch, NativeWatcher, NativeWatchBatch, NativeNamespacePath, CaptureResult, NativeWorkspaceMount, SourceResult, SourceStatus, WorkCounters, WorkspaceRebaseResult, JoinResult, TransactionOperation, GenerationExportManifest, ResolvableFsJoinPlan, NativeRawWorkspaceGraph, NativeRawWorkspaceLineageRecord, WasmRawJoinResult } from "./contracts.js";
import type {
  WorkspaceGraph,
  WorkspaceLineageRecord,
} from "./compat.js";

import { createFilesystemAdapter } from "./filesystem-adapter.js";
import { workCounterKeys } from "./workspace-copies.js";
import { adaptResolvableJoinPlan } from "./workspace-operations.js";

import { decodeMergeConflict as decodeSharedMergeConflict, parseJoinResult as parseSharedJoinResult, parseWorkspaceRebaseResult as parseSharedWorkspaceRebaseResult } from "./workspace-results.js";

type NativeAdapterScope = ReturnType<typeof createNativeAdapter>;
const workspaceScopes = new WeakMap<FsWorkspace, NativeAdapterScope>();
const nativeHandles = new WeakMap<object, object>();

const decodeMergeConflict = (raw: unknown) => decodeSharedMergeConflict(
  (raw as NativeRawMergeConflict),
  "native merge",
);

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS, portableVolumeOptions } from "./contracts.js";
export { CrossVolumeError, MountedView } from "./mounted.js";
export type { MountedCheckout, MountedSnapshot } from "./mounted.js";

const PACKAGE_VERSION = "0.2.0";
const TARGETS = new Set([
  "win32-x64",
  "win32-arm64",
  "linux-x64",
  "linux-arm64",
  "darwin-x64",
  "darwin-arm64",
]);

let bindingPromise: Promise<NativeBindings> | undefined;
const requireNativeCompanion = createRequire(import.meta.url);

type NativeModuleNamespace = NativeBindings & {
  readonly default?: NativeBindings;
};

async function bindings(): Promise<NativeBindings> {
  const target = `${platform}-${arch}`;
  if (!TARGETS.has(target)) {
    throw new Error(`@acyclic-labs/fs has no native companion for ${target}`);
  }
  bindingPromise ??= Promise.resolve().then((): NativeBindings => {
    // N-API companions are Node native modules. `import()` asks the ESM loader
    // to interpret the `.node` file and fails in both Node and Bun, while
    // createRequire resolves the optional companion from this package's
    // installed node_modules directory and delegates loading to Node-API.
    const namespace = requireNativeCompanion(`@acyclic-labs/fs-${target}`) as NativeModuleNamespace;
    const candidate =
      typeof namespace.nativeCapabilities === "function" ? namespace : namespace.default;
    if (candidate === undefined) {
      throw new Error("native companion did not export the generated N-API binding");
    }
    const capabilities = candidate.nativeCapabilities();
    if (capabilities.version !== PACKAGE_VERSION) {
      throw new Error("native companion version does not match @acyclic-labs/fs");
    }
    if (
      nativePlatform(capabilities.platform) !== platform ||
      nativeArchitecture(capabilities.architecture) !== arch
    ) {
      throw new Error("native companion target does not match the current Node.js process");
    }
    return candidate;
  }).catch((error: unknown) => {
    bindingPromise = undefined;
    throw error;
  });
  return bindingPromise;
}

export async function openNativeFs(options: NativeFsOptions): Promise<NativeFsEngine> {
  if (options.root.length === 0) {
    throw new RangeError("native filesystem root must be non-empty");
  }
  const binding = await bindings();
  return adaptFs(
    await binding.NativeFs.open(options.root, options.objectCache),
  );
}

function copyWorkspaceLineageRecord(
  record: NativeRawWorkspaceLineageRecord,
): WorkspaceLineageRecord {
  return {
    version: record.version,
    revision: record.revision,
    workspaceId: copyBytes(record.workspaceId),
    workspaceName: record.workspaceName,
    parentWorkspaceId: copyOptionalBytes(record.parentWorkspaceId),
    parentWorkspaceName: record.parentWorkspaceName,
    forkGeneration: copyBytes(record.forkGeneration),
    initialGeneration: copyBytes(record.initialGeneration),
  };
}

/** Opens durable recursive-workspace lineage over live native workspace handles. */
export async function openNativeWorkspaceGraph(stateRoot: string): Promise<WorkspaceGraph> {
  if (stateRoot.length === 0) throw new RangeError("workspace graph state root must be non-empty");
  const binding = await bindings();
  return adaptWorkspaceGraph(binding.NativeWorkspaceGraph.open(stateRoot));
}

function adaptWorkspaceGraph(raw: NativeRawWorkspaceGraph): WorkspaceGraph {
  return {
    async registerRoot(workspace) {
      return copyWorkspaceLineageRecord(await raw.registerRoot(rawWorkspace(workspace)));
    },
    async fork(parent, destination, idempotencyKey) {
      requireWorkspaceName(destination);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      const scope = workspaceScopes.get(parent);
      if (scope === undefined) throw new TypeError("workspace belongs to another filesystem engine");
      return scope.adaptWorkspace((normalizeNative(
        await raw.fork(rawWorkspace(parent), destination, idempotencyKey),
      ) as WasmRawWorkspace));
    },
    async authorizeJoin(childWorkspaceId, parentWorkspaceId) {
      requireIdentity(childWorkspaceId, "child workspace identity");
      requireIdentity(parentWorkspaceId, "parent workspace identity");
      return copyWorkspaceLineageRecord(
        await raw.authorizeJoin(childWorkspaceId, parentWorkspaceId),
      );
    },
    async ancestors(workspaceId, maximum) {
      requireIdentity(workspaceId, "workspace identity");
      return (await raw.ancestors(workspaceId, maximum)).map(copyWorkspaceLineageRecord);
    },
  };
}

function adaptFs(raw: NativeRawFs): NativeFsEngine {
  const targetMount = nativeMount(raw.capabilities.platform, raw.capabilities.nativeMount);
  const scope = createNativeAdapter();
  const capabilities: EngineCapabilities = {
    version: raw.capabilities.version,
    platform: raw.capabilities.platform,
    architecture: nativeArchitecture(raw.capabilities.architecture),
    authority: "local",
    immutableObjects: "local",
    nativeMount: targetMount,
    writableNativeMount: raw.capabilities.writableMount,
    nativeWatch: raw.capabilities.nativeWatch,
    nativeWatchBackend: nativeWatchBackend(raw.capabilities.nativeWatchBackend),
    nativeWatchPersistentRestart: raw.capabilities.nativeWatchPersistentRestart,
    nativeWatchRootIdentityFencing: raw.capabilities.nativeWatchRootIdentityFencing,
    providerProcessIoObservable: raw.capabilities.providerProcessIoObservable,
  };
  const engine: NativeFsEngine = {
    ...(scope.adaptFs((normalizeNative(raw) as WasmRawFs), undefined, capabilities) as NativeFsEngine),
    async attachDirectory(
      name: string,
      path: string,
      options: import("./contracts.js").NativeSourceOptions,
    ): Promise<NativeFsWorkspace> {
      requireWorkspaceName(name);
      if (path.length === 0) throw new RangeError("source path must be non-empty");
      return scope.adaptWorkspace((normalizeNative(await raw.attachDirectory(name, path, options)) as WasmRawWorkspace));
    },
    get cancelled(): boolean {
      return raw.cancelled;
    },
    cancel(): void {
      raw.cancel();
    },
  };
  return engine;
}

function captureResult(value: { readonly examinedPaths: bigint; readonly changedPaths: bigint; readonly stagedFileBytes: bigint; readonly workJson: string }): CaptureResult {
  return { examinedPaths: value.examinedPaths, changedPaths: value.changedPaths, stagedFileBytes: value.stagedFileBytes, work: parseWork(value.workJson) };
}

function adaptWatcher(raw: NativeRawWatcher): NativeWatcher {
  return {
    async reconcile(maximumPaths, maximumExtentSpans) {
      const value = await raw.reconcile(maximumPaths, maximumExtentSpans);
      return { epoch: value.epoch, baseline: captureResult(value.baseline), postBaseline: nativeWatchBatch(value.postBaseline) };
    },
    async pollCapture(maximumChanges, maximumPaths, maximumExtentSpans) {
      const value = await raw.pollCapture(maximumChanges, maximumPaths, maximumExtentSpans);
      return { epoch: value.epoch, firstSequence: value.firstSequence, nextSequence: value.nextSequence, examinedPaths: value.examinedPaths, changedPaths: value.changedPaths, stagedFileBytes: value.stagedFileBytes, work: parseWork(value.workJson) };
    },
  };
}

function nativeWatchBatch(value: NativeRawWatchBatch): NativeWatchBatch {
  const work = parseWork(value.workJson);
  if (value.status === "changes" && value.firstSequence !== undefined && value.nextSequence !== undefined && value.reason === undefined) {
    return { status: "changes", epoch: value.epoch, firstSequence: value.firstSequence, nextSequence: value.nextSequence, changes: value.changes.map(change => {
      if ((change.kind === "created" || change.kind === "modified" || change.kind === "arrived" || change.kind === "metadata" || change.kind === "removed") && change.path !== undefined && change.from === undefined && change.to === undefined) return { kind: change.kind, path: copyNamespacePath((change.path as NativeNamespacePath)) };
      if (change.kind === "renamed" && change.path === undefined && change.from !== undefined && change.to !== undefined) return { kind: "renamed", from: copyNamespacePath((change.from as NativeNamespacePath)), to: copyNamespacePath((change.to as NativeNamespacePath)) };
      throw new TypeError("native binding returned a malformed watch change");
    }), work };
  }
  if (value.status === "rescan-required" && value.firstSequence === undefined && value.nextSequence === undefined && value.changes.length === 0 && isRescanReason(value.reason)) {
    return { status: "rescan-required", epoch: value.epoch, reason: value.reason, work };
  }
  throw new TypeError("native binding returned a malformed watch batch");
}

function copyNamespacePath(path: NativeNamespacePath): NativeNamespacePath {
  return { components: path.components.map(component => ({ encoding: component.encoding, bytes: copyBytes(component.bytes) })) };
}

function isRescanReason(value: string | undefined): value is Extract<NativeWatchBatch, { readonly status: "rescan-required" }>["reason"] {
  return value === "initial-snapshot-required" || value === "queue-overflow" || value === "native-rescan-required" || value === "backend-error" || value === "unrepresentable-path" || value === "ambiguous-rename" || value === "root-changed";
}

function normalizeNative(value: unknown): unknown {
  if (value == null) return undefined;
  if (value instanceof Uint8Array || typeof value !== "object") return value;
  if (value instanceof Promise) return value.then(normalizeNative);
  if (Array.isArray(value)) return value.map(normalizeNative);
  if (Object.getPrototypeOf(value) === Object.prototype &&
      !Object.values(value).some(field => typeof field === "function")) {
    if (Reflect.get(value, "kind") === "sparse" && !Array.isArray(Reflect.get(value, "spans"))) {
      throw new TypeError("native binding returned a malformed file extent plan");
    }
    return Object.fromEntries(Object.entries(value).map(([key, field]) =>
      key === "workJson" ? ["work", parseWork(String(field))] : [key, normalizeNative(field)],
    ));
  }
  const proxy = new Proxy(value, {
    get(target, key) {
      if (key === "work") return parseWork(Reflect.get(target, "workJson", target));
      if (key === "acquisitionWork") return parseWork(Reflect.get(target, "acquisitionWorkJson", target));
      if (key === "metrics") return async () => {
        const result: unknown = JSON.parse(await Reflect.get(target, "metricsJson", target).call(target));
        if (typeof result !== "object" || result === null) throw new TypeError("native speculation metrics are malformed");
        return result;
      };
      const method: unknown = Reflect.get(target, key, target);
      if (key === "close" && method === undefined) return () => {};
      if (typeof method !== "function") return normalizeNative(method);
      return (...args: unknown[]) => {
        if (key === "applyTransaction" || key === "mutateLive") {
          args[0] = (args[0] as readonly TransactionOperation[]).map(nativeTransactionOperation);
        } else if (key === "exportGenerationBatch" || key === "importGenerationBatch" || key === "restoreVolume") {
          args[0] = nativeManifest(args[0] as GenerationExportManifest);
        } else if (key === "planPromotion") {
          const request = args[0] as import("./contracts.js").PromotionRequest;
          args = [request.operationId, request.acceptedTiers, request.residency, request.destinations];
        }
        return normalizeNative(method.apply(target, args.map(arg =>
          typeof arg === "object" && arg !== null ? nativeHandles.get(arg) ?? arg : arg,
        )));
      };
    },
  });
  nativeHandles.set(proxy, value);
  return proxy;
}

function createNativeAdapter() {
  const adapter = createFilesystemAdapter({
    checkout(handle) {
      const raw = (nativeHandles.get(handle) as NativeRawCheckout);
      return {
        mount(destination, writable) {
          const value = raw.mount(destination, writable);
          return { get id() { return copyBytes(value.id); }, destination: value.destination, revalidate() { value.revalidate(); }, stop() { return value.stop(); } };
        },
        async materialize(options) {
          const value = await raw.materialize(options);
          return { files: value.files, directories: value.directories, symbolicLinks: value.symbolicLinks, specialFiles: value.specialFiles, logicalFileBytes: value.logicalFileBytes, writtenBytes: value.writtenBytes, work: parseWork(value.workJson) };
        },
        async capture(sourceRoot, paths, maximumPaths, maximumExtentSpans) {
          return captureResult(await raw.capture(sourceRoot, paths, maximumPaths, maximumExtentSpans));
        },
        async captureBaseline(sourceRoot, maximumPaths, maximumExtentSpans) {
          return captureResult(await raw.captureBaseline(sourceRoot, maximumPaths, maximumExtentSpans));
        },
        watch(sourceRoot, maximumQueuedChanges, recursive) {
          return adaptWatcher(raw.watch(sourceRoot, maximumQueuedChanges, recursive));
        },
        cancel() { raw.cancel(); },
      };
    },
    workspace(handle, workspace) {
      const raw = (nativeHandles.get(handle) as NativeRawWorkspace);
      workspaceScopes.set(workspace, scope);
      return {
        async sourceState(): Promise<SourceResult> {
          return parseSourceResult(await raw.sourceState());
        },
        async reconcileSource(): Promise<SourceResult> {
          return parseSourceResult(await raw.reconcileSource());
        },
        async rescanSource(): Promise<SourceResult> {
          return parseSourceResult(await raw.rescanSource());
        },
        async seal(): Promise<FsGeneration> {
          return scope.adaptGeneration((await raw.seal() as Parameters<typeof scope.adaptGeneration>[0]));
        },
        async mount(destination, options): Promise<NativeWorkspaceMount> {
          if (destination.length === 0) throw new RangeError("mount destination must be non-empty");
          if (options.subdirectory.length === 0) {
            throw new RangeError("mount subdirectory must be non-empty");
          }
          return adaptWorkspaceMount(await raw.mount(destination, options));
        },
      };
    },
    joinPlan(handle) {
      return adaptJoinPlan((handle as import("./contracts.js").NativeRawJoinPlan));
    },
  });
  const scope = (adapter as Omit<typeof adapter, "adaptWorkspace"> & {
    adaptWorkspace(raw: import("./contracts.js").WasmRawWorkspace): NativeFsWorkspace;
  });
  return scope;
}

function nativeManifest(value: GenerationExportManifest) {
  return { manifestBytes: value.manifestBytes, objects: value.objects, workJson: JSON.stringify(value.work, (_key, field: unknown) => typeof field === "bigint" ? field.toString() : field) };
}

function nativeTransactionOperation(value: TransactionOperation): NativeRawTransactionOperation {
  const operation: NativeRawTransactionOperation = {
    kind: value.kind,
    ...("path" in value ? { path: value.path } : {}),
    ...("source" in value ? { source: value.source } : {}),
    ...("destination" in value ? { destination: value.destination } : {}),
    ...("bytes" in value ? { bytes: copyBytes(value.bytes) } : {}),
    ...("target" in value ? { target: copyBytes(value.target) } : {}),
    ...("payload" in value ? { payload: copyBytes(value.payload) } : {}),
    ...("expectedFileId" in value && value.expectedFileId !== undefined
      ? { expectedFileId: copyBytes(value.expectedFileId) } : {}),
    ...("fileKind" in value ? { fileKind: value.fileKind } : {}),
    ...("offset" in value ? { offset: value.offset } : {}),
    ...("sourceOffset" in value ? { sourceOffset: value.sourceOffset } : {}),
    ...("destinationOffset" in value ? { destinationOffset: value.destinationOffset } : {}),
    ...("length" in value ? { length: value.length } : {}),
    ...("logicalBytes" in value ? { logicalBytes: value.logicalBytes } : {}),
    ...("major" in value ? { major: value.major } : {}),
    ...("minor" in value ? { minor: value.minor } : {}),
    ...("replace" in value ? { replace: value.replace } : {}),
    ...("allocated" in value ? { allocated: value.allocated } : {}),
    ...("extend" in value ? { extend: value.extend } : {}),
    ...("keepSize" in value ? { keepSize: value.keepSize } : {}),
    ...("canonicalBytes" in value ? { canonicalBytes: copyBytes(value.canonicalBytes) } : {}),
  };
  return operation;
}

function rawWorkspace(workspace: FsWorkspace): NativeRawWorkspace {
  const scope = workspaceScopes.get(workspace);
  if (scope === undefined) throw new TypeError("workspace belongs to another filesystem engine");
  return nativeHandles.get(scope.rawWorkspace(workspace)) as NativeRawWorkspace;
}

const parseJoinResult = (value: WasmRawJoinResult): JoinResult => parseSharedJoinResult(
  value,
  decodeMergeConflict,
);
const parseWorkspaceRebaseResult = (value: WasmRawJoinResult): WorkspaceRebaseResult => parseSharedWorkspaceRebaseResult(
  value,
  decodeMergeConflict,
);
const adaptJoinPlan = (raw: import("./contracts.js").NativeRawJoinPlan): ResolvableFsJoinPlan =>
  adaptResolvableJoinPlan((raw as Parameters<typeof adaptResolvableJoinPlan>[0]), parseJoinResult);

function parseSourceResult(value: NativeRawSourceResult): SourceResult {
  const { status } = value;
  if (
    status !== "none" &&
    status !== "clean" &&
    status !== "pending-capture" &&
    status !== "needs-rescan" &&
    status !== "conflict" &&
    status !== "sealed"
  ) {
    throw new TypeError("native source has an invalid status");
  }
  const typedStatus: SourceStatus = status;
  const reason = value.reason;
  if (
    reason !== undefined &&
    reason !== "initial-snapshot-required" &&
    reason !== "queue-overflow" &&
    reason !== "native-rescan-required" &&
    reason !== "backend-error" &&
    reason !== "unrepresentable-path" &&
    reason !== "ambiguous-rename" &&
    reason !== "root-changed"
  ) {
    throw new TypeError("native source has an invalid reason");
  }
  return {
    status: typedStatus,
    reason,
    generationId: value.generationId === undefined ? undefined : copyBytes(value.generationId),
  };
}

function adaptWorkspaceMount(raw: NativeRawWorkspaceMount): NativeWorkspaceMount {
  return {
    get path(): string {
      return raw.path;
    },
    sync() {
      return (raw.sync() as Promise<void>);
    },
    unmount() {
      return (raw.unmount() as Promise<boolean>);
    },
  };
}

function requireWorkspaceName(name: string): void {
  if (name.length === 0) throw new RangeError("workspace name must be non-empty");
}

function nativePlatform(value: string): "win32" | "linux" | "darwin" {
  switch (value) {
    case "windows":
      return "win32";
    case "linux":
      return "linux";
    case "macos":
      return "darwin";
    default:
      throw new Error(`native companion reported unsupported platform ${value}`);
  }
}

function nativeArchitecture(value: string): "x64" | "arm64" {
  switch (value) {
    case "x86_64":
    case "x64":
      return "x64";
    case "aarch64":
    case "arm64":
      return "arm64";
    default:
      throw new Error(`native companion reported unsupported architecture ${value}`);
  }
}

/** Rust emits exact u64 counters; reject any that JSON numbers cannot hold losslessly. */
function parseWork(value: string): WorkCounters {
  const parsed: unknown = JSON.parse(value);
  if (typeof parsed !== "object" || parsed === null) {
    throw new TypeError("native work receipt is malformed");
  }
  const source = parsed as Record<string, unknown>;
  const result = {} as Record<keyof WorkCounters, bigint>;
  for (const key of workCounterKeys) {
    const counter = source[key];
    if (typeof counter === "string" && /^(?:0|[1-9][0-9]*)$/.test(counter)) {
      result[key] = BigInt(counter);
    } else {
      throw new RangeError(`native work counter ${key} must be a canonical decimal string`);
    }
  }
  return result;
}
function nativeMount(
  targetPlatform: string,
  available: boolean,
): EngineCapabilities["nativeMount"] {
  if (!available) {
    return "none";
  }
  switch (targetPlatform) {
    case "linux":
      return "linux-fuse";
    case "macos":
      return "macos-nfs";
    case "windows":
      return "windows-projfs";
    default:
      return "none";
  }
}

function nativeWatchBackend(value: string): EngineCapabilities["nativeWatchBackend"] {
  switch (value) {
    case "linux-inotify":
    case "macos-fsevents":
    case "windows-read-directory-changes":
    case "unsupported":
      return value;
    default:
      throw new Error(`native binding returned unsupported watcher backend ${value}`);
  }
}
