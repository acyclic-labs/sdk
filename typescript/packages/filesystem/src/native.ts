import { arch, platform } from "node:process";
import type {
  EngineCapabilities,
  FsChangeSet,
  FsGeneration,
  FsTransaction,
  FsVolume,
  FsCheckout,
  FsWorkspace,
  NativeBindings,
  NativeFsEngine,
  NativeFsWorkspace,
  NativeFsOptions,
  NativeRawFs,
  NativeRawVolume,
  NativeRawCheckout,
  NativeRawSpeculation,
  NativeRawMutation,
  NativeRawTransactionOperation,
  NativeRawGenerationDiff,
  NativeRawMergeConflict,
  NativeRawSourceResult,
  NativeRawWorkspace,
  NativeRawWorkspaceMount,
  NativeRawWatcher,
  NativeRawWatchBatch,
  NativeWatcher,
  NativeWatchBatch,
  NativeNamespacePath,
  CaptureResult,
  NativeWorkspaceMount,
  SourceResult,
  SourceStatus,
  WorkCounters,
  WorkspaceRebaseResult,
  JoinResult,
  TransactionOperation,
  Speculation,
  SpeculationMetrics,
  GenerationExportManifest,
  GenerationTransferBatch,
  GenerationTransferCursor,
  ObjectCacheStats,
  ResolvableFsJoinPlan,
  ResolvedFile,
  NativeRawGitCompatRepository,
  NativeRawOperationWindowClose,
  NativeRawOperationWindowCoordinator,
  NativeRawOperationWindowLease,
  NativeRawOperationWindowPhase,
  NativeRawWorkspaceGraph,
  NativeRawWorkspaceLineageRecord,
  WasmRawJoinResult,
} from "./contracts.js";
import type {
  GenerationIdentity,
  GitCommitIdentity,
  GitCompatCommand,
  GitCompatRepository,
  CompatibilityWire,
  OperationIdentity,
  OperationWindowClose,
  OperationWindowCoordinator,
  OperationWindowLease,
  OperationWindowPhase,
  WorkspaceGraph,
  WorkspaceContextRegistry,
  WorkspaceIdentity,
  WorkspaceLineageRecord,
} from "./compat.js";
import {
  adaptCompatibilityWire,
  encodeGitCompatCommand,
  finishGitCompatOutput,
  gitCompatSafeTimestamp,
  parseGitCompatOutputJson,
  parseGitPendingTransitionJson,
  stringifyGitFilesystemResult,
} from "./compat.js";

import { adaptWorkspaceContextRegistry } from "./workspace-context.js";
import { adaptTransaction } from "./transaction-adapter.js";
import { createGenerationAdapter } from "./generation-adapter.js";
import { createChangeSetAdapter } from "./change-set-adapter.js";
import { copyBatchLookupEntries, copyDirectoryPage, copyDirectoryRecordPage, copyFileRecord,
  copyGenerationDiff, copyNamedAttributePage, copyNamedAttributeResult, copyStatResult } from "./binding-results.js";
import { bigintRecord, copyWorkspaceStat, copyWorkspaceDirectoryPage, copyWorkspaceExtentPlan, copyFileExtentPlan, copyCheckoutCommit, copyLiveMutation, copyLiveTransaction, copyTransactionResult, copyTransactionRebase, copyRebaseResult } from "./workspace-copies.js";
import { adaptResolvableJoinPlan, workspaceOperations } from "./workspace-operations.js";

import { decodeMergeConflict as decodeSharedMergeConflict, parseJoinResult as parseSharedJoinResult, parseMergePreparation, parseWorkspaceRebaseResult as parseSharedWorkspaceRebaseResult,
  validateJoinOptions, validateWorkspaceRebaseOptions } from "./workspace-results.js";

const { adaptGeneration, rawGeneration } = createGenerationAdapter(
  copyWorkspaceStat, copyWorkspaceDirectoryPage, copyWorkspaceExtentPlan,
);
const nativeGenerationDiff = (value: NativeRawGenerationDiff) => copyGenerationDiff(
  nativeBoundary<Parameters<typeof copyGenerationDiff>[0]>(value),
  parseWork(value.workJson),
);
const workspaceHandles = new WeakMap<FsWorkspace, NativeRawWorkspace>();
const { adaptChangeSet } = createChangeSetAdapter(adaptGeneration, nativeGenerationDiff);
type NativeAdapterScope = {
  readonly adaptGeneration: typeof adaptGeneration;
  readonly rawGeneration: typeof rawGeneration;
  readonly adaptChangeSet: typeof adaptChangeSet;
  readonly workspaceHandles: WeakMap<FsWorkspace, NativeRawWorkspace>;
};

const nativeScope: NativeAdapterScope = {
  adaptGeneration,
  rawGeneration,
  adaptChangeSet,
  workspaceHandles,
};
const workspaceScopes = new WeakMap<FsWorkspace, NativeAdapterScope>();
type NativeFsHandle = { readonly raw: NativeRawFs; readonly scope: NativeAdapterScope };
const fsHandles = new WeakMap<NativeFsEngine, NativeFsHandle>();
const decodeMergeConflict = (raw: unknown) => decodeSharedMergeConflict(
  nativeBoundary<NativeRawMergeConflict>(raw),
  "native merge",
);

/**
 * Generated N-API declarations use Buffer while the provider adapters expose
 * the SDK's Uint8Array surface. Nullable fields are normalized at their use sites. The
 * runtime binding is the authority; this helper marks the one-way adapter
 * boundary without introducing an untyped any escape hatch.
 */
function nativeBoundary<T>(value: unknown): T {
  return value as T;
}

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS, portableVolumeOptions } from "./contracts.js";
export { CrossVolumeError, MountedView } from "./mounted.js";
export type { MountedCheckout, MountedSnapshot } from "./mounted.js";

const PACKAGE_VERSION = "0.1.5";
const TARGETS = new Set([
  "win32-x64",
  "win32-arm64",
  "linux-x64",
  "linux-arm64",
  "darwin-x64",
  "darwin-arm64",
]);

let bindingPromise: Promise<NativeBindings> | undefined;

type NativeModuleNamespace = NativeBindings & {
  readonly default?: NativeBindings;
};

async function bindings(): Promise<NativeBindings> {
  const target = `${platform}-${arch}`;
  if (!TARGETS.has(target)) {
    throw new Error(`@acyclic-labs/fs has no native companion for ${target}`);
  }
  bindingPromise ??= import(`@acyclic-labs/fs-${target}`).then((module): NativeBindings => {
    const namespace = module as NativeModuleNamespace;
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
  requirePositiveInteger(options.objectCache.maximumEntries, "maximum cache entries");
  requirePositiveInteger(options.objectCache.maximumBytes, "maximum cache bytes");
  requirePositiveInteger(options.objectCache.maximumInFlight, "maximum cache in-flight reads");
  requirePositiveInteger(
    options.objectCache.maximumWaitersPerObject,
    "maximum cache waiters per object",
  );
  const binding = await bindings();
  return adaptFs(
    await binding.NativeFs.open(options.root, {
      maximumEntries: options.objectCache.maximumEntries,
      maximumBytes: BigInt(options.objectCache.maximumBytes),
      maximumInFlight: options.objectCache.maximumInFlight,
      maximumWaitersPerObject: options.objectCache.maximumWaitersPerObject,
    }),
  );
}

/** Opens the durable Git-shaped compatibility state machine without invoking system Git. */
export async function openNativeGitCompatRepository(
  stateRoot: string,
  workspaceId: WorkspaceIdentity,
): Promise<GitCompatRepository> {
  requireStateRoot(stateRoot, "Git compatibility");
  const binding = await bindings();
  return adaptGitCompat(binding.NativeGitCompatRepository.open(stateRoot, workspaceId));
}

/** Opens the canonical Rust merge/publication wire codec. */
export async function openNativeCompatibilityWire(): Promise<CompatibilityWire> {
  return adaptCompatibilityWire(await bindings());
}

/** Opens the durable agent-neutral multi-root context registry. */
export async function openNativeWorkspaceContextRegistry(
  stateRoot: string,
): Promise<WorkspaceContextRegistry> {
  requireStateRoot(stateRoot, "workspace context");
  const binding = await bindings();
  return adaptWorkspaceContextRegistry(
    binding.NativeWorkspaceContextRegistry.open(stateRoot),
  );
}

function requireStateRoot(stateRoot: string, feature: string): void {
  if (stateRoot.length === 0) {
    throw new RangeError(`${feature} state root must be non-empty`);
  }
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

function copyOperationWindowLease(
  lease: NativeRawOperationWindowLease,
): OperationWindowLease {
  return {
    workspaceId: copyBytes(lease.workspaceId),
    leaseId: copyBytes(lease.leaseId),
    pinnedParent: copyBytes(lease.pinnedParent),
    expiresAtMillis: lease.expiresAtMillis,
  };
}

function nativeOperationWindowLease(
  lease: OperationWindowLease,
): NativeRawOperationWindowLease {
  requireIdentity(lease.workspaceId, "workspace identity");
  requireIdentity(lease.leaseId, "lease identity");
  requireGenerationIdentity(lease.pinnedParent, "pinned parent");
  return lease;
}

function parseOperationWindowPhase(
  phase: NativeRawOperationWindowPhase,
): OperationWindowPhase {
  if (phase.kind === "idle") return { kind: "idle" };
  if (phase.kind === "active" && phase.pinnedParent !== undefined) {
    return {
      kind: "active",
      pinnedParent: copyBytes(phase.pinnedParent),
      pendingParent: copyOptionalBytes(phase.pendingParent),
      activeLeaseCount: phase.activeLeaseCount ?? 0,
    };
  }
  if (
    phase.kind === "reconciling" && phase.ticket !== undefined &&
    phase.pinnedParent !== undefined
  ) {
    return {
      kind: "reconciling",
      ticket: copyBytes(phase.ticket),
      pinnedParent: copyBytes(phase.pinnedParent),
      pendingParent: copyOptionalBytes(phase.pendingParent),
    };
  }
  throw new TypeError("native operation window returned a malformed phase");
}

function parseOperationWindowClose(
  close: NativeRawOperationWindowClose,
): OperationWindowClose {
  if (close.kind === "still-active" && close.remaining !== undefined) {
    return { kind: "still-active", remaining: close.remaining };
  }
  if (close.kind === "already-closed") return { kind: "already-closed" };
  if (
    close.kind === "reconcile" && close.ticket !== undefined &&
    close.pinnedParent !== undefined
  ) {
    return {
      kind: "reconcile",
      ticket: copyBytes(close.ticket),
      pinnedParent: copyBytes(close.pinnedParent),
      pendingParent: copyOptionalBytes(close.pendingParent),
    };
  }
  throw new TypeError("native operation window returned a malformed close result");
}

/** Opens durable recursive-workspace lineage over live native workspace handles. */
export async function openNativeWorkspaceGraph(stateRoot: string): Promise<WorkspaceGraph> {
  requireStateRoot(stateRoot, "workspace graph");
  const binding = await bindings();
  return adaptWorkspaceGraph(binding.NativeWorkspaceGraph.open(stateRoot));
}

/** Opens the durable overlapping-tool lease coordinator. */
export async function openNativeOperationWindowCoordinator(
  filesystem: NativeFsEngine,
): Promise<OperationWindowCoordinator> {
  const handle = fsHandles.get(filesystem);
  if (handle === undefined) {
    throw new TypeError("operation windows require a native filesystem opened by this module");
  }
  return adaptOperationWindowCoordinator(handle.raw.operationWindows(), handle.scope);
}

function adaptWorkspaceGraph(raw: NativeRawWorkspaceGraph): WorkspaceGraph {
  return {
    async registerRoot(workspace) {
      return copyWorkspaceLineageRecord(await raw.registerRoot(rawWorkspace(workspace)));
    },
    async fork(parent, destination, idempotencyKey) {
      requireWorkspaceName(destination);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      const scope = workspaceScopes.get(parent) ?? nativeScope;
      return adaptWorkspace(
        await raw.fork(rawWorkspace(parent, scope), destination, idempotencyKey),
        scope,
      );
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
      requirePositiveInteger(maximum, "maximum ancestors");
      return (await raw.ancestors(workspaceId, maximum)).map(copyWorkspaceLineageRecord);
    },
  };
}

function adaptOperationWindowCoordinator(
  raw: NativeRawOperationWindowCoordinator,
  scope: NativeAdapterScope = nativeScope,
): OperationWindowCoordinator {
  return {
    async begin(workspaceId, parent, owner, nowMillis, expiresAtMillis) {
      requireIdentity(workspaceId, "workspace identity");
      requireGenerationIdentity(parent, "parent");
      if (owner.length === 0) throw new RangeError("operation owner must be non-empty");
      return copyOperationWindowLease(
        await raw.begin(workspaceId, parent, owner, nowMillis, expiresAtMillis),
      );
    },
    async observeParent(workspaceId, parent) {
      requireIdentity(workspaceId, "workspace identity");
      requireGenerationIdentity(parent, "parent");
      return raw.observeParent(workspaceId, parent);
    },
    async finish(lease, nowMillis) {
      return parseOperationWindowClose(
        await raw.finish(nativeOperationWindowLease(lease), nowMillis),
      );
    },
    async inspect(workspaceId) {
      requireIdentity(workspaceId, "workspace identity");
      return parseOperationWindowPhase(await raw.inspect(workspaceId));
    },
    async finishWorkspace(workspace, lease, nowMillis, options) {
      validateWorkspaceRebaseOptions(options);
      const result = await raw.finishWorkspace(
        rawWorkspace(workspace, scope),
        nativeOperationWindowLease(lease),
        nowMillis,
        options,
      );
      if (result.kind === "still-active" && result.remaining !== undefined) {
        return { kind: "still-active", remaining: result.remaining };
      }
      if (result.kind === "already-closed") return { kind: "already-closed" };
      if (result.kind === "reconciled" && result.rebase !== undefined) {
        return { kind: "reconciled", rebase: parseWorkspaceRebaseResult(nativeBoundary<WasmRawJoinResult>(result.rebase)) };
      }
      throw new TypeError("native operation window returned a malformed workspace close result");
    },
    async recoverWorkspace(workspace, nowMillis, options) {
      validateWorkspaceRebaseOptions(options);
      const result = await raw.recoverWorkspace(rawWorkspace(workspace, scope), nowMillis, options);
      return result == null ? undefined : parseWorkspaceRebaseResult(nativeBoundary<WasmRawJoinResult>(result));
    },
  };
}

function adaptGitCompat(raw: NativeRawGitCompatRepository): GitCompatRepository {
  const repository: GitCompatRepository = {
    async execute(command: GitCompatCommand, workspaceGeneration: GenerationIdentity) {
      return parseGitCompatOutputJson(
        await raw.executeJson(JSON.stringify(encodeGitCompatCommand(command)), workspaceGeneration),
      );
    },
    async executeArgv(argv, workspaceGeneration, defaultAuthor, nowSeconds) {
      gitCompatSafeTimestamp(nowSeconds);
      return parseGitCompatOutputJson(
        await raw.executeArgvJson(argv, workspaceGeneration, defaultAuthor, nowSeconds.toString()),
      );
    },
    async pendingTransition() {
      const value = await raw.pendingTransitionJson();
      return value == null ? undefined : parseGitPendingTransitionJson(value);
    },
    async completeTransition(transition, resultingGeneration) {
      return parseGitCompatOutputJson(
        await raw.completeTransitionJson(transition, resultingGeneration),
      );
    },
    async completeTransitionResult(transition, result) {
      return parseGitCompatOutputJson(
        await raw.completeTransitionResultJson(transition, stringifyGitFilesystemResult(result)),
      );
    },
    async run(command, workspaceGeneration, executor) {
      return finishGitCompatOutput(
        repository,
        await repository.execute(command, workspaceGeneration),
        executor,
      );
    },
    async runArgv(argv, workspaceGeneration, defaultAuthor, nowSeconds, executor) {
      return finishGitCompatOutput(
        repository,
        await repository.executeArgv(argv, workspaceGeneration, defaultAuthor, nowSeconds),
        executor,
      );
    },
    async resume(executor) {
      const pending = await repository.pendingTransition();
      if (pending === undefined) return undefined;
      return finishGitCompatOutput(
        repository,
        { Prepared: { transition: pending.id, action: pending.action } },
        executor,
      );
    },
    abortTransition(transition: OperationIdentity) {
      return raw.abortTransition(transition);
    },
    async registerBranchWorkspace(
      branch: string,
      workspaceId: WorkspaceIdentity,
      head: GitCommitIdentity | undefined,
      switchToBranch: boolean,
    ) {
      return parseGitCompatOutputJson(
        await raw.registerBranchWorkspaceJson(
          branch,
          workspaceId,
          head === undefined ? undefined : gitCommitBytes(head),
          switchToBranch,
        ),
      );
    },
    async recordCommit(
      expectedHead: GitCommitIdentity | undefined,
      generation: GenerationIdentity,
      trackedPaths: readonly string[],
      message: string,
      author: string,
      authoredAtSeconds: bigint,
    ) {
      gitCompatSafeTimestamp(authoredAtSeconds);
      return parseGitCompatOutputJson(
        await raw.recordCommitJson(
          expectedHead === undefined ? undefined : gitCommitBytes(expectedHead),
          generation,
          trackedPaths,
          message,
          author,
          authoredAtSeconds.toString(),
        ),
      );
    },
  };
  return repository;
}

function gitCommitBytes(identity: GitCommitIdentity): Uint8Array {
  if (!/^[0-9a-f]{64}$/.test(identity)) {
    throw new TypeError("Git compatibility commit ID must be 64 lowercase hexadecimal characters");
  }
  return Uint8Array.from(
    identity.match(/../g) ?? [],
    (byte) => Number.parseInt(byte, 16),
  );
}

function adaptFs(raw: NativeRawFs): NativeFsEngine {
  const targetMount = nativeMount(raw.capabilities.platform, raw.capabilities.nativeMount);
  const generationAdapter = createGenerationAdapter(
    copyWorkspaceStat, copyWorkspaceDirectoryPage, copyWorkspaceExtentPlan, "filesystem engine",
  );
  const changeSetAdapter = createChangeSetAdapter(
    generationAdapter.adaptGeneration, nativeGenerationDiff, "filesystem engine",
  );
  const scope: NativeAdapterScope = {
    ...generationAdapter,
    adaptChangeSet: changeSetAdapter.adaptChangeSet,
    workspaceHandles: new WeakMap<FsWorkspace, NativeRawWorkspace>(),
  };
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
    capabilities,
    objectCacheStats(): ObjectCacheStats {
      return copyObjectCacheStats(raw.objectCacheStats());
    },
    clearObjectCache(): void {
      raw.clearObjectCache();
    },
    createSpeculation(volumeId, generationId, options): Speculation {
      return adaptSpeculation(raw.createSpeculation(volumeId, generationId, options));
    },
    async createVolume(options): Promise<FsVolume> {
      return adaptVolume(await raw.createVolume(options));
    },
    async createVolumeWithId(volumeId, options): Promise<FsVolume> {
      return adaptVolume(await raw.createVolumeWithId(volumeId, options));
    },
    async openVolume(volumeId): Promise<FsVolume> {
      return adaptVolume(await raw.openVolume(volumeId));
    },
    async exportObject(objectId, maximumBytes) {
      const value = await raw.exportObject(objectId, maximumBytes);
      return { bytes: copyBytes(value.bytes), work: parseWork(value.workJson) };
    },
    async importObject(objectId, bytes) {
      return mutationResult(await raw.importObject(objectId, bytes));
    },
    async exportGenerationBatch(manifest, cursor, maximumObjects, maximumObjectBytes): Promise<GenerationTransferBatch> {
      const value = await raw.exportGenerationBatch(nativeManifest(manifest), cursor, maximumObjects, maximumObjectBytes);
      return {
        firstObject: value.firstObject,
        nextObject: value.nextObject,
        objects: value.objects.map((object) => copyBytes(object)),
        work: parseWork(value.workJson),
      };
    },
    async importGenerationBatch(manifest, cursor, objects, maximumObjects): Promise<GenerationTransferCursor> {
      const value = await raw.importGenerationBatch(nativeManifest(manifest), cursor, objects, maximumObjects);
      return { nextObject: value.nextObject, work: parseWork(value.workJson) };
    },
    async restoreVolume(manifest, operationId): Promise<FsVolume> {
      return adaptVolume(await raw.restoreVolume(nativeManifest(manifest), operationId));
    },
    async createWorkspace(name: string): Promise<NativeFsWorkspace> {
      requireWorkspaceName(name);
      return adaptWorkspace(await raw.createWorkspace(name), scope);
    },
    async openWorkspace(name: string): Promise<NativeFsWorkspace> {
      requireWorkspaceName(name);
      return adaptWorkspace(await raw.openWorkspace(name), scope);
    },
    async attachDirectory(
      name: string,
      path: string,
      options: import("./contracts.js").NativeSourceOptions,
    ): Promise<NativeFsWorkspace> {
      requireWorkspaceName(name);
      if (path.length === 0) throw new RangeError("source path must be non-empty");
      requirePositiveInteger(options.maximumPaths, "maximum source paths");
      requirePositiveInteger(options.maximumExtentSpans, "maximum source extent spans");
      requirePositiveInteger(options.maximumQueuedChanges, "maximum queued source changes");
      return adaptWorkspace(await raw.attachDirectory(name, path, options), scope);
    },
    get cancelled(): boolean {
      return raw.cancelled;
    },
    cancel(): void {
      raw.cancel();
    },
    close(): void {},
  };
  fsHandles.set(engine, { raw, scope });
  return engine;
}

function adaptVolume(raw: NativeRawVolume): FsVolume {
  return {
    get id() { return copyBytes(raw.id); },
    get acquisitionWork() { return parseWork(raw.acquisitionWorkJson); },
    async diffGenerations(before, after, maximumChanges) {
      return nativeGenerationDiff(await raw.diffGenerations(before, after, maximumChanges));
    },
    async checkout(options) { return adaptCheckout(await raw.checkout(options)); },
  };
}

function adaptCheckout(raw: NativeRawCheckout): FsCheckout {
  return {
    get acquisitionWork() { return parseWork(raw.acquisitionWorkJson); },
    async applyTransaction(operations) {
      const value = await raw.applyTransaction(operations.map(nativeTransactionOperation));
      return copyTransactionResult({ ...value, createdFileIds: value.createdFileIds.map(id => id ?? undefined) }, parseWork(value.workJson));
    },
    async checkpoint() { return checkpointResult(await raw.checkpoint()); },
    async refreshHead() { return checkpointResult(await raw.refreshHead()); },
    async refreshLive() { return checkpointResult(await raw.refreshLive()); },
    async exportManifest(): Promise<GenerationExportManifest> {
      const value = await raw.exportManifest();
      return { manifestBytes: copyBytes(value.manifestBytes), objects: value.objects.map((object) => copyBytes(object)), work: parseWork(value.workJson) };
    },
    async prepareMerge(theirs, maximumChanges, maximumConflicts) {
      const value = await raw.prepareMerge(theirs, maximumChanges, maximumConflicts);
      return parseMergePreparation(
        nativeBoundary<Parameters<typeof parseMergePreparation>[0]>({ ...value, work: parseWork(value.workJson), conflicts: value.conflicts }),
        decodeMergeConflict,
      );
    },
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
    async lookupNoFollow(path) {
      const value = await raw.lookupNoFollow(path);
      return { exists: value.exists, fileId: copyOptionalBytes(value.fileId), fileKind: value.fileKind, resolvedComponents: value.resolvedComponents, work: parseWork(value.workJson) };
    },
    async lookupBatchNoFollow(paths) {
      const value = await raw.lookupBatchNoFollow(paths);
      return { entries: copyBatchLookupEntries(
        nativeBoundary<Parameters<typeof copyBatchLookupEntries>[0]>(value.entries),
      ), retainedAllocationBytes: value.retainedAllocationBytes, work: parseWork(value.workJson) };
    },
    async statNoFollow(path) {
      const value = await raw.statNoFollow(path);
      return copyStatResult(nativeBoundary<Parameters<typeof copyStatResult>[0]>(value), parseWork(value.workJson));
    },
    async readFileRecordById(fileId) {
      const value = await raw.readFileRecordById(fileId);
      return { record: copyFileRecord(nativeBoundary<Parameters<typeof copyFileRecord>[0]>(value.record)), work: parseWork(value.workJson) };
    },
    async readMetadata(path) { return metadataResult(await raw.readMetadata(path)); },
    async readMetadataById(fileId) { return metadataResult(await raw.readMetadataById(fileId)); },
    async setMetadata(path, canonicalBytes) { return mutationResult(await raw.setMetadata(path, canonicalBytes)); },
    async setMetadataById(fileId, canonicalBytes) { return mutationResult(await raw.setMetadataById(fileId, canonicalBytes)); },
    async setAttributes(path, canonicalBytes, logicalBytes) { return mutationResult(await raw.setAttributes(path, canonicalBytes, logicalBytes)); },
    async setAttributesById(fileId, canonicalBytes, logicalBytes) { return mutationResult(await raw.setAttributesById(fileId, canonicalBytes, logicalBytes)); },
    async readNamedAttribute(path, attributeClass, name) {
      const value = await raw.readNamedAttribute(path, attributeClass, name);
      return copyNamedAttributeResult(nativeBoundary<Parameters<typeof copyNamedAttributeResult>[0]>(value), parseWork(value.workJson));
    },
    async listNamedAttributes(path, after, maximumEntries) {
      const value = await raw.listNamedAttributes(path, after?.attributeClass, after?.name, maximumEntries);
      return copyNamedAttributePage(nativeBoundary<Parameters<typeof copyNamedAttributePage>[0]>(value), parseWork(value.workJson));
    },
    async writeNamedAttribute(path, attributeClass, name, bytes, mode) { return mutationResult(await raw.writeNamedAttribute(path, attributeClass, name, bytes, mode)); },
    async removeNamedAttribute(path, attributeClass, name) { return mutationResult(await raw.removeNamedAttribute(path, attributeClass, name)); },
    async resolveFiles(paths) {
      const value = await raw.resolveFiles(paths);
      const files = Array.from({ length: value.length }, (_, index) => {
        const file = value.take(index);
        return file == null ? undefined : adaptResolvedFile(file);
      });
      return { files, work: parseWork(value.workJson) };
    },
    async readFileRange(path, offset, length) { return fileReadResult(await raw.readFileRange(path, offset, length)); },
    async readFileRangeById(fileId, offset, length) { return fileReadResult(await raw.readFileRangeById(fileId, offset, length)); },
    async planFileExtents(path, offset, length, maximumSpans) { return nativeFileExtentPlan(await raw.planFileExtents(path, offset, length, maximumSpans)); },
    async planFileExtentsById(fileId, offset, length, maximumSpans) { return nativeFileExtentPlan(await raw.planFileExtentsById(fileId, offset, length, maximumSpans)); },
    async seekFileExtent(path, offset, target) { return seekResult(nativeBoundary<Parameters<typeof seekResult>[0]>(await raw.seekFileExtent(path, offset, target))); },
    async seekFileExtentById(fileId, offset, target) { return seekResult(nativeBoundary<Parameters<typeof seekResult>[0]>(await raw.seekFileExtentById(fileId, offset, target))); },
    async readSymbolicLink(path) { return fileReadResult(await raw.readSymbolicLink(path)); },
    async readReparsePoint(path) { return fileReadResult(await raw.readReparsePoint(path)); },
    async listDirectory(path, after, maximumEntries) {
      const value = await raw.listDirectory(path, after, maximumEntries);
      return copyDirectoryPage(value, parseWork(value.workJson));
    },
    async listDirectoryRecords(path, after, maximumEntries) {
      const value = await raw.listDirectoryRecords(path, after, maximumEntries);
      return copyDirectoryRecordPage(nativeBoundary<Parameters<typeof copyDirectoryRecordPage>[0]>(value), parseWork(value.workJson));
    },
    async createFile(path, bytes) { return mutationResult(await raw.createFile(path, bytes)); },
    async createDirectory(path) { return mutationResult(await raw.createDirectory(path)); },
    async createSymbolicLink(path, target) { return mutationResult(await raw.createSymbolicLink(path, target)); },
    async createSpecial(path, kind) { return mutationResult(await raw.createSpecial(path, kind)); },
    async createDevice(path, kind, major, minor) { return mutationResult(await raw.createDevice(path, kind, major, minor)); },
    async createReparsePoint(path, payload) { return mutationResult(await raw.createReparsePoint(path, payload)); },
    async writeFile(path, offset, bytes) { return mutationResult(await raw.writeFile(path, offset, bytes)); },
    async writeFileById(fileId, offset, bytes) { return mutationResult(await raw.writeFileById(fileId, offset, bytes)); },
    async remove(path, expectedFileId) { return mutationResult(await raw.remove(path, expectedFileId)); },
    async rename(source, destination, replace) { return mutationResult(await raw.rename(source, destination, replace)); },
    async hardLink(source, destination) { return mutationResult(await raw.hardLink(source, destination)); },
    async resizeFile(path, logicalBytes) { return mutationResult(await raw.resizeFile(path, logicalBytes)); },
    async resizeFileById(fileId, logicalBytes) { return mutationResult(await raw.resizeFileById(fileId, logicalBytes)); },
    async zeroFileRange(path, offset, length, allocated, extend) { return mutationResult(await raw.zeroFileRange(path, offset, length, allocated, extend)); },
    async zeroFileRangeById(fileId, offset, length, allocated, extend) { return mutationResult(await raw.zeroFileRangeById(fileId, offset, length, allocated, extend)); },
    async preallocateFile(path, offset, length, keepSize) { return mutationResult(await raw.preallocateFile(path, offset, length, keepSize)); },
    async preallocateFileById(fileId, offset, length, keepSize) { return mutationResult(await raw.preallocateFileById(fileId, offset, length, keepSize)); },
    async cloneFileRange(source, sourceOffset, destination, destinationOffset, length) { return mutationResult(await raw.cloneFileRange(source, sourceOffset, destination, destinationOffset, length)); },
    async cloneFileRangeById(sourceFileId, sourceOffset, destinationFileId, destinationOffset, length) { return mutationResult(await raw.cloneFileRangeById(sourceFileId, sourceOffset, destinationFileId, destinationOffset, length)); },
    async commit(operationId) { return commitResult(await raw.commit(operationId)); },
    async mutateLive(operations, operationId, maximumAttempts, maximumConflicts) { return liveTransactionResult(await raw.mutateLive(operations.map(nativeTransactionOperation), operationId, maximumAttempts, maximumConflicts)); },
    async resumeLive(operationId, maximumAttempts, maximumConflicts) { return liveMutationResult(await raw.resumeLive(operationId, maximumAttempts, maximumConflicts)); },
    async rebaseHead(maximumConflicts) {
      const value = await raw.rebaseHead(maximumConflicts);
      return copyRebaseResult(nativeBoundary<Parameters<typeof copyRebaseResult>[0]>(value), parseWork(value.workJson));
    },
    async discard() { return mutationResult(await raw.discard()); },
    cancel() { raw.cancel(); },
  };
}

function adaptResolvedFile(raw: import("./contracts.js").NativeRawResolvedFile): ResolvedFile {
  return {
    kind: raw.kind,
    logicalBytes: raw.logicalBytes,
    metadataCanonicalBytes: copyBytes(raw.metadataCanonicalBytes),
    async readRange(offset, length) { return fileReadResult(await raw.readRange(offset, length)); },
    async readSymbolicLink() { return fileReadResult(await raw.readSymbolicLink()); },
  };
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
      if ((change.kind === "created" || change.kind === "modified" || change.kind === "metadata" || change.kind === "removed") && change.path !== undefined && change.from === undefined && change.to === undefined) return { kind: change.kind, path: copyNamespacePath(nativeBoundary<NativeNamespacePath>(change.path)) };
      if (change.kind === "renamed" && change.path === undefined && change.from !== undefined && change.to !== undefined) return { kind: "renamed", from: copyNamespacePath(nativeBoundary<NativeNamespacePath>(change.from)), to: copyNamespacePath(nativeBoundary<NativeNamespacePath>(change.to)) };
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

function adaptSpeculation(raw: NativeRawSpeculation): Speculation {
  return {
    observe(value) { return raw.observe(value); },
    async executeResidency(operationId) { const value = await raw.executeResidency(operationId); return { objectBytes: value.objectBytes, work: parseWork(value.workJson) }; },
    finishResidency(operationId, useful) { return raw.finishResidency(operationId, useful); },
    async planPromotion(request) {
      const value = await raw.planPromotion(request.operationId, request.acceptedTiers, request.residency, request.destinations);
      return {
        status: value.status,
        ...(value.rejection === undefined ? {} : { rejection: value.rejection }),
        ...(value.operationId === undefined ? {} : { operationId: copyBytes(value.operationId) }),
        ...(value.objectId === undefined ? {} : { objectId: copyBytes(value.objectId) }),
        ...(value.sourceLocationId === undefined ? {} : { sourceLocationId: copyBytes(value.sourceLocationId) }),
        ...(value.destinationLocationId === undefined ? {} : { destinationLocationId: copyBytes(value.destinationLocationId) }),
        ...(value.estimatedCostUnits === undefined ? {} : { estimatedCostUnits: value.estimatedCostUnits }),
      };
    },
    finishPromotion(operationId, useful) { return raw.finishPromotion(operationId, useful); },
    preemptForForeground(bytes) { return raw.preemptForForeground(bytes); },
    replaceGeneration(generationId) { return raw.replaceGeneration(generationId); },
    async metrics(): Promise<SpeculationMetrics> {
      const parsed: unknown = JSON.parse(await raw.metricsJson());
      if (typeof parsed !== "object" || parsed === null) throw new TypeError("native speculation metrics are malformed");
      const value = parsed as { residency?: Record<string, string | number>; promotion?: Record<string, string | number> };
      return { residency: bigintRecord(value.residency ?? {}), promotion: bigintRecord(value.promotion ?? {}) };
    },
    cancel() { raw.cancel(); },
  };
}

function nativeManifest(value: GenerationExportManifest) {
  return { manifestBytes: value.manifestBytes, objects: value.objects, workJson: JSON.stringify(value.work) };
}

function copyBytes(value: Uint8Array): Uint8Array {
  return Uint8Array.from(value);
}

function copyOptionalBytes(value: Uint8Array | undefined): Uint8Array | undefined {
  return value === undefined ? undefined : copyBytes(value);
}

function copyObjectCacheStats(value: ObjectCacheStats): ObjectCacheStats {
  return { ...value };
}

function checkpointResult(value: Awaited<ReturnType<NativeRawCheckout["checkpoint"]>>) {
  return { generationId: copyBytes(value.generationId), work: parseWork(value.workJson) };
}

function fileReadResult(value: { readonly bytes: Uint8Array; readonly workJson: string }) {
  return { bytes: copyBytes(value.bytes), work: parseWork(value.workJson) };
}

function metadataResult(value: { readonly canonicalBytes: Uint8Array; readonly workJson: string }) {
  return { canonicalBytes: copyBytes(value.canonicalBytes), work: parseWork(value.workJson) };
}

function mutationResult(value: NativeRawMutation) {
  return { fileId: copyOptionalBytes(value.fileId), work: parseWork(value.workJson) };
}

function seekResult(value: { readonly offset: bigint | undefined; readonly workJson: string }) {
  return { offset: value.offset, work: parseWork(value.workJson) };
}

function nativeFileExtentPlan(value: Awaited<ReturnType<NativeRawCheckout["planFileExtents"]>>) {
  return copyFileExtentPlan(
    nativeBoundary<Parameters<typeof copyFileExtentPlan>[0]>(value),
    parseWork(value.workJson),
    "native binding",
  );
}

function commitResult(value: Awaited<ReturnType<NativeRawCheckout["commit"]>>) {
  return copyCheckoutCommit(nativeBoundary<Parameters<typeof copyCheckoutCommit>[0]>(value), parseWork(value.workJson));
}

function liveMutationResult(value: Awaited<ReturnType<NativeRawCheckout["resumeLive"]>>) {
  return copyLiveMutation(nativeBoundary<Parameters<typeof copyLiveMutation>[0]>(value), parseWork(value.workJson));
}

function liveTransactionResult(value: Awaited<ReturnType<NativeRawCheckout["mutateLive"]>>) {
  return copyLiveTransaction(nativeBoundary<Parameters<typeof copyLiveTransaction>[0]>(value), parseWork(value.workJson));
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

function adaptWorkspace(
  raw: NativeRawWorkspace,
  scope: NativeAdapterScope = nativeScope,
): NativeFsWorkspace {
  const workspace: NativeFsWorkspace = {
    get name() { return raw.name; },
    get id() { return copyBytes(raw.id); },
    ...workspaceOperations(
      nativeBoundary<Parameters<typeof workspaceOperations>[0]>(raw),
      value => scope.adaptGeneration(nativeBoundary<Parameters<typeof scope.adaptGeneration>[0]>(value)),
      value => parseWorkspaceRebaseResult(nativeBoundary<WasmRawJoinResult>(value)),
    ),
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
      return scope.adaptGeneration(nativeBoundary<Parameters<typeof scope.adaptGeneration>[0]>(await raw.seal()));
    },
    async fork(destination: string, idempotencyKey?: Uint8Array): Promise<FsWorkspace> {
      requireWorkspaceName(destination);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return adaptWorkspace(await raw.fork(destination, idempotencyKey), scope);
    },
    async forkAt(destination: string, generation: FsGeneration): Promise<NativeFsWorkspace> {
      requireWorkspaceName(destination);
      return adaptWorkspace(await raw.forkAt(
        destination,
        nativeBoundary<Parameters<typeof raw.forkAt>[1]>(scope.rawGeneration(generation)),
      ), scope);
    },
    async beginTransaction(idempotencyKey?: Uint8Array): Promise<FsTransaction> {
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return adaptTransaction(
        nativeBoundary<Parameters<typeof adaptTransaction>[0]>(await raw.beginTransaction(idempotencyKey)),
        copyTransactionRebase,
      );
    },
    async diff(from, to, maximumChanges): Promise<FsChangeSet> {
      requirePositiveInteger(maximumChanges, "maximum changes");
      return scope.adaptChangeSet(
        nativeBoundary<Parameters<typeof scope.adaptChangeSet>[0]>(
          await raw.diff(
            nativeBoundary<Parameters<typeof raw.diff>[0]>(scope.rawGeneration(from)),
            nativeBoundary<Parameters<typeof raw.diff>[1]>(scope.rawGeneration(to)),
            maximumChanges,
          ),
        ),
      );
    },
    async joinInto(target, options): Promise<ResolvableFsJoinPlan> {
      validateJoinOptions(options);
      return adaptJoinPlan(await raw.joinInto(rawWorkspace(target, scope), options));
    },
    async mount(destination, options): Promise<NativeWorkspaceMount> {
      if (destination.length === 0) throw new RangeError("mount destination must be non-empty");
      if (options.subdirectory.length === 0) {
        throw new RangeError("mount subdirectory must be non-empty");
      }
      return adaptWorkspaceMount(await raw.mount(destination, options));
    },
  };
  scope.workspaceHandles.set(workspace, raw);
  workspaceScopes.set(workspace, scope);
  // Keep the module-wide registry for the engine-neutral workspace graph API.
  workspaceHandles.set(workspace, raw);
  return workspace;
}

function rawWorkspace(
  workspace: FsWorkspace,
  scope: NativeAdapterScope = nativeScope,
): NativeRawWorkspace {
  const raw = scope.workspaceHandles.get(workspace);
  if (raw === undefined) throw new TypeError("workspace belongs to another filesystem engine");
  return raw;
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
  adaptResolvableJoinPlan(nativeBoundary<Parameters<typeof adaptResolvableJoinPlan>[0]>(raw), parseJoinResult);

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
      return nativeBoundary<Promise<void>>(raw.sync());
    },
    unmount() {
      return nativeBoundary<Promise<boolean>>(raw.unmount());
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

function parseWork(value: string): WorkCounters {
  const parsed: unknown = JSON.parse(value);
  if (typeof parsed !== "object" || parsed === null) {
    throw new TypeError("native work receipt is malformed");
  }
  return parsed as WorkCounters;
}

function requireIdentity(value: Uint8Array, label: string): void {
  if (value.byteLength !== 16) {
    throw new RangeError(`${label} must be exactly 16 bytes`);
  }
}

function requireGenerationIdentity(value: Uint8Array, label: string): void {
  if (value.byteLength !== 32) {
    throw new RangeError(`${label} generation identity must be exactly 32 bytes`);
  }
}

function requirePositiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
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
