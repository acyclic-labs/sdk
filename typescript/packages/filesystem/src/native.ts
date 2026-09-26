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
  NativeRawJoinResult,
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
const nativeGenerationDiff = (value: NativeRawGenerationDiff) => copyGenerationDiff(value, parseWork(value.workJson));
const workspaceHandles = new WeakMap<FsWorkspace, NativeRawWorkspace>();
const { adaptChangeSet } = createChangeSetAdapter(adaptGeneration, nativeGenerationDiff);
const fsHandles = new WeakMap<NativeFsEngine, NativeRawFs>();
const decodeMergeConflict = (raw: NativeRawMergeConflict) => decodeSharedMergeConflict(raw, "native merge");

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
  const raw = fsHandles.get(filesystem);
  if (raw === undefined) {
    throw new TypeError("operation windows require a native filesystem opened by this module");
  }
  return adaptOperationWindowCoordinator(raw.operationWindows());
}

function adaptWorkspaceGraph(raw: NativeRawWorkspaceGraph): WorkspaceGraph {
  return {
    async registerRoot(workspace) {
      return copyWorkspaceLineageRecord(await raw.registerRoot(rawWorkspace(workspace)));
    },
    async fork(parent, destination, idempotencyKey) {
      requireWorkspaceName(destination);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return adaptWorkspace(
        await raw.fork(rawWorkspace(parent), destination, idempotencyKey),
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
        rawWorkspace(workspace),
        nativeOperationWindowLease(lease),
        nowMillis,
        options,
      );
      if (result.kind === "still-active" && result.remaining !== undefined) {
        return { kind: "still-active", remaining: result.remaining };
      }
      if (result.kind === "already-closed") return { kind: "already-closed" };
      if (result.kind === "reconciled" && result.rebase !== undefined) {
        return { kind: "reconciled", rebase: parseWorkspaceRebaseResult(result.rebase) };
      }
      throw new TypeError("native operation window returned a malformed workspace close result");
    },
    async recoverWorkspace(workspace, nowMillis, options) {
      validateWorkspaceRebaseOptions(options);
      const result = await raw.recoverWorkspace(rawWorkspace(workspace), nowMillis, options);
      return result === undefined ? undefined : parseWorkspaceRebaseResult(result);
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
      return value === undefined ? undefined : parseGitPendingTransitionJson(value);
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
      return adaptWorkspace(await raw.createWorkspace(name));
    },
    async openWorkspace(name: string): Promise<NativeFsWorkspace> {
      requireWorkspaceName(name);
      return adaptWorkspace(await raw.openWorkspace(name));
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
      return adaptWorkspace(await raw.attachDirectory(name, path, options));
    },
    get cancelled(): boolean {
      return raw.cancelled;
    },
    cancel(): void {
      raw.cancel();
    },
    close(): void {},
  };
  fsHandles.set(engine, raw);
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
      return copyTransactionResult(value, parseWork(value.workJson));
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
      return parseMergePreparation({ ...value, work: parseWork(value.workJson) }, decodeMergeConflict);
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
      return { entries: copyBatchLookupEntries(value.entries), retainedAllocationBytes: value.retainedAllocationBytes, work: parseWork(value.workJson) };
    },
    async statNoFollow(path) {
      const value = await raw.statNoFollow(path);
      return copyStatResult(value, parseWork(value.workJson));
    },
    async readFileRecordById(fileId) {
      const value = await raw.readFileRecordById(fileId);
      return { record: copyFileRecord(value.record), work: parseWork(value.workJson) };
    },
    async readMetadata(path) { return metadataResult(await raw.readMetadata(path)); },
    async readMetadataById(fileId) { return metadataResult(await raw.readMetadataById(fileId)); },
    async setMetadata(path, canonicalBytes) { return mutationResult(await raw.setMetadata(path, canonicalBytes)); },
    async setMetadataById(fileId, canonicalBytes) { return mutationResult(await raw.setMetadataById(fileId, canonicalBytes)); },
    async setAttributes(path, canonicalBytes, logicalBytes) { return mutationResult(await raw.setAttributes(path, canonicalBytes, logicalBytes)); },
    async setAttributesById(fileId, canonicalBytes, logicalBytes) { return mutationResult(await raw.setAttributesById(fileId, canonicalBytes, logicalBytes)); },
    async readNamedAttribute(path, attributeClass, name) {
      const value = await raw.readNamedAttribute(path, attributeClass, name);
      return copyNamedAttributeResult(value, parseWork(value.workJson));
    },
    async listNamedAttributes(path, after, maximumEntries) {
      const value = await raw.listNamedAttributes(path, after?.attributeClass, after?.name, maximumEntries);
      return copyNamedAttributePage(value, parseWork(value.workJson));
    },
    async writeNamedAttribute(path, attributeClass, name, bytes, mode) { return mutationResult(await raw.writeNamedAttribute(path, attributeClass, name, bytes, mode)); },
    async removeNamedAttribute(path, attributeClass, name) { return mutationResult(await raw.removeNamedAttribute(path, attributeClass, name)); },
    async resolveFiles(paths) {
      const value = await raw.resolveFiles(paths);
      const files = Array.from({ length: value.length }, (_, index) => {
        const file = value.take(index);
        return file === undefined ? undefined : adaptResolvedFile(file);
      });
      return { files, work: parseWork(value.workJson) };
    },
    async readFileRange(path, offset, length) { return fileReadResult(await raw.readFileRange(path, offset, length)); },
    async readFileRangeById(fileId, offset, length) { return fileReadResult(await raw.readFileRangeById(fileId, offset, length)); },
    async planFileExtents(path, offset, length, maximumSpans) { return nativeFileExtentPlan(await raw.planFileExtents(path, offset, length, maximumSpans)); },
    async planFileExtentsById(fileId, offset, length, maximumSpans) { return nativeFileExtentPlan(await raw.planFileExtentsById(fileId, offset, length, maximumSpans)); },
    async seekFileExtent(path, offset, target) { return seekResult(await raw.seekFileExtent(path, offset, target)); },
    async seekFileExtentById(fileId, offset, target) { return seekResult(await raw.seekFileExtentById(fileId, offset, target)); },
    async readSymbolicLink(path) { return fileReadResult(await raw.readSymbolicLink(path)); },
    async readReparsePoint(path) { return fileReadResult(await raw.readReparsePoint(path)); },
    async listDirectory(path, after, maximumEntries) {
      const value = await raw.listDirectory(path, after, maximumEntries);
      return copyDirectoryPage(value, parseWork(value.workJson));
    },
    async listDirectoryRecords(path, after, maximumEntries) {
      const value = await raw.listDirectoryRecords(path, after, maximumEntries);
      return copyDirectoryRecordPage(value, parseWork(value.workJson));
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
      return copyRebaseResult(value, parseWork(value.workJson));
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
      if ((change.kind === "created" || change.kind === "modified" || change.kind === "metadata" || change.kind === "removed") && change.path !== undefined && change.from === undefined && change.to === undefined) return { kind: change.kind, path: copyNamespacePath(change.path) };
      if (change.kind === "renamed" && change.path === undefined && change.from !== undefined && change.to !== undefined) return { kind: "renamed", from: copyNamespacePath(change.from), to: copyNamespacePath(change.to) };
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
  return copyFileExtentPlan(value, parseWork(value.workJson), "native binding");
}

function commitResult(value: Awaited<ReturnType<NativeRawCheckout["commit"]>>) {
  return copyCheckoutCommit(value, parseWork(value.workJson));
}

function liveMutationResult(value: Awaited<ReturnType<NativeRawCheckout["resumeLive"]>>) {
  return copyLiveMutation(value, parseWork(value.workJson));
}

function liveTransactionResult(value: Awaited<ReturnType<NativeRawCheckout["mutateLive"]>>) {
  return copyLiveTransaction(value, parseWork(value.workJson));
}

function nativeTransactionOperation(value: TransactionOperation): NativeRawTransactionOperation {
  const operation: NativeRawTransactionOperation = {
    kind: value.kind,
    path: "path" in value ? value.path : undefined,
    source: "source" in value ? value.source : undefined,
    destination: "destination" in value ? value.destination : undefined,
    bytes: "bytes" in value ? value.bytes : undefined,
    target: "target" in value ? value.target : undefined,
    payload: "payload" in value ? value.payload : undefined,
    expectedFileId: "expectedFileId" in value ? value.expectedFileId : undefined,
    fileKind: "fileKind" in value ? value.fileKind : undefined,
    offset: "offset" in value ? value.offset : undefined,
    sourceOffset: "sourceOffset" in value ? value.sourceOffset : undefined,
    destinationOffset: "destinationOffset" in value ? value.destinationOffset : undefined,
    length: "length" in value ? value.length : undefined,
    logicalBytes: "logicalBytes" in value ? value.logicalBytes : undefined,
    major: "major" in value ? value.major : undefined,
    minor: "minor" in value ? value.minor : undefined,
    replace: "replace" in value ? value.replace : undefined,
    allocated: "allocated" in value ? value.allocated : undefined,
    extend: "extend" in value ? value.extend : undefined,
    keepSize: "keepSize" in value ? value.keepSize : undefined,
    canonicalBytes: "canonicalBytes" in value ? value.canonicalBytes : undefined,
  };
  return operation;
}

function adaptWorkspace(raw: NativeRawWorkspace): NativeFsWorkspace {
  const workspace: NativeFsWorkspace = {
    get name() { return raw.name; },
    get id() { return copyBytes(raw.id); },
    ...workspaceOperations(raw, adaptGeneration, parseWorkspaceRebaseResult),
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
      return adaptGeneration(await raw.seal());
    },
    async fork(destination: string, idempotencyKey?: Uint8Array): Promise<FsWorkspace> {
      requireWorkspaceName(destination);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return adaptWorkspace(await raw.fork(destination, idempotencyKey));
    },
    async forkAt(destination: string, generation: FsGeneration): Promise<NativeFsWorkspace> {
      requireWorkspaceName(destination);
      return adaptWorkspace(await raw.forkAt(destination, rawGeneration(generation)));
    },
    async beginTransaction(idempotencyKey?: Uint8Array): Promise<FsTransaction> {
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return adaptTransaction(await raw.beginTransaction(idempotencyKey), copyTransactionRebase);
    },
    async diff(from, to, maximumChanges): Promise<FsChangeSet> {
      requirePositiveInteger(maximumChanges, "maximum changes");
      return adaptChangeSet(
        await raw.diff(rawGeneration(from), rawGeneration(to), maximumChanges),
      );
    },
    async joinInto(target, options): Promise<ResolvableFsJoinPlan> {
      validateJoinOptions(options);
      return adaptJoinPlan(await raw.joinInto(rawWorkspace(target), options));
    },
    async mount(destination, options): Promise<NativeWorkspaceMount> {
      if (destination.length === 0) throw new RangeError("mount destination must be non-empty");
      if (options.subdirectory.length === 0) {
        throw new RangeError("mount subdirectory must be non-empty");
      }
      return adaptWorkspaceMount(await raw.mount(destination, options));
    },
  };
  workspaceHandles.set(workspace, raw);
  return workspace;
}

function rawWorkspace(workspace: FsWorkspace): NativeRawWorkspace {
  const raw = workspaceHandles.get(workspace);
  if (raw === undefined) throw new TypeError("workspace belongs to another filesystem runtime");
  return raw;
}

const parseJoinResult = (value: NativeRawJoinResult): JoinResult => parseSharedJoinResult(value, decodeMergeConflict);
const parseWorkspaceRebaseResult = (value: NativeRawJoinResult): WorkspaceRebaseResult => parseSharedWorkspaceRebaseResult(value, decodeMergeConflict);
const adaptJoinPlan = (raw: import("./contracts.js").NativeRawJoinPlan): ResolvableFsJoinPlan => adaptResolvableJoinPlan(raw, parseJoinResult);

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
      return raw.sync();
    },
    unmount() {
      return raw.unmount();
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
