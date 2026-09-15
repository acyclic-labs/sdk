import { arch, platform } from "node:process";
import type {
  EngineCapabilities,
  GenerationDiff,
  FsChangeSet,
  FsGeneration,
  FsJoinPlan,
  FsTransaction,
  FsVolume,
  FsCheckout,
  FsWorkspace,
  MergeConflict,
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
  NativeRawGeneration,
  NativeRawGenerationDiff,
  NativeRawChangeSet,
  NativeRawJoinPlan,
  NativeRawJoinResult,
  NativeRawSourceResult,
  NativeRawWorkspace,
  NativeRawWorkspaceCommit,
  NativeRawWorkspaceMount,
  NativeRawWorkspaceTransaction,
  NativeRawWatcher,
  NativeRawWatchBatch,
  NativeWatcher,
  NativeWatchBatch,
  NativeNamespacePath,
  CaptureResult,
  NativeWorkspaceMount,
  NativeSourceResult,
  NativeSourceStatus,
  WorkspaceDirectoryPage,
  WorkspaceExtentPlan,
  WorkspaceName,
  WorkspaceStat,
  TransactionConflict,
  TransactionRebaseResult,
  WorkCounters,
  WorkspaceCommit,
  WorkspaceDeleteStatus,
  WorkspaceRebaseOptions,
  WorkspaceRebaseResult,
  WorkspaceRebaseStatus,
  JoinOptions,
  JoinResult,
  JoinStatus,
  TransactionOperation,
  Speculation,
  SpeculationMetrics,
  FileExtentPlan,
  FileRecordSnapshot,
  CommitResult,
  LiveMutationResult,
  LiveTransactionResult,
  GenerationExportManifest,
  GenerationTransferBatch,
  GenerationTransferCursor,
  ObjectCacheStats,
} from "./contracts.js";

const generationHandles = new WeakMap<FsGeneration, NativeRawGeneration>();
const workspaceHandles = new WeakMap<FsWorkspace, NativeRawWorkspace>();
const changeSetHandles = new WeakMap<FsChangeSet, NativeRawChangeSet>();

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS, portableVolumeOptions } from "./contracts.js";
export { CrossVolumeError, MountedView } from "./mounted.js";
export type { MountedCheckout, MountedSnapshot } from "./mounted.js";

const PACKAGE_VERSION = "0.2.0-rc.4";
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
      return { bytes: value.bytes.slice(), work: parseWork(value.workJson) };
    },
    async importObject(objectId, bytes) {
      return mutationResult(await raw.importObject(objectId, bytes));
    },
    async exportGenerationBatch(manifest, cursor, maximumObjects, maximumObjectBytes): Promise<GenerationTransferBatch> {
      const value = await raw.exportGenerationBatch(nativeManifest(manifest), cursor, maximumObjects, maximumObjectBytes);
      return {
        firstObject: value.firstObject,
        nextObject: value.nextObject,
        objects: value.objects.map((object) => object.slice()),
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
    close(): void {
      raw.close();
    },
  };
  return engine;
}

function adaptVolume(raw: NativeRawVolume): FsVolume {
  return {
    get id() { return raw.id.slice(); },
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
      return { createdFileIds: value.createdFileIds.map(copyOptionalBytes), work: parseWork(value.workJson) };
    },
    async checkpoint() { return checkpointResult(await raw.checkpoint()); },
    async refreshHead() { return checkpointResult(await raw.refreshHead()); },
    async refreshLive() { return checkpointResult(await raw.refreshLive()); },
    async exportManifest(): Promise<GenerationExportManifest> {
      const value = await raw.exportManifest();
      return { manifestBytes: value.manifestBytes.slice(), objects: value.objects.map((object) => object.slice()), work: parseWork(value.workJson) };
    },
    async prepareMerge(theirs, maximumChanges, maximumConflicts) {
      const value = await raw.prepareMerge(theirs, maximumChanges, maximumConflicts);
      const work = parseWork(value.workJson);
      if (value.status === "prepared" && value.generationId !== undefined && value.conflicts.length === 0 && !value.truncated) {
        return { status: "prepared", generationId: value.generationId.slice(), conflicts: [], truncated: false, work };
      }
      if (value.status === "conflicted" && value.generationId === undefined) {
        return { status: "conflicted", generationId: undefined, conflicts: value.conflicts.map(decodeMergeConflict), truncated: value.truncated, work };
      }
      throw new TypeError("native binding returned a malformed merge preparation");
    },
    mount(destination, writable) {
      const value = raw.mount(destination, writable);
      return { get id() { return value.id.slice(); }, destination: value.destination, stop() { return value.stop(); } };
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
      return { entries: value.entries, retainedAllocationBytes: value.retainedAllocationBytes, work: parseWork(value.workJson) };
    },
    async statNoFollow(path) {
      const value = await raw.statNoFollow(path);
      return { exists: value.exists, record: value.record === undefined ? undefined : copyFileRecord(value.record), metadataCanonicalBytes: copyOptionalBytes(value.metadataCanonicalBytes), work: parseWork(value.workJson) };
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
      return { exists: value.exists, bytes: copyOptionalBytes(value.bytes), work: parseWork(value.workJson) };
    },
    async listNamedAttributes(path, after, maximumEntries) {
      const value = await raw.listNamedAttributes(path, after?.attributeClass, after?.name, maximumEntries);
      return { entries: value.entries.map((entry) => ({ attributeClass: entry.attributeClass, name: entry.name.slice() })), hasMore: value.hasMore, work: parseWork(value.workJson) };
    },
    async writeNamedAttribute(path, attributeClass, name, bytes, mode) { return mutationResult(await raw.writeNamedAttribute(path, attributeClass, name, bytes, mode)); },
    async removeNamedAttribute(path, attributeClass, name) { return mutationResult(await raw.removeNamedAttribute(path, attributeClass, name)); },
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
      return { entries: value.entries.map((entry) => ({ ...entry, name: entry.name.slice(), fileId: entry.fileId.slice() })), hasMore: value.hasMore, work: parseWork(value.workJson) };
    },
    async listDirectoryRecords(path, after, maximumEntries) {
      const value = await raw.listDirectoryRecords(path, after, maximumEntries);
      return { entries: value.entries.map((entry) => ({ name: entry.name.slice(), record: copyFileRecord(entry.record), metadataCanonicalBytes: entry.metadataCanonicalBytes.slice() })), hasMore: value.hasMore, work: parseWork(value.workJson) };
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
      return { status: value.status, generationId: copyOptionalBytes(value.generationId), conflictCount: value.conflictCount, truncated: value.truncated, work: parseWork(value.workJson) };
    },
    async discard() { return mutationResult(await raw.discard()); },
    cancel() { raw.cancel(); },
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
  return { components: path.components.map(component => ({ encoding: component.encoding, bytes: component.bytes.slice() })) };
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
        ...(value.operationId === undefined ? {} : { operationId: value.operationId.slice() }),
        ...(value.objectId === undefined ? {} : { objectId: value.objectId.slice() }),
        ...(value.sourceLocationId === undefined ? {} : { sourceLocationId: value.sourceLocationId.slice() }),
        ...(value.destinationLocationId === undefined ? {} : { destinationLocationId: value.destinationLocationId.slice() }),
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

function copyOptionalBytes(value: Uint8Array | undefined): Uint8Array | undefined {
  return value?.slice();
}

function copyObjectCacheStats(value: ObjectCacheStats): ObjectCacheStats {
  return { ...value };
}

function checkpointResult(value: Awaited<ReturnType<NativeRawCheckout["checkpoint"]>>) {
  return { generationId: value.generationId.slice(), work: parseWork(value.workJson) };
}

function fileReadResult(value: { readonly bytes: Uint8Array; readonly workJson: string }) {
  return { bytes: value.bytes.slice(), work: parseWork(value.workJson) };
}

function metadataResult(value: { readonly canonicalBytes: Uint8Array; readonly workJson: string }) {
  return { canonicalBytes: value.canonicalBytes.slice(), work: parseWork(value.workJson) };
}

function mutationResult(value: NativeRawMutation) {
  return { fileId: copyOptionalBytes(value.fileId), work: parseWork(value.workJson) };
}

function seekResult(value: { readonly offset: bigint | undefined; readonly workJson: string }) {
  return { offset: value.offset, work: parseWork(value.workJson) };
}

function copyFileRecord(value: FileRecordSnapshot): FileRecordSnapshot {
  return {
    ...value,
    fileId: value.fileId.slice(),
    metadataObject: value.metadataObject.slice(),
    payloadObject: copyOptionalBytes(value.payloadObject),
    inlineBytes: copyOptionalBytes(value.inlineBytes),
  };
}

function nativeFileExtentPlan(value: Awaited<ReturnType<NativeRawCheckout["planFileExtents"]>>): FileExtentPlan {
  if (value.kind === "inline") return { kind: "inline", work: parseWork(value.workJson) };
  return {
    kind: "sparse",
    spans: value.spans.map((span) => {
      const common = { offset: span.offset, length: span.length, sourceEnd: span.sourceEnd };
      if (span.kind === "content") {
        if (span.objectId === undefined || span.objectOffset === undefined) {
          throw new TypeError("native binding returned a malformed content extent");
        }
        return { kind: "content" as const, ...common, objectId: span.objectId.slice(), objectOffset: span.objectOffset };
      }
      return { kind: span.kind, ...common };
    }),
    retainedAllocationBytes: value.retainedAllocationBytes ?? 0n,
    work: parseWork(value.workJson),
  };
}

function commitResult(value: Awaited<ReturnType<NativeRawCheckout["commit"]>>): CommitResult {
  return {
    status: value.status,
    generationId: copyOptionalBytes(value.generationId),
    epoch: value.epoch,
    sequence: value.sequence,
    committedFingerprint: copyOptionalBytes(value.committedFingerprint),
    work: parseWork(value.workJson),
  };
}

function liveMutationResult(value: Awaited<ReturnType<NativeRawCheckout["resumeLive"]>>): LiveMutationResult {
  return {
    status: value.status,
    generationId: copyOptionalBytes(value.generationId),
    epoch: value.epoch,
    sequence: value.sequence,
    conflictCount: value.conflictCount,
    truncated: value.truncated,
    committedFingerprint: copyOptionalBytes(value.committedFingerprint),
    work: parseWork(value.workJson),
  };
}

function liveTransactionResult(value: Awaited<ReturnType<NativeRawCheckout["mutateLive"]>>): LiveTransactionResult {
  return { ...liveMutationResult(value), createdFileIds: value.createdFileIds.map(copyOptionalBytes) };
}

function nativeGenerationDiff(value: NativeRawGenerationDiff): GenerationDiff {
  return {
    files: value.files.map((change) => ({
      ...change,
      fileId: change.fileId.slice(),
      before: change.before === undefined ? undefined : copyFileRecord(change.before),
      after: change.after === undefined ? undefined : copyFileRecord(change.after),
    })),
    bindings: value.bindings.map((change) => ({
      ...change,
      directoryId: change.directoryId.slice(),
      name: { ...change.name, bytes: change.name.bytes.slice() },
    })),
    truncated: value.truncated,
    work: parseWork(value.workJson),
  };
}

function bigintRecord(value: Readonly<Record<string, string | number>>): Readonly<Record<string, bigint>> {
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, BigInt(item)]));
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
    get name(): string {
      return raw.name;
    },
    get id(): Uint8Array {
      return raw.id.slice();
    },
    async head(): Promise<Uint8Array> {
      return (await raw.head()).slice();
    },
    async sync(): Promise<FsGeneration> {
      return adaptGeneration(await raw.sync());
    },
    async checkpoint(label: string): Promise<FsGeneration> {
      requireWorkspaceName(label);
      return adaptGeneration(await raw.checkpoint(label));
    },
    async pin(identity: string): Promise<FsGeneration> {
      requireWorkspaceName(identity);
      return adaptGeneration(await raw.pin(identity));
    },
    async delete(idempotencyKey?: Uint8Array): Promise<WorkspaceDeleteStatus> {
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return parseWorkspaceDelete(await raw.delete(idempotencyKey));
    },
    async sourceState(): Promise<NativeSourceResult> {
      return parseSourceResult(await raw.sourceState());
    },
    async reconcileSource(): Promise<NativeSourceResult> {
      return parseSourceResult(await raw.reconcileSource());
    },
    async rescanSource(): Promise<NativeSourceResult> {
      return parseSourceResult(await raw.rescanSource());
    },
    async seal(): Promise<FsGeneration> {
      return adaptGeneration(await raw.seal());
    },
    async read(path: string, maximumBytes: bigint): Promise<Uint8Array> {
      requirePositive(maximumBytes, "maximum read bytes");
      return (await raw.read(path, maximumBytes)).slice();
    },
    async readRange(path, offset, length) {
      return (await raw.readRange(path, offset, length)).slice();
    },
    async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
    async listDirectory(path, after, maximumEntries) {
      return copyWorkspaceDirectoryPage(await raw.listDirectory(path, after, maximumEntries));
    },
    async readSymbolicLink(path) { return (await raw.readSymbolicLink(path)).slice(); },
    async planExtents(path, offset, length, maximumSpans) {
      return copyWorkspaceExtentPlan(await raw.planExtents(path, offset, length, maximumSpans));
    },
    async write(path: string, bytes: Uint8Array): Promise<WorkspaceCommit> {
      return nativeWorkspaceCommit(await raw.write(path, bytes));
    },
    async remove(path: string): Promise<WorkspaceCommit> {
      return nativeWorkspaceCommit(await raw.remove(path));
    },
    async fork(destination: string): Promise<FsWorkspace> {
      requireWorkspaceName(destination);
      return adaptWorkspace(await raw.fork(destination));
    },
    async forkAt(destination: string, generation: FsGeneration): Promise<NativeFsWorkspace> {
      requireWorkspaceName(destination);
      return adaptWorkspace(await raw.forkAt(destination, rawGeneration(generation)));
    },
    async beginTransaction(idempotencyKey?: Uint8Array): Promise<FsTransaction> {
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return adaptTransaction(await raw.beginTransaction(idempotencyKey));
    },
    async liveRebase(options, idempotencyKey): Promise<WorkspaceRebaseResult> {
      validateWorkspaceRebaseOptions(options);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return parseWorkspaceRebaseResult(
        await raw.liveRebase(
          idempotencyKey,
          options.maximumGenerations,
          options.maximumChanges,
          options.maximumConflicts,
        ),
      );
    },
    async diff(from, to, maximumChanges): Promise<FsChangeSet> {
      requirePositiveInteger(maximumChanges, "maximum changes");
      return adaptChangeSet(
        await raw.diff(rawGeneration(from), rawGeneration(to), maximumChanges),
      );
    },
    async joinInto(target, options): Promise<FsJoinPlan> {
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

function rawGeneration(generation: FsGeneration): NativeRawGeneration {
  const raw = generationHandles.get(generation);
  if (raw === undefined) throw new TypeError("generation belongs to another filesystem runtime");
  return raw;
}

function adaptGeneration(raw: NativeRawGeneration): FsGeneration {
  const generation: FsGeneration = {
    get id(): Uint8Array {
      return raw.id.slice();
    },
    get workspaceId(): Uint8Array {
      return raw.workspaceId.slice();
    },
    async read(path, maximumBytes) {
      requirePositive(maximumBytes, "maximum read bytes");
      return (await raw.read(path, maximumBytes)).slice();
    },
    async readRange(path, offset, length) {
      return (await raw.readRange(path, offset, length)).slice();
    },
    async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
    async listDirectory(path, after, maximumEntries) {
      return copyWorkspaceDirectoryPage(await raw.listDirectory(path, after, maximumEntries));
    },
    async readSymbolicLink(path) { return (await raw.readSymbolicLink(path)).slice(); },
    async planExtents(path, offset, length, maximumSpans) {
      return copyWorkspaceExtentPlan(await raw.planExtents(path, offset, length, maximumSpans));
    },
    async pin(identity) {
      requireWorkspaceName(identity);
      return adaptGeneration(await raw.pin(identity));
    },
  };
  generationHandles.set(generation, raw);
  return generation;
}

function adaptChangeSet(raw: NativeRawChangeSet): FsChangeSet {
  const changeSet: FsChangeSet = {
    get from(): FsGeneration {
      return adaptGeneration(raw.from);
    },
    get to(): FsGeneration {
      return adaptGeneration(raw.to);
    },
    changes(): GenerationDiff {
      return nativeGenerationDiff(raw.changes());
    },
    async compose(next, maximumChanges): Promise<FsChangeSet> {
      requirePositiveInteger(maximumChanges, "maximum changes");
      return adaptChangeSet(await raw.compose(rawChangeSet(next), maximumChanges));
    },
  };
  changeSetHandles.set(changeSet, raw);
  return changeSet;
}

function rawChangeSet(changeSet: FsChangeSet): NativeRawChangeSet {
  const raw = changeSetHandles.get(changeSet);
  if (raw === undefined) throw new TypeError("change set belongs to another filesystem runtime");
  return raw;
}

function adaptJoinPlan(raw: NativeRawJoinPlan): FsJoinPlan {
  return {
    get targetHead(): Uint8Array {
      return raw.targetHead.slice();
    },
    get commonAncestor(): Uint8Array {
      return raw.commonAncestor.slice();
    },
    async apply(ifTarget, idempotencyKey): Promise<JoinResult> {
      requireGenerationIdentity(ifTarget, "target generation");
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return parseJoinResult(await raw.apply(ifTarget, idempotencyKey));
    },
    async close(): Promise<void> {},
  };
}

function validateJoinOptions(options: JoinOptions): void {
  requirePositiveInteger(options.maximumGenerations, "maximum join generations");
  requirePositiveInteger(options.maximumChanges, "maximum join changes");
  requirePositiveInteger(options.maximumConflicts, "maximum join conflicts");
}

function validateWorkspaceRebaseOptions(options: WorkspaceRebaseOptions): void {
  requirePositiveInteger(options.maximumGenerations, "maximum rebase generations");
  requirePositiveInteger(options.maximumChanges, "maximum rebase changes");
  requirePositiveInteger(options.maximumConflicts, "maximum rebase conflicts");
}

function parseWorkspaceRebaseResult(value: NativeRawJoinResult): WorkspaceRebaseResult {
  const { status } = value;
  if (
    status !== "rebased" && status !== "already-rebased" && status !== "current" &&
    status !== "stale" && status !== "conflicted" && status !== "fenced" &&
    status !== "idempotency-conflict"
  ) throw new TypeError("workspace rebase result has an invalid status");
  const typedStatus: WorkspaceRebaseStatus = status;
  return {
    status: typedStatus,
    generationId: value.generationId === undefined ? undefined : value.generationId.slice(),
    conflicts: value.conflicts.map(decodeMergeConflict),
    truncated: value.truncated,
  };
}

function parseJoinResult(value: NativeRawJoinResult): JoinResult {
  const { status } = value;
  if (
    status !== "applied" &&
    status !== "already-applied" &&
    status !== "no-changes" &&
    status !== "stale-target" &&
    status !== "conflicted" &&
    status !== "fenced" &&
    status !== "idempotency-conflict"
  ) {
    throw new TypeError("join result has an invalid status");
  }
  const typedStatus: JoinStatus = status;
  return {
    status: typedStatus,
    generationId: value.generationId === undefined ? undefined : value.generationId.slice(),
    conflicts: value.conflicts.map(decodeMergeConflict),
    truncated: value.truncated,
  };
}

function parseWorkspaceDelete(status: string): WorkspaceDeleteStatus {
  if (
    status !== "deleted" &&
    status !== "already-deleted" &&
    status !== "conflict" &&
    status !== "idempotency-conflict"
  ) {
    throw new TypeError("workspace deletion has an invalid status");
  }
  return status;
}

function parseSourceResult(value: NativeRawSourceResult): NativeSourceResult {
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
  const typedStatus: NativeSourceStatus = status;
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
    generationId: value.generationId === undefined ? undefined : value.generationId.slice(),
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

function adaptTransaction(raw: NativeRawWorkspaceTransaction): FsTransaction {
  return {
    createDirAll(path) {
      return raw.createDirAll(path);
    },
    createDirectory(path) {
      return raw.createDirectory(path);
    },
    createSymbolicLink(path, target) {
      return raw.createSymbolicLink(path, target);
    },
    write(path, bytes) {
      return raw.write(path, bytes);
    },
    remove(path) {
      return raw.remove(path);
    },
    copy(source, destination) {
      return raw.copy(source, destination);
    },
    rename(source, destination) {
      return raw.rename(source, destination);
    },
    hardLink(source, destination) {
      return raw.hardLink(source, destination);
    },
    writeRange(path, offset, bytes) {
      requireNonnegative(offset, "write offset");
      return raw.writeRange(path, offset, bytes);
    },
    resize(path, logicalBytes) {
      requireNonnegative(logicalBytes, "logical bytes");
      return raw.resize(path, logicalBytes);
    },
    zeroRange(path, offset, length, allocated, extend) {
      requireNonnegative(offset, "zero-range offset");
      requireNonnegative(length, "zero-range length");
      return raw.zeroRange(path, offset, length, allocated, extend);
    },
    preallocate(path, offset, length, keepSize) {
      requireNonnegative(offset, "preallocation offset");
      requireNonnegative(length, "preallocation length");
      return raw.preallocate(path, offset, length, keepSize);
    },
    cloneRange(source, sourceOffset, destination, destinationOffset, length) {
      requireNonnegative(sourceOffset, "clone source offset");
      requireNonnegative(destinationOffset, "clone destination offset");
      requireNonnegative(length, "clone length");
      return raw.cloneRange(source, sourceOffset, destination, destinationOffset, length);
    },
    async rebase(maximumConflicts) {
      requirePositiveInteger(maximumConflicts, "maximum transaction conflicts");
      return copyTransactionRebase(await raw.rebase(maximumConflicts));
    },
    async commit() {
      return nativeWorkspaceCommit(await raw.commit());
    },
    async close(): Promise<void> {},
  };
}

function requireWorkspaceName(name: string): void {
  if (name.length === 0) throw new RangeError("workspace name must be non-empty");
}

function nativeWorkspaceCommit(value: NativeRawWorkspaceCommit): WorkspaceCommit {
  const { status } = value;
  if (
    status !== "committed" &&
    status !== "already-committed" &&
    status !== "conflict" &&
    status !== "fenced" &&
    status !== "idempotency-conflict"
  ) {
    throw new TypeError("native workspace commit has an invalid status");
  }
  return {
    status,
    generationId: value.generationId === undefined ? undefined : value.generationId.slice(),
  };
}

function copyWorkspaceName(value: WorkspaceName): WorkspaceName {
  return { encoding: value.encoding, bytes: value.bytes.slice() };
}

function copyWorkspaceStat(value: WorkspaceStat): WorkspaceStat {
  return { ...value, fileId: value.fileId.slice(), metadata: { ...value.metadata } };
}

function copyWorkspaceDirectoryPage(value: WorkspaceDirectoryPage): WorkspaceDirectoryPage {
  return {
    hasMore: value.hasMore,
    entries: value.entries.map((entry) => ({
      name: copyWorkspaceName(entry.name), fileId: entry.fileId.slice(), kind: entry.kind,
    })),
  };
}

function copyWorkspaceExtentPlan(value: WorkspaceExtentPlan): WorkspaceExtentPlan {
  return { spans: value.spans.map((span) => ({ ...span })) };
}

function copyTransactionConflict(value: TransactionConflict): TransactionConflict {
  return {
    ...value,
    fileId: value.fileId?.slice(),
    directoryId: value.directoryId?.slice(),
    name: value.name === undefined ? undefined : copyWorkspaceName(value.name),
    expected: value.expected?.slice(),
    actual: value.actual?.slice(),
  };
}

function copyTransactionRebase(value: TransactionRebaseResult): TransactionRebaseResult {
  if (value.status !== "rebased" && value.status !== "conflicted") {
    throw new TypeError("transaction rebase has an invalid status");
  }
  return {
    status: value.status,
    generationId: value.generationId?.slice(),
    conflicts: value.conflicts.map(copyTransactionConflict),
    truncated: value.truncated,
  };
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

function decodeMergeConflict(raw: {
  readonly kind: string;
  readonly fileId: Uint8Array | undefined;
  readonly directoryId: Uint8Array | undefined;
  readonly name: import("./contracts.js").NativePathComponent | undefined;
}): MergeConflict {
  if (
    raw.kind === "file" &&
    raw.fileId?.byteLength === 16 &&
    raw.directoryId === undefined &&
    raw.name === undefined
  ) {
    return { kind: "file", fileId: raw.fileId };
  }
  if (
    raw.kind === "binding" &&
    raw.fileId === undefined &&
    raw.directoryId?.byteLength === 16 &&
    raw.name !== undefined
  ) {
    return { kind: "binding", directoryId: raw.directoryId, name: raw.name };
  }
  throw new Error("native merge returned a malformed conflict");
}

function requirePositive(value: bigint, label: string): void {
  if (value <= 0n) throw new RangeError(`${label} must be positive`);
}

function requireNonnegative(value: bigint, label: string): void {
  if (value < 0n) throw new RangeError(`${label} must be non-negative`);
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
