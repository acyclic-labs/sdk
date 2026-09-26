import type {
  FsChangeSet,
  FsVolumeEngine,
  FsVolume,
  FsCheckout,
  FsGeneration,
  FsJoinPlan,
  FsTransaction,
  FsWorkspace,
  WasmRawFs,
  WasmRawVolume,
  WasmRawCheckout,
  WasmRawSpeculation,
  WasmRawJoinPlan,
  WasmRawJoinResult,
  WasmRawMergeConflict,
  WasmRawWorkspace,
  WasmRawGenerationDiff,
  WorkspaceRebaseResult,
  JoinResult,
  GenerationExportManifest,
  GenerationTransferBatch,
  GenerationTransferCursor,
  ObjectCacheStats,
  Speculation,
  SpeculationMetrics,
  ResolvedFile,
} from "./contracts.js";

import { copyMergeConflict, decodeMergeConflict as decodeSharedMergeConflict, parseJoinResult as parseSharedJoinResult, parseMergePreparation, parseWorkspaceRebaseResult as parseSharedWorkspaceRebaseResult,
  validateJoinOptions } from "./workspace-results.js";
import { adaptTransaction } from "./transaction-adapter.js";
import { createGenerationAdapter } from "./generation-adapter.js";
import { createChangeSetAdapter } from "./change-set-adapter.js";
import { copyBatchLookupEntries, copyDirectoryPage, copyDirectoryRecordPage, copyFileRecord,
  copyGenerationDiff, copyNamedAttributePage, copyNamedAttributeResult, copyStatResult } from "./binding-results.js";
import { bigintRecord, copyWorkspaceStat, copyWorkspaceDirectoryPage, copyWorkspaceExtentPlan, copyFileExtentPlan, copyCheckoutCommit, copyLiveMutation, copyLiveTransaction, copyTransactionResult, copyTransactionRebase, copyRebaseResult } from "./workspace-copies.js";
import { adaptJoinPlanBase, workspaceOperations } from "./workspace-operations.js";

const { adaptGeneration, rawGeneration } = createGenerationAdapter(
  copyWorkspaceStat, copyWorkspaceDirectoryPage, copyWorkspaceExtentPlan,
);
const generationDiff = (value: WasmRawGenerationDiff) => copyGenerationDiff(value, value.work);
const workspaceHandles = new WeakMap<FsWorkspace, WasmRawWorkspace>();
const { adaptChangeSet } = createChangeSetAdapter(adaptGeneration, generationDiff);
const decodeMergeConflict = (raw: WasmRawMergeConflict) => decodeSharedMergeConflict(raw, "WASM join");

export function adaptWasmFs(raw: WasmRawFs): FsVolumeEngine {
  const engine: FsVolumeEngine = {
    capabilities: raw.capabilities,
    async createWorkspace(name: string): Promise<FsWorkspace> {
      requireWorkspaceName(name);
      return adaptWorkspace(await raw.createWorkspace(name));
    },
    async openWorkspace(name: string): Promise<FsWorkspace> {
      requireWorkspaceName(name);
      return adaptWorkspace(await raw.openWorkspace(name));
    },
    objectCacheStats(): ObjectCacheStats { return objectCacheStats(raw.objectCacheStats()); },
    clearObjectCache(): void { raw.clearObjectCache(); },
    createSpeculation(volumeId, generationId, options): Speculation { return adaptSpeculation(raw.createSpeculation(volumeId, generationId, options)); },
    async createVolume(options): Promise<FsVolume> { return adaptVolume(await raw.createVolume(options)); },
    async createVolumeWithId(volumeId, options): Promise<FsVolume> { return adaptVolume(await raw.createVolumeWithId(volumeId, options)); },
    async openVolume(volumeId): Promise<FsVolume> { return adaptVolume(await raw.openVolume(volumeId)); },
    async exportObject(objectId, maximumBytes) { return copyFileRead(await raw.exportObject(objectId, maximumBytes)); },
    async importObject(objectId, bytes) { return copyMutation(await raw.importObject(objectId, bytes)); },
    async exportGenerationBatch(manifest, cursor, maximumObjects, maximumObjectBytes): Promise<GenerationTransferBatch> {
      const value = await raw.exportGenerationBatch(manifest, cursor, maximumObjects, maximumObjectBytes);
      return { firstObject: BigInt(value.firstObject), nextObject: value.nextObject === undefined ? undefined : BigInt(value.nextObject), objects: value.objects.map(copyBytes), work: value.work };
    },
    async importGenerationBatch(manifest, cursor, objects, maximumObjects): Promise<GenerationTransferCursor> {
      const value = await raw.importGenerationBatch(manifest, cursor, objects, maximumObjects);
      return { nextObject: BigInt(value.nextObject), work: value.work };
    },
    async restoreVolume(manifest, operationId): Promise<FsVolume> { return adaptVolume(await raw.restoreVolume(manifest, operationId)); },
    close(): void {
      raw.close();
    },
  };
  return engine;
}

export { adaptWorkspaceContextRegistry as adaptWasmWorkspaceContextRegistry } from "./workspace-context.js";

function adaptVolume(raw: WasmRawVolume): FsVolume {
  return {
    get id() { return copyBytes(raw.id); },
    get acquisitionWork() { return raw.acquisitionWork; },
    async diffGenerations(before, after, maximumChanges) { return generationDiff(await raw.diffGenerations(before, after, maximumChanges)); },
    async checkout(options) { return adaptCheckout(await raw.checkout(options)); },
  };
}

function adaptCheckout(raw: WasmRawCheckout): FsCheckout {
  return {
    get acquisitionWork() { return raw.acquisitionWork; },
    async applyTransaction(operations) { const value = await raw.applyTransaction(operations); return copyTransactionResult(value, value.work); },
    async checkpoint() { return copyCheckpoint(await raw.checkpoint()); },
    async refreshHead() { return copyCheckpoint(await raw.refreshHead()); },
    async refreshLive() { return copyCheckpoint(await raw.refreshLive()); },
    async exportManifest(): Promise<GenerationExportManifest> { const value = await raw.exportManifest(); return { manifestBytes: copyBytes(value.manifestBytes), objects: value.objects.map(copyBytes), work: value.work }; },
    async prepareMerge(theirs, maximumChanges, maximumConflicts) { return parseMergePreparation(await raw.prepareMerge(theirs, maximumChanges, maximumConflicts), copyMergeConflict); },
    async lookupNoFollow(path) { const value = await raw.lookupNoFollow(path); return { ...value, fileId: copyOptionalBytes(value.fileId) }; },
    async lookupBatchNoFollow(paths) { const value = await raw.lookupBatchNoFollow(paths); return { entries: copyBatchLookupEntries(value.entries), retainedAllocationBytes: BigInt(value.retainedAllocationBytes), work: value.work }; },
    async statNoFollow(path) { const value = await raw.statNoFollow(path); return copyStatResult(value, value.work); },
    async readFileRecordById(fileId) { const value = await raw.readFileRecordById(fileId); return { record: copyFileRecord(value.record), work: value.work }; },
    async readMetadata(path) { return copyMetadata(await raw.readMetadata(path)); },
    async readMetadataById(fileId) { return copyMetadata(await raw.readMetadataById(fileId)); },
    async setMetadata(path, canonicalBytes) { return copyMutation(await raw.setMetadata(path, canonicalBytes)); },
    async setMetadataById(fileId, canonicalBytes) { return copyMutation(await raw.setMetadataById(fileId, canonicalBytes)); },
    async setAttributes(path, canonicalBytes, logicalBytes) { return copyMutation(await raw.setAttributes(path, canonicalBytes, logicalBytes)); },
    async setAttributesById(fileId, canonicalBytes, logicalBytes) { return copyMutation(await raw.setAttributesById(fileId, canonicalBytes, logicalBytes)); },
    async readNamedAttribute(path, attributeClass, name) { const value = await raw.readNamedAttribute(path, attributeClass, name); return copyNamedAttributeResult(value, value.work); },
    async listNamedAttributes(path, after, maximumEntries) { const value = await raw.listNamedAttributes(path, after?.attributeClass, after?.name, maximumEntries); return copyNamedAttributePage(value, value.work); },
    async writeNamedAttribute(path, attributeClass, name, bytes, mode) { return copyMutation(await raw.writeNamedAttribute(path, attributeClass, name, bytes, mode)); },
    async removeNamedAttribute(path, attributeClass, name) { return copyMutation(await raw.removeNamedAttribute(path, attributeClass, name)); },
    async resolveFiles(paths) {
      const value = await raw.resolveFiles(paths);
      const files = Array.from({ length: value.length }, (_, index) => {
        const file = value.take(index);
        return file === undefined ? undefined : adaptResolvedFile(file);
      });
      return { files, work: value.work };
    },
    async readFileRange(path, offset, length) { return copyFileRead(await raw.readFileRange(path, offset, length)); },
    async readFileRangeById(fileId, offset, length) { return copyFileRead(await raw.readFileRangeById(fileId, offset, length)); },
    async planFileExtents(path, offset, length, maximumSpans) { return fileExtentPlan(await raw.planFileExtents(path, offset, length, maximumSpans)); },
    async planFileExtentsById(fileId, offset, length, maximumSpans) { return fileExtentPlan(await raw.planFileExtentsById(fileId, offset, length, maximumSpans)); },
    async seekFileExtent(path, offset, target) { const value = await raw.seekFileExtent(path, offset, target); return { offset: value.offset === undefined ? undefined : BigInt(value.offset), work: value.work }; },
    async seekFileExtentById(fileId, offset, target) { const value = await raw.seekFileExtentById(fileId, offset, target); return { offset: value.offset === undefined ? undefined : BigInt(value.offset), work: value.work }; },
    async readSymbolicLink(path) { return copyFileRead(await raw.readSymbolicLink(path)); },
    async readReparsePoint(path) { return copyFileRead(await raw.readReparsePoint(path)); },
    async listDirectory(path, after, maximumEntries) { const value = await raw.listDirectory(path, after, maximumEntries); return copyDirectoryPage(value, value.work); },
    async listDirectoryRecords(path, after, maximumEntries) { const value = await raw.listDirectoryRecords(path, after, maximumEntries); return copyDirectoryRecordPage(value, value.work); },
    async createFile(path, bytes) { return copyMutation(await raw.createFile(path, bytes)); },
    async createDirectory(path) { return copyMutation(await raw.createDirectory(path)); },
    async createSymbolicLink(path, target) { return copyMutation(await raw.createSymbolicLink(path, target)); },
    async createSpecial(path, kind) { return copyMutation(await raw.createSpecial(path, kind)); },
    async createDevice(path, kind, major, minor) { return copyMutation(await raw.createDevice(path, kind, major, minor)); },
    async createReparsePoint(path, payload) { return copyMutation(await raw.createReparsePoint(path, payload)); },
    async writeFile(path, offset, bytes) { return copyMutation(await raw.writeFile(path, offset, bytes)); },
    async writeFileById(fileId, offset, bytes) { return copyMutation(await raw.writeFileById(fileId, offset, bytes)); },
    async remove(path, expectedFileId) { return copyMutation(await raw.remove(path, expectedFileId)); },
    async rename(source, destination, replace) { return copyMutation(await raw.rename(source, destination, replace)); },
    async hardLink(source, destination) { return copyMutation(await raw.hardLink(source, destination)); },
    async resizeFile(path, logicalBytes) { return copyMutation(await raw.resizeFile(path, logicalBytes)); },
    async resizeFileById(fileId, logicalBytes) { return copyMutation(await raw.resizeFileById(fileId, logicalBytes)); },
    async zeroFileRange(path, offset, length, allocated, extend) { return copyMutation(await raw.zeroFileRange(path, offset, length, allocated, extend)); },
    async zeroFileRangeById(fileId, offset, length, allocated, extend) { return copyMutation(await raw.zeroFileRangeById(fileId, offset, length, allocated, extend)); },
    async preallocateFile(path, offset, length, keepSize) { return copyMutation(await raw.preallocateFile(path, offset, length, keepSize)); },
    async preallocateFileById(fileId, offset, length, keepSize) { return copyMutation(await raw.preallocateFileById(fileId, offset, length, keepSize)); },
    async cloneFileRange(source, sourceOffset, destination, destinationOffset, length) { return copyMutation(await raw.cloneFileRange(source, sourceOffset, destination, destinationOffset, length)); },
    async cloneFileRangeById(sourceFileId, sourceOffset, destinationFileId, destinationOffset, length) { return copyMutation(await raw.cloneFileRangeById(sourceFileId, sourceOffset, destinationFileId, destinationOffset, length)); },
    async commit(operationId) { return commitResult(await raw.commit(operationId)); },
    async mutateLive(operations, operationId, maximumAttempts, maximumConflicts) { return liveTransactionResult(await raw.mutateLive(operations, operationId, maximumAttempts, maximumConflicts)); },
    async resumeLive(operationId, maximumAttempts, maximumConflicts) { return liveMutationResult(await raw.resumeLive(operationId, maximumAttempts, maximumConflicts)); },
    async rebaseHead(maximumConflicts) { const value = await raw.rebaseHead(maximumConflicts); return copyRebaseResult(value, value.work); },
    async discard() { return copyMutation(await raw.discard()); },
  };
}

function adaptResolvedFile(raw: import("./contracts.js").WasmRawResolvedFile): ResolvedFile {
  return {
    kind: raw.kind,
    logicalBytes: raw.logicalBytes,
    metadataCanonicalBytes: Uint8Array.from(raw.metadataCanonicalBytes),
    async readRange(offset, length) { return copyFileRead(await raw.readRange(offset, length)); },
    async readSymbolicLink() { return copyFileRead(await raw.readSymbolicLink()); },
  };
}

function adaptSpeculation(raw: WasmRawSpeculation): Speculation {
  return {
    async observe(value) { return raw.observe(value); },
    async executeResidency(operationId) { const value = await raw.executeResidency(operationId); return { objectBytes: BigInt(value.objectBytes), work: value.work }; },
    async finishResidency(operationId, useful) { raw.finishResidency(operationId, useful); },
    async planPromotion(request) {
      const value = raw.planPromotion(request);
      return {
        status: value.status,
        ...(value.rejection === undefined ? {} : { rejection: value.rejection }),
        ...(value.operationId === undefined ? {} : { operationId: Uint8Array.from(value.operationId) }),
        ...(value.objectId === undefined ? {} : { objectId: Uint8Array.from(value.objectId) }),
        ...(value.sourceLocationId === undefined ? {} : { sourceLocationId: Uint8Array.from(value.sourceLocationId) }),
        ...(value.destinationLocationId === undefined ? {} : { destinationLocationId: Uint8Array.from(value.destinationLocationId) }),
        ...(value.estimatedCostUnits === undefined ? {} : { estimatedCostUnits: BigInt(value.estimatedCostUnits) }),
      };
    },
    async finishPromotion(operationId, useful) { raw.finishPromotion(operationId, useful); },
    async preemptForForeground(bytes) { return raw.preemptForForeground(bytes); },
    async replaceGeneration(generationId) { return raw.replaceGeneration(generationId); },
    async metrics(): Promise<SpeculationMetrics> {
      const value = raw.metrics();
      return {
        residency: bigintRecord(value.residency ?? {}),
        promotion: bigintRecord(value.promotion ?? {}),
      };
    },
    cancel() { raw.cancel(); },
  };
}

function objectCacheStats(value: import("./contracts.js").WasmRawObjectCacheStats): ObjectCacheStats { return { hits: BigInt(value.hits), decodedHits: BigInt(value.decodedHits), misses: BigInt(value.misses), coalescedReads: BigInt(value.coalescedReads), evictions: BigInt(value.evictions), residentEntries: BigInt(value.residentEntries), residentBytes: BigInt(value.residentBytes), residentCanonicalObjects: BigInt(value.residentCanonicalObjects), residentCanonicalBytes: BigInt(value.residentCanonicalBytes), residentDecodedPages: BigInt(value.residentDecodedPages), residentDecodedBytes: BigInt(value.residentDecodedBytes), inFlight: BigInt(value.inFlight) }; }
function copyCheckpoint(value: import("./contracts.js").CheckpointResult): import("./contracts.js").CheckpointResult { return { generationId: copyBytes(value.generationId), work: value.work }; }
function copyMetadata(value: import("./contracts.js").MetadataResult): import("./contracts.js").MetadataResult { return { canonicalBytes: copyBytes(value.canonicalBytes), work: value.work }; }
function fileExtentPlan(value: Awaited<ReturnType<WasmRawCheckout["planFileExtents"]>>) {
  return copyFileExtentPlan(value, value.work, "WASM", true);
}
function copyFileRead(value: import("./contracts.js").FileReadResult): import("./contracts.js").FileReadResult { return { bytes: copyBytes(value.bytes), work: value.work }; }
function copyMutation(value: import("./contracts.js").MutationResult): import("./contracts.js").MutationResult { return { ...value, fileId: copyOptionalBytes(value.fileId) }; }
function commitResult(value: Awaited<ReturnType<WasmRawCheckout["commit"]>>) { return copyCheckoutCommit(value, value.work); }
function liveMutationResult(value: Awaited<ReturnType<WasmRawCheckout["resumeLive"]>>) { return copyLiveMutation(value, value.work); }
function liveTransactionResult(value: Awaited<ReturnType<WasmRawCheckout["mutateLive"]>>) { return copyLiveTransaction(value, value.work); }

function adaptWorkspace(raw: WasmRawWorkspace): FsWorkspace {
  const workspace: FsWorkspace = {
    get name() { return raw.name; },
    get id() { return copyBytes(raw.id); },
    ...workspaceOperations(raw, adaptGeneration, parseWorkspaceRebaseResult),
    async fork(destination: string, idempotencyKey?: Uint8Array): Promise<FsWorkspace> {
      requireWorkspaceName(destination);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
      return adaptWorkspace(await raw.fork(destination, idempotencyKey));
    },
    async forkAt(destination: string, generation: FsGeneration): Promise<FsWorkspace> {
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
    async joinInto(target, options): Promise<FsJoinPlan> {
      validateJoinOptions(options);
      return adaptJoinPlan(await raw.joinInto(rawWorkspace(target), options));
    },
  };
  workspaceHandles.set(workspace, raw);
  return workspace;
}

function rawWorkspace(workspace: FsWorkspace): WasmRawWorkspace {
  const raw = workspaceHandles.get(workspace);
  if (raw === undefined) throw new TypeError("workspace belongs to another filesystem runtime");
  return raw;
}

const parseJoinResult = (value: WasmRawJoinResult): JoinResult => parseSharedJoinResult(value, decodeMergeConflict);
const parseWorkspaceRebaseResult = (value: WasmRawJoinResult): WorkspaceRebaseResult => parseSharedWorkspaceRebaseResult(value, decodeMergeConflict);
const adaptJoinPlan = (raw: WasmRawJoinPlan): FsJoinPlan => adaptJoinPlanBase(raw, parseJoinResult);

function requireWorkspaceName(name: string): void {
  if (name.length === 0) throw new RangeError("workspace name must be non-empty");
}

function copyBytes(value: Uint8Array): Uint8Array {
  return Uint8Array.from(value);
}

function copyOptionalBytes(value: Uint8Array | undefined): Uint8Array | undefined {
  return value === undefined ? undefined : copyBytes(value);
}

function requireIdentity(value: Uint8Array, label: string): void {
  if (value.byteLength !== 16) throw new RangeError(`${label} must be exactly 16 bytes`);
}

function requirePositiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
}
