import { copyBytes, copyOptionalBytes, ownBytes, requireIdentity, requireGenerationIdentity, requireWorkspaceName } from "./binding-values.js";
import type { FsChangeSet, FsVolumeEngine, FsVolume, FsCheckout, FsGeneration, FsJoinPlan, FsTransaction, FsWorkspace, NativeFsWorkspace, GenerationExportManifest, GenerationTransferBatch, GenerationTransferCursor, ObjectCacheStats, Speculation, SpeculationMetrics, ResolvedFile } from "./contracts.js";
import type { WasmRawGeneration, WasmRawChangeSet, WasmRawTransaction, WasmRawFs, WasmRawVolume, WasmRawCheckout, WasmRawSpeculation, WasmRawWorkspace, WasmRawGenerationDiff } from "./binding-types.js";

import { parseMergePreparation, parseWorkspaceCommit, parseWorkspaceDelete, parseWorkspaceRebaseResult } from "./workspace-results.js";
import { adaptOperationWindowCoordinator } from "./operation-windows.js";
import { copyBatchLookupEntries, copyDirectoryPage, copyDirectoryRecordPage, copyFileRecord,
  copyGenerationDiff, copyNamedAttributePage, copyNamedAttributeResult, copyStatResult,
  projectFileKind } from "./binding-results.js";
import { bigintRecord, copyWork, copyFileExtentPlan, copyCheckoutCommit, copyLiveMutation, copyLiveTransaction, copyTransactionResult, copyTransactionRebase, copyRebaseResult, copyWorkspaceStat, copyWorkspaceDirectoryPage, copyWorkspaceExtentPlan } from "./workspace-copies.js";
import { adaptJoinPlan } from "./workspace-operations.js";
import { observed, type AcyclicObserver } from "@acyclic-labs/actors/observe";

export function createFilesystemAdapter(extensions: {
  checkout?: (raw: WasmRawCheckout) => Partial<FsCheckout>;
  workspace?: (raw: WasmRawWorkspace, workspace: FsWorkspace) => Partial<NativeFsWorkspace>;
} = {}) {
  const generationHandles = new WeakMap<FsGeneration, WasmRawGeneration>();
  function adaptGeneration(raw: WasmRawGeneration): FsGeneration {
    const generation: FsGeneration = {
      get id() { return copyBytes(raw.id); },
      get workspaceId() { return copyBytes(raw.workspaceId); },
      async read(path, maximumBytes) {
        if (maximumBytes <= 0n) throw new RangeError("maximum read bytes must be positive");
        return ownBytes(await raw.read(path, maximumBytes));
      },
      async readRange(path, offset, length) { return ownBytes(await raw.readRange(path, offset, length)); },
      async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
      async listDirectory(path, after, maximumEntries) {
        return copyWorkspaceDirectoryPage(await raw.listDirectory(path, after, maximumEntries));
      },
      async readSymbolicLink(path) { return ownBytes(await raw.readSymbolicLink(path)); },
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
  function rawGeneration(generation: FsGeneration): WasmRawGeneration {
    const raw = generationHandles.get(generation);
    if (raw === undefined) throw new TypeError("generation belongs to another filesystem runtime");
    return raw;
  }
  const generationDiff = (value: WasmRawGenerationDiff) => copyGenerationDiff(value, copyWork(value.work));
  const workspaceHandles = new WeakMap<FsWorkspace, WasmRawWorkspace>();
  const fsHandles = new WeakMap<FsVolumeEngine, WasmRawFs>();
  const changeSetHandles = new WeakMap<FsChangeSet, WasmRawChangeSet>();
  function adaptChangeSet(raw: WasmRawChangeSet): FsChangeSet {
    const changeSet: FsChangeSet = {
      get from() { return adaptGeneration(raw.from); },
      get to() { return adaptGeneration(raw.to); },
      changes() { return generationDiff(raw.changes()); },
      async compose(next, maximumChanges) {
        return adaptChangeSet(await raw.compose(rawChangeSet(next), maximumChanges));
      },
    };
    changeSetHandles.set(changeSet, raw);
    return changeSet;
  }
  function rawChangeSet(changeSet: FsChangeSet): WasmRawChangeSet {
    const raw = changeSetHandles.get(changeSet);
    if (raw === undefined) throw new TypeError("change set belongs to another filesystem runtime");
    return raw;
  }

  function adaptFs(fs: WasmRawFs, o?: AcyclicObserver, capabilities = fs.capabilities): FsVolumeEngine {
    const raw = observeRaw(fs, o);
    const engine: FsVolumeEngine = {
      capabilities,
      async createWorkspace(name: string): Promise<FsWorkspace> {
        requireWorkspaceName(name);
        return adaptWorkspace(await raw.createWorkspace(name), o);
      },
      async openWorkspace(name: string): Promise<FsWorkspace> {
        requireWorkspaceName(name);
        return adaptWorkspace(await raw.openWorkspace(name), o);
      },
      objectCacheStats(): ObjectCacheStats { return objectCacheStats(raw.objectCacheStats()); },
      clearObjectCache(): void { raw.clearObjectCache(); },
      createSpeculation(volumeId, generationId, options): Speculation { return adaptSpeculation(raw.createSpeculation(volumeId, generationId, options)); },
      async createVolume(options): Promise<FsVolume> { return adaptVolume(await raw.createVolume(options), o); },
      async createVolumeWithId(volumeId, options): Promise<FsVolume> { return adaptVolume(await raw.createVolumeWithId(volumeId, options), o); },
      async openVolume(volumeId): Promise<FsVolume> { return adaptVolume(await raw.openVolume(volumeId), o); },
      async exportObject(objectId, maximumBytes) { return copyFileRead(await raw.exportObject(objectId, maximumBytes)); },
      async importObject(objectId, bytes) { return copyMutation(await raw.importObject(objectId, bytes)); },
      async exportGenerationBatch(manifest, cursor, maximumObjects, maximumObjectBytes): Promise<GenerationTransferBatch> {
        const value = await raw.exportGenerationBatch(manifest, cursor, maximumObjects, maximumObjectBytes);
        return { firstObject: BigInt(value.firstObject), nextObject: value.nextObject === undefined ? undefined : BigInt(value.nextObject), objects: value.objects.map(ownBytes), work: copyWork(value.work) };
      },
      async importGenerationBatch(manifest, cursor, objects, maximumObjects): Promise<GenerationTransferCursor> {
        const value = await raw.importGenerationBatch(manifest, cursor, objects, maximumObjects);
        return { nextObject: BigInt(value.nextObject), work: copyWork(value.work) };
      },
      async restoreVolume(manifest, operationId): Promise<FsVolume> { return adaptVolume(await raw.restoreVolume(manifest, operationId), o); },
      close(): void {
        raw.close();
      },
    };
    fsHandles.set(engine, raw);
    return engine;
  }

  /** Binds durable windows to the exact browser authority that publishes generations. */
  function adaptOperationWindows(filesystem: FsVolumeEngine) {
    const raw = fsHandles.get(filesystem);
    if (raw === undefined) throw new TypeError("operation windows require a browser filesystem opened by this module");
    return adaptOperationWindowCoordinator(raw.operationWindows(), rawWorkspace);
  }

  function adaptVolume(volume: WasmRawVolume, o: AcyclicObserver | undefined): FsVolume {
    const raw = observeRaw(volume, o);
    return {
      get id() { return copyBytes(raw.id); },
      get acquisitionWork() { return copyWork(raw.acquisitionWork); },
      async diffGenerations(before, after, maximumChanges) { return generationDiff(await raw.diffGenerations(before, after, maximumChanges)); },
      async checkout(options) { return adaptCheckout(await raw.checkout(options), o); },
    };
  }

  function adaptCheckout(checkout: WasmRawCheckout, o: AcyclicObserver | undefined): FsCheckout {
    const raw = observeRaw(checkout, o);
    return {
      ...extensions.checkout?.(checkout),
      get acquisitionWork() { return copyWork(raw.acquisitionWork); },
      async applyTransaction(operations) { const value = await raw.applyTransaction(Array.from(operations)); return copyTransactionResult(value, copyWork(value.work)); },
      async checkpoint() { return copyCheckpoint(await raw.checkpoint()); },
      async refreshHead() { return copyCheckpoint(await raw.refreshHead()); },
      async refreshLive() { return copyCheckpoint(await raw.refreshLive()); },
      async exportManifest(): Promise<GenerationExportManifest> { const value = await raw.exportManifest(); return { manifestBytes: copyBytes(value.manifestBytes), objects: value.objects.map(ownBytes), work: copyWork(value.work) }; },
      async prepareMerge(theirs, maximumChanges, maximumConflicts) {
        const value = await raw.prepareMerge(theirs, maximumChanges, maximumConflicts);
        return parseMergePreparation({ ...value, work: copyWork(value.work) });
      },
      async lookupNoFollow(path) { const value = await raw.lookupNoFollow(path); return { ...value, fileId: copyOptionalBytes(value.fileId), fileKind: value.fileKind === undefined ? undefined : projectFileKind(value.fileKind), work: copyWork(value.work) }; },
      async lookupBatchNoFollow(paths) { const value = await raw.lookupBatchNoFollow(Array.from(paths)); return { entries: copyBatchLookupEntries(value.entries), retainedAllocationBytes: BigInt(value.retainedAllocationBytes), work: copyWork(value.work) }; },
      async statNoFollow(path) { const value = await raw.statNoFollow(path); return copyStatResult(value, copyWork(value.work)); },
      async readFileRecordById(fileId) { const value = await raw.readFileRecordById(fileId); return { record: copyFileRecord(value.record), work: copyWork(value.work) }; },
      async readMetadata(path) { return copyMetadata(await raw.readMetadata(path)); },
      async readMetadataById(fileId) { return copyMetadata(await raw.readMetadataById(fileId)); },
      async setMetadata(path, canonicalBytes) { return copyMutation(await raw.setMetadata(path, canonicalBytes)); },
      async setMetadataById(fileId, canonicalBytes) { return copyMutation(await raw.setMetadataById(fileId, canonicalBytes)); },
      async setAttributes(path, canonicalBytes, logicalBytes) { return copyMutation(await raw.setAttributes(path, canonicalBytes, logicalBytes)); },
      async setAttributesById(fileId, canonicalBytes, logicalBytes) { return copyMutation(await raw.setAttributesById(fileId, canonicalBytes, logicalBytes)); },
      async readNamedAttribute(path, attributeClass, name) { const value = await raw.readNamedAttribute(path, attributeClass, name); return copyNamedAttributeResult(value, copyWork(value.work)); },
      async listNamedAttributes(path, after, maximumEntries) { const value = await raw.listNamedAttributes(path, after?.attributeClass, after?.name, maximumEntries); return copyNamedAttributePage(value, copyWork(value.work)); },
      async writeNamedAttribute(path, attributeClass, name, bytes, mode) { return copyMutation(await raw.writeNamedAttribute(path, attributeClass, name, bytes, mode)); },
      async removeNamedAttribute(path, attributeClass, name) { return copyMutation(await raw.removeNamedAttribute(path, attributeClass, name)); },
      async resolveFiles(paths) {
        const value = await raw.resolveFiles(Array.from(paths));
        const files = Array.from({ length: value.length }, (_, index) => {
          const file = value.take(index);
          return file === undefined ? undefined : adaptResolvedFile(file);
        });
        return { files, work: copyWork(value.work) };
      },
      async readFileRange(path, offset, length) { return copyFileRead(await raw.readFileRange(path, offset, length)); },
      async readFileRangeById(fileId, offset, length) { return copyFileRead(await raw.readFileRangeById(fileId, offset, length)); },
      async planFileExtents(path, offset, length, maximumSpans) { return fileExtentPlan(await raw.planFileExtents(path, offset, length, maximumSpans)); },
      async planFileExtentsById(fileId, offset, length, maximumSpans) { return fileExtentPlan(await raw.planFileExtentsById(fileId, offset, length, maximumSpans)); },
      async seekFileExtent(path, offset, target) { const value = await raw.seekFileExtent(path, offset, target); return { offset: value.offset === undefined ? undefined : BigInt(value.offset), work: copyWork(value.work) }; },
      async seekFileExtentById(fileId, offset, target) { const value = await raw.seekFileExtentById(fileId, offset, target); return { offset: value.offset === undefined ? undefined : BigInt(value.offset), work: copyWork(value.work) }; },
      async readSymbolicLink(path) { return copyFileRead(await raw.readSymbolicLink(path)); },
      async readReparsePoint(path) { return copyFileRead(await raw.readReparsePoint(path)); },
      async listDirectory(path, after, maximumEntries) { const value = await raw.listDirectory(path, after, maximumEntries); return copyDirectoryPage(value, copyWork(value.work)); },
      async listDirectoryRecords(path, after, maximumEntries) { const value = await raw.listDirectoryRecords(path, after, maximumEntries); return copyDirectoryRecordPage(value, copyWork(value.work)); },
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
      async commit(operationId, lease) { const value = await raw.commit(operationId, lease); return copyCheckoutCommit(value, copyWork(value.work)); },
      async mutateLive(operations, operationId, maximumAttempts, maximumConflicts) { const value = await raw.mutateLive(Array.from(operations), operationId, maximumAttempts, maximumConflicts); return copyLiveTransaction(value, copyWork(value.work)); },
      async resumeLive(operationId, maximumAttempts, maximumConflicts) { const value = await raw.resumeLive(operationId, maximumAttempts, maximumConflicts); return copyLiveMutation(value, copyWork(value.work)); },
      async rebaseHead(maximumConflicts) { const value = await raw.rebaseHead(maximumConflicts); return copyRebaseResult(value, copyWork(value.work)); },
      async discard() { return copyMutation(await raw.discard()); },
    };
  }

  function adaptResolvedFile(raw: import("./binding-types.js").WasmRawResolvedFile): ResolvedFile {
    return {
      kind: projectFileKind(raw.kind),
      logicalBytes: raw.logicalBytes,
      metadataCanonicalBytes: Uint8Array.from(raw.metadataCanonicalBytes),
      async readRange(offset, length) { return copyFileRead(await raw.readRange(offset, length)); },
      async readSymbolicLink() { return copyFileRead(await raw.readSymbolicLink()); },
    };
  }

  function adaptSpeculation(raw: WasmRawSpeculation): Speculation {
    return {
      async observe(value) {
        const result = await raw.observe(value);
        return result.rejection === undefined
          ? { status: result.status }
          : { status: result.status, rejection: result.rejection };
      },
      async executeResidency(operationId) { const value = await raw.executeResidency(operationId); return { objectBytes: BigInt(value.objectBytes), work: copyWork(value.work) }; },
      async finishResidency(operationId, useful) { await raw.finishResidency(operationId, useful); },
      async planPromotion(request) {
        const value = await raw.planPromotion(request);
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
      async finishPromotion(operationId, useful) { await raw.finishPromotion(operationId, useful); },
      async preemptForForeground(bytes) { return await raw.preemptForForeground(bytes); },
      async replaceGeneration(generationId) { return await raw.replaceGeneration(generationId); },
      async metrics(): Promise<SpeculationMetrics> {
        const value = await raw.metrics();
        return {
          residency: bigintRecord(value.residency ?? {}),
          promotion: bigintRecord(value.promotion ?? {}),
        };
      },
      cancel() { raw.cancel(); },
    };
  }

  function objectCacheStats(value: import("./binding-types.js").WasmRawObjectCacheStats): ObjectCacheStats { return { hits: BigInt(value.hits), decodedHits: BigInt(value.decodedHits), misses: BigInt(value.misses), coalescedReads: BigInt(value.coalescedReads), evictions: BigInt(value.evictions), residentEntries: BigInt(value.residentEntries), residentBytes: BigInt(value.residentBytes), residentCanonicalObjects: BigInt(value.residentCanonicalObjects), residentCanonicalBytes: BigInt(value.residentCanonicalBytes), residentDecodedPages: BigInt(value.residentDecodedPages), residentDecodedBytes: BigInt(value.residentDecodedBytes), inFlight: BigInt(value.inFlight) }; }
  function copyCheckpoint(value: Awaited<ReturnType<WasmRawCheckout["checkpoint"]>>): import("./contracts.js").CheckpointResult { return { generationId: copyBytes(value.generationId), work: copyWork(value.work) }; }
  function copyMetadata(value: Awaited<ReturnType<WasmRawCheckout["readMetadata"]>>): import("./contracts.js").MetadataResult { return { canonicalBytes: copyBytes(value.canonicalBytes), work: copyWork(value.work) }; }
  function fileExtentPlan(value: Awaited<ReturnType<WasmRawCheckout["planFileExtents"]>>) {
    return copyFileExtentPlan(value, copyWork(value.work));
  }
  function copyFileRead(value: Awaited<ReturnType<WasmRawCheckout["readFileRange"]>>): import("./contracts.js").FileReadResult { return { bytes: ownBytes(value.bytes), work: copyWork(value.work) }; }
  function copyMutation(value: Awaited<ReturnType<WasmRawCheckout["createFile"]>>): import("./contracts.js").MutationResult { return { fileId: copyOptionalBytes(value.fileId), work: copyWork(value.work) }; }

  function adaptWorkspace(handle: WasmRawWorkspace, o: AcyclicObserver | undefined): FsWorkspace {
    const raw = observeRaw(handle, o);
    const workspace: FsWorkspace = {
      get name() { return raw.name; },
      get id() { return copyBytes(raw.id); },
      async head() { return Uint8Array.from(await raw.head()); },
      async generation(id) {
        requireGenerationIdentity(id, "generation identity");
        return adaptGeneration(await raw.generation(Uint8Array.from(id)));
      },
      async sync() { return adaptGeneration(await raw.sync()); },
      async checkpoint(label) {
        return adaptGeneration(await raw.checkpoint(label));
      },
      async pin(identity) {
        return adaptGeneration(await raw.pin(identity));
      },
      async delete(idempotencyKey) {
        return parseWorkspaceDelete(await raw.delete(idempotencyKey));
      },
      async read(path, maximumBytes) {
        if (maximumBytes <= 0n) throw new RangeError("maximum read bytes must be positive");
        return Uint8Array.from(await raw.read(path, maximumBytes));
      },
      async readRange(path, offset, length) { return Uint8Array.from(await raw.readRange(path, offset, length)); },
      async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
      async readSymbolicLink(path) { return Uint8Array.from(await raw.readSymbolicLink(path)); },
      async planExtents(path, offset, length, maximumSpans) {
        return copyWorkspaceExtentPlan(await raw.planExtents(path, offset, length, maximumSpans));
      },
      async write(path, bytes) { return parseWorkspaceCommit(await raw.write(path, bytes)); },
      async remove(path) { return parseWorkspaceCommit(await raw.remove(path)); },
      async liveRebase(options, idempotencyKey) {
        return parseWorkspaceRebaseResult(await raw.liveRebase(
          idempotencyKey, options.maximumGenerations, options.maximumChanges, options.maximumConflicts,
        ));
      },
      async fork(destination: string, idempotencyKey?: Uint8Array): Promise<FsWorkspace> {
        requireWorkspaceName(destination);
        if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
        return adaptWorkspace(await raw.fork(destination, idempotencyKey), o);
      },
      async forkAt(destination: string, generation: FsGeneration, options: import("./contracts.js").WorkspaceForkOptions = {}): Promise<FsWorkspace> {
        requireWorkspaceName(destination);
        return adaptWorkspace(await raw.forkAt(destination, rawGeneration(generation), {
          ...(options.idempotencyKey === undefined ? {} : { idempotencyKey: options.idempotencyKey }),
          ...(options.paths === undefined ? {} : { paths: [...options.paths] }),
        }), o);
      },
      async beginTransaction(idempotencyKey?: Uint8Array): Promise<FsTransaction> {
        if (idempotencyKey !== undefined) requireIdentity(idempotencyKey, "idempotency key");
        return adaptTransaction(await raw.beginTransaction(idempotencyKey));
      },
      async diff(from, to, maximumChanges): Promise<FsChangeSet> {
        if (!Number.isSafeInteger(maximumChanges) || maximumChanges <= 0) {
          throw new RangeError("maximum changes must be a positive safe integer");
        }
        return adaptChangeSet(
          await raw.diff(rawGeneration(from), rawGeneration(to), maximumChanges),
        );
      },
      async joinInto(target, options): Promise<FsJoinPlan> {
        return adaptJoinPlan(await raw.joinInto(rawWorkspace(target), options));
      },
    };
    Object.assign(workspace, extensions.workspace?.(handle, workspace));
    workspaceHandles.set(workspace, handle);
    return workspace;
  }

  function observeRaw<T extends object>(raw: T, observer: AcyclicObserver | undefined): T {
    if (observer === undefined) return raw;
    return new Proxy(raw, {
      get(target, key) {
        const value: unknown = Reflect.get(target, key, target);
        if (typeof value !== "function" || typeof key !== "string") return value;
        return (...args: unknown[]) => {
          const result: unknown = value.apply(target, args);
          return result instanceof Promise ? observed(observer, "fs", key, () => result) : result;
        };
      },
    });
  }

  function rawWorkspace(workspace: FsWorkspace): WasmRawWorkspace {
    const raw = workspaceHandles.get(workspace);
    if (raw === undefined) throw new TypeError("workspace belongs to another filesystem runtime");
    return raw;
  }

  return { adaptFs, adaptWorkspace, adaptGeneration, rawWorkspace, adaptOperationWindows };
}

export const { adaptFs, adaptOperationWindows } = createFilesystemAdapter();

function adaptTransaction(raw: WasmRawTransaction): FsTransaction {
  return {
    createDirAll(path) { return raw.createDirAll(path); },
    createDirectory(path) { return raw.createDirectory(path); },
    createSymbolicLink(path, target) { return raw.createSymbolicLink(path, target); },
    write(path, bytes) { return raw.write(path, bytes); },
    remove(path) { return raw.remove(path); },
    copy(source, destination) { return raw.copy(source, destination); },
    rename(source, destination) { return raw.rename(source, destination); },
    hardLink(source, destination) { return raw.hardLink(source, destination); },
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
      return copyTransactionRebase(await raw.rebase(maximumConflicts));
    },
    async commit(lease) { return parseWorkspaceCommit(await raw.commit(lease)); },
    async close(): Promise<void> {},
  };
}

function requireNonnegative(value: bigint, label: string): void {
  if (value < 0n) throw new RangeError(`${label} must be non-negative`);
}
