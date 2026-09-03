import type {
  DirectoryBindingChange,
  FileRecordSnapshot,
  FsChangeSet,
  FsEngine,
  FsGeneration,
  FsJoinPlan,
  FsTransaction,
  FsWorkspace,
  GenerationDiff,
  MergeConflict,
  TreeEntrySnapshot,
  WasmRawFileRecordSnapshot,
  WasmRawFs,
  WasmRawGeneration,
  WasmRawChangeSet,
  WasmRawJoinPlan,
  WasmRawJoinResult,
  WasmRawMergeConflict,
  WorkspaceDirectoryPage,
  WorkspaceExtentPlan,
  WorkspaceName,
  WorkspaceStat,
  TransactionConflict,
  TransactionRebaseResult,
  WasmRawTransaction,
  WasmRawWorkspace,
  WasmRawGenerationDiff,
  WorkspaceCommit,
  WorkspaceDeleteStatus,
  WorkspaceRebaseOptions,
  WorkspaceRebaseResult,
  WorkspaceRebaseStatus,
  JoinOptions,
  JoinResult,
  JoinStatus,
} from "./contracts.js";

const generationHandles = new WeakMap<FsGeneration, WasmRawGeneration>();
const workspaceHandles = new WeakMap<FsWorkspace, WasmRawWorkspace>();
const changeSetHandles = new WeakMap<FsChangeSet, WasmRawChangeSet>();

export function adaptWasmFs(raw: WasmRawFs): FsEngine {
  const engine = {
    capabilities: raw.capabilities,
    async createWorkspace(name: string): Promise<FsWorkspace> {
      requireWorkspaceName(name);
      return adaptWorkspace(await raw.createWorkspace(name));
    },
    async openWorkspace(name: string): Promise<FsWorkspace> {
      requireWorkspaceName(name);
      return adaptWorkspace(await raw.openWorkspace(name));
    },
    close(): void {
      raw.close();
    },
  };
  return engine;
}

function adaptWorkspace(raw: WasmRawWorkspace): FsWorkspace {
  const workspace: FsWorkspace = {
    get name(): string {
      return raw.name;
    },
    get id(): Uint8Array {
      return copyBytes(raw.id);
    },
    async head(): Promise<Uint8Array> {
      return copyBytes(await raw.head());
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
    async read(path: string, maximumBytes: bigint): Promise<Uint8Array> {
      requirePositive(maximumBytes, "maximum read bytes");
      return copyBytes(await raw.read(path, maximumBytes));
    },
    async readRange(path, offset, length) { return copyBytes(await raw.readRange(path, offset, length)); },
    async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
    async listDirectory(path, after, maximumEntries) {
      return copyWorkspaceDirectoryPage(await raw.listDirectory(path, after, maximumEntries));
    },
    async readSymbolicLink(path) { return copyBytes(await raw.readSymbolicLink(path)); },
    async planExtents(path, offset, length, maximumSpans) {
      return copyWorkspaceExtentPlan(await raw.planExtents(path, offset, length, maximumSpans));
    },
    async write(path: string, bytes: Uint8Array): Promise<WorkspaceCommit> {
      return parseWorkspaceCommit(await raw.write(path, bytes));
    },
    async remove(path: string): Promise<WorkspaceCommit> {
      return parseWorkspaceCommit(await raw.remove(path));
    },
    async fork(destination: string): Promise<FsWorkspace> {
      requireWorkspaceName(destination);
      return adaptWorkspace(await raw.fork(destination));
    },
    async forkAt(destination: string, generation: FsGeneration): Promise<FsWorkspace> {
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
  };
  workspaceHandles.set(workspace, raw);
  return workspace;
}

function rawWorkspace(workspace: FsWorkspace): WasmRawWorkspace {
  const raw = workspaceHandles.get(workspace);
  if (raw === undefined) throw new TypeError("workspace belongs to another filesystem runtime");
  return raw;
}

function rawGeneration(generation: FsGeneration): WasmRawGeneration {
  const raw = generationHandles.get(generation);
  if (raw === undefined) throw new TypeError("generation belongs to another filesystem runtime");
  return raw;
}

function adaptGeneration(raw: WasmRawGeneration): FsGeneration {
  const generation: FsGeneration = {
    get id(): Uint8Array {
      return copyBytes(raw.id);
    },
    get workspaceId(): Uint8Array {
      return copyBytes(raw.workspaceId);
    },
    async read(path, maximumBytes) {
      requirePositive(maximumBytes, "maximum read bytes");
      return copyBytes(await raw.read(path, maximumBytes));
    },
    async readRange(path, offset, length) { return copyBytes(await raw.readRange(path, offset, length)); },
    async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
    async listDirectory(path, after, maximumEntries) {
      return copyWorkspaceDirectoryPage(await raw.listDirectory(path, after, maximumEntries));
    },
    async readSymbolicLink(path) { return copyBytes(await raw.readSymbolicLink(path)); },
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

function adaptChangeSet(raw: WasmRawChangeSet): FsChangeSet {
  const changeSet: FsChangeSet = {
    get from(): FsGeneration {
      return adaptGeneration(raw.from);
    },
    get to(): FsGeneration {
      return adaptGeneration(raw.to);
    },
    changes(): GenerationDiff {
      return generationDiff(raw.changes());
    },
    async compose(next, maximumChanges): Promise<FsChangeSet> {
      requirePositiveInteger(maximumChanges, "maximum changes");
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

function adaptJoinPlan(raw: WasmRawJoinPlan): FsJoinPlan {
  return {
    get targetHead(): Uint8Array {
      return copyBytes(raw.targetHead);
    },
    get commonAncestor(): Uint8Array {
      return copyBytes(raw.commonAncestor);
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

function parseWorkspaceRebaseResult(value: WasmRawJoinResult): WorkspaceRebaseResult {
  const { status } = value;
  if (
    status !== "rebased" && status !== "already-rebased" && status !== "current" &&
    status !== "stale" && status !== "conflicted" && status !== "fenced" &&
    status !== "idempotency-conflict"
  ) throw new TypeError("workspace rebase result has an invalid status");
  const typedStatus: WorkspaceRebaseStatus = status;
  return {
    status: typedStatus,
    generationId: value.generationId === undefined ? undefined : copyBytes(value.generationId),
    conflicts: value.conflicts.map(decodeMergeConflict),
    truncated: value.truncated,
  };
}

function parseJoinResult(value: WasmRawJoinResult): JoinResult {
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
    generationId: value.generationId === undefined ? undefined : copyBytes(value.generationId),
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

function adaptTransaction(raw: WasmRawTransaction): FsTransaction {
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
      return parseWorkspaceCommit(await raw.commit());
    },
    async close(): Promise<void> {},
  };
}

function requireWorkspaceName(name: string): void {
  if (name.length === 0) throw new RangeError("workspace name must be non-empty");
}

function parseWorkspaceCommit(value: unknown): WorkspaceCommit {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("workspace commit must be an object");
  }
  const candidate = value as { readonly status?: unknown; readonly generationId?: unknown };
  const status = candidate.status;
  if (
    status !== "committed" &&
    status !== "already-committed" &&
    status !== "conflict" &&
    status !== "fenced" &&
    status !== "idempotency-conflict"
  ) {
    throw new TypeError("workspace commit has an invalid status");
  }
  const generationId = candidate.generationId;
  if (generationId !== undefined && !(generationId instanceof Uint8Array)) {
    throw new TypeError("workspace commit has an invalid generation identity");
  }
  return { status, generationId: generationId === undefined ? undefined : copyBytes(generationId) };
}

function copyBytes(value: Uint8Array): Uint8Array {
  return Uint8Array.from(value);
}

function copyWorkspaceName(value: WorkspaceName): WorkspaceName {
  return { encoding: value.encoding, bytes: copyBytes(value.bytes) };
}

function copyWorkspaceStat(value: WorkspaceStat): WorkspaceStat {
  const metadata = value.metadata;
  return {
    ...value,
    fileId: copyBytes(value.fileId),
    linkCount: BigInt(value.linkCount),
    logicalBytes: value.logicalBytes === undefined ? undefined : BigInt(value.logicalBytes),
    metadata: {
      ...metadata,
      posixFlags: metadata.posixFlags === undefined ? undefined : BigInt(metadata.posixFlags),
      createdNs: metadata.createdNs === undefined ? undefined : BigInt(metadata.createdNs),
      modifiedNs: metadata.modifiedNs === undefined ? undefined : BigInt(metadata.modifiedNs),
      accessedNs: metadata.accessedNs === undefined ? undefined : BigInt(metadata.accessedNs),
      changedNs: metadata.changedNs === undefined ? undefined : BigInt(metadata.changedNs),
    },
  };
}

function copyWorkspaceDirectoryPage(value: WorkspaceDirectoryPage): WorkspaceDirectoryPage {
  return {
    hasMore: value.hasMore,
    entries: value.entries.map((entry) => ({
      name: copyWorkspaceName(entry.name), fileId: copyBytes(entry.fileId), kind: entry.kind,
    })),
  };
}

function copyWorkspaceExtentPlan(value: WorkspaceExtentPlan): WorkspaceExtentPlan {
  return {
    spans: value.spans.map((span) => ({
      ...span,
      offset: BigInt(span.offset),
      length: BigInt(span.length),
      sourceEnd: BigInt(span.sourceEnd),
    })),
  };
}

function copyTransactionConflict(value: TransactionConflict): TransactionConflict {
  return {
    ...value,
    fileId: value.fileId === undefined ? undefined : copyBytes(value.fileId),
    directoryId: value.directoryId === undefined ? undefined : copyBytes(value.directoryId),
    offset: value.offset === undefined ? undefined : BigInt(value.offset),
    length: value.length === undefined ? undefined : BigInt(value.length),
    name: value.name === undefined ? undefined : copyWorkspaceName(value.name),
    expected: value.expected === undefined ? undefined : copyBytes(value.expected),
    actual: value.actual === undefined ? undefined : copyBytes(value.actual),
  };
}

function copyTransactionRebase(value: TransactionRebaseResult): TransactionRebaseResult {
  if (value.status !== "rebased" && value.status !== "conflicted") {
    throw new TypeError("transaction rebase has an invalid status");
  }
  return {
    status: value.status,
    generationId: value.generationId === undefined ? undefined : copyBytes(value.generationId),
    conflicts: value.conflicts.map(copyTransactionConflict),
    truncated: value.truncated,
  };
}

function copyOptionalBytes(value: Uint8Array | undefined): Uint8Array | undefined {
  return value === undefined ? undefined : copyBytes(value);
}

function copyFileRecord(record: WasmRawFileRecordSnapshot): FileRecordSnapshot {
  return {
    ...record,
    fileId: copyBytes(record.fileId),
    linkCount: BigInt(record.linkCount),
    metadataObject: copyBytes(record.metadataObject),
    logicalBytes: record.logicalBytes === undefined ? undefined : BigInt(record.logicalBytes),
    payloadObject: copyOptionalBytes(record.payloadObject),
    inlineBytes: copyOptionalBytes(record.inlineBytes),
  };
}

function copyPathComponent(component: TreeEntrySnapshot["name"]): TreeEntrySnapshot["name"] {
  return { ...component, bytes: copyBytes(component.bytes) };
}

function copyTreeEntry(entry: TreeEntrySnapshot): TreeEntrySnapshot {
  return {
    ...entry,
    name: copyPathComponent(entry.name),
    fileId: copyBytes(entry.fileId),
  };
}

function generationDiff(diff: WasmRawGenerationDiff): GenerationDiff {
  return {
    ...diff,
    files: diff.files.map((change) => ({
      ...change,
      fileId: copyBytes(change.fileId),
      before: change.before === undefined ? undefined : copyFileRecord(change.before),
      after: change.after === undefined ? undefined : copyFileRecord(change.after),
    })),
    bindings: diff.bindings.map(copyBindingChange),
  };
}

function decodeMergeConflict(raw: WasmRawMergeConflict): MergeConflict {
  if (
    raw.kind === "file" &&
    raw.fileId?.byteLength === 16 &&
    raw.directoryId === undefined &&
    raw.name === undefined
  ) {
    return { kind: "file", fileId: copyBytes(raw.fileId) };
  }
  if (
    raw.kind === "binding" &&
    raw.fileId === undefined &&
    raw.directoryId?.byteLength === 16 &&
    raw.name !== undefined
  ) {
    return {
      kind: "binding",
      directoryId: copyBytes(raw.directoryId),
      name: copyPathComponent(raw.name),
    };
  }
  throw new Error("WASM join returned a malformed conflict");
}

function copyBindingChange(change: DirectoryBindingChange): DirectoryBindingChange {
  return {
    ...change,
    directoryId: copyBytes(change.directoryId),
    name: copyPathComponent(change.name),
    before: change.before === undefined ? undefined : copyTreeEntry(change.before),
    after: change.after === undefined ? undefined : copyTreeEntry(change.after),
  };
}

function requireIdentity(value: Uint8Array, label: string): void {
  if (value.byteLength !== 16) throw new RangeError(`${label} must be exactly 16 bytes`);
}

function requireGenerationIdentity(value: Uint8Array, label: string): void {
  if (value.byteLength !== 32) {
    throw new RangeError(`${label} generation identity must be exactly 32 bytes`);
  }
}

function requireNonnegative(value: bigint, label: string): void {
  if (value < 0n) throw new RangeError(`${label} must be non-negative`);
}

function requirePositive(value: bigint, label: string): void {
  if (value <= 0n) throw new RangeError(`${label} must be positive`);
}

function requirePositiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
}
