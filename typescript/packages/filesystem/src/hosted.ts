import { create } from "@bufbuild/protobuf";
import { Code, ConnectError, createClient, type Client, type Interceptor } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";

import {
  ConflictUse,
  ExtentKind,
  FileKind,
  FilesystemProfile,
  FilesystemService,
  JoinHistory,
  JoinStatus as WireJoinStatus,
  MutationSchema,
  MutationStatus,
  NameEncoding,
  OperationOptionsSchema,
  RebaseStatus,
  SparseTarget,
  type Conflict as WireConflict,
  type DiffResponse,
  type FileRecordSnapshot as WireFileRecordSnapshot,
  type GenerationRef as WireGenerationRef,
  type JoinPlan as WireJoinPlan,
  type LogicalName as WireLogicalName,
  type Metadata as WireMetadata,
  type Mutation as WireMutation,
  type OptionalI64,
  type OptionalU32,
  type OptionalU64,
  type TreeEntrySnapshot as WireTreeEntrySnapshot,
  type WorkCounters as WireWorkCounters,
  type Workspace as WireWorkspace,
  type WorkspaceRef as WireWorkspaceRef,
} from "../generated/proto/filesystem/v2/filesystem_pb.js";
import type {
  DirectoryBindingChange,
  EngineCapabilities,
  FileRecordChange,
  FileRecordSnapshot,
  FsChangeSet,
  FsEngine,
  FsGeneration,
  FsJoinPlan,
  FsTransaction,
  FsWorkspace,
  GenerationDiff,
  HostedFsOptions,
  JoinOptions,
  JoinResult,
  JoinStatus,
  MergeConflict,
  TransactionConflict,
  TransactionRebaseResult,
  TreeEntrySnapshot,
  WorkCounters,
  WorkspaceCommit,
  WorkspaceCommitStatus,
  WorkspaceDeleteStatus,
  WorkspaceDirectoryPage,
  WorkspaceExtentPlan,
  WorkspaceFileKind,
  WorkspaceMetadata,
  WorkspaceName,
  WorkspaceRebaseOptions,
  WorkspaceRebaseResult,
  WorkspaceRebaseStatus,
  WorkspaceStat,
} from "./contracts.js";

export type * from "./public-types.js";

const DEFAULT_MAXIMUM_RESPONSE_BYTES = 24 * 1024 * 1024;
const DEFAULT_MAXIMUM_CONFLICTS = 1_024;

export class HostedFsError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = "HostedFsError";
  }
}

interface HostedClient {
  readonly rpc: Client<typeof FilesystemService>;
  readonly maximumResponseBytes: number;
  readonly maximumTransactionMutations: number;
  closed: boolean;
}

export async function openHostedFs(options: HostedFsOptions): Promise<FsEngine> {
  const endpoint = new URL(options.endpoint);
  if (endpoint.protocol !== "https:" && endpoint.protocol !== "http:") {
    throw new RangeError("hosted filesystem endpoint must use HTTP or HTTPS");
  }
  if (options.bearerToken.length === 0) throw new RangeError("bearer token must be non-empty");
  const maximumResponseBytes = options.maximumResponseBytes ?? DEFAULT_MAXIMUM_RESPONSE_BYTES;
  positiveSafeInteger(maximumResponseBytes, "maximum response bytes");
  const send = options.fetch ?? globalThis.fetch;
  if (send === undefined) throw new TypeError("this runtime does not provide fetch");
  const authorize: Interceptor = (next) => async (request) => {
    request.header.set("authorization", `Bearer ${options.bearerToken}`);
    return next(request);
  };
  const rpcClient = createClient(FilesystemService, createGrpcWebTransport({
    baseUrl: endpoint.href.replace(/\/$/, ""),
    interceptors: [authorize],
    fetch: boundedFetch(send, maximumResponseBytes),
  }));
  const handshake = await call(rpcClient.handshake({}));
  const advertised = required(handshake.capabilities, "filesystem capabilities");
  const client: HostedClient = {
    rpc: rpcClient,
    maximumResponseBytes,
    maximumTransactionMutations: advertised.maximumTransactionMutations,
    closed: false,
  };
  const capabilities: EngineCapabilities = {
    version: advertised.contractVersion,
    platform: "hosted",
    architecture: "service",
    authority: "remote",
    immutableObjects: "remote",
    nativeMount: "none",
    writableNativeMount: false,
    nativeWatch: false,
    nativeWatchBackend: "none",
    nativeWatchPersistentRestart: false,
    nativeWatchRootIdentityFencing: false,
    providerProcessIoObservable: false,
  };
  return {
    capabilities,
    async createWorkspace(name) {
      assertOpen(client);
      requireName(name);
      const response = await call(client.rpc.createWorkspace({
        name,
        profile: FilesystemProfile.PORTABLE,
        operation: operation(),
      }));
      return workspace(client, required(response.workspace, "created workspace"));
    },
    async openWorkspace(name) {
      assertOpen(client);
      requireName(name);
      const response = await call(client.rpc.openWorkspace({
        selector: { case: "name", value: name },
      }));
      return workspace(client, required(response.workspace, "opened workspace"));
    },
    close() { client.closed = true; },
  };
}

type WorkspaceOwner = { readonly client: HostedClient; readonly reference: WireWorkspaceRef };
type GenerationOwner = { readonly client: HostedClient; readonly reference: WireGenerationRef };
const workspaceOwners = new WeakMap<FsWorkspace, WorkspaceOwner>();
const generationOwners = new WeakMap<FsGeneration, GenerationOwner>();
const changeSetOwners = new WeakMap<FsChangeSet, {
  readonly client: HostedClient;
  readonly workspaceId: Uint8Array;
  readonly from: WireGenerationRef;
  readonly to: WireGenerationRef;
}>();

function workspace(client: HostedClient, value: WireWorkspace): FsWorkspace {
  const reference = required(value.workspace, "workspace reference");
  requireBytes(reference.workspaceId, 16, "workspace identity");
  requireName(reference.name);
  const result: FsWorkspace = {
    name: reference.name,
    id: reference.workspaceId.slice(),
    async head() { return (await currentGeneration(client, reference)).generationId.slice(); },
    async sync() { return generation(client, await currentGeneration(client, reference)); },
    async checkpoint(label) {
      requireName(label);
      const response = await call(client.rpc.checkpoint({
        generation: await currentGeneration(client, reference),
        identity: label,
        operation: operation(),
      }));
      return generation(client, required(response.generation, "checkpoint generation"));
    },
    async pin(identity) {
      requireName(identity);
      const response = await call(client.rpc.pin({
        generation: await currentGeneration(client, reference),
        identity,
        operation: operation(),
      }));
      return generation(client, required(response.generation, "pinned generation"));
    },
    async delete(idempotencyKey) {
      const response = await call(client.rpc.deleteWorkspace({
        workspace: reference,
        operation: operation(idempotencyKey),
      }));
      return deleteStatus(response.status);
    },
    async read(path, maximumBytes) {
      return read(client, await currentGeneration(client, reference), path, undefined, maximumBytes);
    },
    async readRange(path, offset, length) {
      return read(client, await currentGeneration(client, reference), path, { offset, length }, length);
    },
    async stat(path) { return stat(client, await currentGeneration(client, reference), path); },
    async listDirectory(path, after, maximumEntries) {
      return list(client, await currentGeneration(client, reference), path, after, maximumEntries);
    },
    async readSymbolicLink(path) {
      return readLink(client, await currentGeneration(client, reference), path);
    },
    async planExtents(path, offset, length, maximumSpans) {
      return extents(client, await currentGeneration(client, reference), path, offset, length, maximumSpans);
    },
    async write(path, bytes) {
      const tx = transaction(client, await currentGeneration(client, reference), undefined);
      await tx.write(path, bytes);
      return tx.commit();
    },
    async remove(path) {
      const tx = transaction(client, await currentGeneration(client, reference), undefined);
      await tx.remove(path);
      return tx.commit();
    },
    async fork(destination) {
      return fork(client, await currentGeneration(client, reference), destination);
    },
    async forkAt(destination, selected) {
      return fork(client, requireGeneration(selected, client, reference.workspaceId), destination);
    },
    async beginTransaction(idempotencyKey) {
      return transaction(client, await currentGeneration(client, reference), idempotencyKey);
    },
    async liveRebase(options, idempotencyKey) {
      validateRebase(options);
      const response = await call(client.rpc.rebase({
        workspace: reference,
        maximumGenerations: options.maximumGenerations,
        maximumChanges: options.maximumChanges,
        maximumConflicts: options.maximumConflicts,
        operation: operation(idempotencyKey),
      }));
      return workspaceRebase(response.status, response.generation, response.conflicts, response.truncated);
    },
    async diff(from, to, maximumChanges) {
      positiveU32(maximumChanges, "maximum changes");
      return diff(
        client,
        reference.workspaceId,
        requireGeneration(from, client, reference.workspaceId),
        requireGeneration(to, client, reference.workspaceId),
        maximumChanges,
      );
    },
    async joinInto(target, options) {
      validateJoin(options);
      const destination = requireWorkspace(target, client);
      const plan = await call(client.rpc.planJoin({
        source: await currentGeneration(client, reference),
        target: await currentGeneration(client, destination.reference),
        maximumChanges: options.maximumChanges,
        maximumConflicts: options.maximumConflicts,
        maximumGenerations: options.maximumGenerations,
        history: joinHistory(options.history),
      }));
      return joinPlan(client, plan);
    },
  };
  workspaceOwners.set(result, { client, reference });
  return result;
}

function generation(client: HostedClient, reference: WireGenerationRef): FsGeneration {
  const owner = required(reference.workspace, "generation workspace");
  requireBytes(reference.generationId, 32, "generation identity");
  const result: FsGeneration = {
    id: reference.generationId.slice(),
    workspaceId: owner.workspaceId.slice(),
    read: (path, maximumBytes) => read(client, reference, path, undefined, maximumBytes),
    readRange: (path, offset, length) => read(client, reference, path, { offset, length }, length),
    stat: (path) => stat(client, reference, path),
    listDirectory: (path, after, maximumEntries) => list(client, reference, path, after, maximumEntries),
    readSymbolicLink: (path) => readLink(client, reference, path),
    planExtents: (path, offset, length, maximumSpans) =>
      extents(client, reference, path, offset, length, maximumSpans),
    async pin(identity) {
      requireName(identity);
      const response = await call(client.rpc.pin({
        generation: reference,
        identity,
        operation: operation(),
      }));
      return generation(client, required(response.generation, "pinned generation"));
    },
  };
  generationOwners.set(result, { client, reference });
  return result;
}

async function currentGeneration(client: HostedClient, workspaceRef: WireWorkspaceRef): Promise<WireGenerationRef> {
  assertOpen(client);
  const response = await call(client.rpc.getHead({ workspace: workspaceRef }));
  return required(response.generation, "workspace head");
}

async function fork(client: HostedClient, source: WireGenerationRef, destinationName: string): Promise<FsWorkspace> {
  requireName(destinationName);
  const response = await call(client.rpc.forkWorkspace({
    source,
    destinationName,
    operation: operation(),
  }));
  return workspace(client, required(response.workspace, "forked workspace"));
}

async function read(
  client: HostedClient,
  selected: WireGenerationRef,
  path: string,
  range: { readonly offset: bigint; readonly length: bigint } | undefined,
  maximumBytes: bigint,
): Promise<Uint8Array> {
  assertOpen(client);
  if (range === undefined) positiveU64(maximumBytes, "maximum read bytes");
  else nonnegativeU64(maximumBytes, "maximum read bytes");
  if (range !== undefined) {
    nonnegativeU64(range.offset, "read offset");
    nonnegativeU64(range.length, "read length");
  }
  const response = await call(client.rpc.read({
    generation: selected,
    path,
    maximumBytes,
    ...(range === undefined ? {} : { range }),
  }));
  return response.contents.slice();
}

async function stat(client: HostedClient, selected: WireGenerationRef, path: string): Promise<WorkspaceStat> {
  assertOpen(client);
  const value = required((await call(client.rpc.stat({ generation: selected, path }))).stat, "file stat");
  return {
    fileId: exactBytes(value.fileId, 16, "file identity"),
    kind: fileKind(value.kind),
    linkCount: value.linkCount,
    logicalBytes: optionalU64(value.logicalBytes),
    metadata: metadata(value.metadata),
  };
}

async function list(
  client: HostedClient,
  selected: WireGenerationRef,
  path: string,
  after: WorkspaceName | undefined,
  maximumEntries: number,
): Promise<WorkspaceDirectoryPage> {
  assertOpen(client);
  positiveU32(maximumEntries, "maximum entries");
  const page = required((await call(client.rpc.listDirectory({
    generation: selected,
    path,
    page: { maximumItems: maximumEntries, ...(after === undefined ? {} : { after: wireName(after) }) },
  }))).page, "directory page");
  return {
    entries: page.entries.map((entry) => {
      const value = required(entry.stat, "directory entry stat");
      return {
        name: logicalName(required(entry.name, "directory entry name")),
        fileId: exactBytes(value.fileId, 16, "file identity"),
        kind: fileKind(value.kind),
      };
    }),
    hasMore: page.next !== undefined,
  };
}

async function readLink(client: HostedClient, selected: WireGenerationRef, path: string): Promise<Uint8Array> {
  assertOpen(client);
  const response = await call(client.rpc.readLink({
    generation: selected,
    path,
    maximumBytes: BigInt(client.maximumResponseBytes),
  }));
  return response.contents.slice();
}

async function extents(
  client: HostedClient,
  selected: WireGenerationRef,
  path: string,
  offset: bigint,
  length: bigint,
  maximumSpans: number,
): Promise<WorkspaceExtentPlan> {
  assertOpen(client);
  nonnegativeU64(offset, "extent offset");
  nonnegativeU64(length, "extent length");
  positiveU32(maximumSpans, "maximum spans");
  const response = await call(client.rpc.planExtents({
    generation: selected,
    path,
    range: { offset, length },
    maximumExtents: maximumSpans,
  }));
  return { spans: response.extents.map((extent) => {
    const range = required(extent.range, "extent range");
    return {
      offset: range.offset,
      length: range.length,
      sourceEnd: range.offset + range.length,
      kind: extentKind(extent.kind),
    };
  }) };
}

function transaction(
  client: HostedClient,
  initialBase: WireGenerationRef,
  suppliedIdempotencyKey: Uint8Array | undefined,
): FsTransaction {
  let base = initialBase;
  const operationOptions = operation(suppliedIdempotencyKey);
  const mutations: WireMutation[] = [];
  let closed = false;
  const stage = (value: WireMutation): Promise<void> => {
    assertOpen(client);
    if (closed) throw new HostedFsError("closed", "transaction is closed");
    if (mutations.length >= client.maximumTransactionMutations) {
      throw new RangeError("transaction exceeds the advertised mutation bound");
    }
    mutations.push(value);
    return Promise.resolve();
  };
  return {
    createDirAll: (path) => stage(mutation("createDirectories", { path })),
    createDirectory: (path) => stage(mutation("createDirectory", { path })),
    createSymbolicLink: (path, target) => stage(mutation("createSymbolicLink", { path, target })),
    write: (path, bytes) => stage(mutation("putFile", { path, contents: bytes })),
    remove: (path) => stage(mutation("remove", { path })),
    copy: (source, destination) => stage(mutation("copyFile", { source, destination })),
    rename: (source, destination) => stage(mutation("rename", { source, destination, replace: false })),
    hardLink: (source, destination) => stage(mutation("hardLink", { source, destination })),
    writeRange(path, offset, bytes) {
      nonnegativeU64(offset, "write offset");
      return stage(mutation("write", { path, offset, contents: bytes }));
    },
    resize(path, logicalBytes) {
      nonnegativeU64(logicalBytes, "logical bytes");
      return stage(mutation("resize", { path, logicalBytes }));
    },
    zeroRange(path, offset, length, allocated, extend) {
      nonnegativeU64(offset, "zero offset");
      nonnegativeU64(length, "zero length");
      return stage(mutation("zeroRange", { path, range: { offset, length }, allocated, extend }));
    },
    preallocate(path, offset, length, keepSize) {
      nonnegativeU64(offset, "preallocation offset");
      nonnegativeU64(length, "preallocation length");
      return stage(mutation("preallocate", { path, range: { offset, length }, keepSize }));
    },
    cloneRange(source, sourceOffset, destination, destinationOffset, length) {
      nonnegativeU64(sourceOffset, "source offset");
      nonnegativeU64(destinationOffset, "destination offset");
      nonnegativeU64(length, "clone length");
      return stage(mutation("cloneRange", { source, sourceOffset, destination, destinationOffset, length }));
    },
    async rebase(maximumConflicts): Promise<TransactionRebaseResult> {
      positiveU32(maximumConflicts, "maximum conflicts");
      const response = await call(client.rpc.rebaseTransaction({
        base,
        mutations,
        maximumConflicts,
        operation: operationOptions,
      }));
      if (response.conflicts.length === 0) {
        base = required(response.base, "rebased transaction base");
        return { status: "rebased", generationId: base.generationId.slice(), conflicts: [], truncated: false };
      }
      return {
        status: "conflicted",
        generationId: undefined,
        conflicts: response.conflicts.map(transactionConflict),
        truncated: response.truncated,
      };
    },
    async commit() {
      if (closed) throw new HostedFsError("closed", "transaction is closed");
      const response = await call(client.rpc.applyTransaction({
        base,
        mutations,
        operation: operationOptions,
        maximumConflicts: DEFAULT_MAXIMUM_CONFLICTS,
      }));
      return commit(response.status, response.generation);
    },
    close() {
      closed = true;
      mutations.length = 0;
      return Promise.resolve();
    },
  };
}

type MutationCase = WireMutation["mutation"] extends { case: infer C } ? C : never;
function mutation(kind: Exclude<MutationCase, undefined>, value: Record<string, unknown>): WireMutation {
  return create(MutationSchema, { mutation: { case: kind, value } });
}

async function diff(
  client: HostedClient,
  workspaceId: Uint8Array,
  from: WireGenerationRef,
  to: WireGenerationRef,
  maximumChanges: number,
): Promise<FsChangeSet> {
  const response = await call(client.rpc.diff({ from, to, maximumChanges }));
  const semantic = generationDiff(response);
  const result: FsChangeSet = {
    from: generation(client, required(response.from, "diff base")),
    to: generation(client, required(response.to, "diff result")),
    changes: () => semantic,
    async compose(next, bound) {
      positiveU32(bound, "maximum changes");
      const owner = changeSetOwners.get(next);
      if (owner === undefined || owner.client !== client || !equalBytes(owner.workspaceId, workspaceId)
        || !equalBytes(owner.from.generationId, to.generationId)) {
        throw new TypeError("change sets are not contiguous in this hosted workspace");
      }
      return diff(client, workspaceId, from, owner.to, bound);
    },
  };
  changeSetOwners.set(result, { client, workspaceId, from, to });
  return result;
}

function joinPlan(client: HostedClient, plan: WireJoinPlan): FsJoinPlan {
  const target = required(plan.expectedTarget, "join target");
  const common = required(plan.commonAncestor, "join common ancestor");
  return {
    targetHead: exactBytes(target.generationId, 32, "target generation"),
    commonAncestor: exactBytes(common.generationId, 32, "common ancestor"),
    async apply(ifTarget, idempotencyKey) {
      requireBytes(ifTarget, 32, "target generation");
      if (!equalBytes(ifTarget, target.generationId)) {
        return { status: "stale-target", generationId: target.generationId.slice(), conflicts: [], truncated: false };
      }
      const response = await call(client.rpc.applyJoin({ plan, operation: operation(idempotencyKey) }));
      return joinResult(response.status, response.generation, response.conflicts, response.truncated);
    },
    close: () => Promise.resolve(),
  };
}

function generationDiff(value: DiffResponse): GenerationDiff {
  return {
    files: value.files.map((entry): FileRecordChange => ({
      fileId: exactBytes(entry.fileId, 16, "file identity"),
      before: entry.before === undefined ? undefined : fileSnapshot(entry.before),
      after: entry.after === undefined ? undefined : fileSnapshot(entry.after),
    })),
    bindings: value.bindings.map((entry): DirectoryBindingChange => ({
      directoryId: exactBytes(entry.directoryId, 16, "directory identity"),
      name: logicalName(required(entry.name, "binding name")),
      before: entry.before === undefined ? undefined : treeEntry(entry.before),
      after: entry.after === undefined ? undefined : treeEntry(entry.after),
    })),
    truncated: value.truncated,
    work: workCounters(required(value.work, "diff work")),
  };
}

function fileSnapshot(value: WireFileRecordSnapshot): FileRecordSnapshot {
  return {
    fileId: exactBytes(value.fileId, 16, "file identity"),
    fileKind: fileKind(value.fileKind),
    linkCount: value.linkCount,
    metadataObject: exactBytes(value.metadataObject, 33, "metadata object"),
    payloadKind: value.payloadKind,
    logicalBytes: optionalU64(value.logicalBytes),
    payloadObject: value.payloadObject.length === 0 ? undefined : exactBytes(value.payloadObject, 33, "payload object"),
    inlineBytes: value.payloadKind === "inline-regular" ? value.inlineBytes.slice() : undefined,
    deviceMajor: optionalU32(value.deviceMajor),
    deviceMinor: optionalU32(value.deviceMinor),
  };
}

function treeEntry(value: WireTreeEntrySnapshot): TreeEntrySnapshot {
  return {
    name: logicalName(required(value.name, "tree entry name")),
    fileId: exactBytes(value.fileId, 16, "file identity"),
    fileKind: fileKind(value.fileKind),
  };
}

function transactionConflict(value: WireConflict): TransactionConflict {
  let region: TransactionConflict["region"];
  let fileId: Uint8Array | undefined;
  let directoryId: Uint8Array | undefined;
  let offset: bigint | undefined;
  let length: bigint | undefined;
  let sparseTarget: "data" | "hole" | undefined;
  let name: WorkspaceName | undefined;
  let maximumEntries: number | undefined;
  switch (value.region.case) {
    case "fileRecord": region = "file-record"; fileId = value.region.value.fileId.slice(); break;
    case "metadata": region = "metadata"; fileId = value.region.value.fileId.slice(); break;
    case "fileLength": region = "file-length"; fileId = value.region.value.fileId.slice(); break;
    case "contentRange": {
      region = "content-range";
      fileId = value.region.value.fileId.slice();
      const range = required(value.region.value.range, "conflict range");
      offset = range.offset;
      length = range.length;
      break;
    }
    case "sparseSeek":
      region = "sparse-seek";
      fileId = value.region.value.fileId.slice();
      offset = value.region.value.offset;
      sparseTarget = value.region.value.target === SparseTarget.DATA ? "data" : "hole";
      break;
    case "directoryName":
      region = "directory-name";
      directoryId = value.region.value.directoryId.slice();
      name = logicalName(required(value.region.value.name, "conflict name"));
      break;
    case "directoryRange":
      region = "directory-range";
      directoryId = value.region.value.directoryId.slice();
      name = value.region.value.after === undefined ? undefined : logicalName(value.region.value.after);
      maximumEntries = value.region.value.maximumEntries;
      break;
    default: throw new HostedFsError("invalid_response", "conflict region is absent");
  }
  return {
    region, fileId, directoryId, offset, length, sparseTarget, name, maximumEntries,
    usage: conflictUse(value.use),
    expected: value.expectedDigest.length === 0 ? undefined : value.expectedDigest.slice(),
    actual: value.actualDigest.length === 0 ? undefined : value.actualDigest.slice(),
  };
}

function mergeConflict(value: WireConflict): MergeConflict {
  switch (value.region.case) {
    case "directoryName": return {
      kind: "binding",
      directoryId: exactBytes(value.region.value.directoryId, 16, "directory identity"),
      name: logicalName(required(value.region.value.name, "conflict name")),
    };
    case "directoryRange": return {
      kind: "binding",
      directoryId: exactBytes(value.region.value.directoryId, 16, "directory identity"),
      name: logicalName(required(value.region.value.after, "conflict range cursor")),
    };
    case "fileRecord": return { kind: "file", fileId: value.region.value.fileId.slice() };
    case "metadata": return { kind: "file", fileId: value.region.value.fileId.slice() };
    case "fileLength": return { kind: "file", fileId: value.region.value.fileId.slice() };
    case "contentRange": return { kind: "file", fileId: value.region.value.fileId.slice() };
    case "sparseSeek": return { kind: "file", fileId: value.region.value.fileId.slice() };
    default: throw new HostedFsError("invalid_response", "merge conflict region is absent");
  }
}

function commit(status: MutationStatus, generationRef: WireGenerationRef | undefined): WorkspaceCommit {
  const statuses: Partial<Record<MutationStatus, WorkspaceCommitStatus>> = {
    [MutationStatus.COMMITTED]: "committed",
    [MutationStatus.ALREADY_COMMITTED]: "already-committed",
    [MutationStatus.CONFLICT]: "conflict",
    [MutationStatus.FENCED]: "fenced",
    [MutationStatus.IDEMPOTENCY_CONFLICT]: "idempotency-conflict",
  };
  const translated = statuses[status];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid mutation status");
  return { status: translated, generationId: generationRef?.generationId.slice() };
}

function workspaceRebase(
  status: RebaseStatus,
  generationRef: WireGenerationRef | undefined,
  conflicts: readonly WireConflict[],
  truncated: boolean,
): WorkspaceRebaseResult {
  const statuses: Partial<Record<RebaseStatus, WorkspaceRebaseStatus>> = {
    [RebaseStatus.REBASED]: "rebased",
    [RebaseStatus.ALREADY_REBASED]: "already-rebased",
    [RebaseStatus.CURRENT]: "current",
    [RebaseStatus.STALE]: "stale",
    [RebaseStatus.CONFLICTED]: "conflicted",
    [RebaseStatus.FENCED]: "fenced",
    [RebaseStatus.IDEMPOTENCY_CONFLICT]: "idempotency-conflict",
  };
  const translated = statuses[status];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid rebase status");
  return {
    status: translated,
    generationId: generationRef?.generationId.slice(),
    conflicts: conflicts.map(mergeConflict),
    truncated,
  };
}

function joinResult(
  status: WireJoinStatus,
  generationRef: WireGenerationRef | undefined,
  conflicts: readonly WireConflict[],
  truncated: boolean,
): JoinResult {
  const statuses: Partial<Record<WireJoinStatus, JoinStatus>> = {
    [WireJoinStatus.APPLIED]: "applied",
    [WireJoinStatus.ALREADY_APPLIED]: "already-applied",
    [WireJoinStatus.NO_CHANGES]: "no-changes",
    [WireJoinStatus.STALE_TARGET]: "stale-target",
    [WireJoinStatus.CONFLICTED]: "conflicted",
    [WireJoinStatus.FENCED]: "fenced",
    [WireJoinStatus.IDEMPOTENCY_CONFLICT]: "idempotency-conflict",
  };
  const translated = statuses[status];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid join status");
  return {
    status: translated,
    generationId: generationRef?.generationId.slice(),
    conflicts: conflicts.map(mergeConflict),
    truncated,
  };
}

function metadata(value: WireMetadata | undefined): WorkspaceMetadata {
  const exact = required(value, "metadata");
  return {
    posixMode: optionalU32(exact.posixMode), posixUid: optionalU32(exact.posixUid),
    posixGid: optionalU32(exact.posixGid), posixFlags: optionalU64(exact.posixFlags),
    windowsAttributes: optionalU32(exact.windowsAttributes), createdNs: optionalI64(exact.createdNs),
    modifiedNs: optionalI64(exact.modifiedNs), accessedNs: optionalI64(exact.accessedNs),
    changedNs: optionalI64(exact.changedNs), hasNamedAttributes: exact.hasNamedAttributes,
    hasAcl: exact.hasAcl, hasSecurityDescriptor: exact.hasSecurityDescriptor,
  };
}

function optionalU32(value: OptionalU32 | undefined): number | undefined {
  return value?.value.case === "present" ? value.value.value : undefined;
}
function optionalU64(value: OptionalU64 | undefined): bigint | undefined {
  return value?.value.case === "present" ? value.value.value : undefined;
}
function optionalI64(value: OptionalI64 | undefined): bigint | undefined {
  return value?.value.case === "present" ? value.value.value : undefined;
}

function logicalName(value: WireLogicalName): WorkspaceName {
  const encoding = value.encoding === NameEncoding.UTF8 ? "utf8"
    : value.encoding === NameEncoding.POSIX_BYTES ? "posix-bytes"
      : value.encoding === NameEncoding.WINDOWS_UTF16LE ? "windows-utf16le" : undefined;
  if (encoding === undefined) throw new HostedFsError("invalid_response", "invalid name encoding");
  return { encoding, bytes: value.bytes.slice() };
}

function wireName(value: WorkspaceName): { readonly encoding: NameEncoding; readonly bytes: Uint8Array } {
  return {
    encoding: value.encoding === "utf8" ? NameEncoding.UTF8
      : value.encoding === "posix-bytes" ? NameEncoding.POSIX_BYTES : NameEncoding.WINDOWS_UTF16LE,
    bytes: value.bytes,
  };
}

function fileKind(value: FileKind): WorkspaceFileKind {
  switch (value) {
    case FileKind.REGULAR: return "regular";
    case FileKind.DIRECTORY: return "directory";
    case FileKind.SYMBOLIC_LINK: return "symbolic-link";
    case FileKind.FIFO: return "fifo";
    case FileKind.SOCKET: return "socket";
    case FileKind.CHARACTER_DEVICE: return "character-device";
    case FileKind.BLOCK_DEVICE: return "block-device";
    case FileKind.REPARSE_POINT: return "reparse-point";
    case FileKind.MOUNT_BOUNDARY: return "mount-boundary";
    default: throw new HostedFsError("invalid_response", "invalid file kind");
  }
}

function extentKind(value: ExtentKind): "hole" | "allocated-zero" | "content" {
  switch (value) {
    case ExtentKind.HOLE: return "hole";
    case ExtentKind.ALLOCATED_ZERO: return "allocated-zero";
    case ExtentKind.CONTENT: return "content";
    default: throw new HostedFsError("invalid_response", "invalid extent kind");
  }
}

function workCounters(value: WireWorkCounters): WorkCounters {
  return {
    authorityRecordsRead: safeNumber(value.authorityRecordsRead, "authority records read"),
    authorityRecordsAppended: safeNumber(value.authorityRecordsAppended, "authority records appended"),
    authorityBytesRead: safeNumber(value.authorityBytesRead, "authority bytes read"),
    authorityBytesWritten: safeNumber(value.authorityBytesWritten, "authority bytes written"),
    objectProbes: safeNumber(value.objectProbes, "object probes"),
    backendReadOperations: safeNumber(value.backendReadOperations, "backend reads"),
    backendWriteOperations: safeNumber(value.backendWriteOperations, "backend writes"),
    durabilityOperations: safeNumber(value.durabilityOperations, "durability operations"),
    pageReads: safeNumber(value.pageReads, "page reads"), pageWrites: safeNumber(value.pageWrites, "page writes"),
    objectBytesRead: safeNumber(value.objectBytesRead, "object bytes read"),
    objectBytesWritten: safeNumber(value.objectBytesWritten, "object bytes written"),
    bytesHashed: safeNumber(value.bytesHashed, "bytes hashed"), bytesCopied: safeNumber(value.bytesCopied, "bytes copied"),
    bytesEncoded: safeNumber(value.bytesEncoded, "bytes encoded"), sourceBytesRead: safeNumber(value.sourceBytesRead, "source bytes read"),
    outputBytes: safeNumber(value.outputBytes, "output bytes"), itemsExamined: safeNumber(value.itemsExamined, "items examined"),
    itemsReturned: safeNumber(value.itemsReturned, "items returned"),
    allocationOperations: safeNumber(value.allocationOperations, "allocation operations"),
    peakAllocationBytes: safeNumber(value.peakAllocationBytes, "peak allocation bytes"),
    materializations: safeNumber(value.materializations, "materializations"),
  };
}

function operation(idempotencyKey?: Uint8Array) {
  const identity = idempotencyKey?.slice() ?? randomIdentity();
  requireBytes(identity, 16, "idempotency key");
  return create(OperationOptionsSchema, { idempotencyKey: identity });
}
function randomIdentity(): Uint8Array {
  const value = new Uint8Array(16);
  globalThis.crypto.getRandomValues(value);
  return value;
}

function requireGeneration(value: FsGeneration, client: HostedClient, workspaceId: Uint8Array): WireGenerationRef {
  const owner = generationOwners.get(value);
  if (owner === undefined || owner.client !== client
    || !equalBytes(required(owner.reference.workspace, "generation workspace").workspaceId, workspaceId)) {
    throw new TypeError("generation belongs to another hosted workspace");
  }
  return owner.reference;
}
function requireWorkspace(value: FsWorkspace, client: HostedClient): WorkspaceOwner {
  const owner = workspaceOwners.get(value);
  if (owner === undefined || owner.client !== client) throw new TypeError("workspace belongs to another hosted runtime");
  return owner;
}

function deleteStatus(value: MutationStatus): WorkspaceDeleteStatus {
  switch (value) {
    case MutationStatus.COMMITTED: return "deleted";
    case MutationStatus.ALREADY_COMMITTED: return "already-deleted";
    case MutationStatus.CONFLICT: return "conflict";
    case MutationStatus.IDEMPOTENCY_CONFLICT: return "idempotency-conflict";
    default: throw new HostedFsError("invalid_response", "invalid delete status");
  }
}
function joinHistory(value: JoinOptions["history"]): JoinHistory {
  switch (value) {
    case "merge": return JoinHistory.MERGE;
    case "rebase": return JoinHistory.REBASE;
    case "squash": return JoinHistory.SQUASH;
    case "cherry-pick": return JoinHistory.CHERRY_PICK;
  }
}
function conflictUse(value: ConflictUse): TransactionConflict["usage"] {
  switch (value) {
    case ConflictUse.OBSERVATION: return "observation";
    case ConflictUse.MUTATION: return "mutation";
    case ConflictUse.OBSERVATION_AND_MUTATION: return "observation-and-mutation";
    default: throw new HostedFsError("invalid_response", "invalid conflict use");
  }
}
function validateJoin(value: JoinOptions): void {
  positiveU32(value.maximumGenerations, "maximum generations");
  positiveU32(value.maximumChanges, "maximum changes");
  positiveU32(value.maximumConflicts, "maximum conflicts");
}
function validateRebase(value: WorkspaceRebaseOptions): void {
  positiveU32(value.maximumGenerations, "maximum generations");
  positiveU32(value.maximumChanges, "maximum changes");
  positiveU32(value.maximumConflicts, "maximum conflicts");
}

function assertOpen(client: HostedClient): void {
  if (client.closed) throw new HostedFsError("closed", "hosted filesystem is closed");
}
function requireName(value: string): void { if (value.length === 0) throw new RangeError("name must be non-empty"); }
function positiveSafeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be positive`);
}
function positiveU32(value: number, name: string): void {
  if (!Number.isInteger(value) || value <= 0 || value > 0xffff_ffff) throw new RangeError(`${name} must be a positive u32`);
}
function positiveU64(value: bigint, name: string): void {
  if (value <= 0n || value > 0xffff_ffff_ffff_ffffn) throw new RangeError(`${name} must be a positive u64`);
}
function nonnegativeU64(value: bigint, name: string): void {
  if (value < 0n || value > 0xffff_ffff_ffff_ffffn) throw new RangeError(`${name} must be a u64`);
}
function required<T>(value: T | undefined, name: string): T {
  if (value === undefined) throw new HostedFsError("invalid_response", `${name} is absent`);
  return value;
}
function requireBytes(value: Uint8Array, length: number, name: string): void {
  if (!(value instanceof Uint8Array) || value.byteLength !== length) {
    throw new HostedFsError("invalid_response", `${name} must contain ${length} bytes`);
  }
}
function exactBytes(value: Uint8Array, length: number, name: string): Uint8Array {
  requireBytes(value, length, name);
  return value.slice();
}
function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  let difference = 0;
  for (let index = 0; index < left.length; index++) difference |= left[index]! ^ right[index]!;
  return difference === 0;
}
function safeNumber(value: bigint, name: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new HostedFsError("invalid_response", `${name} exceeds JavaScript's exact integer range`);
  }
  return Number(value);
}
async function call<T>(request: Promise<T>): Promise<T> {
  try { return await request; }
  catch (error) {
    if (error instanceof HostedFsError) throw error;
    if (error instanceof ConnectError) {
      const codes: Partial<Record<Code, string>> = {
        [Code.Canceled]: "cancelled",
        [Code.InvalidArgument]: "invalid_argument",
        [Code.DeadlineExceeded]: "deadline_exceeded",
        [Code.NotFound]: "not_found",
        [Code.AlreadyExists]: "already_exists",
        [Code.PermissionDenied]: "permission_denied",
        [Code.ResourceExhausted]: "resource_exhausted",
        [Code.FailedPrecondition]: "failed_precondition",
        [Code.Aborted]: "aborted",
        [Code.OutOfRange]: "out_of_range",
        [Code.Unimplemented]: "unsupported",
        [Code.Internal]: "internal",
        [Code.Unavailable]: "unavailable",
        [Code.DataLoss]: "data_loss",
        [Code.Unauthenticated]: "unauthenticated",
      };
      throw new HostedFsError(codes[error.code] ?? "unknown", error.rawMessage);
    }
    throw error;
  }
}

function boundedFetch(send: typeof globalThis.fetch, maximumBytes: number): typeof globalThis.fetch {
  return async (input, init) => {
    const response = await send(input, init);
    const declared = response.headers.get("content-length");
    if (declared !== null && /^\d+$/.test(declared) && BigInt(declared) > BigInt(maximumBytes)) {
      await response.body?.cancel();
      throw new HostedFsError("response_too_large", "filesystem response exceeds its local bound");
    }
    if (response.body === null) return response;
    let received = 0;
    const bounded = response.body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        received += chunk.byteLength;
        if (received > maximumBytes) {
          controller.error(new HostedFsError("response_too_large", "filesystem response exceeds its local bound"));
        } else {
          controller.enqueue(chunk);
        }
      },
    }));
    return new Response(bounded, response);
  };
}
