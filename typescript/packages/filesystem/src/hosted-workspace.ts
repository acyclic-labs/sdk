import { type FsWorkspace, type FsGeneration, type FsChangeSet, type HostedFsWorkspace, type S3Access, type WorkspaceStat, type WorkspaceName, type WorkspaceDirectoryPage, type WorkspaceExtentPlan, type FsTransaction, type TransactionRebaseResult, type FsJoinPlan, type GenerationDiff, type FileRecordChange, type DirectoryBindingChange } from "./contracts.js";
import { type HostedClient, required, requireBytes, requireName, exactBytes, call, operation, HostedFsError, validateMergeLimits, requirePageBound, positiveU32, assertOpen, positiveU64, nonnegativeU64, requireResponseBound, equalBytes } from "./hosted-rpc.js";
import { type WorkspaceRef as WireWorkspaceRef, type GenerationRef as WireGenerationRef, type Workspace as WireWorkspace, GenerationRefSchema, type Mutation as WireMutation, MutationSchema, type JoinPlan as WireJoinPlan, type DiffResponse } from "../generated/proto/filesystem/v2/filesystem_pb.js";
import { create } from "@bufbuild/protobuf";
import { deleteStatus, sourceResult, commit, workspaceRebase, joinHistory, fileKind, metadata, wireName, logicalName, extentKind, transactionConflict, joinResult, fileSnapshot, treeEntry, workCounters } from "./hosted-values.js";
import { secureServiceEndpoint } from "./endpoint.js";
import { DEFAULT_HOSTED_OPTIONS } from "../generated/defaults.js";

const workspaceOwners = new WeakMap<FsWorkspace, { readonly client: HostedClient; readonly reference: WireWorkspaceRef }>();

const generationOwners = new WeakMap<FsGeneration, { readonly client: HostedClient; readonly reference: WireGenerationRef }>();

const changeSetOwners = new WeakMap<FsChangeSet, {
  readonly client: HostedClient;
  readonly workspaceId: Uint8Array;
  readonly from: WireGenerationRef;
  readonly to: WireGenerationRef;
}>();

export function workspace(client: HostedClient, value: WireWorkspace): HostedFsWorkspace {
  const reference = required(value.workspace, "workspace reference");
  requireBytes(reference.workspaceId, 16, "workspace identity");
  requireName(reference.name);
  const result: HostedFsWorkspace = {
    name: reference.name,
    id: Uint8Array.from(reference.workspaceId),
    async head() { return Uint8Array.from((await currentGeneration(client, reference)).generationId); },
    async generation(id) {
      const selected = create(GenerationRefSchema, {
        workspace: reference, generationId: exactBytes(id, 32, "generation identity"),
      });
      await stat(client, selected, "/");
      return generation(client, selected);
    },
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
    async s3Access(writable, expiresAfterSeconds, idempotencyKey) {
      return s3Access(client, reference, writable, expiresAfterSeconds, idempotencyKey);
    },
    async sourceState() {
      requireSourceReconciliation(client);
      return sourceResult(reference, await call(client.rpc.getSourceState({ workspace: reference })));
    },
    async reconcileSource(idempotencyKey) {
      requireSourceReconciliation(client);
      return sourceResult(reference, await call(client.rpc.reconcileSource({
        workspace: reference,
        operation: operation(idempotencyKey),
      })));
    },
    async rescanSource(idempotencyKey) {
      requireSourceReconciliation(client);
      return sourceResult(reference, await call(client.rpc.rescanSource({
        workspace: reference,
        operation: operation(idempotencyKey),
      })));
    },
    async seal(idempotencyKey) {
      requireSourceReconciliation(client);
      const response = await call(client.rpc.sealSource({
        workspace: reference,
        operation: operation(idempotencyKey),
      }));
      const result = sourceResult(reference, response);
      if (result.status !== "sealed" || response.generation === undefined) {
        throw new HostedFsError("invalid_response", "seal did not return a sealed generation");
      }
      return generation(client, response.generation);
    },
    async read(path, maximumBytes) {
      return read(client, await currentGeneration(client, reference), path, undefined, maximumBytes);
    },
    async readRange(path, offset, length) {
      return read(client, await currentGeneration(client, reference), path, { offset, length }, length);
    },
    async stat(path) { return stat(client, await currentGeneration(client, reference), path); },
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
    async forkAt(destination, selected, options = {}) {
      return fork(client, requireGeneration(selected, client, reference.workspaceId), destination, options);
    },
    async beginTransaction(idempotencyKey) {
      return transaction(client, await currentGeneration(client, reference), idempotencyKey);
    },
    async liveRebase(options, idempotencyKey) {
      validateMergeLimits(options);
      requirePageBound(client, options.maximumGenerations, "maximum generations");
      requirePageBound(client, options.maximumChanges, "maximum changes");
      requirePageBound(client, options.maximumConflicts, "maximum conflicts");
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
      requirePageBound(client, maximumChanges, "maximum changes");
      return diff(
        client,
        reference.workspaceId,
        requireGeneration(from, client, reference.workspaceId),
        requireGeneration(to, client, reference.workspaceId),
        maximumChanges,
      );
    },
    async joinInto(target, options) {
      validateMergeLimits(options);
      requirePageBound(client, options.maximumGenerations, "maximum generations");
      requirePageBound(client, options.maximumChanges, "maximum changes");
      requirePageBound(client, options.maximumConflicts, "maximum conflicts");
      const destination = requireWorkspace(target, client);
      const plan = await call(client.rpc.planJoin({
        source: options.sourceGeneration === undefined ? await currentGeneration(client, reference) : { workspace: reference, generationId: options.sourceGeneration },
        target: options.targetGeneration === undefined ? await currentGeneration(client, destination.reference) : { workspace: destination.reference, generationId: options.targetGeneration },
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

function requireSourceReconciliation(client: HostedClient): void {
  assertOpen(client);
  if (!client.sourceReconciliation) {
    throw new HostedFsError("unsupported", "hosted filesystem does not provide source reconciliation");
  }
}

async function s3Access(
  client: HostedClient,
  reference: WireWorkspaceRef,
  writable: boolean,
  expiresAfterSeconds: bigint,
  idempotencyKey?: Uint8Array,
): Promise<S3Access> {
  assertOpen(client);
  if (!client.s3Credentials) {
    throw new HostedFsError("unsupported", "hosted filesystem does not issue S3 credentials");
  }
  positiveU64(expiresAfterSeconds, "S3 credential lifetime");
  const response = await call(client.rpc.issueS3Credential({
    workspace: reference,
    generation: await currentGeneration(client, reference),
    writable,
    expiresAfterSeconds,
    operation: operation(idempotencyKey),
  }));
  if (response.credential.case !== "s3") {
    throw new HostedFsError("protocol", "missing S3 credential");
  }
  secureServiceEndpoint(response.endpoint, message => new HostedFsError("invalid_response", `S3 credential ${message}`));
  const credential = response.credential.value;
  requireName(credential.bucket);
  requireName(credential.region);
  requireName(credential.accessKeyId);
  requireName(credential.secretAccessKey);
  if (response.expiresAtUnixSeconds <= BigInt(Math.floor(Date.now() / 1_000))) {
    throw new HostedFsError("invalid_response", "S3 credential expiry must be in the future");
  }
  return {
    endpoint: response.endpoint,
    bucket: credential.bucket as S3Access["bucket"],
    region: credential.region as S3Access["region"],
    accessKeyId: credential.accessKeyId as S3Access["accessKeyId"],
    secretAccessKey: credential.secretAccessKey as S3Access["secretAccessKey"],
    sessionToken: credential.sessionToken as S3Access["sessionToken"],
    expiresAtUnixSeconds: response.expiresAtUnixSeconds,
  };
}

export function generation(client: HostedClient, reference: WireGenerationRef): FsGeneration {
  const owner = required(reference.workspace, "generation workspace");
  requireBytes(reference.generationId, 32, "generation identity");
  const result: FsGeneration = {
    id: Uint8Array.from(reference.generationId),
    workspaceId: Uint8Array.from(owner.workspaceId),
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

async function fork(
  client: HostedClient,
  source: WireGenerationRef,
  destinationName: string,
  options: import("./contracts.js").WorkspaceForkOptions = {},
): Promise<HostedFsWorkspace> {
  requireName(destinationName);
  const response = await call(client.rpc.forkWorkspace({
    source,
    destinationName,
    operation: operation(options.idempotencyKey),
    selection: options.paths === undefined ? undefined : { paths: [...options.paths] },
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
  requireResponseBound(client, maximumBytes, "maximum read bytes");
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
  return Uint8Array.from(response.contents);
}

async function stat(client: HostedClient, selected: WireGenerationRef, path: string): Promise<WorkspaceStat> {
  assertOpen(client);
  const value = required((await call(client.rpc.stat({ generation: selected, path }))).stat, "file stat");
  return {
    fileId: exactBytes(value.fileId, 16, "file identity"),
    kind: fileKind(value.kind),
    linkCount: value.linkCount,
    logicalBytes: (value.logicalBytes?.value.case === "present" ? value.logicalBytes.value.value : undefined),
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
  requirePageBound(client, maximumEntries, "maximum entries");
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
  return Uint8Array.from(response.contents);
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
  requirePageBound(client, maximumSpans, "maximum spans");
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
      requirePageBound(client, maximumConflicts, "maximum conflicts");
      const response = await call(client.rpc.rebaseTransaction({
        base,
        mutations,
        maximumConflicts,
        operation: operationOptions,
      }));
      if (response.conflicts.length === 0) {
        base = required(response.base, "rebased transaction base");
        return { status: "rebased", generationId: Uint8Array.from(base.generationId), conflicts: [], truncated: false };
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
        // Generated from Rust's DEFAULT_HOSTED_MAXIMUM_PAGE_ITEMS.
        maximumConflicts: Math.min(DEFAULT_HOSTED_OPTIONS.maximumPageItems, client.maximumPageItems),
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
  requirePageBound(client, maximumChanges, "maximum changes");
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
        return { status: "stale-target", generationId: Uint8Array.from(target.generationId), conflicts: [], truncated: false };
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

function requireGeneration(value: FsGeneration, client: HostedClient, workspaceId: Uint8Array): WireGenerationRef {
  const owner = generationOwners.get(value);
  if (owner === undefined || owner.client !== client
    || !equalBytes(required(owner.reference.workspace, "generation workspace").workspaceId, workspaceId)) {
    throw new TypeError("generation belongs to another hosted workspace");
  }
  return owner.reference;
}

function requireWorkspace(value: FsWorkspace, client: HostedClient) {
  const owner = workspaceOwners.get(value);
  if (owner === undefined || owner.client !== client) throw new TypeError("workspace belongs to another hosted runtime");
  return owner;
}
