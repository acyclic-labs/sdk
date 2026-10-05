import { create, toBinary } from "@bufbuild/protobuf";
import { Code, ConnectError, createClient, type Client, type Interceptor } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";

import { FILESYSTEM_DESCRIPTOR_DIGEST } from "../generated/descriptor-digest.js";
import { DEFAULT_HOSTED_OPTIONS } from "../generated/defaults.js";
import {
  FILE_KIND_TO_KIND,
  FILESYSTEM_PROFILE_TO_PROFILE,
  MUTATION_STATUS_TO_COMMIT,
  MUTATION_STATUS_TO_DELETE,
  CONFLICT_USE_TO_USAGE,
  JOIN_HISTORY_FROM_PUBLIC,
  EXTENT_KIND_TO_KIND,
  SOURCE_INVALIDATION_REASON_TO_REASON,
  SOURCE_STATE_TO_STATUS,
  REBASE_STATUS_TO_STATUS,
  JOIN_STATUS_TO_STATUS,
  SPARSE_TARGET_TO_TARGET,
  NAME_ENCODING_TO_PUBLIC,
} from "../generated/hosted-contract.js";

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
  SourceInvalidationReason as WireSourceInvalidationReason,
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
  type SourceResponse as WireSourceResponse,
  type TreeEntrySnapshot as WireTreeEntrySnapshot,
  type WorkCounters as WireWorkCounters,
  type Workspace as WireWorkspace,
  type WorkspaceRef as WireWorkspaceRef,
} from "../generated/proto/filesystem/v2/filesystem_pb.js";
import type {
  DirectoryBindingChange,
  FileRecordChange,
  FileRecordSnapshot,
  FsChangeSet,
  FsGeneration,
  FsJoinPlan,
  FsProfile,
  FsTransaction,
  FsWorkspace,
  GenerationDiff,
  HostedFsEngine,
  HostedFsCapabilities,
  HostedFsOptions,
  HostedFsWorkspace,
  JoinOptions,
  JoinResult,
  S3Access,
  SourceResult,
  MergeConflict,
  TransactionConflict,
  TransactionRebaseResult,
  TreeEntrySnapshot,
  WorkCounters,
  WorkspaceCommit,
  WorkspaceDeleteStatus,
  WorkspaceDirectoryPage,
  WorkspaceExtentPlan,
  WorkspaceFileKind,
  WorkspaceMetadata,
  WorkspaceName,
  WorkspaceRebaseResult,
  WorkspaceStat,
} from "./contracts.js";
import { rustOwnedServiceEndpoint } from "./endpoint.js";
import { validateRustOwnedCredentialPolicy } from "./generated-client.js";

/**
 * Rust-owned hosted request policy exported by the generated WASM package.
 * Keep this boundary structural so a clean source checkout can typecheck
 * before the generated WASM declaration is refreshed.
 */
type HostedRustPolicy = {
  validateHostedPageBound(value: number, maximum: number): void;
  validateHostedTransactionBounds(
    mutationCount: number,
    maximumMutations: number,
    maximumConflicts: number,
    maximumPageItems: number,
  ): void;
  validateHostedGenerationBounds(
    maximumGenerations: number,
    maximumChanges: number,
    maximumConflicts: number,
    maximumPageItems: number,
  ): void;
  validateHostedSourceState(state: number, reason: number, hasGeneration: boolean): void;
  validateHostedGenerationIdentity(
    generationId: Uint8Array,
    ownerWorkspaceId: Uint8Array,
    expectedWorkspaceId: Uint8Array,
  ): void;
};

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS } from "./contracts.js";

export class HostedFsError extends Error {
  constructor(readonly code: string, message: string) {
    super(message);
    this.name = "HostedFsError";
  }
}

interface HostedClient {
  readonly rpc: Client<typeof FilesystemService>;
  readonly rustPolicy: HostedRustPolicy;
  readonly maximumResponseBytes: number;
  readonly maximumTransactionMutations: number;
  readonly maximumPageItems: number;
  readonly s3Credentials: boolean;
  readonly sourceReconciliation: boolean;
  closed: boolean;
}

export async function openHostedFs(options: HostedFsOptions): Promise<HostedFsEngine> {
  const endpoint = await rustOwnedServiceEndpoint(options.endpoint, message => new RangeError(`hosted filesystem ${message}`));
  try {
    await validateRustOwnedCredentialPolicy(options.bearerToken);
  } catch {
    throw new RangeError("invalid bearer token");
  }
  // The credential validator initializes the shared Rust/WASM module. Reuse
  // that initialized module for every hosted request policy decision.
  const rustWasm = await import("../generated/wasm/acyclic_fs_wasm.js") as unknown as HostedRustPolicy;
  const rustPolicy: HostedRustPolicy = {
    validateHostedPageBound: rustWasm.validateHostedPageBound,
    validateHostedTransactionBounds: rustWasm.validateHostedTransactionBounds,
    validateHostedGenerationBounds: rustWasm.validateHostedGenerationBounds,
    validateHostedSourceState: rustWasm.validateHostedSourceState,
    validateHostedGenerationIdentity: rustWasm.validateHostedGenerationIdentity,
  };
  const maximumResponseBytes = options.maximumResponseBytes ?? DEFAULT_HOSTED_OPTIONS.maximumResponseBytes;
  positiveSafeInteger(maximumResponseBytes, "maximum response bytes");
  if (maximumResponseBytes < DEFAULT_HOSTED_OPTIONS.minimumHandshakeResponseBytes) {
    throw new RangeError(
      `maximum response bytes must be at least ${DEFAULT_HOSTED_OPTIONS.minimumHandshakeResponseBytes}`,
    );
  }
  const maximumPayloadResponseBytes = maximumResponseBytes - DEFAULT_HOSTED_OPTIONS.maximumByteResponseEnvelopeBytes;
  const send = options.fetch ?? globalThis.fetch;
  if (send === undefined) throw new TypeError("this runtime does not provide fetch");
  let negotiatedMaximumRequestBytes: bigint | undefined;
  const authorize: Interceptor = (next) => async (request) => {
    request.header.set("authorization", `Bearer ${options.bearerToken}`);
    return next(request);
  };
  const boundRequest: Interceptor = (next) => async (request) => {
    if (!request.stream && negotiatedMaximumRequestBytes !== undefined) {
      const bytes = toBinary(request.method.input, request.message).byteLength;
      if (BigInt(bytes) > negotiatedMaximumRequestBytes) {
        throw new HostedFsError("limit", "hosted filesystem request exceeds the negotiated byte limit");
      }
    }
    return next(request);
  };
  const rpcClient = createClient(FilesystemService, createGrpcWebTransport({
    baseUrl: endpoint.href.replace(/\/$/, ""),
    interceptors: [authorize, boundRequest],
    fetch: boundedFetch(send, maximumResponseBytes),
  }));
  const handshake = await call(rpcClient.handshake({
    protocol: {
      protocol: { version: DEFAULT_HOSTED_OPTIONS.protocolVersion, descriptorDigest: FILESYSTEM_DESCRIPTOR_DIGEST },
      required: { capabilities: [{ name: "filesystem", version: DEFAULT_HOSTED_OPTIONS.protocolVersion }] },
    },
  }));
  const negotiated = required(handshake.protocol, "handshake response");
  const protocol = required(negotiated.protocol, "handshake protocol");
  if (protocol.version !== DEFAULT_HOSTED_OPTIONS.protocolVersion) throw new HostedFsError("protocol", "filesystem protocol version is unsupported");
  if (protocol.descriptorDigest !== FILESYSTEM_DESCRIPTOR_DIGEST) throw new HostedFsError("protocol", "filesystem descriptor digest does not match");
  const supported = required(negotiated.supported, "supported capabilities");
  if (!supported.capabilities.some(capability => capability.name === "filesystem" && capability.version === DEFAULT_HOSTED_OPTIONS.protocolVersion)) {
    throw new HostedFsError("protocol", "filesystem capability version is unsupported");
  }
  const advertised = required(handshake.capabilities, "filesystem capabilities");
  if (advertised.contractVersion !== DEFAULT_HOSTED_OPTIONS.protocolVersion) throw new HostedFsError("protocol", "filesystem contract version is unsupported");
  if (advertised.maximumRequestBytes <= 0n || advertised.maximumResponseBytes <= 0n) {
    throw new HostedFsError("protocol", "filesystem capabilities contain an unbounded byte limit");
  }
  negotiatedMaximumRequestBytes = advertised.maximumRequestBytes;
  positiveSafeInteger(advertised.maximumTransactionMutations, "maximum transaction mutations");
  positiveSafeInteger(advertised.maximumPageItems, "maximum page items");
  const profiles = advertised.profiles.map(profileFromWire);
  const negotiatedResponseBytes = advertised.maximumResponseBytes < BigInt(maximumPayloadResponseBytes)
    ? advertised.maximumResponseBytes
    : BigInt(maximumPayloadResponseBytes);
  const client: HostedClient = {
    rpc: rpcClient,
    rustPolicy,
    maximumResponseBytes: Number(negotiatedResponseBytes),
    maximumTransactionMutations: advertised.maximumTransactionMutations,
    maximumPageItems: advertised.maximumPageItems,
    s3Credentials: advertised.s3Credentials,
    sourceReconciliation: advertised.sourceReconciliation,
    closed: false,
  };
  const capabilities: HostedFsCapabilities = {
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
    profiles,
    maximumRequestBytes: advertised.maximumRequestBytes,
    maximumResponseBytes: negotiatedResponseBytes,
    maximumTransactionMutations: advertised.maximumTransactionMutations,
    maximumPageItems: advertised.maximumPageItems,
    nativeMountCredentials: advertised.nativeMountCredentials,
    s3Credentials: advertised.s3Credentials,
    sourceReconciliation: advertised.sourceReconciliation,
  };
  return {
    capabilities,
    async createWorkspace(name) {
      assertOpen(client);
      const response = await call(client.rpc.createWorkspace({
        name,
        profile: FilesystemProfile.PORTABLE,
        operation: operation(),
      }));
      return workspace(client, required(response.workspace, "created workspace"));
    },
    async openWorkspace(name) {
      assertOpen(client);
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

function workspace(client: HostedClient, value: WireWorkspace): HostedFsWorkspace {
  const reference = required(value.workspace, "workspace reference");
  requireBytes(reference.workspaceId, 16, "workspace identity");
  requireName(reference.name);
  const result: HostedFsWorkspace = {
    name: reference.name,
    id: Uint8Array.from(reference.workspaceId),
    async head() { return Uint8Array.from((await currentGeneration(client, reference)).generationId); },
    async sync() { return generation(client, await currentGeneration(client, reference)); },
    async checkpoint(label) {
      const response = await call(client.rpc.checkpoint({
        generation: await currentGeneration(client, reference),
        identity: label,
        operation: operation(),
      }));
      return generation(client, required(response.generation, "checkpoint generation"));
    },
    async pin(identity) {
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
      return sourceResult(client, reference, await call(client.rpc.getSourceState({ workspace: reference })));
    },
    async reconcileSource(idempotencyKey) {
      requireSourceReconciliation(client);
      return sourceResult(client, reference, await call(client.rpc.reconcileSource({
        workspace: reference,
        operation: operation(idempotencyKey),
      })));
    },
    async rescanSource(idempotencyKey) {
      requireSourceReconciliation(client);
      return sourceResult(client, reference, await call(client.rpc.rescanSource({
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
      const result = sourceResult(client, reference, response);
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
    async forkAt(destination, selected) {
      return fork(client, requireGeneration(selected, client, reference.workspaceId), destination);
    },
    async beginTransaction(idempotencyKey) {
      return transaction(client, await currentGeneration(client, reference), idempotencyKey);
    },
    async liveRebase(options, idempotencyKey) {
      await client.rustPolicy.validateHostedGenerationBounds(
        options.maximumGenerations,
        options.maximumChanges,
        options.maximumConflicts,
        client.maximumPageItems,
      );
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
      return diff(
        client,
        reference.workspaceId,
        requireGeneration(from, client, reference.workspaceId),
        requireGeneration(to, client, reference.workspaceId),
        maximumChanges,
      );
    },
    async joinInto(target, options) {
      await client.rustPolicy.validateHostedGenerationBounds(
        options.maximumGenerations,
        options.maximumChanges,
        options.maximumConflicts,
        client.maximumPageItems,
      );
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

function requireSourceReconciliation(client: HostedClient): void {
  assertOpen(client);
  if (!client.sourceReconciliation) {
    throw new HostedFsError("unsupported", "hosted filesystem does not provide source reconciliation");
  }
}

function sourceResult(
  client: HostedClient,
  workspace: WireWorkspaceRef,
  response: WireSourceResponse,
): SourceResult {
  client.rustPolicy.validateHostedSourceState(
    response.state,
    response.reason,
    response.generation !== undefined,
  );
  const status = SOURCE_STATE_TO_STATUS[response.state];
  if (status === undefined) throw new HostedFsError("invalid_response", "source state is invalid");
  const reason = response.reason === WireSourceInvalidationReason.UNSPECIFIED
    ? undefined
    : SOURCE_INVALIDATION_REASON_TO_REASON[response.reason];
  if (response.reason !== WireSourceInvalidationReason.UNSPECIFIED && reason === undefined) {
    throw new HostedFsError("invalid_response", "source invalidation reason is invalid");
  }
  if ((status === "needs-rescan") !== (reason !== undefined)) {
    throw new HostedFsError("invalid_response", "source state and invalidation reason do not match");
  }
  const selected = response.generation;
  if ((status === "clean" || status === "sealed") !== (selected !== undefined)) {
    throw new HostedFsError("invalid_response", "source generation does not match its state");
  }
  if (selected !== undefined) {
    const owner = required(selected.workspace, "source generation workspace");
    client.rustPolicy.validateHostedGenerationIdentity(
      selected.generationId,
      owner.workspaceId,
      workspace.workspaceId,
    );
    if (owner.name !== workspace.name) throw new HostedFsError("invalid_response", "source generation belongs to another workspace");
  }
  return { status, reason, generationId: copyOptionalBytes(selected?.generationId) };
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
  await rustOwnedServiceEndpoint(response.endpoint, message => new HostedFsError("invalid_response", `S3 credential ${message}`));
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

function generation(client: HostedClient, reference: WireGenerationRef): FsGeneration {
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
): Promise<HostedFsWorkspace> {
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
  await client.rustPolicy.validateHostedPageBound(maximumEntries, client.maximumPageItems);
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
  await client.rustPolicy.validateHostedPageBound(maximumSpans, client.maximumPageItems);
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
  const stage = async (value: WireMutation): Promise<void> => {
    assertOpen(client);
    if (closed) throw new HostedFsError("closed", "transaction is closed");
    await client.rustPolicy.validateHostedTransactionBounds(
      mutations.length + 1,
      client.maximumTransactionMutations,
      client.maximumPageItems,
      client.maximumPageItems,
    );
    mutations.push(value);
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
      return stage(mutation("write", { path, offset, contents: bytes }));
    },
    resize(path, logicalBytes) {
      return stage(mutation("resize", { path, logicalBytes }));
    },
    zeroRange(path, offset, length, allocated, extend) {
      return stage(mutation("zeroRange", { path, range: { offset, length }, allocated, extend }));
    },
    preallocate(path, offset, length, keepSize) {
      return stage(mutation("preallocate", { path, range: { offset, length }, keepSize }));
    },
    cloneRange(source, sourceOffset, destination, destinationOffset, length) {
      return stage(mutation("cloneRange", { source, sourceOffset, destination, destinationOffset, length }));
    },
    async rebase(maximumConflicts): Promise<TransactionRebaseResult> {
      await client.rustPolicy.validateHostedTransactionBounds(
        mutations.length,
        client.maximumTransactionMutations,
        maximumConflicts,
        client.maximumPageItems,
      );
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
      await client.rustPolicy.validateHostedTransactionBounds(
        mutations.length,
        client.maximumTransactionMutations,
        client.maximumPageItems,
        client.maximumPageItems,
      );
      const response = await call(client.rpc.applyTransaction({
        base,
        mutations,
        operation: operationOptions,
        // Generated from Rust's DEFAULT_HOSTED_MAXIMUM_PAGE_ITEMS.
        maximumConflicts: client.maximumPageItems,
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
  await client.rustPolicy.validateHostedPageBound(maximumChanges, client.maximumPageItems);
  const response = await call(client.rpc.diff({ from, to, maximumChanges }));
  const semantic = generationDiff(response);
  const result: FsChangeSet = {
    from: generation(client, required(response.from, "diff base")),
    to: generation(client, required(response.to, "diff result")),
    changes: () => semantic,
    async compose(next, bound) {
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

function fileSnapshot(value: WireFileRecordSnapshot): FileRecordSnapshot {
  return {
    fileId: exactBytes(value.fileId, 16, "file identity"),
    fileKind: fileKind(value.fileKind),
    linkCount: value.linkCount,
    metadataObject: exactBytes(value.metadataObject, 33, "metadata object"),
    payloadKind: value.payloadKind,
    logicalBytes: optionalU64(value.logicalBytes),
    payloadObject: value.payloadObject.length === 0 ? undefined : exactBytes(value.payloadObject, 33, "payload object"),
    inlineBytes: value.payloadKind === "inline-regular" ? Uint8Array.from(value.inlineBytes) : undefined,
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
    case "fileRecord": region = "file-record"; fileId = Uint8Array.from(value.region.value.fileId); break;
    case "metadata": region = "metadata"; fileId = Uint8Array.from(value.region.value.fileId); break;
    case "fileLength": region = "file-length"; fileId = Uint8Array.from(value.region.value.fileId); break;
    case "contentRange": {
      region = "content-range";
      fileId = Uint8Array.from(value.region.value.fileId);
      const range = required(value.region.value.range, "conflict range");
      offset = range.offset;
      length = range.length;
      break;
    }
    case "sparseSeek":
      region = "sparse-seek";
      fileId = Uint8Array.from(value.region.value.fileId);
      offset = value.region.value.offset;
      sparseTarget = SPARSE_TARGET_TO_TARGET[value.region.value.target];
      if (sparseTarget === undefined) throw new HostedFsError("invalid_response", "invalid sparse target");
      break;
    case "directoryName":
      region = "directory-name";
      directoryId = Uint8Array.from(value.region.value.directoryId);
      name = logicalName(required(value.region.value.name, "conflict name"));
      break;
    case "directoryRange":
      region = "directory-range";
      directoryId = Uint8Array.from(value.region.value.directoryId);
      name = value.region.value.after === undefined ? undefined : logicalName(value.region.value.after);
      maximumEntries = value.region.value.maximumEntries;
      break;
    default: throw new HostedFsError("invalid_response", "conflict region is absent");
  }
  return {
    region, fileId, directoryId, offset, length, sparseTarget, name, maximumEntries,
    usage: conflictUse(value.use),
    expected: value.expectedDigest.length === 0 ? undefined : Uint8Array.from(value.expectedDigest),
    actual: value.actualDigest.length === 0 ? undefined : Uint8Array.from(value.actualDigest),
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
    case "fileRecord": return { kind: "file", fileId: Uint8Array.from(value.region.value.fileId) };
    case "metadata": return { kind: "file", fileId: Uint8Array.from(value.region.value.fileId) };
    case "fileLength": return { kind: "file", fileId: Uint8Array.from(value.region.value.fileId) };
    case "contentRange": return { kind: "file", fileId: Uint8Array.from(value.region.value.fileId) };
    case "sparseSeek": return { kind: "file", fileId: Uint8Array.from(value.region.value.fileId) };
    default: throw new HostedFsError("invalid_response", "merge conflict region is absent");
  }
}

function commit(status: MutationStatus, generationRef: WireGenerationRef | undefined): WorkspaceCommit {
  const translated = MUTATION_STATUS_TO_COMMIT[status];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid mutation status");
  return { status: translated, generationId: copyOptionalBytes(generationRef?.generationId) };
}

function workspaceRebase(
  status: RebaseStatus,
  generationRef: WireGenerationRef | undefined,
  conflicts: readonly WireConflict[],
  truncated: boolean,
): WorkspaceRebaseResult {
  const translated = REBASE_STATUS_TO_STATUS[status];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid rebase status");
  return {
    status: translated,
    generationId: copyOptionalBytes(generationRef?.generationId),
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
  const translated = JOIN_STATUS_TO_STATUS[status];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid join status");
  return {
    status: translated,
    generationId: copyOptionalBytes(generationRef?.generationId),
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
  const encoding = NAME_ENCODING_TO_PUBLIC[value.encoding];
  if (encoding === undefined) throw new HostedFsError("invalid_response", "invalid name encoding");
  return { encoding, bytes: Uint8Array.from(value.bytes) };
}

function wireName(value: WorkspaceName): { readonly encoding: NameEncoding; readonly bytes: Uint8Array } {
  return {
    encoding: value.encoding === "utf8" ? NameEncoding.UTF8
      : value.encoding === "posix-bytes" ? NameEncoding.POSIX_BYTES : NameEncoding.WINDOWS_UTF16LE,
    bytes: value.bytes,
  };
}

function fileKind(value: FileKind): WorkspaceFileKind {
  const translated = FILE_KIND_TO_KIND[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid file kind");
  return translated;
}

function extentKind(value: ExtentKind): "hole" | "allocated-zero" | "content" {
  const translated = EXTENT_KIND_TO_KIND[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid extent kind");
  return translated;
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
    sourcePathComponents: safeNumber(value.sourcePathComponents, "source path components"),
    sourceEntriesVisited: safeNumber(value.sourceEntriesVisited, "source entries visited"),
    outputBytes: safeNumber(value.outputBytes, "output bytes"), itemsExamined: safeNumber(value.itemsExamined, "items examined"),
    itemsReturned: safeNumber(value.itemsReturned, "items returned"),
    allocationOperations: safeNumber(value.allocationOperations, "allocation operations"),
    peakAllocationBytes: safeNumber(value.peakAllocationBytes, "peak allocation bytes"),
    materializations: safeNumber(value.materializations, "materializations"),
  };
}

function operation(idempotencyKey?: Uint8Array) {
  const identity = idempotencyKey === undefined ? randomIdentity() : Uint8Array.from(idempotencyKey);
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
  const translated = MUTATION_STATUS_TO_DELETE[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid delete status");
  return translated;
}
function profileFromWire(value: FilesystemProfile): FsProfile {
  const translated = FILESYSTEM_PROFILE_TO_PROFILE[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "filesystem profile is unsupported");
  return translated;
}
function joinHistory(value: JoinOptions["history"]): JoinHistory {
  if (!Object.hasOwn(JOIN_HISTORY_FROM_PUBLIC, value)) throw new TypeError("join history is invalid");
  return JOIN_HISTORY_FROM_PUBLIC[value];
}
function conflictUse(value: ConflictUse): TransactionConflict["usage"] {
  const translated = CONFLICT_USE_TO_USAGE[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid conflict use");
  return translated;
}
function assertOpen(client: HostedClient): void {
  if (client.closed) throw new HostedFsError("closed", "hosted filesystem is closed");
}
function requireName(value: string): void { if (value.length === 0) throw new RangeError("name must be non-empty"); }
function positiveSafeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be positive`);
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
  return Uint8Array.from(value);
}
function copyOptionalBytes(value: Uint8Array | undefined): Uint8Array | undefined {
  return value === undefined ? undefined : Uint8Array.from(value);
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
      const codes: Record<Code, string | undefined> = {
        [Code.Canceled]: "cancelled",
        [Code.Unknown]: undefined,
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
