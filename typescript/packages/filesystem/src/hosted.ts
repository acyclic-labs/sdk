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

type Json = null | boolean | number | string | readonly Json[] | JsonObject;
type JsonObject = { readonly [name: string]: Json };
type Command = JsonObject & { readonly method: string };

const DEFAULT_MAXIMUM_RESPONSE_BYTES = 24 * 1024 * 1024;

export class HostedFsError extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "HostedFsError";
  }
}

class Client {
  readonly endpoint: string;
  readonly bearerToken: string;
  readonly maximumResponseBytes: number;
  readonly send: typeof globalThis.fetch;
  #nextRequest = 0;
  #closed = false;

  constructor(options: HostedFsOptions) {
    const endpoint = new URL(options.endpoint);
    if (endpoint.protocol !== "https:" && endpoint.protocol !== "http:") {
      throw new RangeError("hosted filesystem endpoint must use HTTP or HTTPS");
    }
    if (options.bearerToken.length === 0) throw new RangeError("bearer token must be non-empty");
    const maximum = options.maximumResponseBytes ?? DEFAULT_MAXIMUM_RESPONSE_BYTES;
    if (!Number.isSafeInteger(maximum) || maximum <= 0) {
      throw new RangeError("maximum response bytes must be a positive safe integer");
    }
    this.endpoint = endpoint.href;
    this.bearerToken = options.bearerToken;
    this.maximumResponseBytes = maximum;
    this.send = options.fetch ?? globalThis.fetch;
    if (this.send === undefined) throw new TypeError("this runtime does not provide fetch");
  }

  close(): void { this.#closed = true; }

  async rpc(command: Command): Promise<Record<string, unknown>> {
    if (this.#closed) throw new HostedFsError("closed", "hosted filesystem is closed");
    const id = `fs-${++this.#nextRequest}`;
    const response = await this.send(this.endpoint, {
      method: "POST",
      headers: {
        authorization: `Bearer ${this.bearerToken}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ id, ...command }),
    });
    const bytes = await readBounded(response, this.maximumResponseBytes);
    let decoded: unknown;
    try { decoded = JSON.parse(new TextDecoder().decode(bytes)); }
    catch { throw new HostedFsError("invalid_response", "filesystem response is not JSON"); }
    const envelope = object(decoded, "filesystem response");
    if (string(envelope.id, "response id") !== id) {
      throw new HostedFsError("invalid_response", "filesystem response identity does not match");
    }
    if (!boolean(envelope.ok, "response status")) {
      const error = object(envelope.error, "filesystem error");
      throw new HostedFsError(string(error.code, "error code"), string(error.message, "error message"));
    }
    if (!response.ok) throw new HostedFsError("http_error", `filesystem HTTP status ${response.status}`);
    return object(envelope.result, "filesystem result");
  }
}

export async function openHostedFs(options: HostedFsOptions): Promise<FsEngine> {
  const client = new Client(options);
  const advertised = await client.rpc({ method: "capabilities" });
  const capabilities: EngineCapabilities = {
    version: string(advertised.version, "service version"),
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
      requireName(name);
      return workspace(client, await client.rpc({ method: "create_workspace", name }));
    },
    async openWorkspace(name) {
      requireName(name);
      return workspace(client, await client.rpc({ method: "open_workspace", name }));
    },
    close() { client.close(); },
  };
}

function workspace(client: Client, value: Record<string, unknown>): FsWorkspace {
  const name = string(value.name, "workspace name");
  const id = fromHex(value.workspaceId, 16, "workspace identity");
  const result: FsWorkspace = {
    name,
    id,
    async head() { return generationId(await client.rpc({ method: "workspace_head", workspace: name })); },
    async sync() { return generation(client, name, id, await this.head()); },
    async checkpoint(label) {
      requireName(label);
      return generation(client, name, id, generationId(await client.rpc({ method: "workspace_checkpoint", workspace: name, label })));
    },
    async pin(identity) {
      requireName(identity);
      return generation(client, name, id, generationId(await client.rpc({ method: "workspace_pin", workspace: name, identity })));
    },
    async delete(idempotencyKey) {
      const response = await client.rpc(optionalIdentity({ method: "workspace_delete", workspace: name }, idempotencyKey));
      return deleteStatus(response.status);
    },
    read(path, maximumBytes) { return read(client, name, undefined, path, maximumBytes); },
    readRange(path, offset, length) { return readRange(client, name, undefined, path, offset, length); },
    stat(path) { return stat(client, name, undefined, path); },
    listDirectory(path, after, maximumEntries) { return list(client, name, undefined, path, after, maximumEntries); },
    async readSymbolicLink(path) {
      const response = await client.rpc({ method: "workspace_read_symbolic_link", workspace: name, path });
      return fromBase64(response.targetBase64, "symbolic-link target");
    },
    planExtents(path, offset, length, maximumSpans) {
      return extents(client, name, undefined, path, offset, length, maximumSpans);
    },
    async write(path, bytes) {
      return commit(await client.rpc({ method: "workspace_write", workspace: name, path, bytes_base64: toBase64(bytes) }));
    },
    async remove(path) {
      return commit(await client.rpc({ method: "workspace_remove", workspace: name, path }));
    },
    async fork(destination) {
      requireName(destination);
      return workspace(client, await client.rpc({ method: "workspace_fork", workspace: name, destination }));
    },
    async forkAt(destination, selected) {
      requireName(destination);
      const exact = requireGeneration(selected, client, name);
      return workspace(client, await client.rpc({ method: "workspace_fork", workspace: name, destination, generation_id: toHex(exact.id) }));
    },
    async beginTransaction(idempotencyKey) {
      const response = await client.rpc(optionalIdentity({ method: "workspace_begin_transaction", workspace: name }, idempotencyKey));
      return transaction(client, string(response.transactionId, "transaction identity"));
    },
    async liveRebase(options, idempotencyKey) {
      validateRebase(options);
      return rebaseResult(await client.rpc(optionalIdentity({
        method: "workspace_live_rebase", workspace: name,
        maximum_generations: options.maximumGenerations,
        maximum_changes: options.maximumChanges,
        maximum_conflicts: options.maximumConflicts,
      }, idempotencyKey)));
    },
    async diff(from, to, maximumChanges) {
      positiveInteger(maximumChanges, "maximum changes");
      const first = requireGeneration(from, client, name);
      const second = requireGeneration(to, client, name);
      return changeSet(client, name, id, await client.rpc({
        method: "workspace_diff", workspace: name,
        from_generation_id: toHex(first.id), to_generation_id: toHex(second.id),
        maximum_changes: maximumChanges,
      }));
    },
    async joinInto(target, options) {
      validateJoin(options);
      const destination = requireWorkspace(target, client);
      const response = await client.rpc({
        method: "workspace_plan_join", source: name, target: destination.name,
        history: options.history, maximum_generations: options.maximumGenerations,
        maximum_changes: options.maximumChanges, maximum_conflicts: options.maximumConflicts,
      });
      return joinPlan(client, response);
    },
  };
  workspaceOwners.set(result, { client, name });
  return result;
}

type GenerationHandle = { readonly client: Client; readonly workspace: string; readonly id: Uint8Array };
const generationOwners = new WeakMap<FsGeneration, GenerationHandle>();
const workspaceOwners = new WeakMap<FsWorkspace, { readonly client: Client; readonly name: string }>();

function generation(client: Client, workspaceName: string, workspaceId: Uint8Array, id: Uint8Array): FsGeneration {
  const result: FsGeneration = {
    id: id.slice(), workspaceId: workspaceId.slice(),
    read(path, maximumBytes) { return read(client, workspaceName, id, path, maximumBytes); },
    readRange(path, offset, length) { return readRange(client, workspaceName, id, path, offset, length); },
    stat(path) { return stat(client, workspaceName, id, path); },
    listDirectory(path, after, maximumEntries) { return list(client, workspaceName, id, path, after, maximumEntries); },
    async readSymbolicLink(path) {
      const response = await client.rpc({ method: "workspace_read_symbolic_link", workspace: workspaceName, generation_id: toHex(id), path });
      return fromBase64(response.targetBase64, "symbolic-link target");
    },
    planExtents(path, offset, length, maximumSpans) { return extents(client, workspaceName, id, path, offset, length, maximumSpans); },
    async pin(identity) {
      requireName(identity);
      return generation(client, workspaceName, workspaceId, generationId(await client.rpc({ method: "workspace_pin", workspace: workspaceName, generation_id: toHex(id), identity })));
    },
  };
  generationOwners.set(result, { client, workspace: workspaceName, id });
  return result;
}

function requireGeneration(value: FsGeneration, client: Client, workspaceName: string): GenerationHandle {
  const owned = generationOwners.get(value);
  if (owned === undefined || owned.client !== client || owned.workspace !== workspaceName) {
    throw new TypeError("generation belongs to another hosted workspace");
  }
  return owned;
}

function requireWorkspace(value: FsWorkspace, client: Client): { readonly name: string } {
  const owned = workspaceOwners.get(value);
  if (owned === undefined || owned.client !== client) throw new TypeError("workspace belongs to another hosted runtime");
  return owned;
}

async function read(client: Client, workspace: string, selected: Uint8Array | undefined, path: string, maximumBytes: bigint): Promise<Uint8Array> {
  positive(maximumBytes, "maximum read bytes");
  const response = await client.rpc(withGeneration({ method: "workspace_read", workspace, path, maximum_bytes: decimal(maximumBytes) }, selected));
  return fromBase64(response.bytesBase64, "file bytes");
}

async function readRange(client: Client, workspace: string, selected: Uint8Array | undefined, path: string, offset: bigint, length: bigint): Promise<Uint8Array> {
  nonnegative(offset, "read offset"); nonnegative(length, "read length");
  const response = await client.rpc(withGeneration({ method: "workspace_read_range", workspace, path, offset: decimal(offset), length: decimal(length) }, selected));
  return fromBase64(response.bytesBase64, "file bytes");
}

async function stat(client: Client, workspace: string, selected: Uint8Array | undefined, path: string): Promise<WorkspaceStat> {
  const value = await client.rpc(withGeneration({ method: "workspace_stat", workspace, path }, selected));
  const metadata = object(value.metadata, "workspace metadata");
  return {
    fileId: fromHex(value.fileId, 16, "file identity"), kind: fileKind(value.fileKind),
    linkCount: bigint(value.linkCount, "link count"), logicalBytes: optionalBigint(value.logicalBytes, "logical bytes"),
    metadata: metadataValue(metadata),
  };
}

async function list(client: Client, workspace: string, selected: Uint8Array | undefined, path: string, after: WorkspaceName | undefined, maximumEntries: number): Promise<WorkspaceDirectoryPage> {
  positiveInteger(maximumEntries, "maximum entries");
  let command: Command = withGeneration({ method: "workspace_list_directory", workspace, path, maximum_entries: maximumEntries }, selected);
  if (after !== undefined) command = { ...command, after: { encoding: wireEncoding(after.encoding), bytes_base64: toBase64(after.bytes) } };
  const value = await client.rpc(command);
  return { entries: array(value.entries, "directory entries").map((entry) => {
    const item = object(entry, "directory entry");
    return { name: logicalName(item.name), fileId: fromHex(item.fileId, 16, "file identity"), kind: fileKind(item.fileKind) };
  }), hasMore: boolean(value.hasMore, "directory continuation") };
}

async function extents(client: Client, workspace: string, selected: Uint8Array | undefined, path: string, offset: bigint, length: bigint, maximumSpans: number): Promise<WorkspaceExtentPlan> {
  nonnegative(offset, "extent offset"); nonnegative(length, "extent length"); positiveInteger(maximumSpans, "maximum spans");
  const value = await client.rpc(withGeneration({ method: "workspace_plan_extents", workspace, path, offset: decimal(offset), length: decimal(length), maximum_spans: maximumSpans }, selected));
  return { spans: array(value.spans, "extent spans").map((entry) => {
    const span = object(entry, "extent span");
    const kind = string(span.kind, "extent kind");
    if (kind !== "hole" && kind !== "allocated-zero" && kind !== "content") throw new TypeError("invalid extent kind");
    return { offset: bigint(span.offset, "extent offset"), length: bigint(span.length, "extent length"), sourceEnd: bigint(span.sourceEnd, "extent source end"), kind };
  }) };
}

function transaction(client: Client, transactionId: string): FsTransaction {
  let closed = false;
  const stage = async (operation: JsonObject): Promise<void> => {
    if (closed) throw new HostedFsError("closed", "transaction is closed");
    await client.rpc({ method: "workspace_stage_transaction", transaction_id: transactionId, operation });
  };
  return {
    createDirAll: (path) => stage({ kind: "create-dir-all", path }),
    createDirectory: (path) => stage({ kind: "create-directory", path }),
    createSymbolicLink: (path, target) => stage({ kind: "create-symbolic-link", path, target_base64: toBase64(target) }),
    write: (path, bytes) => stage({ kind: "write", path, bytes_base64: toBase64(bytes) }),
    remove: (path) => stage({ kind: "remove", path }),
    copy: (source, destination) => stage({ kind: "copy", source, destination }),
    rename: (source, destination) => stage({ kind: "rename", source, destination }),
    hardLink: (source, destination) => stage({ kind: "hard-link", source, destination }),
    writeRange: (path, offset, bytes) => { nonnegative(offset, "write offset"); return stage({ kind: "write-range", path, offset: decimal(offset), bytes_base64: toBase64(bytes) }); },
    resize: (path, logicalBytes) => { nonnegative(logicalBytes, "logical bytes"); return stage({ kind: "resize", path, logical_bytes: decimal(logicalBytes) }); },
    zeroRange: (path, offset, length, allocated, extend) => { nonnegative(offset, "zero offset"); nonnegative(length, "zero length"); return stage({ kind: "zero-range", path, offset: decimal(offset), length: decimal(length), allocated, extend }); },
    preallocate: (path, offset, length, keepSize) => { nonnegative(offset, "preallocation offset"); nonnegative(length, "preallocation length"); return stage({ kind: "preallocate", path, offset: decimal(offset), length: decimal(length), keep_size: keepSize }); },
    cloneRange: (source, sourceOffset, destination, destinationOffset, length) => { nonnegative(sourceOffset, "source offset"); nonnegative(destinationOffset, "destination offset"); nonnegative(length, "clone length"); return stage({ kind: "clone-range", source, source_offset: decimal(sourceOffset), destination, destination_offset: decimal(destinationOffset), length: decimal(length) }); },
    async rebase(maximumConflicts) { positiveInteger(maximumConflicts, "maximum conflicts"); return transactionRebase(await client.rpc({ method: "workspace_rebase_transaction", transaction_id: transactionId, maximum_conflicts: maximumConflicts })); },
    async commit() { if (closed) throw new HostedFsError("closed", "transaction is closed"); return commit(await client.rpc({ method: "workspace_commit_transaction", transaction_id: transactionId })); },
    async close() { if (!closed) { await client.rpc({ method: "workspace_close_transaction", transaction_id: transactionId }); closed = true; } },
  };
}

function changeSet(client: Client, workspaceName: string, workspaceId: Uint8Array, value: Record<string, unknown>): FsChangeSet {
  const from = generation(client, workspaceName, workspaceId, fromHex(value.fromGenerationId, 32, "from generation"));
  const to = generation(client, workspaceName, workspaceId, fromHex(value.toGenerationId, 32, "to generation"));
  const diff = generationDiff(value);
  const result: FsChangeSet = {
    from, to, changes: () => diff,
    async compose(next, maximumChanges) {
      positiveInteger(maximumChanges, "maximum changes");
      const owned = changeSetOwners.get(next);
      if (owned === undefined || owned.client !== client || owned.workspace !== workspaceName || toHex(owned.from) !== toHex(to.id)) throw new TypeError("change sets are not contiguous in this hosted workspace");
      return changeSet(client, workspaceName, workspaceId, await client.rpc({ method: "workspace_diff", workspace: workspaceName, from_generation_id: toHex(from.id), to_generation_id: toHex(owned.to), maximum_changes: maximumChanges }));
    },
  };
  changeSetOwners.set(result, { client, workspace: workspaceName, from: from.id, to: to.id });
  return result;
}

const changeSetOwners = new WeakMap<FsChangeSet, { readonly client: Client; readonly workspace: string; readonly from: Uint8Array; readonly to: Uint8Array }>();

function joinPlan(client: Client, value: Record<string, unknown>): FsJoinPlan {
  const planId = string(value.planId, "join plan identity");
  const targetHead = fromHex(value.targetGenerationId, 32, "target generation");
  const commonAncestor = fromHex(value.commonAncestorGenerationId, 32, "common ancestor");
  let closed = false;
  return {
    targetHead, commonAncestor,
    async apply(ifTarget, idempotencyKey) {
      if (closed) throw new HostedFsError("closed", "join plan is closed");
      requireBytes(ifTarget, 32, "target generation");
      return joinResult(await client.rpc(optionalIdentity({ method: "workspace_apply_join", plan_id: planId, if_target_generation_id: toHex(ifTarget) }, idempotencyKey)));
    },
    async close() { if (!closed) { await client.rpc({ method: "workspace_close_join_plan", plan_id: planId }); closed = true; } },
  };
}

function commit(value: Record<string, unknown>): WorkspaceCommit {
  const status = string(value.status, "commit status");
  if (status !== "committed" && status !== "already-committed" && status !== "conflict" && status !== "fenced" && status !== "idempotency-conflict") throw new TypeError("invalid commit status");
  return { status: status as WorkspaceCommitStatus, generationId: value.generationId === undefined || value.generationId === null ? undefined : fromHex(value.generationId, 32, "generation identity") };
}

function rebaseResult(value: Record<string, unknown>): WorkspaceRebaseResult {
  const status = string(value.status, "rebase status");
  if (status !== "rebased" && status !== "already-rebased" && status !== "current" && status !== "stale" && status !== "conflicted" && status !== "fenced" && status !== "idempotency-conflict") throw new TypeError("invalid rebase status");
  return { status: status as WorkspaceRebaseStatus, generationId: optionalHex(value.generationId, 32, "generation identity"), conflicts: conflicts(value.conflicts), truncated: boolean(value.truncated, "conflict truncation") };
}

function joinResult(value: Record<string, unknown>): JoinResult {
  const status = string(value.status, "join status");
  if (status !== "applied" && status !== "already-applied" && status !== "no-changes" && status !== "stale-target" && status !== "conflicted" && status !== "fenced" && status !== "idempotency-conflict") throw new TypeError("invalid join status");
  return { status: status as JoinStatus, generationId: optionalHex(value.generationId, 32, "generation identity"), conflicts: conflicts(value.conflicts), truncated: boolean(value.truncated, "conflict truncation") };
}

function transactionRebase(value: Record<string, unknown>): TransactionRebaseResult {
  const status = string(value.status, "transaction rebase status");
  if (status !== "rebased" && status !== "conflicted") throw new TypeError("invalid transaction rebase status");
  return { status, generationId: optionalHex(value.generationId, 32, "generation identity"), conflicts: array(value.conflicts, "transaction conflicts").map(transactionConflict), truncated: boolean(value.truncated, "conflict truncation") };
}

function transactionConflict(value: unknown): TransactionConflict {
  const item = object(value, "transaction conflict");
  const region = string(item.region, "conflict region");
  if (region !== "file-record" && region !== "metadata" && region !== "file-length" && region !== "content-range" && region !== "sparse-seek" && region !== "directory-name" && region !== "directory-range") throw new TypeError("invalid conflict region");
  const usage = string(item.usage, "dependency use");
  if (usage !== "observation" && usage !== "mutation" && usage !== "observation-and-mutation") throw new TypeError("invalid dependency use");
  const sparse = item.sparseTarget;
  if (sparse !== undefined && sparse !== null && sparse !== "data" && sparse !== "hole") throw new TypeError("invalid sparse target");
  return { region, fileId: optionalHex(item.fileId, 16, "file identity"), directoryId: optionalHex(item.directoryId, 16, "directory identity"), offset: optionalBigint(item.offset, "conflict offset"), length: optionalBigint(item.length, "conflict length"), sparseTarget: sparse === null || sparse === undefined ? undefined : sparse, name: item.name === null || item.name === undefined ? undefined : logicalName(item.name), maximumEntries: item.maximumEntries === null || item.maximumEntries === undefined ? undefined : integer(item.maximumEntries, "maximum entries"), usage, expected: optionalHex(item.expected, 32, "expected digest"), actual: optionalHex(item.actual, 32, "actual digest") };
}

function conflicts(value: unknown): readonly MergeConflict[] { return array(value, "merge conflicts").map((entry) => { const item = object(entry, "merge conflict"); const kind = string(item.kind, "conflict kind"); if (kind === "file") return { kind, fileId: fromHex(item.fileId, 16, "file identity") }; if (kind === "binding") return { kind, directoryId: fromHex(item.directoryId, 16, "directory identity"), name: logicalName(item.name) }; throw new TypeError("invalid merge conflict"); }); }

function generationDiff(value: Record<string, unknown>): GenerationDiff {
  return { files: array(value.files, "file changes").map(fileChange), bindings: array(value.bindings, "binding changes").map(bindingChange), truncated: boolean(value.truncated, "diff truncation"), work: work(value.work) };
}
function fileChange(value: unknown): FileRecordChange { const item = object(value, "file change"); return { fileId: fromHex(item.fileId, 16, "file identity"), before: item.before === null ? undefined : fileSnapshot(item.before), after: item.after === null ? undefined : fileSnapshot(item.after) }; }
function fileSnapshot(value: unknown): FileRecordSnapshot { const item = object(value, "file record"); return { fileId: fromHex(item.fileId, 16, "file identity"), fileKind: fileKind(item.fileKind), linkCount: bigint(item.linkCount, "link count"), metadataObject: fromHex(item.metadataObject, 33, "metadata object"), payloadKind: string(item.payloadKind, "payload kind"), logicalBytes: optionalBigint(item.logicalBytes, "logical bytes"), payloadObject: optionalHex(item.payloadObject, 33, "payload object"), inlineBytes: item.inlineBytesBase64 === null || item.inlineBytesBase64 === undefined ? undefined : fromBase64(item.inlineBytesBase64, "inline bytes"), deviceMajor: optionalInteger(item.deviceMajor, "device major"), deviceMinor: optionalInteger(item.deviceMinor, "device minor") }; }
function bindingChange(value: unknown): DirectoryBindingChange { const item = object(value, "binding change"); return { directoryId: fromHex(item.directoryId, 16, "directory identity"), name: logicalName(item.name), before: item.before === null ? undefined : treeEntry(item.before), after: item.after === null ? undefined : treeEntry(item.after) }; }
function treeEntry(value: unknown): TreeEntrySnapshot { const item = object(value, "tree entry"); return { name: logicalName(item.name), fileId: fromHex(item.fileId, 16, "file identity"), fileKind: fileKind(item.fileKind) }; }

function metadataValue(value: Record<string, unknown>): WorkspaceMetadata { return { posixMode: optionalInteger(value.posixMode, "POSIX mode"), posixUid: optionalInteger(value.posixUid, "POSIX uid"), posixGid: optionalInteger(value.posixGid, "POSIX gid"), posixFlags: optionalBigint(value.posixFlags, "POSIX flags"), windowsAttributes: optionalInteger(value.windowsAttributes, "Windows attributes"), createdNs: optionalBigint(value.createdNs, "created timestamp"), modifiedNs: optionalBigint(value.modifiedNs, "modified timestamp"), accessedNs: optionalBigint(value.accessedNs, "accessed timestamp"), changedNs: optionalBigint(value.changedNs, "changed timestamp"), hasNamedAttributes: boolean(value.hasNamedAttributes, "named attributes"), hasAcl: boolean(value.hasAcl, "ACL"), hasSecurityDescriptor: boolean(value.hasSecurityDescriptor, "security descriptor") }; }
function logicalName(value: unknown): WorkspaceName { const item = object(value, "logical name"); const encoding = string(item.encoding, "name encoding"); if (encoding !== "utf8" && encoding !== "posix_bytes" && encoding !== "windows_utf16le") throw new TypeError("invalid name encoding"); return { encoding: encoding === "posix_bytes" ? "posix-bytes" : encoding === "windows_utf16le" ? "windows-utf16le" : encoding, bytes: fromBase64(item.bytesBase64, "name bytes") }; }
function wireEncoding(value: WorkspaceName["encoding"]): string { return value === "posix-bytes" ? "posix_bytes" : value === "windows-utf16le" ? "windows_utf16le" : value; }
function fileKind(value: unknown): WorkspaceFileKind { const kind = string(value, "file kind").replaceAll("_", "-"); if (kind !== "regular" && kind !== "directory" && kind !== "symbolic-link" && kind !== "fifo" && kind !== "socket" && kind !== "character-device" && kind !== "block-device" && kind !== "reparse-point" && kind !== "mount-boundary") throw new TypeError("invalid file kind"); return kind; }

function work(value: unknown): WorkCounters { const item = object(value, "work counters"); const read = (name: string): number => integer(item[name], `work.${name}`); return { authorityRecordsRead: read("authorityRecordsRead"), authorityRecordsAppended: read("authorityRecordsAppended"), authorityBytesRead: read("authorityBytesRead"), authorityBytesWritten: read("authorityBytesWritten"), objectProbes: read("objectProbes"), backendReadOperations: read("backendReadOperations"), backendWriteOperations: read("backendWriteOperations"), durabilityOperations: read("durabilityOperations"), pageReads: read("pageReads"), pageWrites: read("pageWrites"), objectBytesRead: read("objectBytesRead"), objectBytesWritten: read("objectBytesWritten"), bytesHashed: read("bytesHashed"), bytesCopied: read("bytesCopied"), bytesEncoded: read("bytesEncoded"), sourceBytesRead: read("sourceBytesRead"), outputBytes: read("outputBytes"), itemsExamined: read("itemsExamined"), itemsReturned: read("itemsReturned"), allocationOperations: read("allocationOperations"), peakAllocationBytes: read("peakAllocationBytes"), materializations: read("materializations") }; }

function withGeneration(command: Command, selected: Uint8Array | undefined): Command { return selected === undefined ? command : { ...command, generation_id: toHex(selected) }; }
function optionalIdentity(command: Command, identity: Uint8Array | undefined): Command { if (identity === undefined) return command; requireBytes(identity, 16, "idempotency key"); return { ...command, idempotency_key: toHex(identity) }; }
function generationId(value: Record<string, unknown>): Uint8Array { return fromHex(value.generationId, 32, "generation identity"); }
function deleteStatus(value: unknown): WorkspaceDeleteStatus { const status = string(value, "delete status"); if (status !== "deleted" && status !== "already-deleted" && status !== "conflict" && status !== "idempotency-conflict") throw new TypeError("invalid delete status"); return status; }
function validateJoin(value: JoinOptions): void { positiveInteger(value.maximumGenerations, "maximum generations"); positiveInteger(value.maximumChanges, "maximum changes"); positiveInteger(value.maximumConflicts, "maximum conflicts"); }
function validateRebase(value: WorkspaceRebaseOptions): void { positiveInteger(value.maximumGenerations, "maximum generations"); positiveInteger(value.maximumChanges, "maximum changes"); positiveInteger(value.maximumConflicts, "maximum conflicts"); }
function requireName(value: string): void { if (value.length === 0) throw new RangeError("name must be non-empty"); }
function positive(value: bigint, name: string): void { if (value <= 0n || value > 18_446_744_073_709_551_615n) throw new RangeError(`${name} must be a positive u64`); }
function nonnegative(value: bigint, name: string): void { if (value < 0n || value > 18_446_744_073_709_551_615n) throw new RangeError(`${name} must be a u64`); }
function decimal(value: bigint): string { return value.toString(10); }
function positiveInteger(value: number, name: string): void { if (!Number.isSafeInteger(value) || value <= 0 || value > 0xffff_ffff) throw new RangeError(`${name} must be a positive u32`); }
function object(value: unknown, name: string): Record<string, unknown> { if (typeof value !== "object" || value === null || Array.isArray(value)) throw new TypeError(`${name} must be an object`); return value as Record<string, unknown>; }
function array(value: unknown, name: string): readonly unknown[] { if (!Array.isArray(value)) throw new TypeError(`${name} must be an array`); return value; }
function string(value: unknown, name: string): string { if (typeof value !== "string") throw new TypeError(`${name} must be a string`); return value; }
function boolean(value: unknown, name: string): boolean { if (typeof value !== "boolean") throw new TypeError(`${name} must be boolean`); return value; }
function integer(value: unknown, name: string): number { if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new TypeError(`${name} must be a nonnegative safe integer`); return value; }
function optionalInteger(value: unknown, name: string): number | undefined { return value === null || value === undefined ? undefined : integer(value, name); }
function bigint(value: unknown, name: string): bigint { if (typeof value === "string" && /^[0-9]+$/.test(value)) return BigInt(value); if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return BigInt(value); throw new TypeError(`${name} must be an exact nonnegative integer`); }
function optionalBigint(value: unknown, name: string): bigint | undefined { return value === null || value === undefined ? undefined : bigint(value, name); }
function requireBytes(value: Uint8Array, length: number, name: string): void { if (!(value instanceof Uint8Array) || value.byteLength !== length) throw new RangeError(`${name} must be ${length} bytes`); }
function toHex(value: Uint8Array): string { let result = ""; for (const byte of value) result += byte.toString(16).padStart(2, "0"); return result; }
function fromHex(value: unknown, length: number, name: string): Uint8Array { const text = string(value, name); if (text.length !== length * 2 || !/^[0-9a-f]+$/.test(text)) throw new TypeError(`${name} is not canonical lowercase hex`); const bytes = new Uint8Array(length); for (let index = 0; index < length; index++) bytes[index] = Number.parseInt(text.slice(index * 2, index * 2 + 2), 16); return bytes; }
function optionalHex(value: unknown, length: number, name: string): Uint8Array | undefined { return value === null || value === undefined ? undefined : fromHex(value, length, name); }
function toBase64(value: Uint8Array): string { let binary = ""; const block = 0x8000; for (let offset = 0; offset < value.length; offset += block) binary += String.fromCharCode(...value.subarray(offset, offset + block)); return btoa(binary); }
function fromBase64(value: unknown, name: string): Uint8Array { const text = string(value, name); let decoded: string; try { decoded = atob(text); } catch { throw new TypeError(`${name} is not base64`); } const bytes = new Uint8Array(decoded.length); for (let index = 0; index < decoded.length; index++) bytes[index] = decoded.charCodeAt(index); return bytes; }

async function readBounded(response: Response, maximum: number): Promise<Uint8Array> {
  const declared = response.headers.get("content-length");
  if (declared !== null && (!/^[0-9]+$/.test(declared) || BigInt(declared) > BigInt(maximum))) throw new HostedFsError("response_too_large", "filesystem response exceeds the configured bound");
  if (response.body === null) { const bytes = new Uint8Array(await response.arrayBuffer()); if (bytes.byteLength > maximum) throw new HostedFsError("response_too_large", "filesystem response exceeds the configured bound"); return bytes; }
  const reader = response.body.getReader(); const chunks: Uint8Array[] = []; let total = 0;
  try { for (;;) { const next = await reader.read(); if (next.done) break; total += next.value.byteLength; if (total > maximum) { await reader.cancel(); throw new HostedFsError("response_too_large", "filesystem response exceeds the configured bound"); } chunks.push(next.value); } }
  finally { reader.releaseLock(); }
  const joined = new Uint8Array(total); let offset = 0; for (const chunk of chunks) { joined.set(chunk, offset); offset += chunk.byteLength; } return joined;
}
