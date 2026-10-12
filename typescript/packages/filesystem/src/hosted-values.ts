import { type WorkspaceRef as WireWorkspaceRef, type SourceResponse as WireSourceResponse, SourceInvalidationReason as WireSourceInvalidationReason, type FileRecordSnapshot as WireFileRecordSnapshot, type TreeEntrySnapshot as WireTreeEntrySnapshot, type Conflict as WireConflict, type MutationStatus, type GenerationRef as WireGenerationRef, type RebaseStatus, type JoinStatus as WireJoinStatus, type Metadata as WireMetadata, type LogicalName as WireLogicalName, NameEncoding, type FileKind, type ExtentKind, type WorkCounters as WireWorkCounters, type FilesystemProfile, type JoinHistory, type ConflictUse } from "../generated/proto/filesystem/v2/filesystem_pb.js";
import { type SourceResult, type FileRecordSnapshot, type TreeEntrySnapshot, type TransactionConflict, type WorkspaceName, type MergeConflict, type WorkspaceCommit, type WorkspaceRebaseResult, type JoinResult, type WorkspaceMetadata, type WorkspaceFileKind, type WorkCounters, type WorkspaceDeleteStatus, type FsProfile, type JoinOptions } from "./contracts.js";
import { SOURCE_STATE_TO_STATUS, SOURCE_INVALIDATION_REASON_TO_REASON, isFilePayloadKind, SPARSE_TARGET_TO_TARGET, MUTATION_STATUS_TO_COMMIT, REBASE_STATUS_TO_STATUS, JOIN_STATUS_TO_STATUS, NAME_ENCODING_TO_PUBLIC, FILE_KIND_TO_KIND, EXTENT_KIND_TO_KIND, WORK_COUNTER_KEYS, MUTATION_STATUS_TO_DELETE, FILESYSTEM_PROFILE_TO_PROFILE, JOIN_HISTORY_FROM_PUBLIC, CONFLICT_USE_TO_USAGE } from "../generated/hosted-contract.js";
import { HostedFsError, required, requireBytes, equalBytes, exactBytes } from "./hosted-rpc.js";
import { copyOptionalBytes } from "./binding-values.js";

export function sourceResult(
  workspace: WireWorkspaceRef,
  response: WireSourceResponse,
): SourceResult {
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
    requireBytes(selected.generationId, 32, "source generation identity");
    if (!equalBytes(owner.workspaceId, workspace.workspaceId) || owner.name !== workspace.name) {
      throw new HostedFsError("invalid_response", "source generation belongs to another workspace");
    }
  }
  return { status, reason, generationId: copyOptionalBytes(selected?.generationId) };
}

export function fileSnapshot(value: WireFileRecordSnapshot): FileRecordSnapshot {
  const payloadKind = filePayloadKind(value.payloadKind);
  return {
    fileId: exactBytes(value.fileId, 16, "file identity"),
    fileKind: fileKind(value.fileKind),
    linkCount: value.linkCount,
    metadataObject: exactBytes(value.metadataObject, 33, "metadata object"),
    payloadKind,
    logicalBytes: (value.logicalBytes?.value.case === "present" ? value.logicalBytes.value.value : undefined),
    payloadObject: value.payloadObject.length === 0 ? undefined : exactBytes(value.payloadObject, 33, "payload object"),
    inlineBytes: payloadKind === "inline-regular" ? Uint8Array.from(value.inlineBytes) : undefined,
    deviceMajor: (value.deviceMajor?.value.case === "present" ? value.deviceMajor.value.value : undefined),
    deviceMinor: (value.deviceMinor?.value.case === "present" ? value.deviceMinor.value.value : undefined),
  };
}

function filePayloadKind(value: string): FileRecordSnapshot["payloadKind"] {
  if (!isFilePayloadKind(value)) throw new HostedFsError("invalid_response", "invalid file payload kind");
  return value;
}

export function treeEntry(value: WireTreeEntrySnapshot): TreeEntrySnapshot {
  return {
    name: logicalName(required(value.name, "tree entry name")),
    fileId: exactBytes(value.fileId, 16, "file identity"),
    fileKind: fileKind(value.fileKind),
  };
}

export function transactionConflict(value: WireConflict): TransactionConflict {
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

export function commit(status: MutationStatus, generationRef: WireGenerationRef | undefined): WorkspaceCommit {
  const translated = MUTATION_STATUS_TO_COMMIT[status];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid mutation status");
  return { status: translated, generationId: copyOptionalBytes(generationRef?.generationId) };
}

export function workspaceRebase(
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

export function joinResult(
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

export function metadata(value: WireMetadata | undefined): WorkspaceMetadata {
  const exact = required(value, "metadata");
  return {
    posixMode: (exact.posixMode?.value.case === "present" ? exact.posixMode.value.value : undefined), posixUid: (exact.posixUid?.value.case === "present" ? exact.posixUid.value.value : undefined),
    posixGid: (exact.posixGid?.value.case === "present" ? exact.posixGid.value.value : undefined), posixFlags: (exact.posixFlags?.value.case === "present" ? exact.posixFlags.value.value : undefined),
    windowsAttributes: (exact.windowsAttributes?.value.case === "present" ? exact.windowsAttributes.value.value : undefined), createdNs: (exact.createdNs?.value.case === "present" ? exact.createdNs.value.value : undefined),
    modifiedNs: (exact.modifiedNs?.value.case === "present" ? exact.modifiedNs.value.value : undefined), accessedNs: (exact.accessedNs?.value.case === "present" ? exact.accessedNs.value.value : undefined),
    changedNs: (exact.changedNs?.value.case === "present" ? exact.changedNs.value.value : undefined), hasNamedAttributes: exact.hasNamedAttributes,
    hasAcl: exact.hasAcl, hasSecurityDescriptor: exact.hasSecurityDescriptor,
  };
}

export function logicalName(value: WireLogicalName): WorkspaceName {
  const encoding = NAME_ENCODING_TO_PUBLIC[value.encoding];
  if (encoding === undefined) throw new HostedFsError("invalid_response", "invalid name encoding");
  return { encoding, bytes: Uint8Array.from(value.bytes) };
}

export function wireName(value: WorkspaceName): { readonly encoding: NameEncoding; readonly bytes: Uint8Array } {
  return {
    encoding: value.encoding === "utf8" ? NameEncoding.UTF8
      : value.encoding === "posix-bytes" ? NameEncoding.POSIX_BYTES : NameEncoding.WINDOWS_UTF16LE,
    bytes: value.bytes,
  };
}

export function fileKind(value: FileKind): WorkspaceFileKind {
  const translated = FILE_KIND_TO_KIND[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid file kind");
  return translated;
}

export function extentKind(value: ExtentKind): "hole" | "allocated-zero" | "content" {
  const translated = EXTENT_KIND_TO_KIND[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid extent kind");
  return translated;
}

export function workCounters(value: WireWorkCounters): WorkCounters {
  const result = {} as { -readonly [Key in keyof WorkCounters]: bigint };
  for (const key of WORK_COUNTER_KEYS) result[key] = BigInt(value[key]);
  return result;
}

export function deleteStatus(value: MutationStatus): WorkspaceDeleteStatus {
  const translated = MUTATION_STATUS_TO_DELETE[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid delete status");
  return translated;
}

export function profileFromWire(value: FilesystemProfile): FsProfile {
  const translated = FILESYSTEM_PROFILE_TO_PROFILE[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "filesystem profile is unsupported");
  return translated;
}

export function joinHistory(value: JoinOptions["history"]): JoinHistory {
  if (!Object.hasOwn(JOIN_HISTORY_FROM_PUBLIC, value)) throw new TypeError("join history is invalid");
  return JOIN_HISTORY_FROM_PUBLIC[value];
}

function conflictUse(value: ConflictUse): TransactionConflict["usage"] {
  const translated = CONFLICT_USE_TO_USAGE[value];
  if (translated === undefined) throw new HostedFsError("invalid_response", "invalid conflict use");
  return translated;
}
