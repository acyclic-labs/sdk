import type {
  BatchLookupEntry, DirectoryBindingChange, DirectoryPage, DirectoryRecordPage, FileRecordSnapshot,
  GenerationDiff, NamedAttributePage, NamedAttributeResult, StatResult, TreeEntrySnapshot, WorkCounters,
  WasmRawFileRecordSnapshot,
  WorkspaceFileKind,
} from "./contracts.js";
import { isFileKind } from "../generated/hosted-contract.js";
import { copyBytes } from "./binding-values.js";

/** Validates the generated binding file kind. */
export function projectFileKind(value: string): WorkspaceFileKind {
  if (!isFileKind(value)) throw new TypeError("filesystem binding returned an invalid file kind");
  return value;
}

export function copyFileRecord(record: FileRecordSnapshot | WasmRawFileRecordSnapshot): FileRecordSnapshot {
  return {
    ...record,
    fileKind: projectFileKind(record.fileKind),
    fileId: copyBytes(record.fileId),
    linkCount: BigInt(record.linkCount),
    metadataObject: copyBytes(record.metadataObject),
    logicalBytes: record.logicalBytes === undefined ? undefined : BigInt(record.logicalBytes),
    payloadObject: record.payloadObject === undefined ? undefined : copyBytes(record.payloadObject),
    inlineBytes: record.inlineBytes === undefined ? undefined : copyBytes(record.inlineBytes),
  };
}

export function copyBatchLookupEntries(entries: readonly BatchLookupEntry[]): BatchLookupEntry[] {
  return entries.map((entry) => ({
    ...entry,
    fileId: entry.fileId === undefined ? undefined : copyBytes(entry.fileId),
  }));
}

export function copyStatResult(value: {
  readonly exists: boolean;
  readonly record: FileRecordSnapshot | WasmRawFileRecordSnapshot | undefined;
  readonly metadataCanonicalBytes: Uint8Array | undefined;
}, work: WorkCounters): StatResult {
  return {
    exists: value.exists,
    record: value.record === undefined ? undefined : copyFileRecord(value.record),
    metadataCanonicalBytes: value.metadataCanonicalBytes === undefined ? undefined : copyBytes(value.metadataCanonicalBytes),
    work,
  };
}

export function copyNamedAttributeResult(value: { readonly exists: boolean; readonly bytes: Uint8Array | undefined },
  work: WorkCounters): NamedAttributeResult {
  return { exists: value.exists, bytes: value.bytes === undefined ? undefined : copyBytes(value.bytes), work };
}

export function copyNamedAttributePage(value: {
  readonly entries: readonly { readonly attributeClass: string; readonly name: Uint8Array }[];
  readonly hasMore: boolean;
},
  work: WorkCounters): NamedAttributePage {
  return { entries: value.entries.map(entry => {
    if (entry.attributeClass !== "posix-xattr" && entry.attributeClass !== "windows-stream" &&
        entry.attributeClass !== "mac-resource-fork") {
      throw new TypeError("named attribute result has an invalid class");
    }
    return { attributeClass: entry.attributeClass, name: copyBytes(entry.name) };
  }), hasMore: value.hasMore, work };
}

export function copyDirectoryPage(value: {
  readonly entries: readonly { readonly name: Uint8Array; readonly fileId: Uint8Array; readonly fileKind: WorkspaceFileKind }[];
  readonly hasMore: boolean;
},
  work: WorkCounters): DirectoryPage {
  return { entries: value.entries.map(entry => ({ name: copyBytes(entry.name),
    fileId: copyBytes(entry.fileId), fileKind: projectFileKind(entry.fileKind) })), hasMore: value.hasMore, work };
}

export function copyDirectoryRecordPage(value: {
  readonly entries: readonly {
    readonly name: Uint8Array;
    readonly record: FileRecordSnapshot | WasmRawFileRecordSnapshot;
    readonly metadataCanonicalBytes: Uint8Array;
  }[];
  readonly hasMore: boolean;
}, work: WorkCounters): DirectoryRecordPage {
  return { entries: value.entries.map(entry => ({ name: copyBytes(entry.name),
    record: copyFileRecord(entry.record), metadataCanonicalBytes: copyBytes(entry.metadataCanonicalBytes) })),
    hasMore: value.hasMore, work };
}

function copyTreeEntry(entry: TreeEntrySnapshot): TreeEntrySnapshot {
  return {
    ...entry,
    fileKind: projectFileKind(entry.fileKind),
    fileId: copyBytes(entry.fileId),
    name: { ...entry.name, bytes: copyBytes(entry.name.bytes) },
  };
}

export function copyBindingChange(change: {
  readonly directoryId: Uint8Array;
  readonly name: TreeEntrySnapshot["name"];
  readonly before: TreeEntrySnapshot | undefined;
  readonly after: TreeEntrySnapshot | undefined;
}): DirectoryBindingChange {
  return {
    ...change,
    directoryId: copyBytes(change.directoryId),
    name: { ...change.name, bytes: copyBytes(change.name.bytes) },
    before: change.before === undefined ? undefined : copyTreeEntry(change.before),
    after: change.after === undefined ? undefined : copyTreeEntry(change.after),
  };
}

export function copyGenerationDiff(value: {
  readonly files: readonly {
    readonly fileId: Uint8Array;
    readonly before: FileRecordSnapshot | WasmRawFileRecordSnapshot | undefined;
    readonly after: FileRecordSnapshot | WasmRawFileRecordSnapshot | undefined;
  }[];
  readonly bindings: readonly {
    readonly directoryId: Uint8Array;
    readonly name: TreeEntrySnapshot["name"];
    readonly before: TreeEntrySnapshot | undefined;
    readonly after: TreeEntrySnapshot | undefined;
  }[];
  readonly truncated: boolean;
}, work: WorkCounters): GenerationDiff {
  return {
    files: value.files.map(change => ({
      fileId: copyBytes(change.fileId),
      before: change.before === undefined ? undefined : copyFileRecord(change.before),
      after: change.after === undefined ? undefined : copyFileRecord(change.after),
    })),
    bindings: value.bindings.map(copyBindingChange),
    truncated: value.truncated,
    work,
  };
}
