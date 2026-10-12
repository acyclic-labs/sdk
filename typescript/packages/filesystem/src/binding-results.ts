import type {
  BatchLookupEntry, DirectoryBindingChange, DirectoryPage, DirectoryRecordPage, FileRecordSnapshot,
  GenerationDiff, NamedAttributePage, NamedAttributeResult, StatResult, TreeEntrySnapshot, WorkCounters,
  WorkspaceFileKind,
} from "./contracts.js";
import { isFileKind } from "../generated/hosted-contract.js";
import { copyName } from "./binding-values.js";

/** Validates the generated binding file kind. */
export function projectFileKind(value: string): WorkspaceFileKind {
  if (!isFileKind(value)) throw new TypeError("filesystem binding returned an invalid file kind");
  return value;
}

export function copyFileRecord(record: FileRecordSnapshot): FileRecordSnapshot {
  return {
    ...record,
    fileKind: projectFileKind(record.fileKind),
    fileId: new Uint8Array(record.fileId),
    linkCount: BigInt(record.linkCount),
    metadataObject: new Uint8Array(record.metadataObject),
    logicalBytes: record.logicalBytes === undefined ? undefined : BigInt(record.logicalBytes),
    payloadObject: record.payloadObject === undefined ? undefined : new Uint8Array(record.payloadObject),
    inlineBytes: record.inlineBytes === undefined ? undefined : new Uint8Array(record.inlineBytes),
  };
}

export function copyBatchLookupEntries(entries: readonly BatchLookupEntry[]): BatchLookupEntry[] {
  return entries.map((entry) => ({
    ...entry,
    fileId: entry.fileId === undefined ? undefined : new Uint8Array(entry.fileId),
  }));
}

export function copyStatResult(value: {
  readonly exists: boolean;
  readonly record: FileRecordSnapshot | undefined;
  readonly metadataCanonicalBytes: Uint8Array | undefined;
}, work: WorkCounters): StatResult {
  return {
    exists: value.exists,
    record: value.record === undefined ? undefined : copyFileRecord(value.record),
    metadataCanonicalBytes: value.metadataCanonicalBytes === undefined ? undefined : new Uint8Array(value.metadataCanonicalBytes),
    work,
  };
}

export function copyNamedAttributeResult(value: { readonly exists: boolean; readonly bytes: Uint8Array | undefined },
  work: WorkCounters): NamedAttributeResult {
  return { exists: value.exists, bytes: value.bytes === undefined ? undefined : new Uint8Array(value.bytes), work };
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
    return { attributeClass: entry.attributeClass, name: new Uint8Array(entry.name) };
  }), hasMore: value.hasMore, work };
}

export function copyDirectoryPage(value: {
  readonly entries: readonly { readonly name: Uint8Array; readonly fileId: Uint8Array; readonly fileKind: WorkspaceFileKind }[];
  readonly hasMore: boolean;
},
  work: WorkCounters): DirectoryPage {
  return { entries: value.entries.map(entry => ({ name: new Uint8Array(entry.name),
    fileId: new Uint8Array(entry.fileId), fileKind: projectFileKind(entry.fileKind) })), hasMore: value.hasMore, work };
}

export function copyDirectoryRecordPage(value: {
  readonly entries: readonly {
    readonly name: Uint8Array;
    readonly record: FileRecordSnapshot;
    readonly metadataCanonicalBytes: Uint8Array;
  }[];
  readonly hasMore: boolean;
}, work: WorkCounters): DirectoryRecordPage {
  return { entries: value.entries.map(entry => ({ name: new Uint8Array(entry.name),
    record: copyFileRecord(entry.record), metadataCanonicalBytes: new Uint8Array(entry.metadataCanonicalBytes) })),
    hasMore: value.hasMore, work };
}

function copyTreeEntry(entry: TreeEntrySnapshot): TreeEntrySnapshot {
  return {
    ...entry,
    fileKind: projectFileKind(entry.fileKind),
    fileId: new Uint8Array(entry.fileId),
    name: copyName(entry.name),
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
    directoryId: new Uint8Array(change.directoryId),
    name: copyName(change.name),
    before: change.before === undefined ? undefined : copyTreeEntry(change.before),
    after: change.after === undefined ? undefined : copyTreeEntry(change.after),
  };
}

export function copyGenerationDiff(value: {
  readonly files: readonly {
    readonly fileId: Uint8Array;
    readonly before: FileRecordSnapshot | undefined;
    readonly after: FileRecordSnapshot | undefined;
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
      fileId: new Uint8Array(change.fileId),
      before: change.before === undefined ? undefined : copyFileRecord(change.before),
      after: change.after === undefined ? undefined : copyFileRecord(change.after),
    })),
    bindings: value.bindings.map(copyBindingChange),
    truncated: value.truncated,
    work,
  };
}
