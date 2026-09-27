import type {
  BatchLookupEntry, DirectoryBindingChange, DirectoryPage, DirectoryRecordPage, FileRecordSnapshot,
  GenerationDiff, NamedAttributePage, NamedAttributeResult, StatResult, TreeEntrySnapshot, WorkCounters,
  WasmRawFileRecordSnapshot,
} from "./contracts.js";

export function copyFileRecord(record: FileRecordSnapshot | WasmRawFileRecordSnapshot): FileRecordSnapshot {
  return {
    ...record,
    fileId: Uint8Array.from(record.fileId),
    linkCount: BigInt(record.linkCount),
    metadataObject: Uint8Array.from(record.metadataObject),
    logicalBytes: record.logicalBytes === undefined ? undefined : BigInt(record.logicalBytes),
    payloadObject: record.payloadObject === undefined ? undefined : Uint8Array.from(record.payloadObject),
    inlineBytes: record.inlineBytes === undefined ? undefined : Uint8Array.from(record.inlineBytes),
  };
}

export function copyBatchLookupEntries(entries: readonly BatchLookupEntry[]): BatchLookupEntry[] {
  return entries.map((entry) => ({
    ...entry,
    fileId: entry.fileId === undefined ? undefined : Uint8Array.from(entry.fileId),
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
    metadataCanonicalBytes: value.metadataCanonicalBytes === undefined ? undefined : Uint8Array.from(value.metadataCanonicalBytes),
    work,
  };
}

export function copyNamedAttributeResult(value: { readonly exists: boolean; readonly bytes: Uint8Array | undefined },
  work: WorkCounters): NamedAttributeResult {
  return { exists: value.exists, bytes: value.bytes === undefined ? undefined : Uint8Array.from(value.bytes), work };
}

export function copyNamedAttributePage(value: Pick<NamedAttributePage, "entries" | "hasMore">,
  work: WorkCounters): NamedAttributePage {
  return { entries: value.entries.map(entry => ({ attributeClass: entry.attributeClass,
    name: Uint8Array.from(entry.name) })), hasMore: value.hasMore, work };
}

export function copyDirectoryPage(value: Pick<DirectoryPage, "entries" | "hasMore">,
  work: WorkCounters): DirectoryPage {
  return { entries: value.entries.map(entry => ({ name: Uint8Array.from(entry.name),
    fileId: Uint8Array.from(entry.fileId), fileKind: entry.fileKind })), hasMore: value.hasMore, work };
}

export function copyDirectoryRecordPage(value: {
  readonly entries: readonly {
    readonly name: Uint8Array;
    readonly record: FileRecordSnapshot | WasmRawFileRecordSnapshot;
    readonly metadataCanonicalBytes: Uint8Array;
  }[];
  readonly hasMore: boolean;
}, work: WorkCounters): DirectoryRecordPage {
  return { entries: value.entries.map(entry => ({ name: Uint8Array.from(entry.name),
    record: copyFileRecord(entry.record), metadataCanonicalBytes: Uint8Array.from(entry.metadataCanonicalBytes) })),
    hasMore: value.hasMore, work };
}

function copyTreeEntry(entry: TreeEntrySnapshot): TreeEntrySnapshot {
  return {
    ...entry,
    fileId: Uint8Array.from(entry.fileId),
    name: { ...entry.name, bytes: Uint8Array.from(entry.name.bytes) },
  };
}

export function copyBindingChange(change: DirectoryBindingChange): DirectoryBindingChange {
  return {
    ...change,
    directoryId: Uint8Array.from(change.directoryId),
    name: { ...change.name, bytes: Uint8Array.from(change.name.bytes) },
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
  readonly bindings: readonly DirectoryBindingChange[];
  readonly truncated: boolean;
}, work: WorkCounters): GenerationDiff {
  return {
    files: value.files.map(change => ({
      fileId: Uint8Array.from(change.fileId),
      before: change.before === undefined ? undefined : copyFileRecord(change.before),
      after: change.after === undefined ? undefined : copyFileRecord(change.after),
    })),
    bindings: value.bindings.map(copyBindingChange),
    truncated: value.truncated,
    work,
  };
}
