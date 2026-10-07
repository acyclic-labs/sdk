import type {
  BatchLookupEntry, DirectoryBindingChange, DirectoryPage, DirectoryRecordPage, FileRecordSnapshot,
  GenerationDiff, NamedAttributePage, NamedAttributeResult, StatResult, TreeEntrySnapshot, WorkCounters,
  WasmRawFileRecordSnapshot,
  WorkspaceFileKind,
} from "./contracts.js";
import type * as NativeBinding from "../generated/native/binding.js";
import type * as WasmBinding from "../generated/wasm/acyclic_fs_wasm.js";
import { isFileKind } from "../generated/hosted-contract.js";

type RawFileRecord = WasmRawFileRecordSnapshot;
type BoundaryFileKind = WorkspaceFileKind | WasmBinding.FileKind | NativeBinding.FileKind;
type RawTreeEntry = TreeEntrySnapshot;

/**
 * File-kind values are validated and emitted as finite Rust-owned types at
 * each native/WASM boundary. Keep this projection as a type-preserving seam
 * for the shared adapters; do not duplicate the Rust domain list here.
 */
export function projectFileKind(value: BoundaryFileKind): WorkspaceFileKind {
  if (!isFileKind(value)) throw new TypeError("filesystem binding returned an invalid file kind");
  return value;
}

/** Validate the stringly accessor exposed by generated resolved-file classes. */
export function projectRawFileKind(value: string): WorkspaceFileKind {
  if (!isFileKind(value)) throw new TypeError("filesystem binding returned an invalid file kind");
  return value;
}

export function copyFileRecord(record: FileRecordSnapshot | RawFileRecord): FileRecordSnapshot {
  return {
    ...record,
    fileKind: projectFileKind(record.fileKind),
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
    return { attributeClass: entry.attributeClass, name: Uint8Array.from(entry.name) };
  }), hasMore: value.hasMore, work };
}

export function copyDirectoryPage(value: {
  readonly entries: readonly { readonly name: Uint8Array; readonly fileId: Uint8Array; readonly fileKind: WorkspaceFileKind }[];
  readonly hasMore: boolean;
},
  work: WorkCounters): DirectoryPage {
  return { entries: value.entries.map(entry => ({ name: Uint8Array.from(entry.name),
    fileId: Uint8Array.from(entry.fileId), fileKind: projectFileKind(entry.fileKind) })), hasMore: value.hasMore, work };
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

function copyTreeEntry(entry: RawTreeEntry): TreeEntrySnapshot {
  return {
    ...entry,
    fileKind: projectFileKind(entry.fileKind),
    fileId: Uint8Array.from(entry.fileId),
    name: { ...entry.name, bytes: Uint8Array.from(entry.name.bytes) },
  };
}

export function copyBindingChange(change: {
  readonly directoryId: Uint8Array;
  readonly name: TreeEntrySnapshot["name"];
  readonly before: RawTreeEntry | undefined;
  readonly after: RawTreeEntry | undefined;
}): DirectoryBindingChange {
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
    readonly before: FileRecordSnapshot | RawFileRecord | undefined;
    readonly after: FileRecordSnapshot | RawFileRecord | undefined;
  }[];
  readonly bindings: readonly {
    readonly directoryId: Uint8Array;
    readonly name: TreeEntrySnapshot["name"];
    readonly before: RawTreeEntry | undefined;
    readonly after: RawTreeEntry | undefined;
  }[];
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
