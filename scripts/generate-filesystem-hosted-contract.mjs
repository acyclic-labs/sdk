import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const outputRelative = "typescript/packages/filesystem/generated/hosted-contract";

export const rust = [["acyclic-fs", "example", "filesystem-typescript-hosted-contract"]];

const MAPS = [
  ["FILESYSTEM_PROFILE_TO_PROFILE", "filesystem_profile", "FilesystemProfile", "FILESYSTEM_PROFILE_"],
  ["FILE_KIND_TO_KIND", "file_kind", "FileKind", "FILE_KIND_"],
  ["MUTATION_STATUS_TO_COMMIT", "mutation_status_commit", "MutationStatus", "MUTATION_STATUS_"],
  ["MUTATION_STATUS_TO_DELETE", "mutation_status_delete", "MutationStatus", "MUTATION_STATUS_"],
  ["JOIN_HISTORY_TO_PUBLIC", "join_history", "JoinHistory", "JOIN_HISTORY_"],
  ["CONFLICT_USE_TO_USAGE", "conflict_use", "ConflictUse", "CONFLICT_USE_"],
  ["SPARSE_TARGET_TO_TARGET", "sparse_target", "SparseTarget", "SPARSE_TARGET_"],
  ["EXTENT_KIND_TO_KIND", "extent_kind", "ExtentKind", "EXTENT_KIND_"],
  ["SOURCE_STATE_TO_STATUS", "source_state", "SourceState", "SOURCE_STATE_"],
  ["SOURCE_INVALIDATION_REASON_TO_REASON", "source_invalidation_reason", "SourceInvalidationReason", "SOURCE_INVALIDATION_REASON_"],
  ["NAME_ENCODING_TO_PUBLIC", "name_encoding", "NameEncoding", "NAME_ENCODING_"],
  ["REBASE_STATUS_TO_STATUS", "rebase_status", "RebaseStatus", "REBASE_STATUS_"],
  ["JOIN_STATUS_TO_STATUS", "join_status", "JoinStatus", "JOIN_STATUS_"],
];

function readProtoEnums(root) {
  const temporary = mkdtempSync(join(tmpdir(), "acyclic-filesystem-proto-descriptor-"));
  try {
    const executable = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
    const descriptorPath = join(temporary, "filesystem-descriptor.json");
    const result = spawnSync(
      executable,
      ["build", "--as-file-descriptor-set", "--path", "proto/filesystem/v2", "-o", descriptorPath],
      { cwd: root, encoding: "utf8" },
    );
    if (result.status !== 0) {
      process.stderr.write(result.stdout ?? "");
      process.stderr.write(result.stderr ?? "");
      throw new Error(`Buf filesystem descriptor build failed with status ${result.status ?? "unknown"}`);
    }
    const descriptorSet = JSON.parse(readFileSync(descriptorPath, "utf8"));
    const file = descriptorSet.file?.find(candidate => candidate.name === "filesystem/v2/filesystem.proto");
    if (!file) throw new Error("protobuf filesystem descriptor is absent");
    return new Map((file.enumType ?? []).map(enumDescriptor => [
      enumDescriptor.name,
      (enumDescriptor.value ?? []).map(value => value.name),
    ]));
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

function assertContract(root, contract) {
  if (!contract || typeof contract !== "object" || !contract.maps) {
    throw new Error("Rust hosted contract has no maps object");
  }
  if (!Array.isArray(contract.file_payload_kinds) || contract.file_payload_kinds.length === 0
    || contract.file_payload_kinds.some(value => typeof value !== "string")
    || new Set(contract.file_payload_kinds).size !== contract.file_payload_kinds.length) {
    throw new Error("Rust hosted contract file payload kinds are absent, duplicated, or invalid");
  }
  if (!Array.isArray(contract.file_kinds) || contract.file_kinds.length === 0
    || contract.file_kinds.some(value => typeof value !== "string")
    || new Set(contract.file_kinds).size !== contract.file_kinds.length) {
    throw new Error("Rust hosted contract file kinds are absent, duplicated, or invalid");
  }
  if (!Array.isArray(contract.work_counter_keys) || contract.work_counter_keys.length === 0
    || contract.work_counter_keys.some(value => typeof value !== "string")
    || new Set(contract.work_counter_keys).size !== contract.work_counter_keys.length) {
    throw new Error("Rust hosted contract work counter keys are absent, duplicated, or invalid");
  }
  const protoEnums = readProtoEnums(root);
  for (const [, key, enumName] of MAPS) {
    const entries = contract.maps[key];
    if (!Array.isArray(entries) || entries.length === 0) throw new Error(`Rust hosted contract map ${key} is absent`);
    const wires = entries.map(entry => entry?.wire);
    if (wires.some(value => typeof value !== "string") || new Set(wires).size !== wires.length) {
      throw new Error(`Rust hosted contract map ${key} has duplicate or invalid wire names`);
    }
    const expected = protoEnums.get(enumName);
    if (!expected || expected.length === 0) throw new Error(`protobuf enum ${enumName} is absent or has no values`);
    if (JSON.stringify(wires) !== JSON.stringify(expected)) {
      const missing = expected.filter(value => !wires.includes(value));
      const extra = wires.filter(value => !expected.includes(value));
      throw new Error(`Rust hosted contract map ${key} drift (missing: ${missing.join(", ") || "none"}; extra: ${extra.join(", ") || "none"})`);
    }
    const publicValues = entries.map(entry => entry.public).filter(value => value !== null);
    if (publicValues.some(value => typeof value !== "string") || new Set(publicValues).size !== publicValues.length) {
      throw new Error(`Rust hosted contract map ${key} has duplicate or invalid public values`);
    }
  }
}

function enumMember(fullName, prefix) {
  if (!fullName.startsWith(prefix)) throw new Error(`Rust enum value ${fullName} does not start with ${prefix}`);
  return fullName.slice(prefix.length);
}

function renderMap(name, enumName, prefix, entries) {
  const lines = entries.map(entry => {
    const suffix = enumMember(entry.wire, prefix);
    return `  [${enumName}.${suffix}]: ${entry.public === null ? "undefined" : JSON.stringify(entry.public)},`;
  });
  return `export const ${name} = Object.freeze({\n${lines.join("\n")}\n});`;
}

function renderJoinHistoryReverse(entries) {
  const lines = entries.filter(entry => entry.public !== null).map(entry => {
    const suffix = enumMember(entry.wire, "JOIN_HISTORY_");
    return `  ${JSON.stringify(entry.public)}: JoinHistory.${suffix},`;
  });
  return `export const JOIN_HISTORY_FROM_PUBLIC = Object.freeze(Object.assign(Object.create(null), {\n${lines.join("\n")}\n}));`;
}

export function render([stdout], root) {
  const contract = JSON.parse(stdout);
  assertContract(root, contract);
  const header = "// @generated by scripts/generate-filesystem-hosted-contract.mjs; do not edit.\n\n";
  const imports = [...new Set(MAPS.map(([, , enumName]) => enumName))].join(", ");
  const camel = value => value.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
  const fileKinds = contract.file_kinds.map(value => JSON.stringify(value)).join(", ");
  const payloadKinds = contract.file_payload_kinds.map(value => JSON.stringify(value)).join(", ");
  const workCounterKeys = contract.work_counter_keys.map(value => JSON.stringify(camel(value))).join(", ");
  const source = `${header}import { ${imports} } from "./proto/filesystem/v2/filesystem_pb.js";\n\n` +
    MAPS.map(([name, key, enumName, prefix]) => renderMap(name, enumName, prefix, contract.maps[key])).join("\n\n") +
    `\n\n${renderJoinHistoryReverse(contract.maps.join_history)}\n` +
    `\nexport const FILE_PAYLOAD_KINDS = Object.freeze([${payloadKinds}]);\n` +
    `export const FILE_KINDS = Object.freeze([${fileKinds}]);\n` +
    `export const WORK_COUNTER_KEYS = Object.freeze([${workCounterKeys}]);\n` +
    `export function isFileKind(value) { return FILE_KINDS.includes(value); }\n` +
    `export function isFilePayloadKind(value) { return FILE_PAYLOAD_KINDS.includes(value); }\n`;
  const declaration = `${header}` +
    `import type { FilesystemProfile, FileKind, MutationStatus, JoinHistory as WireJoinHistory, ConflictUse, SparseTarget, ExtentKind, SourceState, SourceInvalidationReason as WireSourceInvalidationReason, NameEncoding, RebaseStatus, JoinStatus as WireJoinStatus } from "./proto/filesystem/v2/filesystem_pb.js";\n` +
    `import type { JoinHistory as PublicJoinHistory, SourceInvalidationReason as PublicSourceInvalidationReason, SourceResult, WorkspaceCommitStatus, WorkspaceDeleteStatus, WorkspaceFileKind, WorkspaceRebaseStatus, JoinStatus as PublicJoinStatus, WorkspaceNameEncoding, TransactionDependencyUse, WorkspaceExtentKind, FsProfile, FilePayloadKind, WorkCounters } from "../src/contracts.js";\n\n` +
    `export declare const FILESYSTEM_PROFILE_TO_PROFILE: Readonly<Record<FilesystemProfile, FsProfile | undefined>>;\n` +
    `export declare const FILE_KIND_TO_KIND: Readonly<Record<FileKind, WorkspaceFileKind | undefined>>;\n` +
    `export declare const MUTATION_STATUS_TO_COMMIT: Readonly<Record<MutationStatus, WorkspaceCommitStatus | undefined>>;\n` +
    `export declare const MUTATION_STATUS_TO_DELETE: Readonly<Record<MutationStatus, WorkspaceDeleteStatus | undefined>>;\n` +
    `export declare const JOIN_HISTORY_TO_PUBLIC: Readonly<Record<WireJoinHistory, PublicJoinHistory | undefined>>;\n` +
    `export declare const JOIN_HISTORY_FROM_PUBLIC: Readonly<Record<PublicJoinHistory, WireJoinHistory>>;\n` +
    `export declare const CONFLICT_USE_TO_USAGE: Readonly<Record<ConflictUse, TransactionDependencyUse | undefined>>;\n` +
    `export declare const SPARSE_TARGET_TO_TARGET: Readonly<Record<SparseTarget, "data" | "hole" | undefined>>;\n` +
    `export declare const EXTENT_KIND_TO_KIND: Readonly<Record<ExtentKind, WorkspaceExtentKind | undefined>>;\n` +
    `export declare const SOURCE_STATE_TO_STATUS: Readonly<Record<SourceState, SourceResult["status"] | undefined>>;\n` +
    `export declare const SOURCE_INVALIDATION_REASON_TO_REASON: Readonly<Record<WireSourceInvalidationReason, PublicSourceInvalidationReason | undefined>>;\n` +
    `export declare const NAME_ENCODING_TO_PUBLIC: Readonly<Record<NameEncoding, WorkspaceNameEncoding | undefined>>;\n` +
    `export declare const REBASE_STATUS_TO_STATUS: Readonly<Record<RebaseStatus, WorkspaceRebaseStatus | undefined>>;\n` +
    `export declare const JOIN_STATUS_TO_STATUS: Readonly<Record<WireJoinStatus, PublicJoinStatus | undefined>>;\n` +
    `export declare const FILE_PAYLOAD_KINDS: readonly FilePayloadKind[];\n` +
    `export declare const FILE_KINDS: readonly WorkspaceFileKind[];\n` +
    `export declare const WORK_COUNTER_KEYS: readonly (keyof WorkCounters)[];\n` +
    `export declare function isFileKind(value: string): value is WorkspaceFileKind;\n` +
    `export declare function isFilePayloadKind(value: string): value is FilePayloadKind;\n`;
  return { [`${outputRelative}.js`]: source, [`${outputRelative}.d.ts`]: declaration };
}
