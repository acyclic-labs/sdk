import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptPath = fileURLToPath(import.meta.url);
const outputRelative = "typescript/packages/filesystem/generated/hosted-contract";

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

function readRustContract(root) {
  const result = spawnSync(
    "cargo",
    [
      "run", "--manifest-path", join(root, "Cargo.toml"), "--package", "acyclic-fs",
      "--example", "filesystem-typescript-hosted-contract", "--no-default-features", "--locked", "--quiet",
    ],
    { cwd: root, encoding: "utf8" },
  );
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(`Rust filesystem hosted contract generator failed with status ${result.status ?? "unknown"}`);
  }
  const lines = (result.stdout ?? "").trim().split(/\r?\n/).filter(Boolean);
  const jsonLine = lines.at(-1);
  if (!jsonLine) throw new Error("Rust filesystem hosted contract generator produced no JSON");
  return JSON.parse(jsonLine);
}

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

function renderSources(root) {
  const contract = readRustContract(root);
  assertContract(root, contract);
  const header = "// @generated by scripts/generate-filesystem-hosted-contract.mjs; do not edit.\n\n";
  const imports = [...new Set(MAPS.map(([, , enumName]) => enumName))].join(", ");
  const source = `${header}import { ${imports} } from "./proto/filesystem/v2/filesystem_pb.js";\n\n` +
    MAPS.map(([name, key, enumName, prefix]) => renderMap(name, enumName, prefix, contract.maps[key])).join("\n\n") +
    `\n\n${renderJoinHistoryReverse(contract.maps.join_history)}\n`;
  const declaration = `${header}` +
    `import type { FilesystemProfile, FileKind, MutationStatus, JoinHistory as WireJoinHistory, ConflictUse, SparseTarget, ExtentKind, SourceState, SourceInvalidationReason as WireSourceInvalidationReason, NameEncoding, RebaseStatus, JoinStatus as WireJoinStatus } from "./proto/filesystem/v2/filesystem_pb.js";\n` +
    `import type { JoinHistory as PublicJoinHistory, SourceInvalidationReason as PublicSourceInvalidationReason, SourceResult, WorkspaceCommitStatus, WorkspaceDeleteStatus, WorkspaceFileKind, WorkspaceRebaseStatus, JoinStatus as PublicJoinStatus, WorkspaceNameEncoding, TransactionDependencyUse, WorkspaceExtentKind, FsProfile } from "../src/contracts.js";\n\n` +
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
    `export declare const JOIN_STATUS_TO_STATUS: Readonly<Record<WireJoinStatus, PublicJoinStatus | undefined>>;\n`;
  return { source, declaration };
}

export function generateFilesystemHostedContract(root, outputDirectory) {
  const output = outputDirectory ?? join(root, "typescript/packages/filesystem/generated");
  const { source, declaration } = renderSources(root);
  writeFileSync(join(output, "hosted-contract.js"), source);
  writeFileSync(join(output, "hosted-contract.d.ts"), declaration);
}

export function checkFilesystemHostedContract(root) {
  const temporary = mkdtempSync(join(tmpdir(), "acyclic-filesystem-hosted-contract-"));
  try {
    generateFilesystemHostedContract(root, temporary);
    for (const file of ["hosted-contract.js", "hosted-contract.d.ts"]) {
      const committed = join(root, `${outputRelative}.${file.endsWith(".js") ? "js" : "d.ts"}`);
      if (!existsSync(committed) || !readFileSync(join(temporary, file)).equals(readFileSync(committed))) {
        throw new Error(`filesystem hosted contract is stale; run bun run generate (${file})`);
      }
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

if (resolve(process.argv[1] ?? "") === resolve(scriptPath)) {
  const root = join(dirname(scriptPath), "..");
  const mode = process.argv[2] ?? "write";
  if (mode === "check") checkFilesystemHostedContract(root);
  else if (mode === "write") generateFilesystemHostedContract(root);
  else throw new Error(`unknown mode: ${mode}`);
}
