import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptPath = fileURLToPath(import.meta.url);

function readRustContract(root) {
  const result = spawnSync(
    process.env.ACYCLIC_CARGO_BIN || "cargo",
    [
      "run",
      "--manifest-path",
      join(root, "Cargo.toml"),
      "--package",
      "acyclic-fs",
      "--example",
      "filesystem-git-compat-contract",
      "--no-default-features",
      "--locked",
      "--quiet",
    ],
    { cwd: root, encoding: "utf8", windowsHide: true },
  );
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(`Rust filesystem Git contract generator failed with status ${result.status ?? "unknown"}`);
  }
  const lines = (result.stdout ?? "").trim().split(/\r?\n/).filter(Boolean);
  const json = lines.at(-1);
  if (!json) throw new Error("Rust filesystem Git contract generator produced no JSON");
  const contract = JSON.parse(json);
  if (!Array.isArray(contract.command_variants) ||
      !contract.command_variants.every(value => typeof value === "string" && value.length > 0) ||
      new Set(contract.command_variants).size !== contract.command_variants.length) {
    throw new Error("filesystem Git contract has invalid command variants");
  }
  if (!Array.isArray(contract.output_variants) ||
      !contract.output_variants.every(value => typeof value === "string" && value.length > 0) ||
      new Set(contract.output_variants).size !== contract.output_variants.length) {
    throw new Error("filesystem Git contract has invalid output variants");
  }
  for (const [name, values] of [
    ["action variants", contract.action_variants],
    ["result variants", contract.result_variants],
  ]) {
    if (!Array.isArray(values) || !values.every(value => typeof value === "string" && value.length > 0) ||
        new Set(values).size !== values.length) {
      throw new Error(`filesystem Git contract has invalid ${name}`);
    }
  }
  for (const [name, variants, types] of [
    ["command", contract.command_variants, contract.command_types],
    ["action", contract.action_variants, contract.action_types],
    ["result", contract.result_variants, contract.result_types],
    ["output", contract.output_variants, contract.output_types],
  ]) {
    if (!Array.isArray(types) || types.length !== variants.length ||
        types.some(entry => !Array.isArray(entry) || entry.length !== 2 ||
          entry.some(value => typeof value !== "string" || value.length === 0)) ||
        new Set(types.map(entry => entry[0])).size !== types.length ||
        types.some(entry => !variants.includes(entry[0]))) {
      throw new Error(`filesystem Git contract has invalid ${name} TypeScript schema`);
    }
  }
  if (!Array.isArray(contract.nested_types) ||
      contract.nested_types.some(entry => !Array.isArray(entry) || entry.length !== 2 ||
        entry.some(value => typeof value !== "string" || value.length === 0)) ||
      new Set(contract.nested_types.map(entry => entry[0])).size !== contract.nested_types.length) {
    throw new Error("filesystem Git contract has invalid nested TypeScript schema");
  }
  for (const [name, values] of [
    ["reset mode", contract.reset_mode_variants],
    ["reset mode TypeScript", contract.reset_mode_typescript_variants],
    ["dirty state", contract.dirty_state_variants],
    ["dirty state TypeScript", contract.dirty_state_typescript_variants],
  ]) {
    if (!Array.isArray(values) || !values.every(value => typeof value === "string" && value.length > 0) ||
        new Set(values).size !== values.length) {
      throw new Error(`filesystem Git contract has invalid ${name} variants`);
    }
  }
  if (contract.reset_mode_variants.length !== contract.reset_mode_typescript_variants.length ||
      contract.dirty_state_variants.length !== contract.dirty_state_typescript_variants.length) {
    throw new Error("filesystem Git scalar enum variant mappings are incomplete");
  }
  for (const [name, values, typescriptValues, wireValues] of [
    ["reset mode wire", contract.reset_mode_wire, contract.reset_mode_typescript_variants, contract.reset_mode_variants],
    ["dirty state wire", contract.dirty_state_wire, contract.dirty_state_typescript_variants, contract.dirty_state_variants],
  ]) {
    if (!Array.isArray(values) || values.length !== typescriptValues.length ||
        values.some(entry => !Array.isArray(entry) || entry.length !== 2 ||
          entry.some(value => typeof value !== "string" || value.length === 0)) ||
        new Set(values.map(entry => entry[0])).size !== values.length ||
        new Set(values.map(entry => entry[1])).size !== values.length ||
        values.some(entry => !typescriptValues.includes(entry[0]))) {
      throw new Error(`filesystem Git contract has invalid ${name} mapping`);
    }
    if (values.some(entry => !wireValues.includes(entry[1]))) {
      throw new Error(`filesystem Git contract has invalid ${name} wire tag`);
    }
  }
  const nestedTypeNames = new Set(contract.nested_types.map(entry => entry[0]));
  const generatedTypeNames = new Set([
    "GitCompatCommand", "GitFilesystemAction", "GitFilesystemResult", "GitCompatOutput",
    "GitPendingTransition", "GitFilesystemExecutor", "GitCompatRepository",
    "GitResetMode", "GitDirtyState",
  ]);
  const referencedNestedTypes = [
    ...contract.command_types,
    ...contract.action_types,
    ...contract.result_types,
    ...contract.output_types,
    ...contract.nested_types,
  ].flatMap(([, declaration]) => declaration.match(/\bGit[A-Z][A-Za-z0-9_]*/g) ?? []);
  for (const name of new Set(referencedNestedTypes)) {
    if (name !== "GitCommitIdentity" &&
        !nestedTypeNames.has(name) && !generatedTypeNames.has(name)) {
      throw new Error(`filesystem Git TypeScript schema references undeclared nested type ${name}`);
    }
  }
  if (!Array.isArray(contract.identity_fields) ||
      contract.identity_fields.some(field => typeof field?.key !== "string" ||
        !Number.isSafeInteger(field.bytes) || field.bytes <= 0) ||
      new Set(contract.identity_fields.map(field => field.key)).size !== contract.identity_fields.length) {
    throw new Error("filesystem Git contract has invalid identity fields");
  }
  if (!Array.isArray(contract.byte_fields) ||
      contract.byte_fields.some(field => typeof field?.key !== "string" ||
        (field.bytes !== null && (!Number.isSafeInteger(field.bytes) || field.bytes <= 0))) ||
      new Set(contract.byte_fields.map(field => field.key)).size !== contract.byte_fields.length) {
    throw new Error("filesystem Git contract has invalid byte fields");
  }
  for (const [name, values] of [
    ["timestamp fields", contract.timestamp_fields],
    ["UUID paths", contract.uuid_paths],
    ["opaque paths", contract.opaque_paths],
    ["pending fields", contract.pending_fields],
  ]) {
    if (!Array.isArray(values) || !values.every(value => typeof value === "string" && value.length > 0) ||
        new Set(values).size !== values.length) {
      throw new Error(`filesystem Git contract has invalid ${name}`);
    }
  }
  if (!Array.isArray(contract.public_aliases) ||
      contract.public_aliases.some(alias => !Array.isArray(alias) || alias.length !== 2 ||
        alias.some(value => typeof value !== "string" || value.length === 0)) ||
      new Set(contract.public_aliases.map(alias => alias[0])).size !== contract.public_aliases.length) {
    throw new Error("filesystem Git contract has invalid public aliases");
  }
  if (!Number.isSafeInteger(contract.transition_identity_bytes) || contract.transition_identity_bytes <= 0) {
    throw new Error("filesystem Git contract has invalid transition identity width");
  }
  return contract;
}

function renderSources(root) {
  const contract = readRustContract(root);
  const identityLengths = Object.fromEntries(
    contract.identity_fields.map(field => [field.key, field.bytes]),
  );
  const byteFields = Object.fromEntries(
    contract.byte_fields.map(field => [field.key, field.bytes]),
  );
  const aliases = Object.fromEntries(contract.public_aliases);
  const resetModeWire = Object.fromEntries(contract.reset_mode_wire);
  const dirtyStateWire = Object.fromEntries(contract.dirty_state_wire);
  const header = "// @generated by scripts/generate-filesystem-git-compat-contract.mjs; do not edit.\n\n";
  const source = `${header}export const GIT_COMPAT_OUTPUT_VARIANTS = new Set(${JSON.stringify(contract.output_variants)});\n` +
    `export function isGitCompatOutputVariant(value) { return GIT_COMPAT_OUTPUT_VARIANTS.has(value); }\n` +
    `export const GIT_COMPAT_COMMAND_VARIANTS = new Set(${JSON.stringify(contract.command_variants)});\n` +
    `export function isGitCompatCommandVariant(value) { return GIT_COMPAT_COMMAND_VARIANTS.has(value); }\n` +
    `export const GIT_COMPAT_ACTION_VARIANTS = new Set(${JSON.stringify(contract.action_variants)});\n` +
    `export const GIT_COMPAT_RESULT_VARIANTS = new Set(${JSON.stringify(contract.result_variants)});\n` +
    `export const GIT_COMPAT_RESET_MODE_VARIANTS = new Set(${JSON.stringify(contract.reset_mode_typescript_variants)});\n` +
    `export const GIT_COMPAT_DIRTY_STATE_VARIANTS = new Set(${JSON.stringify(contract.dirty_state_typescript_variants)});\n` +
    `export const GIT_COMPAT_RESET_MODE_WIRE = Object.freeze(${JSON.stringify(resetModeWire)});\n` +
    `export const GIT_COMPAT_DIRTY_STATE_WIRE = Object.freeze(${JSON.stringify(dirtyStateWire)});\n` +
    `export const GIT_COMPAT_IDENTITY_LENGTHS = Object.freeze(${JSON.stringify(identityLengths)});\n` +
    `export const GIT_COMPAT_BYTE_FIELDS = Object.freeze(${JSON.stringify(byteFields)});\n` +
    `export const GIT_COMPAT_TIMESTAMP_FIELDS = Object.freeze(new Set(${JSON.stringify(contract.timestamp_fields)}));\n` +
    `export const GIT_COMPAT_UUID_PATHS = Object.freeze(new Set(${JSON.stringify(contract.uuid_paths)}));\n` +
    `export const GIT_COMPAT_OPAQUE_PATHS = Object.freeze(new Set(${JSON.stringify(contract.opaque_paths)}));\n` +
    `export const GIT_COMPAT_PUBLIC_ALIASES = Object.freeze(${JSON.stringify(aliases)});\n` +
    `export const GIT_COMPAT_PENDING_FIELDS = Object.freeze(new Set(${JSON.stringify(contract.pending_fields)}));\n` +
    `export const GIT_COMPAT_TRANSITION_IDENTITY_BYTES = ${contract.transition_identity_bytes};\n`;
  const variants = contract.output_variants.map(value => JSON.stringify(value)).join(" | ");
  const actionVariants = contract.action_variants.map(value => JSON.stringify(value)).join(" | ");
  const resultVariants = contract.result_variants.map(value => JSON.stringify(value)).join(" | ");
  const fields = contract.identity_fields.map(field =>
    `  readonly ${JSON.stringify(field.key)}: ${field.bytes};`).join("\n");
  const commandTypes = Object.fromEntries(contract.command_types);
  const actionTypes = Object.fromEntries(contract.action_types);
  const resultTypes = Object.fromEntries(contract.result_types);
  const outputTypes = Object.fromEntries(contract.output_types);
  function unionFor(values, mapping, label) {
    const keys = Object.keys(mapping);
    if (values.length !== keys.length || values.some(value => !keys.includes(value))) {
      throw new Error(`filesystem Git ${label} variants do not have generated type mappings`);
    }
    return values.map(value => mapping[value]).join("\n  | ");
  }
  const commandUnion = unionFor(contract.command_variants, commandTypes, "command");
  const actionUnion = unionFor(contract.action_variants, actionTypes, "action");
  const resultUnion = unionFor(contract.result_variants, resultTypes, "result");
  const outputUnion = unionFor(contract.output_variants, outputTypes, "output");
  const resetModeUnion = contract.reset_mode_typescript_variants.map(value => JSON.stringify(value)).join(" | ");
  const dirtyStateUnion = contract.dirty_state_typescript_variants.map(value => JSON.stringify(value)).join(" | ");
  const nestedDeclarations = contract.nested_types.map(([, declaration]) => `${declaration}\n`).join("");
  const typeDeclarations = `export type WorkspaceIdentity = Uint8Array;\n` +
    `export type GenerationIdentity = Uint8Array;\n` +
    `export type OperationIdentity = Uint8Array;\n` +
    `export type WorkspaceContextIdentity = Uint8Array;\n` +
    `export type WorkspaceRootIdentity = Uint8Array;\n` +
    `export type GitCommitIdentity = string;\n` +
    `export type GitResetMode = ${resetModeUnion};\n` +
    `export type GitDirtyState = ${dirtyStateUnion};\n` +
    `export type GitCompatCommand =\n  | ${commandUnion};\n` +
    nestedDeclarations +
    `export type GitFilesystemAction =\n  | ${actionUnion};\n` +
    `export type GitFilesystemResult =\n  | ${resultUnion};\n` +
    `export interface GitPendingTransition { readonly id: OperationIdentity; readonly action: GitFilesystemAction; readonly mutation: unknown; }\n` +
    `export interface GitFilesystemExecutor { execute(operationId: OperationIdentity, action: GitFilesystemAction): Promise<GitFilesystemResult>; }\n` +
    `export type GitCompatOutput = "NoOp"\n  | ${outputUnion};\n` +
    `export interface GitCompatRepository { execute(command: GitCompatCommand, workspaceGeneration: GenerationIdentity): Promise<GitCompatOutput>; executeArgv(argv: readonly string[], workspaceGeneration: GenerationIdentity, defaultAuthor: string, nowSeconds: bigint): Promise<GitCompatOutput>; pendingTransition(): Promise<GitPendingTransition | undefined>; completeTransition(transition: OperationIdentity, resultingGeneration?: GenerationIdentity): Promise<GitCompatOutput>; completeTransitionResult(transition: OperationIdentity, result: GitFilesystemResult): Promise<GitCompatOutput>; run(command: GitCompatCommand, workspaceGeneration: GenerationIdentity, executor: GitFilesystemExecutor): Promise<GitCompatOutput>; runArgv(argv: readonly string[], workspaceGeneration: GenerationIdentity, defaultAuthor: string, nowSeconds: bigint, executor: GitFilesystemExecutor): Promise<GitCompatOutput>; resume(executor: GitFilesystemExecutor): Promise<GitCompatOutput | undefined>; abortTransition(transition: OperationIdentity): Promise<void>; registerBranchWorkspace(branch: string, workspaceId: WorkspaceIdentity, head: GitCommitIdentity | undefined, switchToBranch: boolean): Promise<GitCompatOutput>; recordCommit(expectedHead: GitCommitIdentity | undefined, generation: GenerationIdentity, trackedPaths: readonly string[], message: string, author: string, authoredAtSeconds: bigint): Promise<GitCompatOutput>; }\n`;
  const declaration = `${header}${typeDeclarations}` +
    `export declare const GIT_COMPAT_OUTPUT_VARIANTS: ReadonlySet<${variants}>;\n` +
    "export declare function isGitCompatOutputVariant(value: string): boolean;\n" +
    `export declare const GIT_COMPAT_COMMAND_VARIANTS: ReadonlySet<${contract.command_variants.map(value => JSON.stringify(value)).join(" | ")}>;\n` +
    "export declare function isGitCompatCommandVariant(value: string): boolean;\n" +
    `export declare const GIT_COMPAT_ACTION_VARIANTS: ReadonlySet<${actionVariants}>;\n` +
    `export declare const GIT_COMPAT_RESULT_VARIANTS: ReadonlySet<${resultVariants}>;\n` +
    `export declare const GIT_COMPAT_RESET_MODE_VARIANTS: ReadonlySet<${resetModeUnion}>;\n` +
    `export declare const GIT_COMPAT_DIRTY_STATE_VARIANTS: ReadonlySet<${dirtyStateUnion}>;\n` +
    `export declare const GIT_COMPAT_RESET_MODE_WIRE: Readonly<Record<GitResetMode, string>>;\n` +
    `export declare const GIT_COMPAT_DIRTY_STATE_WIRE: Readonly<Record<GitDirtyState, string>>;\n` +
    `export declare const GIT_COMPAT_IDENTITY_LENGTHS: Readonly<{\n${fields}\n}>;\n` +
    `export declare const GIT_COMPAT_BYTE_FIELDS: Readonly<Record<string, number | null>>;\n` +
    `export declare const GIT_COMPAT_TIMESTAMP_FIELDS: ReadonlySet<string>;\n` +
    `export declare const GIT_COMPAT_UUID_PATHS: ReadonlySet<string>;\n` +
    `export declare const GIT_COMPAT_OPAQUE_PATHS: ReadonlySet<string>;\n` +
    `export declare const GIT_COMPAT_PUBLIC_ALIASES: Readonly<Record<string, string>>;\n` +
    `export declare const GIT_COMPAT_PENDING_FIELDS: ReadonlySet<string>;\n` +
    `export declare const GIT_COMPAT_TRANSITION_IDENTITY_BYTES: ${contract.transition_identity_bytes};\n`;
  return { source, declaration };
}

export function generateFilesystemGitCompatContract(root, outputDirectory) {
  const output = outputDirectory ?? join(root, "typescript/packages/filesystem/generated");
  const { source, declaration } = renderSources(root);
  writeFileSync(join(output, "git-compat-contract.js"), source);
  writeFileSync(join(output, "git-compat-contract.d.ts"), declaration);
}

export function checkFilesystemGitCompatContract(root) {
  const temporary = mkdtempSync(join(tmpdir(), "acyclic-filesystem-git-compat-contract-"));
  try {
    generateFilesystemGitCompatContract(root, temporary);
    for (const file of ["git-compat-contract.js", "git-compat-contract.d.ts"]) {
      const committed = join(root, "typescript/packages/filesystem/generated", file);
      if (!existsSync(committed) || !readFileSync(join(temporary, file)).equals(readFileSync(committed))) {
        throw new Error(`filesystem Git compatibility contract is stale; run bun run generate (${file})`);
      }
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

if (resolve(process.argv[1] ?? "") === resolve(scriptPath)) {
  const root = join(dirname(scriptPath), "..");
  const mode = process.argv[2] ?? "write";
  if (mode === "check") checkFilesystemGitCompatContract(root);
  else if (mode === "write") generateFilesystemGitCompatContract(root);
  else throw new Error(`unknown mode: ${mode}`);
}
