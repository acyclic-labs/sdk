import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptPath = fileURLToPath(import.meta.url);
const outputRelativePath = "typescript/packages/harness/src/limits-contract.ts";
const limitFields = [
  "file_bytes", "path_bytes", "attachments", "render_bytes", "model_steps",
  "model_events_per_step", "tool_calls_per_step", "context_messages",
];
const forkFields = [
  ["agents", "number"],
  ["resources", "number"],
  ["references", "number"],
  ["attachment_manifest_bytes", "bigint"],
  ["inherited_bytes", "bigint"],
  ["reference_bytes", "bigint"],
  ["inherited_messages", "bigint"],
];
const forkFieldNames = forkFields.map(([field]) => field);
const projectionFields = [
  ["default_max_resolved_bytes", "number"],
  ["default_max_manifest_bytes", "number"],
  ["default_max_attachments", "number"],
  ["default_max_messages", "number"],
  ["default_max_render_bytes", "number"],
  ["max_projected_attachments", "number"],
  ["max_json_bytes", "number"],
];
const projectionFieldNames = projectionFields.map(([field]) => field);

function assertExactKeys(value, expected, label) {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (JSON.stringify(actual) !== JSON.stringify(wanted)) {
    throw new Error(`Harness ${label} field inventory drift: expected ${wanted.join(",")}, got ${actual.join(",")}`);
  }
}

function readRustContract(root) {
  const result = spawnSync(
    process.env.ACYCLIC_CARGO_BIN || "cargo",
    [
      "run", "--manifest-path", join(root, "Cargo.toml"), "--package", "acyclic-harness",
      "--example", "limits-contract", "--no-default-features", "--locked", "--quiet",
    ],
    { cwd: root, encoding: "utf8" },
  );
  if (result.status !== 0) {
    process.stderr.write(result.stdout ?? "");
    process.stderr.write(result.stderr ?? "");
    throw new Error(`Rust Harness limits contract generator failed with status ${result.status ?? "unknown"}`);
  }
  const jsonLine = (result.stdout ?? "").trim().split(/\r?\n/).filter(Boolean).at(-1);
  if (!jsonLine) throw new Error("Rust Harness limits contract generator produced no JSON");
  const contract = JSON.parse(jsonLine);
  assertExactKeys(contract, ["limits", "batch_inputs", "fork", "projection"], "contract");
  if (!Number.isSafeInteger(contract.batch_inputs) || contract.batch_inputs <= 0) {
    throw new Error("Harness batch contract has invalid maximum input count");
  }
  const limits = contract.limits;
  if (!limits || typeof limits !== "object") throw new Error("Harness limits contract is missing limits");
  if (!limits.default || typeof limits.default !== "object" ||
      !limits.maximum || typeof limits.maximum !== "object") {
    throw new Error("Harness limits contract is missing default or maximum limits");
  }
  assertExactKeys(limits, ["default", "maximum", "exact_js_integer", "max_path_bytes", "max_label_bytes"], "limits");
  assertExactKeys(limits.default, limitFields, "limits.default");
  assertExactKeys(limits.maximum, limitFields, "limits.maximum");
  if (!limits || !limits.default || !limits.maximum ||
      limitFields.some(field => !Number.isSafeInteger(limits.default[field]) || limits.default[field] <= 0) ||
      limitFields.some(field => !Number.isSafeInteger(limits.maximum[field]) || limits.maximum[field] <= 0) ||
      limitFields.some(field => limits.default[field] > limits.maximum[field])) {
    throw new Error("Harness limits contract has invalid defaults or ceilings");
  }
  for (const [name, value] of [
    ["exact_js_integer", limits.exact_js_integer],
    ["max_path_bytes", limits.max_path_bytes],
    ["max_label_bytes", limits.max_label_bytes],
  ]) {
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`Harness limits contract has invalid ${name}`);
  }
  if (limits.default.path_bytes !== limits.max_path_bytes ||
      limits.maximum.path_bytes !== limits.max_path_bytes) {
    throw new Error("Harness path limit is not bound to MAX_PATH_BYTES");
  }
  const fork = contract.fork;
  if (!fork || typeof fork !== "object") throw new Error("Harness limits contract is missing fork");
  assertExactKeys(fork, forkFieldNames, "fork");
  if (!fork || forkFields.some(([field, kind]) => {
    const value = fork[field];
    return kind === "bigint" ? !Number.isSafeInteger(value) || value <= 0 :
      !Number.isSafeInteger(value) || value <= 0;
  })) throw new Error("Harness fork contract has invalid ceilings");
  const projection = contract.projection;
  if (!projection || typeof projection !== "object") throw new Error("Harness limits contract is missing projection");
  assertExactKeys(projection, projectionFieldNames, "projection");
  if (projectionFields.some(([field]) =>
    !Number.isSafeInteger(projection[field]) || projection[field] <= 0)) {
    throw new Error("Harness projection contract has invalid defaults or ceilings");
  }
  if (projection.max_projected_attachments > limits.maximum.attachments ||
      projection.default_max_attachments > limits.maximum.attachments ||
      projection.default_max_messages > limits.maximum.context_messages ||
      projection.default_max_render_bytes > limits.maximum.render_bytes ||
      projection.default_max_resolved_bytes > limits.maximum.file_bytes ||
      projection.default_max_manifest_bytes > limits.maximum.file_bytes) {
    throw new Error("Harness projection contract exceeds protocol ceilings");
  }
  return contract;
}

function renderValue(value, kind = "number") {
  if (kind === "bigint") return `${value}n`;
  return String(value);
}

function renderLimitValue(value) {
  return `${value} as number`;
}

function renderContract(root) {
  const contract = readRustContract(root);
  const { limits, fork, projection } = contract;
  const lines = [
    "// @generated by scripts/generate-harness-limits-contract.mjs; do not edit.",
    "",
    `export const HARNESS_LIMITS_DEFAULT = Object.freeze({`,
    ...limitFields.map(field => `  ${field}: ${renderLimitValue(limits.default[field])},`),
    "});",
    `export const HARNESS_LIMITS_MAXIMUM = Object.freeze({`,
    ...limitFields.map(field => `  ${field}: ${renderLimitValue(limits.maximum[field])},`),
    "});",
    `export const HARNESS_MAX_EXACT_JS_INTEGER = ${renderValue(limits.exact_js_integer)};`,
    `export const HARNESS_MAX_PATH_BYTES = ${renderValue(limits.max_path_bytes)};`,
    `export const HARNESS_MAX_LABEL_BYTES = ${renderValue(limits.max_label_bytes)};`,
    `export const HARNESS_MAX_BATCH_INPUTS = ${renderValue(contract.batch_inputs)};`,
    `export const HARNESS_MAX_ATTACHMENT_COUNT = ${renderValue(limits.maximum.attachments)};`,
  ];
  for (const [field, kind] of forkFields) {
    lines.push(`export const MAX_FORK_${field.toUpperCase()} = ${renderValue(fork[field], kind)};`);
  }
  for (const [field, kind] of projectionFields) {
    lines.push(`export const HARNESS_PROJECTION_${field.toUpperCase()} = ${renderValue(projection[field], kind)};`);
  }
  lines.push("");
  return lines.join("\n");
}

export function generateHarnessLimitsContract(root, outputDirectory) {
  const output = outputDirectory ?? dirname(join(root, outputRelativePath));
  writeFileSync(join(output, "limits-contract.ts"), renderContract(root));
}

export function checkHarnessLimitsContract(root) {
  const temporary = mkdtempSync(join(tmpdir(), "acyclic-harness-limits-contract-"));
  try {
    generateHarnessLimitsContract(root, temporary);
    const committed = join(root, outputRelativePath);
    if (!existsSync(committed) || !readFileSync(join(temporary, "limits-contract.ts")).equals(readFileSync(committed))) {
      throw new Error("Harness limits contract is stale; run bun run generate");
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

if (resolve(process.argv[1] ?? "") === resolve(scriptPath)) {
  const root = join(dirname(scriptPath), "..");
  const mode = process.argv[2] ?? "write";
  if (mode === "check") checkHarnessLimitsContract(root);
  else if (mode === "write") generateHarnessLimitsContract(root);
  else throw new Error(`unknown mode: ${mode}`);
}
