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

export const rust = [["acyclic-harness", "example", "limits-contract"]];

function readRustContract(stdout) {
  const contract = JSON.parse(stdout);
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

export function render([stdout]) {
  const contract = readRustContract(stdout);
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
  return { [outputRelativePath]: lines.join("\n") };
}
