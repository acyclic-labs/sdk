import { createHash } from "node:crypto";
import { lstatSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { readBoundedGzip, tarEntries } from "../../archive-utils.mjs";

export const GRAPH_CODER_PACKAGE = "@acyclic-labs/graphcoder";
export const GRAPH_CODER_VERSION = "0.2.0";
export const GRAPH_CODER_EXPORTS = [
  GRAPH_CODER_PACKAGE,
  `${GRAPH_CODER_PACKAGE}/mock`,
  `${GRAPH_CODER_PACKAGE}/bridge`,
  `${GRAPH_CODER_PACKAGE}/node`,
  `${GRAPH_CODER_PACKAGE}/terminal`,
  `${GRAPH_CODER_PACKAGE}/node-dispatcher`,
  `${GRAPH_CODER_PACKAGE}/native-cli`,
];

function fail(message) {
  throw new Error(`graphcoder-package-contract: ${message}`);
}

function regularFile(path, label) {
  const metadata = lstatSync(path, { throwIfNoEntry: false });
  if (!metadata?.isFile() || metadata.isSymbolicLink()) fail(`${label} must be a regular file: ${path}`);
}

function inside(root, path, label) {
  const value = relative(root, path);
  if (value === "" || value === ".." || value.startsWith(`..\\`) || value.startsWith(`../`) || /^[A-Za-z]:/u.test(value)) {
    fail(`${label} escapes the installed package root: ${path}`);
  }
}

function exportedTargets(value) {
  if (typeof value === "string") return [value];
  if (value === null || typeof value !== "object" || Array.isArray(value)) return [];
  return Object.values(value).flatMap(exportedTargets);
}

function inspectArtifact(archive, packageJson) {
  let expanded;
  try { expanded = readBoundedGzip(archive, 100 * 1024 * 1024, 512 * 1024 * 1024).expanded; }
  catch (error) { fail(`package artifact is not a valid bounded gzip archive: ${error instanceof Error ? error.message : String(error)}`); }
  let manifestEntry;
  const files = new Set();
  for (const entry of tarEntries(expanded)) {
    if (entry.path.startsWith("package/") && (entry.type === "0" || entry.type === "\\0")) {
      files.add(entry.path);
      if (entry.path === "package/package.json") manifestEntry = entry;
    }
  }
  if (manifestEntry === undefined) fail("package artifact is missing package/package.json");
  let artifactJson;
  try { artifactJson = JSON.parse(manifestEntry.body); }
  catch (error) { fail(`package artifact manifest is not valid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (artifactJson.name !== packageJson.name || artifactJson.version !== packageJson.version) {
    fail("installed package and package artifact have different name/version");
  }
  if (JSON.stringify(artifactJson.exports) !== JSON.stringify(packageJson.exports)
    || JSON.stringify(artifactJson.bin) !== JSON.stringify(packageJson.bin)) {
    fail("installed package export/bin metadata differs from package artifact");
  }
  for (const target of exportedTargets(packageJson.exports)) {
    if (!target.startsWith("./") || !files.has(`package/${target.slice(2)}`)) {
      fail(`package artifact is missing target ${target}`);
    }
  }
  for (const target of exportedTargets(packageJson.bin)) {
    const relativeTarget = target.startsWith("./") ? target.slice(2) : target;
    if (relativeTarget === "" || relativeTarget.startsWith("../") || !files.has(`package/${relativeTarget}`)) {
      fail(`package artifact is missing target ${target}`);
    }
  }
  return {
    manifest_sha256: createHash("sha256").update(manifestEntry.body).digest("hex"),
    exports: exportedTargets(packageJson.exports),
    bins: exportedTargets(packageJson.bin),
  };
}

export function loadInstalledExport(consumerRoot, specifier) {
  const require = createRequire(resolve(consumerRoot, "package.json"));
  const resolved = require.resolve(specifier);
  return { resolved, module: import(pathToFileURL(resolved).href) };
}

export function inspectInstalledPackage(packageRoot, { artifactPath } = {}) {
  const root = resolve(packageRoot);
  const packageJsonPath = resolve(root, "package.json");
  regularFile(packageJsonPath, "package.json");
  const packageJsonBytes = readFileSync(packageJsonPath);
  let packageJson;
  try { packageJson = JSON.parse(packageJsonBytes); }
  catch (error) { fail(`package.json is not valid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (packageJson.name !== GRAPH_CODER_PACKAGE) fail(`unexpected installed package name: ${String(packageJson.name)}`);
  if (packageJson.version !== GRAPH_CODER_VERSION) fail(`unexpected installed package version: ${String(packageJson.version)}`);
  const require = createRequire(resolve(root, "package.json"));
  const exports = {};
  for (const specifier of GRAPH_CODER_EXPORTS) {
    const resolved = resolve(require.resolve(specifier));
    inside(root, resolved, `${specifier} export`);
    regularFile(resolved, `${specifier} export`);
    exports[specifier] = resolved;
  }
  if (packageJson.bin === undefined || typeof packageJson.bin !== "object" || Array.isArray(packageJson.bin)) fail("package.json bin map is missing");
  const bins = {};
  for (const [name, target] of Object.entries(packageJson.bin)) {
    if (typeof target !== "string" || target.trim() === "") fail(`bin ${name} has no target`);
    const resolved = resolve(root, target);
    inside(root, resolved, `bin ${name}`);
    regularFile(resolved, `bin ${name}`);
    bins[name] = resolved;
  }
  const identity = {
    package: GRAPH_CODER_PACKAGE,
    version: GRAPH_CODER_VERSION,
    root,
    package_json_sha256: createHash("sha256").update(packageJsonBytes).digest("hex"),
    exports,
    bins,
  };
  if (artifactPath !== undefined) {
    const archive = resolve(artifactPath);
    regularFile(archive, "package artifact");
    const artifactContract = inspectArtifact(archive, packageJson);
    identity.artifact = {
      path: archive,
      sha256: createHash("sha256").update(readFileSync(archive)).digest("hex"),
      manifest_sha256: artifactContract.manifest_sha256,
      exports: artifactContract.exports,
      bins: artifactContract.bins,
    };
  }
  return identity;
}

export function assertLazyCounters(observationPath, {
  require = false,
  expectedRequestId,
  expectedMethod,
  expectedExecutable,
} = {}) {
  if (observationPath === undefined || observationPath.trim() === "") {
    if (require) fail("lazy observation path is required for a real-host qualification");
    return undefined;
  }
  const path = resolve(observationPath);
  regularFile(path, "lazy observation");
  let observation;
  try { observation = JSON.parse(readFileSync(path, "utf8")); }
  catch (error) { fail(`lazy observation is not valid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  if (observation?.schema !== "graphcoder.lazy-observation.v1") fail("lazy observation has an unsupported schema");
  const runtime = observation?.runtime;
  if (runtime === null || typeof runtime !== "object" || Array.isArray(runtime)) fail("lazy observation must identify the runtime");
  if (!Number.isSafeInteger(runtime.pid) || runtime.pid <= 0) fail("lazy observation runtime pid must be a positive integer");
  if (typeof runtime.executable !== "string" || runtime.executable.trim() === "") fail("lazy observation runtime executable is required");
  if (expectedExecutable !== undefined && resolve(runtime.executable) !== resolve(expectedExecutable)) {
    fail(`lazy observation runtime executable does not match ${resolve(expectedExecutable)}`);
  }
  const request = observation?.request;
  if (request === null || typeof request !== "object" || Array.isArray(request)) fail("lazy observation must identify the request");
  if (typeof request.request_id !== "string" || request.request_id.trim() === "") fail("lazy observation request id is required");
  if (typeof request.method !== "string" || request.method.trim() === "") fail("lazy observation request method is required");
  if (expectedRequestId !== undefined && request.request_id !== expectedRequestId) {
    fail(`lazy observation covers request ${JSON.stringify(request.request_id)} instead of ${JSON.stringify(expectedRequestId)}`);
  }
  if (expectedMethod !== undefined && request.method !== expectedMethod) {
    fail(`lazy observation covers method ${JSON.stringify(request.method)} instead of ${JSON.stringify(expectedMethod)}`);
  }
  const listing = observation?.during_list_sessions;
  if (listing === null || typeof listing !== "object" || Array.isArray(listing)) fail("lazy observation must contain during_list_sessions");
  const before = listing.counters_before;
  const after = listing.counters_after;
  if (before === null || typeof before !== "object" || Array.isArray(before)) fail("lazy observation must contain counters_before");
  if (after === null || typeof after !== "object" || Array.isArray(after)) fail("lazy observation must contain counters_after");
  for (const field of ["worker_starts", "workspace_reads", "model_dispatches"]) {
    if (!Number.isSafeInteger(before[field]) || before[field] < 0) fail(`lazy observation counters_before.${field} must be a nonnegative integer`);
    if (!Number.isSafeInteger(after[field]) || after[field] < 0) fail(`lazy observation counters_after.${field} must be a nonnegative integer`);
    if (after[field] < before[field]) fail(`lazy observation counters_after.${field} precedes counters_before`);
    const delta = after[field] - before[field];
    if (!Number.isSafeInteger(listing[field]) || listing[field] < 0) fail(`lazy observation ${field} must be a nonnegative integer delta`);
    if (listing[field] !== delta) fail(`lazy observation ${field} does not equal its measured counter delta`);
    if (delta !== 0) fail(`session listing performed ${delta} ${field.replaceAll("_", " ")}`);
  }
  return {
    path,
    during_list_sessions: Object.fromEntries(["worker_starts", "workspace_reads", "model_dispatches"].map(field => [field, listing[field]])),
  };
}
