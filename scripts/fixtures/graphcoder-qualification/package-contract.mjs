import { createHash } from "node:crypto";
import { lstatSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

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
    identity.artifact = {
      path: archive,
      sha256: createHash("sha256").update(readFileSync(archive)).digest("hex"),
    };
  }
  return identity;
}

export function assertLazyCounters(observationPath, { require = false } = {}) {
  if (observationPath === undefined || observationPath.trim() === "") {
    if (require) fail("lazy observation path is required for a real-host qualification");
    return undefined;
  }
  const path = resolve(observationPath);
  regularFile(path, "lazy observation");
  let observation;
  try { observation = JSON.parse(readFileSync(path, "utf8")); }
  catch (error) { fail(`lazy observation is not valid JSON: ${error instanceof Error ? error.message : String(error)}`); }
  const listing = observation?.during_list_sessions;
  if (listing === null || typeof listing !== "object" || Array.isArray(listing)) fail("lazy observation must contain during_list_sessions");
  for (const field of ["worker_starts", "workspace_reads", "model_dispatches"]) {
    if (!Number.isSafeInteger(listing[field]) || listing[field] < 0) fail(`lazy observation ${field} must be a nonnegative integer`);
    if (listing[field] !== 0) fail(`session listing performed ${listing[field]} ${field.replaceAll("_", " ")}`);
  }
  return { path, during_list_sessions: listing };
}
