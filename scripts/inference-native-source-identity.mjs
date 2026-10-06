import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync, realpathSync } from "node:fs";
import { relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");

function cargoMetadata(root, manifestPath, target) {
  const result = spawnSync("cargo", [
    "metadata", "--locked", "--format-version", "1",
    "--manifest-path", manifestPath,
    "--filter-platform", target,
  ], { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  if (result.status !== 0) {
    throw new Error(`cargo metadata failed for ${target}: ${result.stderr.trim() || "unknown error"}`);
  }
  return JSON.parse(result.stdout);
}

function localPackageFiles(root, packagePath) {
  const files = [];
  const visit = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      if (entry.name === ".git" || entry.name === "target") continue;
      const path = `${directory}/${entry.name}`;
      if (entry.isDirectory()) visit(path);
      else if (entry.isFile()) files.push(path);
    }
  };
  visit(packagePath);
  return files;
}

function relativePath(root, path) {
  const result = relative(root, path).replaceAll("\\", "/");
  if (!result || result.startsWith("../") || result === "..") {
    throw new Error(`runtime source path escapes checkout: ${path}`);
  }
  return result;
}

const MODEL_DESCRIPTOR_ENV = "ACYCLIC_INFERENCE_MODEL_DESCRIPTOR";
const REQUIRED_WORKSPACE_INPUTS = [
  "Cargo.toml",
  "Cargo.lock",
  "rust-toolchain.toml",
  ".cargo/config.toml",
];

function workspaceInputs(root) {
  return REQUIRED_WORKSPACE_INPUTS.map((path) => {
    const absolute = resolve(root, path);
    if (!existsSync(absolute)) throw new Error(`runtime source identity input is missing: ${path}`);
    return path;
  });
}

function selectedDescriptor(root, environment) {
  const inferenceRoot = resolve(root, "rust/crates/inference");
  const configured = environment[MODEL_DESCRIPTOR_ENV]?.trim() || null;
  const requested = configured ?? "inference_model_descriptor_docs.bin";
  const absolute = realpathSync(resolve(inferenceRoot, requested));
  const path = relativePath(root, absolute);
  const descriptorBytes = readFileSync(absolute);
  return {
    environment: configured,
    path,
    sha256: sha256(descriptorBytes),
  };
}

function toolchainIdentity(environment) {
  const rustcCommand = environment.RUSTC?.trim() || "rustc";
  const rustc = spawnSync(rustcCommand, ["-Vv"], {
    encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
  });
  if (rustc.status !== 0) {
    throw new Error(`rustc toolchain identity failed: ${rustc.stderr?.trim() || "unknown error"}`);
  }
  const linkerEnvironment = Object.entries(environment)
    .filter(([key]) => /^(?:CARGO_TARGET_.+_LINKER|(?:CC|CXX|AR)(?:_.+)?)$/i.test(key))
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => [key, value]);
  return {
    rustc_command: rustcCommand,
    rustc_verbose: rustc.stdout.trim(),
    linker_environment: linkerEnvironment,
  };
}

function normalizedBuildRecipe(metadata, root, target, reachable, inputs) {
  const packages = new Map(metadata.packages.map((item) => [item.id, item]));
  const nodes = new Map(metadata.resolve.nodes.map((item) => [item.id, item]));
  const normalized = {};
  for (const id of [...reachable].sort()) {
    const packageValue = packages.get(id);
    const node = nodes.get(id);
    if (!packageValue || !node) throw new Error(`Cargo metadata omitted reachable package ${id}`);
    let manifest = null;
    if (packageValue.manifest_path) {
      const absoluteManifest = resolve(packageValue.manifest_path);
      const relativeManifest = relative(root, absoluteManifest).replaceAll("\\", "/");
      if (relativeManifest && !relativeManifest.startsWith("../") && relativeManifest !== "..") {
        manifest = relativeManifest;
      }
    }
    const key = `${packageValue.name}@${packageValue.version}:${manifest ?? "registry"}`;
    const packageDirectory = packageValue.manifest_path
      ? resolve(packageValue.manifest_path, "..") : root;
    const targets = (packageValue.targets ?? []).map((item) => ({
      name: item.name,
      kind: item.kind,
      crate_types: item.crate_types,
      required_features: item.required_features,
    })).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    const dependencies = (packageValue.dependencies ?? []).map((dependency) => {
      let path = dependency.path;
      if (path) path = relativePath(root, resolve(packageDirectory, path));
      return {
        name: dependency.name,
        source: dependency.source,
        req: dependency.req,
        kind: dependency.kind,
        rename: dependency.rename,
        optional: dependency.optional,
        uses_default_features: dependency.uses_default_features,
        features: dependency.features,
        target: dependency.target,
        registry: dependency.registry,
        path,
      };
    }).sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
    normalized[key] = {
      name: packageValue.name,
      version: packageValue.version,
      source: packageValue.source ?? "local",
      manifest,
      features: node.features,
      targets,
      dependencies,
    };
  }
  const rootPackage = packages.get(metadata.resolve.root);
  if (!rootPackage) throw new Error("Cargo metadata root package is missing");
  return {
    schema: "acyclic.sdk.cargo-build-recipe.v1",
    target,
    build_inputs: inputs,
    root: `${rootPackage.name}@${rootPackage.version}`,
    packages: normalized,
  };
}

export function runtimeSourceIdentityFromMetadata(rootDirectory, metadata, target, options = {}) {
  const root = resolve(rootDirectory);
  const environment = options.environment ?? process.env;
  const descriptor = selectedDescriptor(root, environment);
  const inputs = {
    workspace_files: workspaceInputs(root),
    descriptor,
    toolchain: options.toolchain ?? toolchainIdentity(environment),
  };
  const nodes = new Map(metadata.resolve.nodes.map((item) => [item.id, item]));
  const pending = [metadata.resolve.root];
  const reachable = new Set();
  while (pending.length > 0) {
    const id = pending.shift();
    if (reachable.has(id)) continue;
    reachable.add(id);
    const node = nodes.get(id);
    if (!node) throw new Error(`Cargo metadata node is missing for ${id}`);
    pending.push(...(node.dependencies ?? []));
  }
  const packages = new Map(metadata.packages.map((item) => [item.id, item]));
  const files = new Set(inputs.workspace_files);
  files.add(descriptor.path);
  for (const id of reachable) {
    const packageValue = packages.get(id);
    if (!packageValue || packageValue.source !== null) continue;
    const packageDirectory = resolve(packageValue.manifest_path, "..");
    for (const file of localPackageFiles(root, packageDirectory)) files.add(relativePath(root, file));
  }
  const paths = [...files].sort();
  const entries = paths.map((path) => ({ path, sha256: sha256(readFileSync(resolve(root, path))) }));
  const closure = createHash("sha256");
  for (const entry of entries) {
    closure.update(entry.path);
    closure.update(Buffer.from([0]));
    closure.update(readFileSync(resolve(root, entry.path)));
    closure.update(Buffer.from([0]));
  }
  closure.update("__acyclic_build_inputs__");
  closure.update(Buffer.from([0]));
  closure.update(JSON.stringify(inputs));
  closure.update(Buffer.from([0]));
  const recipe = normalizedBuildRecipe(metadata, root, target, reachable, inputs);
  const recipeBytes = Buffer.from(JSON.stringify(recipe));
  return {
    closureSha256: closure.digest("hex"),
    recipeSha256: sha256(recipeBytes),
    files: entries,
    recipe,
    descriptor,
    toolchain: inputs.toolchain,
  };
}

export function runtimeSourceIdentity(rootDirectory, rustManifestPath, target) {
  const root = resolve(rootDirectory);
  const metadata = cargoMetadata(root, resolve(rustManifestPath), target);
  return runtimeSourceIdentityFromMetadata(root, metadata, target);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const targetIndex = process.argv.indexOf("--target");
  const target = targetIndex >= 0 ? process.argv[targetIndex + 1] : undefined;
  if (!target) throw new Error("usage: inference-native-source-identity.mjs --target <rust-target>");
  const root = resolve(fileURLToPath(new URL("..", import.meta.url)));
  const identity = runtimeSourceIdentity(root, `${root}/rust/crates/sdk-inference-native/Cargo.toml`, target);
  console.log(JSON.stringify({
    schema: "acyclic.sdk.inference.native.runtime-source-identity.v1",
    target,
    closure_sha256: identity.closureSha256,
    recipe_sha256: identity.recipeSha256,
    files: identity.files,
    descriptor: identity.descriptor,
    toolchain: identity.toolchain,
  }, null, 2));
}
