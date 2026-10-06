import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync, realpathSync } from "node:fs";
import { delimiter, isAbsolute, join, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");

function cargoMetadata(root, manifestPath, target) {
  const result = spawnSync("cargo", [
    "metadata", "--locked", "--format-version", "1",
    "--manifest-path", manifestPath,
    "--filter-platform", target,
  ], {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    maxBuffer: 64 * 1024 * 1024,
  });
  if (result.status !== 0) {
    const detail = result.error?.message || result.stderr.trim() || `exit status ${result.status ?? "unknown"}`;
    throw new Error(`cargo metadata failed for ${target}: ${detail}`);
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

function environmentValue(environment, name) {
  const entry = Object.entries(environment).find(([key]) => key.toUpperCase() === name.toUpperCase());
  return entry?.[1];
}

function validateToolchainOverrides(environment) {
  for (const name of ["RUSTC", "CARGO"]) {
    const value = environmentValue(environment, name)?.trim();
    if (value && value.toLowerCase() !== name.toLowerCase()) {
      throw new Error(`${name} override is unsupported; use the selected rustup toolchain command ${name.toLowerCase()}`);
    }
  }
}

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

function executablePath(command, environment) {
  const token = command.trim().replace(/^['"]|['"]$/g, "");
  const pathCommand = isAbsolute(token) || token.includes("/") || token.includes("\\");
  if (!token || (!pathCommand && /\s/.test(token))) {
    throw new Error(`toolchain executable command is ambiguous: ${command}`);
  }
  const candidates = pathCommand
    ? [token]
    : (environmentValue(environment, "PATH") ?? "").split(delimiter).filter(Boolean).flatMap((directory) => [
        resolve(directory, token),
        ...(process.platform === "win32" && !token.toLowerCase().endsWith(".exe")
          ? [resolve(directory, `${token}.exe`)] : []),
      ]);
  for (const candidate of candidates) {
    if (existsSync(candidate)) return realpathSync(candidate);
  }
  throw new Error(`toolchain executable is not available on PATH: ${token}`);
}

function executableIdentity(label, command, environment, required = false) {
  if (!command?.trim()) return null;
  let path;
  try {
    path = executablePath(command, environment);
  } catch (error) {
    if (required) throw error;
    return { label, command: command.trim(), path: null, sha256: null };
  }
  return {
    label,
    command: command.trim(),
    path,
    sha256: sha256(readFileSync(path)),
  };
}

function buildEnvironment(environment) {
  return Object.entries(environment)
    .filter(([key]) => /^(?:RUSTFLAGS|CARGO_ENCODED_RUSTFLAGS|CARGO_BUILD_RUSTFLAGS|CARGO_TARGET_.+_RUSTFLAGS|CARGO_PROFILE_.+|CARGO_BUILD_TARGET|CARGO_INCREMENTAL|RUSTC|RUSTC_WRAPPER|RUSTC_WORKSPACE_WRAPPER|CFLAGS|CXXFLAGS|CPPFLAGS|ARFLAGS)(?:_.+)?$/i.test(key))
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => [key, value]);
}

function probe(command, args, environment, label) {
  const result = spawnSync(command, args, {
    encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], env: environment,
  });
  if (result.status !== 0) {
    throw new Error(`${label} probe failed: ${result.error?.message || result.stderr?.trim() || `exit status ${result.status ?? "unknown"}`}`);
  }
  return result.stdout.trim();
}

function rustupWhich(tool, environment) {
  try {
    const command = executablePath("rustup", environment);
    return probe(command, ["which", tool], environment, `rustup which ${tool}`).split(/\r?\n/).at(-1);
  } catch {
    return null;
  }
}

function msvcLinkerPath(target, environment) {
  const architecture = target.includes("aarch64") ? "arm64" : "x64";
  const roots = [];
  for (const name of ["VCToolsInstallDir", "VCINSTALLDIR", "VSINSTALLDIR"]) {
    const value = environmentValue(environment, name)?.trim();
    if (value) roots.push(value);
  }
  const candidates = [];
  for (const root of roots) {
    candidates.push(join(root, "bin", "Hostx64", architecture, "link.exe"));
    candidates.push(join(root, "VC", "Tools", "MSVC", "bin", "Hostx64", architecture, "link.exe"));
  }
  const programFilesX86 = environmentValue(environment, "ProgramFiles(x86)");
  const programFiles = environmentValue(environment, "ProgramFiles");
  for (const base of [programFilesX86, programFiles].filter(Boolean)) {
    candidates.push(join(base, "Microsoft Visual Studio", "Installer", "vswhere.exe"));
  }
  for (const candidate of candidates.slice()) {
    if (candidate.toLowerCase().endsWith("vswhere.exe") && existsSync(candidate)) {
      try {
        const installation = probe(candidate, [
          "-latest", "-products", "*",
          "-requires", "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
          "-property", "installationPath",
        ], environment, "vswhere");
        if (installation) roots.push(installation);
      } catch { /* strict linker resolution below reports the missing toolchain */ }
    }
  }
  for (const root of roots) {
    const normalizedRoot = root.toLowerCase().replaceAll("/", "\\");
    const msvcRoot = normalizedRoot.includes("\\vc\\tools\\msvc")
      ? root
      : join(root, "VC", "Tools", "MSVC");
    const versions = existsSync(msvcRoot)
      ? [msvcRoot, ...readdirSync(msvcRoot, { withFileTypes: true })
          .filter((entry) => entry.isDirectory())
          .sort((left, right) => right.name.localeCompare(left.name))
          .map((entry) => join(msvcRoot, entry.name))]
      : [];
    for (const version of versions) {
      candidates.push(join(version, "bin", "Hostx64", architecture, "link.exe"));
      candidates.push(join(version, "bin", "Hostarm64", architecture, "link.exe"));
    }
  }
  for (const candidate of candidates) {
    if (existsSync(candidate) && !candidate.toLowerCase().endsWith("vswhere.exe") &&
        !candidate.toLowerCase().includes("git\\usr\\bin")) {
      return realpathSync(candidate);
    }
  }
  return null;
}

function configuredLinker(root, target, environment) {
  const targetEnvironment = target.toUpperCase().replaceAll("-", "_");
  for (const key of [`CARGO_TARGET_${targetEnvironment}_LINKER`, "RUSTC_LINKER"]) {
    const value = environmentValue(environment, key)?.trim();
    if (value) {
      if (target.includes("windows-msvc") && value.toLowerCase().split(/[\\/]/).at(-1) === "link.exe") {
        const msvc = msvcLinkerPath(target, environment);
        return { command: msvc ?? value, source: msvc ? "msvc-toolchain" : `environment:${key}-unresolved` };
      }
      return { command: value, source: `environment:${key}` };
    }
  }
  const configPath = resolve(root, ".cargo/config.toml");
  if (existsSync(configPath)) {
    const lines = readFileSync(configPath, "utf8").split(/\r?\n/);
    let section = "";
    for (const line of lines) {
      const header = line.match(/^\s*\[target\.\"([^\"]+)\"\]\s*$/);
      if (header) {
        section = header[1];
        continue;
      }
      const linker = line.match(/^\s*linker\s*=\s*[\"']([^\"']+)[\"']/);
      if (linker && section === target) return { command: linker[1], source: `config:${target}` };
    }
  }
  if (target.includes("windows-msvc")) {
    const msvc = msvcLinkerPath(target, environment);
    return {
      command: msvc ?? "link.exe",
      source: msvc ? "msvc-toolchain" : "msvc-toolchain-unresolved",
    };
  }
  const defaultCommand = target.includes("windows") ? "link" : "cc";
  return { command: defaultCommand, source: `platform-default:${defaultCommand}` };
}

function toolchainIdentity(root, target, environment, requiredExecutables) {
  const rustcCommand = environmentValue(environment, "RUSTC")?.trim() || "rustc";
  const rustcVerbose = probe(rustcCommand, ["-Vv"], environment, "rustc");
  const sysroot = probe(rustcCommand, ["--print", "sysroot"], environment, "rustc sysroot");
  const actualRustc = rustupWhich("rustc", environment) || join(sysroot, "bin", process.platform === "win32" ? "rustc.exe" : "rustc");
  const cargoCommand = environmentValue(environment, "CARGO")?.trim() || "cargo";
  const cargoVerbose = probe(cargoCommand, ["-Vv"], environment, "cargo");
  const actualCargo = rustupWhich("cargo", environment) || join(sysroot, "bin", process.platform === "win32" ? "cargo.exe" : "cargo");
  const linkerEnvironment = Object.entries(environment)
    .filter(([key]) => /^(?:CARGO_TARGET_.+_LINKER|(?:CC|CXX|AR)(?:_.+)?)$/i.test(key))
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => [key, value]);
  const linker = configuredLinker(root, target, environment);
  const executableInputs = [
    executableIdentity("rustc-proxy", rustcCommand, environment, requiredExecutables),
    executableIdentity("rustc", actualRustc, environment, requiredExecutables),
    executableIdentity("cargo-proxy", cargoCommand, environment, requiredExecutables),
    executableIdentity("cargo", actualCargo, environment, requiredExecutables),
    executableIdentity("rustc-wrapper", environmentValue(environment, "RUSTC_WRAPPER"), environment, requiredExecutables),
    executableIdentity("rustc-workspace-wrapper", environmentValue(environment, "RUSTC_WORKSPACE_WRAPPER"), environment, requiredExecutables),
    executableIdentity("effective-linker", linker.command, environment, requiredExecutables),
    ...linkerEnvironment.map(([key, value]) => executableIdentity(key, value, environment, requiredExecutables)),
  ].filter(Boolean);
  return {
    rustc_command: rustcCommand,
    rustc_verbose: rustcVerbose,
    rustc_sysroot: sysroot,
    cargo_command: cargoCommand,
    cargo_verbose: cargoVerbose,
    linker_environment: linkerEnvironment,
    effective_linker: linker,
    executables: executableInputs,
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
  validateToolchainOverrides(environment);
  const descriptor = selectedDescriptor(root, environment);
  const inputs = {
    workspace_files: workspaceInputs(root),
    descriptor,
    environment: buildEnvironment(environment),
    toolchain: options.toolchain ?? toolchainIdentity(root, target, environment, options.requiredExecutables ?? false),
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
  validateToolchainOverrides(process.env);
  const metadata = cargoMetadata(root, resolve(rustManifestPath), target);
  return runtimeSourceIdentityFromMetadata(root, metadata, target, { requiredExecutables: true });
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
