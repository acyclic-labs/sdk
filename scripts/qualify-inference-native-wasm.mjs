#!/usr/bin/env node

// Qualify an installable Rust-generated Inference facade together with its
// platform companion.  This is deliberately a package-boundary check: it
// installs the two produced archives into a throwaway consumer and exercises
// the public fromEnv entry point, rather than importing files from the source
// checkout.
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { arch, platform, report } from "node:process";
import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";

export const QUALIFICATION_SCHEMA = "acyclic.sdk.inference.native-wasm-installed-consumer-qualification.v1";

function value(args, name) {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
}

export function parseArgs(args) {
  if (args.includes("--help")) {
    return { help: true };
  }
  const packageArchive = value(args, "--package-archive");
  const nativeArchive = value(args, "--native-archive");
  const output = value(args, "--output");
  if (!packageArchive || !nativeArchive || !output) {
    throw new Error("usage: qualify-inference-native-wasm.mjs --package-archive FILE --native-archive FILE --output FILE [--npm-cache DIR] [--keep-consumer]");
  }
  return {
    packageArchive: resolve(packageArchive),
    nativeArchive: resolve(nativeArchive),
    output: resolve(output),
    npmCache: value(args, "--npm-cache") ? resolve(value(args, "--npm-cache")) : undefined,
    keepConsumer: args.includes("--keep-consumer"),
  };
}

export function companionTarget(
  currentPlatform = platform,
  currentArch = arch,
  currentReport = report,
) {
  const normalizedArch = currentArch === "x64" || currentArch === "arm64" ? currentArch : currentArch;
  const base = `${currentPlatform}-${normalizedArch}`;
  if (currentPlatform !== "linux") return base;
  let glibc;
  try {
    const header = currentReport?.getReport?.().header;
    if (header && typeof header === "object" && typeof header.glibcVersionRuntime === "string") {
      glibc = true;
    }
  } catch {
    // A missing diagnostic report is classified conservatively as musl.
  }
  return `${base}-${glibc ? "gnu" : "musl"}`;
}

export function sha256(path) {
  return `sha256:${createHash("sha256").update(readFileSync(path)).digest("hex")}`;
}

export function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function requireInput(condition, message) {
  if (!condition) throw new Error(message);
}

export function validateOptionalCompanion(facade, companion, target) {
  requireInput(facade?.name === "@acyclic-labs/inference", `unexpected Inference facade package: ${facade?.name ?? "missing"}`);
  requireInput(typeof facade?.version === "string" && facade.version.length > 0, "Inference facade has no version");
  const expectedName = `@acyclic-labs/inference-${target}`;
  requireInput(companion?.name === expectedName, `native companion package is ${companion?.name ?? "missing"}, expected ${expectedName}`);
  requireInput(typeof companion?.version === "string" && companion.version.length > 0, "native companion has no version");
  requireInput(facade.optionalDependencies?.[expectedName] === companion.version,
    `facade optional dependency does not pin ${expectedName}@${companion.version}`);
  const generated = facade.acyclicGenerated;
  requireInput(generated?.family === "inference", "facade package is missing Rust Inference generation metadata");
  requireInput(generated?.nativeCompanions?.[expectedName] === companion.version,
    `Rust generation metadata does not map ${expectedName} to ${companion.version}`);
  requireInput(generated?.nativeRuntime !== undefined,
    "facade package is missing its Rust native runtime identity");
  const packageFiles = new Set(companion.files ?? []);
  requireInput(companion.main === "index.js" && packageFiles.has("index.js"), "native companion does not expose its Rust loader");
  requireInput(packageFiles.has("acyclic_inference_native.node") && packageFiles.has("BUILD.json"),
    "native companion package does not include its binary and BUILD.json");
  return { expectedName, facadeVersion: facade.version, companionVersion: companion.version };
}

export function validateSourceIdentity(provenance, build) {
  const generated = provenance?.acyclicGenerated ?? provenance;
  requireInput(/^[0-9a-f]{40}$/i.test(generated?.sourceGitSha ?? ""),
    "facade provenance has no immutable source Git OID");
  requireInput(/^[0-9a-f]{40}$/i.test(build?.source_revision ?? ""),
    "native BUILD.json has no immutable source Git OID");
  requireInput(generated.sourceGitSha.toLowerCase() === build.source_revision.toLowerCase(),
    "facade and native companion source Git OIDs differ");

  const modelValue = generated.sourceModelSha256 ?? generated.sourceModelRevision;
  const model = typeof modelValue === "string" && modelValue.startsWith("sha256:")
    ? modelValue.slice("sha256:".length)
    : modelValue;
  const nativeModel = build.source_model_revision;
  requireInput(/^[0-9a-f]{64}$/i.test(model ?? ""), "facade provenance has no Rust model identity");
  requireInput(/^[0-9a-f]{64}$/i.test(nativeModel ?? ""), "native BUILD.json has no Rust model identity");
  requireInput(model.toLowerCase() === nativeModel.toLowerCase(), "facade and native model identities differ");

  const content = generated.sourceContentSha256?.startsWith("sha256:")
    ? generated.sourceContentSha256
    : `sha256:${generated.sourceContentSha256 ?? ""}`;
  requireInput(/^sha256:[0-9a-f]{64}$/i.test(content), "facade provenance has no Rust source-content identity");
  requireInput(/^sha256:[0-9a-f]{64}$/i.test(build.source_content_sha256 ?? ""),
    "native BUILD.json has no Rust source-content identity");
  requireInput(content.toLowerCase() === build.source_content_sha256.toLowerCase(),
    "facade and native source-content identities differ");
  return {
    git: generated.sourceGitSha,
    model: `sha256:${model}`,
    content,
  };
}

function digest(value, label) {
  const normalized = typeof value === "string" && value.startsWith("sha256:")
    ? value
    : `sha256:${value ?? ""}`;
  requireInput(/^sha256:[0-9a-f]{64}$/i.test(normalized), `${label} is missing or malformed`);
  return normalized.toLowerCase();
}

export function validateRuntimeIdentity(build, expected = undefined) {
  const closure = digest(build?.runtime_source_closure_sha256, "native runtime source closure");
  const recipe = digest(build?.runtime_build_recipe_sha256, "native runtime build recipe");
  const files = build?.runtime_source_files;
  requireInput(Array.isArray(files) && files.length > 0, "native runtime source file inventory is missing");
  const paths = new Set();
  for (const entry of files) {
    requireInput(entry && typeof entry.path === "string" && entry.path.length > 0 &&
      !entry.path.startsWith("/") && !entry.path.includes("\\") && !entry.path.split("/").includes(".."),
    "native runtime source file inventory contains an invalid path");
    requireInput(!paths.has(entry.path), `native runtime source file inventory repeats ${entry.path}`);
    paths.add(entry.path);
    requireInput(/^[0-9a-f]{64}$/i.test(entry.sha256 ?? ""),
      `native runtime source file hash is missing for ${entry.path}`);
  }
  requireInput(paths.has("Cargo.lock"), "native runtime source inventory omits Cargo.lock");
  if (expected !== undefined) {
    if (expected.runtime_source_closure_sha256 !== undefined) {
      requireInput(closure === digest(expected.runtime_source_closure_sha256, "advertised runtime source closure"),
        "facade and native runtime source closures differ");
    }
    if (expected.runtime_build_recipe_sha256 !== undefined) {
      requireInput(recipe === digest(expected.runtime_build_recipe_sha256, "advertised runtime build recipe"),
        "facade and native runtime build recipes differ");
    }
  }
  return { closure, recipe, files };
}

function npmExecutable() {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

function installArchives(options, consumer) {
  const installArgs = [
    "install",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
    "--package-lock=false",
    "--offline",
    "--include=optional",
    "--prefix",
    consumer,
    options.packageArchive,
    options.nativeArchive,
  ];
  if (options.npmCache) installArgs.push("--cache", options.npmCache);
  const result = spawnSync(npmExecutable(), installArgs, {
    cwd: consumer,
    encoding: "utf8",
    shell: process.platform === "win32",
    windowsHide: true,
  });
  if (result.status !== 0) {
    throw new Error(`npm installed consumer failed:\n${result.stdout ?? ""}${result.stderr ?? ""}`);
  }
  return {
    command: [npmExecutable(), ...installArgs],
    stdoutSha256: `sha256:${createHash("sha256").update(result.stdout ?? "").digest("hex")}`,
  };
}

async function runFromEnvProbe(consumer, target) {
  const probePath = join(consumer, "inference-native-probe.mjs");
  writeFileSync(probePath, `
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fromEnv } from "@acyclic-labs/inference";
const client = fromEnv({ endpoint: "http://127.0.0.1:1", token: "qualification" });
if (client.transport.constructor.name !== "DeferredNativeInferenceTransport") {
  throw new Error("fromEnv did not select the Rust native transport facade");
}
const controller = new AbortController();
controller.abort();
try {
  await client.inspectRun(new Uint8Array(16), controller.signal);
  throw new Error("pre-aborted native operation unexpectedly resolved");
} catch (error) {
  if (error?.name !== "AbortError") throw error;
}
const require = createRequire(import.meta.url);
const facadeEntry = require.resolve("@acyclic-labs/inference");
const facadeRoot = join(dirname(facadeEntry), "..");
const facadeManifest = JSON.parse(readFileSync(join(facadeRoot, "package.json"), "utf8"));
const companionName = "@acyclic-labs/inference-${target}";
if (facadeManifest.optionalDependencies?.[companionName] === undefined) {
  throw new Error("installed facade does not declare the selected companion as an optional dependency");
}
const facadeRequire = createRequire(facadeEntry);
const native = facadeRequire(companionName);
if (typeof native.NativeInferenceClient !== "function" || typeof native.NativeInferenceCancellation !== "function") {
  throw new Error("installed native companion does not expose the Rust Inference bridge");
}
const cancellation = new native.NativeInferenceCancellation();
if (cancellation.cancelled !== false) throw new Error("native cancellation did not start active");
cancellation.cancel();
if (cancellation.cancelled !== true) throw new Error("native cancellation did not become cancelled");
console.log(JSON.stringify({ transport: client.transport.constructor.name, cancellation: "passed", companion: companionName }));
`, "utf8");
  const result = spawnSync(process.execPath, [probePath], {
    cwd: consumer,
    encoding: "utf8",
    windowsHide: true,
  });
  if (result.status !== 0) {
    throw new Error(`installed Inference consumer failed:\n${result.stdout ?? ""}${result.stderr ?? ""}`);
  }
  const line = result.stdout.trim().split(/\r?\n/).at(-1);
  return JSON.parse(line);
}

async function verifyWasm(installedFacade) {
  const wasmDir = join(installedFacade, "generated", "wasm");
  const modulePath = join(wasmDir, "acyclic_inference_wasm.js");
  const binaryPath = join(wasmDir, "acyclic_inference_wasm_bg.wasm");
  requireInput(existsSync(modulePath) && existsSync(binaryPath), "installed Inference facade is missing Rust WASM artifacts");
  const wasm = await import(pathToFileURL(modulePath).href);
  requireInput(typeof wasm.initSync === "function" && typeof wasm.run_terminal_metadata === "function",
    "installed Inference facade lacks its Rust WASM exports");
  wasm.initSync({ module: new Uint8Array(readFileSync(binaryPath)) });
  const terminalMetadata = wasm.run_terminal_metadata();
  requireInput(typeof terminalMetadata === "string" && terminalMetadata.length > 0,
    "Rust Inference WASM terminal metadata is empty");
  return {
    status: "passed",
    module_sha256: sha256(modulePath),
    binary_sha256: sha256(binaryPath),
  };
}

export async function qualify(options) {
  for (const path of [options.packageArchive, options.nativeArchive]) {
    requireInput(existsSync(path), `missing package archive: ${path}`);
  }
  mkdirSync(dirname(options.output), { recursive: true });
  const tempRoot = mkdtempSync(join(tmpdir(), "acyclic-sdk-inference-installed-"));
  const consumer = join(tempRoot, "consumer");
  mkdirSync(consumer, { recursive: true });
  writeFileSync(join(consumer, "package.json"), JSON.stringify({
    name: "acyclic-inference-installed-consumer",
    private: true,
    version: "0.0.0",
    type: "module",
  }, null, 2));
  let receipt;
  try {
    const install = installArchives(options, consumer);
    const installedFacade = join(consumer, "node_modules", "@acyclic-labs", "inference");
    const target = companionTarget();
    const installedCompanion = join(consumer, "node_modules", "@acyclic-labs", `inference-${target}`);
    const facadeManifest = readJson(join(installedFacade, "package.json"));
    const companionManifest = readJson(join(installedCompanion, "package.json"));
    const mapping = validateOptionalCompanion(facadeManifest, companionManifest, target);
    const provenance = readJson(join(installedFacade, "generated", "rust-provenance.json"));
    const build = readJson(join(installedCompanion, "BUILD.json"));
    const identity = validateSourceIdentity(provenance, build);
    const runtimeIdentity = validateRuntimeIdentity(build, facadeManifest.acyclicGenerated.nativeRuntime);
    const wasm = await verifyWasm(installedFacade);
    const probe = await runFromEnvProbe(consumer, target);
    requireInput(probe.companion === mapping.expectedName,
      "installed facade did not resolve the Rust companion declared by its optional dependency mapping");
    receipt = {
      schema: QUALIFICATION_SCHEMA,
      status: "passed",
      package: {
        name: facadeManifest.name,
        version: facadeManifest.version,
        archive: resolve(options.packageArchive),
        archive_sha256: sha256(options.packageArchive),
      },
      native_companion: {
        name: companionManifest.name,
        version: companionManifest.version,
        target,
        archive: resolve(options.nativeArchive),
        archive_sha256: sha256(options.nativeArchive),
        build_sha256: sha256(join(installedCompanion, "BUILD.json")),
      },
      source_identity: identity,
      runtime_identity: runtimeIdentity,
      checks: {
        npm_install: { status: "passed", ...install },
        optional_dependency_mapping: { status: "passed", ...mapping },
        wasm,
        from_env: { status: "passed", ...probe },
        installed_companion: { status: "passed", package: companionManifest.name },
      },
      consumer_root: options.keepConsumer ? consumer : undefined,
    };
    writeFileSync(options.output, `${JSON.stringify(receipt, null, 2)}\n`);
    return receipt;
  } finally {
    if (!options.keepConsumer) {
      try { rmSync(tempRoot, { recursive: true, force: true }); } catch { /* native DLLs may remain locked on Windows */ }
    }
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  try {
    const options = parseArgs(process.argv.slice(2));
    if (options.help) {
      console.log("usage: qualify-inference-native-wasm.mjs --package-archive FILE --native-archive FILE --output FILE [--npm-cache DIR] [--keep-consumer]");
    } else {
      const receipt = await qualify(options);
      console.log(JSON.stringify({ schema: receipt.schema, status: receipt.status, output: options.output }));
    }
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
