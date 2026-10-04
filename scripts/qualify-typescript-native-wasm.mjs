#!/usr/bin/env node

// Release/manual qualification for the Rust-generated Stream package. The
// package and native companion must carry the same Git, model, and source
// content identities before either runtime lane is accepted.
import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync, mkdtempSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { pathToFileURL } from "node:url";
import { spawnSync } from "node:child_process";

const args = process.argv.slice(2);
const value = (name) => { const i = args.indexOf(name); return i < 0 ? undefined : args[i + 1]; };
const packageDir = resolve(value("--package-root") ?? "");
const nativeDir = resolve(value("--native-package") ?? "");
const dependencyDir = value("--dependencies-root") ? resolve(value("--dependencies-root")) : undefined;
const archive = value("--archive");
const receiptPath = resolve(value("--output") ?? "");
if (!value("--package-root") || !value("--native-package") || !value("--output") || args.includes("--help")) {
  throw new Error("usage: qualify-typescript-native-wasm.mjs --package-root DIR --native-package DIR --output FILE [--dependencies-root DIR] [--archive FILE]");
}
const readJson = (path) => JSON.parse(readFileSync(path, "utf8"));
const hash = (path) => `sha256:${createHash("sha256").update(readFileSync(path)).digest("hex")}`;
const requireInput = (condition, message) => { if (!condition) throw new Error(message); };
const packageManifestPath = join(packageDir, "package.json");
const provenancePath = join(packageDir, "generated", "rust-provenance.json");
const nativeManifestPath = join(nativeDir, "package.json");
const nativeBuildPath = join(nativeDir, "BUILD.json");
for (const path of [packageManifestPath, provenancePath, nativeManifestPath, nativeBuildPath]) requireInput(existsSync(path), `missing qualification input: ${path}`);
const manifest = readJson(packageManifestPath);
const provenance = readJson(provenancePath);
const generated = provenance.acyclicGenerated ?? provenance;
const nativeManifest = readJson(nativeManifestPath);
const nativeBuild = readJson(nativeBuildPath);
requireInput(manifest.name === "@acyclic-labs/stream", `unexpected package: ${manifest.name}`);
requireInput(generated?.family === "stream", "Rust provenance is not for Stream");
requireInput(nativeBuild.schema === "acyclic.sdk.stream.native.build.v1", "native BUILD.json schema mismatch");
const source = {
  git: generated.sourceGitSha,
  model: generated.sourceModelRevision,
  content: generated.sourceContentSha256?.startsWith("sha256:") ? generated.sourceContentSha256 : `sha256:${generated.sourceContentSha256}`,
};
const native = {
  git: nativeBuild.source_revision,
  model: nativeBuild.source_model_revision ?? null,
  content: nativeBuild.source_content_sha256 ?? null,
};
const identityFailure = (message) => {
  mkdirSync(dirname(receiptPath), { recursive: true });
  writeFileSync(receiptPath, `${JSON.stringify({
    schema: "acyclic.sdk.typescript.native-wasm-qualification.v1",
    status: "failed",
    failure: message,
    package: { root: packageDir, provenance_sha256: hash(provenancePath) },
    source_identity: source,
    native_companion: {
      root: nativeDir,
      build_sha256: hash(nativeBuildPath),
      identity: native,
    },
    identity_match: false,
  }, null, 2)}\n`);
};
requireInput(/^[0-9a-f]{40}$/i.test(source.git ?? ""), "package provenance has no Git OID");
requireInput(/^[0-9a-f]{40}$/i.test(native.git ?? ""), "native BUILD.json has no Git OID");
requireInput(source.model && /^[0-9a-f]{64}$/i.test(source.model), "package provenance has no model identity");
requireInput(source.content && /^sha256:[0-9a-f]{64}$/i.test(source.content), "package provenance has no source-content identity");
if (!native.model || !/^[0-9a-f]{64}$/i.test(native.model)) { identityFailure("native BUILD.json has no model identity"); throw new Error("native BUILD.json has no model identity"); }
if (!native.content || !/^sha256:[0-9a-f]{64}$/i.test(native.content)) { identityFailure("native BUILD.json has no source-content identity"); throw new Error("native BUILD.json has no source-content identity"); }
if (source.git.toLowerCase() !== native.git.toLowerCase()) { identityFailure("native companion Git OID differs from generated package"); throw new Error("native companion Git OID differs from generated package"); }
if (source.model.toLowerCase() !== native.model.toLowerCase()) { identityFailure("native companion model identity differs from generated package"); throw new Error("native companion model identity differs from generated package"); }
if (source.content.toLowerCase() !== native.content.toLowerCase()) { identityFailure("native companion source-content identity differs from generated package"); throw new Error("native companion source-content identity differs from generated package"); }

const wasmDir = join(packageDir, "generated", "wasm");
const wasmModulePath = join(wasmDir, "acyclic_stream_wasm.js");
const wasmBinaryPath = join(wasmDir, "acyclic_stream_wasm_bg.wasm");
requireInput(existsSync(wasmModulePath) && existsSync(wasmBinaryPath), "Rust-generated Stream WASM artifacts are missing");
const wasm = await import(pathToFileURL(wasmModulePath).href);
requireInput(typeof wasm.initSync === "function" && typeof wasm.validatePath === "function" && typeof wasm.validateSequence === "function", "WASM module lacks Rust validators");
wasm.initSync({ module: new Uint8Array(readFileSync(wasmBinaryPath)) });
requireInput(wasm.validatePath("qualification/stream") === "", "WASM accepted path did not pass Rust validation");
requireInput(wasm.validatePath("../escape") === "invalid_path", "WASM rejected path did not use Rust validation");
requireInput(wasm.validateSequence("18446744073709551615") === "", "WASM accepted sequence did not pass Rust validation");

const tempRoot = mkdtempSync(join(tmpdir(), "acyclic-sdk-ts-native-wasm-"));
const consumer = join(tempRoot, "consumer");
const nodeModules = join(consumer, "node_modules");
const install = (name, sourceDir) => {
  const target = join(nodeModules, ...name.split("/"));
  mkdirSync(dirname(target), { recursive: true });
  cpSync(sourceDir, target, { recursive: true });
};
install(manifest.name, packageDir);
install(nativeManifest.name, nativeDir);
for (const name of Object.keys(manifest.dependencies ?? {})) {
  requireInput(dependencyDir, `--dependencies-root is required to resolve ${name}`);
  const sourceDir = join(dependencyDir, "node_modules", ...name.split("/"));
  const target = join(nodeModules, ...name.split("/"));
  requireInput(existsSync(sourceDir), `dependency is absent from --dependencies-root: ${name}`);
  mkdirSync(dirname(target), { recursive: true });
  symlinkSync(sourceDir, target, process.platform === "win32" ? "junction" : "dir");
}
const probePath = join(consumer, "probe.mjs");
writeFileSync(probePath, `import { StreamClient } from "@acyclic-labs/stream";
const client = await StreamClient.fromEnv({ endpoint: "https://127.0.0.1:1", token: "qualification" });
if (client.provider.constructor.name !== "NativeStreamProvider") throw new Error("default native transport did not select NativeStreamProvider");
console.log(JSON.stringify({ provider: client.provider.constructor.name, defaultTransport: "grpc" }));\n`);
const result = spawnSync(process.execPath, [probePath], { cwd: consumer, encoding: "utf8", windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
requireInput(result.status === 0, `installed native Stream consumer failed: ${(result.stderr || result.stdout || "").trim()}`);
const nativeProbe = JSON.parse(result.stdout.trim().split(/\r?\n/).at(-1));
requireInput(nativeProbe.provider === "NativeStreamProvider", "installed consumer did not use the native companion");
const receipt = {
  schema: "acyclic.sdk.typescript.native-wasm-qualification.v1", status: "passed",
  package: { name: manifest.name, version: manifest.version, root: packageDir, archive: archive ? resolve(archive) : null, archive_sha256: archive ? hash(resolve(archive)) : null, provenance_sha256: hash(provenancePath) },
  source_identity: source,
  native_companion: { name: nativeManifest.name, version: nativeManifest.version, root: nativeDir, build_sha256: hash(nativeBuildPath), binary_sha256: nativeBuild.artifacts?.binary_sha256 ?? null },
  identity_match: true,
  checks: { wasm: { status: "passed", module_sha256: hash(wasmModulePath), binary_sha256: hash(wasmBinaryPath) }, installed_native: { status: "passed", provider: nativeProbe.provider, resolver: "node_modules package name" } },
};
mkdirSync(dirname(receiptPath), { recursive: true });
writeFileSync(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`);
console.log(JSON.stringify({ schema: receipt.schema, status: receipt.status, output: receiptPath, sourceGitSha: source.git, sourceModelRevision: source.model }));
if (process.platform !== "win32") rmSync(tempRoot, { recursive: true, force: true });
