import { createHash, randomUUID } from "node:crypto";
import { execFileSync } from "node:child_process";
import { cp, lstat, mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const manifestRelative = "rust/crates/actors-napi/Cargo.toml";
const packageRelative = "typescript/packages/actors/package.json";
const defaultOutput = resolve(root, "typescript/packages/actors/generated/native");
const nativeTargetsSchema = "acyclic.actors.native-targets.v1";
const generationSchema = "acyclic.actors.native-generation.v1";
const generationManifestName = "generation-manifest.json";
const sourceRoots = [
  "Cargo.toml",
  "Cargo.lock",
  "rust-toolchain.toml",
  ".cargo/config.toml",
  // acyclic-actors links this crate under non-WASM targets. Keep its source
  // in the attestation so a native build cannot silently use another tree.
  "rust/crates/native-runtime",
  "rust/crates/actors",
  "rust/crates/actors-napi",
  "rust/crates/actors-wasm",
  "rust/vendor/protify-proc-macro-0.1.4",
  "package.json",
  "bun.lock",
  "typescript/packages/actors/package.json",
  "typescript/packages/actors/tsconfig.json",
  "tsconfig.base.json",
  "typescript/packages/actors/src",
  "typescript/packages/actors/generated/proto",
  "scripts/build-actors-native.mjs",
  "scripts/assemble-actors-native-package.mjs",
  "scripts/build-wasm.mjs",
  // The packaged semantic bindings and Proto contract are rendered from Rust.
  // Attest their production entrypoint and Buf inputs as well as the crate.
  "scripts/generate-actors.mjs",
  "scripts/generate.mjs",
  "scripts/sync-generated.mjs",
  "scripts/generated-bindings.mjs",
  "rust/crates/proto-codegen",
  "proto/actors/v1/actors.proto",
  "compatibility/descriptors/actors-v1.bin",
  "buf.yaml",
  "buf.lock",
  "buf.gen.yaml",
];

function usage() {
  return `usage:
  node scripts/build-actors-native.mjs build --target <rust-triple> [--output <native-bundle>] [--target-dir <cargo-target-dir>]
  node scripts/build-actors-native.mjs stage --bundle <native-bundle> [--output <package-native-dir>]
  node scripts/build-actors-native.mjs check [--output <package-native-dir>]

build requires an explicit Rust target and a clean source closure. Release and
manual matrix jobs build into a bundle, then stage copies that attested bundle
into the package. check is cheap and never invokes Cargo or NAPI-RS.`;
}

function parseArgs(argv) {
  const command = argv[0]?.startsWith("-") ? "build" : (argv[0] ?? "check");
  const rest = argv[0]?.startsWith("-") ? argv : argv.slice(1);
  const options = { command };
  for (let index = 0; index < rest.length; index += 1) {
    const arg = rest[index];
    if (arg === "--target") options.target = rest[++index];
    else if (arg === "--output") options.output = rest[++index];
    else if (arg === "--bundle") options.bundle = rest[++index];
    else if (arg === "--target-dir") options.targetDir = rest[++index];
    else if (arg === "--help" || arg === "-h") options.help = true;
    else throw new Error(`unknown option ${arg}\n\n${usage()}`);
  }
  if (options.target === undefined && rest.includes("--target")) throw new Error("--target requires a Rust target triple");
  if (options.output === undefined && rest.includes("--output")) throw new Error("--output requires a directory");
  if (options.bundle === undefined && rest.includes("--bundle")) throw new Error("--bundle requires a directory");
  if (options.targetDir === undefined && rest.includes("--target-dir")) throw new Error("--target-dir requires a directory");
  return options;
}

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function canonicalSha256(value) {
  if (typeof value !== "string") return undefined;
  if (/^[0-9a-f]{64}$/.test(value)) return `sha256:${value}`;
  if (/^sha256:[0-9a-f]{64}$/.test(value)) return value;
  return undefined;
}

function compareCodepoints(left, right) {
  const leftPoints = [...left];
  const rightPoints = [...right];
  const length = Math.min(leftPoints.length, rightPoints.length);
  for (let index = 0; index < length; index += 1) {
    if (leftPoints[index] === rightPoints[index]) continue;
    return leftPoints[index] < rightPoints[index] ? -1 : 1;
  }
  return leftPoints.length - rightPoints.length;
}

function treeDigest(files) {
  const encoded = files.map(file => `${file.path}\0${file.sha256}\0${file.bytes}\n`).join("");
  return digest(Buffer.from(encoded));
}

async function collectFiles(relativePath, files) {
  const absolute = resolve(root, relativePath);
  const metadata = await lstat(absolute);
  if (metadata.isSymbolicLink()) throw new Error(`source closure contains a symlink or reparse point: ${relativePath}`);
  if (metadata.isFile()) {
    const bytes = await readFile(absolute);
    files.push({ path: relativePath.replaceAll(sep, "/"), sha256: digest(bytes), bytes: bytes.length });
    return;
  }
  if (!metadata.isDirectory()) throw new Error(`source closure entry is not a file or directory: ${relativePath}`);
  const entries = (await readdir(absolute, { withFileTypes: true })).sort((left, right) => compareCodepoints(left.name, right.name));
  for (const entry of entries) {
    if (entry.isSymbolicLink()) throw new Error(`source closure contains a symlink or reparse point: ${relativePath}/${entry.name}`);
    await collectFiles(`${relativePath}/${entry.name}`, files);
  }
}

export async function sourceSnapshot() {
  const files = [];
  for (const sourceRoot of sourceRoots) await collectFiles(sourceRoot, files);
  files.sort((left, right) => compareCodepoints(left.path, right.path));
  return { files, sha256: treeDigest(files) };
}

export async function assertSourceSnapshot(expected) {
  const current = await sourceSnapshot();
  if (current.sha256 !== expected.sha256 || JSON.stringify(current.files) !== JSON.stringify(expected.files)) {
    throw new Error("Actors native source closure changed after qualification");
  }
  return current;
}

function sourceRevision() {
  const revision = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  if (!/^[0-9a-f]{40,64}$/.test(revision)) throw new Error("git source revision is unavailable for native artifact provenance");
  return revision;
}

function assertCleanSource() {
  const status = execFileSync("git", ["status", "--porcelain=v1", "--untracked-files=all", "--", ...sourceRoots], { cwd: root, encoding: "utf8" });
  if (status.trim() !== "") throw new Error("Actors native build requires a clean source closure; commit or stage source changes before building");
}

function rustMetadata() {
  const metadata = JSON.parse(execFileSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"], { cwd: root, encoding: "utf8" }));
  const rustPackage = metadata.packages.find(item => item.name === "acyclic-actors-napi");
  const targets = rustPackage?.metadata?.napi?.targets;
  if (!rustPackage || !Array.isArray(targets) || targets.length === 0 || targets.some(target => typeof target !== "string" || target.length === 0) || new Set(targets).size !== targets.length) {
    throw new Error(`${manifestRelative} must declare unique package.metadata.napi.targets in Rust`);
  }
  return { rustPackage, targets };
}

async function packageJson() {
  return JSON.parse(await readFile(resolve(root, packageRelative), "utf8"));
}

function assertVersion(rustPackage, packageManifest) {
  if (rustPackage.version !== packageManifest.version) throw new Error(`acyclic-actors-napi ${rustPackage.version} does not match ${packageRelative} ${packageManifest.version}`);
}

function relativeArtifactPath(name) {
  return `generated/native/${name}`;
}

export async function assertExactInventory(output, allowed) {
  const entries = await readdir(output, { withFileTypes: true });
  const extras = entries.map(entry => entry.name).filter(name => !allowed.has(name));
  if (extras.length > 0) throw new Error(`native bundle ${output} contains unstated files: ${extras.join(", ")}`);
}

async function bundleArtifacts(output) {
  const names = (await readdir(output)).filter(name => name === "binding.cjs" || name === "binding.d.ts" || name.endsWith(".node")).sort();
  await assertExactInventory(output, new Set([...names, generationManifestName, "native-targets.json"]));
  if (!names.includes("binding.cjs") || !names.includes("binding.d.ts")) throw new Error(`native bundle ${output} is missing generated binding loader or declarations`);
  const nodes = names.filter(name => name.endsWith(".node"));
  if (nodes.length !== 1) throw new Error(`native bundle ${output} must contain exactly one .node artifact; found ${nodes.length}`);
  const artifacts = [];
  for (const name of names) {
    const bytes = await readFile(resolve(output, name));
    artifacts.push({ path: relativeArtifactPath(name), sha256: digest(bytes), bytes: bytes.length });
  }
  return { artifacts, node: artifacts.find(item => item.path.endsWith(".node")) };
}

function pathFromArtifact(output, artifactPath) {
  const prefix = "generated/native/";
  if (typeof artifactPath !== "string" || !artifactPath.startsWith(prefix) || artifactPath.includes("..")) throw new Error(`invalid native bundle artifact path ${JSON.stringify(artifactPath)}`);
  const candidate = resolve(output, artifactPath.slice(prefix.length));
  if (!candidate.startsWith(`${resolve(output)}${sep}`)) throw new Error(`native bundle artifact escapes ${output}`);
  return candidate;
}

/** @param {{ expectedTarget?: string }} [options] */
async function assertBundle(output, options = {}) {
  const { expectedTarget } = options;
  const metadataPath = resolve(output, "native-targets.json");
  const generationPath = resolve(output, generationManifestName);
  const metadataBytes = await readFile(metadataPath);
  const generationBytes = await readFile(generationPath);
  const metadata = JSON.parse(metadataBytes.toString("utf8"));
  const generation = JSON.parse(generationBytes.toString("utf8"));
  const packageManifest = await packageJson();
  const { rustPackage, targets } = rustMetadata();
  assertVersion(rustPackage, packageManifest);
  if (metadata.schema !== nativeTargetsSchema || generation.schema !== generationSchema) throw new Error(`native bundle ${output} has unsupported provenance schema`);
  if (metadata.package !== rustPackage.name || generation.package !== rustPackage.name) throw new Error(`native bundle ${output} names the wrong Rust package`);
  if (metadata.version !== packageManifest.version || generation.version !== packageManifest.version) throw new Error(`native bundle ${output} version does not match ${packageRelative}`);
  if (metadata.source_path !== manifestRelative || generation.source_path !== manifestRelative) throw new Error(`native bundle ${output} has the wrong Rust source path`);
  if (canonicalSha256(metadata.generation_sha256) !== digest(generationBytes)) throw new Error(`native bundle ${output} generation manifest digest differs`);
  if (metadata.generation_manifest !== `generated/native/${generationManifestName}`) throw new Error(`native bundle ${output} has the wrong generation manifest path`);
  if (JSON.stringify(metadata.targets) !== JSON.stringify(targets) || JSON.stringify(generation.targets) !== JSON.stringify(targets)) throw new Error(`native bundle ${output} target metadata differs from Rust`);
  if (expectedTarget !== undefined && metadata.selected_target !== expectedTarget) throw new Error(`native bundle selected target ${metadata.selected_target} differs from ${expectedTarget}`);
  if (!targets.includes(metadata.selected_target) || generation.selected_target !== metadata.selected_target) throw new Error("native bundle selected target is not Rust-qualified");
  const revision = sourceRevision();
  const current = await sourceSnapshot();
  if (metadata.source_revision !== revision || generation.revision !== revision) throw new Error("native bundle source revision differs from current checkout");
  if (metadata.source_sha256 !== current.sha256 || generation.source_sha256 !== current.sha256) throw new Error("native bundle source closure digest differs from current checkout");
  if (JSON.stringify(metadata.source_files) !== JSON.stringify(current.files) || JSON.stringify(generation.source_files) !== JSON.stringify(current.files)) throw new Error("native bundle source file attestation differs from current checkout");
  const artifacts = generation.artifacts;
  if (!Array.isArray(artifacts) || !Array.isArray(metadata.artifacts) || JSON.stringify(metadata.artifacts) !== JSON.stringify(artifacts) || metadata.artifact === undefined) throw new Error("native bundle artifact attestation is invalid");
  if (metadata.artifact.sha256 !== artifacts.find(item => item.path === metadata.artifact.path)?.sha256) throw new Error("native bundle selected artifact digest differs");
  for (const artifact of artifacts) {
    const bytes = await readFile(pathFromArtifact(output, artifact.path));
    if (artifact.sha256 !== digest(bytes) || artifact.bytes !== bytes.length) throw new Error(`native bundle artifact differs: ${artifact.path}`);
  }
  const bundle = await bundleArtifacts(output);
  if (JSON.stringify(bundle.artifacts) !== JSON.stringify(artifacts)) throw new Error("native bundle contains unstated or missing generated files");
  return { metadata, generation, artifacts };
}

async function build(options) {
  if (options.target === undefined) throw new Error(`build requires --target <rust-triple>\n\n${usage()}`);
  assertCleanSource();
  const output = resolve(options.output ?? defaultOutput);
  const packageManifest = await packageJson();
  const { rustPackage, targets } = rustMetadata();
  assertVersion(rustPackage, packageManifest);
  if (!targets.includes(options.target)) throw new Error(`unsupported Actors N-API target ${JSON.stringify(options.target)}; expected one of ${targets.join(", ")}`);
  const revision = sourceRevision();
  const source = await sourceSnapshot();
  await mkdir(output, { recursive: true });
  const temporary = await mkdtemp(resolve(tmpdir(), "acyclic-actors-napi-package-"));
  const packagePath = resolve(temporary, `${randomUUID()}.json`);
  await writeFile(packagePath, JSON.stringify({ ...packageManifest, napi: { ...packageManifest.napi, targets } }));
  try {
    // Remove only the two provenance files this command owns. Any other
    // pre-existing entry is rejected by bundleArtifacts rather than hidden.
    await rm(resolve(output, generationManifestName), { force: true });
    await rm(resolve(output, "native-targets.json"), { force: true });
    // Staging and checking a previously qualified bundle must work from the
    // clean publication assembly directory, which has no workspace dev
    // dependencies. Load NAPI-RS only for the build command.
    const { NapiCli } = await import("@napi-rs/cli");
    const buildResult = await new NapiCli().build({
      cwd: root,
      packageJsonPath: packagePath,
      manifestPath: resolve(root, manifestRelative),
      outputDir: output,
      target: options.target,
      targetDir: resolve(options.targetDir ?? resolve(root, "target")),
      platform: true,
      jsPackageName: packageManifest.name,
      jsBinding: "binding.cjs",
      dts: "binding.d.ts",
      release: true,
      cargoOptions: ["--locked"],
    });
    await buildResult.task;
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
  await assertSourceSnapshot(source);
  if (sourceRevision() !== revision) throw new Error("Actors native source changed during native build");
  const bundle = await bundleArtifacts(output);
  const generation = {
    schema: generationSchema,
    package: rustPackage.name,
    version: packageManifest.version,
    source_path: manifestRelative,
    revision,
    source_sha256: source.sha256,
    source_files: source.files,
    targets,
    selected_target: options.target,
    artifacts: bundle.artifacts,
  };
  const generationBytes = Buffer.from(`${JSON.stringify(generation, null, 2)}\n`);
  await writeFile(resolve(output, generationManifestName), generationBytes);
  const metadata = {
    schema: nativeTargetsSchema,
    package: rustPackage.name,
    version: packageManifest.version,
    source_path: manifestRelative,
    source_revision: revision,
    source_sha256: source.sha256,
    source_files: source.files,
    targets,
    selected_target: options.target,
    generation_manifest: `generated/native/${generationManifestName}`,
    generation_sha256: digest(generationBytes),
    artifacts: bundle.artifacts,
    artifact: bundle.node,
  };
  await writeFile(resolve(output, "native-targets.json"), `${JSON.stringify(metadata, null, 2)}\n`);
  await assertBundle(output, { expectedTarget: options.target });
}

async function stage(options) {
  if (options.bundle === undefined) throw new Error(`stage requires --bundle <native-bundle>\n\n${usage()}`);
  const bundle = resolve(options.bundle);
  if (!(await stat(bundle)).isDirectory()) throw new Error(`native bundle is not a directory: ${bundle}`);
  let input = bundle;
  try { await stat(resolve(input, "native-targets.json")); }
  catch { input = resolve(bundle, "generated/native"); }
  await assertBundle(input);
  const output = resolve(options.output ?? defaultOutput);
  await mkdir(output, { recursive: true });
  const metadata = JSON.parse((await readFile(resolve(input, "native-targets.json"))).toString("utf8"));
  const expectedNames = new Set([
    ...metadata.artifacts.map(artifact => artifact.path.slice("generated/native/".length)),
    generationManifestName,
    "native-targets.json",
  ]);
  await assertExactInventory(output, expectedNames);
  for (const artifact of metadata.artifacts) await cp(pathFromArtifact(input, artifact.path), resolve(output, artifact.path.slice("generated/native/".length)));
  await cp(resolve(input, generationManifestName), resolve(output, generationManifestName));
  await cp(resolve(input, "native-targets.json"), resolve(output, "native-targets.json"));
  await assertBundle(output, { expectedTarget: metadata.selected_target });
}

async function check(options) {
  const output = resolve(options.output ?? defaultOutput);
  await assertBundle(output);
  console.log(JSON.stringify({ schema: "acyclic.actors.native-check.v1", status: "passed", output }));
}

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) console.log(usage());
  else if (options.command === "build") await build(options);
  else if (options.command === "stage") await stage(options);
  else if (options.command === "check") await check(options);
  else throw new Error(`unknown command ${options.command}\n\n${usage()}`);
}
