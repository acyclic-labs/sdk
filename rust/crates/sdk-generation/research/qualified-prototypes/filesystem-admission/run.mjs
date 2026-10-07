import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import { execFileSync } from "node:child_process";
import { lstat, readFile, readdir, writeFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, isAbsolute, join, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const ADMISSION_ERROR = "expected a finite integer in the u32 range";
const WASM_NUMBER_ERROR = "expected a JavaScript number";
const REQUIRED_SOURCE_ROOTS = [
  "Cargo.toml",
  "Cargo.lock",
  "rust-toolchain.toml",
  "package.json",
  "bun.lock",
  "proto/filesystem",
  "proto/protocol/v1",
  "rust/crates/filesystem",
  "rust/crates/filesystem-napi",
  "rust/crates/filesystem-wasm",
  "rust/crates/native-runtime",
  "rust/crates/objects",
  "rust/crates/stream",
  "rust/crates/sdk-generation/research/qualified-prototypes/filesystem-admission/run.mjs",
  "scripts/build-wasm.mjs",
  "scripts/check-filesystem-napi.mjs",
  "scripts/check-filesystem-package.sh",
  "scripts/check-generated.mjs",
  "scripts/filesystem-descriptor-digest.mjs",
  "scripts/filesystem-napi-types.mjs",
  "scripts/generate-filesystem-defaults.mjs",
  "scripts/generate-filesystem-git-compat-contract.mjs",
  "scripts/generate-filesystem-hosted-contract.mjs",
  "scripts/generated-bindings.mjs",
  "scripts/generate.mjs",
  "scripts/stage-npm-package.sh",
  "scripts/sync-generated.mjs",
  "scripts/validate-npm-package.mjs",
  "typescript/packages/filesystem/src",
  "typescript/packages/filesystem/test",
  "typescript/packages/filesystem/examples",
  "typescript/packages/filesystem/CHANGELOG.md",
  "typescript/packages/filesystem/LICENSE",
  "typescript/packages/filesystem/README.md",
  "typescript/packages/filesystem/package.json",
  "typescript/packages/filesystem/tsconfig.json",
  "typescript/packages/filesystem/tsconfig.type-tests.json",
];
const GENERATOR_PROVENANCE = Object.freeze({
  lockfiles: ["Cargo.lock", "bun.lock"],
  toolchain: ["rust-toolchain.toml"],
  generator_files: [
    "scripts/build-wasm.mjs",
    "scripts/check-filesystem-napi.mjs",
    "scripts/check-generated.mjs",
    "scripts/filesystem-napi-types.mjs",
    "scripts/generated-bindings.mjs",
    "scripts/generate.mjs",
    "scripts/stage-npm-package.sh",
    "scripts/sync-generated.mjs",
    "scripts/validate-npm-package.mjs",
    "typescript/packages/filesystem/package.json",
    "rust/crates/sdk-generation/research/qualified-prototypes/filesystem-admission/run.mjs",
  ],
  path_dependencies: [
    "rust/crates/native-runtime",
    "rust/crates/objects",
    "rust/crates/stream",
  ],
});
const EDGE_VALUES = [
  ["zero", 0],
  ["one", 1],
  ["u32-max", 4_294_967_295],
];
const EDGE_OUTCOMES = new Map([
  ["zero", "downstream:workspace join exceeds its configured bound"],
  ["one", "downstream:workspace is not a fork"],
  ["u32-max", "downstream:workspace is not a fork"],
]);
const REJECT_VALUES = [
  ["negative", -1],
  ["fractional-half", 0.5],
  ["fractional-one-and-half", 1.5],
  ["u32-overflow", 4_294_967_296],
  ["nan", Number.NaN],
  ["infinity", Number.POSITIVE_INFINITY],
  ["negative-infinity", Number.NEGATIVE_INFINITY],
  ["null", null],
  ["string-one", "1"],
  ["true", true],
  ["false", false],
  ["undefined", undefined],
  ["bigint-one", 1n],
  ["boxed-one", new Number(1)],
  ["symbol-one", Symbol("1")],
];

const args = process.argv.slice(2);
const argument = name => {
  const index = args.indexOf(name);
  if (index < 0 || args[index + 1] === undefined) throw new Error(`missing ${name}`);
  return args[index + 1];
};
const optionalArgument = name => {
  const index = args.indexOf(name);
  return index < 0 ? undefined : args[index + 1];
};
const attestationOnly = args.includes("--attestation-only");
const writeSourceManifest = args.includes("--write-source-manifest");
const manifestOnly = attestationOnly || writeSourceManifest;
const packageRoot = manifestOnly ? undefined : resolve(argument("--package-root"));
const nativeBinding = manifestOnly ? undefined : resolve(argument("--native-binding"));
const nativeArchivePath = manifestOnly ? undefined : resolve(argument("--native-archive"));
const requestedSourceCommit = optionalArgument("--source-commit");
const sourceRoot = resolve(argument("--source-root"));
const sourceManifest = resolve(argument("--source-manifest"));
const archivePath = manifestOnly ? undefined : resolve(argument("--archive"));
const receiptPath = manifestOnly ? undefined : resolve(argument("--receipt"));
if (writeSourceManifest && requestedSourceCommit !== undefined) {
  throw new Error("--write-source-manifest derives --source-commit from the source root HEAD");
}
if (!writeSourceManifest && !/^[0-9a-f]{40}$/i.test(requestedSourceCommit ?? "")) {
  throw new Error("--source-commit must be the full 40-character checked-out commit");
}

const git = (...gitArgs) => execFileSync("git", ["-C", sourceRoot, ...gitArgs], { encoding: "utf8" }).trim();
const actualSourceCommit = git("rev-parse", "--verify", "HEAD");
if (!/^[0-9a-f]{40}$/i.test(actualSourceCommit)) {
  throw new Error(`source root does not resolve to a full Git commit: ${actualSourceCommit}`);
}
const actualSourceState = git("status", "--porcelain=v1", "--untracked-files=all").length === 0 ? "clean" : "dirty";
const sourceCommit = requestedSourceCommit ?? actualSourceCommit;
if (sourceCommit !== actualSourceCommit) {
  throw new Error(`--source-commit does not match the source root HEAD: expected ${actualSourceCommit}, got ${sourceCommit}`);
}

const digest = async path => {
  const hash = createHash("sha256");
  hash.update(await readFile(path));
  return hash.digest("hex");
};
const normalizedDigest = value => String(value).replace(/^sha256:/i, "").toLowerCase();
const canonicalSourceDigest = entries => {
  const canonical = entries
    .map(entry => `${entry.path}\0${normalizedDigest(entry.sha256)}\0${entry.bytes}\n`)
    .join("");
  return createHash("sha256").update(canonical).digest("hex");
};
const compareCodepoints = (left, right) => {
  const leftPoints = [...left];
  const rightPoints = [...right];
  const length = Math.min(leftPoints.length, rightPoints.length);
  for (let index = 0; index < length; index += 1) {
    if (leftPoints[index] === rightPoints[index]) continue;
    return leftPoints[index] < rightPoints[index] ? -1 : 1;
  }
  return leftPoints.length - rightPoints.length;
};
const collectSourceSelector = async (sourceRoot, selector, files = []) => {
  const absolute = resolve(sourceRoot, selector);
  const metadata = await lstat(absolute);
  if (metadata.isSymbolicLink()) throw new Error(`required source selector contains a symlink: ${selector}`);
  if (metadata.isFile()) {
    files.push({ path: selector.replaceAll("\\", "/"), absolute, bytes: metadata.size });
    return files;
  }
  if (!metadata.isDirectory()) throw new Error(`required source selector is not a file or directory: ${selector}`);
  const children = (await readdir(absolute, { withFileTypes: true }))
    .sort((left, right) => compareCodepoints(left.name, right.name));
  for (const child of children) {
    const childPath = `${selector.replaceAll("\\", "/")}/${child.name}`;
    if (child.isSymbolicLink()) throw new Error(`required source selector contains a symlink: ${childPath}`);
    await collectSourceSelector(sourceRoot, childPath, files);
  }
  return files;
};
const selectedSourceEntries = async () => {
  const selected = [];
  for (const selector of REQUIRED_SOURCE_ROOTS) await collectSourceSelector(sourceRoot, selector, selected);
  selected.sort((left, right) => compareCodepoints(left.path, right.path));
  return Promise.all(selected.map(async entry => ({
    path: entry.path,
    sha256: await digest(entry.absolute),
    bytes: entry.bytes,
  })));
};
const generatorProvenancePaths = () => [
  ...GENERATOR_PROVENANCE.lockfiles,
  ...GENERATOR_PROVENANCE.toolchain,
  ...GENERATOR_PROVENANCE.generator_files,
];
const validateGeneratorProvenance = (manifest, entries) => {
  if (JSON.stringify(manifest.generator_provenance) !== JSON.stringify(GENERATOR_PROVENANCE)) {
    throw new Error("source manifest generator/lock provenance differs from the Rust qualification selector");
  }
  const paths = new Set(entries.map(entry => entry.path));
  for (const path of generatorProvenancePaths()) {
    if (!paths.has(path)) throw new Error(`source manifest omits generator provenance file: ${path}`);
  }
  for (const root of GENERATOR_PROVENANCE.path_dependencies) {
    if (![...paths].some(path => path === root || path.startsWith(`${root}/`))) {
      throw new Error(`source manifest omits generator provenance root: ${root}`);
    }
  }
};
const writeSourceManifestFile = async () => {
  const entries = await selectedSourceEntries();
  validateGeneratorProvenance({ generator_provenance: GENERATOR_PROVENANCE }, entries);
  const manifest = {
    schema: "acyclic.sdk.source-attestation.v1",
    inventory_complete: true,
    source_state: actualSourceState,
    source_commit: actualSourceCommit,
    required_source_roots: REQUIRED_SOURCE_ROOTS,
    generator_provenance: GENERATOR_PROVENANCE,
    source_digest: `sha256:${canonicalSourceDigest(entries)}`,
    file_count: entries.length,
    files: entries,
  };
  await mkdir(resolve(sourceManifest, ".."), { recursive: true });
  await writeFile(sourceManifest, `${JSON.stringify(manifest, null, 2)}\n`, { flag: "wx" });
  console.log(JSON.stringify({
    path: sourceManifest,
    source_commit: actualSourceCommit,
    source_state: actualSourceState,
    source_digest: manifest.source_digest,
    file_count: entries.length,
  }, null, 2));
};
const sourceAttestation = async () => {
  const liveSourceCommit = git("rev-parse", "--verify", "HEAD");
  const liveSourceState = git("status", "--porcelain=v1", "--untracked-files=all").length === 0 ? "clean" : "dirty";
  const manifestBytes = await readFile(sourceManifest);
  let manifest;
  try {
    manifest = JSON.parse(manifestBytes.toString("utf8"));
  } catch (error) {
    throw new Error(`source manifest is not valid JSON: ${error.message}`);
  }
  if (manifest?.schema !== "acyclic.sdk.source-attestation.v1") {
    throw new Error("source manifest has unsupported schema");
  }
  if (!manifest.inventory_complete || !["clean", "dirty"].includes(manifest.source_state)) {
    throw new Error("source manifest must declare a complete clean or dirty inventory");
  }
  if (manifest.source_commit !== sourceCommit) {
    throw new Error(`source manifest commit mismatch: expected ${sourceCommit}, got ${manifest.source_commit}`);
  }
  if (manifest.source_commit !== liveSourceCommit) {
    throw new Error(`source manifest commit is not the source root HEAD: expected ${liveSourceCommit}, got ${manifest.source_commit}`);
  }
  if (manifest.source_state !== liveSourceState) {
    throw new Error(`source manifest state does not match Git: expected ${liveSourceState}, got ${manifest.source_state}`);
  }
  if (JSON.stringify(manifest.required_source_roots) !== JSON.stringify(REQUIRED_SOURCE_ROOTS)) {
    throw new Error("source manifest required source selector differs from the Rust qualification selector");
  }
  if (!Array.isArray(manifest.files) || manifest.files.length === 0) {
    throw new Error("source manifest has no files");
  }
  const entries = manifest.files.map(entry => {
    if (!entry || typeof entry.path !== "string" || !/^[0-9a-f]{64}$/i.test(normalizedDigest(entry.sha256))) {
      throw new Error("source manifest contains an invalid file entry");
    }
    const path = entry.path.replaceAll("\\", "/");
    const absolute = resolve(sourceRoot, path);
    const escaped = relative(sourceRoot, absolute);
    if (!path || isAbsolute(path) || escaped === ".." || escaped.startsWith("..\\") || escaped.startsWith("../")) {
      throw new Error(`source manifest path escapes its root: ${path}`);
    }
    if (!Number.isSafeInteger(entry.bytes) || entry.bytes < 0) {
      throw new Error(`source manifest has invalid byte count for ${path}`);
    }
    return { path, absolute, bytes: entry.bytes, sha256: normalizedDigest(entry.sha256) };
  }).sort((left, right) => left.path < right.path ? -1 : left.path > right.path ? 1 : 0);
  for (let index = 1; index < entries.length; index += 1) {
    if (entries[index - 1].path === entries[index].path) {
      throw new Error(`source manifest contains a duplicate path: ${entries[index].path}`);
    }
  }
  for (const entry of entries) {
    const actual = await digest(entry.absolute);
    if (actual !== entry.sha256) {
      throw new Error(`source manifest hash mismatch for ${entry.path}: expected ${actual}, got ${entry.sha256}`);
    }
    const actualBytes = (await lstat(entry.absolute)).size;
    if (actualBytes !== entry.bytes) {
      throw new Error(`source manifest byte count mismatch for ${entry.path}: expected ${actualBytes}, got ${entry.bytes}`);
    }
  }
  if (manifest.file_count !== entries.length) {
    throw new Error(`source manifest file count mismatch: expected ${entries.length}, got ${manifest.file_count}`);
  }
  validateGeneratorProvenance(manifest, entries);
  const selected = [];
  for (const selector of REQUIRED_SOURCE_ROOTS) await collectSourceSelector(sourceRoot, selector, selected);
  selected.sort((left, right) => compareCodepoints(left.path, right.path));
  const selectedPaths = selected.map(entry => entry.path);
  const manifestPaths = entries.map(entry => entry.path);
  if (JSON.stringify(selectedPaths) !== JSON.stringify(manifestPaths)) {
    const missing = selectedPaths.filter(path => !manifestPaths.includes(path));
    const extra = manifestPaths.filter(path => !selectedPaths.includes(path));
    throw new Error(`source manifest does not match required source selector; missing=${missing.join(",")}; extra=${extra.join(",")}`);
  }
  const sourceDigest = canonicalSourceDigest(entries);
  if (normalizedDigest(manifest.source_digest) !== sourceDigest) {
    throw new Error(`source manifest digest mismatch: expected ${sourceDigest}, got ${manifest.source_digest}`);
  }
  return {
    path: basename(sourceManifest),
    sha256: await digest(sourceManifest),
    source_commit: manifest.source_commit,
    source_state: manifest.source_state,
    source_digest: `sha256:${sourceDigest}`,
    file_count: entries.length,
  };
};
if (writeSourceManifest) {
  await writeSourceManifestFile();
  process.exit(0);
}
if (attestationOnly) {
  console.log(JSON.stringify(await sourceAttestation(), null, 2));
  process.exit(0);
}
const describe = value => {
  if (typeof value === "symbol") return "Symbol(1)";
  if (typeof value === "bigint") return `${value}n`;
  if (value instanceof Number) return "new Number(1)";
  if (value === undefined) return "undefined";
  if (Number.isNaN(value)) return "NaN";
  if (value === Number.POSITIVE_INFINITY) return "Infinity";
  if (value === Number.NEGATIVE_INFINITY) return "-Infinity";
  return JSON.stringify(value);
};

const classify = (error, runtime) => {
  const message = String(error?.message ?? error);
  if (message === ADMISSION_ERROR) return "boundary_rejected";
  if (runtime === "wasm" && message === WASM_NUMBER_ERROR) return "boundary_rejected";
  // Native conversion failures remain downstream errors unless the binding
  // returns the canonical Rust admission string above. This deliberately
  // avoids inferring a boundary rejection from the JavaScript input type: a
  // coercing N-API decoder must fail the matrix rather than be misreported as
  // a successful typed boundary.
  return `downstream:${message}`;
};
const values = [...EDGE_VALUES, ...REJECT_VALUES];

async function qualifyWasm() {
  const jsPath = join(packageRoot, "generated", "wasm", "acyclic_fs_wasm.js");
  const wasmPath = join(packageRoot, "generated", "wasm", "acyclic_fs_wasm_bg.wasm");
  const module = await import(pathToFileURL(jsPath).href);
  await module.default({ module_or_path: await readFile(wasmPath) });
  const result = [];
  for (const [label, value] of values) {
    const fs = module.openMemoryFs({
      maximumObjectBytes: 1024 * 1024,
      maximumMemoryBytes: 64 * 1024 * 1024,
      objectCache: { maximumEntries: 8, maximumBytes: 1024, maximumInFlight: 2, maximumWaitersPerObject: 2 },
    });
    let outcome;
    try {
      const workspace = await fs.createWorkspace(`admission-${label}`);
      try {
        await workspace.liveRebase(null, value, 1, 1);
        outcome = "accepted";
      } catch (error) {
        outcome = classify(error, "wasm");
      }
    } finally {
      fs.close?.();
    }
    result.push({ label, value: describe(value), outcome });
  }
  return {
    runtime: "wasm",
    artifacts: {
      js: { path: "generated/wasm/acyclic_fs_wasm.js", sha256: await digest(jsPath) },
      wasm: { path: "generated/wasm/acyclic_fs_wasm_bg.wasm", sha256: await digest(wasmPath) },
    },
    results: result,
  };
}

async function qualifyNative() {
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-fs-admission-"));
  try {
    const binding = createRequire(import.meta.url)(nativeBinding);
    const fs = await binding.NativeFs.open(join(temporary, "engine"), {
      maximumEntries: 64,
      maximumBytes: 1024n * 1024n,
      maximumInFlight: 2,
      maximumWaitersPerObject: 2,
    });
    const result = [];
    try {
      for (const [label, value] of values) {
        const workspace = await fs.createWorkspace(`admission-${label}`);
        let outcome;
        try {
          await workspace.liveRebase(null, value, 1, 1);
          outcome = "accepted";
        } catch (error) {
          outcome = classify(error, "napi");
        }
        result.push({ label, value: describe(value), outcome });
      }
    } finally {
      fs.cancel();
    }
    return {
      runtime: "napi",
      artifacts: { binding: { path: basename(nativeBinding), sha256: await digest(nativeBinding) } },
      results: result,
    };
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
}

const qualificationArtifactPaths = [
  { label: "package_archive", path: archivePath },
  { label: "native_archive", path: nativeArchivePath },
  { label: "wasm_js", path: join(packageRoot, "generated", "wasm", "acyclic_fs_wasm.js") },
  { label: "wasm_binary", path: join(packageRoot, "generated", "wasm", "acyclic_fs_wasm_bg.wasm") },
  { label: "native_binding", path: nativeBinding },
];
const captureQualificationArtifacts = async () => Promise.all(
  qualificationArtifactPaths.map(async artifact => ({
    label: artifact.label,
    path: artifact.path,
    sha256: await digest(artifact.path),
    bytes: (await lstat(artifact.path)).size,
  })),
);
const archiveEntries = archivePath => {
  const listing = execFileSync("tar", ["-tzf", archivePath], { encoding: "utf8" })
    .split(/\r?\n/)
    .map(entry => entry.trim())
    .filter(Boolean);
  const entries = listing.filter(entry => !entry.endsWith("/"));
  const seen = new Set();
  for (const entry of entries) {
    const normalized = entry.replaceAll("\\", "/");
    if (
      normalized.startsWith("/") ||
      /^[A-Za-z]:\//.test(normalized) ||
      normalized.split("/").includes("..") ||
      seen.has(normalized)
    ) {
      throw new Error(`archive has an unsafe or duplicate entry: ${entry}`);
    }
    seen.add(normalized);
  }
  return entries;
};
const extractArchiveEntry = (archivePath, archiveEntry) => execFileSync("tar", ["-xOf", archivePath, archiveEntry], {
  maxBuffer: 512 * 1024 * 1024,
});
const archiveEntryForPath = (entries, expectedPath) => {
  const normalizedPath = expectedPath.replaceAll("\\", "/");
  const matches = entries.filter(entry => {
    const normalized = entry.replaceAll("\\", "/");
    return normalized === normalizedPath || normalized === `package/${normalizedPath}` || normalized.endsWith(`/${normalizedPath}`);
  });
  if (matches.length !== 1) {
    throw new Error(`archive must contain exactly one ${expectedPath}, found ${matches.length}`);
  }
  return matches[0];
};
const validateArchiveFile = async (archivePath, expectedPath, installedPath, label) => {
  const entries = archiveEntries(archivePath);
  const archiveEntry = archiveEntryForPath(entries, expectedPath);
  const archivedBytes = extractArchiveEntry(archivePath, archiveEntry);
  const archivedDigest = createHash("sha256").update(archivedBytes).digest("hex");
  const installedDigest = await digest(installedPath);
  const installedBytes = (await lstat(installedPath)).size;
  if (archivedDigest !== installedDigest || archivedBytes.length !== installedBytes) {
    throw new Error(`${label} differs from archive entry: ${archiveEntry}`);
  }
  return { archive_entry: archiveEntry, sha256: archivedDigest, bytes: archivedBytes.length };
};
const validateNativeArchiveBinding = async () => {
  const entries = archiveEntries(nativeArchivePath);
  const bindingName = basename(nativeBinding);
  const matches = entries.filter(entry => basename(entry.replaceAll("\\", "/")) === bindingName);
  if (matches.length !== 1) {
    throw new Error(`native archive must contain exactly one ${bindingName}, found ${matches.length}`);
  }
  const archiveEntry = matches[0];
  const archivedBinding = extractArchiveEntry(nativeArchivePath, archiveEntry);
  const archivedDigest = createHash("sha256").update(archivedBinding).digest("hex");
  const installedDigest = await digest(nativeBinding);
  if (archivedDigest !== installedDigest) {
    throw new Error(`installed native binding differs from native archive entry: ${archiveEntry}`);
  }
  return { archive_entry: archiveEntry, sha256: archivedDigest, bytes: archivedBinding.length };
};
const assertStableQualificationArtifacts = (before, after) => {
  for (let index = 0; index < before.length; index += 1) {
    const expected = before[index];
    const actual = after[index];
    if (expected.sha256 !== actual.sha256 || expected.bytes !== actual.bytes) {
      throw new Error(`qualification artifact changed during execution: ${expected.label}`);
    }
  }
};
const sourceBefore = await sourceAttestation();
const artifactsBefore = await captureQualificationArtifacts();
const packageArchiveFiles = {
  wasm_js: await validateArchiveFile(
    archivePath,
    "generated/wasm/acyclic_fs_wasm.js",
    join(packageRoot, "generated", "wasm", "acyclic_fs_wasm.js"),
    "package WASM JavaScript",
  ),
  wasm_binary: await validateArchiveFile(
    archivePath,
    "generated/wasm/acyclic_fs_wasm_bg.wasm",
    join(packageRoot, "generated", "wasm", "acyclic_fs_wasm_bg.wasm"),
    "package WASM binary",
  ),
};
const nativeArchiveBinding = await validateNativeArchiveBinding();
const runtimes = [await qualifyWasm(), await qualifyNative()];
const artifactsAfter = await captureQualificationArtifacts();
assertStableQualificationArtifacts(artifactsBefore, artifactsAfter);
const failures = [];
for (const runtime of runtimes) {
  for (const row of runtime.results) {
    const isReject = row.label !== "zero" && row.label !== "one" && row.label !== "u32-max";
    if (isReject && row.outcome !== "boundary_rejected") {
      failures.push(`${runtime.runtime} accepted ${row.value}: ${row.outcome}`);
    }
    if (!isReject && row.outcome !== EDGE_OUTCOMES.get(row.label)) {
      failures.push(`${runtime.runtime} returned unexpected result for valid edge ${row.value}: ${row.outcome}`);
    }
  }
}
if (failures.length > 0) {
  throw new Error(`filesystem admission failed:\n${failures.join("\n")}\nfull matrix: ${JSON.stringify(runtimes)}`);
}
const matrix = JSON.stringify(runtimes);
const attestation = await sourceAttestation();
if (
  attestation.source_digest !== sourceBefore.source_digest ||
  attestation.source_commit !== sourceCommit ||
  attestation.source_state !== sourceBefore.source_state ||
  attestation.file_count !== sourceBefore.file_count
) {
  throw new Error("source checkout changed during qualification");
}
const receipt = {
  schema: 1,
  source_commit: sourceCommit,
  source_attestation: attestation,
  package_archive: {
    path: basename(archivePath),
    sha256: artifactsBefore.find(artifact => artifact.label === "package_archive").sha256,
    files: packageArchiveFiles,
  },
  native_archive: {
    path: basename(nativeArchivePath),
    sha256: artifactsBefore.find(artifact => artifact.label === "native_archive").sha256,
    binding: nativeArchiveBinding,
  },
  matrix_sha256: createHash("sha256").update(matrix).digest("hex"),
  runtimes,
};
await writeFile(receiptPath, `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
console.log(JSON.stringify(receipt, null, 2));

// qualification mutation regression

// qualification mutation regression
