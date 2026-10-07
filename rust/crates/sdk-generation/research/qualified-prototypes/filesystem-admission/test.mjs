import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import {
  appendFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SCRIPT = join(HERE, "run.mjs");
const ROOT = execFileSync("git", ["-C", HERE, "rev-parse", "--show-toplevel"], { encoding: "utf8" }).trim();
const COMMIT = execFileSync("git", ["-C", ROOT, "rev-parse", "--verify", "HEAD"], { encoding: "utf8" }).trim();

const run = (arguments_, options = {}) => new Promise((resolveResult, reject) => {
  const child = spawn(process.execPath, [SCRIPT, ...arguments_], {
    cwd: ROOT,
    ...options,
  });
  let stdout = "";
  let stderr = "";
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", chunk => { stdout += chunk; });
  child.stderr.on("data", chunk => { stderr += chunk; });
  child.once("error", reject);
  child.once("close", status => resolveResult({ status, stdout, stderr, output: `${stdout}${stderr}` }));
});

const writeManifest = async (manifestPath) => {
  const result = await run([
    "--write-source-manifest",
    "--source-root", ROOT,
    "--source-manifest", manifestPath,
  ]);
  assert.equal(result.status, 0, result.output);
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  assert.equal(manifest.file_count, 372, "the authoritative selector must cover the complete 372-file closure");
  return manifest;
};

const attestationArgs = (manifestPath, commit = COMMIT) => [
  "--attestation-only",
  "--source-root", ROOT,
  "--source-manifest", manifestPath,
  "--source-commit", commit,
];

const withManifest = async (manifestPath, mutate, callback) => {
  const original = await readFile(manifestPath);
  try {
    const manifest = JSON.parse(original.toString("utf8"));
    mutate(manifest);
    await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
    return await callback();
  } finally {
    await writeFile(manifestPath, original);
  }
};

const makeFixture = async ({ mode = "normal", archiveJs = undefined } = {}) => {
  const fixture = await mkdtemp(join(tmpdir(), "acyclic-fs-admission-test-"));
  const packageRoot = join(fixture, "package");
  const wasmRoot = join(packageRoot, "generated", "wasm");
  const nativeRoot = join(fixture, "native");
  const archiveRoot = join(fixture, "archive", "package");
  await mkdir(wasmRoot, { recursive: true });
  await mkdir(nativeRoot, { recursive: true });
  await mkdir(join(archiveRoot, "generated", "wasm"), { recursive: true });
  await writeFile(join(packageRoot, "package.json"), '{"type":"module"}\n');

  const mutationTarget = JSON.stringify(mode === "mutate-source" ? SCRIPT : join(wasmRoot, "acyclic_fs_wasm_bg.wasm"));
  const wasmSource = `
import { appendFileSync } from "node:fs";
const mutationTarget = ${mutationTarget};
let mutated = false;
const admission = "expected a finite integer in the u32 range";
export default async function init() {}
export function openMemoryFs() {
  return {
    async createWorkspace() {
      return { async liveRebase(_base, value) {
        if ((${JSON.stringify(mode)} === "mutate-artifact" || ${JSON.stringify(mode)} === "mutate-source") && !mutated) {
          appendFileSync(mutationTarget, Buffer.from("mutation"));
          mutated = true;
        }
        const invalid = typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value) || value < 0 || value > 4294967295;
        if (invalid) {
          if (${JSON.stringify(mode)} === "unknown" && value === null) throw new Error("unknown: " + admission);
          throw new Error(admission);
        }
        throw new Error(value === 0 ? "workspace join exceeds its configured bound" : "workspace is not a fork");
      } };
    },
    close() {},
  };
}
`;
  const wasmJs = join(wasmRoot, "acyclic_fs_wasm.js");
  const wasmBinary = join(wasmRoot, "acyclic_fs_wasm_bg.wasm");
  await writeFile(wasmJs, wasmSource);
  await writeFile(wasmBinary, "wasm-fixture");

  const nativeBinding = join(nativeRoot, "acyclic-fs-test.js");
  const nativeSource = `
const admission = "expected a finite integer in the u32 range";
const mode = ${JSON.stringify(mode)};
function workspace() {
  return { async liveRebase(_base, value) {
    const invalid = typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value) || value < 0 || value > 4294967295;
    if (invalid) {
      if (mode === "unknown" && value === null) throw new Error("unknown: " + admission);
      throw new Error(admission);
    }
    throw new Error(value === 0 ? "workspace join exceeds its configured bound" : "workspace is not a fork");
  } };
}
module.exports = { NativeFs: { async open() { return { async createWorkspace() { return workspace(); }, cancel() {} }; } } };
`;
  await writeFile(nativeBinding, nativeSource);
  await writeFile(join(archiveRoot, "generated", "wasm", "acyclic_fs_wasm.js"), archiveJs ?? wasmSource);
  await writeFile(join(archiveRoot, "generated", "wasm", "acyclic_fs_wasm_bg.wasm"), "wasm-fixture");
  const packageArchive = join(fixture, "package.tgz");
  const nativeArchive = join(fixture, "native.tgz");
  execFileSync("tar", ["-czf", packageArchive, "-C", join(fixture, "archive"), "package"]);
  execFileSync("tar", ["-czf", nativeArchive, "-C", nativeRoot, "acyclic-fs-test.js"]);
  return {
    fixture,
    packageRoot,
    nativeBinding,
    packageArchive,
    nativeArchive,
    receipt: join(fixture, "receipt.json"),
  };
};

const qualifyArgs = (manifestPath, fixture) => [
  "--package-root", fixture.packageRoot,
  "--native-binding", fixture.nativeBinding,
  "--native-archive", fixture.nativeArchive,
  "--source-root", ROOT,
  "--source-manifest", manifestPath,
  "--source-commit", COMMIT,
  "--archive", fixture.packageArchive,
  "--receipt", fixture.receipt,
];

const main = async () => {
  const progress = label => process.stderr.write(`[filesystem-admission-test] ${label}\n`);
  const temporary = await mkdtemp(join(tmpdir(), "acyclic-fs-admission-regressions-"));
  const manifestPath = join(temporary, "source-attestation.json");
  const selectedSource = join(ROOT, "rust", "crates", "sdk-generation", "research", "qualified-prototypes", "filesystem-admission", "run.mjs");
  const selectedOriginal = await readFile(selectedSource);
  try {
    progress("write manifest");
    await writeManifest(manifestPath);

    progress("attestation");
    const valid = await run(attestationArgs(manifestPath));
    assert.equal(valid.status, 0, valid.output);

    progress("wrong commit");
    const wrongCommit = await run(attestationArgs(manifestPath, "0".repeat(40)));
    assert.notEqual(wrongCommit.status, 0);
    assert.match(wrongCommit.output, /--source-commit does not match the source root HEAD/);

    progress("missing selector");
    const missingSelector = await withManifest(manifestPath, manifest => {
      manifest.files.pop();
      manifest.file_count -= 1;
    }, async () => await run(attestationArgs(manifestPath)));
    assert.notEqual(missingSelector.status, 0);
    assert.match(missingSelector.output, /source manifest does not match required source selector; missing=/);

    progress("source mutation");
    const sourceMutation = await (async () => {
      await appendFile(selectedSource, "\n// qualification mutation regression\n");
      try {
        return await run(attestationArgs(manifestPath));
      } finally {
        await writeFile(selectedSource, selectedOriginal);
      }
    })();
    assert.notEqual(sourceMutation.status, 0);
    assert.match(sourceMutation.output, /source manifest hash mismatch for rust\/crates\/sdk-generation\/research\/qualified-prototypes\/filesystem-admission\/run\.mjs/);

    progress("fresh git state");
    const freshGitState = await withManifest(manifestPath, manifest => {
      manifest.source_state = manifest.source_state === "clean" ? "dirty" : "clean";
    }, async () => await run(attestationArgs(manifestPath)));
    assert.notEqual(freshGitState.status, 0);
    assert.match(freshGitState.output, /source manifest state does not match Git: expected/);

    progress("archive mismatch fixture");
    const archiveMismatch = await makeFixture({ archiveJs: "archive-A" });
    try {
      const installedB = await readFile(join(archiveMismatch.packageRoot, "generated", "wasm", "acyclic_fs_wasm.js"));
      await writeFile(join(archiveMismatch.packageRoot, "generated", "wasm", "acyclic_fs_wasm.js"), Buffer.concat([installedB, Buffer.from("installed-B")]));
      progress("archive mismatch qualify");
      const result = await run(qualifyArgs(manifestPath, archiveMismatch));
      assert.notEqual(result.status, 0);
      assert.match(result.output, /package WASM JavaScript differs from archive entry/);
    } finally {
      await rm(archiveMismatch.fixture, { recursive: true, force: true });
    }

    progress("unknown fixture");
    const unknown = await makeFixture({ mode: "unknown" });
    try {
      progress("unknown qualify");
      const result = await run(qualifyArgs(manifestPath, unknown));
      assert.notEqual(result.status, 0);
      assert.match(result.output, /wasm accepted null: downstream:unknown: expected a finite integer in the u32 range/);
    } finally {
      await rm(unknown.fixture, { recursive: true, force: true });
    }

    progress("artifact mutation fixture");
    const artifactMutation = await makeFixture({ mode: "mutate-artifact" });
    try {
      progress("artifact mutation qualify");
      const result = await run(qualifyArgs(manifestPath, artifactMutation));
      assert.notEqual(result.status, 0);
      assert.match(result.output, /qualification artifact changed during execution: wasm_binary/);
    } finally {
      await rm(artifactMutation.fixture, { recursive: true, force: true });
    }

    progress("source during qualification fixture");
    const sourceDuringQualification = await makeFixture({ mode: "mutate-source" });
    try {
      progress("source during qualification qualify");
      const result = await run(qualifyArgs(manifestPath, sourceDuringQualification));
      assert.notEqual(result.status, 0);
      assert.match(result.output, /source manifest hash mismatch for rust\/crates\/sdk-generation\/research\/qualified-prototypes\/filesystem-admission\/run\.mjs/);
    } finally {
      await writeFile(selectedSource, selectedOriginal);
      await rm(sourceDuringQualification.fixture, { recursive: true, force: true });
    }
  } finally {
    await writeFile(selectedSource, selectedOriginal);
    await rm(temporary, { recursive: true, force: true });
  }
};

await main();
console.log("filesystem-admission regression tests passed");
