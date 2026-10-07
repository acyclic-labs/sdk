import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import test from "node:test";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const qualification = join(root, "scripts/typescript-qualification.mjs");
const releasePackages = JSON.parse(readFileSync(join(root, "release/npm-packages.json"), "utf8"))
  .filter(item => item.source === "typescript");

function packageEntries() {
  return releasePackages.map(item => {
    const manifest = JSON.parse(readFileSync(join(root, "typescript/packages", item.directory, "package.json"), "utf8"));
    assert.equal(manifest.name, item.name);
    return {
      asset: `acyclic-labs-${item.slug}-${manifest.version}.tgz`,
      name: item.name,
      version: manifest.version,
      directory: item.directory,
    };
  });
}

const entries = packageEntries();

function writeArchiveFixture(stage, item) {
  const packageDirectory = join(stage, "typescript/packages", item.directory);
  const output = join(stage, "archives");
  mkdirSync(join(packageDirectory, "dist"), { recursive: true });
  mkdirSync(output, { recursive: true });
  writeFileSync(join(packageDirectory, "package.json"), JSON.stringify({
    name: item.name,
    version: item.version,
    repository: { type: "git", url: "git+https://github.com/acyclic-labs/sdk.git", directory: `typescript/packages/${item.directory}` },
    license: "Apache-2.0",
    private: false,
    exports: { ".": { types: "./dist/index.d.ts", default: "./dist/index.js" } },
    types: "./dist/index.d.ts",
  }));
  writeFileSync(join(packageDirectory, "README.md"), `# ${item.name}\n`);
  writeFileSync(join(packageDirectory, "CHANGELOG.md"), `# ${item.name} changelog\n\n## ${item.version} fixture\n`);
  writeFileSync(join(packageDirectory, "dist/index.js"), "export {};\n");
  writeFileSync(join(packageDirectory, "dist/index.d.ts"), "export {};\n");
  const archiveRoot = join(stage, "archive-inputs", item.asset.slice(0, -4));
  cpSync(packageDirectory, join(archiveRoot, "package"), { recursive: true });
  const result = spawnSync("tar", ["-czf", join(output, item.asset), "-C", archiveRoot, "package"], {
    encoding: "utf8",
  });
  assert.equal(result.status, 0, `${result.stderr}\n${result.stdout}`);
  return join(output, item.asset);
}

test("qualification rejects truncated archives before writing a receipt", () => {
  const output = mkdtempSync(join(tmpdir(), "acyclic-qualification-invalid-"));
  try {
    for (const item of packageEntries()) writeFileSync(join(output, item.asset), Buffer.from([1, 2, 3]));

    const result = spawnSync(process.execPath, [qualification, "create", output, "a".repeat(40)], {
      encoding: "utf8",
    });
    assert.notEqual(result.status, 0);
    assert.match(`${result.stderr}\n${result.stdout}`, /incorrect header check/);
    assert.equal(existsSync(join(output, "QUALIFICATION.json")), false);
  } finally {
    rmSync(output, { recursive: true, force: true });
  }
});

test("qualification accepts every valid package archive and records its identity", () => {
  const stage = mkdtempSync(join(tmpdir(), "acyclic-qualification-valid-"));
  try {
    mkdirSync(join(stage, "scripts"), { recursive: true });
    mkdirSync(join(stage, "release"), { recursive: true });
    cpSync(qualification, join(stage, "scripts/typescript-qualification.mjs"));
    cpSync(join(root, "scripts/validate-npm-package.mjs"), join(stage, "scripts/validate-npm-package.mjs"));
    cpSync(join(root, "scripts/archive-utils.mjs"), join(stage, "scripts/archive-utils.mjs"));
    cpSync(join(root, "release/npm-packages.json"), join(stage, "release/npm-packages.json"));
    for (const item of entries) writeArchiveFixture(stage, item);
    const sourceCommit = "c".repeat(40);
    const output = join(stage, "archives");
    const result = spawnSync(process.execPath, [join(stage, "scripts/typescript-qualification.mjs"), "create", output, sourceCommit], {
      encoding: "utf8",
    });
    assert.equal(result.status, 0, `${result.stderr}\n${result.stdout}`);
    const receipt = JSON.parse(readFileSync(join(output, "QUALIFICATION.json"), "utf8"));
    assert.equal(receipt.source_commit, sourceCommit);
    assert.deepEqual(receipt.packages.map(item => item.asset).sort(), entries.map(item => item.asset).sort());
    for (const item of entries) {
      const verified = spawnSync(process.execPath, [
        join(stage, "scripts/typescript-qualification.mjs"),
        "verify",
        join(output, "QUALIFICATION.json"),
        sourceCommit,
        item.asset,
        join(output, item.asset),
      ], { encoding: "utf8" });
      assert.equal(verified.status, 0, `${verified.stderr}\n${verified.stdout}`);
    }
  } finally {
    rmSync(stage, { recursive: true, force: true });
  }
});

test("qualification rejects a valid archive with the wrong manifest identity", () => {
  const stage = mkdtempSync(join(tmpdir(), "acyclic-qualification-identity-"));
  try {
    const selected = entries.find(item => item.name === "@acyclic-labs/stream");
    const archive = writeArchiveFixture(stage, selected);
    const unpacked = mkdtempSync(join(tmpdir(), "acyclic-qualification-unpack-"));
    try {
      const extract = spawnSync("tar", ["-xzf", archive, "-C", unpacked], { encoding: "utf8" });
      assert.equal(extract.status, 0, `${extract.stderr}\n${extract.stdout}`);
      const manifestPath = join(unpacked, "package/package.json");
      const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
      manifest.name = "@acyclic-labs/not-stream";
      writeFileSync(manifestPath, JSON.stringify(manifest));
      const invalidArchive = join(stage, "invalid.tgz");
      const repack = spawnSync("tar", ["-czf", invalidArchive, "-C", unpacked, "package"], { encoding: "utf8" });
      assert.equal(repack.status, 0, `${repack.stderr}\n${repack.stdout}`);
      const validation = spawnSync(process.execPath, [
        join(root, "scripts/validate-npm-package.mjs"),
        invalidArchive,
        selected.name,
        selected.version,
        `typescript/packages/${selected.directory}`,
      ], { encoding: "utf8" });
      assert.notEqual(validation.status, 0);
      assert.match(`${validation.stderr}\n${validation.stdout}`, /manifest does not match/);
    } finally {
      rmSync(unpacked, { recursive: true, force: true });
    }
  } finally {
    rmSync(stage, { recursive: true, force: true });
  }
});

test("verify-only publication staging runs with only the copied qualification script", () => {
  const stage = mkdtempSync(join(tmpdir(), "acyclic-qualification-stage-"));
  try {
    mkdirSync(join(stage, "scripts"), { recursive: true });
    mkdirSync(join(stage, "release"), { recursive: true });
    cpSync(qualification, join(stage, "scripts/typescript-qualification.mjs"));
    cpSync(join(root, "release/npm-packages.json"), join(stage, "release/npm-packages.json"));
    for (const item of releasePackages) {
      const manifest = join(root, "typescript/packages", item.directory, "package.json");
      const target = join(stage, "typescript/packages", item.directory, "package.json");
      mkdirSync(dirname(target), { recursive: true });
      cpSync(manifest, target);
    }

    const sourceCommit = "b".repeat(40);
    const archive = join(stage, "selected.tgz");
    const bytes = Buffer.from([1, 2, 3]);
    writeFileSync(archive, bytes);
    const selected = packageEntries()[0];
    const packages = packageEntries().map(item => ({
      asset: item.asset,
      name: item.name,
      sha256: item.asset === selected.asset ? createHash("sha256").update(bytes).digest("hex") : "0".repeat(64),
      size: item.asset === selected.asset ? bytes.length : 1,
      version: item.version,
    }));
    const receipt = join(stage, "QUALIFICATION.json");
    writeFileSync(receipt, `${JSON.stringify({ revision: 1, source_commit: sourceCommit, packages })}\n`);

    const result = spawnSync(process.execPath, [
      join(stage, "scripts/typescript-qualification.mjs"),
      "verify",
      receipt,
      sourceCommit,
      selected.asset,
      archive,
    ], { cwd: stage, encoding: "utf8" });
    assert.equal(result.status, 0, `${result.stderr}\n${result.stdout}`);
  } finally {
    rmSync(stage, { recursive: true, force: true });
  }
});
