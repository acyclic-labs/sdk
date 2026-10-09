import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { chmodSync, cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
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

for (const command of ["create", "consumer"]) {
  test(`${command} requires its compiler receipt argument`, () => {
    const work = mkdtempSync(join(tmpdir(), "acyclic-required-compiler-receipt-"));
    try {
      const args = command === "create" ? [command, work, "a".repeat(40)] : [command, join(work, "must-not-run")];
      const result = spawnSync(process.execPath, [qualification, ...args], { encoding: "utf8" });
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /usage:.*BUILD_RECEIPT/);
      assert.equal(existsSync(join(work, "QUALIFICATION.json")), false);
    } finally { rmSync(work, { recursive: true, force: true }); }
  });
}

test("failed consumer build cannot emit a compiled-source receipt", () => {
  const work = mkdtempSync(join(tmpdir(), "acyclic-build-receipt-failure-"));
  try {
    const receipt = join(work, "BUILD.json");
    const result = spawnSync(process.execPath, [qualification, "consumer", join(work, "absent-bun"), receipt], { encoding: "utf8" });
    assert.notEqual(result.status, 0);
    assert.equal(JSON.parse(result.stdout).passed, false);
    assert.equal(existsSync(receipt), false);
  } finally { rmSync(work, { recursive: true, force: true }); }
});

test("consumer build refuses to overwrite an existing source receipt", () => {
  const work = mkdtempSync(join(tmpdir(), "acyclic-build-receipt-existing-"));
  try {
    const receipt = join(work, "BUILD.json");
    writeFileSync(receipt, "original receipt\n");
    const result = spawnSync(process.execPath, [qualification, "consumer", join(work, "must-not-run"), receipt], { encoding: "utf8" });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /build receipt already exists/);
    assert.equal(readFileSync(receipt, "utf8"), "original receipt\n");
  } finally { rmSync(work, { recursive: true, force: true }); }
});

for (const mutation of ["source", "stale-dist"]) {
  test(`consumer build rejects ${mutation} before emitting a source receipt`, { skip: process.platform === "win32" }, () => {
    const stage = mkdtempSync(join(tmpdir(), "acyclic-build-receipt-boundary-"));
    try {
      for (const directory of ["scripts", "release", "target", "typescript/packages/demo/src"]) mkdirSync(join(stage, directory), { recursive: true });
      cpSync(qualification, join(stage, "scripts/typescript-qualification.mjs"));
      cpSync(join(root, "release/npm-packages.json"), join(stage, "release/npm-packages.json"));
      writeFileSync(join(stage, ".gitignore"), "target/\ntypescript/packages/*/dist/\n");
      writeFileSync(join(stage, "typescript/packages/demo/src/index.ts"), "export const value = 1;\n");
      const git = args => {
        const result = spawnSync("git", args, { cwd: stage, encoding: "utf8" });
        assert.equal(result.status, 0, result.stderr);
      };
      git(["init", "--quiet"]);
      git(["config", "user.name", "qualification-test"]);
      git(["config", "user.email", "qualification@example.invalid"]);
      git(["add", "."]); git(["commit", "--quiet", "-m", "captured source fixture"]);
      const outputs = releasePackages.map(({ directory }) => {
        const path = `typescript/packages/${directory}/dist/index.js`;
        mkdirSync(dirname(join(stage, path)), { recursive: true });
        writeFileSync(join(stage, path), "export {};\n");
        return path;
      });
      if (mutation === "stale-dist") writeFileSync(join(stage, dirname(outputs[0]), "stale.js"), "stale output\n");
      const fakeBun = join(stage, "target/fake-bun");
      const action = mutation === "source"
        ? "printf '\\nexport const changed = true;\\n' >> typescript/packages/demo/src/index.ts"
        : outputs.map(path => `printf 'TSFILE: %s\\n' '${path}'`).join("\n");
      writeFileSync(fakeBun, `#!/bin/sh\ncase " $* " in *" --version "*) printf 'Version 7.0.2\\n';; *" -b "*)\n${action}\n;; esac\nexit 0\n`);
      chmodSync(fakeBun, 0o700);
      const receipt = join(stage, "target/BUILD.json");
      const result = spawnSync(process.execPath, [join(stage, "scripts/typescript-qualification.mjs"), "consumer", fakeBun, receipt], { encoding: "utf8" });
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, mutation === "source" ? /compiler source changed/ : /dist inventory differs from files emitted/);
      assert.equal(existsSync(receipt), false);
    } finally { rmSync(stage, { recursive: true, force: true }); }
  });
}

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

for (const mutation of ["none", "archive-dist", "output", "source-digest", "commit", "rust-scope", "missing-execution", "missing-emitted", "source-change"]) {
  test(`compiler-bound archive admission: ${mutation}`, () => {
    const stage = mkdtempSync(join(tmpdir(), "acyclic-compiler-archive-"));
    try {
      mkdirSync(join(stage, "scripts"));
      mkdirSync(join(stage, "release"));
      mkdirSync(join(stage, "target"));
      for (const name of ["typescript-qualification.mjs", "validate-npm-package.mjs", "archive-utils.mjs"]) cpSync(join(root, "scripts", name), join(stage, "scripts", name));
      cpSync(join(root, "release/npm-packages.json"), join(stage, "release/npm-packages.json"));
      writeFileSync(join(stage, ".gitignore"), "/target/\n/archives/\n/archive-inputs/\n/typescript/packages/*/dist/\n");
      for (const item of entries) writeArchiveFixture(stage, item);
      const git = args => {
        const result = spawnSync("git", args, { cwd: stage, encoding: "utf8" });
        assert.equal(result.status, 0, result.stderr);
        return result.stdout;
      };
      git(["init", "--quiet"]);
      git(["add", "."]);
      git(["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "core.hooksPath=", "commit", "--quiet", "-m", "fixture"]);
      const sourceCommit = git(["rev-parse", "HEAD"]).trim();
      const record = path => {
        const bytes = readFileSync(join(stage, path));
        return { path, sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`, bytes: bytes.length };
      };
      const sorted = paths => paths.sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b))).map(record);
      const source = sorted(git(["ls-files", "-z", "--", "scripts", "release", "typescript"]).split("\0").filter(Boolean));
      const outputs = sorted(entries.flatMap(item => ["index.js", "index.d.ts"].map(file => `typescript/packages/${item.directory}/dist/${file}`)));
      const sourceDigest = `sha256:${createHash("sha256").update(source.map(file => `${file.path}\0${file.sha256}\0${file.bytes}\n`).join("")).digest("hex")}`;
      // Synthetic receipt tests admission replay only; it is not execution evidence.
      const commands = [
        ["install", "--frozen-lockfile"], ["x", "tsc", "--version"],
        ["x", "tsc", "-b", "--force", "--listEmittedFiles", "--pretty", "false"],
        ["x", "tsc", "-p", "typescript/packages/sdk/consumer-tsconfig.json", "--pretty", "false"],
        ["test", "typescript/packages/sdk/test/public-consumer.test.ts"],
      ];
      const executions = commands.map((args, index) => ({ command: "fixture-bun", arguments: args, stdout: index === 1 ? "Version 5.9.3\n" : index === 2 ? outputs.map(file => `TSFILE: ${file.path}\n`).join("") : "", stderr: "" }));
      const build = { schema: "acyclic.typescript-build-receipt.v1", scope: "typescript-compiler", source_commit: sourceCommit, source_sha256: sourceDigest, source_files: source, outputs, rust_producers_qualified: false, runtime: process.version, executions };
      if (mutation === "output") build.outputs[0].sha256 = `sha256:${"0".repeat(64)}`;
      if (mutation === "source-digest") build.source_sha256 = `sha256:${"0".repeat(64)}`;
      if (mutation === "commit") build.source_commit = "0".repeat(40);
      if (mutation === "rust-scope") build.rust_producers_qualified = true;
      if (mutation === "missing-execution") build.executions.pop();
      if (mutation === "missing-emitted") build.executions[2].stdout = "";
      if (mutation === "source-change") writeFileSync(join(stage, "typescript/packages", entries[0].directory, "README.md"), "changed after compilation\n");
      if (mutation === "archive-dist") {
        const input = join(stage, "archive-inputs", entries[0].asset.slice(0, -4));
        writeFileSync(join(input, "package/dist/index.js"), "export const stale = true;\n");
        const packed = spawnSync("tar", ["-czf", join(stage, "archives", entries[0].asset), "-C", input, "package"], { encoding: "utf8" });
        assert.equal(packed.status, 0, packed.stderr);
      }
      const buildPath = join(stage, "target/BUILD.json");
      writeFileSync(buildPath, JSON.stringify(build));
      const receiptPath = join(stage, "archives/QUALIFICATION.json");
      const result = spawnSync(process.execPath, [join(stage, "scripts/typescript-qualification.mjs"), "create", join(stage, "archives"), sourceCommit, buildPath], { encoding: "utf8" });
      if (mutation === "none") {
        assert.equal(result.status, 0, result.stderr);
        const receipt = JSON.parse(readFileSync(receiptPath, "utf8"));
        assert.equal(receipt.revision, 1);
        assert.equal(receipt.source_sha256, sourceDigest);
        assert.equal(receipt.rust_producers_qualified, false);
        for (const item of entries) {
          const verified = spawnSync(process.execPath, [join(stage, "scripts/typescript-qualification.mjs"), "verify", receiptPath, sourceCommit, item.asset, join(stage, "archives", item.asset)], { encoding: "utf8" });
          assert.equal(verified.status, 0, verified.stderr);
        }
      } else {
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, /archive dist differs|compiler build receipt|compiler source is not a clean|dist inventory differs/);
        assert.equal(existsSync(receiptPath), false);
      }
    } finally { rmSync(stage, { recursive: true, force: true }); }
  });
}

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
    writeFileSync(receipt, `${JSON.stringify({ revision: 1, scope: "typescript-compiler", source_commit: sourceCommit, source_sha256: `sha256:${"c".repeat(64)}`, compiler_build_receipt_sha256: `sha256:${"d".repeat(64)}`, rust_producers_qualified: false, packages })}\n`);

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
