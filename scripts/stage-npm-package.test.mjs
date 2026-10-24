import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

test("package staging admits manifest packages without requiring optional documentation", () => {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const source = mkdtempSync(join(root, "typescript/packages/.stage-fixture-"));
  const scratch = mkdtempSync(join(tmpdir(), "stage-package-"));
  const stage = target => spawnSync("bash", [join(root, "scripts/stage-npm-package.sh"), source, target], { encoding: "utf8" });
  try {
    writeFileSync(join(source, "package.json"), JSON.stringify({ name: "fixture", version: "1.0.0", files: ["dist"] }));
    mkdirSync(join(source, "dist"));
    writeFileSync(join(source, "dist/index.js"), "export const value = 7;\n");
    mkdirSync(join(source, "node_modules"));
    writeFileSync(join(source, "node_modules/unpublished"), "local dependency");
    writeFileSync(join(source, "previous.tgz"), "old archive");
    for (const documented of [false, true]) {
      if (documented) {
        writeFileSync(join(source, "README.md"), "fixture readme\n");
        writeFileSync(join(source, "CHANGELOG.md"), "fixture changelog\n");
      }
      const target = join(scratch, documented ? "documented" : "manifest-only");
      const result = stage(target);
      assert.equal(result.status, 0, result.stderr);
      assert.equal(readFileSync(join(target, "dist/index.js"), "utf8"), "export const value = 7;\n");
      assert.equal(existsSync(join(target, "README.md")), documented);
      assert.equal(existsSync(join(target, "node_modules")), false);
      assert.equal(existsSync(join(target, "previous.tgz")), false);
      assert.notEqual(stage(target).status, 0, "an existing stage cannot be reused");
    }
    rmSync(join(source, "package.json"));
    assert.notEqual(stage(join(scratch, "missing-manifest")).status, 0);
  } finally {
    rmSync(source, { recursive: true, force: true });
    rmSync(scratch, { recursive: true, force: true });
  }
});
