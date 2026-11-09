import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

test("every inventory importer rejects ignored copies before using files", t => {
  const root = mkdtempSync(join(tmpdir(), "sdk-packaged-copies-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  execFileSync("git", ["init", "--quiet", root], { windowsHide: true });
  mkdirSync(join(root, "scripts"));
  writeFileSync(join(root, ".gitignore"), "/generated/*\n!/generated/pinned.json\n");
  const module = new URL("./generated-bindings.mjs", import.meta.url);
  const original = readFileSync(module, "utf8");
  const inventory = "export const packagedSourceCopies = [";
  const run = copy => {
    writeFileSync(join(root, "scripts/generated-bindings.mjs"), original.replace(inventory,
      `${inventory}\n  ["missing-source.json", ${JSON.stringify(copy)}],`));
    return spawnSync(process.execPath, ["--input-type=module", "-e", 'import "./scripts/generated-bindings.mjs"'], {
      cwd: root, encoding: "utf8", windowsHide: true,
    });
  };
  // Absence and existence must have identical policy results, even if force-added.
  const ignored = "generated/build-only.json";
  for (const state of ["absent", "present", "tracked"]) {
    if (state === "present") {
      mkdirSync(join(root, "generated"));
      writeFileSync(join(root, ignored), "{}");
    }
    if (state === "tracked") execFileSync("git", ["add", "--force", ignored], { cwd: root, windowsHide: true });
    const result = run(ignored);
    assert.equal(result.status, 1, result.stderr);
    assert.match(result.stderr, /STEP_FAILED packaged-source-copy-ignored: generated\/build-only\.json/);
    assert.doesNotMatch(result.stderr, /ENOENT/);
  }
  for (const copy of ["pinned/copy.json", "generated/pinned.json"]) {
    const result = run(copy);
    assert.equal(result.status, 0, result.stderr);
  }
});
