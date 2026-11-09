import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { generateFiles } from "./generated-bindings.mjs";

test("private generation publishes changed bytes, preserves unchanged files and prunes only owned output", async t => {
  const output = mkdtempSync(join(tmpdir(), "sdk-generation-publication-"));
  t.after(() => rmSync(output, { recursive: true, force: true }));
  mkdirSync(join(output, "semantic"));
  writeFileSync(join(output, "semantic/stale.ts"), "obsolete");
  writeFileSync(join(output, "package.json"), "source-owned");
  const render = bytes => temporary => {
    mkdirSync(join(temporary, "semantic"));
    writeFileSync(join(temporary, "semantic/current.ts"), bytes);
  };
  await generateFiles(output, render("first"), ["semantic"]);
  assert.equal(existsSync(join(output, "semantic/stale.ts")), false);
  const current = join(output, "semantic/current.ts");
  utimesSync(current, 1, 1);
  const before = statSync(current, { bigint: true }).mtimeNs;
  await generateFiles(output, render("first"), ["semantic"]);
  assert.equal(statSync(current, { bigint: true }).mtimeNs, before);
  await generateFiles(output, render("second"), ["semantic"]);
  assert.equal(readFileSync(current, "utf8"), "second");
  let privateOutput;
  await assert.rejects(generateFiles(output, temporary => {
    privateOutput = temporary;
    render("incomplete")(temporary);
    throw new Error("renderer failed");
  }, ["semantic"]), /renderer failed/);
  assert.equal(existsSync(privateOutput), false);
  assert.equal(readFileSync(current, "utf8"), "second");
  assert.equal(readFileSync(join(output, "package.json"), "utf8"), "source-owned");
});

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
