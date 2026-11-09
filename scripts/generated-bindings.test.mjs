import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, symlinkSync, utimesSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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

test("inventory policy rejects ignored copies in exports and checkouts independently of parent policy", t => {
  const parent = mkdtempSync(join(tmpdir(), "sdk-packaged-copies-"));
  const root = join(parent, "export");
  t.after(() => rmSync(parent, { recursive: true, force: true }));
  execFileSync("git", ["init", "--quiet", parent], { windowsHide: true });
  writeFileSync(join(parent, ".gitignore"), "/export/\n");
  mkdirSync(root);
  mkdirSync(join(root, "scripts"));
  symlinkSync(resolve("node_modules"), join(root, "node_modules"), "junction");
  writeFileSync(join(root, ".gitignore"), "/generated/*\n!/generated/pinned.json\n/nested/*.json\n/blocked/\n");
  mkdirSync(join(root, "nested"));
  writeFileSync(join(root, "nested/.gitignore"), "!pinned.json\nlocal.json\n");
  mkdirSync(join(root, "blocked"));
  writeFileSync(join(root, "blocked/.gitignore"), "!pinned.json\n");
  const module = new URL("./generated-bindings.mjs", import.meta.url);
  const original = readFileSync(module, "utf8");
  const inventory = "export const packagedSourceCopies = [";
  const ignored = "generated/build-only.json";
  const copies = [ignored, "nested/ignored.json", "nested/local.json", "blocked/pinned.json",
    "pinned/copy.json", "generated/pinned.json", "nested/pinned.json"];
  for (const [index, copy] of copies.entries()) {
    writeFileSync(join(root, `scripts/generated-bindings-${index}.mjs`), original.replace(inventory,
      `${inventory}\n  ["missing-source.json", ${JSON.stringify(copy)}],`));
  }
  const run = () => {
    const result = spawnSync(process.execPath, ["--input-type=module", "-e", `
      const results = [];
      for (let index = 0; index < ${copies.length}; index++) {
        try { await import('./scripts/generated-bindings-' + index + '.mjs'); results.push(null); }
        catch (error) { results.push(error.message); }
      }
      console.log(JSON.stringify(results));`], {
      cwd: root, encoding: "utf8", windowsHide: true,
    });
    assert.equal(result.status, 0, result.stderr);
    const errors = JSON.parse(result.stdout);
    for (const [index, copy] of copies.entries()) {
      if (index < 4) {
        assert.ok(errors[index]?.startsWith(`STEP_FAILED packaged-source-copy-ignored: ${copy};`), errors[index]);
      } else assert.equal(errors[index], null, copy);
    }
  };
  // Absence and existence must have identical policy results, even if force-added.
  for (const state of ["absent", "present", "tracked"]) {
    if (state === "present") {
      mkdirSync(join(root, "generated"));
      writeFileSync(join(root, ignored), "{}");
    }
    if (state === "tracked") {
      execFileSync("git", ["init", "--quiet", root], { windowsHide: true });
      execFileSync("git", ["add", "--force", ignored], { cwd: root, windowsHide: true });
    }
    run();
  }
});
