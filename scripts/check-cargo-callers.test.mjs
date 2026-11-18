import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { cargoCallerFindings, cargoCallers, RECORD } from "./check-cargo-callers.mjs";

test("the repository's Cargo callers are exactly the recorded ones", () => {
  const root = fileURLToPath(new URL("..", import.meta.url));
  assert.deepEqual(cargoCallerFindings(root, JSON.parse(readFileSync(join(root, RECORD), "utf8"))), []);
});

test("a new Cargo caller and a stale record entry both refuse", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-cargo-callers-"));
  try {
    const git = (...args) => execFileSync("git", args, { cwd: root });
    git("init", "-q");
    mkdirSync(join(root, "scripts/fixtures"), { recursive: true });
    const put = (file, text) => { writeFileSync(join(root, file), text); git("add", file); };
    put("scripts/generate.mjs", 'run(bazelExecutable(root, "//a:b"), []);\n');
    // Planted invocations are recorded fixtures: a fixture, like documentation, is not tooling.
    const fixture = name => readFileSync(new URL(`./fixtures/cargo-callers/${name}`, import.meta.url), "utf8");
    put("scripts/fixtures/log.mjs", fixture("recorded-log.txt"));
    put("README.md", fixture("recorded-log.txt"));
    assert.deepEqual(cargoCallers(root), []);
    for (const planted of fixture("planted.txt").split("\n").filter(Boolean)) {
      put("scripts/generate.mjs", `${planted}\n`);
      assert.deepEqual(cargoCallerFindings(root, []), ["scripts/generate.mjs invokes Cargo; build or run it through a Bazel target"]);
      assert.deepEqual(cargoCallerFindings(root, ["scripts/generate.mjs"]), []);
    }
    put("scripts/generate.mjs", 'run(bazelExecutable(root, "//a:b"), []);\n');
    assert.deepEqual(cargoCallerFindings(root, ["scripts/generate.mjs"]), [`scripts/generate.mjs no longer invokes Cargo; delete it from ${RECORD}`]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
