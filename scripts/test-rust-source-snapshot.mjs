import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createRustSourceSnapshot } from "./create-rust-source-snapshot.mjs";

function fixture() {
  const scope = mkdtempSync(join(tmpdir(), "acyclic-snapshot-test-"));
  const sourceRoot = join(scope, "source");
  mkdirSync(sourceRoot);
  function git(args) {
    const result = spawnSync("git", ["-C", sourceRoot, ...args], { encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
  }
  git(["init"]);
  writeFileSync(join(sourceRoot, "source.rs"), "pub const VERSION: u32 = 1;\n");
  git(["add", "source.rs"]);
  git(["-c", "user.name=Snapshot Test", "-c", "user.email=snapshot@example.invalid", "commit", "-m", "fixture"]);
  return { scope, sourceRoot, revision: git(["rev-parse", "HEAD"]) };
}

test("snapshot preserves committed identity and excludes active edits", () => {
  const f = fixture();
  writeFileSync(join(f.sourceRoot, "source.rs"), "pub const VERSION: u32 = 2;\n");
  writeFileSync(join(f.sourceRoot, "scratch.rs"), "untracked\n");
  const destination = join(f.scope, "snapshot");
  const receipt = createRustSourceSnapshot({ ...f, destination });
  assert.equal(receipt.source_revision, f.revision);
  assert.equal(receipt.clean, true);
  assert.equal(receipt.qualification, false);
  assert.match(readFileSync(join(destination, "source.rs"), "utf8"), /VERSION: u32 = 1/);
  assert.equal(existsSync(join(destination, "scratch.rs")), false);
  assert.equal(JSON.parse(readFileSync(`${destination}.receipt.json`, "utf8")).source_revision, f.revision);
});

test("archive folder cannot inherit its parent repository identity", () => {
  const f = fixture();
  const archive = join(f.sourceRoot, "archive");
  mkdirSync(archive);
  const destination = join(f.scope, "rejected");
  assert.throws(() => createRustSourceSnapshot({ sourceRoot: archive, destination, revision: f.revision }), /parent Git repository/);
  assert.equal(existsSync(destination), false);
});

test("existing snapshot destination is preserved", () => {
  const f = fixture();
  const destination = join(f.scope, "existing");
  mkdirSync(destination);
  writeFileSync(join(destination, "sentinel"), "preserve");
  assert.throws(() => createRustSourceSnapshot({ ...f, destination }), /already exists/);
  assert.equal(readFileSync(join(destination, "sentinel"), "utf8"), "preserve");
});

test("unknown revision fails before creating a checkout", () => {
  const f = fixture();
  const destination = join(f.scope, "unknown");
  assert.throws(() => createRustSourceSnapshot({ ...f, destination, revision: "missing-ref" }));
  assert.equal(existsSync(destination), false);
});

test("inherited Git repository settings cannot redirect snapshot checkout", () => {
  const selected = fixture();
  const unrelated = fixture();
  const keys = ["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES"];
  const prior = new Map(keys.map(key => [key, process.env[key]]));
  try {
    process.env.GIT_DIR = join(unrelated.sourceRoot, ".git");
    process.env.GIT_WORK_TREE = unrelated.sourceRoot;
    process.env.GIT_COMMON_DIR = join(unrelated.sourceRoot, ".git");
    process.env.GIT_INDEX_FILE = join(unrelated.sourceRoot, ".git", "index");
    process.env.GIT_OBJECT_DIRECTORY = join(unrelated.sourceRoot, ".git", "objects");
    process.env.GIT_ALTERNATE_OBJECT_DIRECTORIES = join(unrelated.sourceRoot, ".git", "objects");
    const destination = join(selected.scope, "independent");
    const receipt = createRustSourceSnapshot({ ...selected, destination });
    assert.equal(receipt.source_revision, selected.revision);
    assert.equal(receipt.clean, true);
    assert.equal(readFileSync(join(unrelated.sourceRoot, ".git", "HEAD"), "utf8").startsWith("ref: "), true);
    assert.equal(existsSync(join(destination, ".git")), true);
  } finally {
    for (const [key, value] of prior) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});

test("snapshot fetches only the selected revision without unrelated history", () => {
  const f = fixture();
  const git = args => spawnSync("git", ["-C", f.sourceRoot, ...args], { encoding: "utf8" });
  writeFileSync(join(f.sourceRoot, "source.rs"), "pub const VERSION: u32 = 3;\n");
  assert.equal(git(["add", "source.rs"]).status, 0);
  assert.equal(git(["-c", "user.name=Snapshot Test", "-c", "user.email=snapshot@example.invalid", "commit", "-m", "selected"]).status, 0);
  const selected = git(["rev-parse", "HEAD"]).stdout.trim();
  const destination = join(f.scope, "shallow");
  createRustSourceSnapshot({ ...f, destination, revision: selected });
  const count = spawnSync("git", ["-C", destination, "rev-list", "--count", "HEAD"], { encoding: "utf8" });
  assert.equal(count.status, 0, count.stderr);
  assert.equal(count.stdout.trim(), "1");
  assert.equal(spawnSync("git", ["-C", destination, "cat-file", "-e", f.revision], { encoding: "utf8" }).status, 1);
});