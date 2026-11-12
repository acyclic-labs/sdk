#!/usr/bin/env node

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { publicationDependencyErrors, registryArchiveMatches } from "./publish-cargo-crates.mjs";

test("publication check rejects a newly publishable workspace crate until its inventory is updated", () => {
  const repository = mkdtempSync(join(tmpdir(), "acyclic-cargo-inventory-"));
  const cargo = args => {
    const result = spawnSync("cargo", args, { cwd: repository, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
  };
  const workspace = members => {
    writeFileSync(join(repository, "Cargo.toml"),
      `[workspace]\nmembers = ${JSON.stringify(members)}\nresolver = "3"\n`);
    cargo(["generate-lockfile", "--offline"]);
  };
  const crate = name => {
    mkdirSync(join(repository, name, "src"), { recursive: true });
    writeFileSync(join(repository, name, "Cargo.toml"),
      `[package]\nname = "${name}"\nversion = "0.1.0"\nedition = "2024"\n`);
    writeFileSync(join(repository, name, "src", "lib.rs"), "");
  };
  const inventory = names => writeFileSync(join(repository, "release", "cargo-crates.json"), JSON.stringify(names));
  const check = () => spawnSync(process.execPath, ["scripts/publish-cargo-crates.mjs", "check"],
    { cwd: repository, encoding: "utf8" });
  try {
    mkdirSync(join(repository, "scripts"));
    mkdirSync(join(repository, "release"));
    for (const name of ["publish-cargo-crates.mjs", "registry-publication.mjs"]) {
      copyFileSync(resolve("scripts", name), join(repository, "scripts", name));
    }
    writeFileSync(join(repository, "release", "cargo-equivalent-archives.json"), "{}");
    crate("alpha");
    workspace(["alpha"]);
    inventory(["alpha"]);
    assert.equal(check().status, 0);

    // Real Cargo discovers the added publishable crate without a Rust build.
    crate("beta");
    workspace(["alpha", "beta"]);
    const missing = check();
    assert.equal(missing.status, 1);
    assert.match(missing.stderr, /must contain every publishable local crate exactly once/);
    inventory(["alpha", "beta"]);
    const repaired = check();
    assert.equal(repaired.status, 0, repaired.stderr);
  } finally {
    rmSync(repository, { recursive: true, force: true });
  }
});

test("pinned Cargo archives require identical inputs and an ancestor source", async t => {
  const repository = mkdtempSync(join(tmpdir(), "acyclic-cargo-equivalence-"));
  const manifest = join(repository, "rust", "crates", "example", "Cargo.toml");
  const source = join(dirname(manifest), "src", "lib.rs");
  const lockfile = join(repository, "Cargo.lock");
  const item = { manifest_path: manifest };
  const observed = { checksum: "published-checksum" };
  const checksum = "rebuilt-checksum";
  const git = args => {
    const result = spawnSync("git", args, { cwd: repository, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
  };
  const commit = message => {
    git(["add", "."]);
    git(["commit", "-qm", message]);
    return git(["rev-parse", "HEAD"]);
  };
  const match = (head, pin) => registryArchiveMatches(
    "example", "0.1.0", observed, checksum, head, item,
    { repository, equivalents: { "example@0.1.0": pin } },
  );

  try {
    mkdirSync(dirname(source), { recursive: true });
    writeFileSync(join(repository, "Cargo.toml"), "[workspace]\n");
    writeFileSync(lockfile, "# locked\n");
    writeFileSync(manifest, "[package]\nname = 'example'\nversion = '0.1.0'\n");
    writeFileSync(source, "pub fn value() -> u8 { 1 }\n");
    git(["init", "-q"]);
    git(["config", "user.name", "Release Test"]);
    git(["config", "user.email", "release-test@example.invalid"]);
    git(["config", "commit.gpgsign", "false"]);
    const original = commit("original package");
    const pin = { checksum: observed.checksum, sourceSha: original };

    await t.test("exact archive bytes need no pin", () => {
      const exact = registryArchiveMatches(
        "example", "0.1.0", { checksum }, checksum, original, item,
        { repository, equivalents: {} },
      );
      assert.equal(exact, "exact");
    });

    writeFileSync(join(repository, "unrelated.txt"), "unrelated\n");
    const later = commit("unrelated change");
    await t.test("pinned registry bytes survive unrelated commits", () => {
      assert.equal(match(later, pin), "equivalent");
    });
    await t.test("invalid pins and non-ancestor commits are rejected", () => {
      assert.equal(match(later, { ...pin, checksum: "other" }), null);
      assert.equal(match(later, { ...pin, sourceSha: "invalid" }), null);
      assert.equal(match(original, { ...pin, sourceSha: later }), null);
      assert.equal(registryArchiveMatches(
        "other", "0.1.0", observed, checksum, later, item,
        { repository, equivalents: { "example@0.1.0": pin } },
      ), null);
    });

    writeFileSync(source, "pub fn value() -> u8 { 2 }\n");
    const changedSource = commit("change crate source");
    await t.test("changed crate source is rejected", () => {
      assert.equal(match(changedSource, pin), null);
    });

    writeFileSync(lockfile, "# changed lockfile\n");
    const changedLockfile = commit("change workspace lockfile");
    await t.test("changed workspace lockfile is rejected", () => {
      assert.equal(match(changedLockfile, { ...pin, sourceSha: changedSource }), null);
    });
  } finally {
    rmSync(repository, { recursive: true, force: true });
  }
});

test("published crates depend only on published, versioned local crates outside dev-dependencies", () => {
  const local = (name, extra = {}) => ({ name, path: `/crates/${name}`, req: "=0.2.0", kind: null, optional: false, ...extra });
  const published = { name: "published", dependencies: [] };
  const byName = new Map([["published", published], ["runtime", { name: "runtime", dependencies: [] }]]);
  const check = dependencies => publicationDependencyErrors([{ ...published, dependencies }], byName);
  assert.deepEqual(check([local("runtime"), local("test-support", { kind: "dev", req: "*" })]), []);
  assert.deepEqual(check([local("test-support", { optional: true, req: "*" })]),
    ["published depends on unpublished test-support (optional normal dependency)"]);
  assert.deepEqual(check([local("generator", { kind: "build" })]),
    ["published depends on unpublished generator (build dependency)"]);
  assert.deepEqual(check([local("runtime", { req: "*" })]),
    ["published depends on runtime without a version requirement"]);
  assert.deepEqual(publicationDependencyErrors([{ name: "unpublished", dependencies: [local("test-support")] }], byName), []);
});
