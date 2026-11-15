import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

// A one-member workspace with the repository's own toolchain and config inputs.
function fixture() {
  const root = mkdtempSync(join(tmpdir(), "bazel-build-files-"));
  mkdirSync(join(root, "scripts"));
  cpSync("scripts/bazel-build-files.mjs", join(root, "scripts/bazel-build-files.mjs"));
  cpSync("rust-toolchain.toml", join(root, "rust-toolchain.toml"));
  mkdirSync(join(root, ".cargo"));
  writeFileSync(join(root, ".cargo/config.toml"), "[env]\n");
  writeFileSync(join(root, "Cargo.toml"), '[workspace]\nmembers = ["member"]\nresolver = "3"\n\n[profile.dev]\nopt-level = 1\n');
  mkdirSync(join(root, "member/src"), { recursive: true });
  writeFileSync(join(root, "member/Cargo.toml"), '[package]\nname = "member"\nversion = "0.1.0"\nedition = "2024"\n');
  writeFileSync(join(root, "member/src/lib.rs"), "pub fn one() -> u8 {\n    1\n}\n");
  execFileSync("cargo", ["generate-lockfile", "--offline"], { cwd: root, stdio: "ignore" });
  execFileSync("git", ["init", "-q"], { cwd: root });
  execFileSync("git", ["add", "-A"], { cwd: root });
  const run = mode => spawnSync(process.execPath, ["scripts/bazel-build-files.mjs", mode], { cwd: root, encoding: "utf8" });
  return { root, run };
}

test("check accepts the generated files and fails on a stale BUILD file", () => {
  const { root, run } = fixture();
  try {
    assert.equal(run("write").status, 0);
    execFileSync("git", ["add", "-A"], { cwd: root });
    const clean = run("check");
    assert.equal(clean.status, 0, clean.stderr);
    // Negative control: a BUILD file that no longer matches the manifests fails and names itself.
    const build = join(root, "member/BUILD.bazel");
    writeFileSync(build, readFileSync(build, "utf8").replace('"src/lib.rs"', '"src/lib.rs",\n        "src/removed.rs"'));
    const stale = run("check");
    assert.notEqual(stale.status, 0);
    assert.match(stale.stderr, /STEP_FAILED bazel-build-files: stale member\/BUILD\.bazel/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
