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
  writeFileSync(join(root, ".cargo/config.toml"), '[target.x86_64-unknown-linux-gnu]\nlinker = "scripts/mold-linker.sh"\n\n[env]\n');
  writeFileSync(join(root, "scripts/mold-linker.sh"), "#!/bin/sh\n");
  writeFileSync(join(root, "Cargo.toml"), '[workspace]\nmembers = ["member"]\nresolver = "3"\n\n[profile.dev]\nopt-level = 1\n');
  mkdirSync(join(root, "member/src"), { recursive: true });
  writeFileSync(join(root, "member/Cargo.toml"), '[package]\nname = "member"\nversion = "0.1.0"\nedition = "2024"\n');
  writeFileSync(join(root, "member/src/lib.rs"), "pub fn one() -> u8 {\n    1\n}\n\n#[test]\nfn reads_cargo_config() {\n    assert!(std::path::Path::new(\"member/../.cargo/config.toml\").is_file());\n}\n");
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
    // The musl platform carries its libc constraint, and its toolchain is registered before the
    // hub so it wins over the gnu toolchain that rules_rust also considers compatible.
    const memberBuild = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    assert.match(memberBuild, /"\/\/:scripts\/mold-linker\.sh"/, "a test that carries Cargo's config also carries the linker it names");
    const buildFile = readFileSync(join(root, "BUILD.bazel"), "utf8");
    const platformStart = buildFile.indexOf('platform(\n    name = "linux_musl",');
    assert.notEqual(platformStart, -1, "linux_musl platform is generated");
    const platformBlock = buildFile.slice(platformStart, buildFile.indexOf("\n)\n", platformStart) + 3);
    assert.match(platformBlock, /":libc_musl"/, "linux_musl platform carries libc_musl");
    const moduleFile = readFileSync(join(root, "cargo.MODULE.bazel"), "utf8");
    const muslToolchain = moduleFile.indexOf("@rust_toolchains//:rust_linux_musl__x86_64-unknown-linux-musl__stable");
    const toolchainHub = moduleFile.indexOf("@rust_toolchains//:all");
    assert.notEqual(muslToolchain, -1, "musl toolchain is registered");
    assert.notEqual(toolchainHub, -1, "toolchain hub is registered");
    assert.ok(muslToolchain < toolchainHub, "musl toolchain is registered before the toolchain hub");
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

test("every toolchain builds the dev profile's opt-level, and a profile Bazel cannot render refuses", () => {
  const { root, run } = fixture();
  try {
    assert.equal(run("write").status, 0);
    const moduleFile = readFileSync(join(root, "cargo.MODULE.bazel"), "utf8");
    // rust.toolchain and the musl repository set each carry the dev profile's opt-level 1 for fastbuild.
    assert.equal(moduleFile.match(/opt_level = \{"dbg":"0","fastbuild":"1","opt":"3"\}/g)?.length, 2);
    for (const [table, line] of [
      ["profile.dev", "debug-assertions = false"],
      ["profile.dev", "overflow-checks = false"],
      ["profile.release", "debug-assertions = true"],
      ["profile.dev.package.\"member\"", "overflow-checks = false"],
      ["profile.dev.build-override", "debug-assertions = false"],
    ]) {
      writeFileSync(join(root, "Cargo.toml"), `[workspace]\nmembers = ["member"]\nresolver = "3"\n\n[${table}]\n${line}\n`);
      const refused = run("check");
      assert.notEqual(refused.status, 0);
      assert.ok(refused.stderr.includes(`STEP_FAILED bazel-build-files: [${table}] ${line} has no Bazel rendering`), refused.stderr);
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
