import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

// A one-member workspace with a local path crate and the repository's own toolchain and config inputs.
function fixture() {
  const root = mkdtempSync(join(tmpdir(), "bazel-build-files-"));
  mkdirSync(join(root, "scripts"));
  cpSync("scripts/bazel-build-files.mjs", join(root, "scripts/bazel-build-files.mjs"));
  cpSync("scripts/policy-core.mjs", join(root, "scripts/policy-core.mjs"));
  cpSync("scripts/wasm-packages.mjs", join(root, "scripts/wasm-packages.mjs"));
  cpSync("rust-toolchain.toml", join(root, "rust-toolchain.toml"));
  mkdirSync(join(root, ".cargo"));
  writeFileSync(join(root, ".cargo/config.toml"), '[target.x86_64-unknown-linux-gnu]\nlinker = "scripts/mold-linker.sh"\n\n[env]\n');
  writeFileSync(join(root, "scripts/mold-linker.sh"), "#!/bin/sh\n");
  writeFileSync(join(root, "Cargo.toml"), '[workspace]\nmembers = ["member"]\nexclude = ["local"]\nresolver = "3"\n\n[profile.dev]\nopt-level = 1\n\n[profile.dev.package.local]\nopt-level = 3\n');
  mkdirSync(join(root, "member/src"), { recursive: true });
  writeFileSync(join(root, "member/Cargo.toml"), '[package]\nname = "member"\nversion = "0.1.0"\nedition = "2024"\n\n[dependencies]\nlocal = { path = "../local" }\n');
  writeFileSync(join(root, "member/src/lib.rs"), "pub fn one() -> u8 {\n    1\n}\n\n#[test]\nfn reads_cargo_config() {\n    assert!(std::path::Path::new(\"member/../.cargo/config.toml\").is_file());\n}\n");
  mkdirSync(join(root, "local/src"), { recursive: true });
  writeFileSync(join(root, "local/Cargo.toml"), '[package]\nname = "local"\nversion = "0.1.0"\nedition = "2024"\n');
  writeFileSync(join(root, "local/src/lib.rs"), "pub fn two() -> u8 {\n    2\n}\n");
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
    const localBuild = readFileSync(join(root, "local/BUILD.bazel"), "utf8");
    assert.match(localBuild, /"-Copt-level=3"/, "a local path crate carries its dev opt-level in its own BUILD");
    assert.doesNotMatch(moduleFile, /crate = "local"/, "a local path crate is not a third-party annotation");
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

test("a nextest capability profile's cases leave the default target for a manual one, and an unknown filter is refused", () => {
  const { root, run } = fixture();
  try {
    mkdirSync(join(root, ".config"));
    writeFileSync(join(root, ".config/nextest.toml"), '[profile.default]\n\n[profile.pg]\ndefault-filter = "package(=member) & test(pg::capability::)"\n');
    assert.equal(run("write").status, 0);
    const build = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    assert.match(build, /name = "member_test",[^)]*exclude = \["pg::capability::"\]/s);
    assert.match(build, /name = "member_test_pg",[^)]*only = "pg::capability::",[^)]*tags = \["manual"\]/s);
    // Negative controls: a filter the targets cannot express, or would match differently, is refused, never approximated.
    for (const filter of ["test(a) | test(b)", "test(=pg::capability::exact)", "test(/pg::.*/)", "test(#pg::*)"]) {
      writeFileSync(join(root, ".config/nextest.toml"), `[profile.pg]\ndefault-filter = "${filter}"\n`);
      const refused = run("write");
      assert.notEqual(refused.status, 0, filter);
      assert.match(refused.stderr, /STEP_FAILED bazel-build-files: nextest profile pg filter/, filter);
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a profile's run wrapper starts each of its target's cases, with the siblings it runs; a partial wrapper is refused", () => {
  const { root, run } = fixture();
  const profile = wrapped => `[profile.cloud]\ndefault-filter = "test(::cloud::)"\n\n[scripts.wrapper.cloud]\ncommand = { command-line = "scripts/cloud.sh", relative-to = "workspace-root" }\n\n[[profile.cloud.scripts]]\nfilter = "${wrapped}"\nrun-wrapper = "cloud"\n`;
  try {
    mkdirSync(join(root, ".config"));
    writeFileSync(join(root, "scripts/cloud.sh"), '#!/usr/bin/env bash\nnode "$(dirname -- "${BASH_SOURCE[0]}")/start.mjs" -- "$@"\n');
    writeFileSync(join(root, "scripts/start.mjs"), "\n");
    writeFileSync(join(root, ".config/nextest.toml"), profile("test(::cloud::)"));
    execFileSync("git", ["add", "-A"], { cwd: root });
    assert.equal(run("write").status, 0);
    const build = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    assert.match(build, /name = "member_test_cloud",[^)]*only = "::cloud::",\n {4}wrapper = "\/\/:scripts\/cloud\.sh",\n {4}wrapper_data = \["\/\/:scripts\/start\.mjs"\],/s);
    assert.doesNotMatch(build.slice(build.indexOf('name = "member_test",'), build.indexOf('name = "member_test_cloud",')), /wrapper/, "the default target runs its cases directly");
    // Negative control: a wrapper over only some of the profile's cases has no target to run under.
    writeFileSync(join(root, ".config/nextest.toml"), profile("test(::cloud::one)"));
    const refused = run("write");
    assert.notEqual(refused.status, 0);
    assert.match(refused.stderr, /STEP_FAILED bazel-build-files: nextest profile cloud wraps cases by a filter other than its default-filter/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
