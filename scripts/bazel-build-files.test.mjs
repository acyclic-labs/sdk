import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";

// A one-member workspace with a local path crate and the repository's own toolchain and config inputs.
// `module` adds the root MODULE.bazel whose resolution a write refreshes.
function fixture({ module = false } = {}) {
  const root = mkdtempSync(join(tmpdir(), "bazel-build-files-"));
  mkdirSync(join(root, "scripts"));
  writeFileSync(join(root, "scripts/bazel-build-files.mjs"), readFileSync("scripts/bazel-build-files.mjs"));
  writeFileSync(join(root, "scripts/bazel-executables.mjs"), readFileSync("scripts/bazel-executables.mjs"));
  writeFileSync(join(root, "scripts/check-bazel-lockfile.mjs"), readFileSync("scripts/check-bazel-lockfile.mjs"));
  writeFileSync(join(root, "scripts/policy-core.mjs"), readFileSync("scripts/policy-core.mjs"));
  writeFileSync(join(root, "scripts/workspace-model.mjs"), readFileSync("scripts/workspace-model.mjs"));
  if (module) writeFileSync(join(root, "MODULE.bazel"), 'module(name = "fixture")\n');
  writeFileSync(join(root, "rust-toolchain.toml"), readFileSync("rust-toolchain.toml"));
  mkdirSync(join(root, ".cargo"));
  writeFileSync(join(root, ".cargo/config.toml"), '[target.x86_64-unknown-linux-gnu]\nlinker = "scripts/mold-linker.sh"\n\n[env]\n');
  writeFileSync(join(root, "scripts/mold-linker.sh"), "#!/bin/sh\n");
  writeFileSync(join(root, "Cargo.toml"), '[workspace]\nmembers = ["member"]\nexclude = ["local"]\nresolver = "3"\n\n[profile.dev]\nopt-level = 1\n\n[profile.dev.package.local]\nopt-level = 3\n');
  mkdirSync(join(root, "member/src"), { recursive: true });
  writeFileSync(join(root, "member/Cargo.toml"), '[package]\nname = "member"\nversion = "0.1.0"\nedition = "2024"\n\n[dependencies]\nlocal = { path = "../local" }\n');
  writeFileSync(join(root, "member/src/lib.rs"), "pub const QUOTE: &str = \"\\\"\";\n\npub fn one() -> u8 {\n    1\n}\n\n#[test]\nfn reads_cargo_config() {\n    assert!(std::path::Path::new(\"member/../.cargo/config.toml\").is_file());\n}\n");
  mkdirSync(join(root, "local/src"), { recursive: true });
  writeFileSync(join(root, "local/Cargo.toml"), '[package]\nname = "local"\nversion = "0.1.0"\nedition = "2024"\n');
  writeFileSync(join(root, "local/src/lib.rs"), "pub fn two() -> u8 {\n    2\n}\n");
  writeFileSync(join(root, "Cargo.lock"), 'version = 4\n\n[[package]]\nname = "local"\nversion = "0.1.0"\n\n[[package]]\nname = "member"\nversion = "0.1.0"\ndependencies = ["local"]\n');
  writeFileSync(join(root, "cargo-bazel.Cargo.lock"), 'version = 4\n\n[[package]]\nname = "direct-cargo-bazel-deps"\nversion = "0.0.1"\n');
  writeFileSync(join(root, "cargo-bazel-lock.json"), '{"crates":{},"conditions":{}}');
  execFileSync("git", ["init", "-q"], { cwd: root });
  execFileSync("git", ["add", "-A"], { cwd: root });
  const run = (mode, env = {}) => spawnSync(process.execPath, ["scripts/bazel-build-files.mjs", mode], { cwd: root, encoding: "utf8", env: { ...process.env, ...env } });
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

test("a patched crate uses crate-universe spokes for its transitive registry dependencies", () => {
  const {root, run} = fixture();
  try {
    writeFileSync(join(root, 'Cargo.toml'), '[workspace]\nmembers=["member"]\n[patch.crates-io]\npatched={path="patched"}\n');
    writeFileSync(join(root, 'member/Cargo.toml'), '[package]\nname="member"\nversion="0.1.0"\n[dependencies]\ntop="1"\n');
    mkdirSync(join(root, 'patched/src'), {recursive:true});
    writeFileSync(join(root, 'patched/Cargo.toml'), '[package]\nname="patched"\nversion="0.1.0"\n[dependencies]\ntransitive="1"\n');
    writeFileSync(join(root, 'patched/src/lib.rs'), '');
    writeFileSync(join(root, 'Cargo.lock'), 'version=4\n[[package]]\nname="member"\nversion="0.1.0"\ndependencies=["top"]\n[[package]]\nname="patched"\nversion="0.1.0"\ndependencies=["transitive"]\n[[package]]\nname="top"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\ndependencies=["patched"]\n[[package]]\nname="transitive"\nversion="1.0.0"\nsource="registry+https://github.com/rust-lang/crates.io-index"\n');
    writeFileSync(join(root, 'cargo-bazel-lock.json'), JSON.stringify({conditions: {}, crates: {
      'top 1.0.0': {targets:[{Library:{crate_name:'top'}}], common_attrs:{deps:[{id:'patched 0.1.0',target:'patched'}]}},
      'transitive 1.0.0': {targets:[{Library:{crate_name:'transitive'}}], common_attrs:{}},
    }}));
    execFileSync('git', ['add', '-A'], {cwd: root});
    const result = run('write');
    assert.equal(result.status, 0, result.stderr);
    assert.match(readFileSync(join(root, 'patched/BUILD.bazel'), 'utf8'), /@sdk_crates__transitive-1\.0\.0\/\/:transitive/);
    assert.match(readFileSync(join(root, 'cargo.MODULE.bazel'), 'utf8'), /"sdk_crates__transitive-1\.0\.0"/);
  } finally { rmSync(root, {recursive: true, force: true}); }
});

test("only a package that declares test-env-inherit gives its tests those host variables", () => {
  const { root, run } = fixture();
  const manifest = join(root, "member/Cargo.toml");
  const undeclared = readFileSync(manifest, "utf8");
  try {
    writeFileSync(manifest, undeclared.replace("\n[dependencies]", '\n[package.metadata.bazel]\ntest-env-inherit = ["SCRATCH"]\ntest-workers = 2\n\n[dependencies]'));
    assert.equal(run("write").status, 0);
    const declared = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    assert.match(declared, /name = "member_test",[^)]*env_inherit = \["SCRATCH"\],/s);
    assert.match(declared, /name = "member_test",[^)]*workers = 2,/s);
    // Negative control: without the declaration no test inherits anything, so no host value keys its action.
    writeFileSync(manifest, undeclared);
    assert.equal(run("write").status, 0);
    assert.doesNotMatch(readFileSync(join(root, "member/BUILD.bazel"), "utf8"), /env_inherit/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("an uncacheable test tag or host test environment is refused", () => {
  const { root, run } = fixture();
  try {
    writeFileSync(join(root, ".bazelrc"), "test --test_env=PATH\n");
    const environment = run("write");
    assert.notEqual(environment.status, 0);
    assert.match(environment.stderr, /STEP_FAILED bazel-build-files: \.bazelrc sets test --test_env=PATH/);
    writeFileSync(join(root, ".bazelrc"), "# test --test_env=PATH\n");
    assert.equal(run("write").status, 0, "a commented-out setting is not a setting");
    rmSync(join(root, ".bazelrc"));
    const script = join(root, "scripts/bazel-build-files.mjs");
    writeFileSync(script, readFileSync(script, "utf8").replace('["tags", q(["manual"])]', '["tags", q(["manual", "external"])]'));
    const tag = run("write");
    assert.notEqual(tag.status, 0);
    assert.match(tag.stderr, /STEP_FAILED bazel-build-files: member\/BUILD\.bazel tags a test external/);
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

// Windows cannot execute a shebang script, so the selected Bazel is this Node executable running a recorder
// that answers only the `mod` invocation: the same fake Bazel runs on every platform.
function fakeBazel(root) {
  const log = join(root, "bazel-args.log");
  const recorder = join(root, "fake-bazel.mjs");
  writeFileSync(recorder, `import { appendFileSync } from "node:fs";
import { basename } from "node:path";
if (basename(process.argv[1]) === "mod" && process.argv[2] === "deps") {
  appendFileSync(process.env.FIXTURE_LOG, JSON.stringify([basename(process.argv[1]), ...process.argv.slice(2)]) + "\\n");
  process.exit(Number(process.env.FIXTURE_STATUS ?? 0));
}
`);
  return {
    log,
    env: {
      ACYCLIC_BAZEL: process.execPath,
      FIXTURE_LOG: log,
      NODE_OPTIONS: [process.env.NODE_OPTIONS, `--import=${pathToFileURL(recorder).href}`].filter(Boolean).join(" "),
    },
  };
}

test("a write refreshes the module lockfile through the selected Bazel, a check never starts Bazel, and a failed refresh fails", () => {
  const { root, run } = fixture({ module: true });
  try {
    const { log, env } = fakeBazel(root);
    const written = run("write", env);
    assert.equal(written.status, 0, written.stderr);
    assert.deepEqual(readFileSync(log, "utf8").trim().split("\n").map(line => JSON.parse(line)), [["mod", "deps", "--lockfile_mode=update"]]);
    // Check mode verifies the checkout and never starts Bazel: its selected command does not exist.
    rmSync(log);
    const checked = run("check", { ...env, ACYCLIC_BAZEL: join(root, "bazel-must-not-start") });
    assert.equal(checked.status, 0, checked.stderr);
    assert.equal(existsSync(log), false, "check mode started Bazel");
    // A lockfile the selected Bazel cannot refresh fails the write instead of passing with a stale one.
    const failed = run("write", { ...env, FIXTURE_STATUS: "7" });
    assert.notEqual(failed.status, 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("the lockfile check runs the selected Bazel in error mode and fails when it refuses", () => {
  const { root } = fixture({ module: true });
  try {
    const { log, env } = fakeBazel(root);
    const check = checkEnv => spawnSync(process.execPath, ["scripts/check-bazel-lockfile.mjs"], { cwd: root, encoding: "utf8", env: { ...process.env, ...checkEnv } });
    const current = check(env);
    assert.equal(current.status, 0, current.stderr);
    assert.deepEqual(JSON.parse(readFileSync(log, "utf8").trim()), ["mod", "deps", "--lockfile_mode=error"]);
    // A stale lockfile makes the selected Bazel refuse, and the check propagates that failure.
    const stale = check({ ...env, FIXTURE_STATUS: "7" });
    assert.notEqual(stale.status, 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
