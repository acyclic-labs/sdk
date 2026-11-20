import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { parseJsonc } from "./jsonc.mjs";

test("lockfile comments preserve quoted syntax and cannot join adjacent tokens", () => {
  const values = ['https://registry.example/a//b', '/* literal */', 'escaped \\" quote', ',}', ',]'];
  const expected = { packages: values };
  const text = `\uFEFF// lockfile\n{ "packages": [${values.map(JSON.stringify).join(", /* package */\n")},], /* end */ }`;
  assert.deepEqual(parseJsonc(text), expected);
  assert.throws(() => parseJsonc('{"version": 1/* comment */2}'), SyntaxError);
});
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
  writeFileSync(join(root, "scripts/jsonc.mjs"), readFileSync("scripts/jsonc.mjs"));
  writeFileSync(join(root, "scripts/protobuf-bindings.mjs"), readFileSync("scripts/protobuf-bindings.mjs"));
  writeFileSync(join(root, "scripts/generate-stream-token-operations.mjs"), readFileSync("scripts/generate-stream-token-operations.mjs"));
  writeFileSync(join(root, "scripts/workspace-model.mjs"), readFileSync("scripts/workspace-model.mjs"));
  mkdirSync(join(root, "rust/crates/test-runner"), { recursive: true });
  writeFileSync(join(root, "rust/crates/test-runner/privileges.txt"), readFileSync("rust/crates/test-runner/privileges.txt"));
  if (module) writeFileSync(join(root, "MODULE.bazel"), 'module(name = "fixture")\n');
  writeFileSync(join(root, "rust-toolchain.toml"), readFileSync("rust-toolchain.toml"));
  for (const source of ["rustfmt.toml", "windows_paths.bzl", "xwin_repo.bzl"]) writeFileSync(join(root, source), readFileSync(source));
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

test("another workspace names this one's libraries by [package.metadata.sdk] and keeps its BUILD.rules.bazel", () => {
  const generator = fixture(), other = fixture();
  try {
    const manifest = join(other.root, "member/Cargo.toml");
    writeFileSync(manifest, readFileSync(manifest, "utf8") + '\n[package.metadata.sdk]\ndeps = ["member"]\n');
    writeFileSync(join(other.root, "member/BUILD.rules.bazel"), 'load("//:closure.bzl", "copied_closure")\n\ncopied_closure(name = "closure")\n');
    const output = join(other.root, "generated.json");
    const generate = () => spawnSync(process.execPath, ["scripts/bazel-build-files.mjs", "--root", other.root,
      "--source-repository", generator.root, "@sdk_source", "write", "--output", output], {cwd: generator.root, encoding: "utf8"});
    const result = generate();
    assert.equal(result.status, 0, result.stderr);
    const build = JSON.parse(readFileSync(output, "utf8"))["member/BUILD.bazel"];
    assert.match(build, /"@sdk_source\/\/member:member"/);
    assert.match(build, /load\("@sdk_source\/\/rust\/crates\/test-runner:isolated_test.bzl"/);
    assert.ok(build.indexOf('load("//:closure.bzl"') < build.indexOf("rust_library("), "rules' loads precede every rule");
    assert.match(build, /copied_closure\(name = "closure"\)\n$/);
    writeFileSync(manifest, readFileSync(manifest, "utf8").replace('deps = ["member"]', 'deps = ["absent"]'));
    const refused = generate();
    assert.equal(refused.status, 1);
    assert.match(refused.stderr, /names absent, which is no library of the --source-repository/);
  } finally {
    rmSync(generator.root, {recursive: true, force: true});
    rmSync(other.root, {recursive: true, force: true});
  }
});
// The wrapper and the generator read one list, so a privilege one accepts the other accepts.
const privileges = readFileSync("rust/crates/test-runner/privileges.txt", "utf8").split("\n").filter(line => line && !line.startsWith("#"));
test("the privileged wrapper accepts exactly the privileges the generator does", { skip: process.platform === "win32" && "the wrapper runs only in POSIX test actions" }, () => {
  const verity = mkdtempSync(join(tmpdir(), "verity-"));
  try {
    // The wrapper checks the names in order and stops at the first unknown, so refusing only the last
    // one shows it accepted every listed one before it.
    const result = spawnSync("sh", ["rust/crates/test-runner/privileged.sh", "true"], { encoding: "utf8",
      env: { PATH: process.env.PATH, ACYCLIC_TEST_VERITY_DIR: verity, ACYCLIC_TEST_PRIVILEGES: [...privileges, "not-a-privilege"].join(" ") } });
    assert.equal(result.status, 1);
    assert.match(result.stderr, /^privileged\.sh: unknown privilege not-a-privilege /m, "every listed privilege is accepted before the unlisted one is refused");
  } finally {
    rmSync(verity, { recursive: true, force: true });
  }
  const { root, run } = fixture();
  try {
    const manifest = join(root, "member/Cargo.toml"), original = readFileSync(manifest, "utf8");
    // One group per privilege in one generation: each is accepted alone.
    const groups = names => names.map(privilege => `\n[[package.metadata.bazel.privileged.member_test]]\nrequires = ["${privilege}"]\ncases = ["reads_cargo_${privilege.replaceAll("-", "_")}"]\n`).join("");
    writeFileSync(manifest, original + groups(privileges));
    const accepted = run("write");
    assert.equal(accepted.status, 0, accepted.stderr);
    writeFileSync(manifest, original + groups(["not-a-privilege"]));
    const refused = run("write");
    assert.equal(refused.status, 1);
    assert.match(refused.stderr, /unknown not-a-privilege/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
test("privileged cases leave the default target for one tagged target per group, and a group naming no test is refused", () => {
  const { root } = fixture();
  try {
    const manifest = join(root, "member/Cargo.toml"), output = join(root, "generated.json");
    writeFileSync(manifest, readFileSync(manifest, "utf8") + '\n[[package.metadata.bazel.privileged.member_test]]\nrequires = ["kvm", "cgroup"]\ncases = ["reads_cargo", "other_case"]\n');
    const generate = () => spawnSync(process.execPath, ["scripts/bazel-build-files.mjs", "write", "--output", output], { cwd: root, encoding: "utf8" });
    const result = generate();
    assert.equal(result.status, 0, result.stderr);
    const build = JSON.parse(readFileSync(output, "utf8"))["member/BUILD.bazel"];
    const target = /isolated_test\(\n    name = "member_test_privileged",\n[\s\S]*?\n\)/.exec(build)?.[0];
    assert.ok(target, build);
    assert.match(target, /only = "reads_cargo\\nother_case"/);
    assert.match(target, /privileges = \["kvm","cgroup"\]/, "isolated_test writes the privileged tags and runs the cases under privileged.sh");
    assert.doesNotMatch(target, /manual/, "a privileged target is never manual, so it is never a hidden skip");
    assert.match(/isolated_test\(\n    name = "member_test",\n[\s\S]*?\n\)/.exec(build)[0], /exclude = \["reads_cargo","other_case"\]/);
    writeFileSync(manifest, readFileSync(manifest, "utf8").replace("privileged.member_test]]", "privileged.absent]]"));
    const refused = generate();
    assert.equal(refused.status, 1);
    assert.match(refused.stderr, /privileged\.absent names no test of the package/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
test("another workspace is refused without the repository that holds this one's crates", () => {
  const generator = fixture(), other = fixture();
  try {
    const result = spawnSync(process.execPath, ["scripts/bazel-build-files.mjs", "--root", other.root, "write"],
      {cwd: generator.root, encoding: "utf8"});
    assert.equal(result.status, 1);
    assert.match(result.stderr, /is another workspace, so its SDK labels need --source-repository/);
    assert.equal(existsSync(join(other.root, "member/BUILD.bazel")), false, "a refused run writes nothing");
  } finally {
    rmSync(generator.root, {recursive: true, force: true});
    rmSync(other.root, {recursive: true, force: true});
  }
});

test("check accepts the generated files and fails on a stale BUILD file", () => {
  const { root, run } = fixture();
  try {
    const workspace = join(root, "Cargo.toml");
    const member = join(root, "member/Cargo.toml");
    const local = join(root, "local/Cargo.toml");
    writeFileSync(workspace, readFileSync(workspace, "utf8") +
      '\n[workspace.lints.rust]\ndead_code = "deny"\nunused = { level = "warn", priority = -1 }\n');
    writeFileSync(member, readFileSync(member, "utf8") + '\n[lints]\nworkspace = true\n');
    writeFileSync(local, readFileSync(local, "utf8") + '\n[lints.rust]\nunsafe_code = "forbid"\n');
    assert.equal(run("write").status, 0);
    execFileSync("git", ["add", "-A"], { cwd: root });
    const clean = run("check");
    assert.equal(clean.status, 0, clean.stderr);
    // The musl platform carries its libc constraint, and its toolchain is registered before the
    // hub so it wins over the gnu toolchain that rules_rust also considers compatible.
    const memberBuild = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    assert.match(memberBuild, /"\/\/:scripts\/mold-linker\.sh"/, "a test that carries Cargo's config also carries the linker it names");
    assert.match(memberBuild, /lint_config = "\/\/:workspace_lints"/);
    assert.doesNotMatch(memberBuild, /extract_cargo_lints/);
    const buildFile = readFileSync(join(root, "BUILD.bazel"), "utf8");
    assert.match(buildFile, /name = "workspace_lints",[\s\S]*?"unused": "warn",[\s\S]*?"dead_code": "deny"/,
      "the workspace group precedes its higher-priority lint");
    const platformStart = buildFile.indexOf('platform(\n    name = "linux_musl",');
    assert.notEqual(platformStart, -1, "linux_musl platform is generated");
    const platformBlock = buildFile.slice(platformStart, buildFile.indexOf("\n)\n", platformStart) + 3);
    assert.match(platformBlock, /":libc_musl"/, "linux_musl platform carries libc_musl");
    const moduleFile = readFileSync(join(root, "cargo.MODULE.bazel"), "utf8");
    const rustfmtBuild = readFileSync(join(root, "toolchains/rustfmt/BUILD.bazel"), "utf8");
    for (const name of ["linux_x86_64", "macos_aarch64", "windows_x86_64"]) {
      assert.match(rustfmtBuild, new RegExp(`name = "${name}"`), `the ${name} rustfmt toolchain is generated`);
    }
    const localBuild = readFileSync(join(root, "local/BUILD.bazel"), "utf8");
    assert.match(localBuild, /"-Copt-level=3"/, "a local path crate carries its dev opt-level in its own BUILD");
    assert.match(localBuild, /extract_cargo_lints\(/);
    assert.match(localBuild, /lint_config = ":lints"/, "the local crate keeps its own lint policy");
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

test("a repository without a toolchain's sources registers no toolchain", () => {
  const { root, run } = fixture();
  try {
    for (const source of ["rustfmt.toml", "windows_paths.bzl", "xwin_repo.bzl"]) rmSync(join(root, source));
    assert.equal(run("write").status, 0);
    const moduleFile = readFileSync(join(root, "cargo.MODULE.bazel"), "utf8");
    assert.doesNotMatch(moduleFile, /toolchains\/(rustfmt|windows)/, "no toolchain the repository cannot resolve is registered");
    assert.doesNotMatch(moduleFile, /xwin_repo\.bzl/, "no repository rule the repository does not hold is loaded");
    assert.match(moduleFile, /@rust_toolchains\/\/:rust_linux_musl__x86_64-unknown-linux-musl__stable/, "the Rust toolchains stay registered");
    assert.equal(existsSync(join(root, "toolchains/rustfmt/BUILD.bazel")), false);
    assert.equal(existsSync(join(root, "toolchains/windows/BUILD.bazel")), false);
    assert.doesNotMatch(readFileSync(join(root, "BUILD.bazel"), "utf8"), /cc_toolchain/, "no C toolchain without its config");
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("the compile environment never carries the per-slot source root", () => {
  const { root, run } = fixture();
  try {
    writeFileSync(join(root, ".cargo/config.toml"),
      '[env]\nACYCLIC_BUILD_SOURCE_ROOT = { value = ".", relative = true, force = true }\nACYCLIC_OTHER = "kept"\n');
    assert.equal(run("write").status, 0);
    const build = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    // A Bazel action's execution root is a per-slot sandbox path: compiling it in re-keys every
    // downstream rlib between slots. Tests resolve the source root at run time instead.
    assert.doesNotMatch(build, /ACYCLIC_BUILD_SOURCE_ROOT/);
    assert.match(build, /"ACYCLIC_OTHER":"kept"/, "an ordinary Cargo environment entry still reaches the compile");
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
    assert.doesNotMatch(readFileSync(join(root, 'BUILD.bazel'), 'utf8'), /node_modules\.bzl/, "a workspace without a WASM runtime needs no SDK staging rule");
    assert.match(readFileSync(join(root, 'patched/BUILD.bazel'), 'utf8'), /@sdk_crates__transitive-1\.0\.0\/\/:transitive/);
    assert.match(readFileSync(join(root, 'cargo.MODULE.bazel'), 'utf8'), /"sdk_crates__transitive-1\.0\.0"/);
  } finally { rmSync(root, {recursive: true, force: true}); }
});

test("compiler memory bookings follow each target's Rust inputs", () => {
  const { root, run } = fixture();
  try {
    mkdirSync(join(root, "member/tests"));
    writeFileSync(join(root, "member/build.rs"), "fn main() {}\n");
    writeFileSync(join(root, "member/tests/main.rs"), "#[test]\nfn sample() {}\n// " + "x".repeat(400_000) + "\n");
    execFileSync("git", ["add", "-A"], { cwd: root });
    const result = run("write");
    assert.equal(result.status, 0, result.stderr);
    const rules = readFileSync(join(root, "member/BUILD.bazel"), "utf8").split(/\n\)\n/);
    const ruleFor = name => rules.find(rule => rule.includes(`    name = "${name}",`));
    assert.match(ruleFor("build_script"), /tags = \["resources:memory:256"\]/);
    assert.match(ruleFor("member"), /tags = \["resources:memory:256"\]/);
    assert.match(ruleFor("member_test_cases"), /tags = \["manual","resources:memory:256"\]/);
    assert.match(ruleFor("main_cases"), /tags = \["manual","resources:memory:512"\]/);
    assert.doesNotMatch(ruleFor("main"), /resources:memory/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
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
    writeFileSync(script, readFileSync(script, "utf8").replace('q(["manual", ...tags])', 'q(["manual", "external", ...tags])'));
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

test("the dev profile's codegen-units reaches Bazel only as rules_rust's setting, which books each compile at its parallelism", () => {
  const { root, run } = fixture();
  try {
    writeFileSync(join(root, "Cargo.toml"), '[workspace]\nmembers = ["member"]\nexclude = ["local"]\nresolver = "3"\n\n[profile.dev]\nopt-level = 1\ncodegen-units = 8\n');
    const refused = run("write");
    assert.notEqual(refused.status, 0);
    assert.ok(refused.stderr.includes('STEP_FAILED bazel-build-files: .bazelrc lacks "common --@rules_rust//rust/settings:codegen_units=8"'), refused.stderr);
    writeFileSync(join(root, ".bazelrc"), "common --@rules_rust//rust/settings:codegen_units=8\n");
    assert.equal(run("write").status, 0);
    // A rustc flag would make each compile run eight code generators while Bazel books it one CPU.
    assert.doesNotMatch(readFileSync(join(root, "cargo.MODULE.bazel"), "utf8"), /codegen-units/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a loom model gets cfg(loom) only from its build script under --config=loom's ACYCLIC_LOOM", () => {
  const { root, run } = fixture();
  try {
    mkdirSync(join(root, "member/tests"));
    writeFileSync(join(root, "member/tests/model.rs"), "#![cfg(loom)]\n");
    const silent = "fn main() {}\n";
    writeFileSync(join(root, "member/build.rs"), silent);
    const noCfg = run("write");
    assert.notEqual(noCfg.status, 0);
    assert.match(noCfg.stderr, /member loom model model has no build script emitting cargo:rustc-cfg=loom/);
    writeFileSync(join(root, "member/build.rs"), 'fn main() {\n    if std::env::var_os("ACYCLIC_LOOM").is_some() && std::env::var_os("CARGO_FEATURE_MODEL_CHECKING").is_some() {\n        println!("cargo:rustc-cfg=loom");\n    }\n}\n');
    const noEnv = run("write");
    assert.notEqual(noEnv.status, 0);
    assert.ok(noEnv.stderr.includes('.bazelrc lacks "build:loom --action_env=ACYCLIC_LOOM=1"'), noEnv.stderr);
    // One line for every package: no per-crate rustc flag names the cfg by hand.
    writeFileSync(join(root, ".bazelrc"), "build:loom --action_env=ACYCLIC_LOOM=1\n");
    assert.equal(run("write").status, 0);
    assert.doesNotMatch(readFileSync(join(root, "member/BUILD.bazel"), "utf8"), /cfg=loom/);
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

test("unit and integration test shards run their own binaries, while integration tests link the library", () => {
  const { root, run } = fixture();
  try {
    const manifest = join(root, "member/Cargo.toml");
    writeFileSync(manifest, readFileSync(manifest, "utf8") +
      '\n[package.metadata.bazel]\nunit-test-shards = 8\nintegration-test-shards = 4\nunit-test-only-sources = ["src/unit.rs"]\n');
    const lib = join(root, "member/src/lib.rs");
    writeFileSync(lib, readFileSync(lib, "utf8") + '\n#[cfg(test)]\nmod unit;\n');
    writeFileSync(join(root, "member/src/unit.rs"), '#[test]\nfn unit_case() {}\n');
    mkdirSync(join(root, "member/tests"));
    writeFileSync(join(root, "member/tests/api.rs"), '#[test]\nfn api_case() { assert_eq!(member::one(), 1); }\n');
    mkdirSync(join(root, ".config"));
    writeFileSync(join(root, ".config/nextest.toml"), '[profile.pg]\ndefault-filter = "test(pg::capability::)"\n');
    execFileSync("git", ["add", "-A"], { cwd: root });
    const generated = run("write");
    assert.equal(generated.status, 0, generated.stderr);
    const build = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    assert.equal(build.match(/crate_root = "src\/lib\.rs"/g)?.length, 2);
    assert.match(build, /rust_test\(\n    name = "member_test_cases",\n[^]*?crate_root = "src\/lib\.rs"/);
    assert.match(build, /name = "member_test",\n    binary = ":member_test_cases",\n[^)]*shard_count = 8,/);
    assert.match(build, /name = "member_test_pg",\n    binary = ":member_test_cases",\n[^)]*only = "pg::capability::"/);
    assert.match(build, /rust_test\(\n    name = "api_cases",\n    crate_root = "tests\/api.rs",\n[^]*?deps = \[\s+":member",/);
    assert.match(build, /name = "api",\n    binary = ":api_cases",\n[^)]*shard_count = 4,/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a library and a binary of the same name get distinct rule names", () => {
  const { root, run } = fixture();
  try {
    writeFileSync(join(root, "member/src/main.rs"), "fn main() {}\n");
    execFileSync("git", ["add", "-A"], { cwd: root });
    const result = run("write");
    assert.equal(result.status, 0, result.stderr);
    const build = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    const names = [...build.matchAll(/^(rust_library|rust_binary)\(\n    name = "([^"]+)",/gm)].map(match => match[2]);
    assert.deepEqual(names.sort(), ["member", "member_lib"]);
    assert.match(build, /rust_library\(\n    name = "member_lib",\n    crate_name = "member",/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("repository-sources gives a build script every file of each package it links, but the skipped directories", () => {
  const { root, run } = fixture();
  try {
    writeFileSync(join(root, "Cargo.toml"), '[workspace]\nmembers = ["member", "local"]\nresolver = "3"\n\n[workspace.metadata.bazel]\nsources-skip-directories = ["target", "node_modules"]\n');
    const manifest = join(root, "member/Cargo.toml");
    writeFileSync(manifest, readFileSync(manifest, "utf8").replace("[dependencies]", 'build = "build.rs"\n\n[package.metadata.bazel]\nrepository-sources = true\n\n[dependencies]'));
    writeFileSync(join(root, "member/build.rs"), "fn main() {}\n");
    execFileSync("git", ["add", "-A"], { cwd: root });
    const result = run("write");
    assert.equal(result.status, 0, result.stderr);
    const member = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    const script = member.slice(member.indexOf("cargo_build_script("), member.indexOf("\n)\n", member.indexOf("cargo_build_script(")));
    assert.match(script, /"\/\/local:sources",\n\s*"\/\/member:sources",/);
    assert.match(readFileSync(join(root, "local/BUILD.bazel"), "utf8"),
      /filegroup\(\n    name = "sources",\n    srcs = glob\(\["\*\*"\], exclude = \["target\/\*\*","\*\*\/target\/\*\*","node_modules\/\*\*","\*\*\/node_modules\/\*\*"\], allow_empty = True\),/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a script a test reads brings its relative import closure, a generated module by its label", () => {
  const { root, run } = fixture();
  try {
    const workspace = join(root, "Cargo.toml");
    writeFileSync(workspace, readFileSync(workspace, "utf8").replace('resolver = "3"\n', 'resolver = "3"\n\n[workspace.metadata.bazel]\ngenerated-files = { "tools/dist/manifest.mjs" = "//bazel:manifest-parser" }\n'));
    mkdirSync(join(root, "tools/lib"), { recursive: true });
    writeFileSync(join(root, "tools/run.mjs"), 'import { steps } from "./lib/steps.mjs";\nawait import("./lib/late.mjs");\nconsole.log(steps);\n');
    writeFileSync(join(root, "tools/lib/steps.mjs"), 'import data from "./data.json" with { type: "json" };\nexport * from "../dist/manifest.mjs";\nexport const steps = data;\n');
    writeFileSync(join(root, "tools/lib/data.json"), "[]\n");
    writeFileSync(join(root, "tools/lib/late.mjs"), "export {};\n");
    writeFileSync(join(root, "tools/unused.mjs"), "export {};\n");
    const lib = join(root, "member/src/lib.rs");
    writeFileSync(lib, readFileSync(lib, "utf8") + '\n#[test]\nfn runs_script() {\n    assert!(std::path::Path::new("tools/run.mjs").is_file());\n}\n');
    execFileSync("git", ["add", "-A"], { cwd: root });
    const result = run("write");
    assert.equal(result.status, 0, result.stderr);
    const build = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    const data = build.slice(build.indexOf("rust_test("));
    for (const label of ["//:tools/run.mjs", "//:tools/lib/steps.mjs", "//:tools/lib/data.json", "//:tools/lib/late.mjs", "//bazel:manifest-parser"]) {
      assert.ok(data.includes(`"${label}"`), `${label} in ${data}`);
    }
    assert.doesNotMatch(data, /unused\.mjs|tools\/dist/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a #[path] or mod inside an inline module resolves under that module's directory", () => {
  const { root, run } = fixture();
  try {
    mkdirSync(join(root, "qualification"));
    writeFileSync(join(root, "qualification/telemetry.rs"), "pub fn sample() {}\n");
    mkdirSync(join(root, "member/src/tests"));
    writeFileSync(join(root, "member/src/tests/support.rs"), "pub fn help() {}\n");
    // The kvm shape: from src/lib.rs, `mod tests { ... }` is the src/tests/ directory.
    writeFileSync(join(root, "member/src/lib.rs"), readFileSync(join(root, "member/src/lib.rs"), "utf8") +
      '\n#[cfg(test)]\nmod tests {\n    #[path = "../../../qualification/telemetry.rs"]\n    mod telemetry;\n    mod support;\n}\n');
    execFileSync("git", ["add", "-A"], { cwd: root });
    const result = run("write");
    assert.equal(result.status, 0, result.stderr);
    assert.match(readFileSync(join(root, "member/BUILD.bazel"), "utf8"), /"\/\/:qualification\/telemetry.rs"/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("the loom split walks each listed package a workspace holds, and a workspace holding none of them (Cloud's) generates", () => {
  const generator = fixture(), other = fixture();
  try {
    const stream = join(generator.root, "rust/crates/stream");
    mkdirSync(join(stream, "src"), { recursive: true });
    writeFileSync(join(stream, "Cargo.toml"), '[package]\nname = "stream"\nversion = "0.1.0"\nedition = "2024"\n');
    writeFileSync(join(stream, "src/lib.rs"), "#[cfg(loom)]\npub fn model(value: Option<u8>) -> u8 {\n    value.unwrap()\n}\n");
    const workspace = join(generator.root, "Cargo.toml");
    writeFileSync(workspace, readFileSync(workspace, "utf8").replace('members = ["member"]', 'members = ["member", "rust/crates/stream"]'));
    writeFileSync(join(generator.root, "Cargo.lock"), readFileSync(join(generator.root, "Cargo.lock"), "utf8") + '\n[[package]]\nname = "stream"\nversion = "0.1.0"\n');
    execFileSync("git", ["add", "-A"], { cwd: generator.root });
    const generate = (...root) => spawnSync(process.execPath, ["scripts/bazel-build-files.mjs", ...root, "write", "--output", join(generator.root, "generated.json")],
      { cwd: generator.root, encoding: "utf8" });
    const refused = generate();
    assert.equal(refused.status, 1);
    assert.match(refused.stderr, /loom split: rust\/crates\/stream\/src\/lib\.rs:3 \.unwrap\(\) is Loom code the stream library build reaches/);
    writeFileSync(join(stream, "src/lib.rs"), readFileSync(join(stream, "src/lib.rs"), "utf8").replace("#[cfg(loom)]", "#[cfg(all(loom, test))]"));
    const split = generate();
    assert.equal(split.status, 0, split.stderr);
    const cloud = generate("--root", other.root, "--source-repository", generator.root, "@sdk_source");
    assert.equal(cloud.status, 0, cloud.stderr);
  } finally {
    rmSync(generator.root, { recursive: true, force: true });
    rmSync(other.root, { recursive: true, force: true });
  }
});

test("another workspace's tests carry the source repository files and the linked libraries' reads", () => {
  const generator = fixture(), other = fixture();
  try {
    writeFileSync(join(generator.root, "member/BUILD.bazel"), "");
    mkdirSync(join(generator.root, "member/tests"));
    writeFileSync(join(generator.root, "member/tests/identity.json"), "{}\n");
    execFileSync("git", ["add", "-A"], { cwd: generator.root });
    // `local` is a member and a normal dependency of `member`: its library code reads a root fixture tree
    // and a file of the source repository, and its own tests read a file only they need.
    const workspace = join(other.root, "Cargo.toml");
    writeFileSync(workspace, readFileSync(workspace, "utf8").replace('members = ["member"]\nexclude = ["local"]', 'members = ["member", "local"]'));
    mkdirSync(join(other.root, "fixtures/persisted"), { recursive: true });
    writeFileSync(join(other.root, "fixtures/persisted/v1.json"), "{}\n");
    mkdirSync(join(other.root, "notes"));
    writeFileSync(join(other.root, "notes/only-local.txt"), "\n");
    writeFileSync(join(other.root, "local/src/lib.rs"), 'pub const PERSISTED: &str = "fixtures/persisted";\n\npub fn identity() -> std::path::PathBuf {\n    test_support::source_path("member/tests/identity.json")\n}\n\n#[cfg(test)]\nmod tests {\n    const OWN: &str = "notes/only-local.txt";\n}\n');
    execFileSync("git", ["add", "-A"], { cwd: other.root });
    const output = join(other.root, "generated.json");
    const result = spawnSync(process.execPath, ["scripts/bazel-build-files.mjs", "--root", other.root,
      "--source-repository", generator.root, "@sdk_source", "write", "--output", output], { cwd: generator.root, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    const generated = JSON.parse(readFileSync(output, "utf8"));
    const test = build => build.slice(build.indexOf("rust_test("));
    assert.match(test(generated["member/BUILD.bazel"]), /"\/\/:tree\/fixtures\/persisted",\n\s*"@sdk_source\/\/member:tests\/identity.json",/);
    assert.doesNotMatch(generated["member/BUILD.bazel"], /only-local/);
    assert.match(test(generated["local/BUILD.bazel"]), /"\/\/:notes\/only-local.txt"/);
  } finally {
    rmSync(generator.root, { recursive: true, force: true });
    rmSync(other.root, { recursive: true, force: true });
  }
});

test("the generator refuses multiple integration binaries and repeated roots", () => {
  const { root, run } = fixture();
  const manifest = join(root, "member/Cargo.toml");
  const original = readFileSync(manifest, "utf8");
  try {
    mkdirSync(join(root, "member/tests"));
    for (const name of ["first", "second"]) {
      writeFileSync(join(root, "member/tests", `${name}.rs`), "#[test] fn case() {}\n");
    }
    execFileSync("git", ["add", "-A"], { cwd: root });
    const multiple = run("write");
    assert.equal(multiple.status, 1);
    assert.match(multiple.stderr, /member has multiple integration test roots/);
    writeFileSync(manifest, original.replace("[package]\n", "[package]\nautotests = false\n") +
      '\n[[test]]\nname = "second_library"\npath = "src/lib.rs"\n');
    const repeated = run("write");
    assert.equal(repeated.status, 1);
    assert.match(repeated.stderr, /member target second_library uses the library as its crate root/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("unit-test-only sources compile only in the one test-config target, and linked unit tests join its suite, without it when one includes the library", () => {
  const { root, run } = fixture();
  try {
    const manifest = join(root, "member/Cargo.toml");
    writeFileSync(manifest, readFileSync(manifest, "utf8") +
      '\n[package.metadata.bazel]\nunit-test-only-sources = ["src/unit/**"]\nlinked-unit-tests = ["linked"]\n');
    const lib = join(root, "member/src/lib.rs");
    writeFileSync(lib, readFileSync(lib, "utf8") + [
      '',
      '#[cfg(test)]',
      '#[path = "unit/first.rs"]',
      'mod first;',
      '#[cfg(test)]',
      '#[path = "unit/second.rs"]',
      'mod second;',
      '',
    ].join('\n'));
    mkdirSync(join(root, "member/src/unit"));
    for (const area of ["first", "second"]) {
      writeFileSync(join(root, `member/src/unit/${area}.rs`), '#[test]\nfn unit_case() {}\n');
    }
    mkdirSync(join(root, "member/tests"));
    for (const name of ["api", "linked"]) {
      writeFileSync(join(root, `member/tests/${name}.rs`), '#[test]\nfn linked_case() { assert_eq!(member::one(), 1); }\n');
    }
    execFileSync("git", ["add", "-A"], { cwd: root });
    const generated = run("write");
    assert.equal(generated.status, 0, generated.stderr);
    const build = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    const rule = name => {
      const start = build.indexOf(`name = "${name}",`);
      return build.slice(start, build.indexOf("\n)\n", start));
    };
    // The library never compiles the unit-test-only sources; the one test-config compile does.
    assert.match(rule("member"), /exclude = \[[^\]]*"src\/unit\/\*\*"/);
    const unit = rule("member_test_internal_cases");
    assert.match(unit, /crate_root = "src\/lib\.rs"/);
    assert.doesNotMatch(unit, /"src\/unit\/\*\*"/);
    // One test-config compile, not one whole-crate compile per area.
    assert.deepEqual(build.match(/name = "member_test_[a-z]+_cases"/g), ['name = "member_test_internal_cases"']);
    const linked = rule("linked_cases");
    assert.match(linked, /srcs = \[\s+"tests\/linked\.rs",\s+\]/);
    assert.match(linked, /deps = \[\s+":member",/);
    assert.match(build, /name = "linked",\s+binary = ":linked_cases",\s+id = "member",/);
    assert.match(build, /test_suite\(\s+name = "member_test",\s+tests = \[\s+":member_test_internal",\s+":linked",\s*\]/);
    // A linked target that includes the library root runs the library's cases with the package binaries,
    // so no library test target reruns them, and the library needs no test = true.
    writeFileSync(join(root, "member/tests/linked.rs"), 'include!("../src/lib.rs");\n');
    writeFileSync(manifest, readFileSync(manifest, "utf8").replace("\n[package.metadata.bazel]\n", '\n[lib]\ntest = false\n\n[package.metadata.bazel]\n'));
    const included = run("write");
    assert.equal(included.status, 0, included.stderr);
    const includedBuild = readFileSync(join(root, "member/BUILD.bazel"), "utf8");
    assert.doesNotMatch(includedBuild, /member_test_internal/);
    assert.match(includedBuild, /test_suite\(\s+name = "member_test",\s+tests = \[\s+":linked",\s*\]/);
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
