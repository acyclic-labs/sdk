import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { buildContractTargets, contractTargetOutputs, runContractTarget } from "./contract-targets.mjs";

test("contract selection refuses every unknown family before Cargo or publication", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-selection-"));
  try {
    for (const families of [["absent-family"], ["stream", "absent-family"], [""]]) {
      const result = spawnSync(process.execPath, [
        fileURLToPath(new URL("./generate-contracts.mjs", import.meta.url)), "check", ...families,
      ], { cwd: root, encoding: "utf8", env: {
        ...process.env, ACYCLIC_CARGO_BIN: join(root, "cargo-must-not-start"), SDK_TEMP_DIR: root,
      } });
      assert.equal(result.status, 1, result.stderr);
      assert.match(result.stderr, /STEP_FAILED contract-selection: unknown family/);
      assert.match(result.stderr, /recovery: bun scripts\/generate-contracts\.mjs check/);
      assert.equal(result.stdout, "");
      assert.equal(statSync(root).isDirectory(), true);
      assert.throws(() => statSync(join(root, "observability")), { code: "ENOENT" });
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// Exercise real Cargo freshness, feature resolution and executable discovery
// with a dependency-free workspace. No substitute artifact planner is mocked.
test("contract binaries use exact Cargo identities and current source/configuration", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-contract-targets-"));
  const targetOverride = process.env.CARGO_TARGET_DIR;
  delete process.env.CARGO_TARGET_DIR;
  const put = (path, content) => writeFileSync(join(root, path), content);
  const cargo = process.env.ACYCLIC_CARGO_BIN || "cargo";
  const command = args => {
    const result = spawnSync(cargo, args, { cwd: root, encoding: "utf8" });
    assert.equal(result.status, 0, result.error?.message ?? result.stderr);
    return result.stdout;
  };
  try {
    put("Cargo.toml", '[workspace]\nmembers = ["alpha", "beta", "dependency"]\nresolver = "2"\n');
    // Force the artifact directory outside the default root, as CI does.
    mkdirSync(join(root, ".cargo"));
    put(".cargo/config.toml", '[build]\ntarget-dir = "configured-target"\n');
    mkdirSync(join(root, "dependency", "src"), { recursive: true });
    put("dependency/Cargo.toml", '[package]\nname = "dependency"\nversion = "0.1.0"\nedition = "2024"\n[features]\ndefault = ["propagated"]\npropagated = []\n');
    put("dependency/src/lib.rs", 'pub fn value() -> bool { cfg!(feature = "propagated") }\n');
    for (const name of ["alpha", "beta"]) {
      mkdirSync(join(root, name, "src"), { recursive: true });
      put(`${name}/Cargo.toml`, `[package]\nname = "${name}"\nversion = "0.1.0"\nedition = "2024"\n[lib]\ncrate-type = ["rlib", "cdylib"]\n[[${name === "alpha" ? "bin" : "example"}]]\nname = "contract"\npath = "src/main.rs"\nrequired-features = ["contract"]\n[features]\ndefault = ["alternate"]\nalternate = []\ncontract = []\n`);
      put(`${name}/src/lib.rs`, 'pub fn value() -> bool { cfg!(feature = "alternate") }\n');
      put(`${name}/src/main.rs`, `fn main() { println!("${name}:{}:{}", ${name}::value(), dependency::value()); }\n`);
      writeFileSync(join(root, name, "Cargo.toml"), '\n[dependencies]\ndependency = { path = "../dependency" }\n', { flag: "a" });
    }
    command(["generate-lockfile", "--offline"]);
    const targets = [["alpha", "bin", "contract"], ["beta", "example", "contract"]];
    const report = {};
    const build = () => buildContractTargets(root, targets, cargo, report);
    const run = binaries => binaries.map(binary => runContractTarget(root, binary).trim());
    const binaries = build();
    assert.deepEqual(run(binaries), ["alpha:false:true", "beta:false:true"]);
    // --no-default-features disables selected packages' defaults; a dependency
    // keeps its defaults unless its dependency declaration disables them.
    assert.deepEqual(report.artifacts.filter(item => item.requested).map(item => item.features), [["contract"], ["contract"]]);
    assert.ok(report.artifacts.some(item => item.name === "dependency" && item.features.includes("propagated")));
    assert.ok(binaries.every(binary => binary.includes("configured-target")));
    assert.deepEqual(run(buildContractTargets(root, targets, cargo, report, () => {}, ["alpha/alternate"])), ["alpha:true:true", "beta:false:true"]);
    assert.deepEqual(run(build()), ["alpha:false:true", "beta:false:true"]);
    const before = binaries.map(binary => statSync(binary).mtimeMs);
    assert.deepEqual(run(build()), ["alpha:false:true", "beta:false:true"]);
    assert.ok(report.artifacts.every(item => item.fresh));
    assert.deepEqual(binaries.map(binary => statSync(binary).mtimeMs), before);
    // An intervening build of a different feature set cannot substitute its
    // executable output; Cargo must qualify the requested configuration again.
    command(["build", "--workspace", "--all-features", "--locked", "--offline"]);
    assert.deepEqual(run(build()), ["alpha:false:true", "beta:false:true"]);

    // A source edit must invalidate the executable, including library inputs.
    put("alpha/src/main.rs", 'fn main() { println!("changed:{}:{}", alpha::value(), dependency::value()); }\n');
    assert.deepEqual(run(build()), ["changed:false:true", "beta:false:true"]);
    put("dependency/src/lib.rs", "pub fn value() -> bool { false }\n");
    assert.deepEqual(run(build()), ["changed:false:false", "beta:false:false"]);
    put("beta/src/lib.rs", "pub fn value() -> bool { true }\n");
    assert.deepEqual(run(build()), ["changed:false:false", "beta:true:false"]);
    rmSync(binaries[0]);
    assert.deepEqual(run(build()), ["changed:false:false", "beta:true:false"]);
    const goodSource = readFileSync(join(root, "alpha/src/main.rs"));
    put("alpha/src/main.rs", "this is not Rust\n");
    assert.throws(build, /Rust contract command failed/);
    put("alpha/src/main.rs", goodSource);
    assert.deepEqual(run(build()), ["changed:false:false", "beta:true:false"]);

    // Capture output from the original Cargo-selected binaries. Count actual
    // starts and decodes independently of the generator's cache implementation.
    const bundle = '{"left":"exact\\nbytes","right":"other","invalid":7}\n';
    put("alpha/src/main.rs", `use std::io::Write; fn main() {
      std::fs::OpenOptions::new().create(true).append(true).open("starts").unwrap().write_all(b"x").unwrap();
      print!("{}", r#"${bundle}"#);
    }\n`);
    put("beta/src/main.rs", 'fn main() { println!("{{not JSON"); }\n');
    const outputFor = contractTargetOutputs(root, build(), new Set([0]));
    const parse = JSON.parse;
    let decodes = 0;
    JSON.parse = (text, ...args) => {
      if (text === bundle) decodes += 1;
      return parse(text, ...args);
    };
    try {
      assert.equal(outputFor(0, "left"), "exact\nbytes");
      assert.equal(outputFor(0, "right"), "other");
      assert.equal(outputFor(0, "left"), "exact\nbytes");
      assert.equal(outputFor(0), bundle);
      assert.throws(() => outputFor(0, "absent"), /STEP_FAILED contract-payload: missing absent; recovery:/);
      assert.throws(() => outputFor(0, "toString"), /STEP_FAILED contract-payload: missing toString; recovery:/);
      assert.throws(() => outputFor(0, "invalid"), /STEP_FAILED contract-payload: missing invalid; recovery:/);
      assert.throws(() => outputFor(1, "left"), /STEP_FAILED contract-payload: malformed bundle.*recovery:/);
      assert.equal(decodes, 1);
      assert.equal(readFileSync(join(root, "starts"), "utf8"), "x");
    } finally {
      JSON.parse = parse;
    }
    assert.throws(() => buildContractTargets(root, [...targets, targets[0]], cargo), /duplicate Rust contract target/);
    assert.throws(() => buildContractTargets(root, [["alpha", "bin", "absent"]], cargo), /unknown Rust contract target/);
    assert.throws(() => buildContractTargets(root, [["alpha", "lib", "alpha"]], cargo), /unknown Rust contract target/);
  } finally {
    if (targetOverride !== undefined) process.env.CARGO_TARGET_DIR = targetOverride;
    rmSync(root, { recursive: true, force: true });
  }
});
