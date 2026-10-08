import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { buildContractTargets, runContractTarget } from "./contract-targets.mjs";

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
      put(`${name}/Cargo.toml`, `[package]\nname = "${name}"\nversion = "0.1.0"\nedition = "2024"\n[lib]\ncrate-type = ["rlib", "cdylib"]\n[[${name === "alpha" ? "bin" : "example"}]]\nname = "contract"\npath = "src/main.rs"\n[features]\ndefault = ["alternate"]\nalternate = []\n`);
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
    assert.deepEqual(report.artifacts.filter(item => item.requested).map(item => item.features), [[], []]);
    assert.ok(report.artifacts.some(item => item.name === "dependency" && item.features.includes("propagated")));
    assert.ok(binaries.every(binary => binary.includes("configured-target")));
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
    assert.throws(() => buildContractTargets(root, [...targets, targets[0]], cargo), /duplicate Rust contract target/);
    assert.throws(() => buildContractTargets(root, [["alpha", "bin", "absent"]], cargo), /unknown Rust contract target/);
    assert.throws(() => buildContractTargets(root, [["alpha", "lib", "alpha"]], cargo), /unknown Rust contract target/);
  } finally {
    if (targetOverride !== undefined) process.env.CARGO_TARGET_DIR = targetOverride;
    rmSync(root, { recursive: true, force: true });
  }
});
