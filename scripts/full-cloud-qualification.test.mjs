import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

test("real nextest selects the cloud capability and refuses an absent producer before executing a case", () => {
  const root = mkdtempSync(path.join(tmpdir(), "sdk-cloud-profile-"));
  const source = path.resolve("scripts/full-cloud-test.sh");
  try {
    mkdirSync(path.join(root, ".config"));
    mkdirSync(path.join(root, "scripts"));
    mkdirSync(path.join(root, "src"));
    writeFileSync(path.join(root, "Cargo.toml"), '[package]\nname="cloud-profile-control"\nversion="0.1.0"\nedition="2024"\n');
    writeFileSync(path.join(root, "src/lib.rs"), `
#[cfg(test)] mod tests {
    #[test] fn portable() {}
    mod full_cloud { #[test] fn requires_topology() {} }
}
`);
    writeFileSync(path.join(root, ".config/nextest.toml"), readFileSync(".config/nextest.toml"));
    symlinkSync(source, path.join(root, "scripts/full-cloud-test.sh"));
    const env = { ...process.env, CARGO_TARGET_DIR: path.join(root, "target"), CARGO_BUILD_RUSTC_WRAPPER: "", RUSTC_WRAPPER: "" };
    delete env.CLOUD_SOURCE;
    delete env.CLOUD_BINARIES;
    const cargo = (...args) => spawnSync("cargo", ["nextest", ...args], { cwd: root, env, encoding: "utf8" });
    const portable = cargo("list", "--offline", "--profile", "ci", "--message-format", "json");
    assert.equal(portable.status, 0, portable.stderr);
    const cases = Object.values(JSON.parse(portable.stdout)["rust-suites"])[0].testcases;
    assert.equal(cases["tests::portable"]["filter-match"].status, "matches");
    assert.notEqual(cases["tests::full_cloud::requires_topology"]["filter-match"].status, "matches");
    const cloud = cargo("run", "--offline", "--profile", "full-cloud", "--no-capture");
    assert.notEqual(cloud.status, 0);
    assert.match(cloud.stderr + cloud.stdout, /STEP_FAILED admit-full-cloud: CLOUD_SOURCE must name an absolute path; recovery:/);
    assert.match(cloud.stderr + cloud.stdout, /requires_topology/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
