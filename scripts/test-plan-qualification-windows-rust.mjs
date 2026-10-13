import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));

test("Windows Rust tests run independently without changing workspace or ignored-test selection", () => {
  const matrix = lanes;
  const rust = matrix.find(lane => lane.lane === "windows-rust");
  assert.equal(rust.runner, "blacksmith-32vcpu-windows-2025");
  assert.equal(rust.workers, 32);
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(rust.runner));
  assert.equal(rust.artifact, "");
  assert.equal(matrix.find(lane => lane.lane === "windows").artifact, "stream-native-windows");
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  const body = script.split("    'windows-rust' {")[1]?.split("    'windows-fork-join' {")[0] ?? assert.fail("missing Rust dispatch");
  assert.ok(body.includes("cargo test --workspace --all-features --locked"));
  assert.doesNotMatch(body, /fork_join/);
  const fork = matrix.find(lane => lane.lane === "windows-fork-join") ?? assert.fail("missing fork/join lane");
  assert.equal(fork.runner, "blacksmith-16vcpu-windows-2025");
  assert.equal(fork.workers, 16);
  const forkBody = script.split("    'windows-fork-join' {")[1]?.split("    'windows-napi' {")[0] ?? assert.fail("missing fork/join dispatch");
  assert.ok(forkBody.includes("cargo test -p acyclic-plugin --all-features --locked --test fork_join --"));
  assert.ok(forkBody.includes("--ignored --test-threads=1 --skip support::"));
  assert.ok(body.includes("Remove-Item Env:RUSTC_WRAPPER"));
  assert.ok(body.includes("finally"));
  const packages = script.slice(script.indexOf("& .\\scripts\\test-qualify-ci-windows.ps1"));
  assert.doesNotMatch(packages, /cargo test/);
  assert.ok(packages.includes("bun test --parallel=4"));
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("max-parallel: 18"));
  const full = lanes;
  assert.ok(full.length <= 18);
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  assert.ok(action.includes("inputs.lane == 'windows-rust'"));
  const projfs = action.slice(action.indexOf("    - name: Enable Windows ProjFS"), action.indexOf("    - name: Run qualification lane"));
  assert.ok(projfs.includes("if: runner.os == 'Windows'"));
});

test("Windows Rust restores dependency builds after toolchain setup and skips unused sccache", () => {
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  const cache = action.slice(action.indexOf("    - name: Restore Windows Rust dependency builds"), action.indexOf("    - name: Enable Windows ProjFS"));
  const finishSetup = action.indexOf("    - name: Finish Windows Rust setup");
  const restoreBuilds = action.indexOf("    - name: Restore Windows Rust dependency builds");
  assert.ok(finishSetup >= 0 && restoreBuilds > finishSetup);
  assert.ok(cache.includes("if: inputs.lane == 'windows-rust'"));
  assert.ok(cache.includes("Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6"));
  assert.ok(cache.includes("shared-key: sdk-windows-rust-v1"));
  assert.ok(cache.includes("workspaces: . -> target"));
  assert.ok(cache.includes("CARGO_TARGET_DIR: ${{ github.workspace }}/target"));
  const windowsLane = action.split("    - name: Run Windows qualification lane")[1]?.split("    - name:")[0] ?? assert.fail("missing Windows dispatch");
  assert.ok(windowsLane.includes("CARGO_TARGET_DIR: ${{ inputs.lane == 'windows-rust' && format('{0}/target', github.workspace) || format('{0}/target', runner.temp) }}"));
  assert.ok(cache.includes("RUSTC_WRAPPER: ''"));
  assert.ok(cache.includes("cache-bin: false"));
  assert.ok(cache.includes("save-if: ${{ github.ref == 'refs/heads/main' && github.event_name != 'pull_request' }}"));
  assert.doesNotMatch(cache, /add-rust-environment-hash-key: false|cache-targets: false|cache-workspace-crates: true/);
  for (const step of ["Restore lane build cache", "Enable compiler cache"]) {
    const body = action.split("    - name: " + step)[1]?.split("    - name:")[0] ?? assert.fail("missing cache step");
    assert.ok(body.includes("if: inputs.lane != 'windows-rust'"));
  }
  const save = action.split("    - name: Save lane build cache")[1]?.split("    - name:")[0] ?? assert.fail("missing cache save");
  assert.ok(save.includes("inputs.lane != 'windows-rust'"));
  assert.ok(workflow.includes("RUSTC_WRAPPER: ${{ matrix.lane != 'windows-rust' && 'sccache' || '' }}"));
  assert.ok(action.includes('if [[ "$LANE" != windows-rust ]]; then'));
});
