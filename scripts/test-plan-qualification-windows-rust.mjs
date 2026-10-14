import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));

test("Windows setup forwards the lane to every phase and retains its detached join", () => {
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  for (const phase of ["start", "dependencies", "finish"])
    assert.ok(action.includes(`windows-rust-setup.ps1 -Phase ${phase} -Lane '\${{ inputs.lane }}'`));
  const setup = readFileSync("scripts/windows-rust-setup.ps1", "utf8");
  assert.ok(setup.includes("switch ($Lane)"));
  assert.ok(setup.includes("Invoke-CimMethod -ClassName Win32_Process -MethodName Create"));
  assert.ok(setup.includes("Invoke-Expression $install"));
  assert.ok(setup.includes('cargo fetch --locked --offline'));
});

test("Windows Rust tests run independently without changing workspace or ignored-test selection", () => {
  const matrix = lanes;
  const rust = matrix.find(lane => lane.lane === "windows-rust");
  assert.equal(rust.runner, "blacksmith-16vcpu-windows-2025");
  assert.equal(rust.workers, 16);
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(rust.runner));
  assert.equal(rust.artifact, "");
  assert.equal(matrix.find(lane => lane.lane === "windows").artifact, "stream-native-windows");
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  const body = script.split("function Invoke-WindowsRustTests {")[1]?.split("switch ($Lane)")[0] ?? assert.fail("missing Rust dispatch");
  assert.ok(body.includes('cargo nextest run @targetArguments --locked --profile ci --test-threads "$env:RUST_TEST_THREADS" --target-dir "$CargoTargetDir"'));
  assert.ok(body.includes("cargo test --workspace --all-features --locked --doc"));
  assert.doesNotMatch(body, /profile\.test\.package|@testProfile|CARGO_PROFILE_TEST_OPT_LEVEL/);
  assert.ok(body.includes("nextest/ci/junit.xml"));
  assert.ok(body.includes("$Shard -eq 'units'"));
  for (const [lane, shard] of [["windows-rust", "units"], ["windows-rust-scenarios", "scenarios"]]) {
    const row = matrix.find(row => row.lane === lane) ?? assert.fail(lane);
    assert.equal(row.runner, "blacksmith-16vcpu-windows-2025");
    assert.equal(row.workers, 16);
    const caseStart = script.indexOf(`    '${lane}' {`, script.indexOf("switch ($Lane)"));
    const caseEnd = script.indexOf("\n    }", caseStart);
    assert.ok(caseStart >= 0 && caseEnd > caseStart, "missing lane case: " + lane);
    assert.ok(script.slice(caseStart, caseEnd).includes("Invoke-WindowsRustTests " + shard));
  }
  assert.ok(body.includes('Copy-Item -LiteralPath $junit -Destination "$env:SDK_TEMP_DIR/observability/rust.xml"'));
  assert.doesNotMatch(body, /--no-default-features|--exclude|--skip|--ignored|--retries|-E /);
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
  assert.ok(packages.includes("bun test --isolate --reporter=junit"));
  assert.doesNotMatch(packages, /--parallel/);
  assert.ok(packages.includes('--reporter-outfile="$env:SDK_TEMP_DIR/observability/bun-test.xml" typescript/packages'));
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("max-parallel: 19"));
  const full = lanes;
  assert.ok(full.length <= 19);
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  assert.ok(action.includes("startsWith(inputs.lane, 'windows-rust')"));
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
  assert.ok(cache.includes("if: startsWith(inputs.lane, 'windows-rust')"));
  assert.ok(cache.includes("Swatinem/rust-cache@6323deb102c322ba6fcbdcafc7e3dddab59af2b6"));
  assert.ok(cache.includes("shared-key: sdk-windows-rust-v2-${{ inputs.lane }}"));
  assert.ok(cache.includes("workspaces: . -> target"));
  assert.ok(cache.includes("CARGO_TARGET_DIR: ${{ format('{0}/target-{1}', github.workspace, inputs.lane) }}"));
  const windowsLane = action.split("    - name: Run Windows qualification lane")[1]?.split("    - name:")[0] ?? assert.fail("missing Windows dispatch");
  assert.ok(windowsLane.includes("CARGO_TARGET_DIR: ${{ startsWith(inputs.lane, 'windows-rust') && format('{0}/target-{1}', github.workspace, inputs.lane) || format('{0}/target', runner.temp) }}"));
  assert.ok(cache.includes("RUSTC_WRAPPER: ''"));
  assert.ok(cache.includes("cache-bin: false"));
  assert.ok(cache.includes("save-if: ${{ github.ref == 'refs/heads/main' && github.event_name != 'pull_request' && github.event_name != 'pull_request_target' }}"));
  assert.doesNotMatch(cache, /add-rust-environment-hash-key: false|cache-targets: false|cache-workspace-crates: true/);
  for (const step of ["Enable compiler cache"]) {
    const body = action.split("    - name: " + step)[1]?.split("    - name:")[0] ?? assert.fail("missing cache step");
    assert.ok(body.includes("if: ${{ !startsWith(inputs.lane, 'windows-rust') }}"));
  }
  assert.doesNotMatch(action, /Restore lane build cache|Save lane build cache/);
  assert.ok(workflow.includes("RUSTC_WRAPPER: ${{ !startsWith(matrix.lane, 'windows-rust') && 'sccache' || '' }}"));
  assert.ok(action.includes('if [[ "$LANE" != windows-rust* ]]; then'));
});

test("Windows Rust nextest is pinned, verified and cached without changing other lanes", () => {
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  const install = action.split("    - name: Install Windows nextest")[1]?.split("    - name:")[0] ?? assert.fail("missing nextest installation");
  assert.ok(install.includes("if: startsWith(inputs.lane, 'windows-rust')"));
  assert.ok(install.includes("cargo-nextest-0.9.146-x86_64-pc-windows-msvc.tar.gz"));
  assert.ok(install.includes("809b01f2c94d031ab6cf5f4d4a4f477c8f51b69c96ab3cb2fe4db7ecb222d0d3"));
  assert.ok(install.includes("Get-FileHash -LiteralPath $archive -Algorithm SHA256"));
  assert.ok(install.includes("Add-Content -LiteralPath $env:GITHUB_PATH -Value $bin"));
  assert.equal((install.match(/-join "`n"/g) ?? []).length, 2);
  assert.equal((install.match(/-notmatch/g) ?? []).length, 2);
  assert.doesNotMatch(install, /-notlike/);
  assert.ok(action.includes("startsWith(inputs.lane, 'windows-rust') && '-nextest-0.9.146' || ''"));
  assert.doesNotMatch(install, /cargo install|latest|nightly/);
});
