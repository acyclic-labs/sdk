import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
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
  assert.equal(rust.artifact, "");
  assert.equal(matrix.find(lane => lane.lane === "windows").artifact, "stream-native-windows");
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  const body = script.split("    'windows-rust' {")[1]?.split("    'windows-fork-join' {")[0] ?? assert.fail("missing Rust dispatch");
  assert.ok(body.includes("cargo test @WorkspaceArgs --all-features --locked --doc"));
  assert.doesNotMatch(body, /profile\.test\.package|@testProfile|CARGO_PROFILE_TEST_OPT_LEVEL/);
  assert.ok(body.includes("nextest/ci/junit.xml"));
  assert.ok(body.includes('Copy-Item -LiteralPath $junit -Destination "$env:SDK_TEMP_DIR/observability/rust.xml"'));
  assert.doesNotMatch(body, /--no-default-features|--exclude|--skip|--ignored|--retries|-E /);
  assert.doesNotMatch(body, /fork_join/);
  const fork = matrix.find(lane => lane.lane === "windows-fork-join") ?? assert.fail("missing fork/join lane");
  const forkBody = script.split("    'windows-fork-join' {")[1]?.split("    'windows-napi' {")[0] ?? assert.fail("missing fork/join dispatch");
  assert.ok(forkBody.includes("cargo nextest run -p acyclic-harness --all-features --locked --lib"));
  assert.ok(forkBody.includes("local::host_workspace::tests:: --test-threads 4"));
  assert.doesNotMatch(forkBody, /--ignored|--skip|--no-run|--test fork_join/);
  assert.ok(body.includes("Remove-Item Env:RUSTC_WRAPPER"));
  assert.ok(body.includes("finally"));
  const packages = script.slice(script.indexOf("& .\\scripts\\test-qualify-ci-windows.ps1"));
  assert.ok(packages.includes("bun test --isolate --reporter=junit"));
  assert.doesNotMatch(packages, /--parallel/);
  assert.ok(packages.includes('--reporter-outfile="$env:SDK_TEMP_DIR/observability/bun-test.xml" typescript/packages'));
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
  assert.ok(cache.includes("save-if: ${{ github.ref == 'refs/heads/main' && github.event_name != 'pull_request' && github.event_name != 'pull_request_target' }}"));
  assert.doesNotMatch(cache, /add-rust-environment-hash-key: false|cache-targets: false|cache-workspace-crates: true/);
  for (const step of ["Enable compiler cache"]) {
    const body = action.split("    - name: " + step)[1]?.split("    - name:")[0] ?? assert.fail("missing cache step");
    assert.ok(body.includes("if: inputs.lane != 'windows-rust'"));
  }
  assert.doesNotMatch(action, /Restore lane build cache|Save lane build cache/);
  assert.ok(workflow.includes("RUSTC_WRAPPER: ${{ matrix.lane != 'windows-rust' && 'sccache' || '' }}"));
  assert.ok(action.includes('if [[ "$LANE" != windows-rust ]]; then'));
});

test("every host installs nextest through the one pinned installer", () => {
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  const install = action.split("    - name: Install pinned nextest")[1]?.split("    - name:")[0] ?? assert.fail("missing nextest installation");
  assert.ok(install.includes("if: inputs.lane == 'windows-rust'"));
  assert.ok(install.includes("shell: bash"));
  assert.ok(install.includes('run: bash scripts/ensure-nextest.sh >> "$GITHUB_PATH"'));
  // The installer and the lane share one cached tool directory.
  const toolsDir = "TOOLS_DIR: ${{ runner.temp }}/tools/${{ inputs.lane }}-${{ runner.arch }}";
  assert.ok(install.includes(toolsDir));
  const lane = action.split("    - name: Run Windows qualification lane")[1]?.split("    - name:")[0] ?? assert.fail("missing Windows lane");
  assert.ok(lane.includes(toolsDir));
  assert.ok(action.includes("key: sdk-tools-v7-${{ inputs.lane }}-${{ runner.os }}-${{ runner.arch }}-${{ hashFiles('scripts/ensure-nextest.sh') }}"));
  assert.match(readFileSync("scripts/qualify-ci.sh", "utf8"), /^ensure_nextest\(\) \{\n  bash scripts\/ensure-nextest\.sh >\/dev\/null\n\}$/m);
  const hosts = readFileSync(".github/workflows/agent-host-qualification.yml", "utf8");
  assert.ok(hosts.includes('run: bash scripts/ensure-nextest.sh >> "$GITHUB_PATH"'));
  // No second copy of the pin: only the installer's code names the release assets.
  const release = "nextest-rs/nextest/" + "releases";
  const code = ["*.sh", "*.ps1", "*.psm1", "*.yml", "*.yaml", "*.mjs", "*.js", "*.ts", "*.py", "*.toml", "*.json", "*.bzl", "*.bazel"];
  const holders = spawnSync("git", ["grep", "-l", "-F", release, "--", ...code], { encoding: "utf8" });
  assert.equal(holders.status, 0, holders.stderr);
  assert.deepEqual(holders.stdout.split("\n").filter(Boolean), ["scripts/ensure-nextest.sh"]);
  const installer = readFileSync("scripts/ensure-nextest.sh", "utf8");
  for (const target of ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "universal-apple-darwin", "x86_64-pc-windows-msvc"])
    assert.ok(installer.includes(`target=${target}\n`), target);
  assert.ok(installer.includes("809b01f2c94d031ab6cf5f4d4a4f477c8f51b69c96ab3cb2fe4db7ecb222d0d3"));
  assert.ok(installer.includes('"$PINNED_ASSET_FETCHER" "$expected" "$url"'));
  assert.doesNotMatch(installer, /cargo install|latest|nightly/);
});
