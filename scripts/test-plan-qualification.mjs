import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import test from "node:test";
import { posix } from "node:path";
import { runInNewContext } from "node:vm";
import YAML from "./vendor/yaml.cjs";
import { compatibilityArtifacts, packagedSourceCopies } from "./generated-bindings.mjs";
import { languageGeneratorBackends, parseGeneratorBackends } from "./plan-qualification.mjs";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));

test("browser catalog target proof precedes dependency qualification", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const command = "python3 scripts/test-provider-catalog-targets.py";
  const check = source => {
    const web = source.match(/^  web\)\n([\s\S]*?)(?=^  \S[^\n]*\)\n|^esac\b)/m)?.[1];
    assert.ok(web, "missing browser qualification");
    const target = web.indexOf("bash scripts/ensure-rust-target.sh wasm32-unknown-unknown");
    const proof = web.indexOf(command);
    const dependencies = web.indexOf("cargo clippy");
    assert.ok(target >= 0 && proof > target && dependencies > proof,
      "bounded catalog target proof must run before dependency qualification");
  };
  check(script);
  assert.throws(() => check(script.replace(command, "")), /bounded catalog target proof/);
});

test("release selection executes the complete registry without Git or GitHub reuse", () => {
  assert.equal(new Set(lanes.map(lane => lane.lane)).size, lanes.length);
  assert.ok(lanes.every(lane => lane.lane !== "typescript"));
  const result = spawnSync(process.execPath, ["scripts/plan-qualification.mjs", "select"], {
    encoding: "utf8", env: { ...process.env, GITHUB_OUTPUT: "", PATH: "" },
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /^matrix=/);
  assert.deepEqual(JSON.parse(result.stdout.trim().slice("matrix=".length)), lanes);
});

test("packaged conformance copies cover the complete vector inventory", () => {
  const prefix = "rust/crates/conformance/";
  const actual = new Set(packagedSourceCopies.flat().filter(path => path.startsWith(prefix)));
  const expected = new Set(readdirSync(prefix + "vectors", { recursive: true, withFileTypes: true })
    .filter(entry => entry.isFile()).map(entry => (entry.parentPath + "/" + entry.name).replaceAll("\\", "/")));
  assert.deepEqual(actual, expected);
});

test("the actual generated-copy guard rejects a mutation of every packaged conformance vector", () => {
  const source = readFileSync("scripts/check-baselines.mjs", "utf8");
  const start = source.indexOf("for (const [source, packaged] of packagedSourceCopies) {");
  const end = source.indexOf("const harnessSuite", start);
  assert.ok(start >= 0 && end > start, "the maintained copy guard must remain identifiable");
  const guard = source.slice(start, end);
  const files = new Map(packagedSourceCopies.flat().map(path => [path, readFileSync(path)]));
  const check = () => runInNewContext(guard, { packagedSourceCopies, readFileSync: path => files.get(path), join: posix.join, root: "." });
  assert.doesNotThrow(check);
  for (const [, packaged] of packagedSourceCopies.filter(([, path]) => path.startsWith("rust/crates/conformance/"))) {
    const original = files.get(packaged);
    assert.ok(original, packaged);
    files.set(packaged, Buffer.concat([original, Buffer.from(" ")]));
    assert.throws(check, { message: `packaged source drift: ${packaged}` });
    files.set(packaged, original);
  }
  assert.doesNotThrow(check);
});

test("generator registry rejects empty, duplicate and unsafe backend coordinates", () => {
  const entry = { name: "go" };
  for (const value of [[], [entry, entry], ["go"], [{ ...entry, name: "../outside" }], [{ ...entry, name: "go;echo" }], [null], {},
    [{ ...entry, name: "Go" }], [{ ...entry, shared: ["archive"] }],
    [{ ...entry, unexpected: true }]]) assert.throws(() => parseGeneratorBackends(value));
});

test("release generator staging runs every registered backend without affected-file selection", () => {
  const workflow = YAML.parse(readFileSync(".github/workflows/sdk-generator.yml", "utf8"));
  const steps = workflow.jobs.go.steps;
  assert.ok(languageGeneratorBackends.includes("go"));
  const setupGo = steps.find(step => step.uses?.startsWith("actions/setup-go@")) ?? assert.fail("missing Go setup");
  const goStaging = steps.find(step => step.name === "Check Go formatting and staging") ?? assert.fail("missing Go staging");
  assert.equal(setupGo.if, undefined);
  assert.equal(goStaging.if, undefined);
  const staging = steps.find(step => step.name === "Check backend staging") ?? assert.fail("missing backend staging");
  assert.equal(staging.if, undefined);
  const code = staging.run.split("<<'JS'\n")[1]?.split("\nJS")[0] ?? assert.fail("missing staging script");
  let invocation;
  runInNewContext(code.replace(/^import .*;\n/gm, ""), {
    languageGeneratorBackends, readdirSync, process: { execPath: process.execPath },
    execFileSync: (command, args) => { assert.equal(command, process.execPath); invocation = Array.from(args); },
  });
  const expected = languageGeneratorBackends.filter(name => name !== "go").flatMap(name => {
    const root = `tools/sdk-generator/backends/${name}/tests`;
    return readdirSync(root).filter(file => file.endsWith(".test.mjs")).sort().map(file => `${root}/${file}`);
  });
  assert.deepEqual(invocation, ["--test", "--test-concurrency=1", "tools/sdk-generator/shared/tests/authority.test.mjs", ...expected]);
});

test("registered backend targets match the maintained transport contracts", () => {
  const expected = ["actors", "stream"].map(name =>
    compatibilityArtifacts[name].schemaDigest.split("proto/").at(-1)).sort();
  for (const backend of languageGeneratorBackends) {
    const root = `tools/sdk-generator/backends/${backend}/src`;
    const declarations = readdirSync(root).filter(name => name.endsWith(".mjs")).flatMap(name =>
      [...readFileSync(`${root}/${name}`, "utf8").matchAll(/\bconst targets\s*=\s*(\[[^;]+\]);/g)]);
    assert.ok(declarations.length, `${backend} must declare its qualified targets`);
    for (const [, targets] of declarations) assert.deepEqual(JSON.parse(targets).sort(), expected, backend);
  }
});
test("Windows qualification native builders exist in the maintained source", () => {
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  const loop = script.match(/foreach \(\$family in @\(([^)]+)\)\) \{[\s\S]*?\n\}/);
  assert.ok(loop, "Windows native build loop is required");
  const families = [...loop[1].matchAll(/'([^']+)'/g)].map(([, family]) => family);
  assert.ok(families.length > 0);
  for (const family of families) {
    assert.doesNotThrow(() => readFileSync(`scripts/build-${family}-native.mjs`, "utf8"),
      `Windows qualification invokes a missing ${family} native builder`);
  }
});

test("release lanes cover the Rust workspace, docs crate, and TypeScript workspace", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  assert.match(script, /^ +nextest --workspace --all-features --locked$/m);
  assert.match(script, /cargo test --manifest-path rust\/crates\/sdk-docs\/Cargo\.toml --locked/);
  assert.match(script, /cargo test --manifest-path rust\/crates\/sdk-generation\/Cargo\.toml --locked/);
  assert.match(script, /node --test rust\/crates\/sdk-generation\/scripts\/test-qualify-typescript-snippets\.mjs/);
  const policy = script.match(/^  policy\)\n([\s\S]*?)(?=^  \S[^\n]*\)\n|^esac\b)/m)?.[1] ?? assert.fail("missing policy lane");
  assert.match(policy, /^    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings$/m);
  assert.doesNotMatch(policy, /full_qualification|exit 0/);
  const linux = script.match(/^  linux\)\n([\s\S]*?)(?=^  \S[^\n]*\)\n|^esac\b)/m)?.[1];
  assert.ok(linux, "missing Linux package lane");
  const testCommand = linux.search(/^ +bun run test$/m);
  const generatedCommand = linux.search(/^ +bun run check:baselines \|\| generated_status=\$\?$/m);
  assert.ok(testCommand >= 0, "Linux package lane must run tests");
  assert.ok(generatedCommand > testCommand, "Linux package generated check must follow tests");
});

test("ARM native binding production runs independently and all original ARM checks remain required",()=>{
 const script=readFileSync("scripts/qualify-ci.sh","utf8").replaceAll("\r\n","\n");
 const branch=name=>script.match(new RegExp("^  "+name+"\\)\\n([\\s\\S]*?)(?=^  \\S[^\\n]*\\)\\n|^esac\\b)","m"))?.[1] ?? assert.fail(`missing qualification branch: ${name}`);
 const tests=branch("linux-arm64"),bindings=branch("linux-arm64-bindings");
 assert.match(tests,/nextest --workspace --all-features --locked/);
 assert.match(tests,/cargo test --workspace --all-features --locked --doc/);
 assert.match(tests,/native_mount_tests/);
 assert.doesNotMatch(tests,/native_binding|finish napi/);
 assert.match(bindings,/cargo fetch --locked/);assert.match(bindings,/native_binding/);
 for(const body of [tests,bindings])assert.match(body,/libfuse3-dev unzip/);
 const matrix=lanes;
 const producer=matrix.find(lane=>lane.lane==="linux-arm64-bindings");
 assert.equal(producer.artifact,"packages-linux-arm64");assert.equal(producer.artifact_path,"packages");
 assert.equal(producer.runner,"blacksmith-4vcpu-ubuntu-2404-arm");
 assert.equal(matrix.filter(lane=>lane.artifact==="packages-linux-arm64").length,1);
 assert.equal(matrix.find(lane=>lane.lane==="linux-arm64").artifact,"");
 const workflow=readFileSync(".github/workflows/qualification.yml","utf8");
 assert.match(workflow,/needs: \[ plan, qualification \]/);assert.match(workflow,/test "\$RESULT" = success/);
 assert.ok(matrix.length<=18,"all ARM work fits in the simultaneous matrix");
});

test("the workspace gate, docs and generation run as independent required matrix lanes",()=>{
 const script=readFileSync("scripts/qualify-ci.sh","utf8").replaceAll("\r\n","\n");
 const branch=name=>script.match(new RegExp("^  "+name+"\\)\\n([\\s\\S]*?)(?=^  \\S[^\\n]*\\)\\n|^esac\\b)","m"))?.[1] ?? assert.fail(`missing qualification branch: ${name}`);
 assert.match(branch("gate"),/nextest --workspace --all-features --locked/);
 assert.match(branch("gate"),/cargo check --locked -p acyclic-harness -p acyclic-harness-core --features filesystem --lib --tests/);
 assert.doesNotMatch(branch("gate"),/sdk-docs|sdk-generation/);
 assert.match(branch("docs"),/cargo test --manifest-path rust\/crates\/sdk-docs\/Cargo.toml --locked/);
 assert.match(branch("generation"),/cargo test --manifest-path rust\/crates\/sdk-generation\/Cargo.toml --locked/);
 assert.match(branch("generation"),/node --test rust\/crates\/sdk-generation\/scripts\/test-qualify-typescript-snippets.mjs/);
 const matrix=lanes;
 for(const name of ["gate","docs","generation"])assert.equal(matrix.filter(lane=>lane.lane===name).length,1);
 const workflow=readFileSync(".github/workflows/qualification.yml","utf8");
 assert.match(workflow,/max-parallel: 18/);
 assert.ok(matrix.length<=18,"all full lanes can start together");
 assert.match(workflow,/needs: \[ plan, qualification \]/);
 assert.match(workflow,/test "\$RESULT" = success/);
});

test("Windows native audits are independent required lanes without moving publication outputs", () => {
  const matrix = lanes;
  const names = ["windows-napi", "windows-arm64", "windows-clippy"];
  for (const name of names) {
    const selected = matrix.filter(lane => lane.lane === name);
    assert.equal(selected.length, 1);
    assert.equal(selected[0].runner, name === "windows-clippy" ? "blacksmith-16vcpu-windows-2025" : "blacksmith-4vcpu-windows-2025");
    assert.equal(selected[0].artifact, "");
  }
  assert.equal(matrix.find(lane => lane.lane === "windows").artifact, "stream-native-windows");
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  assert.doesNotMatch(script, /Start-Background|Complete-Background/);
  for (const command of ["cargo test --workspace --all-features --locked", "cargo test -p acyclic-plugin --all-features --locked --test fork_join",
    "bun test --isolate --reporter=junit", "bun run --filter '@acyclic-labs/fs' test:composition", "node scripts/build-wasm-groups.mjs", "bun x tsc -b --force --pretty false", "bun x tsc -p scripts --pretty false", "bun run check:docs"])
    assert.ok(script.includes(command), command);
  const packages = script.slice(script.indexOf("& .\\scripts\\test-qualify-ci-windows.ps1")).replaceAll("\r\n", "\n");
  assert.ok(packages.includes('--reporter-outfile="$env:SDK_TEMP_DIR/observability/bun-test.xml" typescript/packages'));
  assert.doesNotMatch(packages, /--parallel/);
  assert.match(script, /test-qualify-ci-windows\.ps1/);
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.match(workflow, /max-parallel: 18/);
  assert.ok(matrix.length <= 18);
  assert.match(workflow, /test "\$RESULT" = success/);
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  assert.ok(action.includes('qualify-ci.ps1 -Lane "${{ inputs.lane }}"'));
  for (const name of ["Find Visual Studio clang", "Restore Windows ARM64 compiler", "Install Windows ARM64 compiler"]) {
    const begin = action.indexOf(`    - name: ${name}`);
    const step = action.slice(begin, action.indexOf("\n    - name:", begin + 1));
    assert.match(step, /inputs\.lane == 'windows' \|\| inputs\.lane == 'windows-arm64'/);
  }
});


test("ARM workspace compilation uses the larger runner's complete CPU allocation", () => {
  const matrix = lanes;
  const arm = matrix.find(lane => lane.lane === "linux-arm64");
  assert.equal(arm.runner, "blacksmith-32vcpu-ubuntu-2404-arm");
  assert.equal(arm.workers, 32);
  assert.equal(arm.artifact, "");
  for (const name of ["linux-arm64-bindings", "linux-arm64-musl"]) {
    const lane = matrix.find(lane => lane.lane === name);
    assert.equal(lane.runner, "blacksmith-4vcpu-ubuntu-2404-arm");
    assert.equal(lane.workers, 4);
  }
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("runs-on: ${{ matrix.runner }}"));
  for (const variable of ["CARGO_BUILD_JOBS", "CMAKE_BUILD_PARALLEL_LEVEL", "RAYON_NUM_THREADS", "RUST_TEST_THREADS"])
    assert.ok(workflow.includes(variable + ": ${{ matrix.workers }}"), variable);
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(arm.runner));
});


test("Windows package and clippy compilation use sixteen CPUs while cross-check runners stay bounded", () => {
  const matrix = lanes;
  const windows = matrix.find(lane => lane.lane === "windows");
  assert.equal(windows.runner, "blacksmith-16vcpu-windows-2025");
  assert.equal(windows.workers, 16);
  assert.equal(windows.artifact, "stream-native-windows");
  assert.equal(windows.artifact_path, "stream-native-bundle");
  const clippy = matrix.find(lane => lane.lane === "windows-clippy") ?? assert.fail("missing Windows clippy lane");
  assert.equal(clippy.runner, windows.runner);
  assert.equal(clippy.workers, 16);
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  const clippyBody = script.split("    'windows-clippy' {")[1]?.split("        return")[0] ?? assert.fail("missing Windows clippy dispatch");
  assert.ok(clippyBody.includes("cargo clippy -p acyclic-plugin --all-targets --all-features --locked -- -D warnings"));
  assert.ok(clippyBody.includes("node scripts/clippy-feature-sets.mjs 4"));
  assert.ok(clippyBody.indexOf("$env:CARGO_BUILD_JOBS = '4'") > clippyBody.indexOf("cargo clippy -p acyclic-plugin"));
  assert.ok(clippyBody.indexOf("$env:CARGO_BUILD_JOBS = '4'") < clippyBody.indexOf("node scripts/clippy-feature-sets.mjs 4"));
  for (const name of ["windows-napi", "windows-arm64"]) {
    const audit = matrix.find(lane => lane.lane === name);
    assert.equal(audit.runner, "blacksmith-4vcpu-windows-2025");
    assert.equal(audit.workers, 4);
  }
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("runs-on: ${{ matrix.runner }}"));
  for (const variable of ["CARGO_BUILD_JOBS", "CMAKE_BUILD_PARALLEL_LEVEL", "RAYON_NUM_THREADS", "RUST_TEST_THREADS"])
    assert.ok(workflow.includes(variable + ": ${{ matrix.workers }}"), variable);
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(windows.runner));
});


test("four clippy workers retain every feature invocation and fail when any worker fails", async () => {
  const source = readFileSync("scripts/clippy-feature-sets.mjs", "utf8").replace('import { spawn } from "node:child_process";', "");
  const execute = async (workers, failFirst = false) => {
    const calls = [];
    let active = 0, maximum = 0;
    const process = { argv: ["node", "script", String(workers)], env: { CARGO_TARGET_DIR: "target", CARGO_BUILD_JOBS: "4" }, stderr: { write() {} }, exitCode: 0 };
    const spawn = (command, args, { env }) => {
      calls.push({ command, args, target: env.CARGO_TARGET_DIR, jobs: env.CARGO_BUILD_JOBS });
      maximum = Math.max(maximum, ++active);
      const status = failFirst && calls.length === 1 ? 7 : 0;
      const child = { stdout: { on() {} }, stderr: { on() {} }, on(event, callback) {
        if (event === "close") queueMicrotask(() => { --active; callback(status); });
        return child;
      } };
      return child;
    };
    await runInNewContext(`(async () => { ${source} })()`, { spawn, process });
    return { calls, maximum, exitCode: process.exitCode };
  };
  const serial = await execute(1);
  const parallel = await execute(4);
  assert.equal(parallel.maximum, 4);
  assert.equal(parallel.calls.length, serial.calls.length);
  assert.deepEqual(parallel.calls.map(call => JSON.stringify(call.args)).sort(), serial.calls.map(call => JSON.stringify(call.args)).sort());
  assert.deepEqual([...new Set(parallel.calls.map(call => call.target))].sort(), ["target", "target-clippy-1", "target-clippy-2", "target-clippy-3"]);
  assert.ok(parallel.calls.every(call => call.command === "cargo" && call.jobs === "4"));
  assert.ok(parallel.calls.some(call => JSON.stringify(call.args) === JSON.stringify([
    "clippy", "-p", "acyclic-harness", "-p", "acyclic-harness-core", "--features", "filesystem", "--all-targets", "--locked", "--", "-D", "warnings",
  ])));
  assert.equal(parallel.exitCode, 0);
  const failed = await execute(4, true);
  assert.equal(failed.exitCode, 7);
  assert.equal(failed.calls.length, 4);
});

test("Linux mount and fork/join qualification overlap packages on an independent required runner", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const branch = name => script.match(new RegExp("^  " + name + "\\)\\n([\\s\\S]*?)(?=^  \\S[^\\n]*\\)\\n|^esac\\b)", "m"))?.[1] ?? assert.fail(`missing qualification branch: ${name}`);
  const runtime = branch("linux-runtime"), packages = branch("linux");
  assert.ok(runtime.includes("cargo fetch --locked"));
  assert.ok(runtime.includes("cargo test -p acyclic-fs --features native-mount --locked --lib --"));
  assert.ok(runtime.includes("--ignored --test-threads=1"));
  assert.ok(runtime.includes("fork_join_conformance"));
  assert.doesNotMatch(packages, /fork_join_conformance|cargo test -p acyclic-fs/);
  for (const name of ["inference", "machines", "harness", "filesystem"])
    assert.ok(packages.includes("check-" + name + "-package.sh"));
  assert.ok(packages.includes("run-harness-conformance.mjs"));
  const matrix = lanes;
  const lane = matrix.find(lane => lane.lane === "linux-runtime");
  assert.equal(lane.runner, "blacksmith-4vcpu-ubuntu-2404");
  assert.equal(lane.workers, 4);
  const full = lanes;
  assert.equal(full.filter(lane => lane.lane === "linux-runtime").length, 1);
  assert.equal(lane.artifact, "");
  assert.equal(matrix.find(lane => lane.lane === "linux").artifact, "packages-linux");
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("include: ${{ fromJSON(needs.plan.outputs.matrix) }}"));
  assert.ok(workflow.includes("lane: ${{ matrix.lane }}"));
  assert.ok(matrix.length <= 18);
  assert.ok(workflow.includes("max-parallel: 18"));
  assert.ok(workflow.includes('test "$RESULT" = success'));
});


test("macOS workspace qualification uses all twelve CPUs without changing package evidence", () => {
  const matrix = lanes;
  const macos = matrix.find(lane => lane.lane === "macos");
  assert.equal(macos.runner, "blacksmith-12vcpu-macos-15");
  assert.equal(macos.workers, 12);
  assert.equal(macos.artifact, "packages-macos");
  assert.equal(macos.artifact_path, "packages");
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  const script = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const macosBody = script.match(/^  macos\)\n([\s\S]*?)(?=^  \*\))/m)?.[1] ?? assert.fail("missing macOS lane");
  const prepare = 'background fork-join-build cargo test -p acyclic-plugin --locked --test fork_join';
  const join = '    finish_macos\n';
  const execute = 'CARGO_TARGET_DIR="$target_dir-fork-join" fork_join_conformance';
  assert.ok(macosBody.includes(prepare));
  assert.ok(macosBody.includes('--no-run --target-dir "$target_dir-fork-join"'));
  for (const marker of [join, execute, "nextest --workspace --all-features --locked", "cargo test --workspace --all-features --locked --doc", "native_mount_tests"])
    assert.ok(macosBody.includes(marker), marker);
  assert.ok(macosBody.includes("finish napi x86_64 fork-join-build"));
  for (const command of ["CARGO_BUILD_JOBS=2 background napi", "CARGO_BUILD_JOBS=1 background x86_64", "CARGO_BUILD_JOBS=1 background fork-join-build", "CARGO_BUILD_JOBS=8 nextest", "CARGO_BUILD_JOBS=8 cargo test --workspace"])
    assert.ok(macosBody.includes(command), command);
  assert.ok(macosBody.indexOf(prepare) < macosBody.indexOf("nextest --workspace --all-features --locked"));
  assert.ok(macosBody.indexOf(join) > macosBody.indexOf("cargo test --workspace --all-features --locked --doc"));
  assert.ok(macosBody.indexOf(execute) > macosBody.indexOf(join));
  assert.ok(macosBody.indexOf(execute) > macosBody.indexOf("native_mount_tests"));
  const fork = script.match(/^fork_join_conformance\(\) \{\n([\s\S]*?)^\}/m)?.[1] ?? assert.fail("missing fork/join selection");
  assert.ok(fork.includes("cargo test -p acyclic-plugin --locked --test fork_join --"));
  assert.ok(fork.includes("--ignored --test-threads=1 --skip support::"));
  assert.doesNotMatch(fork, /--all-features|--no-run/);
  assert.ok(workflow.includes("runs-on: ${{ matrix.runner }}"));
  for (const variable of ["CARGO_BUILD_JOBS", "CMAKE_BUILD_PARALLEL_LEVEL", "RAYON_NUM_THREADS", "RUST_TEST_THREADS"])
    assert.ok(workflow.includes(variable + ": ${{ matrix.workers }}"), variable);
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(macos.runner));
});


test("macOS joins every background build on foreground and background failures", async () => {
  const source = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const helpers = source.slice(source.indexOf("background() {"), source.indexOf("native_binding() {"));
  const macos = source.match(/^  macos\)\n([\s\S]*?)(?=^  \*\))/m)?.[1] ?? assert.fail("missing macOS lane");
  const commands = macos.slice(macos.indexOf("    finish_macos() {"), macos.indexOf("    stream_binding"));
  assert.ok(commands.includes("    finish_macos\n"));
  assert.ok(commands.includes("CARGO_BUILD_JOBS=8 nextest --workspace --all-features --locked"));
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/macos-joins-").replaceAll("\\", "/");
  assert.match(root, /^\.tools\/macos-joins-[A-Za-z0-9]+$/);
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const [workspace, docs, napi, fork] of [[0, 0, 0, 0], [7, 0, 0, 0], [0, 17, 0, 0], [0, 0, 11, 0], [0, 0, 0, 13], [7, 0, 11, 13], [0, 0, 11, 13]]) {
      const fixture = `${root}/${workspace}-${docs}-${napi}-${fork}`;
      mkdirSync(fixture);
      /** @type {Promise<{status: number | null, stdout: string, stderr: string, events: number}>} */
      const run = new Promise((resolve, reject) => {
        const child = spawn(bash, ["-c", `set -euo pipefail
exec 3<&0
fixture='${fixture}'
export SDK_TEMP_DIR="$fixture" CARGO_BUILD_JOBS=12
target_dir="$fixture/target"
installed_graphcoder() { :; }
pinned_pi() { :; }
native_binding() { [[ "$CARGO_BUILD_JOBS" == 2 ]]; touch "$fixture/napi.done"; return ${napi}; }
nextest() { printf 'fixture-workspace-started\\n'; [[ "$CARGO_BUILD_JOBS" == 8 ]]; return ${workspace}; }
cargo() {
  if [[ "$1" == check ]]; then
    [[ "$CARGO_BUILD_JOBS" == 1 ]]; touch "$fixture/x86_64.done"
  elif [[ "$*" == *--no-run* ]]; then
    [[ "$CARGO_BUILD_JOBS" == 1 && "$*" == *"--target-dir $target_dir-fork-join"* ]]
    IFS= read -r event <&3
    [[ "$event" == workspace-started ]]
    touch "$fixture/fork.done"; return ${fork}
  else
    [[ "$CARGO_BUILD_JOBS" == 8 && "$*" == *--doc* ]]; return ${docs}
  fi
}
${helpers}
${commands}
printf 'joined\\n'
`], { stdio: "pipe" });
        const deadlock = setTimeout(() => {
          child.kill("SIGKILL");
          child.stdin.destroy();
          child.stdout.destroy();
          child.stderr.destroy();
          reject(new Error(`macOS join fixture deadlocked: ${fixture}\n${stdout}\n${stderr}`));
        }, 60_000);
        let stdout = "", stderr = "", released = false;
        child.stdout.on("data", chunk => {
          stdout += chunk.toString();
          if (!released && stdout.split(/\r?\n/).includes("fixture-workspace-started")) {
            released = true;
            child.stdin.end("workspace-started\n");
          }
        });
        child.stderr.on("data", chunk => { stderr += chunk.toString(); });
        child.stdin.on("error", reject);
        child.on("error", error => { clearTimeout(deadlock); reject(error); });
        child.on("exit", () => { if (!released) child.stdin.end(); });
        child.on("close", status => {
          clearTimeout(deadlock);
          resolve({ status, stdout, stderr, events: stdout.split(/\r?\n/).filter(line => line === "fixture-workspace-started").length });
        });
      });
      const result = await run;
      assert.equal(result.events, 1, result.stderr);
      assert.equal(result.status, workspace || docs || napi || fork, result.stderr);
      for (const name of ["napi", "x86_64", "fork"])
        assert.ok(readdirSync(fixture).includes(`${name}.done`), `${name} not joined`);
      for (const name of ["napi", "x86_64", "fork-join-build"])
        assert.ok(result.stdout.includes(`::group::${name}`), `${name} result missing`);
      assert.equal(result.stdout.includes("joined"), workspace + docs + napi + fork === 0);
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

test("Linux package qualification uses sixteen workers without changing publication inputs", () => {
  const matrix = lanes;
  const linux = matrix.find(lane => lane.lane === "linux") ?? assert.fail("missing Linux lane");
  assert.equal(linux.runner, "blacksmith-16vcpu-ubuntu-2404");
  assert.equal(linux.workers, 16);
  assert.equal(linux.artifact, "packages-linux");
  assert.equal(linux.artifact_path, "packages");
  const runtime = matrix.find(lane => lane.lane === "linux-runtime") ?? assert.fail("missing runtime lane");
  assert.equal(runtime.runner, "blacksmith-4vcpu-ubuntu-2404");
  assert.equal(runtime.workers, 4);
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("runs-on: ${{ matrix.runner }}"));
  for (const variable of ["CARGO_BUILD_JOBS", "CMAKE_BUILD_PARALLEL_LEVEL", "RAYON_NUM_THREADS", "RUST_TEST_THREADS"])
    assert.ok(workflow.includes(variable + ": ${{ matrix.workers }}"), variable);
  assert.ok(workflow.includes("MAKEFLAGS: -j${{ matrix.workers }}"));
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(linux.runner));
});

test("Linux filesystem packaging overlaps Harness conformance and joins both failures", () => {
  const source = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const start = source.indexOf("    background harness-conformance bun");
  const end = source.indexOf("    bun run licenses", start);
  assert.ok(start > source.indexOf('bash scripts/check-harness-package.sh "$SDK_ARTIFACT_DIR/packages/harness"'));
  assert.ok(end > start);
  const commands = source.slice(start, end);
  const helpers = source.slice(source.indexOf("background() {"), source.indexOf("native_binding() {"));
  assert.ok(helpers.includes("finish() {"));
  const controlledHelpers = helpers.replace("finish() {", 'finish() {\n  touch "$fixture/finish-entered"');
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/linux-packages-").replaceAll("\\", "/");
  assert.match(root, /^\.tools\/linux-packages-[A-Za-z0-9]+$/);
  const bash = process.platform === "win32"
    ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const scenario of [
      { fs: 0, harness: 0, early: false }, { fs: 21, harness: 0, early: false },
      { fs: 0, harness: 13, early: false }, { fs: 21, harness: 13, early: false },
      { fs: 21, harness: 0, early: true },
    ]) {
      const fixture = `${root}/${scenario.fs}-${scenario.harness}-${scenario.early}`;
      mkdirSync(fixture);
      const body = scenario.early ? commands.replace(" || filesystem_status=$?", "") : commands;
      if (scenario.early) assert.notEqual(body, commands);
      const result = spawnSync(bash, ["-c", `set -euo pipefail
fixture='${fixture}'
SDK_TEMP_DIR="$fixture"
SDK_ARTIFACT_DIR="$fixture/artifacts"
fail_fs=${scenario.fs}
fail_harness=${scenario.harness}
parent_pid="$BASHPID"
on_exit() {
  if [[ "$BASHPID" != "$parent_pid" ]]; then return; fi
  for child in filesystem harness; do
    if [[ ! -f "$fixture/$child.done" ]]; then printf 'unfinished-at-exit:%s\\n' "$child"; fi
  done
  touch "$fixture/finish-entered"
}
trap on_exit EXIT
bun() {
  [[ "$#" == 4 && "$1" == scripts/run-harness-conformance.mjs ]]
  [[ "$2" == "$SDK_ARTIFACT_DIR/packages/harness" ]]
  [[ "$3" == "$SDK_ARTIFACT_DIR/packages/harness/runner-report.json" ]]
  [[ "$4" == "$SDK_ARTIFACT_DIR/packages/harness/qualification-receipt.json" ]]
  touch "$fixture/harness-started"
  while [[ ! -f "$fixture/filesystem-started" || ! -f "$fixture/finish-entered" ]]; do sleep 0.01; done
  touch "$fixture/harness.done"
  return "$fail_harness"
}
bash() {
  [[ "$#" == 2 && "$1" == scripts/check-filesystem-package.sh ]]
  [[ "$2" == "$SDK_ARTIFACT_DIR/packages/filesystem" ]]
  touch "$fixture/filesystem-started"
  while [[ ! -f "$fixture/harness-started" ]]; do sleep 0.01; done
  touch "$fixture/filesystem.done"
  return "$fail_fs"
}
${controlledHelpers}
${body}
printf 'joined\\n'
`], { encoding: "utf8", timeout: 10000 });
      assert.equal(result.error, undefined);
      assert.equal(result.status, scenario.early ? scenario.fs : scenario.harness || scenario.fs, result.stderr);
      if (scenario.early) assert.match(result.stdout, /unfinished-at-exit:harness/);
      else assert.doesNotMatch(result.stdout, /unfinished-at-exit:/);
      assert.equal(result.stdout.includes("joined"), scenario.fs === 0 && scenario.harness === 0);
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

test("browser profile cleanup is bounded without changing qualification results", () => {
  const source = readFileSync("typescript/packages/filesystem/test/browser-qualify.mjs", "utf8").replaceAll("\r\n", "\n");
  const start = source.indexOf("  try {\n    rmSync(profile,");
  const chromeExit = source.indexOf("await launched.exited");
  assert.ok(chromeExit > 0 && start > chromeExit);
  const end = source.indexOf("\n}\nprocess.exit", start);
  assert.ok(end > start);
  const cleanup = source.slice(start, end) + source.slice(end + 2);
  for (const failed of [false, true]) for (const busy of [false, true]) {
    let attempts = 0;
    let warning = "";
    let exitCode;
    runInNewContext(cleanup, {
      failed, profile: "private-browser-profile",
      rmSync: (profile, options) => {
        attempts++;
        assert.equal(profile, "private-browser-profile");
        assert.equal(options.recursive, true);
        assert.equal(options.force, true);
        const retries = options.maxRetries ?? 0;
        const delay = options.retryDelay ?? 100;
        assert.ok(delay * retries * (retries + 1) / 2 <= 2000, "best-effort profile cleanup must not hold the CI lane for minutes");
        if (busy) throw new Error("ENOTEMPTY");
      },
      process: { stderr: { write: text => { warning += text; } }, exit: code => { exitCode = code; } },
    });
    assert.equal(attempts, 1);
    assert.equal(exitCode, failed ? 1 : 0);
    if (busy) assert.match(warning, /could not remove browser profile private-browser-profile: ENOTEMPTY/);
    else assert.equal(warning, "");
  }
});

test("web browser commands overlap, finish before shell exit and retain named timings", () => {
  const source = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  const start = source.indexOf("    browser_qualification() {");
  const end = source.indexOf("    ;;", start);
  assert.ok(start > source.indexOf("bun run --filter '@acyclic-labs/harness' build"));
  assert.ok(end > start);
  const commands = source.slice(start, end);
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/web-browsers-").replaceAll("\\", "/");
  assert.match(root, /^\.tools\/web-browsers-[A-Za-z0-9]+$/);
  const bash = process.platform === "win32"
    ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "bash";
  try {
    for (const scenario of [
      { failure: "none", earlyExit: false }, { failure: "fs", earlyExit: false },
      { failure: "mcp", earlyExit: false }, { failure: "client", earlyExit: false },
      { failure: "fs", earlyExit: true },
    ]) {
      const { failure, earlyExit } = scenario;
      const fixture = `${root}/${failure}-${earlyExit}`;
      mkdirSync(fixture);
      const body = earlyExit ? commands.replace(
        'if wait "$browser_pid"; then :; else browser_status=1; fi', 'wait "$browser_pid" || exit 1') : commands;
      const controlledBody = body.replace("    browser_status=0", '    touch "$fixture/all-launched"\n    browser_status=0');
      assert.notEqual(controlledBody, body);
      const result = spawnSync(bash, ["-c", `set -euo pipefail
fixture='${fixture}'
observability="$fixture"
fail_id='${failure}'
mutated='${earlyExit ? 1 : 0}'
parent_pid="$BASHPID"
on_exit() {
  if [[ "$BASHPID" != "$parent_pid" ]]; then return; fi
  for child in fs compaction mcp client; do
    if [[ ! -f "$fixture/$child.done" ]]; then printf 'unfinished-at-exit:%s\\n' "$child"; fi
  done
  touch "$fixture/all-launched" "$fixture/release-cleanup"
}
trap on_exit EXIT
bun() {
  case "$*" in
    *"@acyclic-labs/fs"*) id=fs ;;
    *"browser-compaction.html"*) id=compaction ;;
    *"test:browser:mcp"*) id=mcp ;;
    *"test:client:browser"*) id=client ;;
    *) return 98 ;;
  esac
  printf 'started:%s\\n' "$id"
  touch "$fixture/$id"
  while [[ ! -f "$fixture/all-launched" ]]; do sleep 0.01; done
  if [[ "$mutated" == 1 && "$id" != "$fail_id" ]]; then
    while [[ ! -f "$fixture/release-cleanup" ]]; do sleep 0.01; done
  fi
  touch "$fixture/$id.done"
  printf 'completed:%s\\n' "$id"
  [[ "$id" != "$fail_id" ]]
}
${controlledBody}
printf 'joined\\n'
`], { encoding: "utf8", timeout: 10000 });
      assert.equal(result.error, undefined);
      assert.equal(result.status, failure === "none" ? 0 : 1, result.stderr);
      const events = result.stdout.trim().split("\n");
      assert.deepEqual(events.filter(event => event.startsWith("started:")).sort(),
        ["started:client", "started:compaction", "started:fs", "started:mcp"]);
      assert.deepEqual(events.filter(event => event.startsWith("completed:")).sort(),
        ["completed:client", "completed:compaction", "completed:fs", "completed:mcp"]);
      assert.equal(events.includes("joined"), failure === "none");
      if (earlyExit) {
        assert.match(result.stdout, /unfinished-at-exit:/, "mutation must expose unfinished cleanup");
      } else {
        assert.doesNotMatch(result.stdout, /unfinished-at-exit:/);
        const summary = spawnSync(process.execPath, ["scripts/ci-summary.mjs", fixture], {
          encoding: "utf8", env: { ...process.env, GITHUB_STEP_SUMMARY: "" },
        });
        assert.equal(summary.status, 0, summary.stderr);
        assert.ok(summary.stdout.includes("### Parallel browser checks"));
        for (const browser of [{ id: "compaction", command: "bun typescript/packages/filesystem/test/browser-qualify.mjs ../harness/test/browser-compaction.html" }, { id: "fs", command: "bun run --filter @acyclic-labs/fs test:browser" },
          { id: "mcp", command: "bun run --filter @acyclic-labs/harness test:browser:mcp" },
          { id: "client", command: "bun run --filter @acyclic-labs/harness test:client:browser" }]) {
          const row = summary.stdout.split("\n").find(line => line.includes(browser.command)) ?? assert.fail("missing browser timing");
          assert.match(row, /\| \d+(?:\.\d+)?s \|/);
          assert.ok(row.includes(browser.id === failure ? "exit 1" : "passed"));
        }
      }
    }
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

test("host qualification initializes runner-local tools before both Bun installers", () => {
  const workflow = YAML.parse(readFileSync(".github/workflows/agent-host-qualification.yml", "utf8"));
  const job = workflow.jobs.qualify;
  assert.equal(job.env.TOOLS_DIR, undefined);
  assert.doesNotMatch(JSON.stringify(job.env), /\$\{\{\s*runner\./);
  const setup = job.steps.find(step => step.name === "Configure local tools and compiler cache")
    ?? assert.fail("missing host tool setup");
  const installers = job.steps.filter(step => /^Install pinned Bun on (Unix|Windows)$/.test(step.name));
  assert.equal(installers.length, 2);
  for (const installer of installers) assert.ok(job.steps.indexOf(setup) < job.steps.indexOf(installer));
  mkdirSync(".tools", { recursive: true });
  const root = mkdtempSync(".tools/host-tools-");
  const runnerTemp = resolve(root, "runner temp").replaceAll("\\", "/");
  const githubEnv = resolve(root, "job env").replaceAll("\\", "/");
  const bash = process.platform === "win32" ? join(process.env.ProgramFiles ?? "C:/Program Files", "Git", "bin", "bash.exe") : "/bin/bash";
  try {
    const result = spawnSync(bash, ["-e", "-c", setup.run], {
      env: { ...process.env, RUNNER_TEMP: runnerTemp, GITHUB_ENV: githubEnv }, encoding: "utf8", timeout: 10000,
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr);
    assert.deepEqual(readFileSync(githubEnv, "utf8").trim().split(/\r?\n/), [
      `TOOLS_DIR=${runnerTemp}/tools`, `SCCACHE_DIR=${runnerTemp}/sccache`,
    ]);
  } finally {
    assert.ok(resolve(root).startsWith(resolve(".tools") + sep));
    rmSync(root, { recursive: true, force: true });
  }
});

test("docs and generation provision nightly targets after stable", () => {
  const steps = YAML.parse(readFileSync(".github/actions/qualification-lane/action.yml", "utf8")).runs.steps;
  const stable = steps.findIndex(step => step.uses === "./.github/actions/rust-toolchain");
  const nightly = steps.findIndex(step => step.name === "Install pinned Rustdoc toolchain");
  assert.ok(stable >= 0 && nightly > stable);
  assert.equal(steps[nightly].if, "inputs.lane == 'docs' || inputs.lane == 'generation'");
  assert.equal(steps[nightly].run, "rustup toolchain install nightly-2026-07-01 --profile minimal --target wasm32-unknown-unknown --no-self-update");
  for (const name of ["docs", "generation"]) assert.equal(lanes.find(lane => lane.lane === name).runner, "blacksmith-2vcpu-ubuntu-2404");
});
