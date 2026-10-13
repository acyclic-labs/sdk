import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import test from "node:test";
import { posix } from "node:path";
import { runInNewContext } from "node:vm";
import { compatibilityArtifacts, packagedSourceCopies } from "./generated-bindings.mjs";
import { languageGeneratorBackends, parseGeneratorBackends, selectGeneratorBackends } from "./plan-qualification.mjs";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));

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
  const source = readFileSync("scripts/check-generated.mjs", "utf8");
  const start = source.indexOf("  for (const [source, packaged] of packagedSourceCopies) {");
  const end = source.indexOf("  const harnessSuite", start);
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

test("generator scope selects only affected backends and retains package README checks", () => {
  for (const backend of languageGeneratorBackends) {
    assert.deepEqual(selectGeneratorBackends([`tools/sdk-generator/backends/${backend}/src/generate.mjs`]), [backend]);
    assert.deepEqual(selectGeneratorBackends([`tools/sdk-generator/backends/${backend}/README.md`]), []);
  }
  const sharedScope = parseGeneratorBackends([
    { name: "source-only", shared: ["authority"] }, { name: "archive-only", shared: ["archive"] },
    { name: "both-readers", shared: ["authority", "archive"] }, { name: "neither-reader", shared: [] },
  ]);
  assert.deepEqual(selectGeneratorBackends(["tools/sdk-generator/shared/authority.mjs"], sharedScope), ["source-only", "both-readers"]);
  assert.deepEqual(selectGeneratorBackends(["scripts/archive-utils.mjs"], sharedScope), ["archive-only", "both-readers"]);
  assert.deepEqual(selectGeneratorBackends(["tools/sdk-generator/backends/dotnet/templates/package/README.md"]), ["dotnet"]);
  for (const path of [".github/workflows/sdk-generator.yml", ".github/sdk-generator-backends.json", "scripts/plan-qualification.mjs", "scripts/test-plan-qualification.mjs"]) {
    assert.deepEqual(selectGeneratorBackends([path]), languageGeneratorBackends);
  }
});

test("generator registry rejects empty, duplicate and unsafe backend coordinates", () => {
  const entry = { name: "go", shared: [] };
  for (const value of [[], [entry, entry], ["go"], [{ ...entry, name: "../outside" }], [{ ...entry, name: "go;echo" }], [null], {},
    [{ ...entry, name: "Go" }], [{ ...entry, shared: ["unknown"] }], [{ ...entry, shared: ["archive", "archive"] }],
    [{ ...entry, unexpected: true }]]) assert.throws(() => parseGeneratorBackends(value));
});

test("future shared-reader dependencies are declared without planner code changes", () => {
  const registry = parseGeneratorBackends([{ name: "new-backend", shared: ["archive"] }]);
  assert.deepEqual(selectGeneratorBackends(["scripts/archive-utils.mjs"], registry), ["new-backend"]);
  assert.deepEqual(selectGeneratorBackends(["tools/sdk-generator/shared/authority.mjs"], registry), []);
});

test("registered backends declare the shared readers used by their source and controls", () => {
  const registry = parseGeneratorBackends(JSON.parse(readFileSync(".github/sdk-generator-backends.json", "utf8")));
  const sources = root => readdirSync(root, { withFileTypes: true }).flatMap(entry => {
    const path = `${root}/${entry.name}`;
    return entry.isDirectory() ? sources(path) : /\.(mjs|go)$/.test(entry.name) ? [readFileSync(path, "utf8")] : [];
  }).join("\n");
  for (const entry of registry) {
    const text = sources(`tools/sdk-generator/backends/${entry.name}`);
    if (/shared\/(authority\.mjs|protoc\.json)/.test(text)) assert.ok(entry.shared.includes("authority"), `${entry.name} must declare shared authority inputs`);
    if (/scripts\/archive-utils\.mjs/.test(text)) assert.ok(entry.shared.includes("archive"), `${entry.name} must declare archive-reader inputs`);
  }
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
  const generatedCommand = linux.search(/^ +bun run check:generated$/m);
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
 const action=readFileSync(".github/actions/qualification-lane/action.yml","utf8");
 assert.match(action,/inputs\.lane == 'linux-arm64-bindings' && format\('sdk-sccache-v5-linux-arm64-/);
});

test("the workspace gate, docs and generation run as independent required matrix lanes",()=>{
 const script=readFileSync("scripts/qualify-ci.sh","utf8").replaceAll("\r\n","\n");
 const branch=name=>script.match(new RegExp("^  "+name+"\\)\\n([\\s\\S]*?)(?=^  \\S[^\\n]*\\)\\n|^esac\\b)","m"))?.[1] ?? assert.fail(`missing qualification branch: ${name}`);
 assert.match(branch("gate"),/nextest --workspace --all-features --locked/);
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
    assert.equal(selected[0].runner, "blacksmith-4vcpu-windows-2025");
    assert.equal(selected[0].artifact, "");
  }
  assert.equal(matrix.find(lane => lane.lane === "windows").artifact, "stream-native-windows");
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  assert.doesNotMatch(script, /Start-Background|Complete-Background/);
  for (const command of ["cargo test --workspace --all-features --locked", "cargo test -p acyclic-plugin --all-features --locked --test fork_join",
    "bun test --parallel=4 --reporter=junit", "bun run --filter '@acyclic-labs/fs' test:composition", "bun run check"])
    assert.ok(script.includes(command), command);
  assert.match(script, /test-qualify-ci-windows\.ps1/);
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.match(workflow, /max-parallel: 18/);
  assert.ok(matrix.length <= 18);
  assert.match(workflow, /test "\$RESULT" = success/);
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  assert.ok(action.includes('qualify-ci.ps1 -Lane "${{ inputs.lane }}"'));
  assert.match(action, /startsWith\(inputs.lane, 'windows-'\).*sdk-sccache-v5-windows-/);
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


test("Windows workspace compilation uses sixteen CPUs while audit runners stay bounded", () => {
  const matrix = lanes;
  const windows = matrix.find(lane => lane.lane === "windows");
  assert.equal(windows.runner, "blacksmith-16vcpu-windows-2025");
  assert.equal(windows.workers, 16);
  assert.equal(windows.artifact, "stream-native-windows");
  assert.equal(windows.artifact_path, "stream-native-bundle");
  for (const name of ["windows-napi", "windows-arm64", "windows-clippy"]) {
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
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  assert.ok(action.includes("inputs.lane == 'linux-runtime' && format('sdk-sccache-v5-linux-"));
});


test("macOS workspace qualification uses all twelve CPUs without changing package evidence", () => {
  const matrix = lanes;
  const macos = matrix.find(lane => lane.lane === "macos");
  assert.equal(macos.runner, "blacksmith-12vcpu-macos-15");
  assert.equal(macos.workers, 12);
  assert.equal(macos.artifact, "packages-macos");
  assert.equal(macos.artifact_path, "packages");
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("runs-on: ${{ matrix.runner }}"));
  for (const variable of ["CARGO_BUILD_JOBS", "CMAKE_BUILD_PARALLEL_LEVEL", "RAYON_NUM_THREADS", "RUST_TEST_THREADS"])
    assert.ok(workflow.includes(variable + ": ${{ matrix.workers }}"), variable);
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(macos.runner));
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
