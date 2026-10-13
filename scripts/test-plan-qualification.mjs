import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { posix } from "node:path";
import { runInNewContext } from "node:vm";
import { compatibilityArtifacts, packagedSourceCopies } from "./generated-bindings.mjs";

import {
  chooseLanes,
  classifyQualificationEvent,
  ignored,
  languageGeneratorBackends,
  laneKeys,
  mergedPullRequestMarkers,
  parseGeneratorBackends,
  qualificationEventKinds,
  requiresFullQualification,
  retainedArtifact,
  remoteTreeEntries,
  selectGeneratorBackends,
} from "./plan-qualification.mjs";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
const blob = (path, object = "a".repeat(40)) => `100644 blob ${object}\t${path}`;
const generatorPaths = languageGeneratorBackends.map(backend => `tools/sdk-generator/backends/${backend}/src/generate.mjs`);
const tree = [
  blob("Cargo.lock"),
  blob("rust/crates/stream/src/lib.rs"),
  blob("rust/crates/stream/README.md"),
  blob("typescript/packages/stream/src/index.ts"),
  blob("typescript/packages/filesystem/package.json"),
  blob("rust/crates/harness-codex/src/lib.rs"),
  blob("rust/crates/harness/tests/effects.rs"),
  blob("rust/crates/sdk-docs/src/lib.rs"),
  blob("rust/crates/sdk-generation/src/main.rs"),
  blob("rust/crates/sdk-generation/scripts/qualify-typescript-snippets.mjs"),
  blob("README.md"),
  blob(".github/workflows/publish-npm.yml"),
  blob(".github/workflows/qualification.yml"),
  blob(".gitleaks.toml"),
  ...generatorPaths.map(path => blob(path)),
  blob(".github/sdk-generator-backends.json"),
  blob("tools/sdk-generator/shared/authority.mjs"),
  blob("tools/sdk-generator/backends/future/generate.mjs"),
];
const changed = (path, object = "b".repeat(40)) =>
  tree.map(entry => (entry.endsWith(`\t${path}`) ? blob(path, object) : entry));
const differing = (before, after) =>
  Object.keys(before).filter(lane => before[lane] !== after[lane]).sort();
const named = lanes => lanes.map(lane => lane.lane).sort();
const fullLanes = lanes.filter(lane => lane.scope !== "core");

test("every lane names a known input set and a Blacksmith runner", () => {
  for (const lane of lanes) {
    assert.ok(ignored[lane.inputs], lane.lane);
    assert.match(lane.runner, /^blacksmith-/);
  }
});

test("root documentation changes reuse every lane", () => {
  assert.deepEqual(differing(laneKeys(lanes, tree), laneKeys(lanes, changed("README.md"))), []);
});

test("secret scanner configuration reruns policy without rebuilding SDKs", () => {
  for (const scope of ["core", "full"]) {
    const before = laneKeys(lanes, tree, scope);
    const after = laneKeys(lanes, changed(".gitleaks.toml"), scope);
    assert.deepEqual(differing(before, after), ["linux", "macos", "policy"]);
  }
});

test("full qualification has a separate cache identity from the core gate", () => {
  const core = laneKeys(lanes, tree, "core");
  const full = laneKeys(lanes, tree, "full");
  assert.deepEqual(differing(core, full), lanes.map(lane => lane.lane).sort());
});

test("crate documentation reaches rustdoc and every lane", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("rust/crates/stream/README.md"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

test("TypeScript sources execute only TypeScript-observing lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("typescript/packages/stream/src/index.ts"));
  assert.deepEqual(differing(before, after), ["linux", "macos", "policy", "typescript", "windows"]);
});

test("Rust that no package compiles skips the TypeScript lane", () => {
  const before = laneKeys(lanes, tree);
  for (const path of [
    "rust/crates/harness-codex/src/lib.rs",
    "rust/crates/harness/tests/effects.rs",
    "rust/crates/sdk-docs/src/lib.rs",
    "rust/crates/sdk-generation/src/main.rs",
    "rust/crates/sdk-generation/scripts/qualify-typescript-snippets.mjs",
  ]) {
    const after = differing(before, laneKeys(lanes, changed(path)));
    assert.ok(after.includes("gate") && !after.includes("typescript"), path);
  }
  const wasm = differing(before, laneKeys(lanes, changed("rust/crates/stream/src/lib.rs")));
  assert.ok(wasm.includes("typescript"));
});

test("every generated source copy invalidates the TypeScript fingerprint on either side", () => {
  const inputs = [...new Set(packagedSourceCopies.flat())];
  const entries = inputs.map(path => blob(path));
  const before = laneKeys(lanes, entries).typescript;
  for (const path of inputs) {
    const mutated = entries.map(entry => entry.endsWith(`\t${path}`) ? blob(path, "b".repeat(40)) : entry);
    assert.notEqual(laneKeys(lanes, mutated).typescript, before, path);
  }
  const conformance = "rust/crates/conformance/";
  const actual = new Set(inputs.filter(path => path.startsWith(conformance)));
  const inventory = new Set(readdirSync(`${conformance}vectors`, { recursive: true, withFileTypes: true })
    .filter(entry => entry.isFile())
    .map(entry => `${entry.parentPath}/${entry.name}`.replaceAll("\\", "/")));
  assert.deepEqual(actual, inventory, "the copy inventory must cover the complete packaged conformance vector set");
  for (const path of ["Cargo.toml", "src/main.rs", "src/bin/qualify.rs"]) {
    const unrelated = [...entries, blob(`${conformance}${path}`)];
    assert.equal(laneKeys(lanes, unrelated).typescript, before, path);
    unrelated[unrelated.length - 1] = blob(`${conformance}${path}`, "b".repeat(40));
    assert.equal(laneKeys(lanes, unrelated).typescript, before, path);
  }
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

test("the filesystem package manifest reaches native binding lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("typescript/packages/filesystem/package.json"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

test("unrelated workflows reach only policy and repository lanes", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed(".github/workflows/publish-npm.yml"));
  assert.deepEqual(differing(before, after), ["linux", "macos", "policy"]);
});

test("isolated language generator changes reuse Rust and TypeScript builds", () => {
  const before = laneKeys(lanes, tree);
  for (const path of [...generatorPaths, "tools/sdk-generator/shared/authority.mjs", ".github/sdk-generator-backends.json"]) {
    const after = laneKeys(lanes, changed(path));
    assert.deepEqual(differing(before, after), ["linux", "macos", "policy"]);
  }
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

test("unregistered generators retain full qualification inputs", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed("tools/sdk-generator/backends/future/generate.mjs"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

test("the qualification workflow reaches every lane", () => {
  const before = laneKeys(lanes, tree);
  const after = laneKeys(lanes, changed(".github/workflows/qualification.yml"));
  assert.deepEqual(differing(before, after), lanes.map(lane => lane.lane).sort());
});

const source = { run_id: 7, run_attempt: 2, source_commit: "a".repeat(40) };
const everywhere = () => source;
const retainedAll = (runId, prefix) => `${prefix}-${runId}-2`;

test("merged PR proof reuses only executed core jobs for identical consumer inputs", () => {
  const repository = "owner/sdk", head = "c".repeat(40), donor = "d".repeat(40);
  const repo = { full_name: repository };
  const run = { id: 7, run_attempt: 2, workflow_id: 9, path: ".github/workflows/qualification.yml", event: "pull_request", head_sha: donor, head_repository: repo, pull_requests: [], status: "completed", conclusion: "success" };
  const baseline = {
    pr: [{ merged_at: "today", merge_commit_sha: head, base: { ref: "main", repo }, head: { sha: donor, repo } }],
    workflow: { id: 9, path: run.path }, runs: { workflow_runs: [run] },
    jobs: { total_count: 6, jobs: ["plan", "gate", "docs", "generation", "typescript", "policy"].map(name => ({ name, head_sha: donor, run_id: 7, run_attempt: 2, runner_id: 1, status: "completed", conclusion: "success", steps: [{ name: name === "plan" ? "Verify commit signatures and secrets" : "Run qualification lane", status: "completed", conclusion: "success" }] })) },
  };
  const exercise = (alter = data => {}, donorTree = tree, graph = lanes, artifact = retainedAll) => {
    const data = structuredClone(baseline);
    alter(data);
    return mergedPullRequestMarkers(graph, repository, head, {
      retained: artifact,
      treeAt: sha => { assert.ok([head, donor].includes(sha)); return sha === head ? tree : donorTree; },
      query: path => {
        if (path.endsWith("/pulls")) return data.pr;
        if (path.endsWith("/qualification.yml")) return data.workflow;
        if (path.includes("/runs?")) return data.runs;
        if (path.includes("/runs/8/attempts/2/jobs?")) return { total_count: 0, jobs: [] };
        assert.match(path, /\/runs\/7\/attempts\/2\/jobs\?/);
        return data.jobs;
      },
    });
  };
  const markers = exercise();
  assert.deepEqual(Object.keys(markers).sort(), ["docs", "gate", "generation", "policy", "typescript"]);
  assert.deepEqual(markers.gate, { ...source, source_commit: donor });
  const plan = chooseLanes(lanes, { mainPush: true, coreOnly: true, force: false, marker: name => markers[name], retained: retainedAll });
  assert.deepEqual(plan.matrix, []);
  for(const missing of ["docs", "generation"]){
    const incomplete=exercise(data=>{data.jobs.jobs.find(job=>job.name===missing).conclusion="skipped";});
    const remaining=chooseLanes(lanes,{mainPush:true,coreOnly:true,force:false,marker:name=>incomplete[name],retained:retainedAll});
    assert.deepEqual(named(remaining.matrix),[missing]);
  }
  assert.deepEqual(exercise(data => { data.runs.workflow_runs[0].path += "@main"; }), markers);
  assert.deepEqual(exercise(data => { data.runs.workflow_runs[0].pull_requests = [{ number: 999 }]; }), markers, "qualification belongs to the exact source, independent of run PR association");
  assert.deepEqual(exercise(data => { data.runs.workflow_runs.unshift({ ...data.runs.workflow_runs[0], id: 8 }); }), markers, "a reused/skipped attempt must not hide an earlier direct proof");
  assert.deepEqual(exercise(undefined, tree, lanes, () => "").gate, markers.gate, "test-only gates do not require a publication artifact");
  for (const alter of [
    data => { data.pr[0].merged_at = null; },
    data => { data.pr[0].merge_commit_sha = donor; },
    data => { data.pr[0].head.sha = head; },
    data => { data.pr.push(data.pr[0]); },
    data => { data.pr[0].base.ref = "another-base"; },
    data => { data.pr[0].head.repo.full_name = "another/repo"; },
    data => { data.workflow.path = "another.yml"; },
    data => { data.runs.workflow_runs[0].workflow_id = 8; },
    data => { data.runs.workflow_runs[0].path = ".github/workflows/another.yml@main"; },
    data => { data.runs.workflow_runs[0].head_sha = head; },
    data => { data.runs.workflow_runs[0].head_repository = { full_name: "another/repo" }; },
    data => { data.runs.workflow_runs[0].event = "workflow_dispatch"; },
    data => { data.runs.workflow_runs[0].conclusion = "failure"; },
    data => { data.runs.workflow_runs[0].run_attempt = 0; },
    data => { data.jobs.total_count = 5; },
    data => { data.jobs.jobs[0].conclusion = "skipped"; },
    data => { data.jobs.jobs[0].run_attempt = 1; },
  ]) assert.deepEqual(exercise(alter), {});
  for (const alteration of [{ conclusion: "skipped" }, { run_attempt: 1 }, { run_id: 8 }, { runner_id: 0 }, { name: "another-lane" }, { head_sha: head }, { steps: [] }, { steps: [{ name: "Run qualification lane", status: "completed", conclusion: "skipped" }] }]) {
    assert.equal(exercise(data => Object.assign(data.jobs.jobs[1], alteration)).gate, undefined);
  }
  assert.equal(exercise(data => { data.jobs.jobs.push(data.jobs.jobs[1]); data.jobs.total_count++; }).gate, undefined);
  assert.deepEqual(exercise(undefined, changed(".github/workflows/qualification.yml")), {});
  assert.deepEqual(Object.keys(exercise(undefined, changed("README.md"))).sort(), Object.keys(markers).sort());
  assert.equal(exercise(undefined, tree, lanes.map(lane => lane.lane === "gate" ? { ...lane, source_bound: true } : lane)).gate, undefined);
  assert.deepEqual(mergedPullRequestMarkers(lanes, repository, head, { query: () => baseline.pr, treeAt: sha => { if (sha === donor) throw new Error("missing donor object"); return tree; } }), {});
});

test("remote donor trees preserve real Git records and reject incomplete identity", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-remote-tree-"));
  try {
    const git = (args, input) => {
      const result = spawnSync("git", args, { cwd: root, input, encoding: "utf8" });
      assert.equal(result.status, 0, result.stderr);
      return result.stdout;
    };
    git(["init", "--quiet"]);
    const sha = git(["hash-object", "-w", "--stdin"], "fixture").trim();
    const entries = [
      ["100644", "a.c"], ["100644", "a/file"], ["100644", "a0"], ["100755", "executable"],
      ["120000", "symlink"], ["160000", "submodule"], ["100644", "tab\tname"],
      ["100644", "é"], ["100644", "中"], ["100644", "\uE000"], ["100644", "\u{10000}"], ["100644", "line\nname"],
    ].map(([mode, path]) => ({ mode, path, sha, type: mode === "160000" ? "commit" : "blob" }));
    // Index-only fixture: permit Git's portable path bytes without creating
    // tab/newline names that Windows cannot materialize in a checkout.
    for (const entry of entries) git(["-c", "core.protectNTFS=false", "update-index", "--add", "--cacheinfo", `${entry.mode},${sha},${entry.path}`]);
    const treeSha = git(["write-tree"]).trim();
    const expected = git(["ls-tree", "-r", "-z", "--full-tree", treeSha]).split("\0").filter(Boolean);
    const response = { sha: treeSha, truncated: false, tree: [...entries.reverse(), { type: "tree", mode: "040000", path: "a", sha }] };
    assert.deepEqual(remoteTreeEntries(response, treeSha), expected);
    assert.deepEqual(laneKeys(lanes, remoteTreeEntries(response, treeSha)), laneKeys(lanes, expected));
    for (const replacement of [{ sha: "b".repeat(40) }, { truncated: true }, { tree: null }]) {
      assert.throws(() => remoteTreeEntries({ ...response, ...replacement }, treeSha), /incomplete or belongs/);
    }
    for (const replacement of [{ sha: "invalid" }, { mode: "100600" }, { type: "tree-invalid" }, { type: "tree", mode: "100644" }, { path: "" }, { path: "null\0path" }, { path: "/absolute" }, { path: "../parent" }, { path: "a//b" }, { path: "a/./b" }, { path: "\uFFFD" }, { path: "\uD800" }]) {
      assert.throws(() => remoteTreeEntries({ ...response, tree: [{ ...entries[0], ...replacement }] }, treeSha), /invalid entry/);
    }
    assert.throws(() => remoteTreeEntries({ ...response, tree: [entries[0], entries[0]] }, treeSha), /invalid entry/);
    assert.throws(() => remoteTreeEntries({ ...response, tree: [{ ...entries[0], path: "a" }, { ...entries[0], path: "a/b" }] }, treeSha), /invalid entry prefix/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("retained proof artifacts belong to the exact source run and attempt", () => {
  const artifact = { name: "packages-linux-arm64-7-2", expired: false, workflow_run: { id: 7, head_sha: source.source_commit }, digest: `sha256:${"b".repeat(64)}` };
  const lookup = artifacts => retainedArtifact(7, "packages-linux-arm64", 2, source.source_commit, () => ({ artifacts }));
  assert.equal(lookup([artifact]), artifact.name);
  for (const replacement of [
    { name: "packages-linux-arm64-7-3" }, { expired: true }, { digest: null },
    { workflow_run: { id: 8, head_sha: source.source_commit } },
    { workflow_run: { id: 7, head_sha: "d".repeat(40) } },
  ]) assert.equal(lookup([{ ...artifact, ...replacement }]), "");
  assert.equal(lookup([artifact, artifact]), "");
  assert.equal(lookup([]), "");
  assert.equal(lookup([null]), "");
  assert.equal(lookup(null), "");
  const { matrix } = chooseLanes(lanes.filter(lane => lane.lane === "linux-arm64-bindings"), { mainPush: false, force: false, marker: () => source, retained: () => lookup([]) });
  assert.deepEqual(matrix.map(lane => lane.lane), ["linux-arm64-bindings"]);
});

test("the planner CLI promotes main proof and fails closed without changing PR/full policy", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-main-proof-"));
  const put = (path, value) => writeFileSync(join(root, path), value);
  const script = fileURLToPath(new URL("plan-qualification.mjs", import.meta.url));
  const repository = "owner/sdk";
  try {
    mkdirSync(join(root, ".github"));
    put(".github/qualification-lanes.json", JSON.stringify(lanes));
    const git = args => {
      const result = spawnSync("git", args, { cwd: root, encoding: "utf8" });
      assert.equal(result.status, 0, result.stderr);
      return result.stdout.trim();
    };
    git(["init", "--quiet"]);
    git(["add", ".github"]);
    const commit = message => git(["-c", "commit.gpgsign=false", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "--quiet", "-m", message]);
    commit("donor");
    const donor = git(["rev-parse", "HEAD"]);
    const donorTreeSha = git(["rev-parse", "HEAD^{tree}"]);
    assert.notEqual(donorTreeSha, donor);
    const donorEntries = git(["ls-tree", "-r", "-z", "--full-tree", donor]).split("\0").filter(Boolean).map(record => {
      const [mode, type, sha] = record.slice(0, record.indexOf("\t")).split(" ");
      return { mode, type, sha, path: record.slice(record.indexOf("\t") + 1) };
    });
    const donorBranch = git(["branch", "--show-current"]);
    git(["checkout", "--orphan", "landed", "--quiet"]);
    commit("landed");
    const head = git(["rev-parse", "HEAD"]);
    git(["branch", "-D", donorBranch]);
    git(["reflog", "expire", "--expire=now", "--all"]);
    git(["gc", "--prune=now", "--quiet"]);
    assert.notEqual(spawnSync("git", ["cat-file", "-e", donor], { cwd: root }).status, 0, "squash donor must actually be absent");
    const repo = { full_name: repository }, path = ".github/workflows/qualification.yml";
    const donorResponse = { sha: donorTreeSha, truncated: false, tree: donorEntries };
    const donorCommitResponse = { sha: donor, tree: { sha: donorTreeSha } };
    const api = {
      [`repos/${repository}/commits/${head}/pulls`]: [{ merged_at: "today", merge_commit_sha: head, base: { ref: "main", repo }, head: { sha: donor, repo } }],
      [`repos/${repository}/git/commits/${donor}`]: donorCommitResponse,
      [`repos/${repository}/git/trees/${donorTreeSha}?recursive=1`]: donorResponse,
      [`repos/${repository}/actions/workflows/qualification.yml`]: { id: 9, path },
      [`repos/${repository}/actions/workflows/9/runs?event=pull_request&head_sha=${donor}&status=success&per_page=5`]: { workflow_runs: [{ id: 7, run_attempt: 2, workflow_id: 9, path, event: "pull_request", head_sha: donor, head_repository: repo, status: "completed", conclusion: "success" }] },
      [`repos/${repository}/actions/runs/7/attempts/2/jobs?per_page=100`]: { total_count: 6, jobs: ["plan", "gate", "docs", "generation", "typescript", "policy"].map(name => ({ name, head_sha: donor, run_id: 7, run_attempt: 2, runner_id: 1, status: "completed", conclusion: "success", steps: [{ name: name === "plan" ? "Verify commit signatures and secrets" : "Run qualification lane", status: "completed", conclusion: "success" }] })) },
      [`repos/${repository}/actions/runs/8/attempts/1/jobs?per_page=100`]: { jobs: [] },
    };
    put("api.json", JSON.stringify(api));
    mkdirSync(join(root, ".qualification"));
    // Keep production CLI/Git behavior. Substitute only the external API
    // transport, so unavailable APIs and cache promotion exercise the caller.
    put("bootstrap.cjs", 'const cp=require("node:child_process"),fs=require("node:fs"),original=cp.execFileSync; cp.execFileSync=(file,args,options)=>{if(file!=="gh")return original(file,args,options);const data=JSON.parse(fs.readFileSync("api.json"));const path=args.at(-1);if(!data[path])throw new Error("API unavailable");return JSON.stringify(data[path]);};require("node:module").syncBuiltinESMExports();');
    const env = { ...process.env, GITHUB_REPOSITORY: repository, GITHUB_EVENT_NAME: "push", GITHUB_REF: "refs/heads/main", FORCE: "false", GITHUB_RUN_ID: "8", GITHUB_RUN_ATTEMPT: "1", GITHUB_OUTPUT: "", NODE_OPTIONS: `--require=${JSON.stringify(join(root, "bootstrap.cjs"))}`, REUSED: "{}" };
    const run = command => {
      const result = spawnSync(process.execPath, [script, command], { cwd: root, env, encoding: "utf8" });
      assert.equal(result.status, 0, result.stderr);
      return Object.fromEntries(result.stdout.trim().split(/\r?\n/).map(line => { const at = line.indexOf("="); return [line.slice(0, at), JSON.parse(line.slice(at + 1))]; }));
    };
    const selected = run("select");
    assert.deepEqual(selected.matrix, []);
    for (const record of [donorCommitResponse, donorResponse]) {
      const sha = record.sha;
      record.sha = "f".repeat(40);
      put("api.json", JSON.stringify(api));
      assert.deepEqual(named(run("select").matrix), ["docs", "gate", "generation", "policy", "typescript"]);
      record.sha = sha;
    }
    donorResponse.truncated = true;
    put("api.json", JSON.stringify(api));
    assert.deepEqual(named(run("select").matrix), ["docs", "gate", "generation", "policy", "typescript"]);
    donorResponse.truncated = false;
    put("api.json", JSON.stringify(api));
    env.REUSED = JSON.stringify(selected.reused);
    assert.deepEqual(run("record").recorded.sort(), ["docs", "gate", "generation", "policy", "typescript"]);
    assert.deepEqual(JSON.parse(readFileSync(join(root, ".qualification/gate.json"), "utf8")), { run_id: 7, run_attempt: 2, source_commit: donor });
    // Main's promoted markers also work without the donor object/API lookup.
    put("api.json", "{}");
    assert.deepEqual(run("select").matrix, []);
    rmSync(join(root, ".qualification"), { recursive: true, force: true });
    put("api.json", "{}");
    assert.deepEqual(named(run("select").matrix), ["docs", "gate", "generation", "policy", "typescript"]);
    env.GITHUB_EVENT_NAME = "pull_request";
    assert.deepEqual(named(run("select").matrix), ["docs", "gate", "generation", "policy", "typescript"]);
    put("api.json", JSON.stringify({ [`repos/${repository}/actions/runs/8/attempts/1/jobs?per_page=100`]: { jobs: [] } }));
    assert.deepEqual(run("record").recorded, [], "PR reuse must not promote main markers");
    env.REUSED = "{}";
    put("api.json", JSON.stringify({ [`repos/${repository}/actions/runs/8/attempts/1/jobs?per_page=100`]: { jobs: [{ name: "policy", conclusion: "success" }] } }));
    assert.deepEqual(run("record").recorded, ["policy"]);
    assert.deepEqual(JSON.parse(readFileSync(join(root, ".qualification/policy.json"), "utf8")), { run_id: 8, run_attempt: 1, source_commit: head });
    rmSync(join(root, ".qualification"), { recursive: true, force: true });
    env.GITHUB_EVENT_NAME = "workflow_dispatch";
    assert.deepEqual(named(run("select").matrix), named(fullLanes));
    env.FORCE = "true";
    assert.deepEqual(named(run("select").matrix), named(fullLanes));
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("recorded lanes are reused with their retained artifact", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: false, marker: everywhere, retained: retainedAll,
  });
  assert.deepEqual(matrix, []);
  assert.equal(reused.linux.artifact, "packages-linux-7-2");
  assert.equal(reused.web.artifact, "");
});

test("full qualification reruns source-bound packages and runtime tests after a README-only change", () => {
  const before = laneKeys(lanes, tree, "full");
  const after = laneKeys(lanes, changed("README.md"), "full");
  assert.deepEqual(before, after);

  const { matrix, reused } = chooseLanes(lanes, {
    force: false,
    mainPush: false,
    fullQualification: true,

    marker: everywhere,
    retained: retainedAll,
  });
  assert.deepEqual(matrix.map(lane => lane.lane), ["linux", "linux-runtime", "windows", "windows-rust", "macos"]);
  assert.equal(reused.linux, undefined);
  assert.equal(reused.gate.run_id, source.run_id);
});

test("a lane whose artifact expired executes again", () => {
  const { matrix } = chooseLanes(lanes, {
    force: false, mainPush: false, marker: everywhere,
    retained: (runId, prefix) => (prefix === "packages-linux-arm64" ? "" : retainedAll(runId, prefix)),
  });
  assert.deepEqual(matrix.map(lane => lane.lane), ["linux-arm64-bindings"]);
});

test("ordinary main pushes keep downstream qualification off the routine path", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: true, marker: () => null, retained: retainedAll,
    coreOnly: true,
  });
  assert.deepEqual(named(matrix), ["docs", "gate", "generation", "policy", "typescript"]);
  assert.deepEqual(reused, {});
});

test("forced runs execute every full lane", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: true, mainPush: false, marker: everywhere, retained: retainedAll,
  });
  assert.deepEqual(named(matrix), named(fullLanes));
  assert.deepEqual(reused, {});
});

test("forced reusable release calls keep the full lane scope", () => {
  const event = classifyQualificationEvent({
    eventName: "push",
    ref: "refs/tags/acyclic-v1.2.3",
    force: true,
  });
  assert.equal(event, qualificationEventKinds.forcedDispatch);
  assert.equal(requiresFullQualification(event), true);

  const { matrix, reused } = chooseLanes(lanes, {
    force: true,
    mainPush: false,
    coreOnly: true,

    marker: everywhere,
    retained: retainedAll,
  });
  assert.deepEqual(named(matrix), named(fullLanes));
  assert.deepEqual(reused, {});
});

test("routine pull requests qualify only the core lanes", () => {
  const { matrix } = chooseLanes(lanes, {
    force: false,
    mainPush: false,
    pullRequest: true,
    coreOnly: true,

    marker: () => null,
    retained: () => "",
  });
  assert.deepEqual(named(matrix), ["docs", "gate", "generation", "policy", "typescript"]);
});

test("release, manual, and scheduled events are full qualification events", () => {
  /** @type {Array<[{ eventName: string, force?: boolean }, string]>} */
  const cases = [
    [{ eventName: "release" }, qualificationEventKinds.release],
    [{ eventName: "workflow_dispatch", force: false }, qualificationEventKinds.manual],
    [{ eventName: "schedule" }, qualificationEventKinds.schedule],
  ];
  for (const [input, expected] of cases) {
    const event = classifyQualificationEvent(input);
    assert.equal(event, expected);
    assert.equal(requiresFullQualification(event), true);
  }
});

test("the core lanes cover the Rust workspace, docs crate, and TypeScript workspace", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8").replaceAll("\r\n", "\n");
  assert.match(script, /^ +nextest --workspace --all-features --locked$/m);
  assert.match(script, /cargo test --manifest-path rust\/crates\/sdk-docs\/Cargo\.toml --locked/);
  assert.match(script, /cargo test --manifest-path rust\/crates\/sdk-generation\/Cargo\.toml --locked/);
  assert.match(script, /node --test rust\/crates\/sdk-generation\/scripts\/test-qualify-typescript-snippets\.mjs/);
  const policy = script.match(/^  policy\)\n([\s\S]*?)(?=^  \S[^\n]*\)\n|^esac\b)/m)?.[1] ?? assert.fail("missing policy lane");
  assert.match(policy, /^    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings$/m);
  assert.doesNotMatch(policy, /full_qualification|exit 0/);
  const typescript = script.match(/^  typescript\)\n([\s\S]*?)(?=^  \S[^\n]*\)\n|^esac\b)/m)?.[1];
  assert.ok(typescript, "missing TypeScript lane");
  const testCommand = typescript.search(/^ +bun run test$/m);
  const generatedCommand = typescript.search(/^ +bun run check:generated$/m);
  assert.ok(testCommand >= 0, "TypeScript lane must run tests");
  assert.ok(generatedCommand > testCommand, "TypeScript generated check must follow tests");
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
 const matrix=chooseLanes(lanes,{mainPush:false,force:true,marker:everywhere,retained:retainedAll}).matrix;
 const producer=matrix.find(lane=>lane.lane==="linux-arm64-bindings");
 assert.equal(producer.artifact,"packages-linux-arm64");assert.equal(producer.artifact_path,"packages");
 assert.equal(producer.runner,"blacksmith-4vcpu-ubuntu-2404-arm");
 assert.equal(matrix.filter(lane=>lane.artifact==="packages-linux-arm64").length,1);
 assert.equal(matrix.find(lane=>lane.lane==="linux-arm64").artifact,"");
 const workflow=readFileSync(".github/workflows/qualification.yml","utf8");
 assert.match(workflow,/name: Find linux-arm64-bindings marker/);assert.match(workflow,/name: Record linux-arm64-bindings/);
 assert.match(workflow,/needs: \[ plan, qualification \]/);assert.match(workflow,/test "\$RESULT" = success/);
 assert.ok(matrix.length<=17,"all ARM work fits in the simultaneous matrix");
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
 const matrix=chooseLanes(lanes,{mainPush:false,force:true,marker:everywhere,retained:retainedAll}).matrix;
 for(const name of ["gate","docs","generation"])assert.equal(matrix.filter(lane=>lane.lane===name).length,1);
 const workflow=readFileSync(".github/workflows/qualification.yml","utf8");
 assert.match(workflow,/max-parallel: 17/);
 assert.ok(matrix.length<=17,"all full lanes can start together");
 for(const name of ["docs","generation"]){
  assert.match(workflow,new RegExp("name: Find "+name+" marker"));
  assert.match(workflow,new RegExp("name: Record "+name));
 }
 assert.match(workflow,/needs: \[ plan, qualification \]/);
 assert.match(workflow,/test "\$RESULT" = success/);
});

test("Windows native audits are independent required lanes without moving publication outputs", () => {
  const matrix = chooseLanes(lanes, { mainPush: false, force: true, marker: everywhere, retained: retainedAll }).matrix;
  const names = ["windows-napi", "windows-arm64", "windows-clippy"];
  for (const name of names) {
    const selected = matrix.filter(lane => lane.lane === name);
    assert.equal(selected.length, 1);
    assert.equal(selected[0].runner, "blacksmith-4vcpu-windows-2025");
    assert.equal(selected[0].scope, "full");
    assert.equal(selected[0].inputs, "rust");
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
  assert.match(workflow, /max-parallel: 17/);
  assert.ok(matrix.length <= 17);
  for (const name of names) {
    assert.ok(workflow.includes(`name: Find ${name} marker`));
    assert.ok(workflow.includes(`name: Record ${name}`));
  }
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
  const matrix = chooseLanes(lanes, { mainPush: false, force: true, marker: everywhere, retained: retainedAll }).matrix;
  const arm = matrix.find(lane => lane.lane === "linux-arm64");
  assert.equal(arm.runner, "blacksmith-32vcpu-ubuntu-2404-arm");
  assert.equal(arm.workers, 32);
  assert.equal(arm.scope, "full");
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
  const matrix = chooseLanes(lanes, { mainPush: false, force: true, marker: everywhere, retained: retainedAll }).matrix;
  const windows = matrix.find(lane => lane.lane === "windows");
  assert.equal(windows.runner, "blacksmith-16vcpu-windows-2025");
  assert.equal(windows.workers, 16);
  assert.equal(windows.scope, "full");
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
  const matrix = chooseLanes(lanes, { mainPush: false, force: true, marker: everywhere, retained: retainedAll }).matrix;
  const lane = matrix.find(lane => lane.lane === "linux-runtime");
  assert.equal(lane.runner, "blacksmith-4vcpu-ubuntu-2404");
  assert.equal(lane.workers, 4);
  assert.equal(lane.inputs, "rust");
  assert.equal(lane.scope, "full");
  assert.equal(lane.source_bound, true);
  const full = chooseLanes(lanes, { mainPush: false, force: false, fullQualification: true, marker: everywhere, retained: retainedAll }).matrix;
  assert.equal(full.filter(lane => lane.lane === "linux-runtime").length, 1);
  assert.equal(lane.artifact, "");
  assert.equal(matrix.find(lane => lane.lane === "linux").artifact, "packages-linux");
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  for (const marker of ["Find linux-runtime marker", "Record linux-runtime"])
    assert.ok(workflow.includes(marker));
  assert.ok(workflow.includes("include: ${{ fromJSON(needs.plan.outputs.matrix) }}"));
  assert.ok(workflow.includes("lane: ${{ matrix.lane }}"));
  assert.ok(matrix.length <= 17);
  assert.ok(workflow.includes("max-parallel: 17"));
  assert.ok(workflow.includes('test "$RESULT" = success'));
  const action = readFileSync(".github/actions/qualification-lane/action.yml", "utf8");
  assert.ok(action.includes("inputs.lane == 'linux-runtime' && format('sdk-sccache-v5-linux-"));
});


test("macOS workspace qualification uses all twelve CPUs without changing package evidence", () => {
  const matrix = chooseLanes(lanes, { mainPush: false, force: true, marker: everywhere, retained: retainedAll }).matrix;
  const macos = matrix.find(lane => lane.lane === "macos");
  assert.equal(macos.runner, "blacksmith-12vcpu-macos-15");
  assert.equal(macos.workers, 12);
  assert.equal(macos.scope, "full");
  assert.equal(macos.source_bound, true);
  assert.equal(macos.artifact, "packages-macos");
  assert.equal(macos.artifact_path, "packages");
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8");
  assert.ok(workflow.includes("runs-on: ${{ matrix.runner }}"));
  for (const variable of ["CARGO_BUILD_JOBS", "CMAKE_BUILD_PARALLEL_LEVEL", "RAYON_NUM_THREADS", "RUST_TEST_THREADS"])
    assert.ok(workflow.includes(variable + ": ${{ matrix.workers }}"), variable);
  assert.ok(readFileSync(".github/actionlint.yaml", "utf8").includes(macos.runner));
});


test("Linux package qualification uses sixteen workers without changing publication inputs", () => {
  const matrix = chooseLanes(lanes, {
    mainPush: false, force: true, marker: () => null, retained: () => "",
  }).matrix;
  const linux = matrix.find(lane => lane.lane === "linux") ?? assert.fail("missing Linux lane");
  assert.equal(linux.runner, "blacksmith-16vcpu-ubuntu-2404");
  assert.equal(linux.workers, 16);
  assert.equal(linux.scope, "full");
  assert.equal(linux.source_bound, true);
  assert.equal(linux.inputs, "repository");
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
