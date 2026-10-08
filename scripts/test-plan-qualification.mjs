import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  chooseLanes,
  classifyQualificationEvent,
  ignored,
  laneKeys,
  mergedPullRequestMarkers,
  qualificationEventKinds,
  requiresFullQualification,
  retainedArtifact,
} from "./plan-qualification.mjs";

const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
const blob = (path, object = "a".repeat(40)) => `100644 blob ${object}\t${path}`;
const tree = [
  blob("Cargo.lock"),
  blob("rust/crates/stream/src/lib.rs"),
  blob("rust/crates/stream/README.md"),
  blob("typescript/packages/stream/src/index.ts"),
  blob("typescript/packages/filesystem/package.json"),
  blob("rust/crates/harness-codex/src/lib.rs"),
  blob("rust/crates/harness/tests/effects.rs"),
  blob("README.md"),
  blob(".github/workflows/publish-npm.yml"),
  blob(".github/workflows/qualification.yml"),
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
  for (const path of ["rust/crates/harness-codex/src/lib.rs", "rust/crates/harness/tests/effects.rs"]) {
    const after = differing(before, laneKeys(lanes, changed(path)));
    assert.ok(after.includes("gate") && !after.includes("typescript"), path);
  }
  const wasm = differing(before, laneKeys(lanes, changed("rust/crates/stream/src/lib.rs")));
  assert.ok(wasm.includes("typescript"));
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
    jobs: { total_count: 4, jobs: ["plan", "gate", "typescript", "policy"].map(name => ({ name, head_sha: donor, run_id: 7, run_attempt: 2, runner_id: 1, status: "completed", conclusion: "success", steps: [{ name: name === "plan" ? "Verify commit signatures and secrets" : "Run qualification lane", status: "completed", conclusion: "success" }] })) },
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
  assert.deepEqual(Object.keys(markers).sort(), ["gate", "policy", "typescript"]);
  assert.deepEqual(markers.gate, { ...source, source_commit: donor });
  const plan = chooseLanes(lanes, { mainPush: true, coreOnly: true, force: false, marker: name => markers[name], retained: retainedAll });
  assert.deepEqual(plan.matrix, []);
  assert.deepEqual(exercise(data => { data.runs.workflow_runs[0].path += "@main"; }), markers);
  assert.deepEqual(exercise(data => { data.runs.workflow_runs[0].pull_requests = [{ number: 999 }]; }), markers, "qualification belongs to the exact source, independent of run PR association");
  assert.deepEqual(exercise(data => { data.runs.workflow_runs.unshift({ ...data.runs.workflow_runs[0], id: 8 }); }), markers, "a reused/skipped attempt must not hide an earlier direct proof");
  assert.equal(exercise(undefined, tree, lanes, () => "").gate, undefined);
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
  assert.throws(() => mergedPullRequestMarkers(lanes, repository, head, { query: () => baseline.pr, treeAt: sha => { if (sha === donor) throw new Error("missing donor object"); return tree; } }), /missing donor object/);
});

test("retained proof artifacts belong to the exact source run and attempt", () => {
  const artifact = { name: "coverage-7-2", expired: false, workflow_run: { id: 7, head_sha: source.source_commit }, digest: `sha256:${"b".repeat(64)}` };
  const lookup = artifacts => retainedArtifact(7, "coverage", 2, source.source_commit, () => ({ artifacts }));
  assert.equal(lookup([artifact]), artifact.name);
  for (const replacement of [
    { name: "coverage-7-3" }, { expired: true }, { digest: null },
    { workflow_run: { id: 8, head_sha: source.source_commit } },
    { workflow_run: { id: 7, head_sha: "d".repeat(40) } },
  ]) assert.equal(lookup([{ ...artifact, ...replacement }]), "");
  assert.equal(lookup([artifact, artifact]), "");
  assert.equal(lookup([]), "");
  assert.equal(lookup([null]), "");
  assert.equal(lookup(null), "");
  const { matrix } = chooseLanes(lanes, { mainPush: true, coreOnly: true, force: false, marker: () => source, retained: () => lookup([]) });
  assert.deepEqual(matrix.map(lane => lane.lane), ["gate"]);
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
    commit("landed");
    const head = git(["rev-parse", "HEAD"]);
    const repo = { full_name: repository }, path = ".github/workflows/qualification.yml";
    const api = {
      [`repos/${repository}/commits/${head}/pulls`]: [{ merged_at: "today", merge_commit_sha: head, base: { ref: "main", repo }, head: { sha: donor, repo } }],
      [`repos/${repository}/actions/workflows/qualification.yml`]: { id: 9, path },
      [`repos/${repository}/actions/workflows/9/runs?event=pull_request&head_sha=${donor}&status=success&per_page=5`]: { workflow_runs: [{ id: 7, run_attempt: 2, workflow_id: 9, path, event: "pull_request", head_sha: donor, head_repository: repo, status: "completed", conclusion: "success" }] },
      [`repos/${repository}/actions/runs/7/attempts/2/jobs?per_page=100`]: { total_count: 4, jobs: ["plan", "gate", "typescript", "policy"].map(name => ({ name, head_sha: donor, run_id: 7, run_attempt: 2, runner_id: 1, status: "completed", conclusion: "success", steps: [{ name: name === "plan" ? "Verify commit signatures and secrets" : "Run qualification lane", status: "completed", conclusion: "success" }] })) },
      [`repos/${repository}/actions/runs/7/artifacts?per_page=100`]: { artifacts: [{ name: "coverage-7-2", expired: false, workflow_run: { id: 7, head_sha: donor }, digest: `sha256:${"b".repeat(64)}` }] },
      [`repos/${repository}/actions/runs/6/artifacts?per_page=100`]: { artifacts: [] },
      [`repos/${repository}/actions/runs/8/attempts/1/jobs?per_page=100`]: { jobs: [] },
    };
    put("api.json", JSON.stringify(api));
    mkdirSync(join(root, ".qualification"));
    put(".qualification/gate.json", JSON.stringify({ run_id: 6, run_attempt: 1, source_commit: donor }));
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
    env.REUSED = JSON.stringify(selected.reused);
    assert.deepEqual(run("record").recorded.sort(), ["gate", "policy", "typescript"]);
    assert.deepEqual(JSON.parse(readFileSync(join(root, ".qualification/gate.json"), "utf8")), { run_id: 7, run_attempt: 2, source_commit: donor });
    // Main's promoted markers also work without the donor object/API lookup.
    put("api.json", JSON.stringify({ [`repos/${repository}/actions/runs/7/artifacts?per_page=100`]: api[`repos/${repository}/actions/runs/7/artifacts?per_page=100`] }));
    assert.deepEqual(run("select").matrix, []);
    rmSync(join(root, ".qualification"), { recursive: true, force: true });
    put("api.json", "{}");
    assert.deepEqual(named(run("select").matrix), ["gate", "policy", "typescript"]);
    env.GITHUB_EVENT_NAME = "pull_request";
    assert.deepEqual(named(run("select").matrix), ["gate", "policy", "typescript"]);
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

test("full qualification rebuilds source-bound packages after a README-only change", () => {
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
  assert.deepEqual(matrix.map(lane => lane.lane), ["linux", "windows", "macos"]);
  assert.equal(reused.linux, undefined);
  assert.equal(reused.gate.run_id, source.run_id);
});

test("a lane whose artifact expired executes again", () => {
  const { matrix } = chooseLanes(lanes, {
    force: false, mainPush: false, marker: everywhere,
    retained: (runId, prefix) => (prefix === "coverage" ? "" : retainedAll(runId, prefix)),
  });
  assert.deepEqual(matrix.map(lane => lane.lane), ["gate"]);
});

test("ordinary main pushes keep downstream qualification off the routine path", () => {
  const { matrix, reused } = chooseLanes(lanes, {
    force: false, mainPush: true, marker: () => null, retained: retainedAll,
    coreOnly: true,
  });
  assert.deepEqual(named(matrix), ["gate", "policy", "typescript"]);
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
  assert.deepEqual(named(matrix), ["gate", "policy", "typescript"]);
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

test("the workflow keeps full qualification off routine pull requests", () => {
  const workflow = readFileSync(".github/workflows/qualification.yml", "utf8").replaceAll("\r\n", "\n");
  assert.match(workflow, /^  release:/m);
  assert.doesNotMatch(workflow, /^  schedule:/m);
  assert.match(workflow, /default: false/);
  assert.match(workflow, /github\.event_name == 'release' && github\.event\.release\.tag_name/);
  for (const { lane } of lanes) {
    assert.match(workflow, new RegExp(`- name: Find ${lane} marker\\n`), lane);
    assert.match(workflow, new RegExp(`- name: Record ${lane}\\n`), lane);
  }
});

test("the core lanes cover the Rust workspace, docs crate, and TypeScript workspace", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8");
  assert.match(script, /^ +nextest --workspace --all-features --locked$/m);
  assert.match(script, /cargo test --manifest-path rust\/crates\/sdk-docs\/Cargo\.toml --locked/);
  const policy = script.slice(script.indexOf("\n  policy)"));
  assert.match(policy, /^ +cargo clippy --workspace --all-targets --all-features --locked -- -D warnings\n +node --test/m);
  const typescript = script.slice(script.indexOf("\n  typescript)"));
  assert.match(typescript, /bun run test\n(?: .*\n)+? +bun run check:generated\n/);
});
