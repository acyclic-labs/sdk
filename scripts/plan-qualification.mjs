// Plans which SDK qualification lanes must execute for the checked-out tree.
//
// Every lane is fingerprinted from the git blobs it can observe. A lane whose
// fingerprint already qualified (recorded as a cache marker by an earlier run)
// is reused instead of re-executed, and its retained artifact is forwarded into
// this run so release workflows keep finding it under this run's identity.
//
//   plan-qualification.mjs fingerprints   -> keys, lanes
//   plan-qualification.mjs select         -> matrix, reused
//   plan-qualification.mjs record         -> recorded
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { packagedSourceCopies } from "./generated-bindings.mjs";

// check-generated reads both sides of every packaged copy, including crates
// whose executable sources the TypeScript lane otherwise never compiles.
const generatedSourceInputs = new Set(packagedSourceCopies.flat());

// Bump to invalidate every recorded marker at once.
const SCHEMA = "sdk-qualification-v2";

export function parseGeneratorBackends(value) {
  if (!Array.isArray(value) || value.length === 0
    || value.some(entry => !entry || typeof entry.name !== "string" || !/^[a-z][a-z0-9-]*$/.test(entry.name)
      || Object.keys(entry).some(key => !["name", "shared"].includes(key)) || !Array.isArray(entry.shared)
      || new Set(entry.shared).size !== entry.shared.length || entry.shared.some(name => !["authority", "archive"].includes(name)))
    || new Set(value.map(entry => entry.name)).size !== value.length) {
    throw new Error("invalid SDK generator backend registry");
  }
  return Object.freeze(value.map(entry => Object.freeze({ name: entry.name, shared: Object.freeze([...entry.shared]) })));
}
const generatorRegistry = parseGeneratorBackends(JSON.parse(
  readFileSync(new URL("../.github/sdk-generator-backends.json", import.meta.url), "utf8"),
));
export const languageGeneratorBackends = Object.freeze(generatorRegistry.map(entry => entry.name));

export function selectGeneratorBackends(changed, registry = generatorRegistry) {
  const all = changed.some(path => [".github/workflows/sdk-generator.yml", ".github/sdk-generator-backends.json",
    "scripts/plan-qualification.mjs", "scripts/test-plan-qualification.mjs"].includes(path));
  return registry.filter(backend => all || changed.some(path =>
    (path.startsWith(`tools/sdk-generator/backends/${backend.name}/`) && path !== `tools/sdk-generator/backends/${backend.name}/README.md`)
    || (backend.shared.includes("authority") && path.startsWith("tools/sdk-generator/shared/"))
    || (backend.shared.includes("archive") && path === "scripts/archive-utils.mjs"))).map(backend => backend.name);
}

const documentation = path =>
  /^(README|CONTRIBUTING|SECURITY)\.md$/.test(path) || /^docs\/[^/]+\.md$/.test(path);
const qualificationDefinition = path =>
  path === ".github/workflows/qualification.yml" ||
  path === ".github/qualification-lanes.json" ||
  path.startsWith(".github/actions/");
const unrelatedGithub = path => path.startsWith(".github/") && !qualificationDefinition(path);
const standaloneProjects = path => path.startsWith("arena/") || path.startsWith("examples/");
// These isolated generators have focused CI and are not read by Cargo,
// TypeScript generation or native/WASM builds.
const languageGenerator = path =>
  languageGeneratorBackends.some(backend => path.startsWith(`tools/sdk-generator/backends/${backend}/`)) ||
  path.startsWith("tools/sdk-generator/shared/") ||
  path === "tools/sdk-generator/README.md";

export const qualificationEventKinds = Object.freeze({
  pullRequest: "pull_request",
  mainPush: "main_push",
  release: "release",
  forcedDispatch: "forced_dispatch",
  manual: "manual",
  schedule: "schedule",
  other: "other",
});

/** @param {{ eventName?: string, ref?: string, force?: boolean }} event */
export function classifyQualificationEvent({ eventName, ref, force = false }) {
  // Reusable release callers inherit the caller's push/tag event.  An explicit
  // force input is the authoritative signal that this invocation is a full
  // qualification, regardless of the inherited event name.
  if (force) return qualificationEventKinds.forcedDispatch;
  if (eventName === "pull_request") return qualificationEventKinds.pullRequest;
  if (eventName === "release") return qualificationEventKinds.release;
  if (eventName === "workflow_dispatch") {
    return force ? qualificationEventKinds.forcedDispatch : qualificationEventKinds.manual;
  }
  if (eventName === "schedule") return qualificationEventKinds.schedule;
  if (eventName === "push" && ref === "refs/heads/main") return qualificationEventKinds.mainPush;
  return qualificationEventKinds.other;
}

export function requiresFullQualification(event) {
  return event === qualificationEventKinds.release ||
    event === qualificationEventKinds.manual ||
    event === qualificationEventKinds.schedule ||
    event === qualificationEventKinds.forcedDispatch;
}

// Each predicate returns true for paths the lane can never observe.
export const ignored = {
  // Cargo, Rust sources, protocol and conformance data, scripts, release metadata.
  rust: path =>
    documentation(path) ||
    unrelatedGithub(path) ||
    path === ".gitleaks.toml" ||
    standaloneProjects(path) ||
    languageGenerator(path) ||
    (path.startsWith("typescript/") && path !== "typescript/packages/filesystem/package.json") ||
    path.startsWith("languages/") ||
    path.startsWith("ffi/") ||
    ["bun.lock", "package.json", "tsconfig.json", "tsconfig.base.json"].includes(path),
  // The TypeScript workspace and everything its WASM builds, generated
  // contracts and conformance servers compile: every Rust crate except those
  // no package reads, and no Rust integration tests.
  typescript: path =>
    !generatedSourceInputs.has(path) && (ignored.product(path) ||
    /^rust\/crates\/(conformance|harness-codex|machines-daytona|sdk-docs|sdk-generation)\//.test(path) ||
    /^rust\/crates\/[^/]+\/(tests|benches)\//.test(path) ||
    ["plugin/", "languages/", "ffi/"].some(prefix => path.startsWith(prefix))),
  // Rust plus the TypeScript workspace.
  // Scanner configuration is read by preflight/policy, never an SDK build.
  product: path => documentation(path) || unrelatedGithub(path) || path === ".gitleaks.toml" || standaloneProjects(path) || languageGenerator(path),
  // Repository-wide metadata, boundary, and license checks.
  repository: path => documentation(path),
  policy: path => documentation(path),
};

// `entries` are `git ls-tree -r` records: "<mode> <type> <object>\t<path>".
// Core and full qualification must have separate cache identities. A full run
// otherwise could restore a marker written by a cheap pull-request run and
// skip its coverage and downstream checks.
export function laneKeys(lanes, entries, scope = "core") {
  if (scope !== "core" && scope !== "full") throw new Error(`unknown qualification scope ${scope}`);
  const keys = {};
  for (const lane of lanes) {
    const predicate = ignored[lane.inputs];
    if (!predicate) throw new Error(`lane ${lane.lane} names unknown inputs ${lane.inputs}`);
    const digest = createHash("sha256");
    digest.update(`${SCHEMA}\0${scope}\0${JSON.stringify(lane)}\0`);
    for (const entry of entries) {
      if (predicate(entry.slice(entry.indexOf("\t") + 1))) continue;
      digest.update(entry);
      digest.update("\0");
    }
    keys[lane.lane] = `sdk-qualified-${lane.lane}-${digest.digest("hex")}`;
  }
  return keys;
}

// Decides each lane's fate. Pull requests and main pushes run the security
// preflight and the lanes scoped "core"; release, scheduled, manual, and
// forced runs run those scoped "full"; "both" lanes run in either, under
// separate cache identities. `marker(lane)` returns the run that recorded the
// lane's fingerprint, if any; `retained(runId, prefix)` names the artifact that
// run still retains, or "".
export function chooseLanes(lanes, {
  force,
  mainPush,
  fullQualification = false,
  pullRequest = false,
  coreOnly = false,
  marker,
  retained,
}) {
  const matrix = [];
  const reused = {};
  const scope = !force && (pullRequest || coreOnly) ? "core" : "full";
  for (const lane of lanes) {
    if (lane.scope !== "both" && lane.scope !== scope) continue;
    // Source-bound artifacts record the commit they were built from, so every
    // full qualification run must rebuild them for its exact checked-out commit.
    if (force || ((mainPush || fullQualification) && lane.source_bound)) {
      matrix.push(lane);
      continue;
    }
    let source = marker(lane.lane);
    let artifact = "";
    if (source && lane.artifact) {
      artifact = retained(source.run_id, lane.artifact, source.run_attempt, source.source_commit);
      if (!artifact) source = null;
    }
    if (source) reused[lane.lane] = { run_id: source.run_id, run_attempt: source.run_attempt, source_commit: source.source_commit, artifact };
    else matrix.push(lane);
  }
  return { matrix, reused };
}

const git = (...args) => execFileSync("git", args, { encoding: "utf8", maxBuffer: 1 << 28, env: { ...process.env, GIT_NO_LAZY_FETCH: "1" } });
const gh = path => JSON.parse(execFileSync("gh", ["api", "--method", "GET", path], { encoding: "utf8" }));
const output = (name, value) => {
  const text = typeof value === "string" ? value : JSON.stringify(value);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${text}\n`);
  console.log(`${name}=${text}`);
};
const readLanes = () => JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
const reusableCore = lane => ["gate", "typescript", "policy"].includes(lane.lane) &&
  ["core", "both"].includes(lane.scope) && !lane.source_bound;

function fingerprints() {
  const lanes = readLanes();
  const entries = git("ls-tree", "-r", "-z", "--full-tree", "HEAD").split("\0").filter(Boolean);
  const force = process.env.FORCE === "true";
  const event = classifyQualificationEvent({
    eventName: process.env.GITHUB_EVENT_NAME,
    ref: process.env.GITHUB_REF,
    force,
  });
  const scope = requiresFullQualification(event) ? "full" : "core";
  const keys = laneKeys(lanes, entries, scope);
  output("keys", keys);
  output("lanes", lanes.map(lane => lane.lane));
}

// Artifacts are named <artifact>-<run id>-<attempt that uploaded them>.
export function retainedArtifact(runId, prefix, attempt, sourceCommit, query = gh) {
  let artifacts;
  try {
    artifacts = query(
      `repos/${process.env.GITHUB_REPOSITORY}/actions/runs/${runId}/artifacts?per_page=100`,
    ).artifacts;
  } catch (error) {
    console.error(`artifact lookup for run ${runId} failed: ${error instanceof Error ? error.message : String(error)}`);
    return "";
  }
  if (!Array.isArray(artifacts)) return "";
  const name = `${prefix}-${runId}-${attempt}`;
  const candidates = artifacts.filter(artifact => artifact?.expired === false && artifact.name === name &&
    artifact.workflow_run?.id === runId && artifact.workflow_run.head_sha === sourceCommit &&
    /^sha256:[0-9a-f]{64}$/.test(artifact.digest));
  return candidates.length === 1 ? name : "";
}

// Main cannot restore PR-scoped cache markers. Reuse only direct, successful
// PR jobs with identical existing input keys; current commit preflight is
// independent. Deleted squash branches need a bounded Git tree API lookup;
// missing or truncated API evidence conservatively runs lanes.
// The merged PR identifies the donor source, not its run's PR number: GitHub
// can clear run.pull_requests after merging. Core commands are event-invariant.
export function mergedPullRequestMarkers(lanes, repository, head, {
  query = gh,
  treeAt = sha => git("ls-tree", "-r", "-z", "--full-tree", sha).split("\0").filter(Boolean),
  retained = (id, prefix, attempt, commit) => retainedArtifact(id, prefix, attempt, commit, query),
} = {}) {
  const decline = reason => { console.error(`merged PR reuse declined: ${reason}`); return {}; };
  const candidates = query(`repos/${repository}/commits/${head}/pulls`).filter(pr =>
    pr.merged_at && pr.merge_commit_sha === head && pr.base.ref === "main" &&
    pr.base.repo.full_name === repository && pr.head.repo?.full_name === repository);
  if (candidates.length !== 1) return decline("associated merged PR absent or ambiguous");
  const donor = candidates[0].head.sha;
  if (!/^[0-9a-f]{40}$/.test(donor)) return decline("invalid donor head");
  const currentKeys = laneKeys(lanes, treeAt(head), "core");
  let donorTree;
  try { donorTree = treeAt(donor); }
  catch {
    const commit = query(`repos/${repository}/git/commits/${donor}`);
    if (commit.sha !== donor || !/^[0-9a-f]{40}$/.test(commit.tree?.sha)) return decline("donor commit/tree mismatch");
    donorTree = remoteTreeEntries(query(`repos/${repository}/git/trees/${commit.tree.sha}?recursive=1`), commit.tree.sha);
  }
  const donorKeys = laneKeys(lanes, donorTree, "core");
  const eligible = lanes.filter(lane => reusableCore(lane) && currentKeys[lane.lane] === donorKeys[lane.lane]);
  if (!eligible.length) return decline("no matching core input keys");
  const workflow = query(`repos/${repository}/actions/workflows/qualification.yml`);
  if (!Number.isSafeInteger(workflow.id) || workflow.id <= 0 || workflow.path !== ".github/workflows/qualification.yml") return decline("workflow identity mismatch");
  const runs = query(`repos/${repository}/actions/workflows/${workflow.id}/runs?event=pull_request&head_sha=${donor}&status=success&per_page=5`).workflow_runs;
  const validRuns = runs.slice(0, 5).filter(run => run.workflow_id === workflow.id && (run.path === workflow.path || run.path?.startsWith(`${workflow.path}@`)) &&
    run.event === "pull_request" && run.head_sha === donor && run.head_repository?.full_name === repository &&
    run.status === "completed" && run.conclusion === "success" && Number.isSafeInteger(run.id) && run.id > 0 &&
    Number.isSafeInteger(run.run_attempt) && run.run_attempt > 0);
  const markers = {};
  for (const run of validRuns) {
    const response = query(`repos/${repository}/actions/runs/${run.id}/attempts/${run.run_attempt}/jobs?per_page=100`);
    if (response.total_count !== response.jobs.length) continue;
    const executed = name => {
      const jobs = response.jobs.filter(job => job.name === name);
      const steps = jobs[0]?.steps?.filter(step => step.name === (name === "plan" ? "Verify commit signatures and secrets" : "Run qualification lane")) ?? [];
      return jobs.length === 1 && jobs[0].run_id === run.id && jobs[0].run_attempt === run.run_attempt &&
        jobs[0].head_sha === donor && jobs[0].status === "completed" && jobs[0].conclusion === "success" && jobs[0].runner_id > 0 &&
        steps.length === 1 && steps[0].status === "completed" && steps[0].conclusion === "success";
    };
    if (!executed("plan")) continue;
    for (const lane of eligible.filter(lane => !markers[lane.lane] && executed(lane.lane))) {
      if (lane.artifact && !retained(run.id, lane.artifact, run.run_attempt, donor)) continue;
      markers[lane.lane] = { run_id: run.id, run_attempt: run.run_attempt, source_commit: donor };
    }
    if (eligible.every(lane => markers[lane.lane])) break;
  }
  return Object.keys(markers).length ? markers : decline("no directly executed successful attempt with retained artifacts");
}

// Match `git ls-tree -r -z --full-tree`: tree directories are not records,
// paths are unquoted, and recursive entries have bytewise Git path order.
export function remoteTreeEntries(response, sourceTree) {
  if (response.sha !== sourceTree || response.truncated !== false || !Array.isArray(response.tree)) {
    throw new Error("donor Git tree is incomplete or belongs to another source");
  }
  const entries = response.tree;
  const paths = new Set();
  for (const entry of entries) {
    if (!/^[0-9a-f]{40}$/.test(entry.sha) ||
      !((entry.type === "blob" && ["100644", "100755", "120000"].includes(entry.mode)) || (entry.type === "commit" && entry.mode === "160000") || (entry.type === "tree" && entry.mode === "040000")) ||
      typeof entry.path !== "string" || /[\0\uFFFD\uD800-\uDFFF]/u.test(entry.path) ||
      entry.path.split("/").some(part => !part || part === "." || part === "..") || paths.has(entry.path)) {
      throw new Error("donor Git tree has an invalid entry");
    }
    paths.add(entry.path);
  }
  const leaves = entries.filter(entry => entry.type !== "tree");
  const leafPaths = new Set(leaves.map(entry => entry.path));
  for (const entry of entries) {
    const parts = entry.path.split("/");
    for (let index = 1; index < parts.length; index++) {
      if (leafPaths.has(parts.slice(0, index).join("/"))) throw new Error("donor Git tree has an invalid entry prefix");
    }
  }
  return leaves.sort((a, b) => Buffer.compare(Buffer.from(a.path), Buffer.from(b.path)))
    .map(entry => `${entry.mode} ${entry.type} ${entry.sha}\t${entry.path}`);
}

// Cached lane markers are not proof that their enclosing run finished.
// Admission requires the exact successful attempt and checked-out source.
export function completedQualificationMarker(marker, run, repository) {
  return run?.id === marker.run_id && run.run_attempt === marker.run_attempt &&
    run.status === "completed" && run.conclusion === "success" &&
    run.head_sha === marker.source_commit && run.head_repository?.full_name === repository;
}

function recordedMarker(lane) {
  const path = `.qualification/${lane}.json`;
  if (!existsSync(path)) return null;
  try {
    const recorded = JSON.parse(readFileSync(path, "utf8"));
    return Number.isSafeInteger(recorded.run_id) && recorded.run_id > 0 &&
      Number.isSafeInteger(recorded.run_attempt) && recorded.run_attempt > 0 && /^[0-9a-f]{40}$/.test(recorded.source_commit)
      ? recorded
      : null;
  } catch {
    return null;
  }
}

function select() {
  const force = process.env.FORCE === "true";
  const event = classifyQualificationEvent({
    eventName: process.env.GITHUB_EVENT_NAME,
    ref: process.env.GITHUB_REF,
    force,
  });
  const pullRequest = event === qualificationEventKinds.pullRequest;
  const mainPush = event === qualificationEventKinds.mainPush;
  const fullQualification = requiresFullQualification(event);
  const lanes = readLanes();
  let donorMarkers;
  const artifacts = new Map();
  const attempts = new Map();
  const completed = marker => {
    const key = `${marker.run_id}/${marker.run_attempt}`;
    if (!attempts.has(key)) {
      try { attempts.set(key, gh(`repos/${process.env.GITHUB_REPOSITORY}/actions/runs/${marker.run_id}/attempts/${marker.run_attempt}`)); }
      catch (error) {
        console.error(`qualification attempt ${key} unavailable: ${error instanceof Error ? error.message : String(error)}`);
        attempts.set(key, null);
      }
    }
    return completedQualificationMarker(marker, attempts.get(key), process.env.GITHUB_REPOSITORY);
  };
  const retained = (id, prefix, attempt, commit) => {
    const key = JSON.stringify([id, prefix, attempt, commit]);
    if (!artifacts.has(key)) artifacts.set(key, retainedArtifact(id, prefix, attempt, commit));
    return artifacts.get(key);
  };
  const marker = lane => {
    let cached = recordedMarker(lane);
    if (cached && !completed(cached)) cached = null;
    const artifact = lanes.find(definition => definition.lane === lane)?.artifact;
    if (mainPush && cached && artifact && !retained(cached.run_id, artifact, cached.run_attempt, cached.source_commit)) cached = null;
    if (cached || !mainPush) return cached;
    if (!donorMarkers) {
      try { donorMarkers = mergedPullRequestMarkers(lanes, process.env.GITHUB_REPOSITORY, git("rev-parse", "HEAD").trim(), { retained }); }
      catch (error) { console.error(`merged PR reuse unavailable: ${error instanceof Error ? error.message : String(error)}`); donorMarkers = {}; }
    }
    return donorMarkers[lane] ?? null;
  };
  const { matrix, reused } = chooseLanes(lanes, {
    force,
    mainPush,
    fullQualification,
    pullRequest,
    coreOnly: !fullQualification,
    marker,
    retained,
  });
  for (const [lane, source] of Object.entries(reused)) {
    console.error(`${lane}: reused from run ${source.run_id} attempt ${source.run_attempt}`);
  }
  output("matrix", matrix);
  output("reused", reused);
}

// Writes a marker for every lane that qualified in this run, so later runs
// observing the same inputs reuse it.
function record() {
  const repository = process.env.GITHUB_REPOSITORY;
  const runId = Number(process.env.GITHUB_RUN_ID);
  const runAttempt = Number(process.env.GITHUB_RUN_ATTEMPT);
  const reused = JSON.parse(process.env.REUSED || "{}");
  const lanes = readLanes();
  const names = new Set(lanes.map(lane => lane.lane));
  const jobs = gh(`repos/${repository}/actions/runs/${runId}/attempts/${runAttempt}/jobs?per_page=100`).jobs;
  const recorded = [];
  let sourceCommit;
  mkdirSync(".qualification", { recursive: true });
  // Promote validated core reuse into main's cache namespace. A later PR can
  // then restore the same proof without following another PR's cache scope.
  if (classifyQualificationEvent({ eventName: process.env.GITHUB_EVENT_NAME, ref: process.env.GITHUB_REF, force: process.env.FORCE === "true" }) === qualificationEventKinds.mainPush) {
    for (const lane of lanes.filter(lane => reusableCore(lane) && reused[lane.lane])) {
      const { run_id, run_attempt, source_commit } = reused[lane.lane];
      writeFileSync(`.qualification/${lane.lane}.json`, JSON.stringify({ run_id, run_attempt, source_commit }));
      recorded.push(lane.lane);
    }
  }
  for (const job of jobs) {
    if (!names.has(job.name) || job.conclusion !== "success" || reused[job.name]) continue;
    sourceCommit ??= git("rev-parse", "HEAD").trim();
    writeFileSync(`.qualification/${job.name}.json`, JSON.stringify({ run_id: runId, run_attempt: runAttempt, source_commit: sourceCommit }));
    recorded.push(job.name);
  }
  output("recorded", recorded);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const command = process.argv[2];
  if (command === "fingerprints") fingerprints();
  else if (command === "select") select();
  else if (command === "record") record();
  else throw new Error("usage: plan-qualification.mjs fingerprints|select|record");
}
