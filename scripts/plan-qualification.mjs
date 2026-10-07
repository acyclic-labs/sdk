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

// Bump to invalidate every recorded marker at once.
const SCHEMA = "sdk-qualification-v2";

const documentation = path =>
  /^(README|CONTRIBUTING|SECURITY)\.md$/.test(path) || /^docs\/[^/]+\.md$/.test(path);
const qualificationDefinition = path =>
  path === ".github/workflows/qualification.yml" ||
  path === ".github/qualification-lanes.json" ||
  path.startsWith(".github/actions/");
const unrelatedGithub = path => path.startsWith(".github/") && !qualificationDefinition(path);
const standaloneProjects = path => path.startsWith("arena/") || path.startsWith("examples/");

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
    standaloneProjects(path) ||
    (path.startsWith("typescript/") && path !== "typescript/packages/filesystem/package.json") ||
    path.startsWith("languages/") ||
    path.startsWith("ffi/") ||
    ["bun.lock", "package.json", "tsconfig.json", "tsconfig.base.json"].includes(path),
  // The TypeScript workspace and everything its WASM builds, generated
  // contracts and conformance servers compile: every Rust crate except those
  // no package reads, and no Rust integration tests.
  typescript: path =>
    ignored.product(path) ||
    /^rust\/crates\/(conformance|harness-codex|machines-daytona|sdk-docs)\//.test(path) ||
    /^rust\/crates\/[^/]+\/(tests|benches)\//.test(path) ||
    ["plugin/", "languages/", "ffi/"].some(prefix => path.startsWith(prefix)),
  // Rust plus the TypeScript workspace.
  product: path => documentation(path) || unrelatedGithub(path) || standaloneProjects(path),
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
      artifact = retained(source.run_id, lane.artifact);
      if (!artifact) source = null;
    }
    if (source) reused[lane.lane] = { run_id: source.run_id, run_attempt: source.run_attempt, artifact };
    else matrix.push(lane);
  }
  return { matrix, reused };
}

const git = (...args) => execFileSync("git", args, { encoding: "utf8", maxBuffer: 1 << 28 });
const gh = path => JSON.parse(execFileSync("gh", ["api", "--method", "GET", path], { encoding: "utf8" }));
const output = (name, value) => {
  const text = typeof value === "string" ? value : JSON.stringify(value);
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${text}\n`);
  console.log(`${name}=${text}`);
};
const readLanes = () => JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));

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
function retainedArtifact(runId, prefix) {
  let artifacts;
  try {
    artifacts = gh(
      `repos/${process.env.GITHUB_REPOSITORY}/actions/runs/${runId}/artifacts?per_page=100`,
    ).artifacts;
  } catch (error) {
    console.error(`artifact lookup for run ${runId} failed: ${error.message}`);
    return "";
  }
  const pattern = new RegExp(`^${prefix}-${runId}-(\\d+)$`);
  const candidates = artifacts
    .filter(artifact => !artifact.expired && pattern.test(artifact.name))
    .sort((a, b) => Number(b.name.match(pattern)[1]) - Number(a.name.match(pattern)[1]));
  return candidates[0]?.name ?? "";
}

function recordedMarker(lane) {
  const path = `.qualification/${lane}.json`;
  if (!existsSync(path)) return null;
  try {
    const recorded = JSON.parse(readFileSync(path, "utf8"));
    return Number.isSafeInteger(recorded.run_id) && Number.isSafeInteger(recorded.run_attempt)
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
  const { matrix, reused } = chooseLanes(readLanes(), {
    force,
    mainPush,
    fullQualification,
    pullRequest,
    coreOnly: !fullQualification,
    marker: recordedMarker,
    retained: retainedArtifact,
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
  const names = new Set(readLanes().map(lane => lane.lane));
  const jobs = gh(`repos/${repository}/actions/runs/${runId}/attempts/${runAttempt}/jobs?per_page=100`).jobs;
  const recorded = [];
  mkdirSync(".qualification", { recursive: true });
  for (const job of jobs) {
    if (!names.has(job.name) || job.conclusion !== "success" || reused[job.name]) continue;
    writeFileSync(`.qualification/${job.name}.json`, JSON.stringify({ run_id: runId, run_attempt: runAttempt }));
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
