// Plans which SDK qualification lanes must execute for the checked-out tree.
//
// Every lane is fingerprinted from the git blobs it can observe. A lane whose
// fingerprint already qualified (recorded as a cache marker by an earlier run)
// is reused instead of re-executed, and its retained artifact is forwarded into
// this run so release workflows keep finding it under this run's identity.
//
//   plan-qualification.mjs fingerprints   -> keys, lanes
//   plan-qualification.mjs select         -> matrix, reused, trusted
//   plan-qualification.mjs record         -> recorded
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Bump to invalidate every recorded marker at once.
const SCHEMA = "sdk-qualification-v1";

const documentation = path =>
  /^(README|CONTRIBUTING|SECURITY)\.md$/.test(path) || /^docs\/[^/]+\.md$/.test(path);
const qualificationDefinition = path =>
  path === ".github/workflows/qualification.yml" ||
  path === ".github/qualification-lanes.json" ||
  path.startsWith(".github/actions/");
const unrelatedGithub = path => path.startsWith(".github/") && !qualificationDefinition(path);
const standaloneProjects = path => path.startsWith("arena/") || path.startsWith("examples/");

// Each predicate returns true for paths the lane can never observe.
export const ignored = {
  // Cargo, Rust sources, protocol and conformance data, scripts, release metadata.
  rust: path =>
    documentation(path) ||
    unrelatedGithub(path) ||
    standaloneProjects(path) ||
    (path.startsWith("typescript/") && path !== "typescript/packages/filesystem/package.json") ||
    path.startsWith("generated/typescript/") ||
    path.startsWith("languages/") ||
    path.startsWith("ffi/") ||
    ["bun.lock", "package.json", "tsconfig.json"].includes(path),
  // Rust plus the TypeScript workspace.
  product: path => documentation(path) || unrelatedGithub(path) || standaloneProjects(path),
  // Repository-wide metadata, boundary, and license checks.
  repository: path => documentation(path),
  policy: path => documentation(path),
};

// `entries` are `git ls-tree -r` records: "<mode> <type> <object>\t<path>".
export function laneKeys(lanes, entries) {
  const keys = {};
  for (const lane of lanes) {
    const predicate = ignored[lane.inputs];
    if (!predicate) throw new Error(`lane ${lane.lane} names unknown inputs ${lane.inputs}`);
    const digest = createHash("sha256");
    digest.update(`${SCHEMA}\0${JSON.stringify(lane)}\0`);
    for (const entry of entries) {
      if (predicate(entry.slice(entry.indexOf("\t") + 1))) continue;
      digest.update(entry);
      digest.update("\0");
    }
    keys[lane.lane] = `sdk-qualified-${lane.lane}-${digest.digest("hex")}`;
  }
  return keys;
}

// Decides each lane's fate. `marker(lane)` returns the run that recorded the
// lane's fingerprint, if any; `retained(runId, prefix)` names the artifact that
// run still retains, or "".
export function chooseLanes(lanes, { force, mainPush, trusted, marker, retained }) {
  const matrix = [];
  const reused = {};
  for (const lane of lanes) {
    // Source-bound artifacts record the commit they were built from, and
    // releases require that commit to be the main commit being released.
    if (force || (mainPush && lane.source_bound)) {
      matrix.push(lane);
      continue;
    }
    let source = trusted ?? marker(lane.lane);
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
  const keys = laneKeys(lanes, entries);
  output("keys", keys);
  output("lanes", lanes.map(lane => lane.lane));
}

// A squash merge of an up-to-date pull request lands the exact tree that the
// pull request head already qualified. Reuse that run for every lane.
function qualifiedPullRequestRun() {
  const repository = process.env.GITHUB_REPOSITORY;
  const sha = git("rev-parse", "HEAD").trim();
  const tree = git("rev-parse", "HEAD^{tree}").trim();
  let pulls;
  try {
    pulls = gh(`repos/${repository}/commits/${sha}/pulls`);
  } catch (error) {
    console.error(`pull request lookup failed: ${error.message}`);
    return null;
  }
  for (const pull of pulls) {
    if (pull.merge_commit_sha !== sha || pull.base?.ref !== "main") continue;
    const head = pull.head.sha;
    const headTree = gh(`repos/${repository}/git/commits/${head}`).tree.sha;
    if (headTree !== tree) {
      console.error(`#${pull.number} head ${head} tree ${headTree} differs from ${tree}`);
      continue;
    }
    const runs = gh(
      `repos/${repository}/actions/workflows/qualification.yml/runs?head_sha=${head}&event=pull_request&status=success&per_page=20`,
    ).workflow_runs.filter(run => run.head_sha === head && run.conclusion === "success");
    runs.sort((a, b) => b.run_number - a.run_number || b.run_attempt - a.run_attempt);
    if (runs.length > 0) {
      console.error(`reusing #${pull.number} run ${runs[0].id} attempt ${runs[0].run_attempt}`);
      return { run_id: runs[0].id, run_attempt: runs[0].run_attempt };
    }
  }
  return null;
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
  const mainPush = process.env.GITHUB_EVENT_NAME === "push" && process.env.GITHUB_REF === "refs/heads/main";
  const trusted = !force && mainPush ? qualifiedPullRequestRun() : null;
  const { matrix, reused } = chooseLanes(readLanes(), {
    force,
    mainPush,
    trusted,
    marker: recordedMarker,
    retained: retainedArtifact,
  });
  for (const [lane, source] of Object.entries(reused)) {
    console.error(`${lane}: reused from run ${source.run_id} attempt ${source.run_attempt}`);
  }
  // On pull requests, early-start lanes run in their own job that is queued at
  // workflow start and executes only when the plan requires it.
  const early = process.env.GITHUB_EVENT_NAME === "pull_request"
    ? matrix.filter(lane => lane.early_start)
    : [];
  output("matrix", matrix.filter(lane => !early.includes(lane)));
  output("windows", early.some(lane => lane.lane === "windows") ? "true" : "false");
  output("reused", reused);
  output("trusted", trusted ? "true" : "false");
}

// Writes a marker for every lane that qualified in this run, and on main for
// every lane proven by the merged pull request, so later runs observing the
// same inputs reuse it.
function record() {
  const repository = process.env.GITHUB_REPOSITORY;
  const runId = Number(process.env.GITHUB_RUN_ID);
  const runAttempt = Number(process.env.GITHUB_RUN_ATTEMPT);
  const reused = JSON.parse(process.env.REUSED || "{}");
  const trusted = process.env.TRUSTED === "true";
  const names = new Set(readLanes().map(lane => lane.lane));
  const jobs = gh(`repos/${repository}/actions/runs/${runId}/attempts/${runAttempt}/jobs?per_page=100`).jobs;
  const recorded = [];
  mkdirSync(".qualification", { recursive: true });
  for (const job of jobs) {
    if (!names.has(job.name) || job.conclusion !== "success" || reused[job.name]) continue;
    writeFileSync(`.qualification/${job.name}.json`, JSON.stringify({ run_id: runId, run_attempt: runAttempt }));
    recorded.push(job.name);
  }
  if (trusted) {
    for (const [lane, source] of Object.entries(reused)) {
      writeFileSync(
        `.qualification/${lane}.json`,
        JSON.stringify({ run_id: source.run_id, run_attempt: source.run_attempt }),
      );
      recorded.push(lane);
    }
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
