import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile, mkdir } from "node:fs/promises";
import { join, resolve } from "node:path";

// This is a bounded compiler mutation control. It does not parse Rust or emit
// replacement schemas: each case edits one canonical Rust declaration and
// invokes the maintained Cargo/WASM/N-API build graph. The diagnostics record
// whether a new source field can reach every binding without hand-maintained
// mirrors.
const args = process.argv.slice(2);
const argument = name => {
  const index = args.indexOf(name);
  if (index < 0 || args[index + 1] === undefined) throw new Error(`missing ${name}`);
  return args[index + 1];
};

const sourceRoot = resolve(argument("--source-root"));
const sourceCommit = argument("--source-commit");
const outputRoot = resolve(argument("--output"));
const expectPropagation = args.includes("--expect-propagation");
const projectionOnly = args.includes("--projection-only");
if (!/^[0-9a-f]{40}$/i.test(sourceCommit)) throw new Error("--source-commit must be a full Git revision");
await mkdir(outputRoot, { recursive: true });

const run = (file, commandArgs, options = {}) => {
  const result = spawnSync(file, commandArgs, {
    cwd: options.cwd,
    env: options.env,
    encoding: "utf8",
    maxBuffer: 128 * 1024 * 1024,
  });
  if (result.error) throw result.error;
  return { status: result.status ?? 1, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
};

const cases = [
  {
    name: "file-kind-wasm-projection",
    relative: "rust/crates/filesystem/src/kernel/types.rs",
    marker: "serde(rename_all = \"kebab-case\")",
    replacement: "serde(rename_all = \"snake_case\")",
    command: ["check", "--locked", "-p", "acyclic-fs-wasm", "--target", "wasm32-unknown-unknown"],
    expectedDiagnostics: [],
    mutationClass: "canonical-projection",
    mustCompile: true,
  },
  {
    name: "file-kind-napi-projection",
    relative: "rust/crates/filesystem/src/kernel/types.rs",
    marker: "napi_derive::napi(string_enum = \"kebab-case\")",
    replacement: "napi_derive::napi(string_enum = \"snake_case\")",
    command: ["check", "--locked", "-p", "acyclic-fs-napi"],
    expectedDiagnostics: [],
    mutationClass: "canonical-projection",
    mustCompile: true,
  },
  {
    name: "payload-kind-wasm-projection",
    relative: "rust/crates/filesystem/src/kernel/file_table.rs",
    marker: "serde(rename_all = \"kebab-case\")",
    replacement: "serde(rename_all = \"snake_case\")",
    command: ["check", "--locked", "-p", "acyclic-fs-wasm", "--target", "wasm32-unknown-unknown"],
    expectedDiagnostics: [],
    mutationClass: "canonical-projection",
    mustCompile: true,
  },
  {
    name: "payload-kind-napi-projection",
    relative: "rust/crates/filesystem/src/kernel/file_table.rs",
    marker: "napi_derive::napi(string_enum = \"kebab-case\")",
    replacement: "napi_derive::napi(string_enum = \"snake_case\")",
    command: ["check", "--locked", "-p", "acyclic-fs-napi"],
    expectedDiagnostics: [],
    mutationClass: "canonical-projection",
    mustCompile: true,
  },
  {
    name: "file-kind",
    relative: "rust/crates/filesystem/src/kernel/types.rs",
    marker: "    MountBoundary,",
    replacement: "    MountBoundary,\n    SyntheticAuthorityKind,",
    expectedDiagnostics: ["non-exhaustive patterns", "filesystem-wasm", "filesystem-napi"],
  },
  {
    name: "payload-kind",
    relative: "rust/crates/filesystem/src/kernel/file_table.rs",
    marker: "    ReparsePoint {",
    replacement: "    SyntheticAuthorityPayload,\n    ReparsePoint {",
    expectedDiagnostics: ["non-exhaustive patterns"],
  },
  {
    name: "work-counter",
    relative: "rust/crates/filesystem/src/performance.rs",
    marker: "    pub materializations: u64,",
    replacement: "    pub materializations: u64,\n    /// Mutation-control-only counter; production source is restored after the case.\n    pub synthetic_authority_counter: u64,",
    expectedDiagnostics: ["no field `synthetic_authority_counter`", "missing field", "cannot find"],
  },
];

const baseGit = ["-C", sourceRoot];
const actual = execFileSync("git", [...baseGit, "rev-parse", "--verify", "HEAD"], { encoding: "utf8" }).trim();
assert.equal(actual, sourceCommit, `source HEAD differs from requested mutation baseline: ${actual}`);

const fixtureRoot = await mkdtemp(join("Q:/sdk/work", "fs-authority-mutation-"));
const results = [];
const propagationFailures = [];
try {
  for (const testCase of (projectionOnly ? cases.filter(testCase => testCase.mutationClass === "canonical-projection") : cases)) {
    const fixture = join(fixtureRoot, testCase.name);
    const add = run("git", ["-C", sourceRoot, "worktree", "add", "--detach", fixture, sourceCommit], { cwd: sourceRoot });
    if (add.status !== 0) throw new Error(`cannot create ${testCase.name} mutation worktree: ${add.stderr}`);
    try {
      const path = join(fixture, testCase.relative);
      const source = await readFile(path, "utf8");
      assert.equal(source.split(testCase.marker).length, 2, `${testCase.name} mutation marker must be unique`);
      await writeFile(path, source.replace(testCase.marker, testCase.replacement));
      const target = join(fixtureRoot, `${testCase.name}-target`);
      const check = run("cargo", [
        ...(testCase.command ?? ["check", "--locked", "-p", "acyclic-fs-wasm", "-p", "acyclic-fs-napi"]),
        "--target-dir", target,
      ], { cwd: fixture, env: { ...process.env, CARGO_TARGET_DIR: target } });
      const diagnostics = `${check.stdout}\n${check.stderr}`;
      await writeFile(join(outputRoot, `${testCase.name}.log`), diagnostics);
      const diagnosticSha256 = createHash("sha256").update(diagnostics).digest("hex");
      const expected = testCase.expectedDiagnostics.some(needle => diagnostics.includes(needle));
      results.push({
        name: testCase.name,
        cargo_status: check.status,
        expected_manual_mirror_blocker: !expectPropagation && expected,
        expected_propagation: expectPropagation,
        mutation_class: testCase.mutationClass ?? "semantic-closure",
        generator_diagnostics: /(?:error(?:\[[^\]]+\])?:[^\r\n]*\b(?:tsify|napi_derive)\b|cannot find module or crate `tsify`|unresolved import[^\r\n]*tsify)/i.test(diagnostics),
        semantic_closure_diagnostics: /non-exhaustive patterns|missing field `synthetic_authority_counter`/.test(diagnostics),
        diagnostic_sha256: diagnosticSha256,
        diagnostic_bytes: Buffer.byteLength(diagnostics),
      });
      if (testCase.mustCompile) {
        if (check.status !== 0) {
          throw new Error(`${testCase.name} valid canonical projection mutation did not compile; inspect ${join(outputRoot, `${testCase.name}.log`)}`);
        }
      } else if (expectPropagation) {
        if (check.status !== 0) {
          propagationFailures.push(`${testCase.name} mutation failed; canonical source addition did not propagate through the binding graph. Inspect ${join(outputRoot, `${testCase.name}.log`)}`);
        }
      } else {
        if (check.status === 0) {
          throw new Error(`${testCase.name} mutation compiled; binding propagation must be inspected before accepting the historical blocker control`);
        }
        if (!expected) {
          throw new Error(`${testCase.name} mutation failed without an expected source-authority diagnostic; inspect ${join(outputRoot, `${testCase.name}.log`)}`);
        }
      }
    } finally {
      const remove = run("git", ["-C", sourceRoot, "worktree", "remove", "--force", fixture], { cwd: sourceRoot });
      if (remove.status !== 0) throw new Error(`cannot remove mutation worktree ${fixture}: ${remove.stderr}`);
    }
  }
  await writeFile(join(outputRoot, "summary.json"), `${JSON.stringify({
    schema: "acyclic.sdk.source-authority-mutation.v1",
    source_commit: sourceCommit,
    cases: results,
    mode: expectPropagation ? "propagation-required" : "historical-blocker",
    interpretation: expectPropagation
      ? "Canonical projection mutations must compile through the maintained binding graph; semantic enum/counter additions are classified separately because exhaustive Rust matches and struct initializers must be updated in the temporary mutant before generator propagation can be assessed. Generated declaration inspection remains a separate check."
      : "Current manual binding mirrors intentionally reject canonical enum/counter additions at compile time. This is a blocker audit, not a qualification receipt.",
  }, null, 2)}\n`);
  if (propagationFailures.length > 0) throw new Error(propagationFailures.join("\n"));
  console.log(JSON.stringify({ source_commit: sourceCommit, cases: results }, null, 2));
} finally {
  await rm(fixtureRoot, { recursive: true, force: true });
}
