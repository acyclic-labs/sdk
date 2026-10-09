import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildContractTargets, runContractTarget } from "./contract-targets.mjs";

// Rust-owned contracts mirrored into TypeScript. Each module lists the Rust
// targets it reads as [package, kind, name] and renders their stdout into
// repository-relative files.
const modules = [
  "generate-runtime-routes.mjs",
  "filesystem-napi-types.mjs",
  "generate-filesystem-defaults.mjs",
  "generate-filesystem-hosted-contract.mjs",
  "generate-filesystem-git-compat-contract.mjs",
  "generate-inference-defaults.mjs",
  "generate-inference-terminal-metadata.mjs",
  "generate-inference-fixed-width-metadata.mjs",
  "generate-stream-http-contract.mjs",
  "generate-stream-token-operations.mjs",
  "generate-harness-limits-contract.mjs",
  "generate-harness-model-fixtures.mjs",
  "generate-harness-child-page-contract.mjs",
  "generate-harness-private-directory-page-contract.mjs",
  "generate-harness-conversation-page-contract.mjs",
  "generate-machines-managed-oci-contract.mjs",
  "generate-observe.mjs",
];

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const mode = process.argv[2] ?? "write";
if (mode !== "write" && mode !== "check") throw new Error("usage: generate-contracts.mjs write|check");
// One build compiles every contract target from current Rust source; the
// emitted executables are then run directly instead of one `cargo run` each.
// Selected packages omit defaults; Cargo still unifies dependency features.
/** @type {{ revision: number, status: string, phase: string, mode: string, source_commit: string | null, run_id: string | null, cargo_version: string, rustc_version: string, rustc_error?: string, toolchain_ms: number, rustc_ms: number, metadata_ms: number, build_ms: number, artifacts: { package_id: string, name: string, kinds: string[], features: string[], fresh: boolean, executable: string | null, requested: boolean }[], generators: { name: string, execute_ms: number, render_compare_ms: number }[] }} */
const report = { revision: 1, status: "incomplete", phase: "start", mode, source_commit: null, run_id: process.env.GITHUB_RUN_ID ?? null, cargo_version: "", rustc_version: "", toolchain_ms: 0, rustc_ms: 0, metadata_ms: 0, build_ms: 0, artifacts: [], generators: [] };
const started = performance.now();
const saveReport = () => {
  if (!process.env.SDK_TEMP_DIR) return;
  const directory = join(process.env.SDK_TEMP_DIR, "observability");
  mkdirSync(directory, { recursive: true });
  const path = join(directory, "generated-contracts.json");
  const temporary = `${path}.${process.pid}.tmp`;
  writeFileSync(temporary, `${JSON.stringify({ ...report, total_ms: performance.now() - started })}\n`);
  renameSync(temporary, path);
};
// Replace any earlier success before running; abrupt process termination then
// leaves an explicitly incomplete report rather than a stale successful one.
saveReport();
try {
  report.phase = "source";
  saveReport();
  const source = spawnSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" });
  if (source.error || source.status !== 0) throw new Error("cannot identify contract source checkout");
  report.source_commit = source.stdout.trim();
  if (process.env.CI_HEAD_SHA && report.source_commit !== process.env.CI_HEAD_SHA) throw new Error("contract checkout differs from CI_HEAD_SHA");
  report.phase = "imports";
  saveReport();
  const generators = await Promise.all(modules.map(name => import(`./${name}`)));
  const targets = generators.flatMap(generator => generator.rust ?? []);
  const executables = buildContractTargets(root, targets, process.env.ACYCLIC_CARGO_BIN || "cargo", report, saveReport);
  let targetIndex = 0;

  const stale = [];
  for (const [index, generator] of generators.entries()) {
    const executionStart = performance.now();
    report.phase = `execute:${modules[index]}`;
    saveReport();
    const stdout = (generator.rust ?? []).map(() => runContractTarget(root, executables[targetIndex++]));
    const execute_ms = performance.now() - executionStart;
    const renderStart = performance.now();
    report.phase = `render:${modules[index]}`;
    saveReport();
    const files = await generator.render(stdout, root);
    for (const [relative, content] of Object.entries(files)) {
      const path = join(root, relative);
      if (mode === "write") writeFileSync(path, content);
      else if (!existsSync(path) || !Buffer.from(content).equals(readFileSync(path))) stale.push(relative);
    }
    report.generators.push({ name: modules[index], execute_ms, render_compare_ms: performance.now() - renderStart });
    saveReport();
  }
  report.phase = "drift-check";
  saveReport();
  if (stale.length > 0) throw new Error(`generated contracts are stale; run bun run generate:\n${stale.join("\n")}`);
  report.status = "success";
} finally {
  if (report.status !== "success") report.status = "failed";
  const total_ms = performance.now() - started;
  console.error(`Rust contracts: ${report.status}; metadata ${report.metadata_ms.toFixed(0)}ms; build ${report.build_ms.toFixed(0)}ms; total ${total_ms.toFixed(0)}ms`);
  saveReport();
}
