import { mkdirSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildContractTargets, captureContractCommand, contractTargetOutputs } from "./contract-targets.mjs";
import { workspaceManifests } from "./package-closure.mjs";
import { checkGenerated, projectGenerated } from "./generated-bindings.mjs";

// Rust-owned contracts mirrored into TypeScript. Each module lists the Rust
// targets it reads as [package, kind, name] and renders their stdout into
// repository-relative files. A module that compiles crates itself lists them
// as `crates`: a memoized generation keys on their sources.
const availableModules = [
  "generate-stream-authorization.mjs",
  "generate-objects-authorization.mjs",
  "generate-actors-authorization.mjs",
  "generate-machines-authorization.mjs",
  "generate-workers-authorization.mjs",
  "generate-filesystem-authorization.mjs",
  "generate-actors-routes.mjs",
  "generate-actors-rpc-inventory.mjs",
  "generate-stream-rpc-inventory.mjs",
  "generate-objects-rpc-inventory.mjs",
  "generate-inference-routes.mjs",
  "filesystem-napi-types.mjs",
  "stream-napi-types.mjs",
  "actors-napi-types.mjs",
  "filesystem-descriptor-digest.mjs",
  "generate-filesystem-defaults.mjs",
  "generate-filesystem-hosted-contract.mjs",
  "generate-filesystem-object-references.mjs",
  "generate-filesystem-git-compat-contract.mjs",
  "generate-inference-defaults.mjs",
  "generate-inference-terminal-metadata.mjs",
  "generate-inference-widths.mjs",
  "generate-stream-http-contract.mjs",
  "generate-stream-token-operations.mjs",
  "generate-harness-model-fixtures.mjs",
  "generate-harness-file-tools-contract.mjs",
  "generate-machines-managed-oci-contract.mjs",
];
const root = join(dirname(fileURLToPath(import.meta.url)), "..");
export function contractModules(families = []) {
  const knownFamilies = workspaceManifests(root);
  // A complete family or one exact contract can be selected. Exact selection
  // keeps a narrow contract update from compiling unrelated bindings.
  const matchesFamily = (name, family) => name === `generate-${family}.mjs` || name.startsWith(`generate-${family}-`) || name.startsWith(`${family}-`);
  for (const family of families) {
    if (!knownFamilies.has(family) && !availableModules.includes(`generate-${family}.mjs`)) {
      throw new Error(`STEP_FAILED contract-selection: unknown family ${JSON.stringify(family)}; recovery: bun scripts/generate-contracts.mjs check`);
    }
  }
  return availableModules.filter(name => !families.length || families.some(family => matchesFamily(name, family)));

}

// Full generation keeps every output. A selection omits outputs only a native build reads unless it asks for them.
export const keepsGenerator = (generator, families, native) => !generator.nativeOnly || native || families.length === 0;

/** The contract modules are the one computed import: every caller loads them here. @param {string[]} names */
export const loadContractModules = names => Promise.all(names.map(name => import(`./${name}`)));

export async function generateContracts(mode = "write", families = [], { native = false } = {}) {
  let modules = contractModules(families);

  if (mode !== "write" && mode !== "check") throw new Error("STEP_FAILED contract-mode: expected write|check; recovery: bun scripts/generate-contracts.mjs check");
  // One Bazel build brings every contract target up to date from current Rust source; the
  // executables are then run directly, each at most once.
  /** @type {{ revision: number, status: string, phase: string, mode: string, source_commit: string | null, run_id: string | null, build_ms: number, labels: string[], generators: { name: string, execute_ms: number, render_compare_ms: number }[] }} */
  const report = { revision: 3, status: "incomplete", phase: "start", mode, source_commit: null, run_id: process.env.GITHUB_RUN_ID ?? null, build_ms: 0, labels: [], generators: [] };
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
    report.source_commit = captureContractCommand(root, "git", ["rev-parse", "HEAD"], "source").trim();
    if (process.env.CI_HEAD_SHA && report.source_commit !== process.env.CI_HEAD_SHA) throw new Error("contract checkout differs from CI_HEAD_SHA");
    report.phase = "imports";
    saveReport();
    let generators = await loadContractModules(modules);
    const kept = generators.map(generator => keepsGenerator(generator, families, native));
    modules = modules.filter((_, index) => kept[index]);
    generators = generators.filter((_, index) => kept[index]);
    const targets = [...new Set(generators.flatMap(generator => generator.rust ?? []))];
    report.labels = targets;
    const executables = targets.length === 0 ? [] : buildContractTargets(root, targets, report, saveReport);
    const targetIndex = new Map(targets.map((target, index) => [target, index]));
    const consumers = generators.flatMap(generator => (generator.rust ?? [])
      .map(target => [targetIndex.get(target), generator.rustPayload]));
    const outputFor = contractTargetOutputs(root, executables, consumers);

    const stale = [];
    for (const [index, generator] of generators.entries()) {
      const executionStart = performance.now();
      report.phase = `execute:${modules[index]}`;
      saveReport();
      const stdout = (generator.rust ?? []).map(target => {
        const targetSlot = targetIndex.get(target);
        return outputFor(targetSlot, generator.rustPayload);
      });
      const execute_ms = performance.now() - executionStart;
      const renderStart = performance.now();
      report.phase = `render:${modules[index]}`;
      saveReport();
      const files = await generator.render(stdout, root);
      for (const [relative, content] of Object.entries(files)) {
        const path = join(root, relative);
        const baseline = relative.startsWith("fixtures/") || relative.startsWith("rust/crates/harness/fixtures/");
        try {
          if (baseline && mode === "check") checkGenerated(path, content);
          else projectGenerated(path, content, mode === "check");
        } catch (error) {
          if (!error.message.startsWith("STEP_FAILED generated-artifact:")) throw error;
          stale.push(relative);
        }
      }
      report.generators.push({ name: modules[index], execute_ms, render_compare_ms: performance.now() - renderStart });
      saveReport();
    }
    report.phase = "drift-check";
    saveReport();
    if (stale.length > 0) throw new Error(`STEP_FAILED generated-contracts: stale artifacts ${stale.join(", ")}; recovery: bun run generate`);
    report.status = "success";
  } finally {
    if (report.status !== "success") report.status = "failed";
    const total_ms = performance.now() - started;
    console.error(`Rust contracts: ${report.status}; build ${report.build_ms.toFixed(0)}ms; total ${total_ms.toFixed(0)}ms`);
    saveReport();
  }

}

if (import.meta.main) {
  const args = process.argv.slice(3);
  await generateContracts(process.argv[2] ?? "write", args.filter(arg => arg !== "--native"), { native: args.includes("--native") });
}
