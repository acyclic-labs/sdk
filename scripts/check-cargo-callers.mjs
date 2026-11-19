// Repository tooling builds and runs Rust through Bazel. This check refuses a Cargo invocation in any tracked
// tooling file (scripts, workflows, package manifests, Bazel files) that is not one of the recorded remaining
// callers, and refuses a recorded caller that no longer invokes Cargo, so the record only shrinks.
// Documentation, Rust sources (agents run Cargo in the repositories they work on), fixtures, corpora,
// evidence and vendored code are not tooling.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { repositoryRoot as root, starlarkCargoCallers, trackedFiles } from "./policy-core.mjs";

export const RECORD = "scripts/cargo-callers.json";
const TOOLING = /(?:\.(?:mjs|cjs|js|mts|ts|sh|ps1|py|ya?ml|bzl|bazel)|(?:^|\/)package\.json|(?:^|\/)\.bazelrc)$/;
const NOT_TOOLING = /(?:^|\/)(?:fixtures|corpus|evidence|vendor|node_modules|golden)(?:\/|$)/;
// A Cargo subcommand after any toolchain and global flags, or a quoted program name.
const SUBCOMMAND = /\bcargo(?:\s+(?:\+\S+|-{1,2}[A-Za-z][\w-]*(?:[= ][^\s-]\S*)?))*\s+(?:add|bench|build|check|clean|clippy|config|deny|doc|fetch|fix|fmt|fuzz|generate-lockfile|init|install|kani|llvm-cov|locate-project|metadata|miri|new|nextest|package|pkgid|publish|remove|run|rustc|rustdoc|search|test|tree|uninstall|update|vendor|verify-project|version|xtask|yank)(?![\w-])/;
const PROGRAM = /["'`]cargo(?:\.exe)?["'`]/;
const STARLARK = /\.(?:bzl|bazel)$/;

/** Tracked tooling files that invoke Cargo, sorted. @param {string} root */
export function cargoCallers(root) {
  const files = trackedFiles(root).filter(file => TOOLING.test(file) && !NOT_TOOLING.test(file));
  const starlark = Object.create(null);
  const callers = files.filter(file => {
    let text;
    try { text = readFileSync(join(root, file), "utf8"); }
    catch (error) { if (error.code === "ENOENT") return false; throw error; }
    if (SUBCOMMAND.test(text)) return true;
    if (!PROGRAM.test(text)) return false;
    if (!STARLARK.test(file)) return true;
    starlark[file] = text;
    return false;
  });
  return [...callers, ...starlarkCargoCallers(starlark)].sort();
}

/** Refusals of the current tree against the record. @param {string} root @param {string[]} recorded */
export function cargoCallerFindings(root, recorded) {
  const found = cargoCallers(root), record = new Set(recorded);
  return [
    ...found.filter(file => !record.has(file)).map(file => `${file} invokes Cargo; build or run it through a Bazel target`),
    ...recorded.filter(file => !found.includes(file)).map(file => `${file} no longer invokes Cargo; delete it from ${RECORD}`),
  ];
}

if (import.meta.main) {
  const findings = cargoCallerFindings(root, JSON.parse(readFileSync(join(root, RECORD), "utf8")));
  if (findings.length) {
    console.error(findings.map(finding => `STEP_FAILED cargo-callers: ${finding}`).join("\n"));
    process.exitCode = 1;
  }
}
