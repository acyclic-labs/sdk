// Select the Bazel test targets a change can affect. A changed file is a source
// target of its innermost Bazel package; `rdeps` over the whole workspace then
// names every test that reads it, directly or through a library. A change to a
// build input that every target reads (the module, the Bazel configuration, a
// toolchain, a generated BUILD file or the generator itself) selects every test.
// Files outside every package are not Bazel inputs and select nothing.
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { GENERATED_HEADER as GENERATED, bazelCommand } from "./bazel-executables.mjs";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const GLOBAL_INPUTS = [
  /^(?:MODULE\.bazel(?:\.lock)?|REPO\.bazel|cargo\.MODULE\.bazel)$/,
  /^\.bazel(?:rc|version)$/,
  /(?:^|\/)BUILD\.bazel$/,
  /\.bzl$/,
  /^toolchains\//,
  /^patches\//,
  // Both crate universes' lockfiles: a repin changes every target that reads the universe.
  /^cargo(?:-wasm)?-bazel(?:-lock\.json|\.Cargo\.lock)$/,
  // Repository-rule inputs: no build graph edge names them, so a change to one changes the
  // repositories every target resolves through.
  /^(?:bun\.lock|scripts\/ensure-bun\.(?:sh|ps1))$/,
  /^scripts\/bazel-build-files\.mjs$/,
  /^rust\/vendor\//,
];

function normalize(path) {
  return path.replaceAll("\\", "/").replace(/^\.\//, "");
}

function relativePath(root, path) {
  const absolute = isAbsolute(path) ? path : resolve(root, path);
  const value = relative(root, absolute);
  if (value === "" || value === ".") return "";
  if (value === ".." || value.startsWith(".." + sep) || isAbsolute(value)) return null;
  return normalize(value);
}

/** A build input every Bazel target reads, so a change to it selects every test. */
export function isGlobalInput(path) {
  return GLOBAL_INPUTS.some(pattern => pattern.test(path));
}

function packageOf(root, path) {
  let directory = dirname(path);
  while (true) {
    const build = resolve(root, directory, "BUILD.bazel");
    if (existsSync(build)) return { directory: directory === "." ? "" : directory, generated: readFileSync(build, "utf8").startsWith(GENERATED) };
    if (directory === "" || directory === ".") return undefined;
    directory = dirname(directory);
  }
}

/**
 * The Bazel target a changed file names. A generated package exports every file,
 * so the file itself is the target. A hand-written package may not export it, and
 * a deleted file has no target at all: both select the package's targets instead.
 * @returns {string | undefined}
 */
export function bazelLabel(root, path) {
  const found = packageOf(root, path);
  if (!found) return undefined;
  const prefix = found.directory ? `//${found.directory}` : "//";
  if (!found.generated || !existsSync(resolve(root, path))) return `${prefix}:all`;
  const within = found.directory ? path.slice(found.directory.length + 1) : path;
  return `${prefix}:${within}`;
}

/** The query that names every test reading `labels`, or every test for a global input. */
export function selectionExpression(labels, global) {
  if (global) return "tests(//...)";
  if (!labels.length) return undefined;
  return `tests(rdeps(//..., set(${[...labels].sort().join(" ")})))`;
}

/** Bazel query's label output, without its canonical repository prefix. */
export function parseLabels(output) {
  return [...new Set(output.split(/\r?\n/u).filter(Boolean).map(line => line.replace(/^@@?(?=\/\/)/u, "")))].sort();
}

/**
 * @param {{root: string, changed: string[], query: (expression: string) => string}} options
 * @returns {string[]}
 */
export function affectedTargets({ root, changed, query }) {
  if (!Array.isArray(changed)) throw new Error("changed paths must be an array");
  const labels = new Set();
  let global = false;
  for (const file of changed) {
    const path = relativePath(root, file);
    if (path === null || path === "") {
      global = true;
      continue;
    }
    if (isGlobalInput(path)) {
      global = true;
      continue;
    }
    const label = bazelLabel(root, path);
    if (label) labels.add(label);
  }
  const expression = selectionExpression([...labels], global);
  return expression ? parseLabels(query(expression)) : [];
}

export function changedPathsFromGit(base, head = "HEAD", { cwd = ROOT } = {}) {
  return execFileSync("git", ["diff", "--name-only", "--no-renames", "-z", base, head], {
    cwd, encoding: "utf8", maxBuffer: 64 * 1024 * 1024,
  }).split("\0").filter(Boolean);
}

function parseArguments(argv) {
  const options = { root: ROOT, base: undefined, head: "HEAD", changed: [], json: false, count: false };
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index];
    if (argument === "--root") options.root = resolve(argv[++index] ?? "");
    else if (argument === "--base") options.base = argv[++index];
    else if (argument === "--head") options.head = argv[++index];
    else if (argument === "--changed") options.changed.push(argv[++index]);
    else if (argument === "--json") options.json = true;
    else if (argument === "--count") options.count = true;
    else if (argument === "--help") {
      console.log("usage: affected-bazel-targets.mjs [--root DIR] [--base REF] [--head REF] [--changed PATH ...] [--json|--count]");
      process.exit(0);
    } else if (argument.startsWith("--")) throw new Error(`unknown argument ${argument}`);
    else options.changed.push(argument);
  }
  if (!options.base && !options.changed.length) throw new Error("give --base REF or at least one --changed PATH");
  if (options.changed.some(path => !path)) throw new Error("--changed needs a path");
  return options;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = parseArguments(process.argv.slice(2));
  const changed = [
    ...options.changed,
    ...(options.base ? changedPathsFromGit(options.base, options.head, { cwd: options.root }) : []),
  ];
  const bazel = bazelCommand();
  const query = expression => execFileSync(bazel, [
    "query", "--color=no", "--noshow_progress", "--ui_event_filters=-info", "--output=label", expression,
  ], { cwd: options.root, encoding: "utf8", maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "inherit"] });
  const targets = affectedTargets({ root: options.root, changed, query });
  if (options.json) console.log(JSON.stringify(targets));
  else if (options.count) console.log(targets.length);
  else console.log(targets.join("\n"));
}
