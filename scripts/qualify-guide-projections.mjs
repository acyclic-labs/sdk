#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, extname, join, relative, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

// Resolve from the script directory so this remains correct when invoked from a docs checkout, a release archive, or a clean worktree.
const repo = resolve(fileURLToPath(new URL(".", import.meta.url)), "..");
const cargo = process.env.CARGO_BIN ?? (process.platform === "win32" ? join(process.env.USERPROFILE ?? "C:\\Users\\varun", ".cargo", "bin", "cargo.exe") : "cargo");
const args = new Map();
for (let i = 2; i < process.argv.length; i += 1) {
  const value = process.argv[i];
  if (value === "--execute" || value === "--strict") args.set(value, true);
  else if (value.startsWith("--")) args.set(value, process.argv[++i]);
}

const output = resolve(args.get("--output") ?? join(repo, "work", "guide-projection-qualification"));
const snippets = join(output, "snippets");
mkdirSync(snippets, { recursive: true });

function command(name, commandArgs, cwd = repo) {
  const result = spawnSync(name, commandArgs, { cwd, encoding: "utf8", windowsHide: true });
  return {
    command: [name, ...commandArgs].join(" "),
    exitCode: result.status ?? 127,
    stdout: result.stdout ?? "",
    stderr: result.stderr ?? (result.error?.message ?? ""),
  };
}

function allFiles(root) {
  const files = [];
  if (!existsSync(root)) return files;
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) files.push(...allFiles(path));
    else files.push(path);
  }
  return files;
}

function artifact(root, pattern) {
  const normalized = pattern.replaceAll("\\", "/");
  if (!normalized.includes("*")) {
    const path = resolve(root, normalized);
    return existsSync(path) && statSync(path).isFile() ? path : null;
  }
  const expression = new RegExp(`^${normalized.split("*").map((part) => part.replace(/[.+?^${}()|[\\]\\]/g, "\\$&")).join(".*")}$`, "i");
  return allFiles(root).find((path) => expression.test(relative(root, path).replaceAll("\\", "/"))) ?? null;
}

function extension(language) {
  return ({ rust: ".rs", python: ".py", typescript: ".ts", go: ".go", java: ".java", csharp: ".cs", ruby: ".rb", dart: ".dart", php: ".php" })[language];
}

function compile(language, file, cwd) {
  switch (language) {
    case "rust": return command("rustfmt", ["--check", file], cwd);
    case "python": return command("python", ["-m", "py_compile", file], cwd);
    case "typescript": return command("bun", ["build", file, "--no-bundle", "--target=node"], cwd);
    case "go": return command("go", ["fmt", file], cwd);
    case "java": return command("javac", [file], cwd);
    case "csharp": return command("dotnet", ["build", file, "--nologo", "--verbosity", "quiet"], cwd);
    case "ruby": return command("ruby", ["-c", file], cwd);
    case "dart": return command("dart", ["analyze", file], cwd);
    case "php": return command("php", ["-l", file], cwd);
    default: return { command: "", exitCode: 127, stdout: "", stderr: `unsupported language ${language}` };
  }
}

function execute(language, file, cwd) {
  switch (language) {
    case "python": return command("python", [file], cwd);
    case "typescript": return command("bun", [file], cwd);
    case "go": return command("go", ["run", file], cwd);
    case "ruby": return command("ruby", [file], cwd);
    case "dart": return command("dart", ["run", file], cwd);
    case "php": return command("php", [file], cwd);
    default: return { command: "", exitCode: 125, stdout: "", stderr: `execution is release-only for ${language}` };
  }
}

const projectionInput = args.get("--projections");
const manifestCommand = projectionInput
  ? { exitCode: 0, stdout: readFileSync(resolve(repo, projectionInput), "utf8"), stderr: "", command: `file:${projectionInput}` }
  : command(cargo, ["run", "--locked", "--offline", "--manifest-path", join(repo, "rust", "crates", "sdk-examples", "Cargo.toml"), "--example", "guide-projections"]);
if (manifestCommand.exitCode !== 0) {
  console.error(manifestCommand.stderr);
  process.exit(manifestCommand.exitCode);
}
const projections = JSON.parse(manifestCommand.stdout);
const receipts = [];
for (const projection of projections) {
  const directory = join(snippets, projection.scenario_id, projection.language);
  mkdirSync(directory, { recursive: true });
  const file = join(directory, `${projection.scenario_id}${extension(projection.language)}`);
  writeFileSync(file, projection.code);
  const packageArtifact = artifact(repo, projection.artifact_path);
  const receipt = {
    scenario_id: projection.scenario_id,
    family: projection.family,
    operation: projection.operation,
    language: projection.language,
    mode: projection.mode,
    source: projection.source,
    package_manager: projection.package_manager,
    package_name: projection.package_name,
    artifact_path: projection.artifact_path,
    package_artifact: packageArtifact ? relative(repo, packageArtifact).replaceAll("\\", "/") : null,
    package_sha256: packageArtifact ? createHash("sha256").update(readFileSync(packageArtifact)).digest("hex") : null,
    snippet_path: relative(repo, file).replaceAll("\\", "/"),
  };
  if (!packageArtifact) {
    receipt.status = "artifact-missing";
  } else {
    const checked = compile(projection.language, file, directory);
    receipt.compile = checked;
    receipt.status = checked.exitCode === 0 ? "compiled" : "compile-failed";
    if (receipt.status === "compiled" && args.has("--execute")) {
      const ran = execute(projection.language, file, directory);
      receipt.execution = ran;
      receipt.status = ran.exitCode === 0 ? "executed" : "execution-failed";
    }
  }
  writeFileSync(join(directory, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`);
  receipts.push(receipt);
}

const summary = {
  schema: "acyclic.sdk.guide-projection-qualification.v1",
  source: "rust/crates/sdk-examples/src/guide_projections.rs",
  projection_count: receipts.length,
  compiled: receipts.filter((receipt) => receipt.status === "compiled" || receipt.status === "executed").length,
  executed: receipts.filter((receipt) => receipt.status === "executed").length,
  artifact_missing: receipts.filter((receipt) => receipt.status === "artifact-missing").length,
  failed: receipts.filter((receipt) => receipt.status.endsWith("failed")).length,
  receipts,
};
writeFileSync(join(output, "qualification.json"), `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ ...summary, receipts: undefined }, null, 2));
if (args.has("--strict") && (summary.artifact_missing > 0 || summary.failed > 0 || summary.projection_count !== 54)) process.exit(1);
