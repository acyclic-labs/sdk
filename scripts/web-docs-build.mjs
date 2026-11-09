import { spawnSync } from "node:child_process";
import { existsSync, globSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { packageClosure, workspaceManifests } from "./package-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * @param {string} operation
 * @param {string} [selected]
 */
export function webDocsCommands(operation, selected) {
  const generate = ["bun", "run", "generate"];
  const wasm = ["bun", "run", "build:wasm"];
  const compile = ["node", "node_modules/typescript/bin/tsc", "-b", "--force"];
  if (operation === "build") return [generate, wasm, compile];
  if (operation === "check") return [
    ["node", "scripts/generate-sdk-readmes.mjs", "check"], generate, wasm,
    [...compile, "--pretty", "false"],
    ["node", "node_modules/typescript/bin/tsc", "-p", "scripts", "--pretty", "false"],
    ["bun", "run", "check:docs"],
  ];
  if (operation === "package" && workspaceManifests(root).has(selected)) {
    const manifest = workspaceManifests(root).get(selected);
    return [["bun", "run", "--filter", manifest.name, "build"]];
  }
  if (operation === "readmes") return [["node", "scripts/check-sdk-readmes.mjs", "compile"]];
  throw new Error("usage: web-docs-build.mjs build|check|readmes|package PACKAGE");
}

// Declare complete public build products, including generated source and buildinfo.
// Cargo's target directory remains the shared compiler's private warm state.
/** @param {string} [selected] */
export function webDocsOutputs(selected) {
  const packages = selected ? packageClosure(root, selected)
    : new Set([...workspaceManifests(root).keys()].flatMap(name => [...packageClosure(root, name)]));
  const outputs = ["generated", ".tools/sdk-readme-examples"];
  for (const name of packages) {
    const directory = `typescript/packages/${name}`;
    outputs.push(...["dist", "generated", "src/generated", "test/generated", "tsconfig.tsbuildinfo"]
      .map(suffix => `${directory}/${suffix}`));
  }
  // Ignored standalone projections have their canonical declaration in .gitignore.
  for (const line of readFileSync(join(root, ".gitignore"), "utf8").split(/\r?\n/)) {
    const match = /^\/typescript\/packages\/([^/]+)\/.*\.ts$/.exec(line);
    if (match && packages.has(match[1])) outputs.push(line.slice(1));
  }
  for (const manifest of globSync("rust/crates/*/Cargo.toml", { cwd: root })) {
    outputs.push(`${dirname(manifest).replaceAll("\\", "/")}/src/generated`);
  }
  return outputs.sort();
}

async function main() {
  const [operation, selected] = process.argv.slice(2);
  const commands = webDocsCommands(operation, selected);
  const context = process.env.SDK_WEB_DOCS_BUILD_CONTEXT;
  const module = process.env.SDK_ARTIFACT_BUILD_MODULE ?? "/mnt/bb/cx/artifact-build.mjs";
  let status = "FAIL";
  const started = performance.now();
  try {
    if (context && process.env.SDK_WEB_DOCS_BUILD_ACTIVE !== "1") {
      if (!existsSync(module)) throw new Error("declared artifact builder is missing");
      const { artifactBuild } = await import(pathToFileURL(resolve(module)).href);
      process.exitCode = await artifactBuild({ id: `sdk/webdocs/${operation}/${selected ?? "all"}`,
        command: process.execPath, args: [fileURLToPath(import.meta.url), operation, ...(selected ? [selected] : [])],
        cwd: root, env: { ...process.env, SDK_WEB_DOCS_BUILD_ACTIVE: "1" }, context,
        outputs: operation === "readmes" ? [".tools/sdk-readme-examples"] : webDocsOutputs(selected) });
    } else {
      for (const [command, ...args] of commands) {
        const result = spawnSync(command === "node" ? process.execPath : command, args,
          { cwd: root, stdio: "inherit" });
        if (result.error) throw result.error;
        if (result.status !== 0) { process.exitCode = result.status ?? 1; break; }
      }
    }
    if (!process.exitCode) status = "PASS";
  } finally {
    console.log("SDK_WEB_DOCS_RESULT " + JSON.stringify({ phase: operation,
      status, seconds: (performance.now() - started) / 1000 }));
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) await main();
