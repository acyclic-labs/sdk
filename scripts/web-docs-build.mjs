import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { workspaceManifests } from "./package-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

/**
 * @param {string} operation
 * @param {string} [selected]
 */
export function webDocsCommands(operation, selected) {
  const generate = ["bun", "run", "generate"];
  const wasm = ["bun", "scripts/build-wasm-groups.mjs"];
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

async function main() {
  const [operation, selected] = process.argv.slice(2);
  const commands = webDocsCommands(operation, selected);
  let status = "FAIL";
  const started = performance.now();
  try {
    for (const [command, ...args] of commands) {
      const result = spawnSync(command === "node" ? process.execPath : command, args,
        { cwd: root, stdio: "inherit" });
      if (result.error) throw result.error;
      if (result.status !== 0) { process.exitCode = result.status ?? 1; break; }
    }
    if (!process.exitCode) status = "PASS";
  } finally {
    console.log("SDK_WEB_DOCS_RESULT " + JSON.stringify({ phase: operation,
      status, seconds: (performance.now() - started) / 1000 }));
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) await main();
