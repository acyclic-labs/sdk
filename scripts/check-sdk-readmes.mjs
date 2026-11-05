import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { generatedReadmes, services } from "./generate-sdk-readmes.mjs";
import { workspaceManifests } from "./package-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function examples(source, language) {
  source = source.replaceAll("\r\n", "\n");
  const result = new Map();
  for (const section of source.split(/^## /m).slice(1)) {
    const name = section.slice(0, section.indexOf("\n")).trim();
    if (!services.includes(name)) continue;
    const blocks = [...section.matchAll(/^```([^\n]+)\n([\s\S]*?)^```$/gm)]
      .filter(match => match[1] === language);
    if (result.has(name) || blocks.length !== 1) throw new Error(`${name}: expected one ${language} example`);
    result.set(name, blocks[0][2]);
  }
  for (const name of services) if (!result.has(name)) throw new Error(`missing ${name} example`);
  return result;
}

export function checkCopies(guide, llms) {
  for (const name of services) {
    if (guide.get(name) !== llms.get(name)) throw new Error(`${name}: llms.txt differs from the TypeScript guide`);
  }
}

// The sources check reads only checked-in files, so it runs on any source tree (the merge gate's policy step).
// Compiling the examples resolves package sources that import generated modules, so only a caller that has run
// `bun run generate` asks for it (`check:docs`, which `check` runs after generating).
function checkSources() {
  const read = file => readFileSync(resolve(root, file), "utf8");
  for (const [file, { source, generated }] of generatedReadmes(read)) {
    if (source !== generated) throw new Error(`STEP_FAILED README drift: ${file}; recovery: node scripts/generate-sdk-readmes.mjs write`);
  }
  const guide = examples(read("typescript/packages/sdk/README.md"), "ts");
  checkCopies(guide, examples(read("llms.txt"), "ts"));
  examples(read("rust/README.md"), "rust,no_run");
  return guide;
}

function compileExamples(guide) {
  const directory = resolve(root, ".tools/sdk-readme-examples");
  mkdirSync(directory, { recursive: true });
  for (const [name, code] of guide) writeFileSync(resolve(directory, `${name}.mts`), code);
  const paths = {};
  // Resolve each public export to its source, including transitive imports.
  // Missing exports remain missing; no wildcard can admit a private module.
  for (const [directory, manifest] of workspaceManifests(root)) {
    for (const [subpath, entry] of Object.entries(manifest.exports ?? {})) {
      const target = typeof entry === "string" ? entry : entry.types ?? entry.default;
      if (typeof target !== "string" || !/\.(?:d\.ts|js)$/.test(target)) continue;
      const source = target.startsWith("./dist/")
        ? target.replace("./dist/", "./src/").replace(/\.(?:d\.ts|js)$/, ".ts") : target;
      paths[manifest.name + (subpath === "." ? "" : subpath.slice(1))] = [resolve(root, "typescript/packages", directory, source)];
    }
  }
  writeFileSync(resolve(directory, "tsconfig.json"), JSON.stringify({
    compilerOptions: {
      strict: true, noEmit: true, target: "ES2022", module: "NodeNext",
      types: ["node"], paths,
    },
    files: services.map(name => `${name}.mts`),
  }, null, 2));
  const compiler = resolve(root, "node_modules/typescript/bin/tsc");
  const result = spawnSync(process.execPath, [compiler, "-p", resolve(directory, "tsconfig.json"), "--pretty", "false"], { cwd: root, stdio: "inherit" });
  if (result.error || result.status !== 0) throw new Error(`STEP_FAILED README TypeScript compilation (${result.error?.message ?? result.status}); recovery: bun run generate && bun run build:wasm && bun run check:docs`);
  console.log(`${services.length} TypeScript README/llms.txt examples compiled; ${services.length} generated Rust doctests inventoried`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const mode = process.argv.slice(2);
  if (mode.length > 1 || (mode.length === 1 && mode[0] !== "compile")) throw new Error("usage: check-sdk-readmes.mjs [compile]");
  const guide = checkSources();
  if (mode[0] === "compile") compileExamples(guide);
  else console.log(`${services.length} README/llms.txt examples match their sources; compile with: node scripts/check-sdk-readmes.mjs compile`);
}
