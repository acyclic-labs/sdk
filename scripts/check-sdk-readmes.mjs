import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const services = ["Objects", "Streams", "Actors", "Workers", "fs", "Swarm"];

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

export function checkReadmes() {
  const read = file => readFileSync(resolve(root, file), "utf8");
  const guide = examples(read("typescript/packages/sdk/README.md"), "ts");
  checkCopies(guide, examples(read("llms.txt"), "ts"));
  examples(read("rust/README.md"), "rust,no_run");
  const directory = resolve(root, ".tools/sdk-readme-examples");
  mkdirSync(directory, { recursive: true });
  for (const [name, code] of guide) writeFileSync(resolve(directory, `${name}.mts`), code);
  const paths = {
    "@acyclic-labs/objects": ["typescript/packages/objects/src/index.ts"],
    "@acyclic-labs/objects/proto": ["typescript/packages/objects/generated/proto/objects/v1/objects_pb.d.ts"],
    "@acyclic-labs/stream": ["typescript/packages/stream/src/index.ts"],
    "@acyclic-labs/actors": ["typescript/packages/actors/src/index.ts"],
    "@acyclic-labs/workers": ["typescript/packages/workers/src/index.ts"],
    "@acyclic-labs/fs/grpc": ["typescript/packages/filesystem/src/grpc.ts"],
    "@acyclic-labs/swarm": ["typescript/packages/swarm/src/index.ts"],
  };
  // Fail if a documentation import no longer names a public package export.
  for (const specifier of Object.keys(paths)) {
    const parts = specifier.split("/");
    const name = parts[1] === "fs" ? "filesystem" : parts[1];
    const manifest = JSON.parse(read(`typescript/packages/${name}/package.json`));
    const subpath = parts.length === 2 ? "." : `./${parts.slice(2).join("/")}`;
    if (!Object.hasOwn(manifest.exports, subpath)) throw new Error(`missing public export: ${specifier}`);
  }
  writeFileSync(resolve(directory, "tsconfig.json"), JSON.stringify({
    compilerOptions: {
      strict: true, noEmit: true, target: "ES2022", module: "NodeNext",
      types: ["node"], paths: Object.fromEntries(Object.entries(paths)
        .map(([name, files]) => [name, files.map(file => resolve(root, file))])),
    },
    files: services.map(name => `${name}.mts`),
  }, null, 2));
  const compiler = resolve(root, "node_modules/typescript/bin/tsc");
  const result = spawnSync(process.execPath, [compiler, "-p", resolve(directory, "tsconfig.json"), "--pretty", "false"], { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`README TypeScript examples failed (${result.status})`);
  console.log("Six TypeScript README/llms.txt examples compiled; six Rust doctests inventoried");
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) checkReadmes();
