import { existsSync, lstatSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { sha256, within } from "../../../shared/authority.mjs";
import { files } from "./provenance.mjs";

const digestFiles = root => Object.fromEntries(files(root).map(name => [name, sha256(readFileSync(join(root, name)))]));

// Recording an inventory does not admit it. Qualification separately requires
// the externally recorded bytes to match the checked-in qualification pin.
export function inventoryRuntime(root) {
  return { schema: "acyclic.sdk.cpp-runtime-inventory.v1", files: digestFiles(root) };
}

export function inventoryHost(environment) {
  const names = ["cl.exe", "link.exe", "lib.exe", "rc.exe", "mt.exe", "cmake.exe", "ninja.exe"];
  if (!Array.isArray(environment.include) || !Array.isArray(environment.lib)
    || JSON.stringify(Object.keys(environment.tools ?? {}).sort()) !== JSON.stringify(names.toSorted())) throw new Error("host environment inventory differs");
  const candidates = [...environment.include, ...environment.lib, ...Object.values(environment.tools).map(dirname)], roots = [];
  for (const candidate of candidates) {
    if (!isAbsolute(candidate)) throw new Error("host environment paths must be absolute");
    const root = resolve(candidate);
    if (!existsSync(root)) continue;
    if (!lstatSync(root).isDirectory() || lstatSync(root).isSymbolicLink()) throw new Error("host root kind differs");
    if (!roots.some(value => value.toLowerCase() === root.toLowerCase())) roots.push(root);
  }
  const inventory = roots.map(root => ({ root, files_sha256: digestFiles(root) }));
  const tools = Object.fromEntries(Object.entries(environment.tools).map(([name, path]) => [name, { path, sha256: sha256(readFileSync(path)) }]));
  return { schema: "acyclic.sdk.cpp-host-inventory.v1", scope: "MSVC/Windows SDK compiler binaries, include directories and library inputs; system OS files excluded", environment, roots: inventory, tools };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({ options: Object.fromEntries(["kind", "root", "environment", "output"].map(name => [name, { type: "string" }])) });
  if (!values.output || !["host", "runtime"].includes(values.kind)) throw new Error("--kind host|runtime and --output are required");
  const output = resolve(values.output); if (existsSync(output)) throw new Error("inventory output must be absent");
  let inventory;
  if (values.kind === "runtime") {
    if (!values.root || within(resolve(values.root), output)) throw new Error("pass a runtime root and an output outside it");
    inventory = inventoryRuntime(values.root);
  } else {
    if (!values.environment || output === resolve(values.environment)) throw new Error("pass a separate host environment input");
    const bytes = readFileSync(values.environment); inventory = inventoryHost(JSON.parse(bytes));
    if (inventory.roots.some(root => within(root.root, output)) || !readFileSync(values.environment).equals(bytes)) throw new Error("inventory output overlaps source or environment changed");
  }
  const bytes = Buffer.from(JSON.stringify(inventory, null, 2) + "\n");
  writeFileSync(output, bytes, { flag: "wx" }); process.stdout.write(sha256(bytes) + "\n");
}
