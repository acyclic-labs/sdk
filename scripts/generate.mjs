import { spawnSync } from "node:child_process";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { generateProto } from "./generate-proto.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const selected = process.argv[2];
const packages = new Set();
const names = new Map(readdirSync(join(root, "typescript/packages"), { withFileTypes: true })
  .filter(entry => entry.isDirectory()).map(entry => {
    const manifest = JSON.parse(readFileSync(join(root, "typescript/packages", entry.name, "package.json"), "utf8"));
    return [manifest.name, entry.name];
  }));
function visit(name) {
  if (packages.has(name)) return;
  if (!/^[a-z][a-z-]*$/.test(name)) throw new Error("invalid package generation target");
  const directory = join(root, "typescript/packages", name);
  const config = JSON.parse(readFileSync(join(directory, "tsconfig.json"), "utf8"));
  packages.add(name);
  const manifest = JSON.parse(readFileSync(join(directory, "package.json"), "utf8"));
  for (const dependency of Object.keys({ ...manifest.dependencies, ...manifest.peerDependencies })) {
    if (names.has(dependency)) visit(names.get(dependency));
  }
  for (const reference of config.references ?? []) {
    if (!/^\.\.\/[a-z][a-z-]*$/.test(reference.path)) throw new Error("invalid package reference");
    visit(reference.path.slice(3));
  }
}
if (selected) visit(selected);
else for (const name of ["actors", "auth", "filesystem", "harness", "inference", "machines", "objects", "stream", "workers"]) visit(name);

function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Generation failed: ${command} ${args.join(" ")}`);
}

generateProto(packages);
if (packages.has("auth")) run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-auth", "--example", "generate", "--", root]);
if (packages.has("actors")) run(process.execPath, [join(root, "scripts/generate-actors.mjs"), "write"]);
if (packages.has("workers")) {
  const output = join(root, "typescript/packages/workers/src/generated");
  run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-manifest", "--example", "deployment-manifest", "--", output]);
  run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-manifest", "--example", "deployment-manifest", "--",
    join(root, "typescript/packages/workers/test/generated"), join(root, "rust/crates/manifest/fuzz/corpus/manifest_json/valid.json"), "production", "agent"]);
}
run(process.execPath, [join(root, "scripts/generate-contracts.mjs"), "write", ...packages]);
