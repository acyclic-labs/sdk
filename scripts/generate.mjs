import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { generateProto } from "./generate-proto.mjs";
import { packageClosure } from "./package-closure.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const selected = process.argv[2];
const packages = packageClosure(root, selected);

function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Generation failed: ${command} ${args.join(" ")}`);
}

if (packages.has("auth")) run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-auth", "--example", "generate", "--", root]);
generateProto(packages);
if (packages.has("actors")) run(process.execPath, [join(root, "scripts/generate-actors.mjs"), "write"]);
if (packages.has("workers")) {
  const output = join(root, "typescript/packages/workers/src/generated");
  run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-manifest", "--example", "deployment-manifest", "--", output]);
  run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-manifest", "--example", "deployment-manifest", "--",
    join(root, "typescript/packages/workers/test/generated"), join(root, "rust/crates/manifest/fuzz/corpus/manifest_json/valid.json"), "production", "agent"]);
}
run(process.execPath, [join(root, "scripts/generate-contracts.mjs"), "write", ...packages]);
