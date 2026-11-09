import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { generateProto } from "./generate-proto.mjs";
import { packageClosure } from "./package-closure.mjs";
import { generateFiles } from "./generated-bindings.mjs";
import { generateNativeFamilies } from "./native-family.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const check = process.argv[2] === "check";
const selected = process.argv[check ? 3 : 2];
const packages = packageClosure(root, selected);

function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Generation failed: ${command} ${args.join(" ")}`);
}

if (packages.has("auth")) await generateFiles(root, output => run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-auth", "--example", "generate", "--", output]), [], check);
generateProto(packages, check);
generateNativeFamilies(packages, check);
if (packages.has("actors")) {
  const { write } = await import("./generate-actors.mjs");
  await write(check);
}
if (packages.has("workers")) {
  const output = join(root, "typescript/packages/workers/src/generated");
  await generateFiles(output, destination => run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-manifest", "--example", "deployment-manifest", "--", destination]), [], check);
  await generateFiles(join(root, "typescript/packages/workers/test/generated"), destination => run("cargo", ["run", "--locked", "--quiet", "-p", "acyclic-manifest", "--example", "deployment-manifest", "--",
    destination, join(root, "rust/crates/manifest/fuzz/corpus/manifest_json/valid.json"), "production", "agent"]), [], check);
}
// Full generation includes Rust-only contracts, including harness fixtures.
run(process.execPath, [join(root, "scripts/generate-contracts.mjs"), check ? "check" : "write", ...(selected ? packages : [])]);
if (check) run(process.execPath, [join(root, "scripts/check-generated.mjs"), "--no-compile"]);
