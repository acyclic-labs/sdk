import { spawnSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { packageClosure } from "./package-closure.mjs";
import { generateNativeFamilies } from "./native-family.mjs";
import { memoizedGeneration } from "./generation-cache.mjs";
import { bazelExecutables } from "./bazel-executables.mjs";
import {bindingTools, projectBindings} from "./project-bindings.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const native = process.argv.includes("--native");
const args = process.argv.slice(2).filter(arg => arg !== "--native");
const check = args[0] === "check";
const selected = args[check ? 1 : 0];
const packages = packageClosure(root, selected);

function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Generation failed: ${command} ${args.join(" ")}`);
}

async function generate() {
  const options = {families:selected ? [...packages] : [], native};
  const labels = await bindingTools(packages, options);
  const executables = bazelExecutables(root, labels);
  await projectBindings(packages, {...options, check, tools:new Map(labels.map((label,index) => [label,executables[index]]))});
  generateNativeFamilies(packages, check);
  if (check) run(process.execPath, [join(root, "scripts/check-generated.mjs"), "--no-compile"]);
}
// Every generator runs as a Bazel target, and Bazel reads the BUILD files that restate the Cargo manifests, so
// they are made current (or, in check mode, refused when stale) before any generator builds, whatever the
// selection. They read every crate's sources, which the generation key does not cover, so they are rendered on
// every run rather than memoized.
run(process.execPath, [join(root, "scripts/bazel-build-files.mjs"), check ? "check" : "write"]);
// Check mode verifies the checkout, so it always runs. A write is memoized by its exact inputs; full generation also
// emits the native outputs.
if (check) await generate();
else await memoizedGeneration(root, { name: `${selected ?? "all"}${native ? "+native" : ""}`, packages: [...packages], native: native || !selected }, generate);
