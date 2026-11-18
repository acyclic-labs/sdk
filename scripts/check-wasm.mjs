import { repositoryRoot as root } from "./policy-core.mjs";
// Check every WASM library on its real target. Published packages come from
// the build table itself, so a package cannot be built without being checked.
// The browser features below are compiled only by consumers; each is checked
// on its own so one crate's features cannot mask a missing cfg in another.
import { execFileSync, spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { bazelCommand } from "./bazel-executables.mjs";
import { runCommand } from "./run-command.mjs";
import { wasmPackages } from "./wasm-packages.mjs";

const cargo = process.env.ACYCLIC_CARGO_BIN || "cargo";
const target = "wasm32-unknown-unknown";

const selections = [
  ["fs without defaults", ["-p", "acyclic-fs", "--no-default-features"]],
  ["fs browser", ["-p", "acyclic-fs", "--no-default-features", "--features", "browser"]],
  ["harness-core browser", ["-p", "acyclic-harness-core", "--no-default-features", "--features", "browser"]],
  ["harness browser", ["-p", "acyclic-harness", "--no-default-features", "--features", "browser"]],
  ["harness-providers browser", ["-p", "acyclic-harness-providers", "--features", "browser"]],
];

const ensured = spawnSync("bash", [resolve(root, "scripts/ensure-rust-target.sh"), target], { cwd: root, stdio: "inherit", windowsHide: true });
if (ensured.error || ensured.status !== 0) throw new Error(`step=ensure ${target}; recovery: bash scripts/ensure-rust-target.sh ${target}`);

const failed = [];
const published = Object.keys(wasmPackages).map(name => `//wasm:${name}`);
const query = `labels(module, set(${published.join(" ")}))`;
const modules = execFileSync(bazelCommand(), ["query", query], { cwd: root, encoding: "utf8" })
  .trim().split(/\r?\n/).filter(Boolean);
if (modules.length !== published.length) {
  throw new Error(`step=wasm check: expected ${published.length} published modules, got ${modules.length}`);
}
const args = ["build", "-c", "opt", "--platforms=//:wasm32", "--keep_going", ...modules];
if (await runCommand(bazelCommand(), args, { cwd: root }) !== 0) {
  failed.push(`published modules: ${bazelCommand()} ${args.join(" ")}`);
}
for (const [name, selection] of selections) {
  const args = ["check", "--locked", "--keep-going", "--lib", "--target", target, ...selection];
  console.log(`wasm check ${name}: cargo ${args.join(" ")}`);
  if (await runCommand(cargo, args, { cwd: root }) !== 0) failed.push(`${name}: cargo ${args.join(" ")}`);
}
if (failed.length) {
  console.error(`step=wasm check: ${failed.length} checks failed; recovery:\n${failed.join("\n")}`);
  process.exitCode = 1;
}
