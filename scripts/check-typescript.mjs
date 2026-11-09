// The one TypeScript typecheck: every package under typescript/packages compiles
// against freshly built WASM bindings, so a Rust binding change that breaks a
// TypeScript consumer fails here, not in a downstream build.
//   bun scripts/check-typescript.mjs
// It runs in its own checkout of HEAD under the Cargo target directory, so the
// WASM and dist outputs it writes never race another reader of this tree, and
// the stable path keeps the WASM build incremental between runs.
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const run = (program, args, cwd) => {
  const result = spawnSync(program, args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
  if (result.error || result.status !== 0) {
    throw new Error(`STEP typescript failed: ${program} ${args.join(" ")} exited ${result.status}${result.stdout ? `\n${result.stdout}` : ""}`);
  }
  return result.stdout.trim();
};

// The root build references every package, so none can drop out of the check.
const packages = readdirSync(join(root, "typescript/packages"))
  .filter(name => existsSync(join(root, "typescript/packages", name, "tsconfig.json")))
  .map(name => `./typescript/packages/${name}`).sort();
const referenced = JSON.parse(readFileSync(join(root, "tsconfig.json"), "utf8")).references.map(({ path }) => path).sort();
if (JSON.stringify(packages) !== JSON.stringify(referenced)) {
  throw new Error(`STEP typescript failed: tsconfig.json references ${JSON.stringify(referenced)}, packages are ${JSON.stringify(packages)}; recovery: reference every package in tsconfig.json`);
}

const head = run("git", ["rev-parse", "HEAD"], root);
const tree = join(resolve(root, process.env.CARGO_TARGET_DIR ?? "target"), "typescript-check");
if (!existsSync(join(tree, ".git"))) run("git", ["clone", "-q", "--shared", "--no-checkout", root, tree], root);
// This tree's own object store, wherever it lives now.
writeFileSync(join(tree, ".git/objects/info/alternates"), `${resolve(root, run("git", ["rev-parse", "--git-common-dir"], root), "objects")}\n`);
run("git", ["checkout", "-q", "-f", "--detach", head], tree);
run("git", ["clean", "-fdq"], tree);
const bun = process.versions.bun ? process.execPath : "bun";
const result = spawnSync(bun, ["install", "--frozen-lockfile"], { cwd: tree, stdio: "inherit" });
if (result.error || result.status !== 0) throw new Error(`STEP typescript-install failed (exit ${result.status}); recovery: bun install --frozen-lockfile`);
const check = spawnSync(bun, ["run", "check:typescript"], { cwd: tree, stdio: "inherit" });
if (check.error || check.status !== 0) {
  console.error(`STEP typescript failed (exit ${check.status}) at ${head}; recovery: bun run check:typescript`);
  process.exit(1);
}
