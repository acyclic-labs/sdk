import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, statSync } from "node:fs";
import { join, resolve } from "node:path";
import { packageClosure } from "./package-closure.mjs";
import { repositoryRoot as root, trackedFiles } from "./policy-core.mjs";
import test from "node:test";

// Run the real selected generators and Cargo build twice on one persistent
// target. The second invocation is the freshness control, not a retry.
const [family = "workers", mode = "generate"] = process.argv.slice(2);
assert.ok(family && ["generate", "wasm"].includes(mode),
  "STEP_FAILED selection; recovery: node scripts/check-generation-stability.mjs harness [generate|wasm]");
function run(command, args) {
  const started = performance.now();
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", maxBuffer: Infinity });
  if (result.error || result.status !== 0) throw new Error(
    `STEP_FAILED ${command} ${args.join(" ")}; recovery: node scripts/check-generation-stability.mjs ${family} ${mode}\n${result.stdout ?? ""}${result.stderr ?? ""}`,
    { cause: result.error });
  return { output: result.stdout + result.stderr, seconds: (performance.now() - started) / 1000 };
}
test("selected generators preserve source mtimes and WASM Cargo freshness", () => {
  const generateBuild = () => run(process.execPath,
    [join(root, mode === "wasm" ? "scripts/build-wasm.mjs" : "scripts/generate.mjs"), family]);
  const first = generateBuild();
  const paths = new Set(trackedFiles(root, [], {untracked: true}));
  for (const name of packageClosure(root, family)) for (const suffix of ["generated", "src/generated", "test/generated"]) {
    const directory = join(root, "typescript/packages", name, suffix);
    if (!existsSync(directory)) continue;
    for (const entry of readdirSync(directory, { recursive: true, withFileTypes: true })) {
      if (entry.isFile()) paths.add(join(entry.parentPath, entry.name));
    }
  }
  const files = [...paths];
  const before = new Map(files.map(path => [path, statSync(resolve(root, path), { bigint: true }).mtimeNs]));
  const second = generateBuild();
  const changed = files.filter(path => statSync(resolve(root, path), { bigint: true }).mtimeNs !== before.get(path));
  const compiling = second.output.split(/\r?\n/).filter(line => /^\s*Compiling\s/.test(line));
  console.log(JSON.stringify({ family, mode, first_seconds: first.seconds, warm_seconds: second.seconds, changed, compiling }, null, 2));
  assert.deepEqual(changed, [], "unchanged generation rewrote source files");
  if (mode === "wasm") assert.deepEqual(compiling, [], "unchanged generation caused Cargo compilation");
});
