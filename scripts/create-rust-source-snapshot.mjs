import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, realpathSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";

function git(root, args) {
  const env = { ...process.env };
  // Repository selection must follow the explicit root, including for checkout.
  for (const key of Object.keys(env)) {
    if (["GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES"].includes(key.toUpperCase())) delete env[key];
  }
  const result = spawnSync("git", ["-c", "gc.auto=0", "-C", root, ...args], { env, encoding: "utf8", maxBuffer: 8 * 1024 * 1024 });
  if (result.error || result.status !== 0) throw new Error(result.error?.message ?? result.stderr.trim() ?? "Git failed");
  return result.stdout.trim();
}

function canonical(path) {
  const value = realpathSync(resolve(path));
  return process.platform === "win32" ? value.toLowerCase() : value;
}

export function createRustSourceSnapshot({ sourceRoot, destination, revision }) {
  sourceRoot = resolve(sourceRoot);
  destination = resolve(destination);
  const discoveredRoot = git(sourceRoot, ["rev-parse", "--show-toplevel"]);
  if (canonical(sourceRoot) !== canonical(discoveredRoot)) throw new Error("Source root belongs to a parent Git repository; use the exact checkout root");
  const sourceRevision = git(sourceRoot, ["rev-parse", "--verify", "--end-of-options", `${revision}^{commit}`]);
  if (existsSync(destination) || existsSync(`${destination}.receipt.json`)) throw new Error("Snapshot destination already exists; it will not be overwritten");
  mkdirSync(dirname(destination), { recursive: true });
  git(sourceRoot, ["clone", "--local", "--no-hardlinks", "--no-checkout", "--", sourceRoot, destination]);
  git(destination, ["checkout", "--detach", sourceRevision]);
  if (canonical(git(destination, ["rev-parse", "--show-toplevel"])) !== canonical(destination)) throw new Error("Snapshot is not an independent Git checkout");
  if (git(destination, ["rev-parse", "HEAD"]) !== sourceRevision) throw new Error("Snapshot revision mismatch");
  if (git(destination, ["status", "--porcelain=v1", "--untracked-files=all"]) !== "") throw new Error("Snapshot checkout is dirty");
  const receipt = { schema: "acyclic.sdk.source-snapshot.v1", source_root: sourceRoot, snapshot_root: destination, source_revision: sourceRevision, clean: true, capture: "local independent detached Git clone", qualification: false };
  writeFileSync(`${destination}.receipt.json`, `${JSON.stringify(receipt, null, 2)}\n`);
  return receipt;
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    const [sourceRoot, destination, revision] = process.argv.slice(2);
    if (!sourceRoot || !destination || !revision) throw new Error("Usage: create-rust-source-snapshot.mjs SOURCE_ROOT DESTINATION COMMIT");
    console.log(JSON.stringify(createRustSourceSnapshot({ sourceRoot, destination, revision }), null, 2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
