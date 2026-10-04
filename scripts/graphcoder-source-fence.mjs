// Root-bound working-tree identity shared by qualification producers and
// consumers. Inventory is NUL-delimited; newline-bearing paths use argv-safe
// per-file hashing instead of being reparsed as line-delimited input.

import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { devNull, tmpdir } from "node:os";
import { join, resolve } from "node:path";

function hash(value) {
  return createHash("sha256").update(value).digest("hex");
}

export function gitEnvironment() {
  const result = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (typeof value !== "string") continue;
    if (["PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "CI"].includes(key.toUpperCase())) result[key] = value;
  }
  const pathEntry = Object.entries(process.env).find(([key]) => key.toLowerCase() === "path");
  if (pathEntry !== undefined) result[pathEntry[0]] = pathEntry[1];
  const globalConfig = process.platform === "win32" ? join(tmpdir(), "graphcoder-empty-gitconfig") : devNull;
  if (process.platform === "win32") writeFileSync(globalConfig, "", { flag: "a" });
  return {
    ...result,
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: globalConfig,
    GIT_TERMINAL_PROMPT: "0",
  };
}

function trackedPaths(worktree) {
  const bytes = execFileSync("git", ["-C", worktree, "ls-files", "-z"], { encoding: "buffer", windowsHide: true, env: gitEnvironment() });
  return bytes.toString("utf8").split("\0").filter(Boolean).sort();
}

function workingHashes(worktree, paths) {
  if (paths.every(path => !/[\r\n]/u.test(path))) {
    const output = execFileSync("git", ["-C", worktree, "hash-object", "--stdin-paths"], {
      input: `${paths.join("\n")}\n`, encoding: "utf8", windowsHide: true, env: gitEnvironment(),
    }).trim();
    const hashes = output === "" ? [] : output.split(/\r?\n/u);
    if (hashes.length !== paths.length) throw new Error("source-fence: git returned an incomplete working-tree inventory");
    return hashes;
  }
  // Git's stdin-paths mode is newline-delimited. Fall back to argv so a
  // tracked filename containing a newline cannot corrupt the inventory.
  return paths.map(path => execFileSync("git", ["-C", worktree, "hash-object", "--", path], { encoding: "utf8", windowsHide: true, env: gitEnvironment() }).trim());
}

export function workingPathDigests(worktree, paths) {
  const root = resolve(worktree);
  const hashes = workingHashes(root, paths);
  if (hashes.some(value => !/^[0-9a-f]{40}$/u.test(value))) throw new Error("source-fence: git returned an invalid working-tree object hash");
  return hashes;
}

export function workingTreeDigest(worktree) {
  const root = resolve(worktree);
  const paths = trackedPaths(root);
  const hashes = workingPathDigests(root, paths);
  return hash(paths.map((path, index) => `${path}\0${hashes[index]}`).join("\n"));
}

export function fileDigest(path) {
  return hash(readFileSync(path));
}
