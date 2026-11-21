import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const digest = (bytes, key = "") => createHash("sha256").update(key).update(bytes).digest("hex");
export const contentKey = value => digest(JSON.stringify(value));

export function workingBlob(file, algorithm) {
  try {
    const bytes = fs.readFileSync(file);
    return createHash(algorithm).update("blob " + bytes.length + "\0").update(bytes).digest("hex");
  } catch (error) {
    if (error.code !== "ENOENT" && error.code !== "ENOTDIR") throw error;
    return null;
  }
}

export function generatorCache(generator) {
  const directory = process.env.BAZEL_GENERATOR_CACHE_DIR ?? path.join(
    process.env.XDG_CACHE_HOME || path.join(os.homedir(), ".cache"), "bazel-build-files",
  );
  return (kind, key, compute, { valid = () => true, decode = JSON.parse } = {}) => {
    if (!directory) return compute();
    const id = contentKey([generator, kind, key]);
    const parent = path.resolve(directory, "v1", kind, id.slice(0, 2));
    const file = path.join(parent, id.slice(2));
    try {
      const entry = fs.readFileSync(file, "utf8");
      const value = entry.slice(65);
      if (entry.slice(0, 65) === digest(value, id) + "\n") {
        const result = decode(value);
        if (valid(result)) return result;
      }
    } catch (error) {
      if (!(error instanceof SyntaxError) && !error.code) throw error;
    }
    const result = compute();
    const value = JSON.stringify(result);
    const temporary = file + "." + randomUUID();
    try {
      fs.mkdirSync(parent, { recursive: true, mode: 0o700 });
      fs.writeFileSync(temporary, digest(value, id) + "\n" + value, { flag: "wx", mode: 0o600 });
      fs.renameSync(temporary, file);
    } catch (error) {
      if (!error.code) throw error;
      try { fs.unlinkSync(temporary); }
      catch (cleanup) { if (!cleanup.code) throw cleanup; }
    }
    return result;
  };
}

export function trackedBlobs(root) {
  const git = (...args) => execFileSync("git", args, {
    cwd: root, encoding: "utf8", maxBuffer: Infinity,
  }).split("\0").filter(Boolean);
  const dirty = new Set(git("diff-files", "--name-only", "-z", "--"));
  // The index describes staged files as well as the committed tree. A modified worktree file
  // must use its actual bytes, and a symlink's blob describes its link rather than its contents.
  return new Map(git("ls-files", "-v", "--stage", "-z", "--").map(entry => {
    const tab = entry.indexOf("\t"), file = entry.slice(tab + 1);
    const [tag, mode, blob, stage] = entry.slice(0, tab).split(" ");
    return [file, tag === "H" && mode.startsWith("100") && stage === "0" && !dirty.has(file) ? blob : null];
  }));
}
