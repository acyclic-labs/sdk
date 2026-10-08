import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const complete = [".js", ".d.ts", "_bg.wasm.d.ts", "_bg.wasm"];
/** @type {Record<string, string[]>} */
export const committedWasmExtensions = {
  filesystem: complete, stream: complete,
  harness: [".d.ts", "_bg.wasm.d.ts"],
  objects: [], inference: [], machines: [],
};

// Commit regenerated tracked artifacts before checking drift; worktree edits are not baselines.
// Pin once: builds can overwrite tracked packages before the drift check starts.
// Read only the committed surfaces the explicit package policy requires.
/** @param {string} root @param {string} directory @param {[string, string][]} packages */
export function snapshotCommittedWasm(root, directory, packages) {
  /** @param {...string} args */
  const git = (...args) => execFileSync("git", args, {
    cwd: root, stdio: ["ignore", "pipe", "pipe"], maxBuffer: 64 * 1024 * 1024,
    env: { ...process.env, GIT_NO_LAZY_FETCH: "1" },
  });
  const source = git("rev-parse", "--verify", "HEAD^{commit}").toString("utf8").trim();
  if (!/^[0-9a-f]{40}$/.test(source)) throw new Error("invalid generated WASM baseline source");
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, "package.json"), '{"type":"module"}\n');
  /** @type {Record<string, string[]>} */
  const extensions = {};
  for (const [name, basename] of packages) {
    if (!/^[a-z][a-z0-9-]*$/.test(name) || !/^[a-z][a-z0-9_]*$/.test(basename)) {
      throw new Error("invalid generated WASM package identity");
    }
    if (!Object.hasOwn(committedWasmExtensions, name)) throw new Error(`unknown WASM baseline policy: ${name}`);
    extensions[name] = [...committedWasmExtensions[name]];
    const prefix = `typescript/packages/${name}/generated/wasm/`;
    const expected = extensions[name].map(extension => prefix + basename + extension).sort();
    const consumed = new Set(complete.map(extension => prefix + basename + extension));
    const tracked = git("ls-tree", "-r", "--name-only", source, "--", prefix)
      .toString("utf8").trim().split("\n").filter(path => consumed.has(path)).sort();
    if (JSON.stringify(tracked) !== JSON.stringify(expected)) {
      throw new Error(`tracked WASM baseline policy mismatch: ${name}`);
    }
    const destination = join(directory, name);
    mkdirSync(destination, { recursive: true });
    for (const extension of extensions[name]) {
      const filename = `${basename}${extension}`;
      const path = `typescript/packages/${name}/generated/wasm/${filename}`;
      // Missing Git blobs fail closed; never fall back to a rebuilt worktree.
      writeFileSync(join(destination, filename), git("cat-file", "blob", `${source}:${path}`));
    }
  }
  return { source, directory, extensions };
}
