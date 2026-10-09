import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";

// Closure invoke names include private crate build hashes. Rust 1.98 expands
// those names to include the closure's type, but the hashes still vary by host.
const wasmPrivateClosureName = /wasm_bindgen__convert__closures_____invoke__h[0-9a-f]+|wasm_bindgen_[0-9a-f]{8,16}___convert__closures[A-Za-z0-9_]*/g;
const canonicalPrivateClosureName = name => name.startsWith("wasm_bindgen__")
  ? "wasm_bindgen__convert__closures_____invoke__h<private>"
  : name.replace(/(wasm_bindgen|js_sys|web_sys|core)_[0-9a-f]{8,16}(?=_)/g, "$1_<private>");
const canonicalGeneratedJs = source => source
  .replace(wasmPrivateClosureName, canonicalPrivateClosureName)
  .replace(/shim_idx: \d+/g, "shim_idx: <private>");
const declarationBlocks = source => source
  .split(/\r?\n/)
  .reduce((blocks, line) => {
    if (line.startsWith("export ")) blocks.push(line);
    else if (blocks.length > 0) blocks[blocks.length - 1] += `\n${line}`;
    return blocks;
  }, [])
  .map(block => block
    .replace(wasmPrivateClosureName, canonicalPrivateClosureName)
    .replace(/\s+/g, " ")
    .trim())
  .sort();
// Snapshot committed declarations before any maintained producer can overwrite them.
export function wasmSurfaceComparison(root) {
  const git = args => execFileSync("git", args, { cwd: root });
  const revision = git(["rev-parse", "HEAD"]).toString().trim();
  const paths = git(["ls-tree", "-r", "--name-only", "-z", revision, "--", "typescript/packages"])
    .toString().split("\0").filter(path => /\/generated\/wasm\/[^/]+\.(?:js|d\.ts)$/.test(path));
  const tracked = new Map(paths.map(path => [path, git(["show", `${revision}:${path}`]).toString()]));
  return (packageName, basename, freshRoot) => {
    const packageRoot = join(root, `typescript/packages/${packageName}/generated/wasm`);
    for (const extension of [".js", ".d.ts", "_bg.wasm.d.ts"]) {
      const fresh = readFileSync(join(freshRoot, `${basename}${extension}`), "utf8");
      const committed = tracked.get(`typescript/packages/${packageName}/generated/wasm/${basename}${extension}`)
        ?? readFileSync(join(packageRoot, `${basename}${extension}`), "utf8");
      const normalizedFresh = extension === ".js" ? canonicalGeneratedJs(fresh) : declarationBlocks(fresh).join("\n");
      const normalizedCommitted = extension === ".js" ? canonicalGeneratedJs(committed) : declarationBlocks(committed).join("\n");
      if (normalizedFresh !== normalizedCommitted) {
        throw new Error(`generated WASM ${extension} drift: ${packageName}`);
      }
    }
  };
}
