import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { packagedTypeScriptBindings, writeChanged } from "./generated-bindings.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function generateProto(packages = new Set(packagedTypeScriptBindings.flatMap(([, names]) => names))) {
  /** @type {[string, string[]][]} */
  const bindings = [];
  for (const [stem, names] of packagedTypeScriptBindings) {
    const selected = names.filter(name => packages.has(name));
    if (selected.length > 0) bindings.push([stem, selected]);
  }
  if (bindings.length === 0) return;
  const paths = bindings.map(([stem]) => {
    const family = stem.split("/")[0];
    const owner = family === "protocol" ? "harness" : family === "validation" ? "contract-derive" : family;
    return `rust/crates/${owner}/proto/${stem.replace(/_pb$/, ".proto")}`;
  });
  /** @type {Map<string, Set<string>>} */
  const outputs = new Map();
  const temporary = mkdtempSync(join(root, "target-proto-"));
  try {
    const buf = join(root, "node_modules/.bin", process.platform === "win32" ? "buf.exe" : "buf");
    const result = spawnSync(buf, ["generate", "--output", temporary, ...paths.flatMap(path => ["--path", path])], { cwd: root, stdio: "inherit" });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`Protobuf generation failed: ${result.status}`);
    for (const [stem, names] of bindings) {
      for (const extension of [".js", ".d.ts"]) {
        const relative = `${stem}${extension}`;
        const bytes = readFileSync(join(temporary, "generated/typescript", relative));
        for (const name of names) {
          const directory = join(root, "typescript/packages", name, "generated/proto");
          const destination = join(directory, relative);
          let expected = outputs.get(directory);
          if (!expected) { expected = new Set(); outputs.set(directory, expected); }
          expected.add(destination);
          mkdirSync(dirname(destination), { recursive: true });
          writeChanged(destination, bytes);
        }
      }
    }
    for (const [directory, expected] of outputs) {
      if (!existsSync(directory)) continue;
      for (const entry of readdirSync(directory, { recursive: true, withFileTypes: true })) {
        const path = join(entry.parentPath, entry.name);
        if (!entry.isDirectory() && !expected.has(path)) rmSync(path);
      }
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) generateProto();
