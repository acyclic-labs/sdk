import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { packagedTypeScriptBindings, writeChanged } from "./generated-bindings.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function generateProto(packages = new Set(packagedTypeScriptBindings.flatMap(([, names]) => names))) {
  const temporary = mkdtempSync(join(root, "target-proto-"));
  try {
    const buf = join(root, "node_modules/.bin", process.platform === "win32" ? "buf.exe" : "buf");
    const result = spawnSync(buf, ["generate", "--output", temporary], { cwd: root, stdio: "inherit" });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`Protobuf generation failed: ${result.status}`);
    for (const [stem, names] of packagedTypeScriptBindings) {
      for (const extension of [".js", ".d.ts"]) {
        const relative = `${stem}${extension}`;
        const bytes = readFileSync(join(temporary, "generated/typescript", relative));
        for (const name of names.filter(name => packages.has(name))) {
          const destination = join(root, "typescript/packages", name, "generated/proto", relative);
          mkdirSync(dirname(destination), { recursive: true });
          writeChanged(destination, bytes);
        }
      }
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) generateProto();
