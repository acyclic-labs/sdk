import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = join(fileURLToPath(new URL("../", import.meta.url)));
const mode = process.argv[2] ?? "write";
if (mode !== "write" && mode !== "check") throw new Error("expected write or check");
const generated = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-workers", "--example", "workers-module-contract"], { cwd: root, encoding: "utf8" });
if (generated.status !== 0) throw new Error(generated.stderr || "Workers module contract generation failed");
const path = join(root, "typescript", "packages", "workers", "src", "module-contract.ts");
if (mode === "check") {
  if (readFileSync(path, "utf8") !== generated.stdout) throw new Error("Workers module contract drift");
} else writeFileSync(path, generated.stdout);
