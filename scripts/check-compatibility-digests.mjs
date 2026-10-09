import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { compatibilityArtifacts } from "./generated-bindings.mjs";

const root = new URL("..", import.meta.url);
const manifest = JSON.parse(readFileSync(new URL("compatibility/manifest.json", root), "utf8"));
for (const [family, artifacts] of Object.entries(compatibilityArtifacts)) {
  for (const [field, path] of Object.entries(artifacts)) {
    const actual = `sha256:${createHash("sha256").update(readFileSync(new URL(path, root))).digest("hex")}`;
    if (manifest.families[family][field] !== actual) throw new Error(`${family} ${field} mismatch`);
  }
}
