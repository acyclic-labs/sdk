import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { compatibilityArtifacts } from "./generated-bindings.mjs";

const root = new URL("..", import.meta.url);
// Writing is explicit: ordinary imports and checks never bless changed inputs.
const write = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url) && process.argv[2] === "write";
const manifest = JSON.parse(readFileSync(new URL("compatibility/manifest.json", root), "utf8"));
for (const [family, artifacts] of Object.entries(compatibilityArtifacts)) {
  for (const [field, path] of Object.entries(artifacts)) {
    const actual = `sha256:${createHash("sha256").update(readFileSync(new URL(path, root))).digest("hex")}`;
    if (write) manifest.families[family][field] = actual;
    else if (manifest.families[family][field] !== actual) {
      throw new Error(`Compatibility digest check failed: ${family} ${field}; regenerate with bun scripts/check-compatibility-digests.mjs write`);
    }
  }
}
if (write) writeFileSync(new URL("compatibility/manifest.json", root), `${JSON.stringify(manifest, null, 2)}\n`);
