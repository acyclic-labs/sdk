import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const allowed = new Set(["Apache-2.0", "MIT", "ISC", "BSD-2-Clause", "BSD-3-Clause", "BlueOak-1.0.0", "0BSD"]);
const failures = [];
const seen = new Set();
const glob = new Bun.Glob("node_modules/**/package.json");
for await (const path of glob.scan({ cwd: root, onlyFiles: true })) {
  const metadata = JSON.parse(await readFile(join(root, path), "utf8"));
  const key = `${metadata.name}@${metadata.version}`;
  if (seen.has(key) || metadata.private) continue;
  seen.add(key);
  const licenses = String(metadata.license ?? "").split(/\s+(?:OR|AND)\s+|\//);
  if (!licenses.some(license => allowed.has(license.replace(/[()]/g, "")))) failures.push(`${key}: ${metadata.license ?? "missing"}`);
}
if (failures.length) { console.error(failures.join("\n")); process.exitCode = 1; }
