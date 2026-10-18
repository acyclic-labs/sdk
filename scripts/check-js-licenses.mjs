import * as fs from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const allowed = new Set(["Apache-2.0", "MIT", "ISC", "BSD-2-Clause", "BSD-3-Clause", "BlueOak-1.0.0", "0BSD"]);

async function optional(operation) {
  try { return await operation(); }
  catch (error) {
    if (error instanceof Error && "code" in error && (error.code === "ENOENT" || error.code === "ENOTDIR")) return undefined;
    throw error;
  }
}

/**
 * @param {string} root
 * @param {{realpath(path: string): Promise<string>, readFile(path: string, encoding: "utf8"): Promise<string>, readdir(path: string, options: {withFileTypes: true}): Promise<import("node:fs").Dirent[]>}} io
 */
export async function auditLicenses(root, io = fs) {
  const failures = [], identities = new Set(), manifests = new Set();
  // A directory can serve different layout roles through links. Each role is
  // expanded once per physical directory; each physical manifest is read once.
  const visited = new Set();
  const pending = [{ path: join(root, "node_modules"), role: "modules" }];
  while (pending.length) {
    const item = pending.pop();
    if (!item) break;
    const path = await optional(() => io.realpath(item.path));
    if (path === undefined) continue;
    const key = `${item.role}:${path}`;
    if (visited.has(key)) continue;
    visited.add(key);
    if (item.role === "package") {
      const manifest = await optional(() => io.realpath(join(path, "package.json")));
      if (manifest !== undefined && !manifests.has(manifest)) {
        manifests.add(manifest);
        const metadata = JSON.parse(await io.readFile(manifest, "utf8"));
        const identity = `${metadata.name}@${metadata.version}`;
        if (!identities.has(identity) && !metadata.private) {
          identities.add(identity);
          const licenses = String(metadata.license ?? "").split(/\s+(?:OR|AND)\s+|\//);
          if (!licenses.some(license => allowed.has(license.replace(/[()]/g, "")))) {
            failures.push(`${identity}: ${metadata.license ?? "missing"}`);
          }
        }
      }
    }
    if (item.role === "package" || item.role === "container") {
      pending.push({ path: join(path, "node_modules"), role: "modules" });
      continue;
    }
    const entries = await optional(() => io.readdir(path, { withFileTypes: true }));
    for (const entry of entries ?? []) {
      if (!entry.isDirectory() && !entry.isSymbolicLink()) continue;
      let role = "package";
      if (item.role === "modules" && entry.name === ".bun") role = "store";
      else if (item.role === "store") role = "container";
      else if (entry.name.startsWith(".")) continue;
      else if (item.role === "modules" && entry.name.startsWith("@")) role = "scope";
      pending.push({ path: join(path, entry.name), role });
    }
  }
  return failures.sort();
}

if (import.meta.main) {
  const failures = await auditLicenses(dirname(dirname(fileURLToPath(import.meta.url))));
  if (failures.length) { console.error(failures.join("\n")); process.exitCode = 1; }
}
