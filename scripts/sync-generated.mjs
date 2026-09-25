import { copyFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { compatibilityArtifacts, normalizeGeneratedRust, normalizeGeneratedTypeScript, packagedRustBindings, packagedSourceCopies, packagedTypeScriptBindings } from "./generated-bindings.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
for (const [source, packaged] of packagedSourceCopies) {
  const destination = join(root, packaged);
  mkdirSync(dirname(destination), { recursive: true });
  copyFileSync(join(root, source), destination);
}

for (const [relative, packaged] of packagedRustBindings) {
  const source = join(root, "generated/rust", relative);
  const destination = join(root, packaged);
  if (!existsSync(source)) throw new Error(`Rust generation path is missing: ${relative}`);
  const normalized = normalizeGeneratedRust(relative, readFileSync(source, "utf8"));
  mkdirSync(dirname(destination), { recursive: true });
  writeFileSync(source, normalized);
  writeFileSync(destination, normalized);
}

const packageRoot = resolve(root, "typescript/packages");
for (const [stem] of packagedTypeScriptBindings) {
  for (const extension of [".js", ".d.ts"]) {
    const file = `${stem}${extension}`;
    if (!existsSync(join(root, "generated/typescript", file))) {
      throw new Error(`TypeScript generation path is missing: ${file}`);
    }
  }
}
for (const name of new Set(packagedTypeScriptBindings.flatMap(([, packages]) => packages))) {
  const destination = resolve(packageRoot, name, "generated/proto");
  if (!destination.startsWith(`${packageRoot}${sep}`)) {
    throw new Error(`TypeScript generation path escapes the package root: ${name}`);
  }
  rmSync(destination, { recursive: true, force: true });
}
for (const [stem, packages] of packagedTypeScriptBindings) {
  for (const extension of [".js", ".d.ts"]) {
    const file = `${stem}${extension}`;
    const source = join(root, "generated/typescript", file);
    const normalized = normalizeGeneratedTypeScript(readFileSync(source, "utf8"));
    writeFileSync(source, normalized);
    for (const name of packages) {
      const destination = join(root, "typescript/packages", name, "generated/proto", file);
      mkdirSync(dirname(destination), { recursive: true });
      writeFileSync(destination, normalized);
    }
  }
}

const digest = path =>
  `sha256:${createHash("sha256").update(readFileSync(join(root, path))).digest("hex")}`;
const compatibilityPath = join(root, "compatibility/manifest.json");
const compatibility = JSON.parse(readFileSync(compatibilityPath, "utf8"));
for (const [family, artifacts] of Object.entries(compatibilityArtifacts)) {
  for (const [field, path] of Object.entries(artifacts)) {
    compatibility.families[family][field] = digest(path);
  }
}
writeFileSync(compatibilityPath, `${JSON.stringify(compatibility, null, 2)}\n`);
