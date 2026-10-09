import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { compatibilityArtifacts, normalizeGeneratedRust, normalizeGeneratedTypeScript, packagedRustBindings, packagedSourceCopies, packagedTypeScriptBindings } from "./generated-bindings.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const writeChanged = (path, content) => {
  const bytes = Buffer.from(content);
  if (!existsSync(path) || !readFileSync(path).equals(bytes)) writeFileSync(path, bytes);
};
for (const [source, packaged] of packagedSourceCopies) {
  const destination = join(root, packaged);
  mkdirSync(dirname(destination), { recursive: true });
  writeChanged(destination, readFileSync(join(root, source)));
}

// Crates own the only copies of the Rust bindings they compile.
const generatedRust = join(root, "generated/rust");
for (const [relative, packaged] of packagedRustBindings) {
  const source = join(generatedRust, relative);
  if (!existsSync(source)) throw new Error(`Rust generation path is missing: ${relative}`);
  writeChanged(join(root, packaged), normalizeGeneratedRust(relative, readFileSync(source, "utf8")));
}

// Buf's TypeScript output is an intermediate: packages own the only copies.
const generatedTypeScript = join(root, "generated/typescript");
const packageRoot = resolve(root, "typescript/packages");
for (const [stem] of packagedTypeScriptBindings) {
  for (const extension of [".js", ".d.ts"]) {
    const file = `${stem}${extension}`;
    if (!existsSync(join(generatedTypeScript, file))) {
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
    const normalized = normalizeGeneratedTypeScript(readFileSync(join(generatedTypeScript, file), "utf8"));
    for (const name of packages) {
      const destination = join(root, "typescript/packages", name, "generated/proto", file);
      mkdirSync(dirname(destination), { recursive: true });
      writeChanged(destination, normalized);
    }
  }
}
// Both Buf output trees are intermediates; nothing under generated/ is committed.
rmSync(join(root, "generated"), { recursive: true, force: true });

const digest = path =>
  `sha256:${createHash("sha256").update(readFileSync(join(root, path))).digest("hex")}`;
const compatibilityPath = join(root, "compatibility/manifest.json");
const compatibility = JSON.parse(readFileSync(compatibilityPath, "utf8"));
for (const [family, artifacts] of Object.entries(compatibilityArtifacts)) {
  for (const [field, path] of Object.entries(artifacts)) {
    compatibility.families[family][field] = digest(path);
  }
}
writeChanged(compatibilityPath, `${JSON.stringify(compatibility, null, 2)}\n`);
