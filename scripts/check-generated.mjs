import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { compatibilityArtifacts, generatedDescriptors, nativeWasmVector, normalizeGeneratedRust, normalizeGeneratedTypeScript, packagedRustBindings, packagedSourceCopies, packagedTypeScriptBindings } from "./generated-bindings.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const generatedFiles = directory => {
  const files = [];
  const visit = (current, prefix) => {
    for (const entry of readdirSync(current, { withFileTypes: true })) {
      const relative = join(prefix, entry.name);
      if (entry.isDirectory()) visit(join(current, entry.name), relative);
      else if (entry.isFile()) files.push(relative);
    }
  };
  visit(directory, "");
  return files.sort();
};
const temporary = mkdtempSync(join(tmpdir(), "acyclic-sdk-codegen-"));
try {
  for (const [source, packaged] of packagedSourceCopies) {
    if (!readFileSync(join(root, source)).equals(readFileSync(join(root, packaged)))) {
      throw new Error(`packaged source drift: ${packaged}`);
    }
  }
  const harnessSuite = JSON.parse(readFileSync(join(root, compatibilityArtifacts.harness.conformanceDigest), "utf8"));
  const nativeWasmCase = harnessSuite.cases.find(item => item.name === "native-wasm-replay-is-byte-equivalent");
  const vector = JSON.parse(readFileSync(join(root, nativeWasmVector), "utf8"));
  if (JSON.stringify(nativeWasmCase?.vector) !== JSON.stringify(vector)) {
    throw new Error("native/WASM fixture is not bound into the Harness suite");
  }

  const executable = join(root, "node_modules", ".bin", process.platform === "win32" ? "buf.exe" : "buf");
  const generated = spawnSync(
    executable,
    ["generate", "--output", temporary],
    { cwd: root, encoding: "utf8" },
  );
  if (generated.status !== 0) {
    process.stderr.write(generated.stdout ?? "");
    process.stderr.write(generated.stderr ?? "");
    throw new Error(`Buf generation failed with status ${generated.status ?? "unknown"}`);
  }
  const freshTypeScript = join(temporary, "generated/typescript");
  const committedTypeScript = join(root, "generated/typescript");
  const freshFiles = generatedFiles(freshTypeScript);
  if (JSON.stringify(freshFiles) !== JSON.stringify(generatedFiles(committedTypeScript))) {
    throw new Error("generated TypeScript file set drift; run bun run generate");
  }
  for (const relative of freshFiles) {
    const fresh = normalizeGeneratedTypeScript(readFileSync(join(freshTypeScript, relative), "utf8"));
    const committed = readFileSync(join(committedTypeScript, relative), "utf8");
    if (fresh !== committed) throw new Error(`generated TypeScript drift: ${relative}`);
  }
  const freshRust = join(temporary, "generated/rust");
  const committedRust = join(root, "generated/rust");
  const freshRustFiles = generatedFiles(freshRust);
  if (JSON.stringify(freshRustFiles) !== JSON.stringify(generatedFiles(committedRust))) {
    throw new Error("generated Rust file set drift; run bun run generate");
  }
  for (const relative of freshRustFiles) {
    const fresh = normalizeGeneratedRust(relative.replaceAll("\\", "/"), readFileSync(join(freshRust, relative), "utf8"));
    const committed = readFileSync(join(committedRust, relative), "utf8");
    if (fresh !== committed) throw new Error(`generated Rust drift: ${relative}`);
  }
  for (const [source, destination] of generatedDescriptors) {
    const descriptor = join(temporary, destination.replaceAll("/", "-"));
    const built = spawnSync(executable, ["build", "--path", source, "-o", descriptor], {
      cwd: root,
      encoding: "utf8",
    });
    if (built.status !== 0) {
      process.stderr.write(built.stdout ?? "");
      process.stderr.write(built.stderr ?? "");
      throw new Error(`Buf descriptor build failed for ${source}: ${built.status ?? "unknown"}`);
    }
    if (!readFileSync(descriptor).equals(readFileSync(join(root, destination)))) {
      throw new Error(`generated descriptor drift: ${destination}`);
    }
  }

  for (const [relative, packaged] of packagedRustBindings) {
    const canonical = readFileSync(join(committedRust, relative));
    if (!canonical.equals(readFileSync(join(root, packaged)))) {
      throw new Error(`packaged Rust drift: ${relative}`);
    }
  }

  for (const [stem, packages] of packagedTypeScriptBindings) {
    for (const extension of [".js", ".d.ts"]) {
      const relative = `${stem}${extension}`;
      const canonical = readFileSync(join(committedTypeScript, relative));
      for (const name of packages) {
        const packaged = readFileSync(join(root, "typescript/packages", name, "generated/proto", relative));
        if (!canonical.equals(packaged)) throw new Error(`packaged ${name} TypeScript drift: ${relative}`);
      }
    }
  }
  for (const name of new Set(packagedTypeScriptBindings.flatMap(([, packages]) => packages))) {
    const expected = packagedTypeScriptBindings
      .filter(([, packages]) => packages.includes(name))
      .flatMap(([stem]) => [`.d.ts`, `.js`].map(extension => `${stem}${extension}`))
      .sort();
    const actual = generatedFiles(join(root, "typescript/packages", name, "generated/proto"))
      .map(path => path.replaceAll("\\", "/"))
      .sort();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      throw new Error(`packaged ${name} TypeScript file set drift`);
    }
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
