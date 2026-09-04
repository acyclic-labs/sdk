import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const files = [
  ["acyclic/filesystem/v2/acyclic.filesystem.v2.rs", "acyclic.filesystem.v2.tonic.rs"],
  [
    "acyclic/filesystem/daemon/v2/acyclic.filesystem.daemon.v2.rs",
    "acyclic.filesystem.daemon.v2.tonic.rs",
  ],
];

for (const [file, tonic] of files) {
  for (const generatedFile of [file, join(dirname(file), tonic)]) {
    const source = join(root, "generated/rust", generatedFile);
    const destination = join(root, "rust/crates/filesystem/src/generated", generatedFile);
    if (!existsSync(source)) {
      throw new Error(`filesystem code-generation path is missing: ${generatedFile}`);
    }
    const normalized = `${readFileSync(source, "utf8").trimEnd()}\n`;
    mkdirSync(dirname(destination), { recursive: true });
    writeFileSync(source, normalized);
    writeFileSync(destination, normalized);
  }
}

for (const file of [
  "filesystem/v2/filesystem_pb.js",
  "filesystem/v2/filesystem_pb.d.ts",
  "filesystem/daemon/v2/daemon_pb.js",
  "filesystem/daemon/v2/daemon_pb.d.ts",
  "harness/v1/harness_pb.js",
  "harness/v1/harness_pb.d.ts",
]) {
  const source = join(root, "generated/typescript", file);
  const destination = join(root, "typescript/packages/filesystem/generated/proto", file);
  if (!existsSync(source)) {
    throw new Error(`filesystem TypeScript generation path is missing: ${file}`);
  }
  mkdirSync(dirname(destination), { recursive: true });
  copyFileSync(source, destination);
  if (file.endsWith(".js")) {
    const normalized = `${readFileSync(source, "utf8").trimEnd()}\n`;
    writeFileSync(source, normalized);
    writeFileSync(destination, normalized);
  }
}

const digest = path =>
  `sha256:${createHash("sha256").update(readFileSync(join(root, path))).digest("hex")}`;
const compatibilityPath = join(root, "compatibility/manifest.json");
const compatibility = JSON.parse(readFileSync(compatibilityPath, "utf8"));
compatibility.families.filesystem.schemaDigest = digest("proto/filesystem/v2/filesystem.proto");
compatibility.families.filesystem.descriptorDigest = digest(
  "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin",
);
writeFileSync(compatibilityPath, `${JSON.stringify(compatibility, null, 2)}\n`);
