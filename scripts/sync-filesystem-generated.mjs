import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const harnessProto = join(root, "proto/harness/v1/harness.proto");
const packagedHarnessProto = join(root, "rust/crates/harness/proto/harness/v1/harness.proto");
mkdirSync(dirname(packagedHarnessProto), { recursive: true });
copyFileSync(harnessProto, packagedHarnessProto);
copyFileSync(
  join(root, "conformance/vectors/core.json"),
  join(root, "rust/crates/conformance/vectors/harness.json"),
);
const nativeWasmVector = join(root, "conformance/vectors/harness/native-wasm-event-v1.json");
const packagedNativeWasmVector = join(root, "rust/crates/harness/conformance/native-wasm-event-v1.json");
mkdirSync(dirname(packagedNativeWasmVector), { recursive: true });
copyFileSync(nativeWasmVector, packagedNativeWasmVector);

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

for (const file of ["harness/v1/harness_pb.js", "harness/v1/harness_pb.d.ts"]) {
  const source = join(root, "generated/typescript", file);
  const destination = join(root, "typescript/packages/harness/generated/proto", file);
  if (!existsSync(source)) {
    throw new Error(`harness TypeScript generation path is missing: ${file}`);
  }
  mkdirSync(dirname(destination), { recursive: true });
  copyFileSync(source, destination);
  if (file.endsWith(".js")) {
    const normalized = `${readFileSync(source, "utf8").trimEnd()}\n`;
    writeFileSync(source, normalized);
    writeFileSync(destination, normalized);
  }
}

for (const file of ["inference/v1/inference_pb.js", "inference/v1/inference_pb.d.ts"]) {
  const source = join(root, "generated/typescript", file);
  const destination = join(root, "typescript/packages/inference/generated/proto", file);
  if (!existsSync(source)) throw new Error(`inference TypeScript generation path is missing: ${file}`);
  mkdirSync(dirname(destination), { recursive: true });
  copyFileSync(source, destination);
}

const digest = path =>
  `sha256:${createHash("sha256").update(readFileSync(join(root, path))).digest("hex")}`;
const compatibilityPath = join(root, "compatibility/manifest.json");
const compatibility = JSON.parse(readFileSync(compatibilityPath, "utf8"));
compatibility.families.harness.schemaDigest = digest("proto/harness/v1/harness.proto");
compatibility.families.harness.conformanceDigest = digest("conformance/vectors/core.json");
compatibility.families.filesystem.schemaDigest = digest("proto/filesystem/v2/filesystem.proto");
compatibility.families.filesystem.descriptorDigest = digest(
  "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin",
);
writeFileSync(compatibilityPath, `${JSON.stringify(compatibility, null, 2)}\n`);
