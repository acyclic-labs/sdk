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

for (const generatedFile of [
  "acyclic/objects/v1/acyclic.objects.v1.rs",
  "acyclic/objects/v1/acyclic.objects.v1.tonic.rs",
]) {
  const source = join(root, "generated/rust", generatedFile);
  const destination = join(root, "rust/crates/objects/src/generated", generatedFile.split("/").at(-1));
  if (!existsSync(source)) {
    throw new Error(`objects code-generation path is missing: ${generatedFile}`);
  }
  let normalized = `${readFileSync(source, "utf8").trimEnd()}\n`;
  if (generatedFile.endsWith("acyclic.objects.v1.rs")) {
    normalized = normalized.replace(
      /(?:#\[cfg\(feature = "grpc"\)\]\r?\n)?include!\("acyclic\.objects\.v1\.tonic\.rs"\);/,
      '#[cfg(feature = "grpc")]\ninclude!("acyclic.objects.v1.tonic.rs");',
    );
  }
  writeFileSync(source, normalized);
  writeFileSync(destination, normalized);
}

{
  const messages = join(root, "generated/rust/acyclic/machines/v1/acyclic.machines.v1.rs");
  const service = join(root, "generated/rust/acyclic/machines/v1/acyclic.machines.v1.tonic.rs");
  const destination = join(root, "rust/crates/machines/src/generated");
  if (!existsSync(messages) || !existsSync(service)) {
    throw new Error("machines code-generation output is missing");
  }
  for (const source of [messages, service]) {
    const normalized = `${readFileSync(source, "utf8").trimEnd()}\n`;
    writeFileSync(source, normalized);
    writeFileSync(join(destination, source.split(/[\\/]/).at(-1)), normalized);
  }
}

for (const file of [
  "filesystem/v2/filesystem_pb.js",
  "filesystem/v2/filesystem_pb.d.ts",
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
compatibility.families.stream.schemaDigest = digest(
  "rust/crates/stream/proto/stream/v2/stream.proto",
);
compatibility.families.objects.schemaDigest = digest("proto/objects/v1/objects.proto");
compatibility.families.objects.descriptorDigest = digest(
  "rust/crates/objects/src/generated/acyclic-objects-v1.bin",
);
compatibility.families.machines.schemaDigest = digest("proto/machines/v1/machines.proto");
compatibility.families.machines.descriptorDigest = digest(
  "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
);
compatibility.families.inference.schemaDigest = digest("proto/inference/v1/inference.proto");
compatibility.families.inference.descriptorDigest = digest(
  "rust/crates/inference/inference_descriptor.bin",
);
compatibility.families.inference.conformanceDigest = digest(
  "conformance/vectors/inference.json",
);
writeFileSync(compatibilityPath, `${JSON.stringify(compatibility, null, 2)}\n`);
