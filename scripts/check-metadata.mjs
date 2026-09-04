import Ajv2020 from "ajv/dist/2020.js";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

const root = new URL("..", import.meta.url);
const load = async path => JSON.parse(await readFile(new URL(path, root), "utf8"));
const ajv = new Ajv2020({ allErrors: true });
const documents = [
  ["provenance/manifest.json", "compatibility/schemas/provenance.schema.json"],
  ["languages/package-names.json", "compatibility/schemas/package-names.schema.json"],
  ["compatibility/manifest.json", "compatibility/schemas/compatibility.schema.json"],
];
for (const [documentPath, schemaPath] of documents) {
  const validate = ajv.compile(await load(schemaPath));
  if (!validate(await load(documentPath))) throw new Error(`${documentPath}: ${ajv.errorsText(validate.errors)}`);
}
const provenance = await load("provenance/manifest.json");
for (const item of provenance.imports) if (item.auditResult !== "approved") throw new Error(`unapproved import: ${item.destinationPath}`);

const digest = async path => `sha256:${createHash("sha256").update(await readFile(new URL(path, root))).digest("hex")}`;
const compatibility = await load("compatibility/manifest.json");
const inferenceIndex = (await readFile(new URL("registry/in/fe/inference-sdk", root), "utf8"))
  .trim()
  .split("\n")
  .map(line => JSON.parse(line));
const inferenceRelease = inferenceIndex.find(item => item.vers === compatibility.families.inference.version);
if (!inferenceRelease || inferenceRelease.name !== "inference-sdk" || !/^[0-9a-f]{64}$/.test(inferenceRelease.cksum) || inferenceRelease.yanked) {
  throw new Error("current Inference family has no immutable sparse-registry package entry");
}
const familyArtifacts = {
  harness: {
    schemaDigest: "proto/harness/v1/harness.proto",
  },
  filesystem: {
    schemaDigest: "proto/filesystem/v1/filesystem.proto",
    descriptorDigest: "rust/crates/filesystem/src/generated/acyclic-filesystem-v1.bin",
    conformanceDigest: "conformance/vectors/filesystem/dependency-content-range-v1.json",
  },
  stream: {
    schemaDigest: "rust/crates/stream/proto/stream/v2/stream.proto",
    descriptorDigest: "rust/crates/stream/proto/stream/v2/stream_descriptor.bin",
    conformanceDigest: "conformance/vectors/stream.json",
  },
  objects: {
    schemaDigest: "proto/objects/v1/objects.proto",
    descriptorDigest: "rust/crates/objects/src/generated/acyclic-objects-v1.bin",
    conformanceDigest: "conformance/vectors/objects.json",
  },
  machines: {
    schemaDigest: "proto/machines/v1/machines.proto",
    descriptorDigest: "rust/crates/machines/src/generated/acyclic-machines-v1.bin",
    conformanceDigest: "conformance/vectors/machines.json",
  },
  inference: {
    schemaDigest: "proto/inference/v1/inference.proto",
    descriptorDigest: "rust/crates/inference/inference_descriptor.bin",
    conformanceDigest: "conformance/vectors/inference.json",
  },
};
for (const [family, artifacts] of Object.entries(familyArtifacts)) {
  for (const [field, path] of Object.entries(artifacts)) {
    if (compatibility.families[family][field] !== await digest(path)) {
      throw new Error(`${family} ${field} mismatch`);
    }
  }
}

for (const [canonical, packaged] of [
  ["conformance/vectors/stream.json", "rust/crates/stream/conformance/stream.json"],
  ["conformance/vectors/stream.json", "rust/crates/conformance/vectors/stream.json"],
  ["conformance/vectors/objects.json", "rust/crates/conformance/vectors/objects.json"],
  ["conformance/vectors/machines.json", "rust/crates/conformance/vectors/machines.json"],
  ["conformance/vectors/filesystem/dependency-content-range-v1.json", "rust/crates/conformance/vectors/filesystem/dependency-content-range-v1.json"],
]) {
  if (await digest(canonical) !== await digest(packaged)) {
    throw new Error(`packaged conformance vector drift: ${packaged}`);
  }
}

const validateProvenance = new Ajv2020().compile(await load("compatibility/schemas/provenance.schema.json"));
if (validateProvenance({ imports: [{ sourceCommit: "short" }] })) throw new Error("malformed provenance fixture was accepted");
