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
const familyArtifacts = {
  filesystem: {
    schemaDigest: "proto/filesystem/v1/filesystem.proto",
    descriptorDigest: "generated/rust/acyclic/filesystem/v1/acyclic.filesystem.v1.rs",
    conformanceDigest: "conformance/vectors/filesystem/dependency-content-range-v1.json",
  },
  stream: {
    schemaDigest: "rust/crates/stream/proto/stream/v2/stream.proto",
    conformanceDigest: "conformance/vectors/stream.json",
  },
  objects: {
    schemaDigest: "proto/objects/v1/objects.proto",
    conformanceDigest: "conformance/vectors/objects.json",
  },
  machines: {
    schemaDigest: "proto/machines/v1/machines.proto",
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

const validateProvenance = new Ajv2020().compile(await load("compatibility/schemas/provenance.schema.json"));
if (validateProvenance({ imports: [{ sourceCommit: "short" }] })) throw new Error("malformed provenance fixture was accepted");
