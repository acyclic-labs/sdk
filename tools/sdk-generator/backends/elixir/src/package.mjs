import { readFileSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { readDeclaredTarGzip } from "../../../../../scripts/archive-utils.mjs";
import { equalEntries, loadAuthority, sha256, verifyPackageOutputs } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
export const pins = { ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"))),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"))) };
export const targets = ["actors/v1/actors.proto", "stream/v1/stream.proto"];
export const producerSources = ["generate.mjs", "runtime.mjs", "../../../shared/authority.mjs", "../../../shared/protoc.json", "../toolchains/toolchain.json"];

// Archive admission only. Native compilation and consumer execution are separate.
export function readPackage(args) {
  if (!/^[a-f0-9]{64}$/.test(args.sha256)) throw new Error("archive SHA256 is required");
  if (statSync(args.receipt).size > 1024 * 1024) throw new Error("generation receipt exceeds size bound");
  const receiptBytes = readFileSync(args.receipt), receipt = JSON.parse(receiptBytes), approved = loadAuthority(args.authority);
  if (receipt.schema !== "acyclic.sdk.elixir-producer-receipt.v1" || receipt.authority !== "rust" || receipt.target !== "elixir"
    || !Array.isArray(receipt.outputs) || !receipt.output_sha256 || !receipt.input_sha256) throw new Error("unsupported Elixir generation receipt");
  if (receipt.source_revision !== approved.manifest.source_revision || receipt.authority_manifest_sha256 !== sha256(approved.bytes)
    || !equalEntries(receipt.input_sha256, Object.fromEntries([...approved.inputs].map(([name, bytes]) => [name, sha256(bytes)])))) throw new Error("package authority differs");
  if (targets.some(name => !approved.descriptors.has(name))) throw new Error("tested family lacks Rust descriptors");
  const sourceHashes = Object.fromEntries(producerSources.map(name => [name, sha256(readFileSync(join(directory, name)))]));
  if (receipt.generator_sha256 !== sourceHashes["generate.mjs"] || receipt.authority_reader_sha256 !== sourceHashes["../../../shared/authority.mjs"]
    || !equalEntries(receipt.source_sha256 ?? {}, sourceHashes) || receipt.toolchain_sha256 !== sha256(JSON.stringify(pins))) throw new Error("producer source or pins differ");
  const runtime = pins.runtime["linux-x64"];
  if (receipt.protoc_version !== pins.protoc_version || receipt.protobuf_version !== pins.protobuf_version
    || receipt.otp_version !== runtime.otp_version || receipt.elixir_version !== runtime.elixir_version || receipt.runtime_inventory_sha256 !== runtime.inventory_sha256
    || !equalEntries(receipt.tool_sha256 ?? {}, { protoc: pins.protoc["linux-x64"].sha256, elixir_plugin: pins.elixir_plugin["linux-x64"].sha256 })) throw new Error("generation tools differ from admitted pins");
  const templates = Object.fromEntries(["mix.exs", "README.md"].map(name => [name, readFileSync(join(directory, "../templates/package", name))]));
  if (!equalEntries(receipt.template_sha256 ?? {}, Object.fromEntries(Object.entries(templates).map(([name, bytes]) => [name, sha256(bytes)])))) throw new Error("package templates differ");
  const required = new Set(["mix.exs", "README.md", "LICENSE", "NOTICE", "authority/rust-authority.json",
    ...[...approved.inputs.keys()].map(name => "authority/" + name),
    ...approved.manifest.families.map(family => "lib/acyclic/" + family.source.slice(0, -6) + ".pb.ex")]);
  verifyPackageOutputs(receipt.outputs, receipt.output_sha256, required, "unsafe, duplicate or unexpected Elixir package output");
  const { compressed, payload } = readDeclaredTarGzip(args.package, args.sha256, receipt.output_sha256, 64 * 1024 * 1024);
  if (Object.entries(templates).some(([name, bytes]) => !payload.get(name).equals(bytes)) || !payload.get("authority/rust-authority.json").equals(approved.bytes)) throw new Error("package metadata differs from maintained inputs");
  for (const [name, bytes] of approved.inputs) if (!payload.get("authority/" + name).equals(bytes)) throw new Error("packaged Rust input differs");
  return { payload, receipt, receiptBytes, approved, archiveBytes: compressed };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({ options: Object.fromEntries(["package", "sha256", "receipt", "authority"].map(name => [name, { type: "string" }])) });
  if (["package", "sha256", "receipt", "authority"].some(name => !values[name])) throw new Error("all archive admission arguments are required");
  const admitted = readPackage(values);
  console.log(JSON.stringify({ scope: "Elixir archive admission only", archive_sha256: sha256(admitted.archiveBytes), source_revision: admitted.receipt.source_revision, payload_files: admitted.payload.size }, null, 2));
}
