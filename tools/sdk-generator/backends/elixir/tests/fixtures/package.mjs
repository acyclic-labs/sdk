import { authorityFixture, packageFixture } from "../../../../shared/tests/package-fixture.mjs";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { pins, producerSources, targets } from "../../src/package.mjs";
import { sha256 } from "../../../../shared/authority.mjs";
const sourceDirectory = join(dirname(fileURLToPath(import.meta.url)), "../../src");
export function fixture(t) {
  const { root, authority, inputs, manifest, inputHashes } = authorityFixture(t, "elixir", targets);
  const templates = Object.fromEntries(["mix.exs", "README.md"].map(name => [name, readFileSync(join(sourceDirectory, "../templates/package", name))]));
  const payload = new Map([["LICENSE", Buffer.from("license")], ["NOTICE", Buffer.from("notice")],
    ...Object.entries(templates), ["authority/rust-authority.json", manifest]]);
  for (const [name, bytes] of inputs) payload.set("authority/" + name, bytes);
  for (const source of targets) payload.set("lib/acyclic/" + source.slice(0, -6) + ".pb.ex", Buffer.from("archive staging fixture"));
  const sourceHashes = Object.fromEntries(producerSources.map(name => [name, sha256(readFileSync(join(sourceDirectory, name)))]));
  const runtime = pins.runtime["linux-x64"];
  const receipt = { schema: "acyclic.sdk.elixir-producer-receipt.v1", authority: "rust", target: "elixir", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), input_sha256: inputHashes,
    generator_sha256: sourceHashes["generate.mjs"], authority_reader_sha256: sourceHashes["../../../shared/authority.mjs"], source_sha256: sourceHashes,
    toolchain_sha256: sha256(JSON.stringify(pins)), protoc_version: pins.protoc_version, protobuf_version: pins.protobuf_version,
    otp_version: runtime.otp_version, elixir_version: runtime.elixir_version, runtime_inventory_sha256: runtime.inventory_sha256,
    tool_sha256: { protoc: pins.protoc["linux-x64"].sha256, elixir_plugin: pins.elixir_plugin["linux-x64"].sha256 },
    template_sha256: Object.fromEntries(Object.entries(templates).map(([name, bytes]) => [name, sha256(bytes)])),
    outputs: [...payload.keys()].sort(), output_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])) };
  return packageFixture(root, authority, receipt, payload);
}
