import { authorityFixture, packageFixture } from "../../../../shared/tests/package-fixture.mjs";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { pins, targets } from "../../src/package.mjs";
import { sha256 } from "../../../../shared/authority.mjs";
const directory = join(dirname(fileURLToPath(import.meta.url)), "..");
export function fixture(t) {
  const { root, authority, manifest, inputHashes } = authorityFixture(t, "swift", targets);
  const payload = new Map([["LICENSE", Buffer.from("license")], ["NOTICE", Buffer.from("notice")],
    ["Package.swift", readFileSync(join(directory, "../templates/package/Package.swift"))], ["authority/rust-authority.json", manifest]]);
  for (const source of targets) for (const suffix of [".pb.swift", ".grpc.swift"]) payload.set("Sources/AcyclicTransport/" + source.slice(0, -6) + suffix, Buffer.from("staging fixture"));
  const receipt = {
    schema: "acyclic.sdk.swift-producer-receipt.v1", authority: "rust", target: "swift", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), input_sha256: inputHashes,
    generator_sha256: sha256(readFileSync(join(directory, "../src/generate.mjs"))),
    authority_reader_sha256: sha256(readFileSync(join(directory, "../../../shared/authority.mjs"))), toolchain_sha256: sha256(JSON.stringify(pins)),
    protoc_version: pins.protoc_version, swift_plugin_version: pins.swift_plugin_version, grpc_plugin_version: pins.grpc_plugin_version,
    swift_version: pins.swift_runtime["linux-x64"].version, swift_runtime_sha256: pins.swift_runtime["linux-x64"].files,
    tool_sha256: { protoc: pins.protoc["linux-x64"].sha256, swift_plugin: pins.swift_plugin["linux-x64"].sha256, grpc_plugin: pins.grpc_plugin["linux-x64"].sha256 },
    outputs: [...payload.keys()].sort(), output_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])),
  };
  return packageFixture(root, authority, receipt, payload);
}
