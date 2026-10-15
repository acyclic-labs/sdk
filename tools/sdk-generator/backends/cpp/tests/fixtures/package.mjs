import { authorityFixture, packageFixture } from "../../../../shared/tests/package-fixture.mjs";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { pins, producerSources, targets } from "../../src/package.mjs";
import { sha256 } from "../../../../shared/authority.mjs";
const sourceDirectory = join(dirname(fileURLToPath(import.meta.url)), "../../src");
export function fixture(t) {
  const { root, authority, manifest, inputHashes } = authorityFixture(t, "cpp", targets);
  const templates = Object.fromEntries(["CMakeLists.txt", "AcyclicTransportConfig.cmake.in"].map(name => [name, readFileSync(join(sourceDirectory, "../templates/package", name))]));
  const payload = new Map([["LICENSE", Buffer.from("license")], ["NOTICE", Buffer.from("notice")],
    ...Object.entries(templates), ["authority/rust-authority.json", manifest]]);
  for (const source of targets) for (const suffix of [".pb.h", ".pb.cc", ".grpc.pb.h", ".grpc.pb.cc"]) {
    payload.set((suffix.endsWith(".h") ? "include/" : "src/") + source.slice(0, -6) + suffix, Buffer.from("archive staging fixture"));
  }
  const sourceHashes = Object.fromEntries(producerSources.map(name => [name, sha256(readFileSync(join(sourceDirectory, name)))]));
  const receipt = { schema: "acyclic.sdk.cpp-producer-receipt.v1", authority: "rust", target: "cpp", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), input_sha256: inputHashes,
    generator_sha256: sourceHashes["generate.mjs"], authority_reader_sha256: sourceHashes["../../../shared/authority.mjs"], source_sha256: sourceHashes,
    toolchain_sha256: sha256(JSON.stringify(pins)), protoc_version: pins.protoc_version,
    cpp_plugin_version: pins.cpp_plugin_version, cpp_plugin_coordinate: pins.cpp_plugin["win32-x64"].url,
    tool_sha256: { protoc: pins.protoc["win32-x64"].sha256, cpp_plugin: pins.cpp_plugin["win32-x64"].sha256 },
    template_sha256: Object.fromEntries(Object.entries(templates).map(([name, bytes]) => [name, sha256(bytes)])),
    outputs: [...payload.keys()].sort(), output_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])) };
  return packageFixture(root, authority, receipt, payload);
}
