import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { pins, producerSources, targets } from "../../src/package.mjs";
import { sha256 } from "../../../../shared/authority.mjs";
const sourceDirectory = join(dirname(fileURLToPath(import.meta.url)), "../../src");
function tar(entries) {
  const blocks = [];
  for (const { name, bytes, type = "0" } of entries) {
    const header = Buffer.alloc(512);
    header.write(name, 0, 100); header.write("0000644\0", 100); header.write("0000000\0", 108); header.write("0000000\0", 116);
    header.write(bytes.length.toString(8).padStart(11, "0") + "\0", 124); header.write("00000000000\0", 136);
    header.fill(32, 148, 156); header.write(type, 156); header.write("ustar\0", 257); header.write("00", 263);
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    header.write(checksum.toString(8).padStart(6, "0") + "\0 ", 148);
    blocks.push(header, bytes, Buffer.alloc((512 - bytes.length % 512) % 512));
  }
  return Buffer.concat([...blocks, Buffer.alloc(1024)]);
}
export function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "cpp-package-test-"));
  t.after(() => rmSync(root, { recursive: true }));
  const authority = join(root, "authority"); mkdirSync(authority);
  const inputs = new Map(), families = [];
  for (const source of targets) {
    const descriptor = source + ".bin";
    for (const name of [source, descriptor]) {
      mkdirSync(dirname(join(authority, name)), { recursive: true }); writeFileSync(join(authority, name), name); inputs.set(name, Buffer.from(name));
    }
    families.push({ source, source_sha256: sha256(source), descriptor, descriptor_sha256: sha256(descriptor) });
  }
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  writeFileSync(join(authority, "rust-authority.json"), manifest);
  const templates = Object.fromEntries(["CMakeLists.txt", "AcyclicTransportConfig.cmake.in"].map(name => [name, readFileSync(join(sourceDirectory, "../templates/package", name))]));
  const payload = new Map([["LICENSE", Buffer.from("license")], ["NOTICE", Buffer.from("notice")],
    ...Object.entries(templates), ["authority/rust-authority.json", manifest]]);
  for (const source of targets) for (const suffix of [".pb.h", ".pb.cc", ".grpc.pb.h", ".grpc.pb.cc"]) {
    payload.set((suffix.endsWith(".h") ? "include/" : "src/") + source.slice(0, -6) + suffix, Buffer.from("archive staging fixture"));
  }
  const sourceHashes = Object.fromEntries(producerSources.map(name => [name, sha256(readFileSync(join(sourceDirectory, name)))]));
  const receipt = { schema: "acyclic.sdk.cpp-producer-receipt.v1", authority: "rust", target: "cpp", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), input_sha256: Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)])),
    generator_sha256: sourceHashes["generate.mjs"], authority_reader_sha256: sourceHashes["../../../shared/authority.mjs"], source_sha256: sourceHashes,
    toolchain_sha256: sha256(JSON.stringify(pins)), protoc_version: pins.protoc_version,
    cpp_plugin_version: pins.cpp_plugin_version, cpp_plugin_coordinate: pins.cpp_plugin["win32-x64"].url,
    tool_sha256: { protoc: pins.protoc["win32-x64"].sha256, cpp_plugin: pins.cpp_plugin["win32-x64"].sha256 },
    template_sha256: Object.fromEntries(Object.entries(templates).map(([name, bytes]) => [name, sha256(bytes)])),
    outputs: [...payload.keys()].sort(), output_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])) };
  const args = { authority, receipt: join(root, "receipt.json"), package: join(root, "package.tar.gz") };
  const saveReceipt = () => writeFileSync(args.receipt, JSON.stringify(receipt));
  const entries = () => [...payload].map(([name, bytes]) => ({ name, bytes }));
  const saveArchive = (members = entries()) => { const bytes = gzipSync(tar(members)); writeFileSync(args.package, bytes); args.sha256 = sha256(bytes); };
  saveReceipt(); saveArchive();
  return { args, receipt, payload, saveReceipt, saveArchive, entries };
}
