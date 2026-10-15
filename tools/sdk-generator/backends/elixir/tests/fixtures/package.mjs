import { tar } from "../../../../shared/tests/archive.mjs";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { pins, producerSources, targets } from "../../src/package.mjs";
import { sha256 } from "../../../../shared/authority.mjs";
const sourceDirectory = join(dirname(fileURLToPath(import.meta.url)), "../../src");
export function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "elixir-package-test-"));
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
  const templates = Object.fromEntries(["mix.exs", "README.md"].map(name => [name, readFileSync(join(sourceDirectory, "../templates/package", name))]));
  const payload = new Map([["LICENSE", Buffer.from("license")], ["NOTICE", Buffer.from("notice")],
    ...Object.entries(templates), ["authority/rust-authority.json", manifest]]);
  for (const [name, bytes] of inputs) payload.set("authority/" + name, bytes);
  for (const source of targets) payload.set("lib/acyclic/" + source.slice(0, -6) + ".pb.ex", Buffer.from("archive staging fixture"));
  const sourceHashes = Object.fromEntries(producerSources.map(name => [name, sha256(readFileSync(join(sourceDirectory, name)))]));
  const runtime = pins.runtime["linux-x64"];
  const receipt = { schema: "acyclic.sdk.elixir-producer-receipt.v1", authority: "rust", target: "elixir", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), input_sha256: Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)])),
    generator_sha256: sourceHashes["generate.mjs"], authority_reader_sha256: sourceHashes["../../../shared/authority.mjs"], source_sha256: sourceHashes,
    toolchain_sha256: sha256(JSON.stringify(pins)), protoc_version: pins.protoc_version, protobuf_version: pins.protobuf_version,
    otp_version: runtime.otp_version, elixir_version: runtime.elixir_version, runtime_inventory_sha256: runtime.inventory_sha256,
    tool_sha256: { protoc: pins.protoc["linux-x64"].sha256, elixir_plugin: pins.elixir_plugin["linux-x64"].sha256 },
    template_sha256: Object.fromEntries(Object.entries(templates).map(([name, bytes]) => [name, sha256(bytes)])),
    outputs: [...payload.keys()].sort(), output_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])) };
  const args = { authority, receipt: join(root, "receipt.json"), package: join(root, "package.tar.gz") };
  const saveReceipt = () => writeFileSync(args.receipt, JSON.stringify(receipt));
  const entries = () => [...payload].map(([name, bytes]) => ({ name, bytes }));
  const saveArchive = (members = entries()) => { const bytes = gzipSync(tar(members)); writeFileSync(args.package, bytes); args.sha256 = sha256(bytes); };
  saveReceipt(); saveArchive();
  return { args, receipt, payload, saveReceipt, saveArchive, entries };
}
