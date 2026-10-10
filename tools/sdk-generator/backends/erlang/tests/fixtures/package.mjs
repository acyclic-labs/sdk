import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { packageTemplates, pins, producerSources } from "../../src/generate.mjs";
import { expectedModules } from "../../src/package.mjs";
import { loadAuthority, sha256 } from "../../../../shared/authority.mjs";

const sourceDirectory = join(dirname(fileURLToPath(import.meta.url)), "../../src");
export function tar(entries) {
  const blocks = [];
  for (const { name, bytes, type = "0" } of entries) {
    const header = Buffer.alloc(512);
    header.write(name, 0, 100); header.write("0000644\0", 100); header.write("0000000\0", 108); header.write("0000000\0", 116);
    header.write(bytes.length.toString(8).padStart(11, "0") + "\0", 124); header.write("00000000000\0", 136);
    header.fill(32, 148, 156); header.write(type, 156); header.write("ustar\0", 257); header.write("00", 263);
    header.write(header.reduce((sum, byte) => sum + byte, 0).toString(8).padStart(6, "0") + "\0 ", 148);
    blocks.push(header, bytes, Buffer.alloc((512 - bytes.length % 512) % 512));
  }
  return Buffer.concat([...blocks, Buffer.alloc(1024)]);
}

export function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "erlang-package-test-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const authority = join(root, "authority"); mkdirSync(authority);
  const inputs = new Map(), families = [];
  for (const [name, version] of [["actors", 1], ["workers", 1], ["stream", 2]]) {
    const source = `${name}/v${version}/${name}.proto`, descriptor = source + ".bin";
    const schema = Buffer.from(`syntax = "proto3";\npackage acyclic.${name}.v${version};\nservice ${name[0].toUpperCase() + name.slice(1)}Service {\n}\n`);
    for (const [file, bytes] of [[source, schema], [descriptor, Buffer.from("Rust descriptor " + name)]]) {
      mkdirSync(dirname(join(authority, file)), { recursive: true }); writeFileSync(join(authority, file), bytes); inputs.set(file, bytes);
    }
    families.push({ source, descriptor, source_sha256: sha256(schema), descriptor_sha256: sha256(inputs.get(descriptor)) });
  }
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  writeFileSync(join(authority, "rust-authority.json"), manifest);
  const templates = new Map([...packageTemplates].map(([name, template]) => [name, readFileSync(join(sourceDirectory, "../templates/package", template))]));
  const payload = new Map([["LICENSE", Buffer.from("license")], ["NOTICE", Buffer.from("notice")], ...templates, ["authority/rust-authority.json", manifest]]);
  for (const [name, bytes] of inputs) payload.set("authority/" + name, bytes);
  for (const name of expectedModules(loadAuthority(authority))) payload.set(name, Buffer.from("archive admission fixture: " + name));
  const runtime = pins.runtime["linux-x64"];
  const receipt = { schema: "acyclic.sdk.erlang-producer-receipt.v1", authority: "rust", target: "erlang", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), input_sha256: Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)])),
    source_sha256: Object.fromEntries(producerSources.map(name => [name, sha256(readFileSync(join(sourceDirectory, name)))])),
    toolchain_sha256: sha256(JSON.stringify(pins)), gpb_version: pins.gpb_version, grpcbox_plugin_version: pins.grpcbox_plugin_version,
    rebar_version: pins.rebar_version, generator_inventory_sha256: pins.generator_inventory_sha256,
    otp_version: runtime.otp_version, runtime_inventory_sha256: runtime.inventory_sha256,
    template_sha256: Object.fromEntries([...templates].map(([name, bytes]) => [name, sha256(bytes)])),
    outputs: [...payload.keys()].sort(), output_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])) };
  const args = { authority, receipt: join(root, "receipt.json"), package: join(root, "package.tar.gz") };
  const saveReceipt = () => writeFileSync(args.receipt, JSON.stringify(receipt));
  const entries = () => [...payload].map(([name, bytes]) => ({ name, bytes }));
  const saveArchive = (members = entries()) => { const bytes = gzipSync(tar(members)); writeFileSync(args.package, bytes); args.sha256 = sha256(bytes); };
  saveReceipt(); saveArchive();
  return { args, receipt, payload, saveReceipt, saveArchive, entries };
}
