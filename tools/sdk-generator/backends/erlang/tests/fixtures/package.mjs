import { packageFixture } from "../../../../shared/tests/package-fixture.mjs";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { packageTemplates, pins, producerSources } from "../../src/generate.mjs";
import { expectedModules } from "../../src/package.mjs";
import { loadAuthority, sha256 } from "../../../../shared/authority.mjs";

const sourceDirectory = join(dirname(fileURLToPath(import.meta.url)), "../../src");

export function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "erlang-package-test-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const authority = join(root, "authority"); mkdirSync(authority);
  const inputs = new Map(), families = [];
  for (const [name, version] of [["actors", 1], ["stream", 1]]) {
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
  return packageFixture(root, authority, receipt, payload);
}
