import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { gzipSync } from "node:zlib";
import { sha256 } from "../authority.mjs";
import { tar } from "./archive.mjs";

export function authorityFixture(t, language, targets) {
  const root = mkdtempSync(join(tmpdir(), `${language}-package-test-`));
  t.after(() => rmSync(root, { recursive: true }));
  const authority = join(root, "authority"); mkdirSync(authority);
  const inputs = new Map(), families = [];
  for (const source of targets) {
    const descriptor = source + ".bin";
    for (const name of [source, descriptor]) {
      mkdirSync(dirname(join(authority, name)), { recursive: true });
      writeFileSync(join(authority, name), name); inputs.set(name, Buffer.from(name));
    }
    families.push({ source, source_sha256: sha256(source), descriptor, descriptor_sha256: sha256(descriptor) });
  }
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  writeFileSync(join(authority, "rust-authority.json"), manifest);
  return { root, authority, inputs, manifest, inputHashes: Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)])) };
}

export function sharedDescriptorAuthority(authority) {
  const put = (name, bytes) => {
    const path = join(authority, name); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes);
  };
  const families = ["actors/v1/actors.proto", "stream/v1/stream.proto"].map(source => {
    put(source, source);
    return { source, source_sha256: sha256(source), descriptor: "shared.bin", descriptor_sha256: sha256("descriptors") };
  });
  put("shared.bin", "descriptors"); put("import.proto", "import");
  families.push({ source: "import.proto", source_sha256: sha256("import") });
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  put("rust-authority.json", manifest);
  return manifest;
}

export function packageFixture(root, authority, receipt, payload) {
  const args = { authority, receipt: join(root, "receipt.json"), package: join(root, "package.tar.gz") };
  const saveReceipt = () => writeFileSync(args.receipt, JSON.stringify(receipt));
  const entries = () => [...payload].map(([name, bytes]) => ({ name, bytes }));
  const saveArchive = (members = entries()) => { const bytes = gzipSync(tar(members)); writeFileSync(args.package, bytes); args.sha256 = sha256(bytes); };
  saveReceipt(); saveArchive();
  return { args, receipt, payload, saveReceipt, saveArchive, entries };
}
