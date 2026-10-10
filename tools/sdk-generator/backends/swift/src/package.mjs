import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { readBoundedGzip, tarEntries } from "../../../../../scripts/archive-utils.mjs";
import { canonical, loadAuthority, sha256 } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
export const pins = {
  ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"))),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"))),
};
export const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v1/stream.proto"];
const equal = (left, right) => JSON.stringify(Object.entries(left).sort()) === JSON.stringify(Object.entries(right).sort());

// Admission only. Compilation and native transport controls must run separately.
export function readPackage(args) {
  if (!/^[a-f0-9]{64}$/.test(args.sha256)) throw new Error("archive SHA256 is required");
  const receiptBytes = readFileSync(args.receipt), receipt = JSON.parse(receiptBytes);
  const approved = loadAuthority(args.authority);
  if (receipt.schema !== "acyclic.sdk.swift-producer-receipt.v1" || receipt.target !== "swift" || receipt.authority !== "rust"
    || !Array.isArray(receipt.outputs) || !receipt.output_sha256 || !receipt.input_sha256) throw new Error("unsupported Swift generation receipt");
  if (receipt.source_revision !== approved.manifest.source_revision || receipt.authority_manifest_sha256 !== sha256(approved.bytes)
    || !equal(receipt.input_sha256, Object.fromEntries([...approved.inputs].map(([name, bytes]) => [name, sha256(bytes)])))) throw new Error("package authority differs");
  if (targets.some(name => !approved.descriptors.has(name))) throw new Error("tested family lacks Rust descriptors");
  if (receipt.generator_sha256 !== sha256(readFileSync(join(directory, "generate.mjs")))
    || receipt.authority_reader_sha256 !== sha256(readFileSync(join(directory, "../../../shared/authority.mjs")))
    || receipt.toolchain_sha256 !== sha256(JSON.stringify(pins))) throw new Error("producer source or pins differ");
  const host = "linux-x64";
  if (receipt.protoc_version !== pins.protoc_version || receipt.swift_plugin_version !== pins.swift_plugin_version
    || receipt.grpc_plugin_version !== pins.grpc_plugin_version || receipt.swift_version !== pins.swift_runtime[host].version
    || !equal(receipt.tool_sha256 ?? {}, { protoc: pins.protoc[host].sha256, swift_plugin: pins.swift_plugin[host].sha256, grpc_plugin: pins.grpc_plugin[host].sha256 })
    || !equal(receipt.swift_runtime_sha256 ?? {}, pins.swift_runtime[host].files)) throw new Error("generation tools differ from admitted pins");
  const metadata = ["Package.swift", "LICENSE", "NOTICE", "authority/rust-authority.json"];
  const names = new Set(), folded = new Set();
  for (const name of receipt.outputs) {
    if (!canonical(name) || folded.has(name.toLowerCase()) || (!metadata.includes(name) && !/^Sources\/AcyclicTransport\/.+\.(pb|grpc)\.swift$/.test(name))) throw new Error("unsafe, duplicate or unexpected Swift package output");
    names.add(name); folded.add(name.toLowerCase());
  }
  if (!equal(receipt.output_sha256, Object.fromEntries([...names].map(name => [name, receipt.output_sha256[name]])))
    || [...names].some(name => !/^[a-f0-9]{64}$/.test(receipt.output_sha256[name]))) throw new Error("package digest inventory differs");
  if (metadata.some(name => !names.has(name))) throw new Error("package metadata missing");
  for (const source of targets) for (const suffix of [".pb.swift", ".grpc.swift"]) {
    if (!names.has("Sources/AcyclicTransport/" + source.slice(0, -6) + suffix)) throw new Error("tested family bindings missing");
  }
  const { compressed, expanded } = readBoundedGzip(args.package, 16 * 1024 * 1024, 128 * 1024 * 1024);
  if (sha256(compressed) !== args.sha256) throw new Error("archive digest differs");
  const payload = new Map();
  for (const entry of tarEntries(expanded)) {
    if (entry.type !== "0" || !canonical(entry.path) || payload.has(entry.path) || !names.has(entry.path)) throw new Error("unsafe, duplicate or unexpected archive member");
    if (sha256(entry.body) !== receipt.output_sha256[entry.path]) throw new Error("archive payload digest differs");
    payload.set(entry.path, entry.body);
  }
  if (payload.size !== names.size) throw new Error("archive payload inventory differs");
  if (sha256(payload.get("Package.swift")) !== sha256(readFileSync(join(directory, "../templates/package/Package.swift")))
    || !payload.get("authority/rust-authority.json").equals(approved.bytes)) throw new Error("package metadata differs from maintained inputs");
  return { payload, receipt, receiptBytes, approved, archiveBytes: compressed };
}
