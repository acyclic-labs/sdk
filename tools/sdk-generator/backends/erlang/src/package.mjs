import { readFileSync, statSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { readDeclaredTarGzip } from "../../../../../scripts/archive-utils.mjs";
import { equalEntries, loadAuthority, sha256, verifyPackageOutputs } from "../../../shared/authority.mjs";
import { packageTemplates, pins, producerSources } from "./generate.mjs";

const directory = dirname(fileURLToPath(import.meta.url));

function snake(name) {
  for (const expression of [/(.)([A-Z][a-z]+)/g, /(.)([0-9]+)/g, /([a-z0-9])([A-Z])/g]) name = name.replace(expression, "$1_$2");
  return name.replaceAll(".", "_").replaceAll("__", "_").toLowerCase();
}

export function expectedModules(approved) {
  const names = [];
  for (const family of approved.manifest.families) {
    const source = approved.inputs.get(family.source).toString("utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/[^\n]*/g, "");
    const packages = [...source.matchAll(/^\s*package\s+([A-Za-z_][\w.]*)\s*;/gm)];
    const services = [...source.matchAll(/^\s*service\s+([A-Za-z_]\w*)\s*\{/gm)];
    if (packages.length !== 1 || services.length !== 1 || !approved.descriptors.has(family.source)) throw new Error("unsupported or descriptor-free Rust service schema");
    const prefix = snake(packages[0][1] + "." + services[0][1]);
    names.push("src/" + basename(family.source).slice(0, -6) + "_pb.erl", "src/" + prefix + "_client.erl", "src/" + prefix + "_bhvr.erl");
  }
  if (new Set(names.map(name => name.toLowerCase())).size !== names.length) throw new Error("generated Erlang module names collide");
  return names.sort();
}

// Admission checks archive bytes; installed compilation and RPC are separate.
export function readPackage(args) {
  if (!/^[a-f0-9]{64}$/.test(args.sha256)) throw new Error("archive SHA256 is required");
  if (statSync(args.receipt).size > 1024 * 1024) throw new Error("generation receipt exceeds size bound");
  const receiptBytes = readFileSync(args.receipt), receipt = JSON.parse(receiptBytes), approved = loadAuthority(args.authority);
  if (receipt.schema !== "acyclic.sdk.erlang-producer-receipt.v1" || receipt.authority !== "rust" || receipt.target !== "erlang"
    || !Array.isArray(receipt.outputs) || !receipt.input_sha256 || !receipt.output_sha256) throw new Error("unsupported Erlang generation receipt");
  if (receipt.source_revision !== approved.manifest.source_revision || receipt.authority_manifest_sha256 !== sha256(approved.bytes)
    || !equalEntries(receipt.input_sha256, Object.fromEntries([...approved.inputs].map(([name, bytes]) => [name, sha256(bytes)])))) throw new Error("package authority differs");
  const sources = Object.fromEntries(producerSources.map(name => [name, sha256(readFileSync(join(directory, name)))]));
  if (!equalEntries(receipt.source_sha256 ?? {}, sources) || receipt.toolchain_sha256 !== sha256(JSON.stringify(pins))) throw new Error("producer source or pins differ");
  const runtime = pins.runtime["linux-x64"];
  if (receipt.otp_version !== runtime.otp_version || receipt.runtime_inventory_sha256 !== runtime.inventory_sha256
    || receipt.gpb_version !== pins.gpb_version || receipt.grpcbox_plugin_version !== pins.grpcbox_plugin_version
    || receipt.rebar_version !== pins.rebar_version || receipt.generator_inventory_sha256 !== pins.generator_inventory_sha256) throw new Error("generation tool admission differs");
  const templates = new Map([...packageTemplates].map(([name, template]) => [name, readFileSync(join(directory, "../templates/package", template))]));
  if (!equalEntries(receipt.template_sha256 ?? {}, Object.fromEntries([...templates].map(([name, bytes]) => [name, sha256(bytes)])))) throw new Error("package templates differ");
  const required = new Set(["LICENSE", "NOTICE", ...templates.keys(), "authority/rust-authority.json",
    ...[...approved.inputs.keys()].map(name => "authority/" + name), ...expectedModules(approved)]);
  verifyPackageOutputs(receipt.outputs, receipt.output_sha256, required, "unsafe, duplicate or unexpected package output");
  const { compressed, payload } = readDeclaredTarGzip(args.package, args.sha256, receipt.output_sha256, 64 * 1024 * 1024);
  for (const [name, bytes] of templates) if (!payload.get(name).equals(bytes)) throw new Error("package metadata differs from maintained template");
  if (!payload.get("authority/rust-authority.json").equals(approved.bytes)) throw new Error("packaged authority manifest differs");
  for (const [name, bytes] of approved.inputs) if (!payload.get("authority/" + name).equals(bytes)) throw new Error("packaged Rust input differs");
  return { payload, receipt, receiptBytes, approved, archiveBytes: compressed };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["package", "sha256", "receipt", "authority"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("all archive admission arguments are required");
  const admitted = readPackage(values);
  console.log(JSON.stringify({ scope: "Erlang archive admission only", archive_sha256: sha256(admitted.archiveBytes), source_revision: admitted.receipt.source_revision, payload_files: admitted.payload.size }, null, 2));
}
