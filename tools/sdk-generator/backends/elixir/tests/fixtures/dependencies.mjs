import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { gzipSync } from "node:zlib";
import { sha256 } from "../../../../shared/authority.mjs";
import { tar } from "./package.mjs";

export function dependencyFixture(t) {
  const root = mkdtempSync(join(tmpdir(), "elixir-dependencies-"));
  t.after(() => rmSync(root, { recursive: true }));
  const cache = join(root, "cache"), deps = join(root, "deps"), toolHome = join(root, "tools");
  for (const path of [cache, deps, toolHome]) mkdirSync(path);
  const pins = { hex_version: "fixture", dependencies: [] }, entries = {}, inner = "1".repeat(64);
  for (const name of ["googleapis", "grpc", "grpc_core", "jason", "protobuf", "telemetry"]) {
    const metadata = Buffer.from("metadata " + name);
    const payload = [{ name: "mix.exs", bytes: Buffer.from("project " + name) }, { name: "lib/source.ex", bytes: Buffer.from("source " + name) }];
    const members = [{ name: "CHECKSUM", bytes: Buffer.from(inner) }, { name: "VERSION", bytes: Buffer.from("3") },
      { name: "contents.tar.gz", bytes: gzipSync(tar(payload)) }, { name: "metadata.config", bytes: metadata }];
    const bytes = tar(members), pin = { name, version: "1.0.0", archive_sha256: sha256(bytes) };
    pins.dependencies.push(pin); entries[name] = { members, payload, metadata };
    writeFileSync(join(cache, name + "-1.0.0.tar"), bytes);
    for (const entry of [...payload, { name: "hex_metadata.config", bytes: metadata }, { name: ".hex", bytes: Buffer.from("installation metadata") }]) {
      const target = join(deps, name, entry.name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, entry.bytes);
    }
  }
  const lock = Buffer.from("%{\n" + pins.dependencies.map(pin => `  "${pin.name}": {:hex, :${pin.name}, "${pin.version}", "${inner}", [:mix], [], "hexpm", "${pin.archive_sha256}"},`).join("\n") + "\n}\n");
  pins.lock_sha256 = sha256(lock);
  const tools = { ".mix/archives/hex-fixture/ebin/hex.beam": Buffer.from("fixture tool"), ".mix/elixir/1-20-otp-29/rebar3": Buffer.from("fixture rebar"), ".hex/cache.ets": Buffer.from("fixture registry") };
  for (const [name, bytes] of Object.entries(tools)) { const target = join(toolHome, name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, bytes); }
  const toolIndex = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.elixir-qualification-tools.v1", hex_version: "fixture", files_sha256: Object.fromEntries(Object.entries(tools).map(([name, bytes]) => [name, sha256(bytes)])) }));
  pins.tool_inventory_sha256 = sha256(toolIndex); pins.tool_files = Object.keys(tools).length;
  return { root, cache, deps, toolHome, pins, lock, toolIndex, entries, tools,
    replaceArchive(name, members) { const bytes = tar(members); writeFileSync(join(cache, name + "-1.0.0.tar"), bytes); pins.dependencies.find(pin => pin.name === name).archive_sha256 = sha256(bytes); } };
}
