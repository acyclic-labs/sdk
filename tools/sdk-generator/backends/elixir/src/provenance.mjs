import { lstatSync, readFileSync, readdirSync } from "node:fs";
import { join, posix } from "node:path";
import { gunzipSync } from "node:zlib";
import { tarEntries } from "../../../../../scripts/archive-utils.mjs";
import { canonical, sha256 } from "../../../shared/authority.mjs";

export function files(root, prefix = "") {
  const current = lstatSync(join(root, prefix));
  if (!current.isDirectory() || current.isSymbolicLink()) throw new Error("source directory kind differs");
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error("source inventory contains a link: " + name);
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("source inventory contains a non-file");
    return [name];
  }).sort();
}

export function readDependencies(cache, pins) {
  const admitted = new Map();
  for (const pin of pins.dependencies) {
    if (!/^[a-z_]+$/.test(pin.name) || !/^[0-9.]+$/.test(pin.version) || admitted.has(pin.name)) throw new Error("unsafe or duplicate dependency coordinate");
    const filename = `${pin.name}-${pin.version}.tar`, bytes = readFileSync(join(cache, filename));
    if (bytes.length > 16 * 1024 * 1024 || sha256(bytes) !== pin.archive_sha256) throw new Error("dependency archive differs: " + filename);
    const outer = tarEntries(bytes), names = outer.map(entry => entry.path).sort();
    if (JSON.stringify(names) !== JSON.stringify(["CHECKSUM", "VERSION", "contents.tar.gz", "metadata.config"])
      || outer.some(entry => entry.type !== "0")) throw new Error("Hex envelope differs");
    const get = name => outer.find(entry => entry.path === name).body;
    const expanded = gunzipSync(get("contents.tar.gz"), { maxOutputLength: 32 * 1024 * 1024 });
    const payload = new Map(), folded = new Set();
    for (const entry of tarEntries(expanded)) {
      if (entry.type !== "0" || !canonical(entry.path) || folded.has(entry.path.toLowerCase())) throw new Error("unsafe package source inventory");
      payload.set(entry.path, entry.body); folded.add(entry.path.toLowerCase());
    }
    admitted.set(pin.name, { pin, filename, bytes, payload, metadata: get("metadata.config"), innerChecksum: get("CHECKSUM").toString().trim().toLowerCase() });
  }
  return admitted;
}

export function verifyLock(bytes, dependencies, pins) {
  if (sha256(bytes) !== pins.lock_sha256) throw new Error("consumer lock differs from pin");
  const entries = [...bytes.toString().matchAll(/^\s*"([a-z_]+)": \{:hex, :[a-z_]+, "([0-9.]+)", "([a-f0-9]{64})", [^\n]+"hexpm", "([a-f0-9]{64})"\},?$/gm)];
  if (entries.length !== dependencies.size || new Set(entries.map(entry => entry[1])).size !== dependencies.size) throw new Error("dependency lock inventory differs");
  for (const [, name, version, inner, outer] of entries) {
    const admitted = dependencies.get(name);
    if (!admitted || admitted.pin.version !== version || admitted.pin.archive_sha256 !== outer || admitted.innerChecksum !== inner) throw new Error("locked package coordinate differs");
  }
}

export function verifyInstalledDependencies(root, dependencies) {
  if (JSON.stringify(readdirSync(root).sort()) !== JSON.stringify([...dependencies.keys()].sort())) throw new Error("compiled dependency directory inventory differs");
  const result = {};
  for (const [name, admitted] of dependencies) {
    const source = join(root, name), metadata = [".hex", ".fetch", "hex_metadata.config"];
    if (name === "telemetry") metadata.push("_build/prod/lib/.rebar3/rebar_compiler_erl/source.dag");
    const inventory = files(source), expected = new Set(admitted.payload.keys());
    if (inventory.some(file => !expected.has(file) && !metadata.includes(file)) || [...expected].some(file => !inventory.includes(file))) throw new Error("unexpected compiled source inventory: " + name);
    for (const [file, bytes] of admitted.payload) if (!readFileSync(join(source, file)).equals(bytes)) throw new Error("compiled dependency source differs: " + name + "/" + file);
    if (inventory.includes("hex_metadata.config") && !readFileSync(join(source, "hex_metadata.config")).equals(admitted.metadata)) throw new Error("installed Hex metadata differs");
    result[name] = { version: admitted.pin.version, archive_sha256: admitted.pin.archive_sha256,
      files_sha256: Object.fromEntries(inventory.map(file => [file, sha256(readFileSync(join(source, file)))])) };
  }
  return result;
}

export function readTools(home, inventoryBytes, pins) {
  if (sha256(inventoryBytes) !== pins.tool_inventory_sha256) throw new Error("qualification tool inventory differs from pin");
  const inventory = JSON.parse(inventoryBytes);
  if (inventory.schema !== "acyclic.sdk.elixir-qualification-tools.v1" || inventory.hex_version !== pins.hex_version
    || Object.keys(inventory.files_sha256 ?? {}).length !== pins.tool_files) throw new Error("qualification tool inventory metadata differs");
  const payload = new Map();
  for (const [name, digest] of Object.entries(inventory.files_sha256)) {
    if (!canonical(name) || !(name.startsWith(".mix/archives/") || name === ".mix/elixir/1-20-otp-29/rebar3" || name === ".hex/cache.ets")) throw new Error("unexpected qualification tool input");
    const target = join(home, name);
    if (!lstatSync(target).isFile() || lstatSync(target).isSymbolicLink()) throw new Error("qualification tool kind differs");
    const bytes = readFileSync(target); if (sha256(bytes) !== digest) throw new Error("qualification tool payload differs: " + name);
    payload.set(name, bytes);
  }
  if (JSON.stringify(files(join(home, ".mix/archives")).map(name => ".mix/archives/" + name)) !== JSON.stringify([...payload.keys()].filter(name => name.startsWith(".mix/archives/")).sort())) throw new Error("Hex installation inventory differs");
  return payload;
}
