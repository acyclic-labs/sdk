import { lstatSync, readFileSync, readdirSync, realpathSync } from "node:fs";
import { isAbsolute, join, posix } from "node:path";
import { canonical, sha256, within } from "../../../shared/authority.mjs";

export function files(root, prefix = "") {
  const directory = join(root, prefix), kind = lstatSync(directory);
  if (!kind.isDirectory() || kind.isSymbolicLink()) throw new Error("inventory directory kind differs");
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const name = posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error("inventory contains a link");
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("inventory contains a non-file");
    return [name];
  }).sort();
}

function verifyFiles(root, expected) {
  if (!expected || Object.keys(expected).some(name => !canonical(name) || !/^[a-f0-9]{64}$/.test(expected[name]))) throw new Error("unsafe inventory member");
  if (new Set(Object.keys(expected).map(name => name.toLowerCase())).size !== Object.keys(expected).length) throw new Error("inventory has case aliases");
  if (JSON.stringify(files(root)) !== JSON.stringify(Object.keys(expected).sort())) throw new Error("file inventory differs");
  for (const [name, digest] of Object.entries(expected)) {
    if (!within(root, realpathSync(join(root, name))) || sha256(readFileSync(join(root, name))) !== digest) throw new Error("inventory file differs: " + name);
  }
}

export function verifyRuntime(root, bytes, pins) {
  if (sha256(bytes) !== pins.runtime_inventory_sha256) throw new Error("runtime inventory differs from pin");
  const inventory = JSON.parse(bytes);
  if (inventory.schema !== "acyclic.sdk.cpp-runtime-inventory.v1" || Object.keys(inventory.files ?? {}).length !== pins.runtime_files) throw new Error("runtime inventory metadata differs");
  verifyFiles(root, inventory.files);
  return inventory;
}

export function verifyHost(bytes, pins) {
  if (sha256(bytes) !== pins.host_inventory_sha256) throw new Error("host SDK inventory differs from pin");
  const inventory = JSON.parse(bytes);
  if (inventory.schema !== "acyclic.sdk.cpp-host-inventory.v1" || !Array.isArray(inventory.roots)
    || inventory.roots.length !== pins.host_inventory_roots || inventory.roots.reduce((sum, root) => sum + Object.keys(root.files_sha256 ?? {}).length, 0) !== pins.host_inventory_files) throw new Error("host SDK inventory metadata differs");
  const roots = new Set();
  for (const entry of inventory.roots) {
    if (!isAbsolute(entry.root) || roots.has(entry.root.toLowerCase())) throw new Error("unsafe or duplicate host SDK root");
    roots.add(entry.root.toLowerCase()); verifyFiles(entry.root, entry.files_sha256);
  }
  for (const name of ["include", "lib"]) {
    if (!Array.isArray(inventory.environment?.[name]) || !inventory.environment[name].length) throw new Error("missing host SDK environment");
    for (const root of inventory.environment[name]) {
      // VsDevCmd includes optional absent components. Only existing directories
      // can contribute compiler inputs, and every such directory is admitted.
      let present = false;
      try { present = lstatSync(root).isDirectory(); } catch (error) { if (error.code !== "ENOENT") throw error; }
      if (present && !roots.has(realpathSync(root).toLowerCase())) throw new Error("unadmitted compiler search directory");
    }
  }
  const tools = ["cl.exe", "link.exe", "lib.exe", "rc.exe", "mt.exe", "cmake.exe", "ninja.exe"];
  if (JSON.stringify(Object.keys(inventory.tools ?? {}).sort()) !== JSON.stringify(tools.sort())) throw new Error("host tool inventory differs");
  for (const name of tools) {
    const pin = inventory.tools[name];
    if (!isAbsolute(pin.path) || !/^[a-f0-9]{64}$/.test(pin.sha256) || !inventory.roots.some(root => within(root.root, pin.path))
      || sha256(readFileSync(pin.path)) !== pin.sha256) throw new Error("host build tool differs: " + name);
  }
  return inventory;
}

export function verifyCompiledSources(build, expected) {
  const entries = JSON.parse(readFileSync(join(build, "compile_commands.json")));
  if (!Array.isArray(entries)) throw new Error("compiled source inventory malformed");
  const normalize = name => realpathSync(name).toLowerCase();
  const actual = entries.map(entry => normalize(isAbsolute(entry.file) ? entry.file : join(entry.directory, entry.file))).sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected.map(normalize).sort())) throw new Error("compiled source inventory differs");
  return entries;
}

export function verifyPrefixes(build, runtime, installed) {
  const cache = readFileSync(join(build, "CMakeCache.txt"), "utf8");
  const normalize = name => name.replaceAll("\\", "/").toLowerCase();
  const expected = { Protobuf: join(runtime, "lib/cmake/protobuf"), absl: join(runtime, "lib/cmake/absl"), gRPC: join(runtime, "lib/cmake/grpc"), utf8_range: join(runtime, "lib/cmake/utf8_range") };
  if (installed) expected.AcyclicTransport = join(installed, "lib/cmake/AcyclicTransport");
  for (const [name, path] of Object.entries(expected)) {
    const value = cache.match(new RegExp("^" + name + "_DIR:PATH=(.*)$", "m"))?.[1].trim();
    if (!value || normalize(value) !== normalize(path)) throw new Error("CMake dependency prefix differs: " + name);
  }
}
