import { lstatSync, readFileSync, readdirSync, readlinkSync, realpathSync } from "node:fs";
import { isAbsolute, join, posix, resolve } from "node:path";
import { canonical, sha256, within } from "../../../shared/authority.mjs";

export function verifyTree(root, inventory, roots = [""]) {
  const expected = inventory.files_sha256, expectedLinks = inventory.links ?? {};
  if (!expected || !Object.keys(expected).length || !Array.isArray(roots) || !roots.length
    || new Set(roots).size !== roots.length || roots.some(name => name !== "" && (!canonical(name) || name.includes("/")))) throw new Error("invalid inventory roots");
  for (const [name, digest] of Object.entries(expected)) {
    if (!canonical(name) || !/^[a-f0-9]{64}$/.test(digest) || Object.hasOwn(expectedLinks, name)) throw new Error("invalid inventory file");
  }
  for (const [name, target] of Object.entries(expectedLinks)) {
    if (!canonical(name) || typeof target !== "string" || isAbsolute(target) || target.includes("\\") || target.includes(":" )
      || !within(root, resolve(root, posix.dirname(name), target))) throw new Error("invalid inventory link");
  }
  const files = [], links = {};
  function walk(prefix) {
    const directory = join(root, prefix), stat = lstatSync(directory);
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("inventory directory kind differs");
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const name = posix.join(prefix, entry.name), target = join(root, name);
      if (!canonical(name) || !within(root, realpathSync(target))) throw new Error("inventory path escapes root");
      if (entry.isSymbolicLink()) links[name] = readlinkSync(target);
      else if (entry.isDirectory()) walk(name);
      else if (entry.isFile()) files.push(name);
      else throw new Error("inventory contains a non-file");
    }
  }
  for (const prefix of roots) walk(prefix);
  const ordered = value => Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)));
  if (JSON.stringify(files.sort()) !== JSON.stringify(Object.keys(expected).sort())
    || JSON.stringify(ordered(links)) !== JSON.stringify(ordered(expectedLinks))) throw new Error("file or link inventory differs");
  for (const [name, digest] of Object.entries(expected)) if (sha256(readFileSync(join(root, name))) !== digest) throw new Error("inventory file differs: " + name);
  return inventory;
}

export function verifyRuntime(root, bytes, pin) {
  if (sha256(bytes) !== pin.inventory_sha256) throw new Error("runtime inventory differs from pin");
  const index = JSON.parse(bytes);
  if (index.schema !== "acyclic.sdk.erlang-runtime-inventory.v1" || index.otp_version !== pin.otp_version
    || index.otp_archive_sha256 !== pin.otp_archive_sha256 || Object.keys(index.files_sha256 ?? {}).length !== pin.files
    || Object.keys(index.links ?? {}).length !== pin.links || pin.roots?.length !== 1
    || pin.roots[0] !== "OTP-" + pin.otp_version) throw new Error("runtime inventory metadata differs");
  return verifyTree(root, index, pin.roots);
}

export function verifyTools(root, bytes, pin) {
  if (sha256(bytes) !== pin.generator_inventory_sha256) throw new Error("generator inventory differs from pin");
  const index = JSON.parse(bytes);
  if (index.schema !== "acyclic.sdk.erlang-generation-tools.v1" || index.gpb_version !== pin.gpb_version
    || index.grpcbox_plugin_version !== pin.grpcbox_plugin_version || index.rebar_version !== pin.rebar_version
    || Object.keys(index.files_sha256 ?? {}).length !== pin.generator_files) throw new Error("generator inventory metadata differs");
  return verifyTree(root, index);
}
