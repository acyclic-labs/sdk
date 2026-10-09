import { lstatSync, readFileSync, readdirSync, readlinkSync, realpathSync } from "node:fs";
import { join, posix } from "node:path";
import { canonical, sha256, within } from "../../../shared/authority.mjs";

export function verifyRuntime(root, inventoryBytes, pin) {
  if (sha256(inventoryBytes) !== pin.inventory_sha256) throw new Error("runtime inventory differs from pin");
  const inventory = JSON.parse(inventoryBytes);
  if (inventory.otp_version !== pin.otp_version || inventory.elixir_version !== pin.elixir_version
    || inventory.otp_archive_sha256 !== pin.otp_archive_sha256 || inventory.elixir_archive_sha256 !== pin.elixir_archive_sha256
    || Object.keys(inventory.files_sha256 ?? {}).length !== pin.files || Object.keys(inventory.links ?? {}).length !== pin.links) {
    throw new Error("runtime inventory metadata differs");
  }
  const files = [], links = {};
  function walk(prefix) {
    const directory = join(root, prefix);
    if (!lstatSync(directory).isDirectory() || lstatSync(directory).isSymbolicLink()) throw new Error("runtime directory kind differs");
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const name = posix.join(prefix, entry.name), target = join(root, name);
      if (!canonical(name) || !within(root, realpathSync(target))) throw new Error("runtime path escapes admitted root");
      if (entry.isSymbolicLink()) links[name] = readlinkSync(target);
      else if (entry.isDirectory()) walk(name);
      else if (entry.isFile()) files.push(name);
      else throw new Error("runtime contains a non-file");
    }
  }
  if (!Array.isArray(pin.roots) || pin.roots.length !== 2 || pin.roots.some(name => !canonical(name) || name.includes("/"))
    || new Set(pin.roots).size !== 2) throw new Error("invalid admitted runtime roots");
  for (const name of pin.roots) walk(name);
  const ordered = value => Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)));
  if (JSON.stringify(files.sort()) !== JSON.stringify(Object.keys(inventory.files_sha256).sort())
    || JSON.stringify(ordered(links)) !== JSON.stringify(ordered(inventory.links))) throw new Error("runtime file or link inventory differs");
  for (const [name, digest] of Object.entries(inventory.files_sha256)) {
    if (sha256(readFileSync(join(root, name))) !== digest) throw new Error("runtime file differs: " + name);
  }
  return inventory;
}
