import { createHash } from "node:crypto";
import { lstatSync, readFileSync, readdirSync, readlinkSync, realpathSync } from "node:fs";
import { dirname, join, posix, resolve } from "node:path";
import { canonical, readInput, sha256, within } from "../../../shared/authority.mjs";

export function tree(root, { gitMetadata = false } = {}) {
  const files = [], links = {};
  function walk(prefix = "") {
    for (const entry of readdirSync(join(root, prefix), { withFileTypes: true })) {
      if (gitMetadata && !prefix && entry.name === ".git") continue;
      const name = posix.join(prefix, entry.name), path = join(root, name);
      if (!canonical(name)) throw new Error("unsafe input inventory path");
      if (entry.isSymbolicLink()) {
        if (!within(root, realpathSync(path))) throw new Error("input link escapes root");
        links[name] = readlinkSync(path);
        if (lstatSync(realpathSync(path)).isFile()) files.push(name);
      } else if (entry.isDirectory()) walk(name);
      else if (entry.isFile()) files.push(name);
      else throw new Error("input inventory contains a non-file");
    }
  }
  walk();
  return { files: files.sort(), links };
}
const equal = (left, right) => JSON.stringify(Object.entries(left).sort()) === JSON.stringify(Object.entries(right).sort());

export function verifyRuntime(root, inventoryBytes, pin) {
  if (sha256(inventoryBytes) !== pin.sdk_inventory_sha256) throw new Error("SDK inventory differs from pin");
  const inventory = JSON.parse(inventoryBytes);
  if (inventory.archive_sha256 !== pin.sdk_archive_sha256 || Object.keys(inventory.files ?? {}).length !== pin.sdk_files
    || Object.keys(inventory.links ?? {}).length !== pin.sdk_links) throw new Error("SDK inventory metadata differs");
  const actual = tree(root);
  if (JSON.stringify(actual.files) !== JSON.stringify(Object.keys(inventory.files).sort()) || !equal(actual.links, inventory.links)) throw new Error("SDK file or link inventory differs");
  for (const [name, digest] of Object.entries(inventory.files)) readInput(root, name, digest);
  return inventory;
}

// The git function must execute the admitted Git binary with replacement objects
// and inherited Git configuration disabled by the qualifier.
export function verifySource(root, pin, git, { cloneInput = false } = {}) {
  if (git(root, ["rev-parse", "HEAD"]).trim() !== pin.revision) throw new Error("dependency revision differs: " + pin.name);
  const hashes = {}, names = new Set();
  for (const record of git(root, ["ls-tree", "-r", "-z", pin.revision]).split("\0").filter(Boolean)) {
    const match = /^(100644|100755|120000) blob ([a-f0-9]{40})\t([\s\S]+)$/.exec(record);
    if (!match || !canonical(match[3]) || names.has(match[3])) throw new Error("unsafe dependency Git tree");
    const [, mode, object, name] = match, path = join(root, name), stat = lstatSync(path);
    if (!within(root, realpathSync(path))) throw new Error("dependency file containment differs");
    let bytes;
    if (mode === "120000") {
      if (stat.isSymbolicLink()) bytes = Buffer.from(readlinkSync(path));
      else if (cloneInput && stat.isFile()) {
        // Windows source checkouts may represent Git symlinks as their target
        // text. Only the local clone input permits this representation; owned
        // mirrors and compiled checkouts must contain actual symlinks.
        bytes = readFileSync(path);
        const target = resolve(dirname(path), bytes.toString());
        if (!within(root, target) || !within(root, realpathSync(target))) throw new Error("dependency link text escapes source root");
      } else throw new Error("dependency file kind differs");
    } else {
      if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("dependency file kind differs");
      bytes = readFileSync(path);
    }
    const blob = createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
    if (blob !== object) throw new Error("dependency source differs from pinned Git object: " + name);
    hashes[name] = sha256(bytes); names.add(name);
  }
  if (!names.size) throw new Error("dependency source tree is empty");
  if (!cloneInput) {
    const actual = tree(root, { gitMetadata: true });
    const present = [...new Set([...actual.files, ...Object.keys(actual.links)])].sort();
    if (JSON.stringify(present) !== JSON.stringify([...names].sort())) throw new Error("dependency source inventory differs");
  }
  return { ...pin, files_sha256: hashes };
}
