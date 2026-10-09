import { lstatSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { sha256, within } from "../../../shared/authority.mjs";
import { tree } from "./provenance.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = JSON.parse(readFileSync(join(directory, "../toolchains/qualification.json")));
const runtime = JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"))).swift_runtime["linux-x64"];
const sort = entries => entries.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);

// Reproduce the external SDK inventory from a prepared extraction. The pinned
// inventory digest binds all file bytes and link targets to the admitted SDK.
export function inventorySDK(args, { toolchain = pinned, swiftVersion = runtime.version } = {}) {
  const root = realpathSync(args["swift-home"]);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("SDK inventory output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [root, realpathSync(join(directory, "../../.."))]) {
    if (within(input, output) || within(output, input)) throw new Error("SDK inventory output overlaps protected input");
  }
  const contents = tree(root);
  if (contents.files.length !== toolchain.sdk_files || Object.keys(contents.links).length !== toolchain.sdk_links) throw new Error("SDK inventory size differs from pin");
  const inventory = {
    archive_sha256: toolchain.sdk_archive_sha256,
    platform: "Ubuntu 22.04 x86_64", swift_version: swiftVersion,
    files: Object.fromEntries(contents.files.map(name => [name, sha256(readFileSync(join(root, name)))])),
    links: Object.fromEntries(sort(Object.entries(contents.links))),
  };
  // Keep the external artifact's canonical ASCII JSON encoding reproducible.
  const encoded = JSON.stringify(inventory, null, 2).replace(/[\u007f-\uffff]/g, value => "\\u" + value.charCodeAt(0).toString(16).padStart(4, "0")) + "\n";
  if (sha256(encoded) !== toolchain.sdk_inventory_sha256) throw new Error("SDK file bytes or links differ from admitted inventory");
  writeFileSync(output, encoded, { flag: "wx" });
  return { sha256: sha256(encoded), files: contents.files.length, links: Object.keys(contents.links).length };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({ options: { "swift-home": { type: "string" }, output: { type: "string" } } });
  if (!values["swift-home"] || !values.output) throw new Error("--swift-home and --output are required");
  console.log(JSON.stringify(inventorySDK(values), null, 2));
}
