#!/usr/bin/env node

/**
 * Bind an installed JavaScript/WASM consumer run to the exact archive and
 * Rust source revision produced by a package qualification lane.
 */
import { createHash } from "node:crypto";
import { gunzipSync } from "node:zlib";
import { existsSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";

const [outputArgument, family, command] = process.argv.slice(2);
if (!outputArgument || !["filesystem", "harness"].includes(family) || !command) {
  throw new Error("usage: write-installed-package-receipt.mjs OUTPUT_DIR filesystem|harness COMMAND");
}

const output = resolve(outputArgument);
const fail = message => { throw new Error(message); };
const requireFile = path => {
  if (!existsSync(path) || !statSync(path).isFile()) fail(`missing package artifact: ${path}`);
  return path;
};
const digest = bytes => `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
const fileDigest = path => digest(readFileSync(path));
const sourceCommit = readFileSync(requireFile(join(output, "SOURCE_COMMIT")), "utf8").trim();
if (!/^[0-9a-f]{40}$/i.test(sourceCommit)) fail(`invalid SOURCE_COMMIT: ${sourceCommit}`);

const packageName = family === "filesystem" ? "acyclic-fs.tgz" : "acyclic-harness.tgz";
const wasmEntry = family === "filesystem"
  ? "package/generated/wasm/acyclic_fs_wasm_bg.wasm"
  : "package/generated/wasm/acyclic_harness_wasm_bg.wasm";
const archive = requireFile(join(output, packageName));
const tarBytes = gunzipSync(readFileSync(archive));
let wasmBytes;
for (let offset = 0; offset + 512 <= tarBytes.length;) {
  const header = tarBytes.subarray(offset, offset + 512);
  if (header.every(byte => byte === 0)) break;
  const name = header.subarray(0, 100).toString("utf8").replace(/\0.*$/, "");
  const sizeText = header.subarray(124, 136).toString("ascii").replace(/\0.*$/, "").trim();
  const size = Number.parseInt(sizeText || "0", 8);
  const bodyStart = offset + 512;
  if (name === wasmEntry) {
    wasmBytes = tarBytes.subarray(bodyStart, bodyStart + size);
    break;
  }
  offset = bodyStart + Math.ceil(size / 512) * 512;
}
if (!wasmBytes?.length) fail(`package archive is missing executable WASM entry ${wasmEntry}`);
const checks = family === "filesystem"
  ? ["npm-install", "archive-only-extraction", "packaged-wasm-load", "memory-workspace-consumer",
    "negative-consumer-entrypoint", "negative-public-method-removal", "negative-wasm-tamper"]
  : ["npm-install", "archive-only-extraction", "packaged-wasm-load", "NativeContracts-consumer", "client-transport-conformance",
    "negative-consumer-entrypoint", "negative-public-method-removal", "negative-wasm-tamper"];
const receipt = {
  schema: "acyclic.sdk.installed-package-qualification.v1",
  status: "passed",
  package: family,
  source_revision: sourceCommit,
  source_revision_kind: "git-oid",
  archive: {
    name: packageName,
    sha256: fileDigest(archive),
    wasm_entry: wasmEntry,
    wasm_sha256: digest(wasmBytes),
  },
  consumer: {
    status: "passed",
    command,
    checks,
  },
};
writeFileSync(join(output, "installed-consumer-receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`);
const checksumPath = join(output, "SHA256SUMS");
if (existsSync(checksumPath)) {
  const checksumName = "installed-consumer-receipt.json";
  const lines = readFileSync(checksumPath, "utf8")
    .split(/\r?\n/)
    .filter(line => line && !line.endsWith(`  ${checksumName}`));
  lines.push(`${fileDigest(join(output, checksumName)).slice("sha256:".length)}  ${checksumName}`);
  writeFileSync(checksumPath, `${lines.join("\n")}\n`);
}
console.log(JSON.stringify({ status: receipt.status, package: family, source_revision: sourceCommit }));
