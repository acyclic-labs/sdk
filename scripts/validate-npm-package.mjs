#!/usr/bin/env node

import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { readBoundedGzip, sha256, tarEntries } from "./archive-utils.mjs";

const MAX_ARCHIVE_BYTES = 104_857_600;
const MAX_EXPANDED_BYTES = 536_870_912;
const REPOSITORY_URL = "git+https://github.com/acyclic-labs/sdk.git";

function fail(message) {
  throw new Error(message);
}

export function validateArchive(archive, name, version, directory) {
  const { compressed, expanded } = readBoundedGzip(archive, MAX_ARCHIVE_BYTES, MAX_EXPANDED_BYTES);

  const seen = new Set();
  let manifest;
  let readme;
  let hasJs = false;
  let hasTypes = false;
  for (const entry of tarEntries(expanded)) {
    const parts = entry.path.split("/");
    if (
      seen.has(entry.path)
      || entry.path.startsWith("/")
      || parts.includes("..")
      || parts.length === 0
      || parts[0] !== "package"
      || !["0", "\0", "5"].includes(entry.type)
    ) fail("npm archive contains an unsafe path");
    seen.add(entry.path);
    if (entry.type === "5") continue;
    if (entry.path === "package/package.json") {
      if (entry.body.length > 1_048_576) fail("npm manifest exceeds its size bound");
      manifest = entry.body;
    } else if (entry.path === "package/README.md") {
      if (entry.body.length > 262_144) fail("npm README exceeds its size bound");
      readme = entry.body;
    } else if (entry.path.startsWith("package/dist/")) {
      hasJs ||= /\.(?:js|mjs|cjs)$/.test(entry.path);
      hasTypes ||= entry.path.endsWith(".d.ts");
    }
  }
  if (!manifest || !readme || readme.toString("utf8").trim() === "" || !hasJs || !hasTypes) {
    fail("npm archive lacks its README, manifest, or compiled public output");
  }
  const metadata = JSON.parse(manifest.toString("utf8"));
  const expectedRepository = { type: "git", url: REPOSITORY_URL, directory };
  if (
    metadata.name !== name
    || metadata.version !== version
    || metadata.private !== false
    || metadata.license !== "Apache-2.0"
    || JSON.stringify(metadata.repository) !== JSON.stringify(expectedRepository)
  ) fail("npm manifest does not match the qualified package");
  if (readme.toString("utf8").split(/\r?\n/, 1)[0].trim() !== `# ${name}`) {
    fail("npm README does not match the qualified package");
  }
  return sha256(compressed);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv.length !== 6) fail("usage: validate-npm-package.mjs ARCHIVE NAME VERSION DIRECTORY");
  console.log(validateArchive(...process.argv.slice(2)));
}
