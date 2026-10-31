#!/usr/bin/env node

import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { readTarGzip, collectBody } from "./archive-utils.mjs";

const REPOSITORY_URL = "git+https://github.com/acyclic-labs/sdk.git";

function fail(message) {
  throw new Error(message);
}

export async function validateArchive(archive, name, version, directory) {
  const seen = new Set();
  let manifest;
  let readme;
  let changelog;
  let hasJs = false;
  let hasTypes = false;
  const digest = await readTarGzip(archive, async entry => {
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
    if (entry.type === "5") return;
    if (entry.path === "package/package.json") {
      manifest = await collectBody(entry);
    } else if (entry.path === "package/README.md") {
      readme = await collectBody(entry);
    } else if (entry.path === "package/CHANGELOG.md") {
      changelog = await collectBody(entry);
    } else if (entry.path.startsWith("package/dist/")) {
      hasJs ||= /\.(?:js|mjs|cjs)$/.test(entry.path);
      hasTypes ||= entry.path.endsWith(".d.ts");
    }
  });
  if (!manifest || !readme || readme.toString("utf8").trim() === "" || !changelog || !hasJs || !hasTypes) {
    fail("npm archive lacks its README, changelog, manifest, or compiled public output");
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
  const exportedFiles = value => typeof value === "string"
    ? [value]
    : value && typeof value === "object"
      ? Object.values(value).flatMap(exportedFiles)
      : [];
  for (const target of exportedFiles(metadata.exports)) {
    if (!target.startsWith("./") || !seen.has(`package/${target.slice(2)}`)) {
      fail(`npm archive lacks exported file: ${target}`);
    }
  }
  if (metadata.types && !seen.has(`package/${metadata.types.replace(/^\.\//, "")}`)) {
    fail(`npm archive lacks package types: ${metadata.types}`);
  }
  if (readme.toString("utf8").split(/\r?\n/, 1)[0].trim() !== `# ${name}`) {
    fail("npm README does not match the qualified package");
  }
  const changelogLines = changelog.toString("utf8").split(/\r?\n/);
  if (changelogLines[0].trim() !== `# ${name} changelog`
    || !changelogLines.some(line => line.startsWith(`## ${version} `))) {
    fail("npm changelog does not match the release history");
  }
  return digest.digest;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv.length !== 6) fail("usage: validate-npm-package.mjs ARCHIVE NAME VERSION DIRECTORY");
  console.log(await validateArchive(...process.argv.slice(2)));
}
