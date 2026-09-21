#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function fail(message) {
  throw new Error(message);
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", ...options });
  if (result.error) throw result.error;
  return result;
}

function integrity(path) {
  return `sha512-${createHash("sha512").update(readFileSync(path)).digest("base64")}`;
}

function publishedIntegrity(name, version) {
  const result = run("npm", ["view", `${name}@${version}`, "dist.integrity", "--json"]);
  if (result.status === 0) return JSON.parse(result.stdout);
  if (/E404|404 Not Found/.test(`${result.stderr}\n${result.stdout}`)) return null;
  fail(`could not inspect ${name}@${version}: ${result.stderr || result.stdout}`);
}

const [artifactArgument, sourceSha, releaseVersion] = process.argv.slice(2);
if (!artifactArgument || !/^[0-9a-f]{40}$/.test(sourceSha ?? "") || !/^\d+\.\d+\.\d+(?:[+-][0-9A-Za-z.-]+)?$/.test(releaseVersion ?? "")) {
  fail("usage: publish-npm-packages.mjs ARTIFACT_DIR SOURCE_SHA VERSION");
}

const artifactDirectory = resolve(artifactArgument);
const receipt = join(artifactDirectory, "QUALIFICATION.json");
const packages = JSON.parse(readFileSync(join(root, "release", "npm-packages.json"), "utf8"));

for (const item of packages) {
  if (!item || !["typescript", "plugin"].includes(item.source)) fail("npm publication source is invalid");
  const manifestPath = item.source === "typescript"
    ? join(root, "typescript", "packages", item.directory, "package.json")
    : join(root, "plugin", "package.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  if (manifest.name !== item.name || manifest.version !== releaseVersion || manifest.private !== false) {
    fail(`release manifest mismatch for ${item.directory}`);
  }
  const asset = `acyclic-labs-${item.slug}-${releaseVersion}.tgz`;
  const archive = join(artifactDirectory, asset);
  const verification = item.source === "typescript"
    ? run("node", ["scripts/typescript-qualification.mjs", "verify", receipt, sourceSha, asset, archive], { stdio: "inherit" })
    : run("node", ["scripts/plugin-qualification.mjs", "verify", "plugin/QUALIFICATION.json", sourceSha, archive, releaseVersion], { stdio: "inherit" });
  if (verification.status !== 0) fail(`qualified archive verification failed for ${item.name}`);

  const expectedIntegrity = integrity(archive);
  const observedIntegrity = publishedIntegrity(item.name, releaseVersion);
  if (observedIntegrity !== null) {
    if (observedIntegrity !== expectedIntegrity) fail(`${item.name}@${releaseVersion} exists with different bytes`);
    console.log(`Already published exact archive: ${item.name}@${releaseVersion}`);
    continue;
  }

  const publication = run("npm", ["publish", archive, "--access", "public", "--tag", "latest", "--provenance"], { stdio: "inherit" });
  if (publication.status !== 0) fail(`npm publication failed for ${item.name}@${releaseVersion}`);
  const registryIntegrity = publishedIntegrity(item.name, releaseVersion);
  if (registryIntegrity !== expectedIntegrity) fail(`registry bytes differ for ${item.name}@${releaseVersion}`);
  console.log(`Published and verified: ${item.name}@${releaseVersion}`);
}
