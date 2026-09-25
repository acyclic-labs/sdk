#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";

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
  const result = run("npm", ["view", `${name}@${version}`, "dist.integrity", "--json", "--prefer-online"]);
  if (result.status === 0) return JSON.parse(result.stdout);
  if (/E404|404 Not Found/.test(`${result.stderr}\n${result.stdout}`)) return null;
  fail(`could not inspect ${name}@${version}: ${result.stderr || result.stdout}`);
}

function publishedLatest(name) {
  const result = run("npm", ["view", name, "dist-tags.latest", "--json", "--prefer-online"]);
  if (/E404|404 Not Found/.test(`${result.stderr}\n${result.stdout}`)) return null;
  if (result.status !== 0) fail(`could not inspect latest for ${name}: ${result.stderr || result.stdout}`);
  return JSON.parse(result.stdout);
}

async function waitForPublishedExact(name, version, expectedIntegrity) {
  const deadline = Date.now() + 300_000;
  let observedIntegrity;
  let latest;
  while (true) {
    observedIntegrity = publishedIntegrity(name, version);
    if (observedIntegrity !== null && observedIntegrity !== expectedIntegrity) {
      fail(`${name}@${version} exists with different bytes`);
    }
    if (observedIntegrity === expectedIntegrity) {
      latest = publishedLatest(name);
      if (latest === version) return;
    }
    if (Date.now() >= deadline) {
      fail(`${name}@${version} did not become visible with exact bytes and latest tag; observed integrity: ${observedIntegrity ?? "not found"}, latest: ${latest ?? "not found"}`);
    }
    await delay(5_000);
  }
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
    await waitForPublishedExact(item.name, releaseVersion, expectedIntegrity);
    continue;
  }

  const publication = run("npm", ["publish", archive, "--access", "public", "--tag", "latest", "--provenance"], { stdio: "inherit" });
  if (publication.status !== 0) fail(`npm publication failed for ${item.name}@${releaseVersion}`);
  await waitForPublishedExact(item.name, releaseVersion, expectedIntegrity);
  console.log(`Published and verified: ${item.name}@${releaseVersion}`);
}
