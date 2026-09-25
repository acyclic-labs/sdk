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

export function parseNpmView(result, label) {
  if (result.status === 0) return result.stdout.trim() ? JSON.parse(result.stdout) : null;
  if (/E404|404 Not Found/.test(`${result.stderr}\n${result.stdout}`)) return null;
  fail(`could not inspect ${label}: ${result.stderr || result.stdout}`);
}

function publishedIntegrity(name, version) {
  return parseNpmView(
    run("npm", ["view", `${name}@${version}`, "dist.integrity", "--json", "--prefer-online"]),
    `${name}@${version}`,
  );
}

function publishedLatest(name) {
  return parseNpmView(
    run("npm", ["view", name, "dist-tags.latest", "--json", "--prefer-online"]),
    `latest for ${name}`,
  );
}

export async function waitForPublishedExact(name, version, expectedIntegrity, probes = {}) {
  const readIntegrity = probes.readIntegrity ?? publishedIntegrity;
  const readLatest = probes.readLatest ?? publishedLatest;
  const pause = probes.pause ?? delay;
  const now = probes.now ?? Date.now;
  const deadline = now() + (probes.timeoutMs ?? 300_000);
  let observedIntegrity;
  let latest;
  while (true) {
    observedIntegrity = readIntegrity(name, version);
    if (observedIntegrity !== null && observedIntegrity !== expectedIntegrity) {
      fail(`${name}@${version} exists with different bytes`);
    }
    if (observedIntegrity === expectedIntegrity) {
      latest = readLatest(name);
      if (latest === version) return;
    }
    if (now() >= deadline) {
      if (observedIntegrity === expectedIntegrity && latest !== null) {
        fail(`${name} latest is ${latest}, not ${version}; repair the dist-tag interactively`);
      }
      fail(`${name}@${version} did not become visible with exact bytes and latest tag; observed integrity: ${observedIntegrity ?? "not found"}, latest: ${latest ?? "not found"}`);
    }
    await pause(5_000);
  }
}

export async function verifyPublicationAttempt(publication, name, version, expectedIntegrity, probes = {}) {
  const output = `${publication.stdout ?? ""}\n${publication.stderr ?? ""}`;
  if (publication.status !== 0 && !/cannot publish over (?:the )?previously published versions?/i.test(output)) {
    fail(`npm publication failed for ${name}@${version}: ${output}`);
  }
  await waitForPublishedExact(name, version, expectedIntegrity, probes);
}

async function main() {
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

    const publication = run("npm", ["publish", archive, "--access", "public", "--tag", "latest", "--provenance"]);
    process.stdout.write(publication.stdout ?? "");
    process.stderr.write(publication.stderr ?? "");
    await verifyPublicationAttempt(publication, item.name, releaseVersion, expectedIntegrity);
    console.log(`Published and verified: ${item.name}@${releaseVersion}`);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main();
}
