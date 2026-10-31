#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { RegistryPublicationPending, publicationCommand } from "./registry-publication.mjs";
import { verifyNativeAssembly } from "./assemble-stream-native-package.mjs";
import { verifyNativeAssembly as verifyActorsNativeAssembly } from "./assemble-actors-native-package.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function fail(message) {
  throw new Error(message);
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", maxBuffer: Infinity, ...options });
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

function inspectPublishedExact(name, version, expectedIntegrity, {
  integrity: observedIntegrity = publishedIntegrity(name, version), publicationStatus,
}) {
  if (observedIntegrity !== null && observedIntegrity !== expectedIntegrity) {
    fail(`step=npm-publication-identity ${name}@${version} exists with different bytes; recovery command: ${publicationCommand()}`);
  }
  // Do not query a tag for an archive that has not become observable.
  const latest = observedIntegrity === expectedIntegrity ? publishedLatest(name) : null;
  if (observedIntegrity !== expectedIntegrity || latest !== version) {
    throw new RegistryPublicationPending("npm", name, version,
      { integrity: observedIntegrity, latest, publicationStatus });
  }
}

function verifyPublicationAttempt(publication, name, version, expectedIntegrity) {
  const output = `${publication.stdout ?? ""}\n${publication.stderr ?? ""}`;
  if (publication.status !== 0 && !/cannot publish over (?:the )?previously published versions?/i.test(output)) {
    fail(`step=npm-publication ${name}@${version} failed: ${output}; recovery command: ${publicationCommand()}`);
  }
  inspectPublishedExact(name, version, expectedIntegrity, { publicationStatus: publication.status });
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

    if (["stream", "actors"].includes(item.directory)) {
      const inventory = JSON.parse(readFileSync(join(root, `release/${item.directory}-native-packages.json`), "utf8"));
      const verify = ({ stream: verifyNativeAssembly, actors: verifyActorsNativeAssembly })[item.directory];
      const assembly = await verify(artifactDirectory, sourceSha, releaseVersion, inventory);
      await publishNativeAssembly(assembly, artifactDirectory);
      continue;
    }

    publishExact(item.name, releaseVersion, archive);
  }
}

export async function publishNativeAssembly(assembly, directory, publish = publishExact) {
  for (const companion of assembly.companions) await publish(companion.name, companion.version, join(directory, companion.asset));
  await publish(assembly.parent.name, assembly.parent.version, join(directory, assembly.parent.asset));
}

function publishExact(name, version, archive) {
  const expectedIntegrity = integrity(archive);
  const observedIntegrity = publishedIntegrity(name, version);
  if (observedIntegrity !== null) {
    inspectPublishedExact(name, version, expectedIntegrity, { integrity: observedIntegrity });
    console.log(`Already published exact archive: ${name}@${version}`);
    return;
  }

  const publication = run("npm", ["publish", archive, "--access", "public", "--tag", "latest", "--provenance"]);
  process.stdout.write(publication.stdout ?? "");
  process.stderr.write(publication.stderr ?? "");
  verifyPublicationAttempt(publication, name, version, expectedIntegrity);
  console.log(`Published and verified: ${name}@${version}`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main();
}
