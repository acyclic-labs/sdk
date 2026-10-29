#!/usr/bin/env node
import { captureCommand } from "./process-capture.mjs";

import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { publicationMain, publicationReceipt, retainPublication, requirePublication } from "./publication-receipt.mjs";
import { verifyNativeAssembly } from "./assemble-stream-native-package.mjs";
import { verifyNativeAssembly as verifyActorsNativeAssembly } from "./assemble-actors-native-package.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function fail(message) {
  throw new Error(message);
}

function run(command, args, options = {}) {
  const result = captureCommand(command, args, { cwd: root, encoding: "utf8", ...options });
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

export async function reconcilePublishedExact(name, version, expectedIntegrity, probes = {}) {
  const readIntegrity = probes.readIntegrity ?? publishedIntegrity;
  const readLatest = probes.readLatest ?? publishedLatest;
  const observedIntegrity = readIntegrity(name, version);
  return publicationReceipt(
    { registry: "npm", sourceSha: probes.sourceSha, name, version, expectedDigest: expectedIntegrity },
    { digest: observedIntegrity, tag: observedIntegrity === expectedIntegrity ? { expected: version, observed: readLatest(name) } : null },
  );
}

export async function verifyPublicationAttempt(publication, name, version, expectedIntegrity, probes = {}) {
  const output = `${publication.stdout ?? ""}\n${publication.stderr ?? ""}`;
  if (publication.status !== 0 && !/cannot publish over (?:the )?previously published versions?/i.test(output)) {
    fail(`npm publication failed for ${name}@${version}: ${output}`);
  }
  return reconcilePublishedExact(name, version, expectedIntegrity, probes);
}

async function main() {
  const arguments_ = process.argv.slice(2);
  const reconcileOnly = arguments_[0] === "reconcile";
  if (reconcileOnly) arguments_.shift();
  const [artifactArgument, sourceSha, releaseVersion] = arguments_;
  if (!artifactArgument || !/^[0-9a-f]{40}$/.test(sourceSha ?? "") || !/^\d+\.\d+\.\d+(?:[+-][0-9A-Za-z.-]+)?$/.test(releaseVersion ?? "")) {
    fail("usage: publish-npm-packages.mjs [reconcile] ARTIFACT_DIR SOURCE_SHA VERSION");
  }

  const artifactDirectory = resolve(artifactArgument);
  const receipt = join(artifactDirectory, "QUALIFICATION.json");
  const publish = (name, version, archive) => publishExact(name, version, archive, {
    sourceSha, reconcileOnly, directory: join(artifactDirectory, "registry-publication"),
    command: `node scripts/publish-npm-packages.mjs reconcile ${JSON.stringify(artifactArgument)} ${sourceSha} ${releaseVersion}`,
  });
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
      await publishNativeAssembly(assembly, artifactDirectory, publish);
      continue;
    }

    await publish(item.name, releaseVersion, archive);
  }
}

export async function publishNativeAssembly(assembly, directory, publish) {
  for (const companion of assembly.companions) await publish(companion.name, companion.version, join(directory, companion.asset));
  await publish(assembly.parent.name, assembly.parent.version, join(directory, assembly.parent.asset));
}

export async function publishExact(name, version, archive, policy) {
    const expectedIntegrity = integrity(archive);
    const observedIntegrity = publishedIntegrity(name, version);
    if (observedIntegrity !== null || policy.reconcileOnly) {
      if (observedIntegrity !== null && observedIntegrity !== expectedIntegrity) fail(`${name}@${version} exists with different bytes`);
      const receipt = await reconcilePublishedExact(name, version, expectedIntegrity, {
        sourceSha: policy.sourceSha, readIntegrity: () => observedIntegrity,
      });
      requirePublication(receipt, policy.directory, policy.command);
      console.log(`Verified exact archive: ${name}@${version}`);
      return;
    }

    const unconfirmed = submission => publicationReceipt(
      { registry: "npm", sourceSha: policy.sourceSha, name, version, expectedDigest: expectedIntegrity },
      { digest: null, submission },
    );
    retainPublication(unconfirmed({ state: "Prepared" }), policy.directory);
    const publication = run("npm", ["publish", archive, "--access", "public", "--tag", "latest", "--provenance"]);
    retainPublication(unconfirmed({ state: "Returned", status: publication.status, signal: publication.signal }), policy.directory);
    process.stdout.write(publication.stdout ?? "");
    process.stderr.write(publication.stderr ?? "");
    const receipt = await verifyPublicationAttempt(publication, name, version, expectedIntegrity, { sourceSha: policy.sourceSha });
    requirePublication(receipt, policy.directory, policy.command);
    console.log(`Published and verified: ${name}@${version}`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await publicationMain(main);
}
