#!/usr/bin/env node

import { repositoryRoot as root } from "./policy-core.mjs";
import { workspacePackages } from "./workspace-model.mjs";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { spawnSync } from "node:child_process";
import { RegistryPublicationUnobservable, publicationCommand, observePublication } from "./registry-publication.mjs";

const order = JSON.parse(readFileSync(join(root, "release", "cargo-crates.json"), "utf8"));
const equivalents = JSON.parse(readFileSync(join(root, "release", "cargo-equivalent-archives.json"), "utf8"));

function fail(message) {
  throw new Error(message);
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", maxBuffer: Infinity, ...options });
  if (result.error) throw result.error;
  return result;
}

export function localCargoPackages() {
  const packages = workspacePackages(root);
  const selected = new Set(packages.filter(item => item.workspace_member));
  const byDirectory = new Map(packages.map(item => [dirname(item.manifest_path), item]));
  for (const item of selected) {
    for (const dependency of item.dependencies) {
      if (!dependency.path) continue;
      const local = byDirectory.get(dependency.path);
      if (!local) fail(`missing local publication dependency ${dependency.path}`);
      selected.add(local);
    }
  }
  return [...selected];
}


// The publication order is a topological order of the publishable local crates:
// every crate follows the local crates it depends on. Deriving it from Cargo
// declarations means a new publishable crate cannot be left out of the inventory.
export function publicationOrder(packages) {
  const publishable = packages.filter(item => item.source === null && (item.publish === null || item.publish.includes("crates-io")));
  const byName = new Map(publishable.map(item => [item.name, item]));
  const remaining = new Map(publishable.map(item => [item.name, new Set(item.dependencies
    .filter(dependency => dependency.kind !== "dev" && byName.has(dependency.name))
    .map(dependency => dependency.name))]));
  const order = [];
  while (remaining.size) {
    const ready = [...remaining]
      .filter(([, dependencies]) => [...dependencies].every(name => !remaining.has(name)))
      .map(([name]) => name)
      .sort();
    if (!ready.length) fail("Cargo publication order has a cycle");
    for (const name of ready) {
      order.push(name);
      remaining.delete(name);
    }
  }
  return order;
}

function generate() {
  const order = publicationOrder(localCargoPackages());
  writeFileSync(join(root, "release", "cargo-crates.json"), `${JSON.stringify(order, null, 2)}\n`);
}

export function metadata(releaseVersion) {
  const packages = localCargoPackages();
  const publishable = packages.filter(item => item.source === null && (item.publish === null || item.publish.includes("crates-io")));
  const byName = new Map(publishable.map(item => [item.name, item]));
  if (new Set(order).size !== order.length || order.some(name => !byName.has(name)) || byName.size !== order.length) {
    fail("release/cargo-crates.json must contain every publishable local crate exactly once");
  }
  const unpublishable = publicationDependencyErrors(packages, byName);
  if (unpublishable.length) fail(unpublishable.join("\n"));
  const missingReadmes = readmeErrors(publishable);
  if (missingReadmes.length) fail(missingReadmes.join("\n"));
  const position = new Map(order.map((name, index) => [name, index]));
  for (const item of publishable) {
    if (releaseVersion && item.version !== releaseVersion) fail(`${item.name} is not version ${releaseVersion}`);
    for (const dependency of item.dependencies) {
      if (dependency.kind === "dev" || !byName.has(dependency.name)) continue;
      if (position.get(dependency.name) >= position.get(item.name)) {
        fail(`${dependency.name} must precede ${item.name} in the Cargo publication order`);
      }
    }
  }
  return byName;
}

// `cargo package` refuses a declared readme that is not in the crate, so the
// publication inventory checks the file before the release reaches Cargo.
export function readmeErrors(packages) {
  return packages.flatMap(item => {
    if (!item.readme) return [];
    const path = resolve(dirname(item.manifest_path), item.readme);
    return existsSync(path) ? [] : [`${item.name} declares missing readme ${item.readme}`];
  });
}

// `cargo package` rejects every non-dev local dependency, optional or not, that
// is not itself published with a version requirement. Test-only crates
// (`publish = false`) must stay dev-dependencies of published crates.
export function publicationDependencyErrors(packages, byName) {
  const errors = [];
  for (const item of packages) {
    if (!byName.has(item.name)) continue;
    for (const dependency of item.dependencies) {
      if (dependency.kind === "dev" || !dependency.path) continue;
      const kind = `${dependency.optional ? "optional " : ""}${dependency.kind ?? "normal"}`;
      if (!byName.has(dependency.name)) {
        errors.push(`${item.name} depends on unpublished ${dependency.name} (${kind} dependency)`);
      } else if (dependency.req === "*") {
        errors.push(`${item.name} depends on ${dependency.name} without a version requirement`);
      }
    }
  }
  return errors;
}

async function registryVersion(name, version) {
  const response = await fetch(`https://crates.io/api/v1/crates/${encodeURIComponent(name)}/${encodeURIComponent(version)}`, {
    headers: { "User-Agent": "acyclic-release/0.1 (github.com/acyclic-labs/sdk)" },
  });
  if (response.status === 404) return null;
  if (!response.ok) fail(`step=cargo-registry-observation ${name}@${version} returned HTTP ${response.status}; recovery command: ${publicationCommand()}`);
  const observed = (await response.json()).version;
  if (!observed || observed.num !== version || typeof observed.checksum !== "string" || typeof observed.yanked !== "boolean") {
    fail(`step=cargo-registry-observation ${name}@${version} returned invalid exact-version metadata; recovery command: ${publicationCommand()}`);
  }
  return observed;
}

function archiveChecksum(name, version, item) {
  const archive = join(root, "target", "package", `${name}-${version}.crate`);
  try {
    unlinkSync(archive);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const packaged = run("cargo", ["package", "--locked", "--no-verify", "--manifest-path", item.manifest_path, "--target-dir", join(root, "target")], { stdio: "inherit" });
  if (packaged.status !== 0) fail(`cargo package failed for ${name}`);
  return createHash("sha256").update(readFileSync(archive)).digest("hex");
}

export function registryArchiveMatches(name, version, observed, checksum, sourceSha, item, options = {}) {
  if (observed.checksum === checksum) return "exact";
  const repository = options.repository ?? root;
  const approved = (options.equivalents ?? equivalents)[`${name}@${version}`];
  if (
    !approved
    || approved.checksum !== observed.checksum
    || !/^[0-9a-f]{40}$/u.test(approved.sourceSha)
  ) return null;
  const packageDir = relative(repository, dirname(item.manifest_path)).split(sep).join("/");
  if (!packageDir || packageDir.startsWith("../") || packageDir === "..") return null;
  const git = args => run("git", args, { cwd: repository });
  if (git(["merge-base", "--is-ancestor", approved.sourceSha, sourceSha]).status !== 0) return null;
  // Cargo packages the workspace manifest/lockfile and this crate directory.
  // A pinned registry checksum is accepted only while those inputs are unchanged.
  const unchanged = git([
    "diff", "--quiet", approved.sourceSha, sourceSha, "--",
    "Cargo.toml", "Cargo.lock", packageDir,
  ]);
  return unchanged.status === 0 ? "equivalent" : null;
}

async function publish(sourceSha, releaseVersion) {
  if (!/^[0-9a-f]{40}$/.test(sourceSha)) fail("source commit must be a full lowercase Git SHA-1");
  if (!/^\d+\.\d+\.\d+(?:[+-][0-9A-Za-z.-]+)?$/.test(releaseVersion)) fail("release version is invalid");
  const packages = metadata(releaseVersion);
  const head = run("git", ["rev-parse", "HEAD"]);
  if (head.status !== 0 || head.stdout.trim() !== sourceSha) fail("publication source differs from the qualified commit");
  const dirty = run("git", ["status", "--porcelain=v1"]);
  if (dirty.status !== 0 || dirty.stdout !== "") fail("publication source is dirty");

  for (const name of order) {
    const item = packages.get(name);
    const checksum = archiveChecksum(name, releaseVersion, item);
    await observePublication(
      { registry: "cargo", sourceSha, name, version: releaseVersion, expectedDigest: `sha256:${checksum}` },
      async observation => {
        observation.phase = "archive";
        const observed = observation.registry = await registryVersion(name, releaseVersion);
        if (observed) {
          if (observed.yanked) fail(`${name}@${releaseVersion} is yanked`);
          const match = registryArchiveMatches(name, releaseVersion, observed, checksum, sourceSha, item);
          if (!match) fail(`${name}@${releaseVersion} exists with different contents`);
          console.log(`Already published ${match} crate: ${name}@${releaseVersion}`);
          return;
        }
        observation.phase = "publication";
        const result = run("cargo", ["publish", "--locked", "--no-verify", "--manifest-path", item.manifest_path], { stdio: "inherit" });
        observation.publicationStatus = result.status;
        // Inspect propagation once after the original publisher has ended.
        observation.phase = "archive";
        const published = observation.registry = await registryVersion(name, releaseVersion);
        if (!published) {
          if (result.status !== 0) {
            fail(`step=cargo-publication ${name}@${releaseVersion} failed and its acceptance is unobservable; recovery command: ${publicationCommand()}`);
          }
          throw new RegistryPublicationUnobservable("cargo", name, releaseVersion,
            { visible: false, publicationStatus: result.status });
        }
        if (published.yanked) fail(`step=cargo-publication-identity ${name}@${releaseVersion} is yanked; recovery command: ${publicationCommand()}`);
        if (!registryArchiveMatches(name, releaseVersion, published, checksum, sourceSha, item)) {
          fail(`step=cargo-publication-identity ${name}@${releaseVersion} exists with different contents; recovery command: ${publicationCommand()}`);
        }
        console.log(`Published and verified: ${name}@${releaseVersion}`);
      },
    );
  }
}

if (import.meta.main) {
  const [command, sourceSha, releaseVersion] = process.argv.slice(2);
  if (command === "generate" && sourceSha === undefined && releaseVersion === undefined) generate();
  else if (command === "check" && sourceSha === undefined && releaseVersion === undefined) metadata();
  else if (command === "check" && sourceSha && releaseVersion) metadata(releaseVersion);
  else if (command === "publish" && sourceSha && releaseVersion) await publish(sourceSha, releaseVersion);
  else fail("usage: publish-cargo-crates.mjs generate | check [SOURCE_SHA VERSION] | publish SOURCE_SHA VERSION");
}
