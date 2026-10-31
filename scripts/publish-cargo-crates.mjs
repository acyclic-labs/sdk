#!/usr/bin/env node
import { captureCommand } from "./process-capture.mjs";
import { publicationMain, publicationReceipt, retainPublication, requirePublication } from "./publication-receipt.mjs";

import { createHash } from "node:crypto";
import { readFileSync, unlinkSync } from "node:fs";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const order = JSON.parse(readFileSync(join(root, "release", "cargo-crates.json"), "utf8"));
const equivalents = JSON.parse(readFileSync(join(root, "release", "cargo-equivalent-archives.json"), "utf8"));

function fail(message) {
  throw new Error(message);
}

function run(command, args, options = {}) {
  const result = captureCommand(command, args, { cwd: root, encoding: "utf8", ...options });
  if (result.error) throw result.error;
  return result;
}

export function metadata(releaseVersion) {
  const result = run("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"]);
  if (result.status !== 0) fail(result.stderr || "cargo metadata failed");
  const packages = JSON.parse(result.stdout).packages;
  const known = new Set(packages.map(item => item.name));
  for (const item of packages) {
    for (const dependency of item.dependencies) {
      if (!dependency.path || !dependency.name.startsWith("acyclic-") || known.has(dependency.name)) continue;
      const local = run("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1",
        "--manifest-path", join(dependency.path, "Cargo.toml")]);
      if (local.status !== 0) fail(local.stderr || "local Cargo metadata failed");
      for (const candidate of JSON.parse(local.stdout).packages) {
        if (!known.has(candidate.name)) { known.add(candidate.name); packages.push(candidate); }
      }
    }
  }
  const publishable = packages.filter(item => item.source === null && (item.publish === null || item.publish.includes("crates-io")));
  const byName = new Map(publishable.map(item => [item.name, item]));
  if (new Set(order).size !== order.length || order.some(name => !byName.has(name)) || byName.size !== order.length) {
    fail("release/cargo-crates.json must contain every publishable local crate exactly once");
  }
  const unpublishable = publicationDependencyErrors(packages, byName);
  if (unpublishable.length) fail(unpublishable.join("\n"));
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
  const response = await fetch(`https://crates.io/api/v1/crates/${encodeURIComponent(name)}`, {
    headers: { "User-Agent": "acyclic-release/0.1 (github.com/acyclic-labs/sdk)" },
  });
  if (response.status === 404) return null;
  if (!response.ok) fail(`crates.io returned ${response.status} for ${name}`);
  const payload = await response.json();
  return payload.versions.find(item => item.num === version) ?? null;
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

export async function reconcileRegistry(name, version, checksum, sourceSha, item, lookup = registryVersion) {
  const observed = await lookup(name, version);
  if (observed?.yanked) fail(`${name}@${version} is yanked`);
  return publicationReceipt(
    { registry: "crates.io", sourceSha, name, version, expectedDigest: checksum },
    { digest: observed?.checksum ?? null },
    () => registryArchiveMatches(name, version, observed, checksum, sourceSha, item),
  );
}

async function publish(sourceSha, releaseVersion, reconcileOnly = false) {
  if (!/^[0-9a-f]{40}$/.test(sourceSha)) fail("source commit must be a full lowercase Git SHA-1");
  if (!/^\d+\.\d+\.\d+(?:[+-][0-9A-Za-z.-]+)?$/.test(releaseVersion)) fail("release version is invalid");
  const packages = metadata(releaseVersion);
  const head = run("git", ["rev-parse", "HEAD"]);
  if (head.status !== 0 || head.stdout.trim() !== sourceSha) fail("publication source differs from the qualified commit");
  const dirty = run("git", ["status", "--porcelain=v1"]);
  if (dirty.status !== 0 || dirty.stdout !== "") fail("publication source is dirty");
  const receiptDirectory = join(root, "target", "registry-publication");
  const retain = receipt => requirePublication(receipt, receiptDirectory,
    `node scripts/publish-cargo-crates.mjs reconcile ${sourceSha} ${releaseVersion}`);

  for (const name of order) {
    const item = packages.get(name);
    const checksum = archiveChecksum(name, releaseVersion, item);
    const observed = await registryVersion(name, releaseVersion);
    if (observed || reconcileOnly) {
      retain(await reconcileRegistry(name, releaseVersion, checksum, sourceSha, item, async () => observed));
      console.log(`Verified published crate: ${name}@${releaseVersion}`);
      continue;
    }
    const unconfirmed = submission => publicationReceipt(
      { registry: "crates.io", sourceSha, name, version: releaseVersion, expectedDigest: checksum },
      { digest: null, submission },
    );
    retainPublication(unconfirmed({ state: "Prepared" }), receiptDirectory);
    const result = run("cargo", ["publish", "--locked", "--no-verify", "--manifest-path", item.manifest_path], { stdio: "inherit" });
    retainPublication(unconfirmed({ state: "Returned", status: result.status, signal: result.signal }), receiptDirectory);
    let raced;
    if (result.status !== 0) {
      raced = await registryVersion(name, releaseVersion);
      if (!raced || raced.yanked || !registryArchiveMatches(name, releaseVersion, raced, checksum, sourceSha, item)) {
        fail(`cargo publish failed for ${name}`);
      }
    }
    retain(await reconcileRegistry(name, releaseVersion, checksum, sourceSha, item,
      raced ? async () => raced : registryVersion));
    console.log(`Published and verified: ${name}@${releaseVersion}`);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [command, sourceSha, releaseVersion] = process.argv.slice(2);
  if (command === "check" && sourceSha === undefined && releaseVersion === undefined) metadata();
  else if (command === "check" && sourceSha && releaseVersion) metadata(releaseVersion);
  else if (["publish", "reconcile"].includes(command) && sourceSha && releaseVersion) {
    await publicationMain(() => publish(sourceSha, releaseVersion, command === "reconcile"));
  } else fail("usage: publish-cargo-crates.mjs check [SOURCE_SHA VERSION] | publish|reconcile SOURCE_SHA VERSION");
}
