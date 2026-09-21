#!/usr/bin/env node

import { createHash } from "node:crypto";
import { readFileSync, unlinkSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const order = JSON.parse(readFileSync(join(root, "release", "cargo-crates.json"), "utf8"));

function fail(message) {
  throw new Error(message);
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", ...options });
  if (result.error) throw result.error;
  return result;
}

function metadata(releaseVersion) {
  const result = run("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1"]);
  if (result.status !== 0) fail(result.stderr || "cargo metadata failed");
  const packages = JSON.parse(result.stdout).packages;
  const publishable = packages.filter(item => item.publish === null || item.publish.includes("crates-io"));
  const byName = new Map(publishable.map(item => [item.name, item]));
  if (new Set(order).size !== order.length || order.some(name => !byName.has(name)) || byName.size !== order.length) {
    fail("release/cargo-crates.json must contain every publishable workspace crate exactly once");
  }
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

async function registryVersion(name, version) {
  const response = await fetch(`https://crates.io/api/v1/crates/${encodeURIComponent(name)}`, {
    headers: { "User-Agent": "acyclic-release/0.1 (github.com/acyclic-labs/sdk)" },
  });
  if (response.status === 404) return null;
  if (!response.ok) fail(`crates.io returned ${response.status} for ${name}`);
  const payload = await response.json();
  return payload.versions.find(item => item.num === version) ?? null;
}

function archiveChecksum(name, version) {
  const archive = join(root, "target", "package", `${name}-${version}.crate`);
  try {
    unlinkSync(archive);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const packaged = run("cargo", ["package", "--locked", "--no-verify", "-p", name], { stdio: "inherit" });
  if (packaged.status !== 0) fail(`cargo package failed for ${name}`);
  return createHash("sha256").update(readFileSync(archive)).digest("hex");
}

async function waitForRegistry(name, version, checksum) {
  for (let attempt = 0; attempt < 90; attempt += 1) {
    const observed = await registryVersion(name, version);
    if (observed) {
      if (observed.yanked) fail(`${name}@${version} is yanked`);
      if (observed.checksum !== checksum) fail(`${name}@${version} exists with different bytes`);
      return;
    }
    await new Promise(resolveDelay => setTimeout(resolveDelay, 2_000));
  }
  fail(`${name}@${version} did not become visible on crates.io`);
}

async function publish(sourceSha, releaseVersion) {
  if (!/^[0-9a-f]{40}$/.test(sourceSha)) fail("source commit must be a full lowercase Git SHA-1");
  if (!/^\d+\.\d+\.\d+(?:[+-][0-9A-Za-z.-]+)?$/.test(releaseVersion)) fail("release version is invalid");
  metadata(releaseVersion);
  const head = run("git", ["rev-parse", "HEAD"]);
  if (head.status !== 0 || head.stdout.trim() !== sourceSha) fail("publication source differs from the qualified commit");
  const dirty = run("git", ["status", "--porcelain=v1"]);
  if (dirty.status !== 0 || dirty.stdout !== "") fail("publication source is dirty");

  for (const name of order) {
    const checksum = archiveChecksum(name, releaseVersion);
    const observed = await registryVersion(name, releaseVersion);
    if (observed) {
      if (observed.yanked) fail(`${name}@${releaseVersion} is yanked`);
      if (observed.checksum !== checksum) fail(`${name}@${releaseVersion} exists with different bytes`);
      console.log(`Already published exact crate: ${name}@${releaseVersion}`);
      continue;
    }
    const result = run("cargo", ["publish", "--locked", "--no-verify", "-p", name], { stdio: "inherit" });
    if (result.status !== 0) {
      const raced = await registryVersion(name, releaseVersion);
      if (!raced || raced.yanked || raced.checksum !== checksum) fail(`cargo publish failed for ${name}`);
    }
    await waitForRegistry(name, releaseVersion, checksum);
    console.log(`Published and verified: ${name}@${releaseVersion}`);
  }
}

const [command, sourceSha, releaseVersion] = process.argv.slice(2);
if (command === "check" && sourceSha === undefined && releaseVersion === undefined) metadata();
else if (command === "check" && sourceSha && releaseVersion) metadata(releaseVersion);
else if (command === "publish" && sourceSha && releaseVersion) await publish(sourceSha, releaseVersion);
else fail("usage: publish-cargo-crates.mjs check [SOURCE_SHA VERSION] | publish SOURCE_SHA VERSION");
