#!/usr/bin/env node

import { createHash } from "node:crypto";
import { existsSync, lstatSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const SHA_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const VERSION_PATTERN = /^\d+\.\d+\.\d+(?:[+-][0-9A-Za-z.-]+)?$/;

function fail(message) {
  throw new Error(message);
}

function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
  if (result.error || result.status !== 0) fail(`${command} failed: ${result.error ?? result.stderr}`);
  return result.stdout.trim();
}

function hash(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function spdxId(identity) {
  return `SPDXRef-${hash(Buffer.from(identity)).slice(0, 24)}`;
}

function createSbom(outputArgument, sourceSha, version) {
  if (!SHA_PATTERN.test(sourceSha ?? "") || !VERSION_PATTERN.test(version ?? "")) fail("SBOM release identity is invalid");
  const output = resolve(outputArgument);
  if (existsSync(output)) fail("SBOM output already exists");

  const cargoReleaseNames = JSON.parse(readFileSync(resolve(root, "release", "cargo-crates.json"), "utf8"));
  const npmReleaseEntries = JSON.parse(readFileSync(resolve(root, "release", "npm-packages.json"), "utf8"));
  if (!Array.isArray(cargoReleaseNames) || new Set(cargoReleaseNames).size !== cargoReleaseNames.length) fail("Cargo release manifest is invalid");
  if (!Array.isArray(npmReleaseEntries) || new Set(npmReleaseEntries.map(item => item.name)).size !== npmReleaseEntries.length) fail("npm release manifest is invalid");
  const metadata = JSON.parse(run("cargo", ["metadata", "--locked", "--format-version", "1"]));
  const nodes = new Map(metadata.resolve.nodes.map(node => [node.id, node]));
  const packages = new Map(metadata.packages.map(item => [item.id, item]));
  const cargoRoots = cargoReleaseNames.map(name => {
    const matches = metadata.packages.filter(item => item.name === name);
    if (matches.length !== 1 || matches[0].version !== version) fail(`Cargo release identity differs for ${name}`);
    return matches[0];
  });
  const reachable = new Set();
  const pending = cargoRoots.map(item => item.id);
  while (pending.length > 0) {
    const id = pending.pop();
    if (reachable.has(id)) continue;
    reachable.add(id);
    const node = nodes.get(id);
    if (!node) fail(`Cargo dependency graph omits ${id}`);
    for (const dependency of node.dependencies) pending.push(dependency);
  }

  const cargoPackages = [...reachable].map(id => {
    const item = packages.get(id);
    if (!item) fail(`Cargo metadata omits ${id}`);
    return item;
  }).sort((left, right) => left.id.localeCompare(right.id));
  const npmPackages = npmReleaseEntries.map(item => {
    if (!item || !["typescript", "plugin"].includes(item.source)) fail("npm release source is invalid");
    const path = item.source === "typescript"
      ? resolve(root, "typescript", "packages", item.directory, "package.json")
      : resolve(root, "plugin", "package.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    if (manifest.name !== item.name || manifest.version !== version || manifest.private !== false) {
      fail(`npm release identity differs for ${item.name}`);
    }
    return manifest;
  });
  const npmIds = new Map(npmPackages.map(item => [item.name, spdxId(`npm:${item.name}@${item.version}`)]));
  const rustIds = new Map(cargoPackages.map(item => [item.id, spdxId(`cargo:${item.id}`)]));
  const externalNpm = new Map();
  for (const item of npmPackages) {
    for (const [name, requirement] of Object.entries({
      ...item.dependencies,
      ...item.optionalDependencies,
      ...item.peerDependencies,
    })) {
      if (!npmIds.has(name)) externalNpm.set(`${name}\0${requirement}`, { name, version: requirement });
    }
  }
  const npmRecords = [...npmPackages.map(item => ({ name: item.name, version: item.version, license: item.license })),
    ...externalNpm.values()].map(item => {
    const [scope, name] = item.name.startsWith("@") ? item.name.split("/") : [null, item.name];
    if (!name || (scope && !scope.startsWith("@"))) fail(`npm package name is invalid: ${item.name}`);
    const purlName = scope ? `${encodeURIComponent(scope)}/${encodeURIComponent(name)}` : encodeURIComponent(name);
    const id = npmIds.get(item.name) ?? spdxId(`npm:${item.name}@${item.version}`);
    return {
      SPDXID: id,
      name: item.name,
      versionInfo: item.version,
      downloadLocation: "NOASSERTION",
      filesAnalyzed: false,
      licenseConcluded: "NOASSERTION",
      licenseDeclared: item.license ?? "NOASSERTION",
      copyrightText: "NOASSERTION",
      externalRefs: [{
        referenceCategory: "PACKAGE-MANAGER",
        referenceType: "purl",
        referenceLocator: `pkg:npm/${purlName}@${encodeURIComponent(item.version)}`,
      }],
    };
  });
  const packageRecords = [...npmRecords, ...cargoPackages.map(item => ({
    SPDXID: rustIds.get(item.id),
    name: item.name,
    versionInfo: item.version,
    downloadLocation: "NOASSERTION",
    filesAnalyzed: false,
    licenseConcluded: "NOASSERTION",
    licenseDeclared: item.license ?? "NOASSERTION",
    copyrightText: "NOASSERTION",
    externalRefs: [{
      referenceCategory: "PACKAGE-MANAGER",
      referenceType: "purl",
      referenceLocator: `pkg:cargo/${encodeURIComponent(item.name)}@${item.version}`,
    }],
  }))];

  const relationships = [];
  for (const item of npmPackages) {
    relationships.push({ spdxElementId: "SPDXRef-DOCUMENT", relationshipType: "DESCRIBES", relatedSpdxElement: npmIds.get(item.name) });
    for (const [name, requirement] of Object.entries({ ...item.dependencies, ...item.optionalDependencies, ...item.peerDependencies })) {
      relationships.push({
        spdxElementId: npmIds.get(item.name),
        relationshipType: "DEPENDS_ON",
        relatedSpdxElement: npmIds.get(name) ?? spdxId(`npm:${name}@${requirement}`),
      });
    }
  }
  for (const item of cargoRoots) {
    relationships.push({ spdxElementId: "SPDXRef-DOCUMENT", relationshipType: "DESCRIBES", relatedSpdxElement: rustIds.get(item.id) });
  }
  const pluginCargo = cargoRoots.find(item => item.name === "acyclic-labs-plugin");
  if (!pluginCargo || !npmIds.has("@acyclic-labs/plugin")) fail("plugin release surface is missing");
  relationships.push({ spdxElementId: npmIds.get("@acyclic-labs/plugin"), relationshipType: "CONTAINS", relatedSpdxElement: rustIds.get(pluginCargo.id) });
  for (const item of cargoPackages) {
    for (const dependency of nodes.get(item.id).dependencies) {
      if (reachable.has(dependency)) relationships.push({
        spdxElementId: rustIds.get(item.id),
        relationshipType: "DEPENDS_ON",
        relatedSpdxElement: rustIds.get(dependency),
      });
    }
  }
  relationships.sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)));

  const created = run("git", ["show", "-s", "--format=%cI", sourceSha]);
  const sbom = {
    spdxVersion: "SPDX-2.3",
    dataLicense: "CC0-1.0",
    SPDXID: "SPDXRef-DOCUMENT",
    name: `acyclic-${version}`,
    documentNamespace: `https://github.com/acyclic-labs/sdk/releases/tag/acyclic-v${encodeURIComponent(version)}/spdx/${sourceSha}`,
    creationInfo: { created, creators: ["Tool: acyclic-release-evidence/1"] },
    packages: packageRecords,
    relationships,
  };
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, `${JSON.stringify(sbom, null, 2)}\n`, { flag: "wx" });
}

function safeSubject(manifestRoot, argument) {
  const path = resolve(argument);
  const relativePath = relative(manifestRoot, path);
  if (!relativePath || relativePath === ".." || relativePath.startsWith(`..${sep}`) || isAbsolute(relativePath)) {
    fail(`release subject escapes its manifest directory: ${argument}`);
  }
  const metadata = lstatSync(path);
  if (!metadata.isFile() || metadata.isSymbolicLink()) fail(`release subject is not a regular file: ${argument}`);
  if (relative(manifestRoot, realpathSync(path)).startsWith(`..${sep}`)) fail(`release subject resolves outside its manifest directory: ${argument}`);
  return { path, name: relativePath.split(sep).join("/") };
}

function createChecksums(outputArgument, inputArguments) {
  if (inputArguments.length === 0) fail("at least one release subject is required");
  const output = resolve(outputArgument);
  if (existsSync(output)) fail("checksum manifest already exists");
  const manifestRoot = dirname(output);
  const subjects = inputArguments.map(item => safeSubject(manifestRoot, item));
  subjects.sort((left, right) => left.name.localeCompare(right.name));
  if (new Set(subjects.map(item => item.name)).size !== subjects.length) fail("checksum subjects contain duplicates");
  writeFileSync(output, `${subjects.map(item => `${hash(readFileSync(item.path))}  ${item.name}`).join("\n")}\n`, { flag: "wx" });
}

function verifyChecksums(inputArgument) {
  const input = resolve(inputArgument);
  const manifestRoot = dirname(input);
  const lines = readFileSync(input, "utf8").trimEnd().split("\n");
  if (lines.length === 0) fail("checksum manifest is empty");
  const names = new Set();
  for (const line of lines) {
    const match = /^([0-9a-f]{64})  ([^\r\n]+)$/.exec(line);
    if (!match) fail("checksum manifest syntax is invalid");
    const [, expected, name] = match;
    if (names.has(name)) fail("checksum manifest contains duplicate subjects");
    names.add(name);
    const subject = safeSubject(manifestRoot, resolve(manifestRoot, ...name.split("/")));
    if (subject.name !== name || hash(readFileSync(subject.path)) !== expected) fail(`release checksum differs for ${name}`);
  }
}

const [command, ...args] = process.argv.slice(2);
if (command === "sbom" && args.length === 3) createSbom(...args);
else if (command === "checksums" && args.length >= 2) createChecksums(args[0], args.slice(1));
else if (command === "verify" && args.length === 1) verifyChecksums(args[0]);
else fail("usage: release-evidence.mjs sbom OUTPUT SOURCE_SHA VERSION | checksums OUTPUT SUBJECT... | verify CHECKSUMS");
