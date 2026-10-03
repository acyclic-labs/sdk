import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const REQUIRED_PROFILE_NAMES = [
  "host-default",
  "native-bindings",
  "host-capabilities",
  "wasm-bindings",
];

export function requiredPackages(manifest) {
  return new Set((manifest.profiles ?? []).flatMap(profile =>
    (profile.packages ?? []).map(packageEntry => packageEntry.package)));
}

export function normalizedFeatures(features) {
  return [...(features ?? [])].sort();
}

function assertionFailure(message) {
  throw new Error(message);
}

function isNonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function isBlake3Digest(value) {
  return typeof value === "string" && /^[0-9a-f]{64}$/i.test(value);
}

/**
 * Validate the package/facade inventory and compiler-resolved graph metadata
 * emitted by sdk-docs. This consumes JSON so it can validate retained CLI
 * output after the producer exits.
 */
export function validateStrictDocsBundle(value, manifest, label = "sdk-docs output") {
  const bundle = value ?? {};
  if (bundle.schema_version !== 1) assertionFailure(`${label}: expected bundle schema_version 1`);
  if (!isNonEmptyString(bundle.source_revision) || bundle.source_revision === "unknown") {
    assertionFailure(`${label}: source_revision must identify the producer snapshot`);
  }
  if (manifest?.schema_version !== 1) assertionFailure(`${label}: profile manifest must use schema 1`);

  const profileNames = (manifest.profiles ?? []).map(profile => profile.name);
  if (JSON.stringify(profileNames) !== JSON.stringify(REQUIRED_PROFILE_NAMES)) {
    assertionFailure(`${label}: profile manifest must retain the four required target/feature profiles`);
  }
  const crates = bundle.crates ?? [];
  const byPackage = new Map();
  for (const crate of crates) {
    if (!isNonEmptyString(crate.package_name)) assertionFailure(`${label}: crate has no package_name`);
    if (byPackage.has(crate.package_name)) assertionFailure(`${label}: duplicate package ${crate.package_name}`);
    byPackage.set(crate.package_name, crate);
    if (crate.analysis_mode !== "rustdoc-json") {
      assertionFailure(`${label}: package ${crate.package_name} is not backed by rustdoc-json`);
    }
  }

  const expected = requiredPackages(manifest);
  for (const packageName of expected) {
    const crate = byPackage.get(packageName);
    if (!crate) assertionFailure(`${label}: required package/facade ${packageName} is absent`);
    const graphs = crate.graphs ?? [];
    for (const profile of manifest.profiles) {
      const packageEntry = profile.packages.find(entry => entry.package === packageName);
      if (!packageEntry) continue;
      const graph = graphs.find(candidate =>
        candidate.profile === profile.name
        && isNonEmptyString(candidate.target)
        && (packageEntry.target === "host" || candidate.target === packageEntry.target)
        && JSON.stringify(normalizedFeatures(candidate.features)) === JSON.stringify(normalizedFeatures(packageEntry.features))
        && (candidate.public_items?.length ?? 0) > 0,
      );
      if (!graph) assertionFailure(`${label}: ${packageName} has no resolved public graph for ${profile.name}`);
      if (!graph.rustdoc
        || !isNonEmptyString(graph.rustdoc.path)
        || !isBlake3Digest(graph.rustdoc.blake3)
        || !isBlake3Digest(graph.rustdoc.source_blake3)
        || graph.rustdoc.source_revision !== bundle.source_revision
        || !isBlake3Digest(graph.rustdoc.profile_blake3)
        || !isNonEmptyString(graph.rustdoc.target)
        || graph.rustdoc.target !== graph.target
        || !Array.isArray(graph.rustdoc.features)
        || JSON.stringify(normalizedFeatures(graph.rustdoc.features)) !== JSON.stringify(normalizedFeatures(graph.features))
        || graph.profile_blake3 !== graph.rustdoc.profile_blake3) {
        assertionFailure(`${label}: ${packageName}/${profile.name} has incomplete rustdoc source binding`);
      }
      for (const item of graph.public_items ?? []) {
        if (!isNonEmptyString(item.name) || !isNonEmptyString(item.kind)) {
          assertionFailure(`${label}: ${packageName}/${profile.name} contains an unnamed graph item`);
        }
        if (!isNonEmptyString(item.module_path)) {
          assertionFailure(`${label}: ${packageName}/${profile.name}/${item.name} has no module_path`);
        }
        if (!isNonEmptyString(item.source_path) || !Number.isInteger(item.source_line) || item.source_line < 1) {
          assertionFailure(`${label}: ${packageName}/${profile.name}/${item.name} has no source path/line`);
        }
        // `use` and `mod` nodes are graph edges/namespaces, so rustdoc does
        // not provide declaration signatures for them. Semantic declarations
        // must carry a signature; graph edges remain covered by their path.
        if (!new Set(["use", "mod"]).has(item.kind)
          && (item.signature === undefined || item.signature === null)) {
          assertionFailure(`${label}: ${packageName}/${profile.name}/${item.name} has no semantic signature`);
        }
      }
    }
  }

  const statuses = bundle.profiles ?? [];
  if (statuses.length !== manifest.profiles.length) {
    assertionFailure(`${label}: profile status inventory is incomplete`);
  }
  for (const profile of manifest.profiles) {
    const status = statuses.find(candidate => candidate.profile?.name === profile.name);
    if (!status || status.complete !== true
      || (status.missing_packages?.length ?? 0) > 0
      || (status.unresolved_packages?.length ?? 0) > 0) {
      assertionFailure(`${label}: profile ${profile.name} is incomplete`);
    }
  }
}

async function main() {
  const args = process.argv.slice(2);
  const valueFor = flag => {
    const index = args.indexOf(flag);
    return index >= 0 ? args[index + 1] : undefined;
  };
  const bundlePath = valueFor("--bundle");
  const profilePath = valueFor("--profiles");
  if (!bundlePath || !profilePath) {
    console.error("usage: node scripts/validate-rustdoc-graphs.mjs --bundle docs.json --profiles docs/rustdoc-profiles.json");
    process.exitCode = 2;
    return;
  }
  try {
    const bundle = JSON.parse(await readFile(bundlePath, "utf8"));
    const manifest = JSON.parse(await readFile(profilePath, "utf8"));
    validateStrictDocsBundle(bundle, manifest, bundlePath);
    console.log(`validated strict rustdoc graph inventory: ${bundlePath}`);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url).toLowerCase() === resolve(process.argv[1]).toLowerCase()) {
  await main();
}
