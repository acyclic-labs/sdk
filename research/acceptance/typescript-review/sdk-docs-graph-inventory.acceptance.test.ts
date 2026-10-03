import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const REQUIRED_PROFILE_NAMES = [
  "host-default",
  "native-bindings",
  "host-capabilities",
  "wasm-bindings",
] as const;

type ProfilePackage = {
  package: string;
  target: string;
  features?: string[];
  default_features?: boolean;
};

type Profile = { name: string; packages: ProfilePackage[] };
type ProfileManifest = { schema_version: number; profiles: Profile[] };

type PublicItem = {
  name?: string;
  kind?: string;
  module_path?: string | null;
  signature?: unknown;
  source_path?: string | null;
  source_line?: number | null;
  reexport?: unknown;
};

type Graph = {
  profile?: string;
  target?: string;
  features?: string[];
  profile_blake3?: string;
  public_items?: PublicItem[];
  rustdoc?: {
    path?: string;
    blake3?: string;
    source_blake3?: string;
    source_revision?: string;
    profile_blake3?: string;
  };
};

type CrateBundle = {
  package_name?: string;
  analysis_mode?: string;
  public_items?: PublicItem[];
  graphs?: Graph[];
};

type DocsBundle = {
  schema_version?: number;
  source_revision?: string;
  crates?: CrateBundle[];
  profiles?: Array<{
    profile?: Profile;
    complete?: boolean;
    missing_packages?: string[];
    unresolved_packages?: string[];
  }>;
};

function assertionFailure(message: string): never {
  throw new Error(message);
}

function requiredPackages(manifest: ProfileManifest): Set<string> {
  return new Set(manifest.profiles.flatMap(profile => profile.packages.map(packageEntry => packageEntry.package)));
}

function normalizedFeatures(features: string[] | undefined): string[] {
  return [...(features ?? [])].sort();
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

/**
 * Validate the package/facade inventory and compiler-resolved graph metadata
 * emitted by sdk-docs. This intentionally consumes JSON rather than producer
 * types so it can validate a retained CLI output after the producer exits.
 */
export function validateStrictDocsBundle(
  value: unknown,
  manifest: ProfileManifest,
  label = "sdk-docs output",
): asserts value is DocsBundle {
  const bundle = value as DocsBundle;
  if (bundle.schema_version !== 1) assertionFailure(`${label}: expected bundle schema_version 1`);
  if (!isNonEmptyString(bundle.source_revision) || bundle.source_revision === "unknown") {
    assertionFailure(`${label}: source_revision must identify the producer snapshot`);
  }
  if (manifest.schema_version !== 1) assertionFailure(`${label}: profile manifest schema drifted from schema 1`);

  const profileNames = manifest.profiles.map(profile => profile.name);
  if (JSON.stringify(profileNames) !== JSON.stringify(REQUIRED_PROFILE_NAMES)) {
    assertionFailure(`${label}: profile manifest must retain the four required target/feature profiles`);
  }
  const crates = bundle.crates ?? [];
  const byPackage = new Map<string, CrateBundle>();
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
        && (packageEntry.target === "host" || candidate.target === packageEntry.target)
        && JSON.stringify(normalizedFeatures(candidate.features)) === JSON.stringify(normalizedFeatures(packageEntry.features))
        && (candidate.public_items?.length ?? 0) > 0,
      );
      if (!graph) {
        assertionFailure(`${label}: ${packageName} has no resolved public graph for ${profile.name}`);
      }
      if (!graph.rustdoc || !isNonEmptyString(graph.rustdoc.path)
        || !isNonEmptyString(graph.rustdoc.blake3)
        || !isNonEmptyString(graph.rustdoc.source_blake3)
        || graph.rustdoc.source_revision !== bundle.source_revision
        || !isNonEmptyString(graph.rustdoc.profile_blake3)
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
        // not provide a declaration signature for them. Every semantic public
        // declaration must carry one; unresolved `use` edges are rejected.
        const signatureRequired = !new Set(["use", "mod"]).has(item.kind);
        if (signatureRequired && (item.signature === undefined || item.signature === null)) {
          assertionFailure(`${label}: ${packageName}/${profile.name}/${item.name} has no semantic signature`);
        }
        if (item.kind === "use" && item.signature == null && item.reexport == null) {
          assertionFailure(`${label}: ${packageName}/${profile.name}/${item.name} is an unresolved public use edge`);
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
    if (!status || status.complete !== true || (status.missing_packages?.length ?? 0) > 0 || (status.unresolved_packages?.length ?? 0) > 0) {
      assertionFailure(`${label}: profile ${profile.name} is incomplete`);
    }
  }
}

function syntheticStrictBundle(manifest: ProfileManifest): DocsBundle {
  const sourceRevision = "synthetic-validator-fixture-revision";
  const crates = [...requiredPackages(manifest)].map(packageName => {
    const packageProfiles = manifest.profiles.filter(profile => profile.packages.some(entry => entry.package === packageName));
    const graphs = packageProfiles.map(profile => {
      const entry = profile.packages.find(candidate => candidate.package === packageName)!;
      const profileBlake3 = `synthetic-${profile.name}`;
      const item: PublicItem = {
        name: "SyntheticFacade",
        kind: "struct",
        module_path: "synthetic::SyntheticFacade",
        signature: { kind: "unit" },
        source_path: `rust/crates/${packageName}/src/lib.rs`,
        source_line: 1,
      };
      return {
        profile: profile.name,
        target: entry.target === "host" ? "synthetic-host" : entry.target,
        features: normalizedFeatures(entry.features),
        profile_blake3: profileBlake3,
        public_items: [item],
        rustdoc: {
          path: `synthetic/${profile.name}/${packageName}.json`,
          blake3: `synthetic-json-${profile.name}-${packageName}`,
          source_blake3: `synthetic-source-${packageName}`,
          source_revision: sourceRevision,
          profile_blake3: profileBlake3,
        },
      } satisfies Graph;
    });
    return { package_name: packageName, analysis_mode: "rustdoc-json", graphs };
  });
  return {
    schema_version: 1,
    source_revision: sourceRevision,
    crates,
    profiles: manifest.profiles.map(profile => ({ profile, complete: true, missing_packages: [], unresolved_packages: [] })),
  };
}

async function loadProfileManifest(): Promise<ProfileManifest> {
  return JSON.parse(await readFile(join(root, "docs/rustdoc-profiles.json"), "utf8")) as ProfileManifest;
}

test("synthetic validator fixture covers required package/facade inventory and strict graph metadata", async () => {
  const manifest = await loadProfileManifest();
  const fixture = syntheticStrictBundle(manifest);
  expect(() => validateStrictDocsBundle(fixture, manifest, "synthetic validator fixture")).not.toThrow();

  const missingPackage = structuredClone(fixture);
  missingPackage.crates = missingPackage.crates?.slice(1);
  expect(() => validateStrictDocsBundle(missingPackage, manifest, "synthetic validator fixture")).toThrow(/required package\/facade/);

  const missingSourceBinding = structuredClone(fixture);
  const firstGraph = missingSourceBinding.crates?.[0]?.graphs?.[0];
  if (!firstGraph?.public_items?.[0]) throw new Error("synthetic fixture did not contain a graph item");
  firstGraph.public_items[0].module_path = null;
  expect(() => validateStrictDocsBundle(missingSourceBinding, manifest, "synthetic validator fixture")).toThrow(/no module_path/);
});

const liveOutput = process.env.SDK_DOCS_FULL_OUTPUT;
test.skipIf(!liveOutput)("retained real sdk-docs full output has complete package and graph coverage", async () => {
  const outputPath = liveOutput!.toLowerCase().endsWith(".json") ? liveOutput! : join(liveOutput!, "docs.json");
  const manifest = await loadProfileManifest();
  const bundle = JSON.parse(await readFile(outputPath, "utf8")) as unknown;
  validateStrictDocsBundle(bundle, manifest, `real sdk-docs output at ${outputPath}`);
});
