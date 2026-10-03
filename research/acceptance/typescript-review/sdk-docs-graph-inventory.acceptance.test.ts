import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
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
    target?: string;
    features?: string[];
    profile_blake3?: string;
  };
};

type CrateBundle = {
  package_name?: string;
  analysis_mode?: string;
  content_blake3?: string;
  sources?: Array<{ path: string; blake3: string; contents: string }>;
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

import { normalizedFeatures, requiredPackages, validateStrictDocsBundle } from "../../../scripts/validate-rustdoc-graphs.mjs";

function syntheticStrictBundle(manifest: ProfileManifest): DocsBundle {
  const sourceRevision = "synthetic-validator-fixture-revision";
  const crates = [...requiredPackages(manifest)].map(packageName => {
    const packageProfiles = manifest.profiles.filter(profile => profile.packages.some(entry => entry.package === packageName));
    const graphs = packageProfiles.map(profile => {
      const entry = profile.packages.find(candidate => candidate.package === packageName)!;
      const profileBlake3 = "b".repeat(64);
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
          blake3: "d".repeat(64),
          source_blake3: "c".repeat(64),
          source_revision: sourceRevision,
          target: entry.target === "host" ? "synthetic-host" : entry.target,
          features: normalizedFeatures(entry.features),
          profile_blake3: profileBlake3,
        },
      } satisfies Graph;
    });
    return {
      package_name: packageName,
      analysis_mode: "rustdoc-json",
      content_blake3: "e".repeat(64),
      sources: [{
        path: `rust/crates/${packageName}/src/lib.rs`,
        blake3: "f".repeat(64),
        contents: "pub struct SyntheticFacade;\n",
      }],
      graphs,
    };
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

  const scratch = await mkdtemp(join(tmpdir(), "sdk-docs-graph-validator-"));
  try {
    const bundlePath = join(scratch, "docs.json");
    await writeFile(bundlePath, JSON.stringify(fixture));
    const cli = spawnSync("node", [
      join(root, "scripts/validate-rustdoc-graphs.mjs"),
      "--bundle", bundlePath,
      "--profiles", join(root, "docs/rustdoc-profiles.json"),
    ], { cwd: root, encoding: "utf8" });
    expect(cli.status, `${cli.stdout}\n${cli.stderr}`).toBe(0);
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }

  const missingPackage = structuredClone(fixture);
  missingPackage.crates = missingPackage.crates?.slice(1);
  expect(() => validateStrictDocsBundle(missingPackage, manifest, "synthetic validator fixture")).toThrow(/required package\/facade/);

  const missingSourceBinding = structuredClone(fixture);
  const firstGraph = missingSourceBinding.crates?.[0]?.graphs?.[0];
  if (!firstGraph?.public_items?.[0]) throw new Error("synthetic fixture did not contain a graph item");
  firstGraph.public_items[0].module_path = null;
  expect(() => validateStrictDocsBundle(missingSourceBinding, manifest, "synthetic validator fixture")).toThrow(/no module_path/);

  const outOfRangeSource = structuredClone(fixture);
  const outOfRangeGraph = outOfRangeSource.crates?.[0]?.graphs?.[0];
  if (!outOfRangeGraph?.public_items?.[0]) throw new Error("synthetic fixture did not contain a source-bound item");
  outOfRangeGraph.public_items[0].source_line = 99;
  expect(() => validateStrictDocsBundle(outOfRangeSource, manifest, "synthetic validator fixture")).toThrow(/exceeds retained source/);
});

const liveOutput = process.env.SDK_DOCS_FULL_OUTPUT;
test.skipIf(!liveOutput)("retained real sdk-docs full output has complete package and graph coverage", async () => {
  const outputPath = liveOutput!.toLowerCase().endsWith(".json") ? liveOutput! : join(liveOutput!, "docs.json");
  const manifest = await loadProfileManifest();
  const bundle = JSON.parse(await readFile(outputPath, "utf8")) as unknown;
  validateStrictDocsBundle(bundle, manifest, `real sdk-docs output at ${outputPath}`);
});
