import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const cliWebsiteRoot = process.argv.find((argument) => argument.startsWith("--website-root="))?.slice("--website-root=".length);
const websiteRoot = cliWebsiteRoot ?? process.env.ACYC_DOCS_WEBSITE_ROOT ?? "Q:/sdk/work/rust-sdk-docs-website";
const root = (path: string) => join(websiteRoot, path);
const websiteVersions = () => import(pathToFileURL(root("src/lib/docs/versions.ts")).href);

const qualifiedReleaseBundle = (revision: string) => ({
  schemaVersion: "sdk-docs-bundle.v1",
  source: {
    repository: "https://github.com/acyclic-labs/sdk",
    revision,
    generator: "fixture",
    generatorVersion: "fixture",
    channel: "release",
    releaseQualification: {
      schema: "acyclic.sdk.docs.release-qualification.v1",
      version: "1.2.3",
      tag: "v1.2.3",
      revision,
      qualified: true,
    },
  },
  families: [{
    slug: "actors",
    title: "Actors",
    crate: "acyclic-actors",
    version: null,
    maturity: null,
    deployment: null,
    qualification: null,
    summary: "fixture",
    items: [],
  }],
});

test("archive loader rejects a release-qualified bundle relabeled as preview", async () => {
  const { createDocsReleaseArchive, loadDocsVersion, referenceBundleSha256, setDocsVersionData } = await websiteVersions();
  const bundle = qualifiedReleaseBundle("1".repeat(40));
  const release = await createDocsReleaseArchive("1.2.3", bundle);
  const { tag: _releaseTag, ...releaseWithoutTag } = release;
  const relabeled = {
    ...releaseWithoutTag,
    version: "preview",
    channel: "preview" as const,
  };
  const preview = {
    version: "preview",
    channel: "preview",
    revision: release.revision,
    bundleSha256: release.bundleSha256,
    archive: "releases/preview.json",
  };

  setDocsVersionData(
    { schemaVersion: 1, releases: [], preview },
    { "preview.json": relabeled },
  );
  await expect(loadDocsVersion("preview")).rejects.toThrow("archive source channel does not match preview");
  expect(await referenceBundleSha256(release.bundle)).toBe(release.bundleSha256);
});

test("browser bundle embeds the cataloged preview archive and preserves the optional Vite glob branch", async () => {
  const catalog = JSON.parse(await readFile(root("src/lib/generated/docs-versions.json"), "utf8")) as {
    preview: { bundleSha256: string };
  };
  const out = await mkdtemp(join(tmpdir(), "acyclic-docs-router-"));
  try {
    const result = await Bun.build({
      entrypoints: [root("src/lib/docs/versions.ts")],
      target: "browser",
      outdir: out,
      minify: false,
    });
    expect(result.success).toBe(true);
    const output = result.outputs[0]?.path;
    expect(output).toBeDefined();
    const javascript = await readFile(output!, "utf8");
    expect(javascript).toContain(catalog.preview.bundleSha256);
    expect(javascript).toContain("import.meta.glob");
  } finally {
    await rm(out, { recursive: true, force: true });
  }
});
