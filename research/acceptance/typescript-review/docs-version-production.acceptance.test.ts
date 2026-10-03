import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  createDocsReleaseArchive,
  loadDocsVersion,
  referenceBundleSha256,
  setDocsVersionData,
  type DocsVersionIdentity,
  type ReferenceBundle,
} from "../../../src/lib/docs/versions";

const root = (path: string) => fileURLToPath(new URL(`../../../${path}`, import.meta.url));

const qualifiedReleaseBundle = (revision: string): ReferenceBundle => ({
  sourceRevision: revision,
  source: { revision, channel: "release", status: "qualified" },
  guides: [{ path: "guide.md", contents: "release guide" }],
  snippets: [{ path: "example.ts", contents: "export {};" }],
  packages: [{ name: "acyclic-sdk", version: "1.0.0" }],
});

test("archive loader currently accepts a release-qualified bundle relabeled as preview", async () => {
  const bundle = qualifiedReleaseBundle("1".repeat(40));
  const release = await createDocsReleaseArchive("1.2.3", bundle);
  const relabeled = {
    ...release,
    version: "preview",
    channel: "preview" as const,
  };
  const preview: DocsVersionIdentity = {
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
  const resolved = await loadDocsVersion("preview");
  expect(resolved?.bundle.source).toMatchObject({ channel: "release", status: "qualified" });
  expect(await referenceBundleSha256(resolved!.bundle)).toBe(release.bundleSha256);
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
