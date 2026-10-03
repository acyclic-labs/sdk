import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { cp, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../../", import.meta.url));

function sha256(bytes: Buffer | string) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function runDocs(
  repositoryRoot: string,
  bundleRoot: string,
  outputRoot: string,
  scratch: string,
  docsManifestRoot: string,
  authorityPath: string,
  sourceRevision: string,
  authoritySha256: string,
) {
  return spawnSync("cargo", [
    "run", "--manifest-path", join(docsManifestRoot, "Cargo.toml"),
    "--locked", "--offline", "--quiet", "--",
    "--repo-root", repositoryRoot,
    "--output", join(outputRoot, "bundle.json"),
    "--website-output", join(outputRoot, "website.json"),
    "--examples-bundle", bundleRoot,
    "--source-authority", authorityPath,
    "--source-authority-sha256", authoritySha256,
    "--source-revision", sourceRevision,
  ], {
    cwd: root,
    encoding: "utf8",
    env: { ...process.env, CARGO_TARGET_DIR: join(scratch, "sdk-docs-target") },
  });
}

async function initializeRepository(repositoryRoot: string): Promise<string> {
  const init = spawnSync("git", ["init", "--quiet"], { cwd: repositoryRoot, encoding: "utf8" });
  if (init.status !== 0) throw new Error(`unable to initialize fixture repository: ${init.stderr || init.stdout}`);
  for (const [key, value] of [["user.email", "acceptance@example.invalid"], ["user.name", "Acceptance Fixture"]]) {
    const config = spawnSync("git", ["config", key, value], { cwd: repositoryRoot, encoding: "utf8" });
    if (config.status !== 0) throw new Error(`unable to configure fixture repository: ${config.stderr || config.stdout}`);
  }
  const add = spawnSync("git", ["add", "."], { cwd: repositoryRoot, encoding: "utf8" });
  if (add.status !== 0) throw new Error(`unable to stage fixture repository: ${add.stderr || add.stdout}`);
  const commit = spawnSync("git", ["commit", "--quiet", "--message", "immutable fixture baseline"], {
    cwd: repositoryRoot, encoding: "utf8",
  });
  if (commit.status !== 0) throw new Error(`unable to commit fixture repository: ${commit.stderr || commit.stdout}`);
  const revision = spawnSync("git", ["rev-parse", "HEAD"], { cwd: repositoryRoot, encoding: "utf8" });
  if (revision.status !== 0) throw new Error(`unable to resolve fixture repository revision: ${revision.stderr || revision.stdout}`);
  return revision.stdout.trim();
}

async function writeSourceAuthority(repositoryRoot: string, authorityPath: string, sourceRevision: string) {
  const relative = "rust/crates/sdk-examples/src/lib.rs";
  const bytes = await readFile(join(repositoryRoot, relative));
  await writeFile(authorityPath, JSON.stringify({
    schema: "acyclic.sdk.examples.source-authority.v1",
    source_revision: sourceRevision,
    source_path: relative,
    source_sha256: sha256(bytes),
    source_files: [relative],
    source_file_hashes: { [relative]: sha256(bytes) },
  }, null, 2) + "\n");
}

async function stageDocsCrate(scratch: string) {
  const docsManifestRoot = join(scratch, "sdk-docs");
  await cp(join(root, "rust/crates/sdk-docs"), docsManifestRoot, { recursive: true });
  const lock = spawnSync("cargo", [
    "generate-lockfile", "--manifest-path", join(docsManifestRoot, "Cargo.toml"), "--offline",
  ], { cwd: root, encoding: "utf8" });
  if (lock.status !== 0) {
    throw new Error(`unable to stage disposable sdk-docs lockfile: ${lock.stderr || lock.stdout}`);
  }
  return docsManifestRoot;
}

async function writeFabricatedBundle(
  repositoryRoot: string,
  bundleRoot: string,
  declaredSourceHash?: string,
  validArtifact = false,
  boundOutput = false,
  sourceRevision = "website-gate-negative-revision",
) {
  const sourcePath = join(repositoryRoot, "rust/crates/sdk-examples/src/lib.rs");
  const sourceBytes = await readFile(sourcePath);
  const sourceHash = sha256(sourceBytes);
  const manifestSourceHash = declaredSourceHash ?? sourceHash;
  const snippetCode = "// fabricated receipt fixture\n";
  const artifactPath = validArtifact ? "artifacts/fabricated-package.tgz" : "C:/does-not-exist/fabricated-package.tgz";
  const artifactBytes = Buffer.from("bound artifact bytes\n");
  const outputBytes = Buffer.from("bound output evidence\n");
  const outputPath = "artifacts/fabricated-stdout.log";
  const stderrPath = "artifacts/fabricated-stderr.log";
  await mkdir(join(bundleRoot, "snippets/fabricated"), { recursive: true });
  if (validArtifact) {
    await mkdir(join(bundleRoot, "artifacts"), { recursive: true });
    await writeFile(join(bundleRoot, artifactPath), artifactBytes);
  }
  if (boundOutput) {
    await mkdir(join(bundleRoot, "artifacts"), { recursive: true });
    await writeFile(join(bundleRoot, outputPath), outputBytes);
    await writeFile(join(bundleRoot, stderrPath), Buffer.alloc(0));
  }
  await writeFile(join(bundleRoot, "snippets/fabricated/rust.rs"), snippetCode);
  await writeFile(join(bundleRoot, "sdk-examples-manifest.json"), JSON.stringify({
    schema: "acyclic.sdk.examples.bundle.v1",
    generator: "acyclic-sdk-examples@0.2.0",
    source: {
      revision: sourceRevision,
      path: "rust/crates/sdk-examples/src/lib.rs",
      // Keep source identity valid so this fixture isolates receipt/artifact
      // qualification checks below.
      sha256: manifestSourceHash,
    },
    snippets: [{
      id: "fabricated",
      family: "actors",
      title: "Fabricated qualified receipt",
      language: "rust",
      source: "rust/crates/sdk-examples/src/lib.rs",
      source_sha256: manifestSourceHash,
      capability: "supported",
      validation: {
        declared_level: "executed",
        declared_status: "passed",
        evidence: "fabricated test evidence",
        receipt: {
          status: "qualified",
          command: "cargo test",
          // Empty stdout/stderr hashes are valid SHA-256 strings but contain no
          // evidence that a consumer emitted or asserted anything.
          stdout_sha256: boundOutput ? sha256(outputBytes) : sha256(Buffer.alloc(0)),
          stderr_sha256: sha256(Buffer.alloc(0)),
          ...(boundOutput ? {
            stdout_path: outputPath,
            stderr_path: stderrPath,
            assertion_count: 1,
          } : {}),
          source_revision: sourceRevision,
          source_path: "rust/crates/sdk-examples/src/lib.rs",
          source_sha256: manifestSourceHash,
          artifact: {
            kind: "generated-package",
            path: artifactPath,
            sha256: validArtifact ? sha256(artifactBytes) : sha256("fabricated artifact"),
          },
        },
      },
      path: "snippets/fabricated/rust.rs",
      code_sha256: sha256(snippetCode),
    }],
    files: [
      "snippets/fabricated/rust.rs",
      ...(validArtifact ? [artifactPath] : []),
      ...(boundOutput ? [outputPath] : []),
      ...(boundOutput ? [stderrPath] : []),
    ],
    // Keep this value live so the fixture proves the repository source exists;
    // it is intentionally not used as the declared source identity above.
    source_bytes_sha256_for_test_only: sha256(sourceBytes),
  }, null, 2) + "\n");
}

test("website gate rejects fabricated qualified receipt metadata", async () => {
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-website-gate-negative-"));
  try {
    const repositoryRoot = join(scratch, "repo");
    const bundleRoot = join(scratch, "bundle");
    const outputRoot = join(scratch, "output");
    const authorityPath = join(scratch, "source-authority.json");
    const docsManifestRoot = await stageDocsCrate(scratch);
    await mkdir(join(repositoryRoot, "rust/crates"), { recursive: true });
    await cp(join(root, "rust/crates/objects"), join(repositoryRoot, "rust/crates/objects"), { recursive: true });
    await mkdir(join(repositoryRoot, "rust/crates/sdk-examples/src"), { recursive: true });
    await cp(
      join(root, "rust/crates/sdk-examples/src/lib.rs"),
      join(repositoryRoot, "rust/crates/sdk-examples/src/lib.rs"),
    );
    await mkdir(outputRoot, { recursive: true });
    const sourceRevision = await initializeRepository(repositoryRoot);
    await writeFabricatedBundle(repositoryRoot, bundleRoot, undefined, false, false, sourceRevision);
    await writeSourceAuthority(repositoryRoot, authorityPath, sourceRevision);
    const authoritySha256 = sha256(await readFile(authorityPath));

    const result = runDocs(repositoryRoot, bundleRoot, outputRoot, scratch, docsManifestRoot, authorityPath, sourceRevision, authoritySha256);
    expect(result.status).not.toBe(0);
    expect(`${result.stderr}\n${result.stdout}`)
      .toContain("has unsafe artifact path: C:/does-not-exist/fabricated-package.tgz");
    let websiteExists = true;
    try {
      await readFile(join(outputRoot, "website.json"));
    } catch {
      websiteExists = false;
    }
    expect(websiteExists).toBe(false);
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 300_000);

test("website gate rejects source SHA-256 drift before receipt projection", async () => {
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-website-source-drift-"));
  try {
    const repositoryRoot = join(scratch, "repo");
    const bundleRoot = join(scratch, "bundle");
    const outputRoot = join(scratch, "output");
    const authorityPath = join(scratch, "source-authority.json");
    const docsManifestRoot = await stageDocsCrate(scratch);
    await mkdir(join(repositoryRoot, "rust/crates"), { recursive: true });
    await cp(join(root, "rust/crates/objects"), join(repositoryRoot, "rust/crates/objects"), { recursive: true });
    await mkdir(join(repositoryRoot, "rust/crates/sdk-examples/src"), { recursive: true });
    await cp(
      join(root, "rust/crates/sdk-examples/src/lib.rs"),
      join(repositoryRoot, "rust/crates/sdk-examples/src/lib.rs"),
    );
    await mkdir(outputRoot, { recursive: true });
    const sourceRevision = await initializeRepository(repositoryRoot);
    await writeFabricatedBundle(repositoryRoot, bundleRoot, "sha256:declared-but-wrong", false, false, sourceRevision);
    await writeSourceAuthority(repositoryRoot, authorityPath, sourceRevision);
    const authoritySha256 = sha256(await readFile(authorityPath));

    const result = runDocs(repositoryRoot, bundleRoot, outputRoot, scratch, docsManifestRoot, authorityPath, sourceRevision, authoritySha256);
    expect(result.status).not.toBe(0);
    expect(`${result.stderr}\n${result.stdout}`).toContain("source sha256 mismatch");
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 300_000);

test("website gate rejects qualified receipts with empty output evidence", async () => {
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-website-empty-output-"));
  try {
    const repositoryRoot = join(scratch, "repo");
    const bundleRoot = join(scratch, "bundle");
    const outputRoot = join(scratch, "output");
    const authorityPath = join(scratch, "source-authority.json");
    const docsManifestRoot = await stageDocsCrate(scratch);
    await mkdir(join(repositoryRoot, "rust/crates"), { recursive: true });
    await cp(join(root, "rust/crates/objects"), join(repositoryRoot, "rust/crates/objects"), { recursive: true });
    await mkdir(join(repositoryRoot, "rust/crates/sdk-examples/src"), { recursive: true });
    await cp(
      join(root, "rust/crates/sdk-examples/src/lib.rs"),
      join(repositoryRoot, "rust/crates/sdk-examples/src/lib.rs"),
    );
    await mkdir(outputRoot, { recursive: true });
    const sourceRevision = await initializeRepository(repositoryRoot);
    await writeFabricatedBundle(repositoryRoot, bundleRoot, undefined, true, false, sourceRevision);
    await writeSourceAuthority(repositoryRoot, authorityPath, sourceRevision);
    const authoritySha256 = sha256(await readFile(authorityPath));

    const result = runDocs(repositoryRoot, bundleRoot, outputRoot, scratch, docsManifestRoot, authorityPath, sourceRevision, authoritySha256);
    expect(result.status).not.toBe(0);
    expect(`${result.stderr}\n${result.stdout}`)
      .toContain("has no stdout path");
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 300_000);

test("website gate accepts a clean authority-bound bundle and emits its authority digest", async () => {
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-website-authority-positive-"));
  try {
    const repositoryRoot = join(scratch, "repo");
    const bundleRoot = join(scratch, "bundle");
    const outputRoot = join(scratch, "output");
    const authorityPath = join(scratch, "source-authority.json");
    const docsManifestRoot = await stageDocsCrate(scratch);
    await mkdir(join(repositoryRoot, "rust/crates"), { recursive: true });
    await cp(join(root, "rust/crates/objects"), join(repositoryRoot, "rust/crates/objects"), { recursive: true });
    await mkdir(join(repositoryRoot, "rust/crates/sdk-examples/src"), { recursive: true });
    await cp(
      join(root, "rust/crates/sdk-examples/src/lib.rs"),
      join(repositoryRoot, "rust/crates/sdk-examples/src/lib.rs"),
    );
    await mkdir(outputRoot, { recursive: true });
    const sourceRevision = await initializeRepository(repositoryRoot);
    await writeSourceAuthority(repositoryRoot, authorityPath, sourceRevision);
    await writeFabricatedBundle(repositoryRoot, bundleRoot, undefined, true, true, sourceRevision);
    const authoritySha256 = sha256(await readFile(authorityPath));

    const result = runDocs(repositoryRoot, bundleRoot, outputRoot, scratch, docsManifestRoot, authorityPath, sourceRevision, authoritySha256);
    expect(result.status).toBe(0);
    const website = JSON.parse(await readFile(join(outputRoot, "website.json"), "utf8")) as {
      scenarioBundle: { source: { source_authority_sha256: string } };
    };
    expect(website.scenarioBundle.source.source_authority_sha256)
      .toBe(sha256(await readFile(authorityPath)));
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 300_000);

test("website gate rejects a caller-mutated authority file under the same revision", async () => {
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-website-mutable-authority-"));
  try {
    const repositoryRoot = join(scratch, "repo");
    const bundleRoot = join(scratch, "bundle");
    const outputRoot = join(scratch, "output");
    const authorityPath = join(scratch, "source-authority.json");
    const docsManifestRoot = await stageDocsCrate(scratch);
    await mkdir(join(repositoryRoot, "rust/crates"), { recursive: true });
    await cp(join(root, "rust/crates/objects"), join(repositoryRoot, "rust/crates/objects"), { recursive: true });
    await mkdir(join(repositoryRoot, "rust/crates/sdk-examples/src"), { recursive: true });
    await cp(join(root, "rust/crates/sdk-examples/src/lib.rs"), join(repositoryRoot, "rust/crates/sdk-examples/src/lib.rs"));
    await mkdir(outputRoot, { recursive: true });
    const sourceRevision = await initializeRepository(repositoryRoot);
    await writeSourceAuthority(repositoryRoot, authorityPath, sourceRevision);
    const trustedAuthoritySha256 = sha256(await readFile(authorityPath));
    const authority = JSON.parse(await readFile(authorityPath, "utf8")) as Record<string, unknown>;
    authority.caller_mutation = "same revision, changed authority bytes";
    await writeFile(authorityPath, JSON.stringify(authority, null, 2) + "\n");
    await writeFabricatedBundle(repositoryRoot, bundleRoot, undefined, true, true, sourceRevision);

    const result = runDocs(repositoryRoot, bundleRoot, outputRoot, scratch, docsManifestRoot, authorityPath, sourceRevision, trustedAuthoritySha256);
    expect(result.status).not.toBe(0);
    expect(`${result.stderr}\n${result.stdout}`).toContain("source authority digest mismatch");
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 300_000);

test("website gate rejects a forged source closure when source and receipt are edited together", async () => {
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-forged-source-closure-"));
  try {
    const repositoryRoot = join(scratch, "repo");
    const bundleRoot = join(scratch, "bundle");
    const outputRoot = join(scratch, "output");
    const authorityPath = join(scratch, "source-authority.json");
    const docsManifestRoot = await stageDocsCrate(scratch);
    await mkdir(join(repositoryRoot, "rust/crates"), { recursive: true });
    await cp(join(root, "rust/crates/objects"), join(repositoryRoot, "rust/crates/objects"), { recursive: true });
    await mkdir(join(repositoryRoot, "rust/crates/sdk-examples/src"), { recursive: true });
    const sourcePath = join(repositoryRoot, "rust/crates/sdk-examples/src/lib.rs");
    await cp(join(root, "rust/crates/sdk-examples/src/lib.rs"), sourcePath);
    const sourceRevision = await initializeRepository(repositoryRoot);
    await writeSourceAuthority(repositoryRoot, authorityPath, sourceRevision);
    const authoritySha256 = sha256(await readFile(authorityPath));
    await writeFile(sourcePath, `${await readFile(sourcePath, "utf8")}\n// forged source closure\n`);
    await mkdir(outputRoot, { recursive: true });
    await writeFabricatedBundle(repositoryRoot, bundleRoot, undefined, true, true, sourceRevision);

    const result = runDocs(repositoryRoot, bundleRoot, outputRoot, scratch, docsManifestRoot, authorityPath, sourceRevision, authoritySha256);
    expect(result.status).not.toBe(0);
    expect(`${result.stderr}\n${result.stdout}`).toContain("immutable source/revision closure mismatch");
    let websiteExists = true;
    try {
      await readFile(join(outputRoot, "website.json"));
    } catch {
      websiteExists = false;
    }
    expect(websiteExists).toBe(false);
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 300_000);
