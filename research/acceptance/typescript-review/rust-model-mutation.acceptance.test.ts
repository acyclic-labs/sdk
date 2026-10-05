import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { cp, mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../../", import.meta.url));
async function copyRustCrate(name: string, scratch: string): Promise<string> {
  const source = join(root, "rust/crates", name);
  const destination = join(scratch, "rust/crates", name);
  await mkdir(join(destination, "src"), { recursive: true });
  await cp(join(source, "src"), join(destination, "src"), { recursive: true });
  for (const file of ["Cargo.toml", "Cargo.lock"]) {
    await cp(join(source, file), join(destination, file));
  }
  return destination;
}

function runGeneratorMode(manifestPath: string, output: string, cwd: string, mode: "write" | "check") {
  return spawnSync("cargo", [
    "run", "--manifest-path", manifestPath, "--locked", "--quiet", "--", mode, output,
  ], {
    cwd,
    encoding: "utf8",
    env: { ...process.env, CARGO_TARGET_DIR: join(cwd, "target") },
  });
}

function runGenerator(manifestPath: string, output: string, cwd: string) {
  return runGeneratorMode(manifestPath, output, cwd, "write");
}

function runProductionGenerator(scratch: string) {
  return spawnSync("bun", ["scripts/generate-typescript-packages.mjs", "write"], {
    cwd: scratch,
    encoding: "utf8",
    env: { ...process.env, CARGO_TARGET_DIR: join(scratch, "target") },
  });
}

function regenerateScratchLock(scratch: string) {
  return spawnSync("cargo", [
    "generate-lockfile", "--manifest-path", "rust/crates/sdk-typescript/Cargo.toml", "--offline",
  ], {
    cwd: scratch,
    encoding: "utf8",
  });
}

async function stageGeneratorInputs(scratch: string) {
  await Promise.all([
    copyRustCrate("sdk-typescript", scratch),
    copyRustCrate("sdk-contract-wire", scratch),
    copyRustCrate("sdk-contract-options", scratch),
    copyRustCrate("sdk-contract-validation", scratch),
  ]);
  await mkdir(join(scratch, "rust/crates/sdk-contract-wire/tests/fixtures"), { recursive: true });
  await mkdir(join(scratch, "rust/crates/actors/src/generated"), { recursive: true });
  await mkdir(join(scratch, "rust/crates/workers/src/generated"), { recursive: true });
  await mkdir(join(scratch, "rust/crates/objects/src/generated"), { recursive: true });
  await mkdir(join(scratch, "rust/crates/machines/src/generated"), { recursive: true });
  await mkdir(join(scratch, "rust/crates/filesystem/src/generated"), { recursive: true });
  await mkdir(join(scratch, "rust/crates/harness/src/generated"), { recursive: true });
  await mkdir(join(scratch, "rust/crates/stream/proto/stream/v2"), { recursive: true });
  await mkdir(join(scratch, "scripts"), { recursive: true });
  await mkdir(join(scratch, "typescript/packages/objects/src"), { recursive: true });
  await mkdir(join(scratch, "typescript/packages/stream/src"), { recursive: true });
  await cp(
    join(root, "scripts/generate-typescript-packages.mjs"),
    join(scratch, "scripts/generate-typescript-packages.mjs"),
  );
  await Promise.all([
    cp(
      join(root, "rust/crates/sdk-contract-wire/tests/fixtures/filesystem-v2.descriptor.bin"),
      join(scratch, "rust/crates/sdk-contract-wire/tests/fixtures/filesystem-v2.descriptor.bin"),
    ),
    cp(
      join(root, "rust/crates/sdk-contract-wire/tests/fixtures/harness-v2.descriptor.bin"),
      join(scratch, "rust/crates/sdk-contract-wire/tests/fixtures/harness-v2.descriptor.bin"),
    ),
    cp(
      join(root, "rust/crates/inference/inference_descriptor.bin"),
      join(scratch, "rust/crates/inference/inference_descriptor.bin"),
    ),
    cp(
      join(root, "rust/crates/actors/src/generated/acyclic-actors-v1.bin"),
      join(scratch, "rust/crates/actors/src/generated/acyclic-actors-v1.bin"),
    ),
    cp(
      join(root, "rust/crates/workers/src/generated/acyclic-workers-v1.bin"),
      join(scratch, "rust/crates/workers/src/generated/acyclic-workers-v1.bin"),
    ),
    cp(
      join(root, "rust/crates/objects/src/generated/acyclic-objects-v2.bin"),
      join(scratch, "rust/crates/objects/src/generated/acyclic-objects-v2.bin"),
    ),
    cp(
      join(root, "rust/crates/machines/src/generated/acyclic-machines-v1.bin"),
      join(scratch, "rust/crates/machines/src/generated/acyclic-machines-v1.bin"),
    ),
    cp(
      join(root, "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin"),
      join(scratch, "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin"),
    ),
    cp(
      join(root, "rust/crates/harness/src/generated/harness-archived-v2.bin"),
      join(scratch, "rust/crates/harness/src/generated/harness-archived-v2.bin"),
    ),
    cp(
      join(root, "rust/crates/stream/proto/stream/v2/stream_descriptor.bin"),
      join(scratch, "rust/crates/stream/proto/stream/v2/stream_descriptor.bin"),
    ),
  ]);
  const lock = regenerateScratchLock(scratch);
  if (lock.status !== 0) throw new Error(`scratch lock generation failed (${lock.status}): ${lock.stderr}`);
}

async function packGeneratedSource(scratch: string, source: string, family: "objects" | "stream") {
  const packageDir = join(scratch, `package-${family}`);
  const packDir = join(scratch, "packs");
  await mkdir(packageDir, { recursive: true });
  await mkdir(packDir, { recursive: true });
  await cp(source, join(packageDir, "generated-client.ts"));
  await writeFile(join(packageDir, "package.json"), JSON.stringify({
    name: `regenerated-${family}`,
    version: "0.0.0",
    type: "module",
    exports: { ".": "./generated-client.ts" },
    files: ["generated-client.ts", "package.json"],
  }));
  const packed = spawnSync("bun", ["pm", "pack", "--destination", packDir], {
    cwd: packageDir,
    encoding: "utf8",
  });
  if (packed.status !== 0) throw new Error(`packing ${family} metadata failed (${packed.status}): ${packed.stderr}`);
  const archive = (await readdir(packDir)).find(name => name.endsWith(".tgz") && name.startsWith(`regenerated-${family}-`));
  if (archive === undefined) throw new Error(`packing ${family} metadata produced no archive`);
  return join(packDir, archive);
}

test("disposable Rust model and policy mutations reach installed TS packages only through regeneration", async () => {
  const objectsModelPath = join(root, "rust/crates/sdk-contract-wire/src/objects.rs");
  const streamModelPath = join(root, "rust/crates/sdk-contract-wire/src/stream.rs");
  const checkedInObjectsFacade = await readFile(join(root, "typescript/packages/objects/src/generated-client.ts"), "utf8");
  const checkedInStreamFacade = await readFile(join(root, "typescript/packages/stream/src/generated-client.ts"), "utf8");
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-rust-model-mutation-"));
  try {
    const baselineOutput = join(scratch, "baseline");
    await Promise.all([
      copyRustCrate("sdk-typescript", scratch),
      copyRustCrate("sdk-contract-wire", scratch),
      copyRustCrate("sdk-contract-options", scratch),
      copyRustCrate("sdk-contract-validation", scratch),
    ]);
    await mkdir(join(scratch, "rust/crates/sdk-contract-wire/tests/fixtures"), { recursive: true });
    await Promise.all([
      mkdir(join(scratch, "rust/crates/actors/src/generated"), { recursive: true }),
      mkdir(join(scratch, "rust/crates/workers/src/generated"), { recursive: true }),
      mkdir(join(scratch, "rust/crates/objects/src/generated"), { recursive: true }),
      mkdir(join(scratch, "rust/crates/stream/proto/stream/v2"), { recursive: true }),
      mkdir(join(scratch, "rust/crates/machines/src/generated"), { recursive: true }),
      mkdir(join(scratch, "rust/crates/filesystem/src/generated"), { recursive: true }),
      mkdir(join(scratch, "rust/crates/harness/src/generated"), { recursive: true }),
      mkdir(join(scratch, "scripts"), { recursive: true }),
      mkdir(join(scratch, "typescript/packages/actors/src"), { recursive: true }),
      mkdir(join(scratch, "typescript/packages/workers/src"), { recursive: true }),
      mkdir(join(scratch, "typescript/packages/filesystem/src"), { recursive: true }),
      mkdir(join(scratch, "typescript/packages/harness/src"), { recursive: true }),
      mkdir(join(scratch, "typescript/packages/inference/src"), { recursive: true }),
      mkdir(join(scratch, "typescript/packages/machines/src"), { recursive: true }),
      mkdir(join(scratch, "typescript/packages/objects/src"), { recursive: true }),
      mkdir(join(scratch, "typescript/packages/stream/src"), { recursive: true }),
    ]);
    await cp(
      join(root, "scripts/generate-typescript-packages.mjs"),
        join(scratch, "scripts/generate-typescript-packages.mjs"),
      );
    await Promise.all([
      cp(
        join(root, "rust/crates/sdk-contract-wire/tests/fixtures/filesystem-v2.descriptor.bin"),
        join(scratch, "rust/crates/sdk-contract-wire/tests/fixtures/filesystem-v2.descriptor.bin"),
      ),
      cp(
        join(root, "rust/crates/sdk-contract-wire/tests/fixtures/harness-v2.descriptor.bin"),
        join(scratch, "rust/crates/sdk-contract-wire/tests/fixtures/harness-v2.descriptor.bin"),
      ),
      cp(
        join(root, "rust/crates/inference/inference_descriptor.bin"),
        join(scratch, "rust/crates/inference/inference_descriptor.bin"),
      ),
      cp(
        join(root, "rust/crates/actors/src/generated/acyclic-actors-v1.bin"),
        join(scratch, "rust/crates/actors/src/generated/acyclic-actors-v1.bin"),
      ),
      cp(
        join(root, "rust/crates/workers/src/generated/acyclic-workers-v1.bin"),
        join(scratch, "rust/crates/workers/src/generated/acyclic-workers-v1.bin"),
      ),
      cp(
        join(root, "rust/crates/objects/src/generated/acyclic-objects-v2.bin"),
        join(scratch, "rust/crates/objects/src/generated/acyclic-objects-v2.bin"),
      ),
      cp(
        join(root, "rust/crates/stream/proto/stream/v2/stream_descriptor.bin"),
        join(scratch, "rust/crates/stream/proto/stream/v2/stream_descriptor.bin"),
      ),
      cp(
        join(root, "rust/crates/machines/src/generated/acyclic-machines-v1.bin"),
        join(scratch, "rust/crates/machines/src/generated/acyclic-machines-v1.bin"),
      ),
      cp(
        join(root, "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin"),
        join(scratch, "rust/crates/filesystem/src/generated/acyclic-filesystem-v2.bin"),
      ),
      cp(
        join(root, "rust/crates/harness/src/generated/harness-archived-v2.bin"),
        join(scratch, "rust/crates/harness/src/generated/harness-archived-v2.bin"),
      ),
    ]);
    const lock = regenerateScratchLock(scratch);
    if (lock.status !== 0) throw new Error(`scratch lock generation failed (${lock.status}): ${lock.stderr}`);
    const baseline = runGenerator(
      join(scratch, "rust/crates/sdk-typescript/Cargo.toml"), baselineOutput, scratch,
    );
    if (baseline.status !== 0) throw new Error(`baseline generator failed (${baseline.status}): ${baseline.stderr}`);
    const baselineManifest = JSON.parse(await readFile(join(baselineOutput, "manifest.json"), "utf8")) as {
      services: Array<{ family: string; methods: Array<{ path: string }>; source_content_sha256: string }>;
    };
    expect(baselineManifest.services.map(service => service.family)).toEqual([
      "actors", "workers", "objects", "stream", "inference", "machines", "filesystem", "harness",
    ]);
    const copiedObjectsPath = join(scratch, "rust/crates/sdk-contract-wire/src/objects.rs");
    const copiedStreamPath = join(scratch, "rust/crates/sdk-contract-wire/src/stream.rs");
    const copiedGeneratorPath = join(scratch, "rust/crates/sdk-typescript/src/main.rs");
    const objectsCopy = (await readFile(objectsModelPath, "utf8"))
      .replace('path: "/v2/objects/buckets/create"', 'path: "/v2/objects/buckets/create-mutated"')
      .replaceAll('docs: "Creates a logical bucket."', 'docs: "MUTATED OBJECTS DOC."');
    const streamCopy = (await readFile(streamModelPath, "utf8"))
      .replace('path: "/v1/stream/append"', 'path: "/v1/stream/append-mutated"')
      .replaceAll('docs: "Appends records to a stream with an optional expected tail."', 'docs: "MUTATED STREAM DOC."');
    expect(objectsCopy).toContain("/v2/objects/buckets/create-mutated");
    expect(objectsCopy).toContain("MUTATED OBJECTS DOC.");
    expect(streamCopy).toContain("/v1/stream/append-mutated");
    expect(streamCopy).toContain("MUTATED STREAM DOC.");
    const generatorCopy = (await readFile(join(root, "rust/crates/sdk-typescript/src/main.rs"), "utf8"))
      .replace(
        'protocol: "https-or-loopback-http".to_owned()',
        'protocol: "https-or-loopback-http-mutated".to_owned()',
      );
    expect(generatorCopy).toContain('protocol: "https-or-loopback-http-mutated".to_owned()');
    await writeFile(copiedObjectsPath, objectsCopy);
    await writeFile(copiedStreamPath, streamCopy);
    await writeFile(copiedGeneratorPath, generatorCopy);

    const mutatedOutput = join(scratch, "mutated");
    const mutated = runGenerator(
      join(scratch, "rust/crates/sdk-typescript/Cargo.toml"), mutatedOutput, scratch,
    );
    if (mutated.status !== 0) {
      throw new Error(`mutated generator failed (${mutated.status}): ${mutated.stderr}`);
    }
    const mutatedManifest = JSON.parse(await readFile(join(mutatedOutput, "manifest.json"), "utf8")) as {
      services: Array<{ family: string; methods: Array<{ path: string }>; source_content_sha256: string }>;
    };
    const baselineObjects = baselineManifest.services.find(service => service.family === "objects");
    const mutatedObjects = mutatedManifest.services.find(service => service.family === "objects");
    const baselineStream = baselineManifest.services.find(service => service.family === "stream");
    const mutatedStream = mutatedManifest.services.find(service => service.family === "stream");
    expect(baselineObjects).toBeDefined();
    expect(mutatedObjects).toBeDefined();
    expect(baselineStream).toBeDefined();
    expect(mutatedStream).toBeDefined();
    expect(mutatedObjects!.methods[0].path).toBe("v2/objects/buckets/create-mutated");
    expect(mutatedStream!.methods.find(method => method.path.includes("append-mutated"))).toBeDefined();
    expect(mutatedObjects!.source_content_sha256).not.toBe(baselineObjects!.source_content_sha256);
    expect(mutatedStream!.source_content_sha256).not.toBe(baselineStream!.source_content_sha256);
    expect(await readFile(join(mutatedOutput, "objects-metadata.ts"), "utf8")).toContain("MUTATED OBJECTS DOC.");
    expect(await readFile(join(mutatedOutput, "stream-metadata.ts"), "utf8")).toContain("MUTATED STREAM DOC.");

    // Exercise the checked-in production package path from a disposable clone.
    // The copied script resolves its root from import.meta.dirname, so this
    // proves that a Rust model mutation reaches the package facade through the
    // same generator used by the repository's package scripts.
    const production = runProductionGenerator(scratch);
    if (production.status !== 0) {
      throw new Error(`production package generator failed (${production.status}): ${production.stderr}`);
    }
    const productionObjects = join(scratch, "typescript/packages/objects/src/generated-client.ts");
    const productionStream = join(scratch, "typescript/packages/stream/src/generated-client.ts");
    const productionObjectsSource = await readFile(productionObjects, "utf8");
    const productionStreamSource = await readFile(productionStream, "utf8");
    expect(productionObjectsSource).toContain('path: "v2/objects/buckets/create-mutated"');
    expect(productionObjectsSource).toContain('docs: "MUTATED OBJECTS DOC."');
    expect(productionObjectsSource).toContain('protocol: "https-or-loopback-http-mutated"');
    expect(productionStreamSource).toContain('path: "v1/stream/append-mutated"');
    expect(productionStreamSource).toContain('docs: "MUTATED STREAM DOC."');

    const objectsArchive = await packGeneratedSource(scratch, productionObjects, "objects");
    const streamArchive = await packGeneratedSource(scratch, productionStream, "stream");
    const consumer = join(scratch, "consumer");
    await mkdir(consumer, { recursive: true });
    await writeFile(join(consumer, "package.json"), JSON.stringify({
      name: "regenerated-metadata-consumer",
      private: true,
      type: "module",
      dependencies: {
        "regenerated-objects": `file:${objectsArchive}`,
        "regenerated-stream": `file:${streamArchive}`,
      },
    }));
    await writeFile(join(consumer, "verify.mjs"), [
      'import { OBJECTS_METHODS, OBJECTS_REMOTE_POLICY, OBJECTS_ROUTES, OBJECTS_SOURCE } from "regenerated-objects";',
      'import { STREAM_METHODS, STREAM_ROUTES, STREAM_SOURCE } from "regenerated-stream";',
      'if (OBJECTS_METHODS.createBucket.path !== "v2/objects/buckets/create-mutated" || OBJECTS_ROUTES.createBucket.path !== OBJECTS_METHODS.createBucket.path) throw new Error("installed Objects route did not regenerate");',
      'if (OBJECTS_REMOTE_POLICY.protocol !== "https-or-loopback-http-mutated") throw new Error("installed Objects policy did not regenerate");',
      'if (!OBJECTS_SOURCE.sourceContentSha256) throw new Error("installed Objects provenance is missing");',
      'if (!STREAM_METHODS.append.path.includes("append-mutated") || STREAM_ROUTES.append.path !== STREAM_METHODS.append.path) throw new Error("installed Stream route did not regenerate");',
      'if (!STREAM_SOURCE.sourceContentSha256) throw new Error("installed Stream provenance is missing");',
    ].join("\n"));
    const install = spawnSync("bun", ["install", "--offline", "--ignore-scripts"], {
      cwd: consumer,
      encoding: "utf8",
    });
    if (install.status !== 0) throw new Error(`clean consumer install failed (${install.status}): ${install.stderr}`);
    const consumed = spawnSync("bun", ["verify.mjs"], { cwd: consumer, encoding: "utf8" });
    if (consumed.status !== 0) throw new Error(`installed metadata verification failed (${consumed.status}): ${consumed.stderr}`);
    expect(await readFile(join(root, "typescript/packages/objects/src/generated-client.ts"), "utf8")).toBe(checkedInObjectsFacade);
    expect(await readFile(join(root, "typescript/packages/stream/src/generated-client.ts"), "utf8")).toBe(checkedInStreamFacade);

    // The consumer verifies the actual generated-client.ts artifacts after a
    // clean package installation, including the route aliases and provenance.
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 120_000);

test("generator check is non-mutating and rejects authoritative or generated-only drift", async () => {
  const scratch = await mkdtemp(join(tmpdir(), "acyclic-generator-drift-"));
  try {
    await stageGeneratorInputs(scratch);
    const manifestPath = join(scratch, "rust/crates/sdk-typescript/Cargo.toml");
    const archivedOutput = join(scratch, "archived-baseline");
    const baseline = runGenerator(manifestPath, archivedOutput, scratch);
    if (baseline.status !== 0) throw new Error(`baseline generator failed (${baseline.status}): ${baseline.stderr}`);

    const names = await readdir(archivedOutput);
    const archivedBytes = new Map<string, Buffer>();
    for (const name of names) archivedBytes.set(name, await readFile(join(archivedOutput, name)));

    const clean = runGeneratorMode(manifestPath, archivedOutput, scratch, "check");
    expect(clean.status).toBe(0);
    for (const [name, bytes] of archivedBytes) {
      expect(await readFile(join(archivedOutput, name))).toEqual(bytes);
    }

    const authoritative = join(scratch, "rust/crates/sdk-contract-wire/src/objects.rs");
    const originalAuthoritative = await readFile(authoritative);
    await writeFile(authoritative, Buffer.concat([originalAuthoritative, Buffer.from("\n// adversarial authoritative-input mutation\n")]));
    const changedAuthoritative = runGeneratorMode(manifestPath, archivedOutput, scratch, "check");
    expect(changedAuthoritative.status).not.toBe(0);
    expect(`${changedAuthoritative.stderr}\n${changedAuthoritative.stdout}`).toContain("generated output drift");
    for (const [name, bytes] of archivedBytes) {
      expect(await readFile(join(archivedOutput, name))).toEqual(bytes);
    }
    await writeFile(authoritative, originalAuthoritative);

    const restored = runGeneratorMode(manifestPath, archivedOutput, scratch, "check");
    expect(restored.status).toBe(0);

    const generatedOnly = join(scratch, "generated-only-edit");
    await cp(archivedOutput, generatedOnly, { recursive: true });
    const edited = join(generatedOnly, "objects-metadata.ts");
    await writeFile(edited, `${await readFile(edited, "utf8")}\n// unauthorized generated-output edit\n`);
    const changedGenerated = runGeneratorMode(manifestPath, generatedOnly, scratch, "check");
    expect(changedGenerated.status).not.toBe(0);
    expect(`${changedGenerated.stderr}\n${changedGenerated.stdout}`).toContain("generated output drift");

    const staleOnly = join(scratch, "stale-output");
    await cp(archivedOutput, staleOnly, { recursive: true });
    await writeFile(join(staleOnly, "archived-old-output.ts"), "stale generated output\n");
    const excludedGenerated = runGeneratorMode(manifestPath, staleOnly, scratch, "check");
    expect(excludedGenerated.status).not.toBe(0);
    expect(`${excludedGenerated.stderr}\n${excludedGenerated.stdout}`).toContain("stale generated output");
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}, 120_000);
