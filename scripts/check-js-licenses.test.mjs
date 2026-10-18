import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import fc from "fast-check";
import { readFile, readdir } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { assertGraphResult, checkGraph, graphFixture, measure } from "./test-support/license-graphs.mjs";
import { fuzz } from "./check-js-licenses.fuzz.mjs";

const checker = fileURLToPath(new URL("./check-js-licenses.mjs", import.meta.url));

function packageAt(root, path, metadata) {
  const directory = join(root, path);
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, "package.json"), JSON.stringify({ version: "1.0.0", ...metadata }));
  return directory;
}

function linkPackage(root, path, target) {
  const link = join(root, path);
  mkdirSync(dirname(link), { recursive: true });
  symlinkSync(process.platform === "win32" ? target : relative(dirname(link), target), link,
    process.platform === "win32" ? "junction" : "dir");
}

function checkFixture(setup) {
  const root = mkdtempSync(join(tmpdir(), "acyclic-js-licenses-"));
  try {
    mkdirSync(join(root, "scripts"));
    copyFileSync(checker, join(root, "scripts/check-js-licenses.mjs"));
    setup(root);
    const result = spawnSync(process.execPath, [join(root, "scripts/check-js-licenses.mjs")], {
      cwd: root, encoding: "utf8", timeout: 10_000,
    });
    if (result.error) throw result.error;
    expect(result.signal).toBeNull();
    return { status: result.status, errors: result.stderr.trim().split("\n").filter(Boolean) };
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

test("rejects a disallowed direct dependency in Bun's isolated store", () => {
  const result = checkFixture(root => {
    const packageRoot = packageAt(root, "node_modules/.bun/direct@1.0.0/node_modules/direct", {
      name: "direct", license: "GPL-3.0-only",
    });
    linkPackage(root, "node_modules/direct", packageRoot);
  });
  expect(result).toEqual({ status: 1, errors: ["direct@1.0.0: GPL-3.0-only"] });
});

test("checks isolated transitive dependencies that have no root package link", () => {
  const result = checkFixture(root => {
    const direct = packageAt(root, "node_modules/.bun/direct@1.0.0/node_modules/direct", {
      name: "direct", license: "MIT", dependencies: { transitive: "1.0.0" },
    });
    const transitive = packageAt(root, "node_modules/.bun/transitive@1.0.0/node_modules/transitive", {
      name: "transitive", license: "GPL-3.0-only",
    });
    linkPackage(root, "node_modules/direct", direct);
    linkPackage(root, "node_modules/.bun/direct@1.0.0/node_modules/transitive", transitive);
  });
  expect(result).toEqual({ status: 1, errors: ["transitive@1.0.0: GPL-3.0-only"] });
});

test("checks hidden-store packages with no incoming links", () => {
  const result = checkFixture(root => {
    packageAt(root, "node_modules/.bun/unlinked@1.0.0/node_modules/unlinked", {
      name: "unlinked", license: "GPL-3.0-only",
    });
  });
  expect(result).toEqual({ status: 1, errors: ["unlinked@1.0.0: GPL-3.0-only"] });
});

test("checks dependencies beneath a linked private workspace", () => {
  const result = checkFixture(root => {
    const workspace = packageAt(root, "packages/workspace", { name: "workspace", private: true });
    packageAt(root, "packages/workspace/node_modules/nested", { name: "nested", license: "GPL-3.0-only" });
    linkPackage(root, "node_modules/workspace", workspace);
  });
  expect(result).toEqual({ status: 1, errors: ["nested@1.0.0: GPL-3.0-only"] });
});

test("deduplicates linked store entries and terminates on dependency cycles", () => {
  const result = checkFixture(root => {
    const packageRoot = packageAt(root, "node_modules/.bun/direct@1.0.0/node_modules/direct", {
      name: "direct", license: "GPL-3.0-only",
    });
    linkPackage(root, "node_modules/direct", packageRoot);
    linkPackage(root, "node_modules/alias", packageRoot);
    linkPackage(root, "node_modules/.bun/direct@1.0.0/node_modules/direct/node_modules/direct", packageRoot);
  });
  expect(result).toEqual({ status: 1, errors: ["direct@1.0.0: GPL-3.0-only"] });
});

test("keeps ordinary installed-package license failures", () => {
  const result = checkFixture(root => {
    packageAt(root, "node_modules/unknown", { name: "unknown" });
    packageAt(root, "node_modules/disallowed", { name: "disallowed", license: "GPL-3.0-only" });
  });
  expect(result.status).toBe(1);
  expect(result.errors.sort()).toEqual(["disallowed@1.0.0: GPL-3.0-only", "unknown@1.0.0: missing"]);
});

test("ignores internal module-format manifests while checking nested installed packages", () => {
  const result = checkFixture(root => {
    const parent = packageAt(root, "node_modules/@scope/parent", { name: "@scope/parent", license: "MIT" });
    for (const [directory, type] of [["commonjs", "commonjs"], ["esm", "module"]]) {
      const output = join(parent, "dist", directory);
      mkdirSync(output, { recursive: true });
      writeFileSync(join(output, "package.json"), JSON.stringify({ type, sideEffects: false }));
    }
    packageAt(root, "node_modules/@scope/parent/node_modules/nested", { name: "nested", license: "GPL-3.0-only" });
  });
  expect(result).toEqual({ status: 1, errors: ["nested@1.0.0: GPL-3.0-only"] });
});

test("preserves the existing allowlist and private-package exemption", () => {
  const result = checkFixture(root => {
    packageAt(root, "node_modules/allowed", { name: "allowed", license: "MIT" });
    const allowed = packageAt(root, "node_modules/.bun/@scope+allowed@1.0.0/node_modules/@scope/allowed", {
      name: "@scope/allowed", license: "Apache-2.0",
    });
    linkPackage(root, "node_modules/@scope/allowed", allowed);
    packageAt(root, "node_modules/private", { name: "private", private: true });
  });
  expect(result).toEqual({ status: 0, errors: [] });
});

const graphs = fc.uint8Array({ minLength: 1, maxLength: 32 });

test("distinct package directories sharing one physical manifest read it once", async () => {
  const root = mkdtempSync(join(tmpdir(), "acyclic-license-manifest-"));
  try {
    const first = packageAt(root, "node_modules/first", { name: "shared", license: "GPL-3.0-only" });
    mkdirSync(join(root, "node_modules/second"));
    symlinkSync(join(first, "package.json"), join(root, "node_modules/second/package.json"), "file");
    const result = await measure(root);
    expect(result.errors).toEqual(["shared@1.0.0: GPL-3.0-only"]);
    expect(result.reads.length).toBe(1);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("generated graph/path property: exact physical reads, complete errors and no repeated expansions", async () => {
  await fc.assert(fc.asyncProperty(graphs, bytes => checkGraph(bytes)), { numRuns: 64, seed: 979 });
});

test("seed corpus and byte fuzz property cover aliases, cycles, scopes and Unicode paths", async () => {
  const corpus = new URL("./test-support/license-corpus/", import.meta.url);
  for (const name of await readdir(corpus)) {
    await fuzz(Uint8Array.from(JSON.parse(await readFile(new URL(name, corpus), "utf8"))));
  }
  await fc.assert(fc.asyncProperty(graphs, bytes => fuzz(bytes)), { numRuns: 64, seed: 357413 });
});

async function withMutant(rewrite, check) {
  const root = mkdtempSync(join(tmpdir(), "acyclic-license-mutant-"));
  try {
    const path = join(root, "checker.mjs");
    writeFileSync(path, rewrite(await readFile(checker, "utf8")));
    await check((await import(pathToFileURL(path).href)).auditLicenses);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

test("negative control: omitting canonical deduplication violates the same exact work assertions", async () => {
  await withMutant(source => source
    .replace("if (visited.has(key)) continue;", "")
    .replace("!manifests.has(manifest)", "true"), async audit => {
      const fixture = graphFixture(Uint8Array.of(3, 0, 1, 2, 3), true);
      try {
        const result = await measure(fixture.root, audit);
        expect(() => assertGraphResult(fixture, result)).toThrow();
        expect(result.reads.length).toBeGreaterThan(fixture.count);
        expect(new Set(result.directories).size).toBeLessThan(result.directories.length);
      } finally {
        rmSync(fixture.root, { recursive: true, force: true });
      }
    });
});

test("negative control: omitting hidden store discovery fails and shrinks the graph property", async () => {
  await withMutant(source => source.replace('entry.name === ".bun"', 'entry.name === ".never"'), async audit => {
    const result = await fc.check(fc.asyncProperty(graphs, bytes => checkGraph(bytes, audit)), { numRuns: 32, seed: 979 });
    expect(result.failed).toBe(true);
    expect(result.counterexample).not.toBeNull();
  });
});

test("paired alias chains: canonical traversal reads exactly N manifests while the former glob repeats reads", async () => {
  for (const count of [2, 4, 8]) {
    // Independent installations: no decoder, manifest cache or fixture state is shared.
    const candidate = graphFixture(Uint8Array.of(count - 1), true);
    const baseline = graphFixture(Uint8Array.of(count - 1), true);
    try {
      const candidateStarted = performance.now();
      const result = await measure(candidate.root);
      const candidateMs = performance.now() - candidateStarted;
      const errors = new Set(), identities = new Set(), bytesByIdentity = new Map();
      let reads = 0;
      const started = performance.now();
      for await (const path of new Bun.Glob("node_modules/**/package.json").scan({
        cwd: baseline.root, onlyFiles: true, dot: true, followSymlinks: true,
      })) {
        if (!/(?:^|[\\/])node_modules[\\/](?:@[^\\/]+[\\/])?[^@.\\/][^\\/]*[\\/]package\.json$/.test(path)) continue;
        const bytes = await readFile(join(baseline.root, path), "utf8");
        const metadata = JSON.parse(bytes);
        reads++;
        const key = `${metadata.name}@${metadata.version}`;
        if (identities.has(key) || metadata.private) continue;
        identities.add(key);
        bytesByIdentity.set(key, bytes);
        if (metadata.license !== "MIT") errors.add(`${key}: ${metadata.license}`);
      }
      expect(result.errors).toEqual([...errors].sort());
      expect(new Map(result.reads.map(([, bytes]) => {
        const metadata = JSON.parse(bytes);
        return [`${metadata.name}@${metadata.version}`, bytes];
      }))).toEqual(bytesByIdentity);
      expect(result.reads.length).toBe(count);
      expect(reads).toBeGreaterThan(count);
      console.log(`alias chain ${count}: former glob ${reads} reads / ${(performance.now() - started).toFixed(2)} ms; canonical ${result.reads.length} reads / ${candidateMs.toFixed(2)} ms`);
    } finally {
      rmSync(candidate.root, { recursive: true, force: true });
      rmSync(baseline.root, { recursive: true, force: true });
    }
  }
});
