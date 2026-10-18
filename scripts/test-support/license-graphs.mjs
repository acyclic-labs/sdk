import { strict as assert } from "node:assert";
import * as fs from "node:fs/promises";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { auditLicenses } from "../check-js-licenses.mjs";

export function packageAt(root, path, metadata) {
  const directory = join(root, path);
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, "package.json"), JSON.stringify({ version: "1.0.0", ...metadata }));
  return directory;
}

export function linkPackage(root, path, target) {
  const link = join(root, path);
  mkdirSync(dirname(link), { recursive: true });
  symlinkSync(process.platform === "win32" ? target : relative(dirname(link), target), link,
    process.platform === "win32" ? "junction" : "dir");
}

export function graphFixture(bytes, chain = false) {
  const root = mkdtempSync(join(tmpdir(), "acyclic-license-graph-"));
  const count = 1 + (bytes[0] ?? 0) % 8;
  const paths = [], expected = [], contents = new Map();
  const names = ["plain", "space name", "é", "e\u0301", "@scope/scoped", "dash-name", "under_score", "日本語"];
  try {
    for (let index = 0; index < count; index++) {
      const value = bytes[index + 1] ?? index;
      const metadata = { name: `p${index}`, license: value % 2 ? "MIT" : "GPL-3.0-only" };
      const path = packageAt(root, `node_modules/.bun/p${index}@1/node_modules/${names[value % names.length]}`, metadata);
      paths.push(path);
      contents.set(realpathSync(join(path, "package.json")), JSON.stringify({ version: "1.0.0", ...metadata }));
      if (value % 2 === 0) expected.push(`p${index}@1.0.0: GPL-3.0-only`);
      // Published module-format manifests are not installed package roots.
      packageAt(path, "dist/commonjs", { type: "commonjs" });
    }
    linkPackage(root, "node_modules/start", paths[0]);
    for (let index = 0; index < count; index++) {
      const value = bytes[index + 1] ?? index;
      const targets = chain ? (index + 1 < count ? [index + 1, index + 1] : [])
        : [value % count, (value + index) % count];
      for (const [alias, target] of targets.entries()) {
        const scope = value % 3 ? "" : "@aliases/";
        linkPackage(paths[index], `node_modules/${scope}edge${alias}`, paths[target]);
      }
    }
    return { root, count, expected: expected.sort(), contents };
  } catch (error) {
    rmSync(root, { recursive: true, force: true });
    throw error;
  }
}

export async function measure(root, audit = auditLicenses) {
  const reads = [], directories = [];
  const errors = await audit(root, {
    ...fs,
    async readFile(path, encoding) {
      const bytes = await fs.readFile(path, encoding);
      reads.push([String(path), bytes]);
      return bytes;
    },
    async readdir(path, options) {
      directories.push(String(path));
      return fs.readdir(path, options);
    },
  });
  return { errors, reads, directories };
}

export function assertGraphResult(fixture, result) {
  assert.deepEqual(result.errors, fixture.expected);
  assert.equal(result.reads.length, fixture.count, "exactly one read per physical manifest");
  assert.deepEqual(new Map(result.reads), fixture.contents, "full manifest bytes and canonical paths");
  assert.equal(new Set(result.directories).size, result.directories.length, "expand each physical directory once");
}

export async function checkGraph(bytes, audit = auditLicenses) {
  const fixture = graphFixture(bytes);
  try {
    const result = await measure(fixture.root, audit);
    assertGraphResult(fixture, result);
  } finally {
    rmSync(fixture.root, { recursive: true, force: true });
  }
}
