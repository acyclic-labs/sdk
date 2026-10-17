import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

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
