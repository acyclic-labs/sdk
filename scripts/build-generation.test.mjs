import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, posix } from "node:path";
import { runInNewContext } from "node:vm";
import test from "node:test";
import fc from "fast-check";
import { packageClosure } from "./package-closure.mjs";
import { contractModules } from "./generate-contracts.mjs";
import { createNativeProducer } from "./build-native-family.mjs";
import { nativeFamily } from "./native-family.mjs";
import ts from "typescript-parser";

const source = readFileSync("scripts/package-closure.mjs", "utf8");
const closure = source.slice(source.indexOf("export function")).replaceAll("export function", "function");
const names = "abcdefghijkl";

test("package closure follows declared workspaces and ignores leftover directories", t => {
  const root = mkdtempSync(join(tmpdir(), "package-closure-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  writeFileSync(`${root}/package.json`, JSON.stringify({ workspaces: ["typescript/packages/*"] }));
  for (const name of ["a", "b"]) {
    const directory = `${root}/typescript/packages/${name}`;
    mkdirSync(directory, { recursive: true });
    writeFileSync(`${directory}/package.json`, JSON.stringify({ name: `@example/${name}`,
      dependencies: name === "a" ? { "@example/b": "workspace:*" } : {} }));
    writeFileSync(`${directory}/tsconfig.json`, JSON.stringify({}));
  }
  assert.deepEqual([...packageClosure(root, "a")], ["b", "a"]);
  mkdirSync(`${root}/typescript/packages/b/node_modules`, { recursive: true });
  mkdirSync(`${root}/typescript/packages/c/node_modules`, { recursive: true });
  assert.deepEqual([...packageClosure(root, "a")], ["b", "a"]);
  writeFileSync(`${root}/package.json`, JSON.stringify({ workspaces: ["typescript/packages/[ab]"] }));
  mkdirSync(`${root}/typescript/packages/z`, { recursive: true });
  writeFileSync(`${root}/typescript/packages/z/package.json`, "invalid undeclared manifest");
  assert.deepEqual([...packageClosure(root, "a")], ["b", "a"]);
  writeFileSync(`${root}/typescript/packages/b/package.json`, "invalid declared manifest");
  assert.throws(() => packageClosure(root, "a"), SyntaxError);
});

function actual(rows, selected, code = closure) {
  const context = {
    root: "root", selected: names[selected % rows.length], join: posix.join,
    basename: posix.basename, dirname: posix.dirname,
    globSync: () => rows.map((_, index) => `typescript/packages/${names[index]}/package.json`),
    readFileSync: path => {
      if (path === "root/package.json") return JSON.stringify({ workspaces: ["typescript/packages/*"] });
      const index = names.indexOf(path.split("/")[3]);
      assert.ok(index >= 0 && index < rows.length, path);
      const row = rows[index];
      return JSON.stringify(path.endsWith("tsconfig.json")
        ? { references: row.refs.map(index => ({ path: `../${names[index % rows.length]}` })) }
        : { name: `@acyclic-labs/${names[index]}`,
          dependencies: Object.fromEntries(row.dependencies.map(index => [`@acyclic-labs/${names[index % rows.length]}`, "0.2.0"])),
          peerDependencies: Object.fromEntries(row.peers.map(index => [`@acyclic-labs/${names[index % rows.length]}`, "0.2.0"])) });
    },
  };
  return Array.from(runInNewContext(`${code}\n[...packageClosure(root, selected)]`, context));
}

function expected(rows, selected) {
  const reached = new Set([selected % rows.length]);
  let prior;
  do {
    prior = reached.size;
    for (const index of [...reached]) {
      for (const next of [...rows[index].refs, ...rows[index].dependencies, ...rows[index].peers]) reached.add(next % rows.length);
    }
  } while (reached.size !== prior);
  return [...reached].map(index => names[index]).sort();
}

test("package generation covers exactly the transitive project and npm dependency closure", () => {
  const edges = fc.array(fc.nat(11), { maxLength: 12 });
  fc.assert(fc.property(fc.array(fc.record({ refs: edges, dependencies: edges, peers: edges }), { minLength: 1, maxLength: 12 }), fc.nat(11), (rows, selected) => {
    assert.deepEqual(actual(rows, selected).sort(), expected(rows, selected));
  }), { numRuns: 64 });
  const rows = [{ refs: [], dependencies: [], peers: [1] }, { refs: [], dependencies: [], peers: [] }];
  assert.notDeepEqual(actual(rows, 0, closure.replace("...manifest.peerDependencies", "...{}")).sort(), expected(rows, 0));
});

test("each package dependency precedes its consumer and is prepared exactly once", () => {
  const check = (rows, selected, code = closure) => {
    const result = actual(rows, selected, code);
    assert.equal(result.length, new Set(result).size);
    for (const name of result) {
      assert.ok(typeof name === "string");
      const row = rows[names.indexOf(name)];
      for (const next of [...row.refs, ...row.dependencies, ...row.peers]) {
        assert.ok(result.indexOf(names[next]) < result.indexOf(name));
      }
    }
  };
  const edges = fc.array(fc.nat(11), { maxLength: 12 });
  fc.assert(fc.property(fc.array(fc.record({ refs: edges, dependencies: edges, peers: edges }), { minLength: 1, maxLength: 12 }), fc.nat(11), (rows, selected) => {
    const acyclic = rows.map((row, index) => Object.fromEntries(Object.entries(row)
      .map(([kind, edges]) => [kind, index === 0 ? [] : edges.map(edge => edge % index)])));
    check(acyclic, selected);
  }), { numRuns: 64 });
  const old = closure.replace("    packages.add(name);", "")
    .replace("visited.add(name);", "visited.add(name); packages.add(name);");
  assert.throws(() => check([{ refs: [], dependencies: [], peers: [] }, { refs: [0], dependencies: [], peers: [] }], 1, old));
});

test("generated runtime bindings stay out of the source index", () => {
  const files = execFileSync("git", ["ls-files", "-z"], { encoding: "utf8" }).split("\0");
  for (const file of files) {
    assert.doesNotMatch(file, /^rust\/crates\/[^/]+\/src\/generated\//);
    if (/^typescript\/packages\/[^/]+\/(?:src\/|test\/)?generated\//.test(file)) {
      assert.ok(["typescript/packages/filesystem/generated/wasm/.npmignore", "typescript/packages/filesystem/generated/wasm/package.json"].includes(file), file);
    }
  }
});

test("both native admission baselines equal the actual Rust generator", () => {
  for (const family of ["actors", "stream"]) {
    createNativeProducer(nativeFamily(family)).assertCurrentNativeFamily();
  }
});

test("native source roots cover the generators' transitive script imports", () => {
  function check(roots, generators, changes = new Map()) {
    const pending = [...generators, ...roots.filter(path => path.startsWith("scripts/") && path.endsWith(".mjs"))];
    const visited = new Set();
    while (pending.length) {
      const path = pending.pop();
      if (visited.has(path)) continue;
      assert.ok(roots.some(root => path === root || path.startsWith(root + "/")), "imports unattested " + path);
      visited.add(path);
      const source = changes.get(path) ?? readFileSync(path, "utf8");
      const syntax = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.JS);
      const imports = [];
      function visit(node) {
        if ((ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) && node.moduleSpecifier) {
          imports.push(node.moduleSpecifier.text);
        } else if (ts.isCallExpression(node) && node.expression.kind === ts.SyntaxKind.ImportKeyword) {
          const target = node.arguments[0];
          if (target && ts.isStringLiteral(target)) imports.push(target.text);
          else assert.ok(path === "scripts/generate-contracts.mjs" && target?.getText(syntax) === '`./${name}`', "uncovered computed import in " + path);
        }
        ts.forEachChild(node, visit);
      }
      visit(syntax);
      for (const specifier of imports) {
        if (!specifier.startsWith(".")) continue;
        const imported = posix.normalize(posix.join(posix.dirname(path), specifier));
        assert.ok(roots.some(root => imported === root || imported.startsWith(root + "/")), path + " imports unattested " + imported);
        if (imported.endsWith(".mjs")) pending.push(imported);
      }
    }
  }
  for (const family of ["actors", "stream"]) {
    const { sourceRoots } = JSON.parse(readFileSync("compatibility/native-families/" + family + ".json", "utf8"));
    const generators = contractModules([...packageClosure(process.cwd(), family)]).map(name => "scripts/" + name);
    check(sourceRoots, generators);
    assert.throws(() => check(sourceRoots.filter(path => path !== "scripts/package-closure.mjs"), generators), /imports unattested scripts\/package-closure\.mjs/);
    const changed = new Map([["scripts/package-closure.mjs", 'import "./unattested-generator-helper.mjs";']]);
    assert.throws(() => check(sourceRoots, generators, changed), /imports unattested scripts\/unattested-generator-helper\.mjs/);
    changed.set("scripts/package-closure.mjs", 'await import("./unattested-generator-helper.mjs");');
    assert.throws(() => check(sourceRoots, generators, changed), /imports unattested scripts\/unattested-generator-helper\.mjs/);
    changed.set("scripts/package-closure.mjs", 'const name = "./unattested-generator-helper.mjs"; await import(name);');
    assert.throws(() => check(sourceRoots, generators, changed), /uncovered computed import/);
  }
});
