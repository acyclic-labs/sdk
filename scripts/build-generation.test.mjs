import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { posix } from "node:path";
import { runInNewContext } from "node:vm";
import test from "node:test";
import fc from "fast-check";

const source = readFileSync("scripts/package-closure.mjs", "utf8");
const closure = source.slice(source.indexOf("export function")).replace("export function", "function");
const names = "abcdefghijkl";

function actual(rows, selected, code = closure) {
  const context = {
    root: "root", selected: names[selected % rows.length], join: posix.join,
    readdirSync: () => rows.map((_, index) => ({ name: names[index], isDirectory: () => true })),
    readFileSync: path => {
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

test("every TypeScript package prepares generation for build, test and prepack", () => {
  for (const name of ["actors", "auth", "filesystem", "harness", "inference", "machines", "objects", "pi", "sdk", "stream", "swarm", "workers"]) {
    const { scripts } = JSON.parse(readFileSync(`typescript/packages/${name}/package.json`, "utf8"));
    assert.equal(scripts.generate, `bun ../../../scripts/generate.mjs ${name}`);
    for (const operation of ["build", "check", "test", "prepack"]) if (scripts[operation]) assert.match(scripts[operation], /^bun run generate(?: && |$)/, `${name}: ${operation}`);
  }
});

test("native source roots cover the generators' transitive script imports", () => {
  // Node's module parser enumerates static imports without executing generators.
  execFileSync(process.execPath, ["--experimental-vm-modules", "--input-type=module", "-e", String.raw`
    import assert from "node:assert/strict";
    import { readFileSync } from "node:fs";
    import { posix } from "node:path";
    import { SourceTextModule, runInNewContext } from "node:vm";
    import { packageClosure } from "./scripts/package-closure.mjs";

    function check(roots, generators, changes = new Map()) {
      const pending = [...generators, ...roots.filter(path => path.startsWith("scripts/") && path.endsWith(".mjs"))];
      const visited = new Set();
      while (pending.length) {
        const path = pending.pop();
        if (visited.has(path)) continue;
        assert.ok(roots.some(root => path === root || path.startsWith(root + "/")), "imports unattested " + path);
        visited.add(path);
        const source = changes.get(path) ?? readFileSync(path, "utf8");
        const imports = new SourceTextModule(source).dependencySpecifiers;
        // Literal dynamic imports are not in SourceTextModule's static graph.
        const dynamic = [];
        for (const call of source.matchAll(/\bimport\s*\(\s*([^)]*?)\s*\)/g)) {
          const literal = /^(["'])([^"']+)\1$/.exec(call[1]);
          if (literal) dynamic.push(literal[2]);
          else assert.ok(path === "scripts/generate-contracts.mjs" && call[1] === String.fromCharCode(96) + "./$" + "{name}" + String.fromCharCode(96), "uncovered computed import in " + path);
        }
        for (const specifier of [...imports, ...dynamic]) {
          if (!specifier.startsWith(".")) continue;
          const imported = posix.normalize(posix.join(posix.dirname(path), specifier));
          assert.ok(roots.some(root => imported === root || imported.startsWith(root + "/")), path + " imports unattested " + imported);
          if (imported.endsWith(".mjs")) pending.push(imported);
        }
      }
    }
    const contracts = readFileSync("scripts/generate-contracts.mjs", "utf8");
    const selection = contracts.slice(contracts.indexOf("const modules ="), contracts.indexOf("const root ="));
    for (const family of ["actors", "stream"]) {
      const { sourceRoots } = JSON.parse(readFileSync("compatibility/native-families/" + family + ".json", "utf8"));
      // Use the real selector for the computed import in generate-contracts.
      const modules = runInNewContext(selection + "modules", { process: { argv: ["node", "generate-contracts", "write", ...packageClosure(process.cwd(), family)] } });
      const generators = modules.map(name => "scripts/" + name);
      check(sourceRoots, generators);
      assert.throws(() => check(sourceRoots.filter(path => path !== "scripts/package-closure.mjs"), generators), /imports unattested scripts\/package-closure\.mjs/);
      const changed = new Map([["scripts/package-closure.mjs", 'import "./unattested-generator-helper.mjs";']]);
      assert.throws(() => check(sourceRoots, generators, changed), /imports unattested scripts\/unattested-generator-helper\.mjs/);
      changed.set("scripts/package-closure.mjs", 'await import("./unattested-generator-helper.mjs");');
      assert.throws(() => check(sourceRoots, generators, changed), /imports unattested scripts\/unattested-generator-helper\.mjs/);
      changed.set("scripts/package-closure.mjs", 'const name = "./unattested-generator-helper.mjs"; await import(name);');
      assert.throws(() => check(sourceRoots, generators, changed), /uncovered computed import/);
    }
  `], { stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
});
