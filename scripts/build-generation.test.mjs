import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { posix } from "node:path";
import { runInNewContext } from "node:vm";
import test from "node:test";
import fc from "fast-check";

const source = readFileSync("scripts/generate.mjs", "utf8");
const closure = source.slice(source.indexOf("const packages = new Set();"), source.indexOf("function run("));
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
  return Array.from(runInNewContext(`${code}\n[...packages].sort()`, context));
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
    assert.deepEqual(actual(rows, selected), expected(rows, selected));
  }), { numRuns: 64 });
  const rows = [{ refs: [], dependencies: [], peers: [1] }, { refs: [], dependencies: [], peers: [] }];
  assert.notDeepEqual(actual(rows, 0, closure.replace("...manifest.peerDependencies", "...{}")), expected(rows, 0));
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
    for (const operation of ["build", "test", "prepack"]) if (scripts[operation]) assert.match(scripts[operation], /^bun run generate(?: && |$)/, `${name}: ${operation}`);
  }
});
