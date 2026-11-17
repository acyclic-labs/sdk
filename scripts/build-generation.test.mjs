import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, posix } from "node:path";
import { runInNewContext } from "node:vm";
import test from "node:test";
import fc from "fast-check";
import { packageClosure } from "./package-closure.mjs";
import { contractModules, keepsGenerator } from "./generate-contracts.mjs";
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
    assert.doesNotMatch(file, /^typescript\/packages\/[^/]+\/(?:src\/|test\/)?generated\//);
  }
});

test("both native admission baselines equal the actual Rust generator", () => {
  for (const family of ["actors", "stream"]) {
    createNativeProducer(nativeFamily(family)).assertCurrentNativeFamily();
  }
});

test("native source roots cover the generators' transitive script imports", () => {
  function check(roots, generators, changes = new Map()) {
    const pending = [...generators];
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
    // Entry points describe execution; allowed roots describe attested inputs.
    // Widening an allowed root must never remove code from the import walk.
    const generators = ["scripts/generate.mjs", "scripts/check-baselines.mjs",
      `scripts/build-${family}-native.mjs`, `scripts/assemble-${family}-native-package.mjs`,
      ...contractModules([...packageClosure(process.cwd(), family)]).map(name => "scripts/" + name)];
    check(sourceRoots, generators);
    assert.throws(() => check(sourceRoots.filter(path => path !== "scripts"), generators), /imports unattested scripts\//);
    check(["scripts", "tools"], generators);
    const changed = new Map([["scripts/package-closure.mjs", 'import "../unattested-generator-helper.mjs";']]);
    assert.throws(() => check(["scripts", "tools"], generators, changed), /imports unattested unattested-generator-helper\.mjs/);
    changed.set("scripts/package-closure.mjs", 'await import("../unattested-generator-helper.mjs");');
    assert.throws(() => check(["scripts", "tools"], generators, changed), /imports unattested unattested-generator-helper\.mjs/);
    changed.set("scripts/package-closure.mjs", 'const name = "../unattested-generator-helper.mjs"; await import(name);');
    assert.throws(() => check(["scripts", "tools"], generators, changed), /uncovered computed import/);
  }
});

test("a selected generation emits actors N-API declarations only for a native build", async () => {
  const actors = await import("./actors-napi-types.mjs"), stream = await import("./stream-napi-types.mjs");
  const families = [...packageClosure(process.cwd(), "actors")];
  assert.ok(contractModules(families).includes("actors-napi-types.mjs"), "the native build still attests the module");
  assert.equal(keepsGenerator(actors, families, false), false, "a WASM build of actors never compiles the N-API crate");
  assert.equal(keepsGenerator(actors, families, true), true);
  assert.equal(keepsGenerator(actors, [], false), true, "full generation keeps every output");
  // Stream's TypeScript imports its declarations as types, so every selection keeps them.
  assert.equal(keepsGenerator(stream, ["stream"], false), true);
});

test("a memoized generation restores exactly what its run projected and pruned, and only for what it covered", async () => {
  const { memoizedGeneration } = await import("./generation-cache.mjs");
  const { recordGeneration, writeChanged } = await import("./generated-bindings.mjs");
  const root = mkdtempSync(join(tmpdir(), "sdk-memo-root-"));
  const store = mkdtempSync(join(tmpdir(), "sdk-memo-store-"));
  const previous = process.env.SDK_GENERATION_CACHE;
  process.env.SDK_GENERATION_CACHE = store;
  try {
    const runs = [];
    const generate = async () => {
      runs.push("run");
      writeChanged(join(root, "a.txt"), "a");
      writeChanged(join(root, "out/x.txt"), "x");
      writeChanged(join(root, "out/dropped.txt"), "dropped");
      recordGeneration("P", join(root, "out"));
      rmSync(join(root, "out/dropped.txt"));
    };
    const actors = { name: "actors", packages: ["core", "auth", "actors"], native: false };
    await memoizedGeneration(root, actors, generate, { key: "k" });
    assert.deepEqual(runs, ["run"]);
    rmSync(join(root, "a.txt"));
    writeFileSync(join(root, "out/x.txt"), "edited");
    writeFileSync(join(root, "out/stale.txt"), "stale");
    // A narrower selection is covered; the hit restores projections and prunes what the run pruned.
    await memoizedGeneration(root, { name: "auth", packages: ["core", "auth"], native: false }, generate, { key: "k" });
    assert.deepEqual(runs, ["run"]);
    assert.equal(readFileSync(join(root, "a.txt"), "utf8"), "a");
    assert.equal(readFileSync(join(root, "out/x.txt"), "utf8"), "x");
    assert.throws(() => readFileSync(join(root, "out/stale.txt")), { code: "ENOENT" });
    assert.throws(() => readFileSync(join(root, "out/dropped.txt")), { code: "ENOENT" });
    // Native outputs and other inputs are not covered.
    await memoizedGeneration(root, { ...actors, name: "actors+native", native: true }, generate, { key: "k" });
    await memoizedGeneration(root, actors, generate, { key: "other" });
    assert.deepEqual(runs, ["run", "run", "run"]);
    // An action owning only its artifacts neither overwrites nor prunes the source-owned files beside them on a hit.
    const { isWasmArtifactPath } = await import("./archive-utils.mjs");
    const owns = path => isWasmArtifactPath(path.slice("wasm/".length));
    writeChanged(join(root, "wasm/package.json"), "source");
    const place = async () => {
      runs.push("run");
      writeChanged(join(root, "wasm/module.js"), "module");
      recordGeneration("P", join(root, "wasm"));
      recordGeneration("W", join(root, "wasm/package.json"));
    };
    await memoizedGeneration(root, actors, place, { key: "owned", owns });
    writeFileSync(join(root, "wasm/package.json"), "edited");
    writeFileSync(join(root, "wasm/.npmignore"), "added");
    writeFileSync(join(root, "wasm/stale.js"), "stale");
    await memoizedGeneration(root, actors, place, { key: "owned", owns });
    assert.deepEqual(runs, ["run", "run", "run", "run"]);
    assert.equal(readFileSync(join(root, "wasm/package.json"), "utf8"), "edited");
    assert.equal(readFileSync(join(root, "wasm/.npmignore"), "utf8"), "added");
    assert.throws(() => readFileSync(join(root, "wasm/stale.js")), { code: "ENOENT" });
    // A concurrent publisher of the same key, in another checkout, keeps its own outputs; the first entry serves hits.
    const elsewhere = mkdtempSync(join(tmpdir(), "sdk-memo-elsewhere-"));
    try {
      await memoizedGeneration(root, actors, async () => {
        writeChanged(join(root, "a.txt"), `checkout ${root}`);
        await memoizedGeneration(elsewhere, actors, async () => writeChanged(join(elsewhere, "a.txt"), `checkout ${elsewhere}`), { key: "race" });
      }, { key: "race" });
      assert.equal(readFileSync(join(root, "a.txt"), "utf8"), `checkout ${root}`);
      await memoizedGeneration(root, actors, generate, { key: "race" });
      assert.equal(readFileSync(join(root, "a.txt"), "utf8"), `checkout ${elsewhere}`);
    } finally { rmSync(elsewhere, { recursive: true, force: true }); }    // A damaged entry is an error, never a hit.
    writeFileSync(join(store, "other", "actors", "files", "a.txt"), "tampered");
    await assert.rejects(memoizedGeneration(root, actors, generate, { key: "other" }), /damaged entry/);
  } finally {
    if (previous === undefined) delete process.env.SDK_GENERATION_CACHE; else process.env.SDK_GENERATION_CACHE = previous;
    rmSync(root, { recursive: true, force: true });
    rmSync(store, { recursive: true, force: true });
  }
});

test("a generation key covers every crate a generator compiles and names content alone, whatever its Git state", async () => {
  const { digests, generationInputs, generationKey } = await import("./generation-cache.mjs");
  const store = mkdtempSync(join(tmpdir(), "sdk-memo-store-"));
  const repository = mkdtempSync(join(tmpdir(), "sdk-memo-digests-"));
  const previous = process.env.SDK_GENERATION_CACHE;
  process.env.SDK_GENERATION_CACHE = store;
  try {
    // N-API declarations compile crates no contract target depends on; their sources are inputs all the same.
    const inputs = new Set(await generationInputs(process.cwd()));
    for (const crate of ["actors-napi", "stream-napi", "filesystem-napi", "filesystem"]) {
      assert.ok(inputs.has(`rust/crates/${crate}/src/lib.rs`), `${crate} sources are generation inputs`);
    }
    // The generators and the WASM build live in tools/; a change there must select a new entry.
    for (const tool of ["tools/generate-proto.mjs", "tools/build-wasm.mjs"]) {
      assert.ok(inputs.has(tool), `${tool} is a generation input`);
    }
    // The compiler Cargo runs and every setting that changes its output select the entry; output directories do not.
    const fake = join(repository, "rustc");
    writeFileSync(fake, "#!/bin/sh\necho fake-rustc\n", { mode: 0o755 });
    const keyWith = async settings => {
      const saved = Object.fromEntries(Object.keys(settings).map(name => [name, process.env[name]]));
      Object.assign(process.env, settings);
      try { return await generationKey(process.cwd()); }
      finally { for (const [name, value] of Object.entries(saved)) if (value === undefined) delete process.env[name]; else process.env[name] = value; }
    };
    const base = await keyWith({});
    assert.notEqual(await keyWith({ RUSTC: fake }), base, "RUSTC selects the compiler");
    assert.notEqual(await keyWith({ CARGO_PROFILE_WASM_RELEASE_OPT_LEVEL: "0" }), base, "a profile override changes the output");
    assert.equal(await keyWith({ CARGO_TARGET_DIR: join(repository, "target") }), base, "an output directory selects nothing");
    assert.equal(await keyWith({ CARGO_BUILD_JOBS: "6" }), base, "parallelism selects nothing");
    assert.equal(await keyWith({ CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER: "cc" }), base, "a linker selects nothing");
    assert.equal(await keyWith({ CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER: "runner" }), base, "a runner selects nothing");
    rmSync(fake);
    const git = (...args) => execFileSync("git", ["-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false", ...args], { cwd: repository });
    git("init", "-q");
    writeFileSync(join(repository, "kept"), "kept");
    writeFileSync(join(repository, "edited"), "before");
    git("add", "."); git("commit", "-q", "-m", "base");
    writeFileSync(join(repository, "edited"), "after");
    writeFileSync(join(repository, "added"), "added");
    rmSync(join(repository, "kept"));
    const working = digests(repository, ["."]);
    git("add", "-A");
    assert.deepEqual(digests(repository, ["."]), working, "staging keeps every identity");
    git("commit", "-q", "-m", "next");
    assert.deepEqual(digests(repository, ["."]), working, "committing keeps every identity");
    assert.deepEqual([...working.keys()].sort(), ["added", "edited"], "a deleted file is absent, as after its deletion commits");
  } finally {
    if (previous === undefined) delete process.env.SDK_GENERATION_CACHE; else process.env.SDK_GENERATION_CACHE = previous;
    rmSync(store, { recursive: true, force: true });
    rmSync(repository, { recursive: true, force: true });
  }
});