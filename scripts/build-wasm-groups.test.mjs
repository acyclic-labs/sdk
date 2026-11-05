import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import test from "node:test";
import { runInNewContext } from "node:vm";
import { packageClosure, workspaceManifests } from "./package-closure.mjs";

const source = readFileSync("scripts/build-wasm-groups.mjs", "utf8").replace(/^import .*;\r?\n/, "");
const build = readFileSync("scripts/build-wasm.mjs", "utf8");
const inventory = build.slice(build.indexOf("const packages = {"), build.indexOf("const [packageArgument"));
const names = [...inventory.matchAll(/^  ([a-z]+): \{/gm)].map(match => match[1]);

test("every standalone project compilation prepares its complete WASM closure", () => {
  const packages = workspaceManifests(process.cwd());
  const wasm = name => [...packageClosure(process.cwd(), name)]
    .map(name => name === "workers" ? "manifest" : name).filter(name => names.includes(name));

  function check(name, operation, changed = new Map()) {
    const prepared = new Set();
    const active = new Set();
    function run(name, operation) {
      const key = `${name}:${operation}`;
      assert.ok(!active.has(key), `recursive package script ${key}`);
      active.add(key);
      const script = changed.get(key) ?? packages.get(name).scripts[operation];
      assert.equal(typeof script, "string", key);
      for (const command of script.split(" && ")) {
        const builder = /^bun \.\.\/\.\.\/\.\.\/scripts\/build-wasm\.mjs ([a-z-]+)$/.exec(command);
        const dependency = /^bun run --cwd \.\.\/([a-z-]+) ([a-z0-9:-]+)$/.exec(command);
        const local = /^bun run ([a-z0-9:-]+)$/.exec(command);
        if (builder) {
          for (const target of wasm(builder[1] === "manifest" ? "workers" : builder[1])) prepared.add(target);
        } else if (dependency) {
          run(dependency[1], dependency[2]);
        } else if (local) {
          run(name, local[1]);
        } else if (/\btsc\s+-b(?:\s|$)/.test(command)) {
          for (const target of wasm(name)) {
            assert.ok(prepared.has(target), `${key} compiles before preparing ${target} WASM`);
          }
        }
      }
      active.delete(key);
    }
    run(name, operation);
  }

  // Enumerate every package and every command that invokes project compilation,
  // including commands reached through local/dependency bun-run prerequisites.
  for (const [name, { scripts }] of packages) {
    for (const [operation, command] of Object.entries(scripts)) {
      if (["build", "test", "check"].includes(operation) || /\btsc\s+-b(?:\s|$)/.test(command)) check(name, operation);
    }
  }
  for (const operation of ["build", "test"]) {
    const original = packages.get("sdk").scripts[operation];
    assert.throws(() => check("sdk", operation, new Map([
      [`sdk:${operation}`, original.replace("bun run build:wasm && ", "")],
    ])), /compiles before preparing/);
  }
  assert.throws(() => check("sdk", "build", new Map([
    ["sdk:build:wasm", "bun ../../../scripts/build-wasm.mjs actors"],
  ])), /compiles before preparing/);
});

test("standalone package WASM targets follow references while groups keep exclusive targets", () => {
  const selection = build.slice(build.indexOf("const grouped ="), build.indexOf("if (!grouped)"));
  const targets = (name, grouped) => Array.from(runInNewContext(`${selection}\ntargets`, {
    packageArgument: name, packageClosure, root: process.cwd(),
    packages: Object.fromEntries(names.map(name => [name, {}])),
    process: { env: grouped ? { ACYCLIC_WASM_GROUPS_PREPARED: "1" } : {} },
  }));
  assert.deepEqual(targets("filesystem", false), ["auth", "actors", "filesystem"]);
  assert.deepEqual(targets("harness", false), ["auth", "actors", "machines", "objects", "harness"]);
  for (const name of names) assert.deepEqual(targets(name, true), [name]);
});

test("shared generation precedes every build under all 72 group completion orders", async () => {
  async function exercise(code, stream, harness) {
    let generations = 0;
    const pending = new Map();
    const state = { execPath: "fixture-node", env: {}, exitCode: 0 };
    // Share the driver's promise realm: each injected completion advances one
    // production await, without cross-realm promise assimilation steps.
    const execute = new Function("runCommand", "process", "console", `return (async () => {${code}\n})()`);
    const execution = execute((_executable, args, options) => {
        if (args[0] === "scripts/generate.mjs") { generations++; return Promise.resolve(0); }
        assert.equal(generations, 1, "generation must precede every build");
        assert.equal(options.env.ACYCLIC_WASM_GROUPS_PREPARED, "1");
        const group = ["stream", "harness"].includes(args[1]) ? args[1] : "root";
        assert.ok(!pending.has(group));
        return new Promise(resolve => pending.set(group, resolve));
      }, state, { error: error => { throw error; } });
    void execution.catch(() => {}); // The broken control can fail before joining.
    await Promise.resolve();
    assert.equal(generations, 1, "generation must precede group creation");
    for (let step = 0; step < 9; step++) {
      const group = step === stream ? "stream" : step === harness ? "harness" : "root";
      const complete = pending.get(group);
      assert.ok(complete, `missing ${group} at ${step}`);
      pending.delete(group);
      complete(0);
      await Promise.resolve();
    }
    await execution;
    assert.equal(state.exitCode, 0);
    assert.equal(generations, 1);
    assert.equal(pending.size, 0);
  }
  for (let stream = 0; stream < 9; stream++) for (let harness = 0; harness < 9; harness++) {
    if (stream !== harness) await exercise(source, stream, harness);
  }
  const broken = source.replace(/const generated = await runCommand[\s\S]*?(?=const results =)/, "");
  // Group creation would immediately invoke the conflicting writers again.
  await assert.rejects(exercise(broken, 0, 1), /generation must precede/);
});

test("failed generation starts no WASM group", async () => {
  for (const generated of [{ status: 7 }, { error: new Error("generator spawn failed") }]) {
    let children = 0;
    await assert.rejects(runInNewContext(`(async () => {${source}\n})()`, {
      process: { execPath: "node", env: {} },
      runCommand: (_executable, args) => {
        if (args[0] !== "scripts/generate.mjs") { children++; throw new Error("child must not start"); }
        if (generated.error) throw generated.error;
        return Promise.resolve(generated.status);
      },
    }), /generate WASM inputs: exited with 7|generator spawn failed/);
    assert.equal(children, 0);
  }
});

for (const wrapper of ["", "sccache"]) for (const failure of [undefined, "stream", "spawn", "multiple", "root-multiple"]) {
  test(`WASM groups join every child: ${failure ?? "success"}, wrapper=${wrapper || "disabled"}`, async () => {
    const calls = [];
    const completed = [];
    const paths = new Set();
    let live = 0, budget = 0, maximum = 0, generations = 0;
    const state = { execPath: "fixture-node", env: { RUSTC_WRAPPER: wrapper, CARGO_BUILD_JOBS: "16" }, exitCode: 0 };
    const errors = [];
    const runCommand = (executable, args, options) => {
      assert.equal(executable, state.execPath);
      if (args[0] === "scripts/generate.mjs") {
        assert.deepEqual(Array.from(args), ["scripts/generate.mjs"]);
        assert.equal(live, 0);
        generations++;
        return Promise.resolve(0);
      }
      assert.equal(generations, 1, "shared generation must finish once before any group starts");
      assert.equal(options.env.ACYCLIC_WASM_GROUPS_PREPARED, "1");
      assert.equal(args[0], "scripts/build-wasm.mjs");
      assert.equal(options.env.RUSTC_WRAPPER, wrapper);
      const name = args[1];
      const directory = name === "harness" ? "harness-wasm"
        : ["actors", "stream"].includes(name)
          ? JSON.parse(readFileSync(`compatibility/native-families/${name}.json`, "utf8")).wasm.targetDirectory ?? "root"
          : "root";
      assert.ok(!paths.has(directory), `concurrent target directory: ${directory}`);
      paths.add(directory);
      const workers = Number(options.env.CARGO_BUILD_JOBS);
      assert.equal(workers, name === "stream" ? 4 : 6, name);
      budget += workers; live++; maximum = Math.max(maximum, live);
      assert.ok(budget <= 16, `oversubscribed: ${budget}`);
      calls.push(name);
      return new Promise((resolve, reject) => setImmediate(() => {
        paths.delete(directory); budget -= workers; live--;
        completed.push(name);
        if (failure === "spawn" && name === "stream") { reject(new Error("spawn failed")); return; }
        resolve(name === failure || failure === "multiple" && ["stream", "harness"].includes(name)
          || failure === "root-multiple" && ["actors", "stream"].includes(name) ? 7 : 0);
      }));
    };
    await runInNewContext(`(async () => {${source}\n})()`, {
      runCommand, process: state, console: { error: error => { assert.equal(live, 0); errors.push(String(error)); } },
    });
    assert.equal(generations, 1);
    assert.equal(maximum, 3);
    assert.equal(live, 0);
    const skipped = failure === "root-multiple" ? ["auth", "filesystem", "objects", "machines", "inference", "manifest"] : [];
    assert.deepEqual(calls.slice().sort(), names.filter(name => !skipped.includes(name)).sort());
    assert.equal(state.exitCode, failure ? 1 : 0);
    assert.equal(errors.length, ["multiple", "root-multiple"].includes(failure ?? "") ? 2 : failure ? 1 : 0);
    if (failure === "multiple" || failure === "root-multiple") {
      for (const name of ["stream", failure === "multiple" ? "harness" : "actors"])
        assert.ok(errors.includes(`Error: step=build ${name} WASM: exited with 7; recovery: node scripts/build-wasm.mjs ${name}`));
    }
    if (failure === "root-multiple") {
      assert.deepEqual(completed.slice().sort(), ["actors", "harness", "stream"]);
      for (const name of skipped) assert.ok(!calls.includes(name), `${name} started after actors failed`);
    }
    assert.equal(state.env.CARGO_BUILD_JOBS, "16");
  });
}

test("Windows retains all check commands after grouped WASM builds with the wrapper disabled", () => {
  const script = readFileSync("scripts/qualify-ci.ps1", "utf8");
  const block = script.slice(script.indexOf("$rustcWrapper = $env:RUSTC_WRAPPER", script.indexOf("$wasmBindgenRoot")), script.indexOf("$StreamNativeRoot"));
  const commands = ["Remove-Item Env:RUSTC_WRAPPER", "node scripts/build-wasm-groups.mjs", "bun x tsc -b --force --pretty false", "bun x tsc -p scripts --pretty false", "bun run check:docs", "} finally {"];
  let previous = -1;
  for (const command of commands) {
    const index = block.indexOf(command);
    assert.ok(index > previous, command); previous = index;
  }
  assert.ok(block.includes("$env:RUSTC_WRAPPER = $rustcWrapper"));
});

test("Linux warms isolated WASM targets within its worker budget before the complete test command", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8");
  const block = script.slice(script.indexOf("  linux)"), script.indexOf("  linux-musl|linux-arm64-musl)"));
  const prepare = block.indexOf("node scripts/build-wasm-groups.mjs");
  assert.ok(prepare >= 0);
  for (const prerequisite of ["    client_bindings", "finish napi", "ensure-wasm-bindgen.sh"]) {
    const index = block.indexOf(prerequisite);
    assert.ok(index >= 0 && index < prepare, prerequisite);
  }
  const fullTest = block.indexOf("    bun run test");
  assert.ok(fullTest >= 0 && prepare < fullTest);
  assert.equal((block.match(/node scripts\/build-wasm-groups\.mjs/g) ?? []).length, 1);
  assert.match(script, /^#![^\n]+\r?\nset -euo pipefail\r?\n/);
});


test("WASM builds reject manual JS byte copies outside the snapshot boundary", () => {
  const start = build.indexOf("const assertByteSnapshotBoundary =");
  const end = build.indexOf("const checkByteSnapshotBoundary =", start);
  const check = runInNewContext(`${build.slice(start, end)}; assertByteSnapshotBoundary`);
  const directory = "rust/crates/harness/src/wasm";
  for (const file of readdirSync(directory, { recursive: true, encoding: 'utf8' }).filter(file => file.endsWith(".rs"))) {
    check(file, readFileSync(`${directory}/${file}`, "utf8").replaceAll("\r\n", "\n"));
  }
  for (const expression of ["caller.to_vec()", "caller.copy_to(&mut bytes)", "Uint8Array::new(&caller)"]) {
    assert.throws(() => check("file_worker.rs", expression), /copy caller arrays through/);
  }
  assert.ok(build.indexOf("checkByteSnapshotBoundary(join(root,") < build.indexOf("const cargo ="));
});
