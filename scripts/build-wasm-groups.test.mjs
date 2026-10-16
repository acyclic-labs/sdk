import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { readFileSync } from "node:fs";
import test from "node:test";
import { runInNewContext } from "node:vm";

const source = readFileSync("scripts/build-wasm-groups.mjs", "utf8").replace(/^import .*;\r?\n/, "");
const build = readFileSync("scripts/build-wasm.mjs", "utf8");
const inventory = build.slice(build.indexOf("const packages = {"), build.indexOf("const [packageArgument"));
const names = [...inventory.matchAll(/^  ([a-z]+): \{/gm)].map(match => match[1]);

for (const wrapper of ["", "sccache"]) for (const failure of [undefined, "stream", "spawn", "multiple", "root-multiple"]) {
  test(`WASM groups join every child: ${failure ?? "success"}, wrapper=${wrapper || "disabled"}`, async () => {
    const calls = [];
    const completed = [];
    const paths = new Set();
    let live = 0, budget = 0, maximum = 0;
    const state = { execPath: "fixture-node", env: { RUSTC_WRAPPER: wrapper, CARGO_BUILD_JOBS: "16" }, exitCode: 0 };
    const errors = [];
    const spawn = (executable, args, options) => {
      assert.equal(executable, state.execPath);
      assert.equal(args[0], "scripts/build-wasm.mjs");
      assert.equal(options.stdio, "inherit");
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
      const child = new EventEmitter();
      setImmediate(() => {
        paths.delete(directory); budget -= workers; live--;
        completed.push(name);
        if (failure === "spawn" && name === "stream") child.emit("error", new Error("spawn failed"));
        child.emit("close", name === failure || failure === "multiple" && ["stream", "harness"].includes(name)
          || failure === "root-multiple" && ["actors", "stream"].includes(name) ? 7 : 0);
      });
      return child;
    };
    await runInNewContext(`(async () => {${source}\n})()`, {
      spawn, process: state, console: { error: error => { assert.equal(live, 0); errors.push(String(error)); } },
    });
    assert.equal(maximum, 3);
    assert.equal(live, 0);
    const skipped = failure === "root-multiple" ? ["auth", "filesystem", "objects", "machines", "inference", "manifest"] : [];
    assert.deepEqual(calls.slice().sort(), names.filter(name => !skipped.includes(name)).sort());
    assert.equal(state.exitCode, failure ? 1 : 0);
    assert.equal(errors.length, ["multiple", "root-multiple"].includes(failure ?? "") ? 2 : failure ? 1 : 0);
    if (failure === "multiple" || failure === "root-multiple") {
      assert.ok(errors.includes("Error: stream WASM build exited with 7"));
      assert.ok(errors.includes(`Error: ${failure === "multiple" ? "harness" : "actors"} WASM build exited with 7`));
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
  assert.equal(JSON.parse(readFileSync("package.json", "utf8")).scripts.check,
    "bun run generate && bun run build:wasm && tsc -b --force --pretty false && tsc -p scripts --pretty false && bun run check:docs");
});

test("Linux warms isolated WASM targets within its worker budget before the complete test command", () => {
  const script = readFileSync("scripts/qualify-ci.sh", "utf8");
  const block = script.slice(script.indexOf("  linux)"), script.indexOf("  linux-musl|linux-arm64-musl)"));
  const matrix = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
  const lane = matrix.find(entry => entry.lane === "linux") ?? assert.fail("missing Linux lane");
  assert.equal(lane.workers, 16);
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
  assert.match(block, /    node scripts\/build-wasm-groups\.mjs\r?\n    bun run test\r?\n/);
  assert.equal(JSON.parse(readFileSync("package.json", "utf8")).scripts.test,
    "bun run check && bun run check:types && mkdir -p target && bun test --parallel=4 --reporter=junit --reporter-outfile=target/bun-test.xml typescript/packages && bun run --filter '@acyclic-labs/fs' test:composition && bun run test:contracts");
});
