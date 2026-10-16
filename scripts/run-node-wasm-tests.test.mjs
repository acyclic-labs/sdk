import { expect, test } from "bun:test";
import fc from "fast-check";
import { bindingKey, cachedRunner, runNodeWasmTests, runnerIdentity } from "./run-node-wasm-tests.mjs";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync, existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join } from "node:path";

function invalidationOracle(keyFor, digest, change) {
  const key = keyFor(digest, "runner", [], [], "script");
  expect(keyFor(digest, "runner", [], [], "script")).toBe(key);
  expect(keyFor(digest + change, "runner", [], [], "script")).not.toBe(key);
  expect(keyFor(digest, "runner" + change, [], [], "script")).not.toBe(key);
  expect(keyFor(digest, "runner", [change], [], "script")).not.toBe(key);
  expect(keyFor(digest, "runner", [], [["NODE_ARGS",change]], "script")).not.toBe(key);
  expect(keyFor(digest, "runner", [], [], "script" + change)).not.toBe(key);
}

test("binding cache invalidates every input and retains identical inputs", () => {
  fc.assert(fc.property(fc.string(), fc.string({minLength:1}), (digest, change) => {
    invalidationOracle(bindingKey, digest, change);
    expect(() => invalidationOracle((_digest, version, args, env, source) => bindingKey("omitted", version, args, env, source), digest, change)).toThrow();
    expect(() => invalidationOracle((digest, _runner, args, env, source) => bindingKey(digest, "omitted", args, env, source), digest, change)).toThrow();
  }), {numRuns:64});
});

test("same-version runner wrappers generate and reuse separate bindings", async () => {
  const root = mkdtempSync(join(tmpdir(), "node-wasm-wrappers-"));
  try {
    const module = join(root, "fixture.wasm");
    const generated = join(root, "generated.txt");
    const executed = join(root, "executed.txt");
    writeFileSync(module, "fixture module digest");
    const wrappers = ["first", "second"].map(name => {
      const path = join(root, name);
      const result = `require("node:fs").appendFileSync(${JSON.stringify(executed)}, ${JSON.stringify(name + "\n")});`;
      writeFileSync(path, `#!${process.execPath}
const fs = require("node:fs"), path = require("node:path");
if (process.argv[2] === "--version") { console.log("wasm-bindgen-test-runner 0.2.117"); process.exit(0); }
fs.appendFileSync(${JSON.stringify(generated)}, ${JSON.stringify(name + "\n")});
const output = path.join(process.env.TMPDIR, "generated");
fs.mkdirSync(output, {recursive:true});
fs.writeFileSync(path.join(output, "run.cjs"), ${JSON.stringify(result)});
${result}
`, {mode:0o700});
      return path;
    });
    // Launch script fixtures through the interpreter on every OS, including
    // Windows where CreateProcess does not interpret executable shebangs.
    const execute = (command, args, options) => wrappers.includes(command)
      ? spawnSync(process.execPath, [command, ...args], options)
      : spawnSync(command, args, options);
    const versions = wrappers.map(path => execute(path, ["--version"], {encoding:"utf8"}).stdout);
    expect(versions[0]).toBe("wasm-bindgen-test-runner 0.2.117\n");
    expect(versions[1]).toBe(versions[0]);
    const identities = await Promise.all(wrappers.map(runnerIdentity));
    expect(identities[0].digest).not.toBe(identities[1].digest);
    // Old version-only keys collide; the production identity must distinguish them.
    const assertIsolated = identities => expect(bindingKey("module", identities[0], [], [], "script"))
      .not.toBe(bindingKey("module", identities[1], [], [], "script"));
    assertIsolated(identities);
    expect(() => assertIsolated(versions)).toThrow();
    for (const wrapper of wrappers) {
      expect(await runNodeWasmTests([module], wrapper, execute)).toBe(0);
      expect(await runNodeWasmTests([module], wrapper, execute)).toBe(0);
    }
    expect(readFileSync(generated, "utf8")).toBe("first\nsecond\n");
    expect(readFileSync(executed, "utf8")).toBe("first\nfirst\nsecond\nsecond\n");
  } finally { rmSync(root, {recursive:true,force:true}); }
});

test("cache finds both upstream Node module formats", () => {
  for (const file of ["run.cjs", "run.mjs"]) {
    const cache = mkdtempSync(join(tmpdir(), "node-wasm-cache-"));
    try {
      mkdirSync(join(cache,"generated"));
      const entry = join(cache,"generated",file);
      writeFileSync(entry, "// upstream runner artifact");
      expect(cachedRunner(cache)).toBe(entry);
      if (file === "run.cjs") {
        expect(() => expect(existsSync(join(cache,"generated","run.mjs"))).toBe(true)).toThrow();
      }
    } finally { rmSync(cache,{recursive:true,force:true}); }
  }
});
