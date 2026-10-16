import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { runInNewContext } from "node:vm";
import test from "node:test";
import fc from "fast-check";
import { compatibilityArtifacts, packagedTypeScriptBindings, writeChanged } from "./generated-bindings.mjs";

const source = readFileSync(new URL("./generate-proto.mjs", import.meta.url), "utf8");
const implementation = source.slice(source.indexOf("export function"), source.lastIndexOf("\nif (process.argv"))
  .replace("export function", "function");
const names = [...new Set(packagedTypeScriptBindings.flatMap(([, names]) => names)), "workers"];
const sources = new Map(packagedTypeScriptBindings.map(([stem]) => [stem,
  stem === "validation/v1/options_pb" ? "rust/crates/contract-derive/proto/validation/v1/options.proto"
    : stem === "protocol/v1/protocol_pb" ? "rust/crates/harness/proto/protocol/v1/protocol.proto"
    : stem === "stream/v1/session_pb" ? "rust/crates/stream/proto/stream/v1/session.proto"
    : compatibilityArtifacts[stem.split("/")[0]].schemaDigest]));

function exercise(selected, seed, code = implementation) {
  const root = mkdtempSync(join(tmpdir(), "sdk-generate-proto-"));
  const output = name => join(root, "typescript/packages", name, "generated/proto");
  const chosen = packagedTypeScriptBindings.filter(([, names]) => names.some(name => selected.includes(name)));
  const payload = stem => Buffer.from(`${seed}:${stem}`);
  const put = (path, content) => { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, content); };
  let calls = 0;
  let writes = 0;
  const context = {
    root, packagedTypeScriptBindings, dirname, join, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync,
    process: { platform: process.platform },
    writeFileSync: (path, bytes) => { writes++; writeFileSync(path, bytes); },
    spawnSync: (_command, args) => {
      calls++;
      assert.equal(args[0], "generate");
      assert.equal(args[1], "--output");
      assert.deepEqual(Array.from(args.slice(3)), chosen.flatMap(([stem]) => ["--path", sources.get(stem)]));
      for (const [stem] of chosen) for (const extension of [".js", ".d.ts"]) {
        put(join(args[2], "generated/typescript", stem + extension), payload(stem));
      }
      return { status: 0 };
    },
  };
  context.writeChanged = runInNewContext(`(${writeChanged.toString()})`, context);
  const generate = runInNewContext(`${code}\ngenerateProto`, context);
  try {
    for (const name of names) put(join(output(name), "obsolete.js"), "old");
    const handwritten = join(root, "typescript/packages/filesystem/generated/wasm/package.json");
    put(handwritten, "handwritten metadata");
    generate(new Set(selected));
    assert.equal(calls, chosen.length > 0 ? 1 : 0);
    let expectedWrites = 0;
    for (const name of names) {
      const expected = chosen.filter(([, names]) => names.includes(name) && selected.includes(name));
      assert.equal(existsSync(join(output(name), "obsolete.js")), expected.length === 0, `obsolete output: ${name}`);
      for (const [stem] of expected) for (const extension of [".js", ".d.ts"]) {
        assert.deepEqual(readFileSync(join(output(name), stem + extension)), payload(stem));
        expectedWrites++;
      }
    }
    assert.equal(writes, expectedWrites);
    writes = 0;
    generate(new Set(selected));
    assert.equal(writes, 0, "unchanged projections must retain their files");
    assert.equal(readFileSync(handwritten, "utf8"), "handwritten metadata");
    assert.ok(readdirSync(root).every(name => !name.startsWith("target-proto-")));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

test("generation emits exactly the selected package closure and removes only its stale projections", () => {
  fc.assert(fc.property(fc.subarray(names), fc.integer(), (selected, seed) => exercise(selected, seed)), { numRuns: 64 });
  assert.throws(() => exercise(["objects"], 0, implementation.replace("rmSync(path);", "void path;")), /obsolete output/);
});
