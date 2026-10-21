import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cpSync, symlinkSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
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
    : stem === "protocol/v1/protocol_pb" ? "rust/crates/harness-core/proto/protocol/v1/protocol.proto"
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

test("real TypeScript generation needs no network or populated Buf cache", { skip: process.platform !== "linux" }, () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-proto-offline-"));
  // The filter is inherited by Buf and its local plugin. Unlike proxy settings,
  // it also blocks direct IP connections and cannot fall back to a warm cache.
  const isolated = String.raw`
import ctypes, ctypes.util, errno, os, socket, sys
lib = ctypes.CDLL(ctypes.util.find_library("seccomp"), use_errno=True)
lib.seccomp_init.argtypes = [ctypes.c_uint32]
lib.seccomp_init.restype = ctypes.c_void_p
lib.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
lib.seccomp_rule_add.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint]
lib.seccomp_load.argtypes = [ctypes.c_void_p]
context = lib.seccomp_init(0x7fff0000)
assert context
number = lib.seccomp_syscall_resolve_name(b"socket")
assert number >= 0
assert lib.seccomp_rule_add(context, 0x50000 | errno.EPERM, number, 0) == 0
assert lib.seccomp_load(context) == 0
for family in [socket.AF_INET, socket.AF_INET6]:
    try:
        socket.socket(family, socket.SOCK_STREAM)
    except OSError as error:
        assert error.errno == errno.EPERM
    else:
        raise AssertionError("network socket unexpectedly allowed")
os.execv(sys.argv[1], sys.argv[1:])
`;
  try {
    for (const file of ["buf.yaml", "buf.lock", "buf.gen.yaml", "package.json",
      "scripts/generate-proto.mjs", "scripts/generated-bindings.mjs"]) {
      mkdirSync(dirname(join(root, file)), { recursive: true });
      cpSync(file, join(root, file));
    }
    const configuration = readFileSync(join(root, "buf.yaml"), "utf8");
    for (const [, path] of configuration.matchAll(/^  - path: (.+)$/gm)) {
      cpSync(path, join(root, path), { recursive: true });
    }
    symlinkSync(resolve("node_modules"), join(root, "node_modules"), "dir");
    const generate = label => {
      const cache = join(root, label);
      mkdirSync(cache);
      assert.deepEqual(readdirSync(cache), []);
      return spawnSync("python3", ["-c", isolated, process.execPath, "scripts/generate-proto.mjs"], {
        cwd: root, encoding: "utf8", timeout: 8_000,
        env: { ...process.env, BUF_CACHE_DIR: join(cache, "buf"), XDG_CACHE_HOME: cache, HOME: cache,
          BUN_INSTALL_CACHE_DIR: join(cache, "bun") },
      });
    };
    const result = generate("empty-cache");
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    for (const [stem, names] of packagedTypeScriptBindings) for (const name of names) {
      for (const extension of [".js", ".d.ts"]) {
        assert.ok(readFileSync(join(root, "typescript/packages", name, "generated/proto", stem + extension)).length > 0);
      }
    }
    // Restoring the unused registry dependency must fail under the same filter.
    writeFileSync(join(root, "buf.yaml"), configuration + "\ndeps:\n  - buf.build/googleapis/googleapis\n");
    writeFileSync(join(root, "buf.lock"), `version: v2
deps:
  - name: buf.build/googleapis/googleapis
    commit: c17df5b2beca46928cc87d5656bd5343
    digest: b5:648a01e0170d4512dea7d564016165decd1ed6e34bef79fe54753e51ad7e27545709ad9157d7551270147d551155c595a2fb0bf5bb33b1c83040ddbce915c604
`);
    const registry = generate("negative-cache");
    assert.ifError(registry.error);
    assert.notEqual(registry.status, 0);
    assert.match(registry.stderr, /operation not permitted|server hosted at that remote is unavailable/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
