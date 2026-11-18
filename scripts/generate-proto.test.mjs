import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cpSync, symlinkSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import fc from "fast-check";
import { packagedTypeScriptBindings, retiredTypeScriptBindingPackages, selectTypeScriptBindings } from "./protobuf-bindings.mjs";

const names = [...new Set([...packagedTypeScriptBindings.flatMap(([, names]) => names), ...retiredTypeScriptBindingPackages, "workers"])];

test("the shared binding plan projects exactly the selected packages", () => {
  fc.assert(fc.property(fc.subarray(names), selected => {
    const planned = selectTypeScriptBindings(new Set(selected)).flatMap(([stem, targets, sourceRoot]) =>
      targets.map(target => [target, stem, sourceRoot]));
    const expected = selected.flatMap(target => packagedTypeScriptBindings
      .filter(([, targets]) => targets.includes(target)).map(([stem, , sourceRoot]) => [target, stem, sourceRoot]));
    const ordered = rows => rows.map(row => JSON.stringify(row)).sort();
    assert.deepEqual(ordered(planned), ordered(expected));
  }), { numRuns: 64 });
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
    for (const file of [".gitignore", "buf.yaml", "buf.lock", "buf.gen.yaml", "package.json",
      "scripts/generate-proto.mjs", "scripts/generated-bindings.mjs", "scripts/protobuf-bindings.mjs", "scripts/policy-core.mjs"]) {
      mkdirSync(dirname(join(root, file)), { recursive: true });
      cpSync(file, join(root, file));
    }
    const configuration = readFileSync(join(root, "buf.yaml"), "utf8");
    for (const [, path] of configuration.matchAll(/^  - path: (.+)$/gm)) {
      cpSync(path, join(root, path), { recursive: true });
    }
    symlinkSync(resolve("node_modules"), join(root, "node_modules"), "dir");
    const output = name => join(root, "typescript/packages", name, "generated/proto");
    const put = (path, bytes) => { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes); };
    for (const name of names) put(join(output(name), "obsolete.js"), "obsolete projection");
    const handwritten = join(root, "typescript/packages/filesystem/package.json");
    put(handwritten, "handwritten metadata");
    const generate = (label, packages = undefined) => {
      const cache = join(root, label);
      mkdirSync(cache);
      assert.deepEqual(readdirSync(cache), []);
      const args = packages === undefined ? ["scripts/generate-proto.mjs"] : ["--input-type=module", "-e",
        `import { generateProto } from './scripts/generate-proto.mjs'; generateProto(new Set(${JSON.stringify(packages)}));`];
      return spawnSync("python3", ["-c", isolated, process.execPath, ...args], {
        cwd: root, encoding: "utf8",
        env: { ...process.env, BUF_CACHE_DIR: join(cache, "buf"), XDG_CACHE_HOME: cache, HOME: cache,
          BUN_INSTALL_CACHE_DIR: join(cache, "bun") },
      });
    };
    const result = generate("empty-cache");
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stdout + result.stderr);
    const defaults = new Set([...packagedTypeScriptBindings.flatMap(([, names]) => names), ...retiredTypeScriptBindingPackages]);
    for (const name of names) assert.equal(existsSync(join(output(name), "obsolete.js")), !defaults.has(name));
    for (const [stem, names] of packagedTypeScriptBindings) for (const name of names) {
      for (const extension of [".js", ".d.ts"]) {
        assert.ok(readFileSync(join(root, "typescript/packages", name, "generated/proto", stem + extension)).length > 0);
      }
    }
    const untouched = join(output("auth"), "auth/v1/auth_pb.js");
    put(untouched, "unselected projection");
    put(join(output("stream"), "obsolete.js"), "obsolete projection");
    const scoped = generate("scoped-cache", ["stream"]);
    assert.ifError(scoped.error);
    assert.equal(scoped.status, 0, scoped.stdout + scoped.stderr);
    assert.equal(readFileSync(untouched, "utf8"), "unselected projection");
    assert.equal(existsSync(join(output("stream"), "obsolete.js")), false);
    // An empty binding plan still owns and prunes its retired family's output.
    put(join(output("harness"), "obsolete.js"), "retired projection");
    const retired = generate("retired-cache", ["harness"]);
    assert.ifError(retired.error);
    assert.equal(retired.status, 0, retired.stdout + retired.stderr);
    assert.equal(existsSync(join(output("harness"), "obsolete.js")), false);
    assert.equal(readFileSync(handwritten, "utf8"), "handwritten metadata");
    assert.ok(readdirSync(root).every(name => !name.startsWith("target-proto-")));
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
