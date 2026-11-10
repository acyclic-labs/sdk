import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, readdir, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { createNativeProducer } from "./build-native-family.mjs";
import { nativeFamily } from "./native-family.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
function run(command, args, env = process.env) {
  const result = spawnSync(command, args, { cwd: root, env, stdio: "inherit" });
  assert.ifError(result.error);
  assert.equal(result.status, 0, `${command} ${args.join(" ")} failed`);
}

async function identity(path) {
  const { ino, size, mtimeNs } = await stat(path, { bigint: true });
  return { ino, size, mtimeNs };
}

async function dependencies(directory, artifact) {
  const files = (await readdir(directory)).filter(name => /\.(?:rlib|rmeta)$/.test(name)
    && !name.startsWith(`lib${artifact}-`)).sort();
  assert.ok(files.length > 0, "the real build must populate its dependency cache");
  return new Map(await Promise.all(files.map(async name => /** @type {const} */ ([name, await identity(join(directory, name))]))));
}

for (const name of ["actors", "stream"]) {
  test(`${name} captures current compiler provenance with cold and warm dependencies`, async () => {
    // Generation uses the caller's shared cache. Only the compiler-cache
    // regression gets fresh, temporary Cargo roots and real compiler processes.
    run(process.execPath, ["scripts/generate.mjs", name]);
    const cache = process.env.CARGO_TARGET_DIR || join(root, "target");
    await mkdir(cache, { recursive: true });
    const temporary = await mkdtemp(join(cache, "wasm-provenance-"));
    try {
      const family = nativeFamily(name);
      const producer = createNativeProducer(family);
      const source = await producer.sourceSnapshot();
      const revision = spawnSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" });
      assert.ifError(revision.error);
      assert.equal(revision.status, 0);
      const output = join(temporary, "output");
      const target = join(temporary, "target");
      const env = { ...process.env, CARGO_TARGET_DIR: target, ACYCLIC_WASM_GROUPS_PREPARED: "1" };
      const profile = process.env.ACYCLIC_WASM_PROFILE || "wasm-release";
      const directory = join(target, "wasm-producers", name, "wasm32-unknown-unknown",
        profile === "dev" ? "debug" : profile);
      const module = join(directory, `${family.wasm.artifact}.wasm`);
      let coldModule, coldDependencies;
      for (const phase of ["cold", "warm"]) {
        run(process.execPath, ["scripts/build-wasm.mjs", name, output], env);
        const receipt = JSON.parse(await readFile(join(output, "producer-receipt.json"), "utf8"));
        assert.equal(receipt.source_commit, revision.stdout.trim(), `${phase} source revision`);
        assert.equal(receipt.source_sha256, source.sha256, `${phase} source digest`);
        assert.deepEqual(receipt.source_files, source.files, `${phase} source inventory`);
        assert.equal(receipt.build.module_sha256,
          `sha256:${createHash("sha256").update(await readFile(module)).digest("hex")}`);
        const invocation = receipt.build.rustc.invocation;
        assert.equal(invocation.source, "rustc-invocation");
        assert.equal(invocation.args[invocation.args.indexOf("--crate-name") + 1], family.wasm.artifact);
        assert.match(invocation.args.join("\0"), /--emit(?:=|\0)[^\0]*link/);
        assert.deepEqual(receipt.build.rustc.identity,
          producer.capturedCompilerIdentity(invocation, "wasm32-unknown-unknown"));
        const currentModule = await identity(module);
        const currentDependencies = await dependencies(join(directory, "deps"), family.wasm.artifact);
        if (phase === "cold") {
          coldModule = currentModule;
          coldDependencies = currentDependencies;
        } else {
          // Without the root clean, Cargo reuses the binary and the capture
          // has no link invocation. A clean of dependencies also fails here.
          assert.notDeepEqual(currentModule, coldModule, "the selected root must link again");
          assert.deepEqual(currentDependencies, coldDependencies, "dependencies must remain cached");
        }
      }
    } finally {
      await rm(temporary, { recursive: true, force: true });
    }
  });
}
