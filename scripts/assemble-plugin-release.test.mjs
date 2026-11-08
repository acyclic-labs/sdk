import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { hostFixtureDirectory } from "./graphcoder-e2e/fixture-directory.mjs";

const binary = process.env.ACYCLIC_TEST_BINARY;
const recovery = "set ACYCLIC_TEST_BINARY to the source-qualified shipping executable and run node --test scripts/assemble-plugin-release.test.mjs";
assert.ok(binary && isAbsolute(binary), `step=admit release executable; recovery: ${recovery}`);
const script = fileURLToPath(new URL("./assemble-plugin-release.mjs", import.meta.url));
const archives = ["acyclic-plugin-npm.tgz", "acyclic-plugin.tar.gz"];

test("release assembly publishes only runnable packages and retires installation state", () => {
  const root = hostFixtureDirectory("plugin-release-");
  try {
    const temporary = join(root, "tmp"), home = join(root, "home"), output = join(root, "release");
    mkdirSync(temporary); mkdirSync(home);
    // Preserve actual executable bytes as the corruption fixture's provenance.
    const truncated = join(root, "truncated-acyclic");
    const bytes = readFileSync(binary);
    assert.ok(bytes.length > 128, "shipping executable must exceed the truncated header");
    writeFileSync(truncated, bytes.subarray(0, 128), { mode: 0o755 });
    const env = { PATH: process.env.PATH ?? "", HOME: home, USERPROFILE: home,
      TMPDIR: temporary, TEMP: temporary, TMP: temporary, LANG: "C.UTF-8", NODE_DISABLE_COMPILE_CACHE: "1" };
    for (const key of ["SystemRoot", "ComSpec", "PATHEXT"]) {
      if (process.env[key]) env[key] = process.env[key];
    }
    const assemble = executable => {
      const result = spawnSync(process.execPath, [script, "--binary", executable, "--out", output],
        { cwd: root, env, encoding: "utf8", windowsHide: true });
      assert.ifError(result.error);
      assert.equal(result.signal, null, result.stderr);
      assert.deepEqual(readdirSync(temporary), [], `temporary installation files survived: ${result.stderr}`);
      return result;
    };
    const refused = assemble(truncated);
    assert.notEqual(refused.status, 0, refused.stdout);
    assert.match(refused.stderr, /step=.*recovery:/);
    assert.deepEqual(readdirSync(output).filter(name => name !== "package"), [],
      "failed fresh assembly published release assets");

    const accepted = assemble(binary);
    assert.equal(accepted.status, 0, accepted.stderr);
    const published = archives.map(name => readFileSync(join(output, name)));
    for (const archive of published) assert.ok(archive.length > 0, "published archive is empty");

    const rebuild = assemble(truncated);
    assert.notEqual(rebuild.status, 0, rebuild.stdout);
    assert.deepEqual(archives.map(name => readFileSync(join(output, name))), published,
      "failed rebuild replaced a previously verified archive");
    assert.deepEqual(readdirSync(output).filter(name => name.endsWith(".tmp")), [],
      "failed rebuild left pending archives");
  } catch (cause) {
    throw new Error(`step=verify release publication and cleanup: ${cause.message}; recovery: ${recovery}`, { cause });
  } finally {
    try { rmSync(root, { recursive: true, force: true }); }
    catch (cause) {
      throw new Error(`step=retire release fixture: ${cause.message}; recovery: ${recovery}`, { cause });
    }
  }
});
