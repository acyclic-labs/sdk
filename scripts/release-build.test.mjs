import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { releaseBuild } from "./release-build.mjs";

const adapter = fileURLToPath(new URL("./release-build.mjs", import.meta.url));
const fixture = fileURLToPath(new URL("./tests/fixtures/release-build/artifact-build.mjs", import.meta.url));

function workspace(t) {
  const base = resolve(realpathSync(tmpdir()));
  const dir = mkdtempSync(join(base, "release-build-test-"));
  t.after(() => {
    assert.equal(dirname(resolve(dir)), base);
    rmSync(dir, { recursive: true, force: true });
  });
  return dir;
}

function cacheEnv(dir, result) {
  return {
    ...process.env,
    SDK_RELEASE_BUILD_CONTEXT: join(dir, "admitted context.json"),
    SDK_ARTIFACT_BUILD_MODULE: fixture,
    RELEASE_TEST_INVOCATION: join(dir, "invocation.json"),
    RELEASE_TEST_RESULT: String(result),
  };
}

function assertTerminal(stderr, id, status) {
  const lines = stderr.split(/\r?\n/).filter(line => line.startsWith("RELEASE_BUILD_RESULT "));
  assert.equal(lines.length, 1, stderr);
  const result = JSON.parse(lines[0].slice("RELEASE_BUILD_RESULT ".length));
  assert.equal(result.id, id);
  assert.equal(result.status, status);
  assert.ok(Number.isFinite(result.seconds) && result.seconds >= 0);
}

function run(script, args, options) {
  const result = spawnSync(process.execPath, [script, ...args], { encoding: "utf8", ...options });
  assert.ifError(result.error);
  assert.equal(result.signal, null, result.stderr);
  return result;
}

for (const outcome of [0, 7, "throw", "missing-module"]) {
  for (const entry of ["API", "CLI"]) {
    test(`cached release ${entry}: ${outcome}`, async t => {
      const cwd = workspace(t);
      const env = cacheEnv(cwd, outcome);
      if (outcome === "missing-module") env.SDK_ARTIFACT_BUILD_MODULE = join(cwd, "missing.mjs");
      const options = { id: "qualifier", command: "compiler-must-not-run", args: ["--locked", "with spaces"],
        cwd, env, output: join(cwd, "output with spaces") };
      if (entry === "API") {
        const errors = [];
        t.mock.method(console, "error", line => errors.push(line));
        const pending = releaseBuild(options);
        if (typeof outcome === "number") assert.equal(await pending, outcome);
        else await assert.rejects(pending, outcome === "throw" ? /artifact fixture failed/ : { code: "ERR_MODULE_NOT_FOUND" });
        assertTerminal(errors.join("\n"), options.id, outcome === 0 ? "PASS" : "FAIL");
      } else {
        const result = run(adapter, [options.id, "output with spaces", "--", options.command, ...options.args], { cwd, env });
        assert.equal(result.status, typeof outcome === "number" ? outcome : 1, result.stderr);
        if (outcome === "throw") assert.match(result.stderr, /artifact fixture failed/);
        if (outcome === "missing-module") assert.match(result.stderr, /ERR_MODULE_NOT_FOUND/);
        assertTerminal(result.stderr, options.id, outcome === 0 ? "PASS" : "FAIL");
      }
      if (outcome === "missing-module") assert.equal(existsSync(env.RELEASE_TEST_INVOCATION), false);
      else {
        const invocation = JSON.parse(readFileSync(env.RELEASE_TEST_INVOCATION, "utf8"));
        const { env: actualEnv, ...actual } = invocation;
        const { env: expectedEnv, ...expected } = { ...options, context: env.SDK_RELEASE_BUILD_CONTEXT };
        assert.deepEqual(actual, expected);
        // Child-process environments may normalize platform-owned values.
        if (entry === "API") assert.deepEqual(actualEnv, expectedEnv);
        else for (const name of ["SDK_RELEASE_BUILD_CONTEXT", "SDK_ARTIFACT_BUILD_MODULE", "RELEASE_TEST_INVOCATION", "RELEASE_TEST_RESULT"]) {
          assert.equal(actualEnv[name], env[name]);
        }
      }
    });
  }
}
