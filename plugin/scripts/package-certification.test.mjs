import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes } from "../../scripts/graphcoder-e2e/process.mjs";
import { hostFixtureDirectory } from "../../scripts/graphcoder-e2e/fixture-directory.mjs";

const plugin = fileURLToPath(new URL("../", import.meta.url));
const binary = process.env.ACYCLIC_TEST_BINARY;
assert.ok(binary && isAbsolute(binary), "step=admit packaging verifier; recovery=set ACYCLIC_TEST_BINARY to the source-qualified shipping executable and run node --test --test-reporter=tap --test-concurrency=1 plugin/scripts/package-certification.test.mjs");
const packageVersion = JSON.parse(readFileSync(join(plugin, "package.json"), "utf8")).version;

const { blake3 } = createRequire(join(plugin, "tests/hosts/package.json"))("@noble/hashes/blake3");

test("universal package certification delegates complete artifact policy to the native verifier", async () => {
  const processes = new Processes();
  const root = hostFixtureDirectory("package-certification-");
  const output = join(root, "release");
  const validate = [join(plugin, "scripts/validate-package.mjs"), output, "--require-universal"];
  try {
    const home = join(root, "home"), temporary = join(root, "tmp");
    mkdirSync(home); mkdirSync(temporary);
    const env = { PATH: process.env.PATH ?? "", HOME: home, USERPROFILE: home,
      CODEX_HOME: join(home, "unused-codex-profile"), TMPDIR: temporary, TEMP: temporary, TMP: temporary,
      LANG: "C.UTF-8" };
    for (const key of ["SystemRoot", "ComSpec", "PATHEXT", "E2E_PROCESS_HOST"]) {
      if (process.env[key]) env[key] = process.env[key];
    }
    const run = argv => processes.runOwned(process.execPath, argv, { cwd: root, env });
    const version = await processes.runOwned(binary, ["--version"], { cwd: root, env });
    assert.equal(version.code, 0, version.stderr);
    assert.equal(version.stdout.trim(), `acyclic ${packageVersion}`);
    const targets = JSON.parse(readFileSync(join(plugin, "bin/targets.json"), "utf8")).targets;
    // These are internal cross-target policy inputs. Every artifact contains
    // the admitted shipping executable's real bytes, not a foreign OS fixture
    // or live qualification result. Only the native host artifact is executed.
    const build = await run([join(plugin, "scripts/package.mjs"),
      "--out", output, ...Object.keys(targets).flatMap(target => ["--binary", `${target}=${binary}`])]);
    assert.equal(build.code, 0, build.stderr);
    const certification = join(output, "plugin/certification");
    mkdirSync(certification);
    const digest = Buffer.from(blake3(readFileSync(binary))).toString("hex");
    const documents = [];
    for (const [target, os, arch, kind] of [
      ["linux-x64-gnu", "linux", "x86_64", "linux-fuse"],
      ["darwin-arm64", "macos", "aarch64", "macos-nfs"],
      ["win32-x64", "windows", "x86_64", "windows-projfs"],
    ]) {
      const document = {
        schema: "acyclic-native-mount-qualification-v2", os, arch, required_kind: kind,
        release_version: packageVersion, executable_blake3: digest, passed: true,
        capability: { kind, available: true, writable: true,
          provider_process_io_observable: os !== "windows", session_isolation: "SharedProcess",
          unavailable_reason: null },
        coverage: ["create-read-write", "atomic-save", "rename-delete", "rename-before-hydration",
          "nested-paths", "large-directory-paging", "concurrent-handles", "watchers",
          "crash-detach-recovery", "mount-restoration", "hard-links", "symbolic-links-reparse-points",
          "metadata", "case-behavior", "escape-attempts", "root-checkout-untouched", "git-administration-untouched"],
        cases: ["real-mount-mutation-matrix", "crash-detach-recovery", "checkout-and-git-untouched"]
          .map(name => ({ name, status: "passed", reason: null })),
      };
      const path = join(certification, `native-mount-${target}.json`);
      writeFileSync(path, JSON.stringify(document));
      documents.push({ path, document, target });
    }
    const complete = await run(validate);
    assert.equal(complete.code, 0, complete.stderr);
    for (const { path, document, target } of documents) {
      writeFileSync(path, JSON.stringify({ ...document, coverage: [] }));
      const incomplete = await run(validate);
      assert.notEqual(incomplete.code, 0, `${target}: incomplete release report accepted`);
      assert.match(incomplete.stderr, new RegExp(`step=verify ${target} certification`));
      assert.match(incomplete.stderr, /recovery argv:/);
      writeFileSync(path, JSON.stringify(document));
    }
  } catch (cause) {
    throw new Error(`step=verify universal package receipt admission: ${cause.message}; recovery: with the admitted ACYCLIC_TEST_BINARY, run node --test --test-reporter=tap --test-concurrency=1 plugin/scripts/package-certification.test.mjs`, { cause });
  } finally {
    try {
      await processes.close();
      rmSync(root, { recursive: true, force: true });
    } catch (cause) {
      throw new Error(`step=drain packaging processes and retire fixture: ${cause.message}; recovery=run node --test --test-reporter=tap --test-concurrency=1 plugin/scripts/package-certification.test.mjs with the admitted ACYCLIC_TEST_BINARY`, { cause });
    }
  }
});
