import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { plugin, target, packageVersion, fixture } from "./install-fixture.mjs";
const { blake3 } = createRequire(join(plugin, "tests/hosts/package.json"))("@noble/hashes/blake3");
test("installer verifies bundled certification without external mutable state", async () => {
  const value = fixture();
  const state = join(value.root, "state");
  try {
    mkdirSync(state);
    const platform = { linux: "linux", darwin: "macos", win32: "windows" }[process.platform];
    const architecture = { x64: "x86_64", arm64: "aarch64" }[process.arch];
    const backend = { linux: "linux-fuse", darwin: "macos-nfs", win32: "windows-projfs" }[process.platform];
    const name = `native-mount-${target}.json`;
    const certification = join(value.root, "certification");
    mkdirSync(certification);
    const receipt = {
      schema: "acyclic-native-mount-qualification-v2",
      os: platform,
      arch: architecture,
      required_kind: backend,
      release_version: packageVersion,
      executable_blake3: Buffer.from(blake3(readFileSync(value.source))).toString("hex"),
      capability: {
        kind: backend,
        available: true,
        writable: true,
        provider_process_io_observable: platform !== "windows",
        session_isolation: "SharedProcess",
        unavailable_reason: null,
      },
      // Internal policy inputs exercise the actual CLI; this is not a live
      // qualification receipt and is never exported as release evidence.
      coverage: ["create-read-write", "atomic-save", "rename-delete", "rename-before-hydration",
        "nested-paths", "large-directory-paging", "concurrent-handles", "watchers",
        "crash-detach-recovery", "mount-restoration", "hard-links", "symbolic-links-reparse-points",
        "metadata", "case-behavior", "escape-attempts", "root-checkout-untouched", "git-administration-untouched"],
      cases: ["real-mount-mutation-matrix", "crash-detach-recovery", "checkout-and-git-untouched"]
        .map(name => ({ name, status: "passed", reason: null })),
      passed: true,
    };
    writeFileSync(join(certification, name), JSON.stringify(receipt));
    const environment = {
      ...(process.platform === "win32"
        ? { LOCALAPPDATA: state }
        : { XDG_STATE_HOME: state }),
    };
    const installed = await value.install(null, null, environment);
    assert.equal(installed.code, 0, installed.stderr);
    assert.deepEqual(readdirSync(state), [], "install must not create an external mutable certification cache");
    assert.deepEqual(JSON.parse(readFileSync(join(certification, name), "utf8")), receipt);
    // The former NODE_ENV=test bypass accepted these incomplete reports. Keep
    // that environment in the regression to prove it grants no exception.
    for (const invalid of [
      { ...receipt, coverage: [] },
      { ...receipt, cases: [] },
      { ...receipt, executable_blake3: "0".repeat(64) },
    ]) {
      writeFileSync(join(certification, name), JSON.stringify(invalid));
      const rejected = await value.install(null, null, {
        ...environment, ACYCLIC_INSTALL_TEST_CERTIFICATION: "1",
      });
      assert.notEqual(rejected.code, 0, "incomplete or unbound certification accepted");
      assert.match(rejected.stderr, /step=verify installed release certification/);
      assert.deepEqual(readdirSync(state), [], "rejection must not create external state");
    }
  } catch (cause) {
    throw new Error(`step=verify bundled installer certification: ${cause.message}; recovery: with the admitted ACYCLIC_TEST_BINARY, run node --test --test-reporter=tap --test-concurrency=1 plugin/scripts/install-certification.test.mjs`, { cause });
  } finally {
    await value.close();
  }
});
