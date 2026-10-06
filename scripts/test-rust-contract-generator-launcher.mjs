import assert from "node:assert/strict";
import test from "node:test";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { runRustContractGenerator } from "./run-rust-contract-generator.mjs";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const buildRoot = resolve(repositoryRoot, "..", ".test-rust-contract-launcher");

function fakeCargoPath() {
  const path = join(buildRoot, "fake-cargo.mjs");
  writeFileSync(path, "process.exitCode = 0;\n");
  return path;
}

function withLauncherEnvironment(callback) {
  const previous = {
    SDK_BUILD_ROOT: process.env.SDK_BUILD_ROOT,
    ACYCLIC_SDK_WORK_ROOT: process.env.ACYCLIC_SDK_WORK_ROOT,
    CARGO_TARGET_DIR: process.env.CARGO_TARGET_DIR,
    ACYCLIC_CARGO_BIN: process.env.ACYCLIC_CARGO_BIN,
  };
  process.env.SDK_BUILD_ROOT = buildRoot;
  delete process.env.ACYCLIC_SDK_WORK_ROOT;
  delete process.env.CARGO_TARGET_DIR;
    delete process.env.ACYCLIC_CARGO_BIN;
  try {
    return callback();
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test("contract generation uses a source-keyed Cargo target and cleans owned output", () => {
  rmSync(buildRoot, { recursive: true, force: true });
  mkdirSync(buildRoot, { recursive: true });
  withLauncherEnvironment(() => {
    runRustContractGenerator(repositoryRoot, "all-write", undefined, {
      cargoProgram: process.execPath,
      cargoArgs: [fakeCargoPath()],
    });

    const cacheRoot = join(buildRoot, "acyclic-sdk-contracts");
    const children = requireChildren(join(buildRoot, "acyclic-sdk-contracts"));
    assert.equal(children.length, 1);
    assert.match(children[0], /^[0-9a-f]{16}$/);
    const target = join(cacheRoot, children[0], "cargo-target");
    assert.equal(existsSync(join(target, ".acyclic-sdk-source-owner.json")), true);
    assert.deepEqual(requireChildren(cacheRoot), children, "owned output staging must be cleaned");
  });
  rmSync(buildRoot, { recursive: true, force: true });
});

test("contract generation isolates default Cargo targets by source root", () => {
  rmSync(buildRoot, { recursive: true, force: true });
  mkdirSync(buildRoot, { recursive: true });
  const secondSourceRoot = join(buildRoot, ".second-contract-source");
  mkdirSync(secondSourceRoot, { recursive: true });
  withLauncherEnvironment(() => {
    const options = { cargoProgram: process.execPath, cargoArgs: [fakeCargoPath()] };
    runRustContractGenerator(repositoryRoot, "all-write", undefined, options);
    runRustContractGenerator(secondSourceRoot, "all-write", undefined, options);

    const children = requireChildren(join(buildRoot, "acyclic-sdk-contracts"));
    assert.equal(children.length, 2);
    assert.ok(children.every((child) => /^[0-9a-f]{16}$/.test(child)));
    assert.notEqual(children[0], children[1], "source roots must never share a final Cargo binary");
  });
  rmSync(buildRoot, { recursive: true, force: true });
});

test("contract generation rejects an explicit target without matching source ownership", () => {
  rmSync(buildRoot, { recursive: true, force: true });
  mkdirSync(buildRoot, { recursive: true });
  const explicitTarget = join(buildRoot, "explicit-target");
  const secondSourceRoot = join(buildRoot, ".second-contract-source");
  mkdirSync(secondSourceRoot, { recursive: true });
  withLauncherEnvironment(() => {
    const options = {
      cargoProgram: process.execPath,
      cargoArgs: [fakeCargoPath()],
      cargoTargetDirectory: explicitTarget,
    };
    runRustContractGenerator(repositoryRoot, "all-write", undefined, options);
    assert.equal(
      JSON.parse(readFileSync(join(explicitTarget, ".acyclic-sdk-source-owner.json"), "utf8")).canonical_source_root,
      repositoryRoot,
    );
    assert.throws(
      () => runRustContractGenerator(secondSourceRoot, "all-write", undefined, options),
      /Cargo target directory is owned by a different Rust source/,
    );
  });
  rmSync(buildRoot, { recursive: true, force: true });
});

test("contract generation rejects output nested inside the Rust source", () => {
  rmSync(buildRoot, { recursive: true, force: true });
  mkdirSync(buildRoot, { recursive: true });
  withLauncherEnvironment(() => {
    assert.throws(
      () => runRustContractGenerator(repositoryRoot, "all-write", join(repositoryRoot, "target", "contract-test")),
      /Contract output must be outside the Rust source root/,
    );
  });
  rmSync(buildRoot, { recursive: true, force: true });
});

function requireChildren(path) {
  // Avoid depending on directory enumeration order in the assertion.
  return [...new Set(readdirSync(path))].sort();
}
