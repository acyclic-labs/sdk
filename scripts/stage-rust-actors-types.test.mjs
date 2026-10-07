import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import test from "node:test";

const stageSource = fileURLToPath(new URL("./stage-rust-actors-types.mjs", import.meta.url));

function artifact(path, bytes) {
  return {
    path,
    sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
    bytes: bytes.length,
  };
}

function fixture(artifacts = []) {
  const root = mkdtempSync(join(tmpdir(), "acyclic-stage-workers-"));
  const script = join(root, "scripts", "stage-rust-actors-types.mjs");
  mkdirSync(dirname(script), { recursive: true });
  cpSync(stageSource, script);
  const bundle = join(root, "bundle");
  const protoRelative = "generated/workers/proto/workers/v1/workers.proto";
  const descriptorRelative = "generated/workers/acyclic-workers-v1.bin";
  const proto = Buffer.from("// Generated from Rust-owned Workers contract. Do not edit.\nsyntax = \"proto3\";\n");
  const descriptor = Buffer.from("descriptor");
  const protoPath = join(bundle, protoRelative);
  const descriptorPath = join(bundle, descriptorRelative);
  mkdirSync(dirname(protoPath), { recursive: true });
  mkdirSync(dirname(descriptorPath), { recursive: true });
  writeFileSync(protoPath, proto);
  writeFileSync(descriptorPath, descriptor);
  writeFileSync(
    join(bundle, "generation-manifest.json"),
    JSON.stringify({
      schema: "acyclic.sdk.generation.v1",
      family: "acyclic_actors",
      families: ["acyclic_actors", "acyclic_workers"],
      artifacts: artifacts.length ? artifacts : [artifact(protoRelative, proto), artifact(descriptorRelative, descriptor)],
    }),
  );
  return { root, bundle, protoRelative, descriptorRelative, proto, descriptor };
}

function run(fixtureValue, operation) {
  return spawnSync(
    process.execPath,
    [join(fixtureValue.root, "scripts/stage-rust-actors-types.mjs"), operation, fixtureValue.bundle],
    { cwd: fixtureValue.root, encoding: "utf8" },
  );
}

test("Workers contract write and check share the attested bundle", () => {
  const fixtureValue = fixture();
  const destination = join(fixtureValue.root, "proto/workers/v1/workers.proto");
  try {
    const written = run(fixtureValue, "contract-write");
    assert.equal(written.status, 0, written.stderr);
    assert.deepEqual(require("node:fs").readFileSync(destination), fixtureValue.proto);
    const checked = run(fixtureValue, "contract-check");
    assert.equal(checked.status, 0, checked.stderr);
  } finally {
    rmSync(fixtureValue.root, { recursive: true, force: true });
  }
});

test("Workers contract check rejects stale staged output", () => {
  const fixtureValue = fixture();
  const destination = join(fixtureValue.root, "proto/workers/v1/workers.proto");
  try {
    mkdirSync(dirname(destination), { recursive: true });
    writeFileSync(destination, "stale\n");
    const result = run(fixtureValue, "contract-check");
    assert.notEqual(result.status, 0);
    assert.match(`${result.stdout}\n${result.stderr}`, /staged Workers proto is stale/);
  } finally {
    rmSync(fixtureValue.root, { recursive: true, force: true });
  }
});

test("Workers contract staging rejects missing and tampered artifacts", async t => {
  await t.test("missing descriptor", () => {
    const fixtureValue = fixture();
    try {
      rmSync(join(fixtureValue.bundle, fixtureValue.descriptorRelative));
      const result = run(fixtureValue, "contract-write");
      assert.notEqual(result.status, 0);
      assert.match(`${result.stdout}\n${result.stderr}`, /generated bundle artifact is missing/);
    } finally {
      rmSync(fixtureValue.root, { recursive: true, force: true });
    }
  });

  await t.test("tampered proto", () => {
    const fixtureValue = fixture();
    try {
      writeFileSync(join(fixtureValue.bundle, fixtureValue.protoRelative), "tampered\n");
      const result = run(fixtureValue, "contract-write");
      assert.notEqual(result.status, 0);
      assert.match(`${result.stdout}\n${result.stderr}`, /generation manifest does not attest/);
    } finally {
      rmSync(fixtureValue.root, { recursive: true, force: true });
    }
  });
});

test("Workers contract staging requires the Workers family", () => {
  const fixtureValue = fixture([], "0.2.0");
  try {
    const manifestPath = join(fixtureValue.bundle, "generation-manifest.json");
    writeFileSync(manifestPath, JSON.stringify({
      schema: "acyclic.sdk.generation.v1",
      family: "acyclic_actors",
      families: ["acyclic_actors"],
      artifacts: [artifact(fixtureValue.protoRelative, fixtureValue.proto), artifact(fixtureValue.descriptorRelative, fixtureValue.descriptor)],
    }));
    const result = run(fixtureValue, "contract-write");
    assert.notEqual(result.status, 0);
    assert.match(`${result.stdout}\n${result.stderr}`, /does not contain the Workers documentation family/);
  } finally {
    rmSync(fixtureValue.root, { recursive: true, force: true });
  }
});

test("Workers staging rejects duplicate artifact paths", () => {
  const path = "generated/workers/acyclic-workers-v1.bin";
  const bytes = Buffer.from("descriptor");
  const fixtureValue = fixture([artifact(path, bytes), artifact(path, bytes)]);
  try {
    const result = run(fixtureValue, "contract-check");
    assert.notEqual(result.status, 0);
    assert.match(`${result.stdout}\n${result.stderr}`, /duplicate or invalid artifact paths/);
  } finally {
    rmSync(fixtureValue.root, { recursive: true, force: true });
  }
});

test("Workers staging rejects a junction destination when supported", t => {
  const fixtureValue = fixture();
  const outside = mkdtempSync(join(tmpdir(), "acyclic-stage-workers-outside-"));
  const workersParent = join(fixtureValue.root, "proto", "workers");
  try {
    mkdirSync(dirname(workersParent), { recursive: true });
    try {
      symlinkSync(outside, workersParent, "junction");
    } catch (error) {
      t.skip(`junction creation unavailable: ${error.message}`);
      return;
    }
    const result = run(fixtureValue, "contract-write");
    assert.notEqual(result.status, 0);
    assert.match(`${result.stdout}\n${result.stderr}`, /symlink or reparse point|resolves outside checkout/);
    assert.equal(existsSync(join(outside, "v1", "workers.proto")), false);
  } finally {
    rmSync(fixtureValue.root, { recursive: true, force: true });
    rmSync(outside, { recursive: true, force: true });
  }
});
