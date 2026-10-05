import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { test } from "node:test";
import { runNativeBindingProducer, verifyNativeBindingReceipt } from "./graphcoder-native-binding-producer.mjs";

const root = process.cwd();
const freshOutput = () => {
  const parent = join(root, "target", "graphcoder-package-qualification", "tmp");
  mkdirSync(parent, { recursive: true });
  return mkdtempSync(join(parent, "native-binding-producer-"));
};
const outputArgument = directory => relative(root, directory).replaceAll(sep, "/");
const fixtureTool = {
  executable: process.execPath,
  version: process.version,
  executable_sha256: createHash("sha256").update(readFileSync(process.execPath)).digest("hex"),
  version_sha256: createHash("sha256").update(process.version).digest("hex"),
};
const fixtureResolveTool = () => fixtureTool;

test("native binding producer refuses a failed Cargo build without a receipt", async () => {
  const parent = freshOutput();
  const output = join(parent, "output");
  try {
    await assert.rejects(
      runNativeBindingProducer({
        outputArgument: outputArgument(output),
        resolveTool: fixtureResolveTool,
        execute: async () => ({ status: 1, signal: null, error: null, stdout: "", stderr: "cargo failed" }),
      }),
      /Cargo build failed/u,
    );
    assert.equal(existsSync(join(output, "producer-receipt.json")), false);
  } finally { rmSync(parent, { recursive: true, force: true }); }
});

test("native binding producer refuses an unavailable Cargo identity", async () => {
  const parent = freshOutput();
  const output = join(parent, "output");
  try {
    await assert.rejects(
      runNativeBindingProducer({
        outputArgument: outputArgument(output),
        resolveTool: () => { throw new Error("missing cargo tool"); },
        execute: async () => { throw new Error("must not dispatch"); },
      }),
      /missing cargo tool/u,
    );
    assert.equal(existsSync(join(output, "producer-receipt.json")), false);
  } finally { rmSync(parent, { recursive: true, force: true }); }
});

test("native binding producer refuses a successful Cargo exit without the expected output", async () => {
  const parent = freshOutput();
  const output = join(parent, "output");
  try {
    await assert.rejects(
      runNativeBindingProducer({
        outputArgument: outputArgument(output),
        resolveTool: fixtureResolveTool,
        execute: async () => ({ status: 0, signal: null, error: null, stdout: "", stderr: "" }),
      }),
      /did not produce/u,
    );
    assert.equal(existsSync(join(output, "producer-receipt.json")), false);
  } finally { rmSync(parent, { recursive: true, force: true }); }
});

test("native binding receipt binds actual Cargo argv and rejects tampered bytes", async () => {
  const parent = freshOutput();
  const output = join(parent, "output");
  try {
    const result = await runNativeBindingProducer({
      outputArgument: outputArgument(output),
      resolveTool: fixtureResolveTool,
      attemptNonce: "fixture-native-binding-attempt",
      now: (() => { let index = 0; return () => new Date(`2026-10-06T00:00:0${index++}.000Z`); })(),
      execute: async (_executable, args) => {
        const target = args.at(-1);
        mkdirSync(join(target, "debug"), { recursive: true });
        writeFileSync(join(target, "debug", "acyclic_fs_napi.dll"), Buffer.from("MZ fixture native binding bytes"));
        return { status: 0, signal: null, error: null, stdout: "cargo build passed", stderr: "" };
      },
    });
    const receipt = JSON.parse(readFileSync(result.receiptPath, "utf8"));
    assert.equal(receipt.producer_id, "graphcoder-native-binding-producer");
    assert.equal(receipt.tool.executable, process.execPath);
    assert.deepEqual(receipt.build.argv, receipt.tool.args);
    assert.ok(receipt.build.argv.includes("-j1"));
    assert.equal(existsSync(receipt.build.stdout_path), true);
    assert.equal(existsSync(receipt.build.stderr_path), true);
    assert.equal(receipt.sha256, result.record.sha256);
    verifyNativeBindingReceipt({ artifactPath: result.bindingPath, receiptPath: result.receiptPath });
    writeFileSync(receipt.build.stdout_path, "tampered cargo output");
    assert.throws(
      () => verifyNativeBindingReceipt({ artifactPath: result.bindingPath, receiptPath: result.receiptPath }),
      /Cargo stdout log digest does not match/u,
    );
    writeFileSync(result.bindingPath, Buffer.from("MZ tampered native binding bytes"));
    assert.throws(
      () => verifyNativeBindingReceipt({ artifactPath: result.bindingPath, receiptPath: result.receiptPath }),
      /Cargo stdout log digest does not match|producer receipt does not match artifact bytes|artifact digest does not match/u,
    );
  } finally { rmSync(parent, { recursive: true, force: true }); }
});

test("native binding producer rejects a source identity change during Cargo", async () => {
  const parent = freshOutput();
  const output = join(parent, "output");
  const first = { source_commit: "a".repeat(40), source_tree: "b".repeat(40), source_working_tree_sha256: "c".repeat(64) };
  const second = { ...first, source_working_tree_sha256: "d".repeat(64) };
  let identityCall = 0;
  try {
    await assert.rejects(
      runNativeBindingProducer({
        outputArgument: outputArgument(output),
        resolveTool: fixtureResolveTool,
        sourceIdentityFn: () => identityCall++ === 0 ? first : second,
        execute: async (_executable, args) => {
          const target = args.at(-1);
          mkdirSync(join(target, "debug"), { recursive: true });
          writeFileSync(join(target, "debug", "acyclic_fs_napi.dll"), Buffer.from("MZ fixture native binding bytes"));
          return { status: 0, signal: null, error: null, stdout: "cargo build passed", stderr: "" };
        },
      }),
      /source identity changed during Cargo build/u,
    );
    assert.equal(existsSync(join(output, "producer-receipt.json")), false);
  } finally { rmSync(parent, { recursive: true, force: true }); }
});
