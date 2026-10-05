import assert from "node:assert/strict";
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

test("native binding producer refuses a failed Cargo build without a receipt", async () => {
  const parent = freshOutput();
  const output = join(parent, "output");
  try {
    await assert.rejects(
      runNativeBindingProducer({
        outputArgument: outputArgument(output),
        execute: async () => ({ status: 1, signal: null, error: null, stdout: "", stderr: "cargo failed" }),
      }),
      /Cargo build failed/u,
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
    assert.equal(receipt.tool.executable, "cargo");
    assert.deepEqual(receipt.build.argv, receipt.tool.args);
    assert.equal(receipt.sha256, result.record.sha256);
    verifyNativeBindingReceipt({ artifactPath: result.bindingPath, receiptPath: result.receiptPath });
    writeFileSync(result.bindingPath, Buffer.from("MZ tampered native binding bytes"));
    assert.throws(
      () => verifyNativeBindingReceipt({ artifactPath: result.bindingPath, receiptPath: result.receiptPath }),
      /producer receipt does not match artifact bytes|artifact digest does not match/u,
    );
  } finally { rmSync(parent, { recursive: true, force: true }); }
});
