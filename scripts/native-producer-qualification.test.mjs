import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { nativeFamily } from "./native-family.mjs";

const module = await import(process.env.NATIVE_PRODUCER_UNDER_TEST ?? "./build-native-family.mjs");
const producer = module.createNativeProducer(nativeFamily("stream"));
// Captured real compiler/generator output, not a manufactured tool fixture.
// Mode mutations below test admission only; they are never build evidence.
const captured = JSON.parse(await readFile(new URL("./fixtures/native-producer-captured-20261015.json", import.meta.url), "utf8"));
function developmentInputs() {
  const inputs = structuredClone(captured.raw_build_inputs);
  inputs.schema += ".development-only";
  inputs.profile.name = "dev";
  inputs.generator.options.release = false;
  return inputs;
}

test("captured release receipt remains production-only", () => {
  producer.assertBuildInputs(captured.raw_build_inputs);
  assert.throws(() => producer.assertBuildInputs(captured.raw_build_inputs, { qualificationDev: true }), /unsupported schema/);
  assert.equal(producer.buildInputsReceipt(captured.raw_build_inputs, captured.raw_build_inputs).schema,
    "acyclic.stream.native-build-inputs-receipt.v1");
});

test("dev qualification preserves captured provenance and cannot qualify production", () => {
  const inputs = developmentInputs();
  producer.assertBuildInputs(inputs, { qualificationDev: true });
  const receipt = producer.buildInputsReceipt(inputs, inputs, { qualificationDev: true });
  assert.equal(receipt.schema, "acyclic.stream.native-build-inputs-receipt.v1.development-only");
  assert.deepEqual(receipt.raw_build_inputs.compiler, captured.raw_build_inputs.compiler);
  assert.deepEqual(receipt.raw_build_inputs.linker, captured.raw_build_inputs.linker);
  assert.throws(() => producer.assertBuildInputs(inputs), /unsupported schema/);
  assert.throws(() => producer.buildInputsReceipt(inputs, inputs), /unsupported schema/);
  // Rewriting just the outer marker cannot turn dev compiler options into
  // release evidence. Each independent admission barrier remains necessary.
  inputs.schema = captured.raw_build_inputs.schema;
  assert.throws(() => producer.assertBuildInputs(inputs), /generator options/);
  inputs.generator.options.release = true;
  assert.throws(() => producer.assertBuildInputs(inputs), /profile is not release/);
});

test("stage has no development mode even when explicitly requested", async () => {
  await assert.rejects(producer.main(["stage", "--qualification-dev", "--bundle", "unused"]),
    /step=qualification-mode.*development-only evidence cannot be staged.*recovery=/);
});
