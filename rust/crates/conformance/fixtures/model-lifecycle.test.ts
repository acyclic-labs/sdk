import assert from "node:assert/strict";
import test from "node:test";
import { ModelLifecycle, modelObservations } from "./model-lifecycle.js";

test("actual entry and abort observations replay without scalar probes", async () => {
  const lifecycle = new ModelLifecycle();
  lifecycle.started();
  lifecycle.aborted();
  const observations = modelObservations(lifecycle.observe());
  assert.equal((await observations.next()).value, "started");
  assert.equal((await observations.next()).value, "aborted");
  assert.equal((await observations.next()).done, true);
  assert.throws(() => lifecycle.observe(), /already claimed/);
});

test("missing entry and ignored abort both refuse within the fixture deadline", async () => {
  for (const entered of [false, true]) {
    const lifecycle = new ModelLifecycle();
    if (entered) lifecycle.started();
    const observations = modelObservations(lifecycle.observe(), 10);
    if (entered) assert.equal((await observations.next()).value, "started");
    await assert.rejects(observations.next(), /missing lifecycle event/);
  }
});

test("abort without real entry and a missing actual abort cannot pass", async () => {
  const lifecycle = new ModelLifecycle();
  assert.throws(() => lifecycle.aborted(), /invalid lifecycle order/);
  const truncated = new ReadableStream<Uint8Array>({ start(controller) {
    controller.enqueue(Uint8Array.of(1));
    controller.close();
  } });
  const observations = modelObservations(truncated);
  assert.equal((await observations.next()).value, "started");
  await assert.rejects(observations.next(), /incomplete/);
});
