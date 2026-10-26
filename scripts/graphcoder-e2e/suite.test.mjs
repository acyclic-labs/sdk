import assert from "node:assert/strict";
import test from "node:test";
import { withOwnedCase } from "./suite.mjs";

test("owned cases join cleanup before returning or propagating failure", async () => {
  for (const failed of [false, true]) {
    const drain = Promise.withResolvers(), started = Promise.withResolvers();
    const failure = new Error("selected scenario failed");
    let settled = false, closes = 0;
    const scope = { async close() { closes++; started.resolve(); await drain.promise; } };
    const running = withOwnedCase({ processes: scope, run: async () => {
      if (failed) throw failure;
      return "scenario result";
    } });
    running.then(() => settled = true, () => settled = true);
    await Promise.resolve();
    assert.equal(closes, 1, "every selected case must join the supplied process owner's drain");
    await started.promise;
    assert.equal(settled, false, "selected case must retain custody until physical drain");
    drain.resolve();
    if (failed) await assert.rejects(running, error => error === failure);
    else {
      const receipt = await running;
      assert.equal(receipt.run, "scenario result");
      assert.equal(receipt.expired, false);
      assert.ok(receipt.seconds > 0 && receipt.seconds < 10);
    }
  }
});

test("one virtual case deadline closes the supplied scope and joins its drain", async context => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const complete = Promise.withResolvers(), drain = Promise.withResolvers();
  let closes = 0, settled = false;
  const scope = { async close() { closes++; complete.resolve("interrupted"); await drain.promise; } };
  const running = withOwnedCase({ processes: scope, run: () => complete.promise });
  running.then(() => settled = true);
  context.mock.timers.tick(8999);
  assert.equal(closes, 0);
  context.mock.timers.tick(1);
  assert.equal(closes, 1, "the common nine-second reservation must close the caller's scope");
  await complete.promise;
  assert.equal(settled, false);
  drain.resolve();
  const receipt = await running;
  assert.equal(receipt.expired, true, "expiry must remain a failed receipt even if interruption returns normally");
  assert.equal(receipt.run, "interrupted");
  assert.equal(closes, 1, "deadline and finally share one drain");
});
