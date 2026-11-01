import assert from "node:assert/strict";
import { once } from "node:events";
import test from "node:test";
import { deadlineSignal } from "../dist/deadline.js";

test("host timers preserve long deadlines while finite expiry and cancellation retire their owners", async () => {
  const parent = new AbortController();
  const expired = new Error("deadline expired");
  const long = deadlineSignal(0xffff_ffff + 1000, () => expired, parent.signal);
  const huge = deadlineSignal((1n << 64n) - 1n, () => expired, parent.signal);
  const short = deadlineSignal(10, () => expired);
  try {
    await once(short.signal, "abort");
    assert.equal(short.signal.reason, expired);
    assert.equal(long.signal.aborted, false);
    assert.equal(huge.signal.aborted, false);
    const cancelled = new Error("caller cancelled");
    parent.abort(cancelled);
    assert.equal(long.signal.reason, cancelled);
    assert.equal(huge.signal.reason, cancelled);
  } finally { long.cleanup(); huge.cleanup(); short.cleanup(); }
});
