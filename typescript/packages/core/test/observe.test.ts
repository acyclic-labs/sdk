import { expect, test } from "bun:test";
import { observedIterable, type OperationEvent } from "../src/observe.js";

test("iterable observations retain late failures and report after original cleanup", async () => {
  for (const terminal of ["error", "return", "end"] as const) {
    const events: OperationEvent[] = [];
    const failure = Object.assign(new Error("producer lost"), { code: "unavailable" });
    let released = false;
    const source = (async function* () {
      try {
        yield 1;
        if (terminal === "error") throw failure;
      } finally { released = true; }
    })();
    const iterator = observedIterable({ onOperation(event) {
      expect(released).toBe(true);
      events.push(event);
    } }, "stream", "read", source)[Symbol.asyncIterator]();
    expect(await iterator.next()).toEqual({ value: 1, done: false });
    expect(events).toHaveLength(0);
    if (terminal === "error") await expect(iterator.next()).rejects.toBe(failure);
    else if (terminal === "return") await iterator.return!();
    else expect(await iterator.next()).toHaveProperty("done", true);
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({ family: "stream", op: "read", ok: terminal !== "error" });
    if (terminal === "error") expect(events[0]).toHaveProperty("code", "unavailable");
  }
});
