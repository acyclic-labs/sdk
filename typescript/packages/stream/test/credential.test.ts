import { expect, test } from "bun:test";
import fc from "fast-check";
import { webSocketSession } from "../src/session-websocket.js";

test("every browser session attempt resolves a fresh credential before dialing", async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 16 }), async values => {
    let calls = 0;
    const sent: string[][] = [];
    const stopped = new Error("stop before opening a socket");
    const connect = webSocketSession("https://streams.example.test", () => values[calls++]!, (_url, protocols) => {
      sent.push(protocols);
      throw stopped;
    });
    for (const _value of values) await expect(connect(undefined, new AbortController().signal)).rejects.toBe(stopped);
    expect(sent).toEqual(values.map(value => ["acyclic.session.v1", `acyclic.bearer.${Buffer.from(value).toString("base64url")}`]));
    await expect(connect(undefined, new AbortController().signal)).rejects.toThrow(TypeError);
    expect(sent.length).toBe(values.length);
    const controller = new AbortController();
    controller.abort(stopped);
    await expect(connect(undefined, controller.signal)).rejects.toBe(stopped);
    expect(calls).toBe(values.length + 1);
  }), { numRuns: 64 });
});

test("cancellation while renewing prevents the browser dial", async () => {
  await fc.assert(fc.asyncProperty(fc.stringMatching(/^[a-z]{1,32}$/), async value => {
    const controller = new AbortController();
    const stopped = new Error("cancelled");
    let dials = 0;
    const connect = webSocketSession("https://streams.example.test", signal => {
      expect(signal).toBe(controller.signal);
      controller.abort(stopped);
      return Promise.resolve(value);
    }, () => { dials++; throw new Error("cancelled renewal dialed"); });
    await expect(connect(undefined, controller.signal)).rejects.toBe(stopped);
    expect(dials).toBe(0);
  }), { numRuns: 64 });
});
