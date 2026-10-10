import { expect, test } from "bun:test";
import { SnapshotStore } from "../src/client-snapshot.js";
import { publishClientSnapshots, receiveClientSnapshots } from "../src/client-worker.js";

const codec = {
  encode(value: number): Uint8Array { return new TextEncoder().encode(String(value)); },
  decode(bytes: Uint8Array): number { return Number(new TextDecoder().decode(bytes)); },
};

test("optional selected snapshot channel coalesces under backpressure and disposes", async () => {
  const { port1, port2 } = new MessageChannel();
  const source = new SnapshotStore(1);
  const errors: unknown[] = [];
  let encodes = 0;
  const stop = publishClientSnapshots(source, port1, { ...codec, encode(value) { encodes++; return codec.encode(value); } }, 16, error => errors.push(error));
  source.publish(2); source.publish(3);
  expect(encodes).toBe(1);
  const target = receiveClientSnapshots(0, port2, codec, 16, error => errors.push(error));
  const received: number[] = [];
  const done = Promise.withResolvers<void>();
  const unsubscribe = target.subscribe(() => {
    received.push(target.getSnapshot());
    if (target.getSnapshot() === 3) done.resolve();
  });
  try {
    await done.promise;
    expect(received).toEqual([1, 3]);
    expect(encodes).toBe(2);
    expect(errors).toEqual([]);
    stop(); stop(); target.dispose(); target.dispose(); unsubscribe();
    source.publish(4);
    expect(encodes).toBe(2);
  } finally { stop(); target.dispose(); }
});

test("oversized publisher stops once and detaches its source", () => {
  const { port1, port2 } = new MessageChannel();
  const source = new SnapshotStore(100);
  const errors: unknown[] = [];
  const stop = publishClientSnapshots(source, port1, codec, 1, error => errors.push(error));
  source.publish(200);
  expect(errors).toHaveLength(1);
  stop(); port2.close();
});

test("receiver rejects reordered/oversized snapshots before decoding", async () => {
  for (const frame of [
    { kind: "snapshot", sequence: 2, bytes: new Uint8Array([1]) },
    { kind: "snapshot", sequence: 1, bytes: new Uint8Array(17) },
  ]) {
    const { port1, port2 } = new MessageChannel();
    let decodes = 0;
    const failed = Promise.withResolvers<unknown>();
    const target = receiveClientSnapshots(0, port2, { ...codec, decode(bytes) { decodes++; return codec.decode(bytes); } }, 16, error => failed.resolve(error));
    port1.postMessage(frame);
    try {
      expect(await failed.promise).toBeInstanceOf(Error);
      expect(decodes).toBe(0);
      expect(target.getSnapshot()).toBe(0);
    } finally { target.dispose(); port1.close(); }
  }
});
