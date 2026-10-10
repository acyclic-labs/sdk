import { expect, test } from "bun:test";
import { SnapshotStore } from "../src/client-snapshot.js";
import { clientReadable } from "../src/svelte.js";

test("construction/read are effect-free, last detach and dispose are idempotent", () => {
  let attached = 0;
  let detached = 0;
  const initial = Object.freeze({ value: 1 });
  const store = new SnapshotStore(initial, () => { attached++; return () => { detached++; }; });
  expect(store.getSnapshot()).toBe(initial);
  expect(attached).toBe(0);
  const callback = () => {};
  const first = store.subscribe(callback);
  const second = store.subscribe(callback);
  expect(attached).toBe(1);
  first(); first();
  expect(detached).toBe(0);
  second();
  expect(detached).toBe(1);
  const third = store.subscribe(callback);
  expect(attached).toBe(2);
  store.dispose(); store.dispose(); third();
  expect(detached).toBe(2);
  expect(() => store.subscribe(callback)).toThrow("disposed");
});

test("same snapshot has no notification and disposed/released callbacks never run", () => {
  const store = new SnapshotStore(0);
  let calls = 0;
  let release = () => {};
  store.subscribe(() => { calls++; release(); });
  release = store.subscribe(() => { throw new Error("removed callback"); });
  store.publish(0);
  expect(calls).toBe(0);
  store.publish(1);
  expect(calls).toBe(1);
  store.dispose();
  store.publish(2);
  expect(store.getSnapshot()).toBe(1);
});

test("Svelte immediate synchronous subscribe, upstream initial update and cleanup", () => {
  let detached = 0;
  const store = new SnapshotStore(0, publish => { publish(1); return () => { detached++; }; });
  const values: number[] = [];
  const stop = clientReadable(store).subscribe(value => { values.push(value); });
  expect(values).toEqual([1]);
  store.publish(2);
  expect(values).toEqual([1, 2]);
  stop(); stop();
  expect(detached).toBe(1);
  store.publish(3);
  expect(values).toEqual([1, 2]);
});

test("failed upstream attach and failed immediate consumer do not leak subscriptions", () => {
  let attempts = 0;
  const broken = new SnapshotStore(0, () => { attempts++; throw new Error("attach"); });
  expect(() => broken.subscribe(() => {})).toThrow("attach");
  expect(() => broken.subscribe(() => {})).toThrow("attach");
  expect(attempts).toBe(2);
  let stopped = 0;
  const store = new SnapshotStore(0, () => () => { stopped++; });
  expect(() => clientReadable(store).subscribe(() => { throw new Error("consumer"); })).toThrow("consumer");
  expect(stopped).toBe(1);
});

test("a failed callback does not prevent later subscriptions observing publication", async () => {
  const errors: unknown[] = [];
  const store = new SnapshotStore(0, undefined, error => errors.push(error));
  store.subscribe(() => { throw new Error("subscriber"); });
  let observed = 0;
  store.subscribe(() => { observed = store.getSnapshot(); });
  store.publish(1);
  expect(observed).toBe(1);
  await Promise.resolve();
  expect(errors).toHaveLength(1);
  store.dispose();
});
