import { expect, test } from "bun:test";
import { SessionQueue } from "../src/session-queue.js";

test("ring wraps and releases consumed slots while preserving undefined values", async () => {
  const queue = new SessionQueue<number | undefined>(3);
  const reader = queue[Symbol.asyncIterator]();
  queue.push(undefined); queue.push(1); queue.push(2);
  expect(await reader.next()).toEqual({ value: undefined, done: false });
  expect((await reader.next()).value).toBe(1);
  queue.push(3); queue.push(4);
  expect(queue.available).toBe(0);
  expect((await reader.next()).value).toBe(2);
  expect((await reader.next()).value).toBe(3);
  expect((await reader.next()).value).toBe(4);
  expect(queue.available).toBe(3);
  queue.end();
  expect((await reader.next()).done).toBe(true);
});

test("discard admits waiting producers before disposing the detached old values", async () => {
  const queue = new SessionQueue<number>(2);
  const reader = queue[Symbol.asyncIterator]();
  queue.push(0); queue.push(1);
  expect((await reader.next()).value).toBe(0);
  queue.push(2); // logical order wraps around the backing array
  const waiting = queue.send(3);
  const discarded: number[] = [];
  queue.discard(value => {
    expect(queue.available).toBe(1); // the waiting writer already owns one slot
    discarded.push(value);
  });
  await waiting;
  expect(discarded).toEqual([1, 2]);
  expect((await reader.next()).value).toBe(3);
  queue.end();
  expect((await reader.next()).done).toBe(true);
});

test("terminal errors follow buffered records and settle blocked producers", async () => {
  const queue = new SessionQueue<number>(1);
  queue.push(1);
  const writing = queue.send(2).then(() => "accepted", error => error);
  const error = new Error("terminal");
  queue.end(error);
  expect(await writing).toBe(error);
  const reader = queue[Symbol.asyncIterator]();
  expect((await reader.next()).value).toBe(1);
  await expect(reader.next()).rejects.toBe(error);
  expect((await reader.next()).done).toBe(true);
  expect(queue.available).toBe(1);
});


test("unbounded ingress preserves bursts, wrapped growth and detached disposal", async () => {
  const queue = new SessionQueue<number | undefined>();
  const reader = queue[Symbol.asyncIterator]();
  for (let value = 0; value < 64; value += 1) queue.push(value);
  for (let value = 0; value < 37; value += 1) expect((await reader.next()).value).toBe(value);
  for (let value = 64; value < 3073; value += 1) queue.push(value);
  queue.push(undefined);
  for (let value = 37; value < 3073; value += 1) expect((await reader.next()).value).toBe(value);
  expect((await reader.next()).value).toBeUndefined();
  for (let value = 0; value < 1025; value += 1) queue.push(value);
  const discarded: (number | undefined)[] = [];
  queue.discard(value => {
    discarded.push(value);
    // Reentrant production grows the NEW buffer while the wrapped old one
    // is still retiring. Its original geometry must stay immutable.
    for (let copy = 0; copy < 4; copy += 1) queue.push(value);
  });
  expect(discarded).toEqual(Array.from({ length: 1025 }, (_, value) => value));
  for (let value = 0; value < 1025; value += 1) {
    for (let copy = 0; copy < 4; copy += 1) expect((await reader.next()).value).toBe(value);
  }
  queue.end();
  expect((await reader.next()).done).toBe(true);
});
