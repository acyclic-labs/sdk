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
