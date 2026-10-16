import { expect, test } from "bun:test";
import { HydrationCache, PageWindow, type Page } from "../src/index.js";

test("hydration cache obeys entry and byte bounds", () => {
  const cache = new HydrationCache<string, string>(2, 5);
  cache.set("a", "aa", 2);
  cache.set("b", "bb", 2);
  cache.get("a");
  cache.set("c", "ccc", 3);
  expect(cache.get("b")).toBeUndefined();
  expect(cache.bytes).toBe(5);
  expect(cache.size).toBe(2);
});

test("page window coalesces same-direction loads and deduplicates items", async () => {
  let calls = 0;
  const window = new PageWindow(
    (item: { id: string }) => item.id,
    async () => {
      calls += 1;
      await Promise.resolve();
      return {
        generation: "one",
        items: [{ id: "a" }, { id: "a" }, { id: "b" }],
        hasMoreBefore: false,
        hasMoreAfter: false,
      };
    },
    10,
  );
  await Promise.all([window.load("before"), window.load("before")]);
  expect(calls).toBe(1);
  expect(window.items.map(item => item.id)).toEqual(["a", "b"]);
});

test("page reset fences an older in-flight response", async () => {
  let release: ((page: Page<{ id: string }, unknown>) => void) | undefined;
  const window = new PageWindow(
    (item: { id: string }) => item.id,
    () => new Promise<Page<{ id: string }, unknown>>(resolve => { release = resolve; }),
    10,
  );
  const stale = window.load("before");
  window.reset({
    generation: "two",
    items: [{ id: "fresh" }],
    hasMoreBefore: false,
    hasMoreAfter: false,
  });
  release?.({
    generation: "one",
    items: [{ id: "stale" }],
    hasMoreBefore: false,
    hasMoreAfter: false,
  });
  expect(await stale).toEqual([]);
  expect(window.items.map(item => item.id)).toEqual(["fresh"]);
});

test("page window evicts the opposite edge at maxItems after loads and oversized reset", async () => {
  type Item = { id: string; value: number };
  const pages: Page<Item, string>[] = [
    { generation: "one", items: [{ id: "c", value: 2 }, { id: "d", value: 1 }], after: "d", hasMoreBefore: true, hasMoreAfter: true },
    { generation: "one", items: [{ id: "e", value: 1 }, { id: "f", value: 1 }, { id: "g", value: 1 }], after: "g", hasMoreBefore: true, hasMoreAfter: true },
    { generation: "one", items: [{ id: "b", value: 2 }, { id: "c", value: 3 }], before: "b", hasMoreBefore: false, hasMoreAfter: true },
  ];
  const requests: [string, string | undefined][] = [];
  const window = new PageWindow<Item, string>(item => item.id, async (direction, cursor) => {
    requests.push([direction, cursor]);
    const page = pages.shift();
    if (page === undefined) throw new Error("unexpected page request");
    return page;
  }, 2);
  window.reset({ generation: "one", items: [{ id: "b", value: 1 }, { id: "c", value: 1 }], before: "b", after: "c", hasMoreBefore: true, hasMoreAfter: true });
  expect(window.items).toEqual([{ id: "b", value: 1 }, { id: "c", value: 1 }]);
  expect(await window.load("after")).toEqual([{ id: "d", value: 1 }]);
  expect(window.items).toEqual([{ id: "c", value: 2 }, { id: "d", value: 1 }]);
  expect(await window.load("after")).toEqual([{ id: "f", value: 1 }, { id: "g", value: 1 }]);
  expect(window.items).toEqual([{ id: "f", value: 1 }, { id: "g", value: 1 }]);
  expect(await window.load("before")).toEqual([{ id: "b", value: 2 }, { id: "c", value: 3 }]);
  expect(window.items).toEqual([{ id: "b", value: 2 }, { id: "c", value: 3 }]);
  expect(requests).toEqual([["after", "c"], ["after", "d"], ["before", "b"]]);
  expect(await window.load("before")).toEqual([]);
  expect(requests).toHaveLength(3);

  window.reset({ generation: "two", items: [{ id: "x", value: 1 }, { id: "y", value: 1 }, { id: "z", value: 1 }, { id: "w", value: 1 }], hasMoreBefore: false, hasMoreAfter: false });
  expect(window.items).toEqual([{ id: "z", value: 1 }, { id: "w", value: 1 }]);
  expect(window.hasMoreBefore).toBe(false);
  expect(window.hasMoreAfter).toBe(false);
});

test("a new page generation fences both stale page directions without clearing a fresh in-flight load", async () => {
  type Item = { id: string };
  const requests: { direction: string; cursor: string | undefined; resolve: (page: Page<Item, string>) => void }[] = [];
  const window = new PageWindow<Item, string>(item => item.id, (direction, cursor) =>
    new Promise(resolve => { requests.push({ direction, cursor, resolve }); }), 2);
  window.reset({ generation: "old", items: [{ id: "old" }], before: "old-before", after: "old-after", hasMoreBefore: true, hasMoreAfter: true });
  const staleBefore = window.load("before");
  const staleAfter = window.load("after");
  {
    window.reset({ generation: "new", items: [{ id: "fresh-a" }, { id: "fresh-b" }, { id: "fresh-c" }], before: "new-before", after: "new-after", hasMoreBefore: true, hasMoreAfter: true });
  }
  expect(window.items).toEqual([{ id: "fresh-b" }, { id: "fresh-c" }]);
  const freshBefore = window.load("before");
  expect(requests.map(({ direction, cursor }) => [direction, cursor])).toEqual([
    ["before", "old-before"], ["after", "old-after"], ["before", "new-before"],
  ]);
  for (const request of requests.slice(0, 2)) {
    request.resolve({ generation: "old", items: [{ id: "stale" }], before: "stale-before", after: "stale-after", hasMoreBefore: false, hasMoreAfter: false });
  }
  expect(await staleBefore).toEqual([]);
  expect(await staleAfter).toEqual([]);
  expect(window.items).toEqual([{ id: "fresh-b" }, { id: "fresh-c" }]);
  expect(window.hasMoreBefore).toBe(true);
  expect(window.hasMoreAfter).toBe(true);
  expect(window.load("before")).toBe(freshBefore);
  expect(requests).toHaveLength(3);
  requests[2]?.resolve({ generation: "new", items: [{ id: "fresh-a" }], before: "new-start", hasMoreBefore: false, hasMoreAfter: true });
  expect(await freshBefore).toEqual([{ id: "fresh-a" }]);
  expect(window.items).toEqual([{ id: "fresh-a" }, { id: "fresh-b" }]);
  expect(window.hasMoreBefore).toBe(false);
  expect(window.hasMoreAfter).toBe(true);
});
