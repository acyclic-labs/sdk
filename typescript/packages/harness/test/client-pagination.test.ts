import { expect, test } from "bun:test";
import { HydrationCache, initializeClientQueries, PageWindow, type Page } from "../src/index.js";

await initializeClientQueries();

type Item = { id: string };
const encode = (page: Page): Uint8Array => new TextEncoder().encode(JSON.stringify(page));
const page = (ids: readonly string[], before: string, after: string,
  hasMoreBefore = true, hasMoreAfter = true, generation = "one"): Uint8Array => encode({
    generation, items: ids.map(id => ({ key: id, value: { id } })),
    before, after, hasMoreBefore, hasMoreAfter,
  });
const ids = (window: PageWindow): string[] => (window.items() as readonly Item[]).map(item => item.id);

// Existing unrelated hydration oracle remains intact.
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

test("one physical fetch owns both page directions until settlement", () => {
  using window = new PageWindow(2, 4, 4096);
  using request = window.begin("after")!;
  expect(request.cursor()).toBeUndefined();
  expect(() => window.begin("after")).toThrow();
  expect(() => window.begin("before")).toThrow();
  window.finish(request, page(["a", "b"], "start", "end", false, false));
  expect(ids(window)).toEqual(["a", "b"]);
  expect(window.begin("before")).toBeUndefined();
  expect(window.begin("after")).toBeUndefined();
});

test("exclusive provider pages refuse duplicate identities atomically", () => {
  using window = new PageWindow(2, 4, 4096);
  window.reset(page(["a"], "start", "a"));
  const retained = window.items();
  using request = window.begin("after")!;
  // The old Map silently admitted this nonexclusive provider response.
  expect(() => window.finish(request, page(["a", "b"], "a", "b")))
    .toThrow("duplicate key");
  expect(window.items()).toBe(retained);
  expect(ids(window)).toEqual(["a"]);
  using next = window.begin("after")!;
  expect(next.cursor()).toBe("a");
  window.finish(next, undefined);
});

test("reset fences the response but retains physical fetch custody", () => {
  using window = new PageWindow(2, 4, 4096);
  window.reset(page(["old"], "old-before", "old-after"));
  using stale = window.begin("before")!;
  window.reset(page(["fresh"], "new-before", "new-after", true, true, "two"));
  expect(() => window.begin("before")).toThrow("not settled");
  expect(() => window.finish(stale, page(["stale"], "x", "y")))
    .toThrow("predates the reset");
  expect(ids(window)).toEqual(["fresh"]);
  using fresh = window.begin("before")!;
  expect(fresh.cursor()).toBe("new-before");
  window.finish(fresh, undefined);
});

test("whole-page eviction moves both opaque exclusive edges without holes", () => {
  using window = new PageWindow(2, 4, 4096);
  window.reset(page(["a", "b"], "opaque-0", "opaque-2", false, true));
  using first = window.begin("after")!;
  expect(first.cursor()).toBe("opaque-2");
  window.finish(first, page(["c", "d"], "opaque-2", "opaque-4"));
  using second = window.begin("after")!;
  expect(second.cursor()).toBe("opaque-4");
  window.finish(second, page(["e", "f"], "opaque-4", "opaque-6", true, false));
  expect(ids(window)).toEqual(["c", "d", "e", "f"]);
  expect(window.pageCount()).toBe(2);
  expect(window.hasMore("before")).toBe(true);
  using backfill = window.begin("before")!;
  // The previous TS implementation left this at the evicted opaque-0 edge.
  expect(backfill.cursor()).toBe("opaque-2");
  window.finish(backfill, page(["a", "b"], "opaque-0", "opaque-2", false, true));
  expect(ids(window)).toEqual(["a", "b", "c", "d"]);
  expect(window.hasMore("after")).toBe(true);
  using forward = window.begin("after")!;
  expect(forward.cursor()).toBe("opaque-4");
  window.finish(forward, undefined);
});

test("oversized and changed-cut pages refuse without truncating provider custody", () => {
  using window = new PageWindow(2, 2, 4096);
  window.reset(page(["a"], "0", "1"));
  const retained = window.items();
  expect(() => window.reset(page(["x", "y", "z"], "0", "3"))).toThrow("capacity");
  expect(window.items()).toBe(retained);
  using request = window.begin("after")!;
  expect(() => window.finish(request, page(["b"], "1", "2", true, true, "two")))
    .toThrow("generation changed");
  expect(window.items()).toBe(retained);
  using invalid = window.begin("after")!;
  expect(() => window.finish(invalid, new TextEncoder().encode("broken"))).toThrow();
  using next = window.begin("after")!;
  expect(next.cursor()).toBe("1");
  window.finish(next, undefined);
});

test("unchanged reads reuse the exported immutable array", () => {
  using window = new PageWindow(2, 2, 4096);
  window.reset(page(["a"], "0", "1"));
  const retained = window.items();
  expect(Object.isFrozen(retained)).toBe(true);
  for (let read = 0; read < 100; read++) expect(window.items()).toBe(retained);
  expect(window.retainedBytes()).toBeGreaterThan(0n);
  expect(window.retainedBytes()).toBeLessThanOrEqual(4096n);
});

test("page values retain exact integers and ordinary object property access", () => {
  using window = new PageWindow(1, 1, 4096);
  window.reset(new TextEncoder().encode('{"generation":"one","items":[{"key":"a","value":{"id":"a","inbox":9007199254740993}}],"before":"0","after":"1","hasMoreBefore":false,"hasMoreAfter":false}'));
  const values = window.items() as readonly { id: string; inbox: bigint }[];
  expect(values[0]?.id).toBe("a");
  expect(values[0]?.inbox).toBe(9007199254740993n);
});
