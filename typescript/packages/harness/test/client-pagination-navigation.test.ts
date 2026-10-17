import { expect, test } from "bun:test";
import { PageWindow, type Page, type PageDirection } from "../src/pagination.js";

type Item = { id: string; sequence: number; value: number };
type Cursor = { direction: PageDirection; boundary: number };
const history: readonly Item[] = Array.from({ length: 10 }, (_, index) => ({
  id: `message-${index * 17 + 29}`, sequence: index + 1, value: 0,
}));

function cursor(item: Item, direction: PageDirection): Cursor {
  return { direction, boundary: item.sequence };
}

function page(items: readonly Item[], generation = "pinned"): Page<Item, Cursor> {
  const first = items.at(0), last = items.at(-1);
  return {
    generation, items,
    ...(first === undefined ? {} : { before: cursor(first, "before") }),
    ...(last === undefined ? {} : { after: cursor(last, "after") }),
    hasMoreBefore: first !== undefined && first.sequence > 1,
    hasMoreAfter: last !== undefined && last.sequence < history.length,
  };
}

function fixture(first: number, last: number, maximum = 2, itemCursors = false) {
  const requests: { direction: PageDirection; cursor: Cursor | undefined }[] = [];
  const window = new PageWindow<Item, Cursor>(item => item.id, async (direction, anchor) => {
    requests.push({ direction, cursor: anchor });
    if (anchor !== undefined && anchor.direction !== direction) throw new Error("wrong cursor direction");
    return page(direction === "before"
      ? history.filter(item => item.sequence < (anchor?.boundary ?? history.length + 1)).slice(-2)
      : history.filter(item => item.sequence > (anchor?.boundary ?? 0)).slice(0, 2));
  }, maximum, itemCursors ? cursor : undefined);
  window.reset(page(history.filter(item => item.sequence >= first && item.sequence <= last)));
  return { window, requests };
}

const sequences = (window: PageWindow<Item, Cursor>): number[] => window.items.map(item => item.sequence);

test("paging without eviction keeps both original window edges", async () => {
  const { window, requests } = fixture(3, 4, 8);
  await window.load("after");
  await window.load("before");
  expect(sequences(window)).toEqual([1, 2, 3, 4, 5, 6]);
  expect(requests.map(request => request.cursor)).toEqual([
    { direction: "after", boundary: 4 }, { direction: "before", boundary: 3 },
  ]);
});

test("forward eviction reopens the beginning using the retained page's opaque cursor", async () => {
  const { window, requests } = fixture(1, 2);
  expect(window.hasMoreBefore).toBe(false);
  await window.load("after");
  expect(sequences(window)).toEqual([3, 4]);
  expect(window.hasMoreBefore).toBe(true);
  await window.load("before");
  expect(sequences(window)).toEqual([1, 2]);
  expect(requests.at(-1)?.cursor).toEqual({ direction: "before", boundary: 3 });
  expect(window.hasMoreBefore).toBe(false);
  expect(window.hasMoreAfter).toBe(true);
});

test("backward eviction reopens the end using the retained page's opaque cursor", async () => {
  const { window, requests } = fixture(9, 10);
  expect(window.hasMoreAfter).toBe(false);
  await window.load("before");
  expect(sequences(window)).toEqual([7, 8]);
  expect(window.hasMoreAfter).toBe(true);
  await window.load("after");
  expect(sequences(window)).toEqual([9, 10]);
  expect(requests.at(-1)?.cursor).toEqual({ direction: "after", boundary: 8 });
});

test("reversing after several evictions reloads the adjacent page", async () => {
  const { window, requests } = fixture(3, 4);
  await window.load("after");
  await window.load("after");
  expect(sequences(window)).toEqual([7, 8]);
  await window.load("before");
  expect(sequences(window)).toEqual([5, 6]);
  expect(requests.at(-1)?.cursor).toEqual({ direction: "before", boundary: 7 });
  await window.load("before");
  expect(sequences(window)).toEqual([3, 4]);
});

test("item cursors preserve both directions when the bound cuts through pages", async () => {
  const { window, requests } = fixture(1, 2, 3, true);
  await window.load("after");
  expect(sequences(window)).toEqual([2, 3, 4]);
  await window.load("before");
  expect(sequences(window)).toEqual([1, 2, 3]);
  await window.load("after");
  expect(sequences(window)).toEqual([3, 4, 5]);
  await window.load("before");
  expect(sequences(window)).toEqual([1, 2, 3]);
  expect(requests.map(request => request.cursor)).toEqual([
    { direction: "after", boundary: 2 }, { direction: "before", boundary: 2 },
    { direction: "after", boundary: 3 }, { direction: "before", boundary: 3 },
  ]);
});

for (const initialDirection of ["before", "after"] as const) {
  test(`item cursors support pages containing only their requested edge when starting ${initialDirection}`, async () => {
    const requests: { direction: PageDirection; cursor: Cursor | undefined }[] = [];
    const window = new PageWindow<Item, Cursor>(item => item.id, async (direction, anchor) => {
      requests.push({ direction, cursor: anchor });
      if (anchor === undefined || anchor.direction !== direction) throw new Error("missing directional cursor");
      const result = page(direction === "before"
        ? history.filter(item => item.sequence < anchor.boundary).slice(-2)
        : history.filter(item => item.sequence > anchor.boundary).slice(0, 2));
      if (direction === "before") {
        const { after, ...partial } = result;
        return partial;
      }
      const { before, ...partial } = result;
      return partial;
    }, 2, cursor);
    const initial = initialDirection === "before" ? history.slice(-2) : history.slice(0, 2);
    window.reset(page(initial));
    await window.load(initialDirection);
    const opposite = initialDirection === "before" ? "after" : "before";
    await window.load(opposite);
    expect(sequences(window)).toEqual(initial.map(item => item.sequence));
    expect(requests.at(-1)?.cursor).toEqual({ direction: opposite, boundary: opposite === "before" ? 3 : 8 });
  });
}

test("oversized reset retains a usable before cursor for discarded messages", async () => {
  const { window, requests } = fixture(1, 10, 3, true);
  expect(sequences(window)).toEqual([8, 9, 10]);
  expect(window.hasMoreBefore).toBe(true);
  expect(window.hasMoreAfter).toBe(false);
  await window.load("before");
  expect(sequences(window)).toEqual([6, 7, 8]);
  expect(requests.at(-1)?.cursor).toEqual({ direction: "before", boundary: 8 });
  await window.load("after");
  expect(sequences(window)).toEqual([8, 9, 10]);
});

test("a missing interior cursor rejects a load without changing its items or next request", async () => {
  const { window, requests } = fixture(1, 2, 3);
  await expect(window.load("after")).rejects.toThrow("cursorForItem");
  expect(sequences(window)).toEqual([1, 2]);
  expect(window.hasMoreBefore).toBe(false);
  expect(window.hasMoreAfter).toBe(true);
  await expect(window.load("after")).rejects.toThrow("cursorForItem");
  expect(requests.map(request => request.cursor)).toEqual([
    { direction: "after", boundary: 2 }, { direction: "after", boundary: 2 },
  ]);
  expect(() => window.reset(page(history, "new"))).toThrow("cursorForItem");
  expect(sequences(window)).toEqual([1, 2]);
});

test("overlapping pages update values and reuse a known new boundary cursor", async () => {
  const requests: (Cursor | undefined)[] = [];
  const window = new PageWindow<Item, Cursor>(item => item.id, async (direction, anchor) => {
    requests.push(anchor);
    const items = direction === "after" ? history.slice(1, 3) : history.slice(0, 1);
    return page(items.map(item => ({ ...item, value: 1 })));
  }, 2);
  window.reset(page(history.slice(0, 2)));
  expect((await window.load("after")).map(item => item.sequence)).toEqual([3]);
  expect(window.items.map(({ sequence, value }) => [sequence, value])).toEqual([[2, 1], [3, 1]]);
  await window.load("before");
  expect(sequences(window)).toEqual([1, 2]);
  expect(requests.at(-1)).toEqual({ direction: "before", boundary: 2 });
});

test("an initial page establishes exhaustion at both edges", async () => {
  let calls = 0;
  const window = new PageWindow<Item, Cursor>(item => item.id, async () => {
    calls += 1;
    return page(history);
  }, history.length);
  await window.load("after");
  expect(window.hasMoreBefore).toBe(false);
  expect(window.hasMoreAfter).toBe(false);
  expect(await window.load("before")).toEqual([]);
  expect(calls).toBe(1);
});

test("eviction fences an opposite-edge response without clearing a newer request", async () => {
  const requests: {
    direction: PageDirection; cursor: Cursor | undefined; resolve: (value: Page<Item, Cursor>) => void;
  }[] = [];
  const window = new PageWindow<Item, Cursor>(item => item.id, (direction, anchor) =>
    new Promise(resolve => requests.push({ direction, cursor: anchor, resolve })), 2);
  window.reset(page(history.slice(2, 4)));
  const staleBefore = window.load("before");
  const after = window.load("after");
  requests[1]?.resolve(page(history.slice(4, 6)));
  await after;
  expect(sequences(window)).toEqual([5, 6]);
  const freshBefore = window.load("before");
  expect(window.load("before")).toBe(freshBefore);
  expect(requests.at(-1)?.cursor).toEqual({ direction: "before", boundary: 5 });
  requests[0]?.resolve(page(history.slice(0, 2)));
  expect(await staleBefore).toEqual([]);
  expect(window.load("before")).toBe(freshBefore);
  expect(sequences(window)).toEqual([5, 6]);
  requests[2]?.resolve(page(history.slice(2, 4)));
  expect((await freshBefore).map(item => item.sequence)).toEqual([3, 4]);
  expect(sequences(window)).toEqual([3, 4]);
});

test("an omitted directional page cursor rejects aligned eviction without changing the window", async () => {
  const requests: (Cursor | undefined)[] = [];
  const window = new PageWindow<Item, Cursor>(item => item.id, async (_direction, anchor) => {
    requests.push(anchor);
    const { before, ...result } = page(history.slice(2, 4));
    return result;
  }, 2);
  window.reset(page(history.slice(0, 2)));
  await expect(window.load("after")).rejects.toThrow("cursorForItem");
  expect(sequences(window)).toEqual([1, 2]);
  expect(window.hasMoreBefore).toBe(false);
  expect(window.hasMoreAfter).toBe(true);
  await expect(window.load("after")).rejects.toThrow("cursorForItem");
  expect(requests).toEqual([
    { direction: "after", boundary: 2 }, { direction: "after", boundary: 2 },
  ]);
});

test("a failing reset cursor callback preserves the old generation and its in-flight load", async () => {
  const failure = new Error("cursor unavailable");
  const requests: { cursor: Cursor | undefined; resolve: (value: Page<Item, Cursor>) => void }[] = [];
  const window = new PageWindow<Item, Cursor>(item => item.id, (_direction, anchor) =>
    new Promise(resolve => requests.push({ cursor: anchor, resolve })), 2, () => { throw failure; });
  window.reset(page(history.slice(0, 2)));
  const pending = window.load("after");
  expect(() => window.reset(page(history, "replacement"))).toThrow(failure);
  expect(sequences(window)).toEqual([1, 2]);
  expect(window.hasMoreBefore).toBe(false);
  expect(window.load("after")).toBe(pending);
  expect(requests).toHaveLength(1);
  requests[0]?.resolve(page(history.slice(2, 4)));
  expect((await pending).map(item => item.sequence)).toEqual([3, 4]);
  expect(sequences(window)).toEqual([3, 4]);
});
