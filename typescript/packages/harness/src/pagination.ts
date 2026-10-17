export type PageDirection = "before" | "after";

export interface Page<Item, Cursor> {
  readonly generation: string;
  readonly items: readonly Item[];
  readonly before?: Cursor;
  readonly after?: Cursor;
  readonly hasMoreBefore: boolean;
  readonly hasMoreAfter: boolean;
}

export type PageLoader<Item, Cursor> = (
  direction: PageDirection,
  cursor: Cursor | undefined,
) => Promise<Page<Item, Cursor>>;

interface CursorAnchor<Cursor> {
  readonly cursor: Cursor | undefined;
}

interface WindowItem<Item, Cursor> {
  readonly item: Item;
  readonly before?: CursorAnchor<Cursor>;
  readonly after?: CursorAnchor<Cursor>;
}

/** Bidirectional, deduplicating, single-flight page window.
 * Supply cursorForItem when a retained boundary lacks a directional page cursor.
 * Page-edge cursors are reused when possible; opaque cursors are never inferred from keys.
 */
export class PageWindow<Item, Cursor> {
  #items = new Map<string, WindowItem<Item, Cursor>>();
  #before: Cursor | undefined;
  #after: Cursor | undefined;
  #hasMoreBefore = true;
  #hasMoreAfter = true;
  #inflight = new Map<PageDirection, Promise<readonly Item[]>>();
  #generation: string | undefined;
  #epoch = 0;
  #edgeRevision = { before: 0, after: 0 };

  constructor(
    readonly key: (item: Item) => string,
    readonly loadPage: PageLoader<Item, Cursor>,
    readonly maxItems: number,
    readonly cursorForItem?: (item: Item, direction: PageDirection) => Cursor,
  ) {
    if (!Number.isSafeInteger(maxItems) || maxItems < 1) throw new RangeError("maxItems");
  }

  get items(): readonly Item[] {
    return [...this.#items.values()].map(entry => entry.item);
  }

  get hasMoreBefore(): boolean {
    return this.#hasMoreBefore;
  }

  get hasMoreAfter(): boolean {
    return this.#hasMoreAfter;
  }

  /** Replaces the window from an authoritative snapshot and fences older requests. */
  reset(page: Page<Item, Cursor>): void {
    const staged = this.#stage(page, "after", true);
    this.#epoch += 1;
    this.#inflight.clear();
    this.#items = staged.items;
    this.#before = page.before;
    this.#after = page.after;
    this.#hasMoreBefore = page.hasMoreBefore;
    this.#hasMoreAfter = page.hasMoreAfter;
    this.#generation = page.generation;
    if (staged.evicted !== undefined) this.#setEdge(staged.evicted.direction, staged.evicted.cursor, true, true);
  }

  load(direction: PageDirection): Promise<readonly Item[]> {
    const existing = this.#inflight.get(direction);
    if (existing !== undefined) return existing;
    if (direction === "before" ? !this.#hasMoreBefore : !this.#hasMoreAfter) {
      return Promise.resolve([]);
    }
    const epoch = this.#epoch;
    const request = this.#load(direction, epoch).finally(() => {
      if (this.#inflight.get(direction) === request) this.#inflight.delete(direction);
    });
    this.#inflight.set(direction, request);
    return request;
  }

  async #load(direction: PageDirection, epoch: number): Promise<readonly Item[]> {
    const revision = this.#edgeRevision[direction];
    const page = await this.loadPage(direction, direction === "before" ? this.#before : this.#after);
    if (epoch !== this.#epoch || revision !== this.#edgeRevision[direction]) return [];
    if (this.#generation !== undefined && page.generation !== this.#generation) {
      throw new Error("page generation changed; reset from an authoritative snapshot");
    }
    const empty = this.#items.size === 0;
    const staged = this.#stage(page, direction);
    this.#items = staged.items;
    this.#generation = page.generation;
    this.#setEdge(direction, page[direction], direction === "before" ? page.hasMoreBefore : page.hasMoreAfter);
    if (empty) {
      const opposite = direction === "before" ? "after" : "before";
      this.#setEdge(opposite, page[opposite], opposite === "before" ? page.hasMoreBefore : page.hasMoreAfter, true);
    }
    if (staged.evicted !== undefined) this.#setEdge(staged.evicted.direction, staged.evicted.cursor, true, true);
    return staged.added.flatMap(key => {
      const entry = this.#items.get(key);
      return entry === undefined ? [] : [entry.item];
    });
  }

  #stage(page: Page<Item, Cursor>, direction: PageDirection, replace = false) {
    let items = replace ? new Map<string, WindowItem<Item, Cursor>>() : new Map(this.#items);
    const pageKeys = new Set<string>();
    const added = new Map<string, WindowItem<Item, Cursor>>();
    for (const item of page.items) {
      const key = this.key(item);
      const entry = { ...items.get(key), item };
      if (!items.has(key)) added.set(key, entry);
      items.set(key, entry);
      pageKeys.add(key);
    }
    if (direction === "before") items = new Map([...added, ...items]);
    const keys = [...pageKeys];
    for (const [edge, key] of [["before", keys[0]], ["after", keys.at(-1)]] as const) {
      if (key === undefined || page[edge] === undefined) continue;
      const entry = items.get(key);
      if (entry !== undefined) items.set(key, { ...entry, [edge]: { cursor: page[edge] } });
    }
    const evicted = this.#evict(items, direction);
    return { items, added: [...added.keys()], evicted };
  }

  #evict(items: Map<string, WindowItem<Item, Cursor>>, direction: PageDirection):
    { direction: PageDirection; cursor: Cursor | undefined } | undefined {
    const excess = items.size - this.maxItems;
    if (excess <= 0) return undefined;
    const keys = [...items.keys()];
    const edge = direction === "before" ? "after" : "before";
    const boundary = direction === "before" ? keys[this.maxItems - 1] : keys[excess];
    const entry = boundary === undefined ? undefined : items.get(boundary);
    let anchor = entry?.[edge];
    if (anchor === undefined && entry !== undefined && this.cursorForItem !== undefined) {
      anchor = { cursor: this.cursorForItem(entry.item, edge) };
    }
    if (anchor === undefined) throw new RangeError("PageWindow requires cursorForItem for the retained boundary");
    if (boundary !== undefined && entry !== undefined) items.set(boundary, { ...entry, [edge]: anchor });
    for (const key of direction === "before" ? keys.slice(-excess) : keys.slice(0, excess)) items.delete(key);
    return { direction: edge, cursor: anchor.cursor };
  }

  #setEdge(direction: PageDirection, cursor: Cursor | undefined, hasMore: boolean, invalidate = false): void {
    if (direction === "before") {
      this.#before = cursor;
      this.#hasMoreBefore = hasMore;
    } else {
      this.#after = cursor;
      this.#hasMoreAfter = hasMore;
    }
    if (invalidate) {
      this.#edgeRevision[direction] += 1;
      this.#inflight.delete(direction);
    }
  }
}
