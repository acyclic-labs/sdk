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

/** Bidirectional, deduplicating, single-flight page window. */
export class PageWindow<Item, Cursor> {
  readonly #items = new Map<string, Item>();
  #ordered: string[] = [];
  #before: Cursor | undefined;
  #after: Cursor | undefined;
  #hasMoreBefore = true;
  #hasMoreAfter = true;
  #inflight = new Map<PageDirection, Promise<readonly Item[]>>();
  #generation: string | undefined;
  #epoch = 0;

  constructor(
    readonly key: (item: Item) => string,
    readonly loadPage: PageLoader<Item, Cursor>,
    readonly maxItems: number,
  ) {
    if (!Number.isSafeInteger(maxItems) || maxItems < 1) throw new RangeError("maxItems");
  }

  get items(): readonly Item[] {
    return this.#ordered.map(key => this.#items.get(key) as Item);
  }

  get hasMoreBefore(): boolean {
    return this.#hasMoreBefore;
  }

  get hasMoreAfter(): boolean {
    return this.#hasMoreAfter;
  }

  /** Replaces the window from an authoritative snapshot and fences older requests. */
  reset(page: Page<Item, Cursor>): void {
    this.#epoch += 1;
    this.#inflight.clear();
    this.#items.clear();
    this.#ordered = [];
    for (const item of page.items) {
      const key = this.key(item);
      if (!this.#items.has(key)) this.#ordered.push(key);
      this.#items.set(key, item);
    }
    this.#before = page.before;
    this.#after = page.after;
    this.#hasMoreBefore = page.hasMoreBefore;
    this.#hasMoreAfter = page.hasMoreAfter;
    this.#generation = page.generation;
    this.#evict("after");
  }

  load(direction: PageDirection): Promise<readonly Item[]> {
    const existing = this.#inflight.get(direction);
    if (existing !== undefined) return existing;
    if (direction === "before" ? !this.#hasMoreBefore : !this.#hasMoreAfter) {
      return Promise.resolve([]);
    }
    const epoch = this.#epoch;
    const request = this.#load(direction, epoch).finally(() => {
      if (this.#epoch === epoch) this.#inflight.delete(direction);
    });
    this.#inflight.set(direction, request);
    return request;
  }

  async #load(direction: PageDirection, epoch: number): Promise<readonly Item[]> {
    const page = await this.loadPage(direction, direction === "before" ? this.#before : this.#after);
    if (epoch !== this.#epoch) return [];
    if (this.#generation !== undefined && page.generation !== this.#generation) {
      throw new Error("page generation changed; reset from an authoritative snapshot");
    }
    this.#generation = page.generation;
    const added: string[] = [];
    for (const item of page.items) {
      const key = this.key(item);
      if (!this.#items.has(key)) added.push(key);
      this.#items.set(key, item);
    }
    this.#ordered = direction === "before" ? [...added, ...this.#ordered] : [...this.#ordered, ...added];
    if (direction === "before") {
      this.#before = page.before;
      this.#hasMoreBefore = page.hasMoreBefore;
    } else {
      this.#after = page.after;
      this.#hasMoreAfter = page.hasMoreAfter;
    }
    this.#evict(direction);
    return added.map(key => this.#items.get(key)).filter((item): item is Item => item !== undefined);
  }

  #evict(direction: PageDirection): void {
    while (this.#ordered.length > this.maxItems) {
      const evicted = direction === "before" ? this.#ordered.pop() : this.#ordered.shift();
      if (evicted !== undefined) this.#items.delete(evicted);
    }
  }
}
