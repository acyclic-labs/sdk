/** Value plus an explicit hydration weight. */
export interface Hydrated<Value> {
  readonly value: Value;
  readonly bytes: number;
}

/** Byte- and entry-bounded least-recently-used hydration cache. */
export class HydrationCache<Key, Value> {
  readonly #values = new Map<Key, Hydrated<Value>>();
  #bytes = 0;

  constructor(
    readonly maxEntries: number,
    readonly maxBytes: number,
  ) {
    if (!Number.isSafeInteger(maxEntries) || maxEntries < 1) throw new RangeError("maxEntries");
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) throw new RangeError("maxBytes");
  }

  get size(): number {
    return this.#values.size;
  }

  get bytes(): number {
    return this.#bytes;
  }

  get(key: Key): Value | undefined {
    const entry = this.#values.get(key);
    if (entry === undefined) return undefined;
    this.#values.delete(key);
    this.#values.set(key, entry);
    return entry.value;
  }

  set(key: Key, value: Value, bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.maxBytes) {
      throw new RangeError("value exceeds the hydration cache byte budget");
    }
    const previous = this.#values.get(key);
    if (previous !== undefined) this.#bytes -= previous.bytes;
    this.#values.delete(key);
    this.#values.set(key, { value, bytes });
    this.#bytes += bytes;
    while (this.#values.size > this.maxEntries || this.#bytes > this.maxBytes) {
      const oldest = this.#values.entries().next().value as [Key, Hydrated<Value>] | undefined;
      if (oldest === undefined) break;
      this.#values.delete(oldest[0]);
      this.#bytes -= oldest[1].bytes;
    }
  }

  delete(key: Key): boolean {
    const entry = this.#values.get(key);
    if (entry === undefined) return false;
    this.#bytes -= entry.bytes;
    return this.#values.delete(key);
  }

  clear(): void {
    this.#values.clear();
    this.#bytes = 0;
  }
}
