// Generated from the Rust semantic metadata entrypoint. Do not edit.

/**
 * A byte view with no mutating methods or mutable aliases. Runtime bridges
 * still receive ordinary Uint8Array values at the wire boundary; this is the
 * public semantic projection exposed to callers.
 */
export interface ReadonlyByteSurface {
  readonly [index: number]: number;
  readonly length: number;
  readonly byteLength: number;
  at(index: number): number | undefined;
  slice(start?: number, end?: number): ReadonlyBytes;
  subarray(start?: number, end?: number): ReadonlyBytes;
  every(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): boolean;
  filter(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): ReadonlyBytes;
  find(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): number | undefined;
  findIndex(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): number;
  findLast?(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): number | undefined;
  findLastIndex?(predicate: (value: number, index: number, array: ReadonlyBytes) => unknown, thisArg?: unknown): number;
  forEach(callbackfn: (value: number, index: number, array: ReadonlyBytes) => void, thisArg?: unknown): void;
  includes(searchElement: number, fromIndex?: number): boolean;
  indexOf(searchElement: number, fromIndex?: number): number;
  join(separator?: string): string;
  lastIndexOf(searchElement: number, fromIndex?: number): number;
  map(callbackfn: (value: number, index: number, array: ReadonlyBytes) => number, thisArg?: unknown): ReadonlyBytes;
  reduce(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: ReadonlyBytes) => number, initialValue?: number): number;
  reduce<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: ReadonlyBytes) => U, initialValue: U): U;
  reduceRight(callbackfn: (previousValue: number, currentValue: number, currentIndex: number, array: ReadonlyBytes) => number, initialValue?: number): number;
  reduceRight<U>(callbackfn: (previousValue: U, currentValue: number, currentIndex: number, array: ReadonlyBytes) => U, initialValue: U): U;
  entries(): IterableIterator<[number, number]>;
  keys(): IterableIterator<number>;
  values(): IterableIterator<number>;
  [Symbol.iterator](): IterableIterator<number>;
  toLocaleString(): string;
  toString(): string;
}

/** Preserve Rust nominal brands while projecting byte operations readonly. */
export type ReadonlyBytes<T extends Uint8Array = Uint8Array> = ReadonlyByteSurface & {
  readonly [K in Exclude<keyof T, keyof Uint8Array>]: T[K]
};

/** Recursively project Rust semantic records and collections to readonly data. */
export type ReadonlySemantic<T> = T extends Uint8Array
  ? ReadonlyBytes<T>
  : T extends (...args: never[]) => unknown
    ? T
    : T extends readonly (infer Item)[]
      ? readonly ReadonlySemantic<Item>[]
      : T extends object
        ? { readonly [K in keyof T]: ReadonlySemantic<T[K]> }
        : T;
