//! Shared maintainer-side readonly TypeScript projection.
/// Exact byte and recursive semantic projection used by generated SDK types.
pub const TYPESCRIPT_READONLY: &str = r#"// Generated from the Rust semantic metadata entrypoint. Do not edit.

/**
 * Readonly views of byte fields and nested semantic records.
 * Byte methods, returned views and callback arguments expose readonly surfaces.
 * Rust nominal brands remain part of the projected type.
 */
import { protoInt64 } from "@bufbuild/protobuf";

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

/** Inputs may be supplied by existing Uint8Array callers; the output view remains readonly. */
export type ReadonlyInputSemantic<T> = T extends Uint8Array
  ? ReadonlyBytes<T> | Uint8Array
  : T extends (...args: never[]) => unknown
    ? T
    : T extends readonly (infer Item)[]
      ? readonly ReadonlyInputSemantic<Item>[]
      : T extends object
        ? { readonly [K in keyof T]: ReadonlyInputSemantic<T[K]> }
        : T;

/** Typed scalar encoding requires the maintained bigint-capable codec. */
export function requireBigIntCodec(): void {
  if (!protoInt64.supported) throw new TypeError("BigInt protobuf codec is unavailable");
}

/** Detach canonical Buf records from wire metadata and mutable backing bytes. */
export function canonicalSemantic(value: unknown): unknown {
  if (value === null || value === undefined || typeof value !== "object") return value;
  if (value instanceof Uint8Array) return value.slice();
  if (Array.isArray(value)) return Object.freeze(value.map(canonicalSemantic));
  const entries = Object.entries(value);
  if (entries.length === 1 && entries[0][0] === "case" && entries[0][1] === undefined) return undefined;
  return Object.freeze(Object.fromEntries(entries.filter(([key, item]) => key !== "$typeName" && key !== "$unknown" && item !== undefined)
    .map(([key, item]) => [key, canonicalSemantic(item)]).filter(([, item]) => item !== undefined)));
}

type EqualKeys<A, B> = [Exclude<keyof A, keyof B>, Exclude<keyof B, keyof A>] extends [never, never] ? true : false;
type Clean<T> = Omit<T, '$typeName' | '$unknown'>;
type Optional<T, K extends keyof T> = {} extends Pick<T, K> ? true : false;
type All<T> = Exclude<T, true> extends never ? true : false;
type Cases<T> = T extends { case: infer C } ? C : never;
type Same<A, B> = [A, B] extends [B, A] ? true : false;
type Oneof<S, W> = [Cases<W>] extends [never] ? true : Same<Exclude<Cases<S>, undefined>, Exclude<Cases<W>, undefined>> extends true ? undefined extends Cases<S> ? undefined extends Cases<W> ? true : false : true : false;
type Choose<S, W> = S extends { case: infer C } ? Extract<W, { case: C }> : S extends null | undefined ? Extract<W, S> : Exclude<W, null | undefined>;
type Exact<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? (<T>() => T extends B ? 1 : 2) extends (<T>() => T extends A ? 1 : 2) ? true : false : false;
type SeenPair<S, W, Seen extends readonly (readonly [unknown, unknown])[]> = Seen extends readonly [infer Head extends readonly [unknown, unknown], ...infer Tail extends readonly (readonly [unknown, unknown])[]] ? Exact<S, Head[0]> extends true ? Exact<W, Head[1]> extends true ? true : SeenPair<S, W, Tail> : SeenPair<S, W, Tail> : false;
type NodePresence<S, W> = All<{ [K in keyof S]-?: K extends keyof W ? Optional<S, K> extends true ? Optional<W, K> : true : false }[keyof S]>;
type Guard<S, W, Seen extends readonly (readonly [unknown, unknown])[] = []> = 0 extends (1 & S) ? false : 0 extends (1 & W) ? false : [S] extends [never] ? false : [W] extends [never] ? false :
  Oneof<S, W> extends true ? All<S extends unknown ? Part<S, Choose<S, W>, Seen> : never> : false;
type Part<S, W, Seen extends readonly (readonly [unknown, unknown])[]> = [W] extends [never] ? false :
  [W] extends [Uint8Array] ? S extends ReadonlyBytes | Uint8Array ? true : false :
  [W] extends [string | bigint | boolean | number | undefined | null] ? [S] extends [W] ? true : false :
  W extends readonly (infer V)[] ? S extends readonly (infer U)[] ? SeenPair<S, W, Seen> extends true ? true : Guard<U, V, [...Seen, [S, W]]> : false :
  W extends object ? S extends object ? EqualKeys<Clean<S>, Clean<W>> extends true ?
  NodePresence<Clean<S>, Clean<W>> extends true ? SeenPair<S, W, Seen> extends true ? true : All<{ [K in keyof Clean<S>]-?: K extends keyof Clean<W> ? Guard<Clean<S>[K], Clean<W>[K], [...Seen, [S, W]]> : false }[keyof Clean<S>]> : false : false : false : false;
export type CanonicalCompatible<Semantic, Wire> = Guard<Semantic, Wire>;
"#;
