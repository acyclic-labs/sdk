/** Capture exactly one caller-owned byte view before any asynchronous boundary. */
const U8 = Uint8Array;
const prototype = Object.getPrototypeOf(U8.prototype);
const getter = (name: PropertyKey) => Object.getOwnPropertyDescriptor(prototype, name)!.get!;
const backing = getter("buffer"), length = getter("byteLength"), kind = getter(Symbol.toStringTag);
const ordinary = Object.getOwnPropertyDescriptor(ArrayBuffer.prototype, "byteLength")!.get!;
const set = U8.prototype.set, apply = Reflect.apply;

/** One allocation and one copy of the supplied extent, independent of its backing size.
 * Captured intrinsics bypass caller properties, iterators and species constructors.
 * Shared, detached and non-byte views fail before any asynchronous work.
 */
export function snapshotBytes(input: Uint8Array): Uint8Array<ArrayBuffer> {
  const buffer = apply(backing, input, []);
  apply(ordinary, buffer, []);
  if (apply(kind, input, []) !== "Uint8Array") throw new TypeError("ordinary byte view required");
  const bytes = new U8(apply(length, input, []));
  apply(set, bytes, [input]);
  return bytes;
}
