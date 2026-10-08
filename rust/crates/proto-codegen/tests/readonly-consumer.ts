// Compile this consumer against the actual private CLI readonly output.
import type { ReadonlyBytes, ReadonlySemantic, ReadonlyInputSemantic } from "./readonly.js";
type Digest = Uint8Array & { readonly __brand: "digest" };
declare const value: ReadonlySemantic<{ digest: Digest; nested: { bytes: Uint8Array[] }; states: (0 | 1 | 2)[] }>;
const brand: "digest" = value.digest.__brand;
const state: 0 | 1 | 2 = value.states[0]!;
const copy: ReadonlyBytes = value.digest.subarray();
// @ts-expect-error readonly numeric index
value.digest[0] = 1;
// @ts-expect-error mutating byte method
value.digest.set(new Uint8Array());
// @ts-expect-error mutable backing buffer is hidden
value.digest.buffer;
// @ts-expect-error alias remains readonly
value.digest.subarray()[0] = 1;
// @ts-expect-error alias exposes no mutation method
value.digest.subarray().fill(0);
value.digest.forEach((_byte, _index, bytes) => {
  // @ts-expect-error callback cannot mutate the byte surface
  bytes[0] = 1;
  // @ts-expect-error callback cannot expose backing memory
  bytes.buffer;
});
// @ts-expect-error nested arrays are readonly
value.nested.bytes.push(new Uint8Array());
// @ts-expect-error nested bytes expose no mutator
value.nested.bytes[0]!.reverse();
// @ts-expect-error enum array is readonly
value.states.push(1);
// @ts-expect-error enum discriminants remain exact
const wrong: 17 = state;
// @ts-expect-error readonly output is not a mutable Uint8Array
const mutable: Uint8Array = value.digest;
const input: ReadonlyInputSemantic<{ bytes: Uint8Array }> = { bytes: new Uint8Array() };
void [brand, state, copy, input];
