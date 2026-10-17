import { expect, test } from "bun:test";
import fc from "fast-check";
import { readFile } from "node:fs/promises";
import { Capability, TrustSnapshot, type TrustState } from "../src/holder.js";

const root = new Uint8Array(await readFile(new URL("../../../../rust/crates/auth/fuzz/corpus/verifier/root", import.meta.url)));
const hex = (value: string) => Uint8Array.from(Buffer.from(value, "hex"));
const state = (): TrustState => ({
  environment: "test", account: "account", minimumKeyEpoch: 1n,
  environmentKeys: [hex("8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c")],
  issuerKeys: [hex("8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394")],
  revokedKeyIds: [], revokedBlockIds: [],
});

test("WASM holders preserve signatures, lineage, effective limits and revocation", async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-z]{1,8}$/), { minLength: 1, maxLength: 4 }), fc.bigInt({ min: 0n, max: 999n }), async (ids, maximum) => {
    const trust = new TrustSnapshot(state());
    let holder = Capability.verify(root, trust, 100n);
    try {
      for (const id of ids) {
        const child = holder.attenuate([
          { kind: { case: "lineageAppend", value: { ids: [id] } } },
          { kind: { case: "limits", value: { bytes: maximum } } },
          { kind: { case: "filter", value: {} } },
        ]);
        holder.free();
        holder = child;
      }
      expect(holder.principal().lineage).toEqual(ids);
      expect(holder.limits().bytes).toBe(maximum);
      expect(holder.grants().length).toBeGreaterThan(0);
      expect(holder.filters().map(clause => clause.subjectId)).toEqual(ids);
      expect(holder.filters().map(clause => clause.declaringBlock)).toEqual(ids.map((_, i) => i + 1));
      expect(holder.expiresAtUnixSeconds()).toBe(1000n);
      expect(holder.notBeforeUnixSeconds()).toBe(1n);
      const encoded = holder.encode();
      const admitted = Capability.verify(encoded, trust, 100n);
      try { expect(admitted.encode()).toEqual(encoded); } finally { admitted.free(); encoded.fill(0); }
      for (const id of holder.blockIds()) {
        const revoked = new TrustSnapshot({ ...state(), revokedBlockIds: [id] });
        try { expect(() => holder.check(revoked, 100n)).toThrow(); } finally { revoked.free(); }
      }
      const sealed = holder.seal();
      try {
        expect(() => holder.encode()).toThrow();
        expect(() => sealed.attenuate([])).toThrow();
        const bytes = sealed.encode();
        const copy = Capability.verify(bytes, trust, 100n);
        copy.free(); bytes.fill(0);
      } finally { sealed.free(); }
    } finally { holder.free(); trust.free(); }
  }), { numRuns: 16 });
});

test("WASM input never changes and tampered chains fail", () => {
  fc.assert(fc.property(fc.integer({ min: 0, max: root.length - 1 }), fc.integer({ min: 1, max: 255 }), (index, mask) => {
    const bytes = root.slice();
    bytes[index] = bytes[index]! ^ mask;
    const before = bytes.slice();
    const trust = new TrustSnapshot(state());
    try { expect(() => Capability.verify(bytes, trust, 100n)).toThrow(); expect(bytes).toEqual(before); }
    finally { bytes.fill(0); before.fill(0); trust.free(); }
  }), { numRuns: 32 });
});

test("WASM clocks and epochs cannot wrap into valid uint64 values", () => {
  fc.assert(fc.property(fc.bigInt({ min: 1n, max: 1000n }), offset => {
    for (const invalid of [-offset, (1n << 64n) + offset]) {
      expect(() => new TrustSnapshot({ ...state(), minimumKeyEpoch: invalid })).toThrow();
      const trust = new TrustSnapshot(state());
      try { expect(() => Capability.verify(root, trust, invalid)).toThrow(); }
      finally { trust.free(); }
    }
  }), { numRuns: 16 });
});
