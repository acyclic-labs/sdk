import { expect, test } from "bun:test";
import { blobDigest, immutableObjectsOverV1, MemoryObjectsV1 } from "../src/index.js";

const reference = async (bytes: Uint8Array, sealed = false) => {
  const key = new Uint8Array(sealed ? 113 : 33);
  key[0] = sealed ? 0x81 : 0x01;
  key.set((await blobDigest(bytes)).blake3, 1);
  if (sealed) key.fill(7, 33);
  return key;
};

test("immutable references map onto retained Objects v1 blobs", async () => {
  const provider = await MemoryObjectsV1.create();
  const objects = immutableObjectsOverV1(provider, new TextEncoder().encode("volume"));
  const bytes = new TextEncoder().encode("object bytes");
  const plain = await reference(bytes), sealed = await reference(bytes, true);
  expect(await objects.length(plain)).toBeNull();
  expect(await objects.read(plain, 64n)).toBeNull();
  await objects.put([plain, sealed], [bytes, bytes]);
  // Both references name the same plaintext blob.
  expect(await objects.length(sealed)).toBe(bytes.length);
  expect(await objects.read(plain, 64n)).toEqual(bytes);
  await expect(objects.read(plain, 1n)).rejects.toBeDefined();
  // Negative controls: bytes that do not match the reference, and malformed keys.
  await expect(objects.put([plain], [new TextEncoder().encode("other")])).rejects.toThrow("immutable bytes changed");
  await expect(objects.length(plain.slice(0, 32))).rejects.toThrow("key invalid");
  await expect(objects.length(Uint8Array.of(0x81, ...plain.slice(1)))).rejects.toThrow("key invalid");
  // Retention: every put batch is held under the producer's root, and only there.
  await provider.collect();
  expect(await objects.read(sealed, 64n)).toEqual(bytes);
  expect((await provider.release(new TextEncoder().encode("other root"))).existed).toBe(false);
  expect((await provider.release(new TextEncoder().encode("volume"))).existed).toBe(true);
});
