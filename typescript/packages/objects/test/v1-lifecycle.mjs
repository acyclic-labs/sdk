import assert from "node:assert/strict";
import * as wire from "@acyclic-labs/objects/proto";
import { blobDigest, verified } from "@acyclic-labs/objects";
const fail = (code) => error => error.code === code;
export const bytes = new Uint8Array(135000).map((_, index) => index % 251);

/** One account's blob lifecycle through any provider; every outcome stays distinct. */
export async function lifecycle(provider) {
  const created = await provider.put(bytes);
  assert.equal(created.outcome.case, "created");
  const digest = verified(created).digest;
  assert.deepEqual(digest.blake3, (await blobDigest(bytes)).blake3);
  assert.equal(verified(created).length, BigInt(bytes.length));
  assert.equal((await provider.put(bytes)).outcome.case, "alreadyExists");
  const whole = await provider.get({ digest, maximumBytes: BigInt(bytes.length) });
  assert.deepEqual(whole.body, bytes);
  assert.equal(whole.info.kind, wire.Kind.BLOB);
  const range = await provider.get({ digest, range: { start: 10n, end: 20n }, maximumBytes: 10n });
  assert.equal(range.start, 10n);
  assert.deepEqual(range.body, bytes.slice(10, 20));
  await assert.rejects(provider.get({ digest, maximumBytes: 16n }), fail(wire.ErrorCode.QUOTA_EXCEEDED));
  await assert.rejects(provider.get({ digest, range: { start: 200000n }, maximumBytes: 16n }), fail(wire.ErrorCode.RANGE_NOT_SATISFIABLE));
  assert.equal((await provider.head(digest)).length, BigInt(bytes.length));

  const absent = await blobDigest(new Uint8Array([9, 9, 9]));
  await assert.rejects(provider.head(absent), fail(wire.ErrorCode.NOT_FOUND));
  await assert.rejects(provider.put(new Uint8Array([1]), [absent]), fail(wire.ErrorCode.MISSING_REFERENCE));
  const tail = new Uint8Array([1, 2, 3]);
  const child = verified(await provider.put(tail, [digest])).digest;
  assert.deepEqual((await provider.head(child)).references.map(value => value.blake3), [digest.blake3]);
  await assert.rejects(provider.put(tail), fail(wire.ErrorCode.CONFLICT));
  assert.equal((await provider.delete(digest)).status, wire.DeleteStatus.REFERENCED);

  const manifest = verified(await provider.putManifest({ parts: [{ digest, length: BigInt(bytes.length) }, { digest: child, length: 3n }] }));
  assert.equal(manifest.length, BigInt(bytes.length + 3));
  const joined = await provider.get({ digest: manifest.digest, range: { start: BigInt(bytes.length - 2) }, maximumBytes: 5n });
  assert.equal(joined.info.kind, wire.Kind.MANIFEST);
  assert.deepEqual([...joined.body], [...bytes.slice(-2), ...tail]);

  const listed = await provider.list({ limit: 2 });
  assert.ok(listed.digests.length <= 2);
  const root = new TextEncoder().encode("lifecycle-root");
  assert.equal((await provider.retain(root, [manifest.digest])).references, 1n);
  assert.equal((await provider.delete(manifest.digest)).status, wire.DeleteStatus.REFERENCED);
  assert.equal((await provider.release(root)).existed, true);
  assert.equal((await provider.release(root)).existed, false);
  assert.equal((await provider.delete(manifest.digest)).status, wire.DeleteStatus.DELETED);
  assert.equal((await provider.delete(manifest.digest)).status, wire.DeleteStatus.ABSENT);
  await provider.collect();
  assert.ok((await provider.usage()).blobs >= 2n);
}
