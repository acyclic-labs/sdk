import { create } from "@bufbuild/protobuf";
import { DigestSchema, ErrorCode, verified, ObjectsV1Error, type ObjectsV1Provider } from "./v1.js";

/** Original host-owned immutable storage port. Keys are canonical object
 * references, not bare content digests. A successful put is durable before
 * publication; byte validation and authority remain with the original owner. */
export interface ImmutableObjects {
  length(key: Uint8Array): Promise<number | null>;
  read(key: Uint8Array, maximum: bigint): Promise<Uint8Array | null>;
  put(keys: Uint8Array[], values: Uint8Array[]): Promise<void>;
}

/** Expose only immutable storage operations from the admitted host port.
 * Preserve its receiver, exact keys, promises and terminal errors. This adds
 * no transport, codec, copying, retention operation or durability barrier. */
export function materializeImmutableObjects(host: ImmutableObjects): ImmutableObjects {
  return Object.freeze({
    length: host.length.bind(host),
    read: host.read.bind(host),
    put: host.put.bind(host),
  });
}

/** The Objects v1 blob a canonical object reference names. A reference is a
 * tag byte (bit 7 marks a sealed association), the BLAKE3-256 of the object's
 * plaintext bytes, then for sealed references 80 bytes of association. Those
 * plaintext bytes are what the port stores, so the blob is that digest's. */
function blob(key: Uint8Array) {
  const sealed = key.length > 0 && (key[0]! & 0x80) !== 0;
  if (key.length !== (sealed ? 113 : 33)) {
    throw new TypeError("immutable object key invalid; recovery: pass the canonical object reference the filesystem produced");
  }
  return create(DigestSchema, { blake3: key.slice(1, 33) });
}

function absent(error: unknown): boolean {
  return error instanceof ObjectsV1Error && error.code === ErrorCode.NOT_FOUND;
}

/** The one mapping of the immutable port onto an account's Objects v1 blobs.
 * `root` is the producer's retention root (at most 1024 bytes): every put
 * batch is retained under it, so collection never sweeps a published object.
 * Grants: `objects/head`, `objects/get`, `objects/put`, `objects/retain`. */
export function immutableObjectsOverV1(provider: ObjectsV1Provider, root: Uint8Array): ImmutableObjects {
  const retained = Uint8Array.from(root);
  return Object.freeze({
    async length(key: Uint8Array): Promise<number | null> {
      const digest = blob(key);
      try { return Number((await provider.head(digest)).length); } catch (error) { if (absent(error)) return null; throw error; }
    },
    async read(key: Uint8Array, maximum: bigint): Promise<Uint8Array | null> {
      const digest = blob(key);
      try { return (await provider.get({ digest, range: { start: 0n }, maximumBytes: maximum })).body; }
      catch (error) { if (absent(error)) return null; throw error; }
    },
    async put(keys: Uint8Array[], values: Uint8Array[]): Promise<void> {
      if (keys.length === 0 || keys.length !== values.length) {
        throw new TypeError("immutable object batch invalid; recovery: pass one value per key");
      }
      const digests = keys.map(blob);
      await Promise.all(values.map(async (body, index) => {
        // Created and AlreadyExists are durable; Unknown is never success.
        const stored = verified(await provider.put(body));
        if (stored === undefined) throw new ObjectsV1Error(ErrorCode.UNAVAILABLE);
        const expected = digests[index]!.blake3, actual = stored.digest?.blake3 ?? new Uint8Array();
        if (actual.length !== expected.length || actual.some((byte, at) => byte !== expected[at])) {
          throw new TypeError("immutable bytes changed; recovery: write the bytes the reference names");
        }
      }));
      await provider.retain(retained, digests);
    },
  });
}
