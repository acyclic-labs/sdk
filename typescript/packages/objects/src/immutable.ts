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
