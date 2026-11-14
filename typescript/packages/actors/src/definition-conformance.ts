import { GUEST_ABI_VERSION, type ActorDefinition } from "./generated/guest.js";

/** The Handler ABI a backend's host core runs: the namespace manifest and one delivery. */
export interface DefinitionHost {
  /** The code SHA-256 this backend admitted for the deployment. */
  readonly code: Uint8Array;
  manifest(): Promise<Uint8Array>;
  onRecord(bytes: Uint8Array, signal?: AbortSignal): Promise<Uint8Array>;
}

// The Rust-captured record delivery and transition of `actors/tests/fixtures/guest-record.json`:
// actor "one" of namespace "counter", record 5 of subscription "inbox", reply [1, 0xff].
const DELIVERY = Uint8Array.of(255, 65, 68, 76, 86, 1, 10, 63, 0, 0, 0, 0, 0, 0, 0, 0, 97, 99, 121, 99, 108, 105, 99, 46, 97, 99, 116, 111, 114, 1, 0, 0, 0, 0, 0, 0, 0, 7, 97, 99, 99, 111, 117, 110, 116, 0, 0, 0, 0, 0, 0, 0, 7, 99, 111, 117, 110, 116, 101, 114, 0, 0, 0, 0, 0, 0, 0, 3, 111, 110, 101, 16, 3, 24, 164, 13, 34, 9, 10, 5, 105, 110, 98, 111, 120, 16, 5, 50, 57, 8, 5, 18, 3, 251, 255, 0, 26, 32, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 171, 32, 99, 58, 12, 105, 110, 112, 117, 116, 115, 47, 105, 110, 98, 111, 120, 66, 23, 10, 5, 105, 110, 98, 111, 120, 18, 12, 105, 110, 112, 117, 116, 115, 47, 105, 110, 98, 111, 120, 24, 5, 74, 32, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 17, 82, 12, 97, 99, 116, 111, 114, 115, 47, 115, 116, 97, 116, 101, 88, 9);
const TRANSITION = Uint8Array.of(255, 65, 84, 82, 78, 3, 10, 15, 10, 7, 99, 111, 117, 110, 116, 101, 114, 18, 1, 49, 26, 1, 1, 32, 1, 58, 30, 26, 28, 97, 99, 121, 99, 108, 105, 99, 46, 97, 99, 116, 111, 114, 115, 46, 116, 114, 97, 110, 115, 105, 116, 105, 111, 110, 46, 118, 51, 74, 2, 1, 255);
const CODE = new Uint8Array(32).fill(7);
const counter = { name: "counter", version: "1" };

/** A public ActorDefinition, deployed exactly as an author's module exports it. */
export function conformanceDefinition(): { actor: ActorDefinition<{ bytes: Uint8Array; count: number }, Record<string, never>> } {
  return {
    actor: {
      abiVersion: GUEST_ABI_VERSION,
      codec: { identity: counter, encode: value => value.bytes, decode: value => ({ bytes: value.bytes, count: 0 }) },
      restore(checkpoint) { return { bytes: checkpoint?.bytes ?? new Uint8Array(), count: 0 }; },
      event(state, event) {
        if (event.type !== "stream") return null;
        let count = 0;
        state.with(value => { count = ++value.count; value.bytes = Uint8Array.of(count); return undefined; });
        return { type: "reply", payload: Uint8Array.of(count, 0xff) };
      },
      checkpoint(state) {
        let bytes: Uint8Array = new Uint8Array();
        state.with(value => { bytes = value.bytes; return undefined; });
        return { codec: counter, bytes };
      },
      fetch() { return new Response(null); },
    },
  };
}

const field = (tag: number, bytes: Uint8Array) => [tag, bytes.length, ...bytes];
const text = (value: string) => new TextEncoder().encode(value);

/** The strict-wire manifest (`guest::wire`, version 1) of exactly the admitted code,
 * the definition's codec, the "inbox" subscription, no bindings or interrupts, and
 * one limits message: the only field a host chooses. */
function strictManifest(manifest: Uint8Array, code: Uint8Array): boolean {
  if (code.length !== 32) return false;
  const prefix = [0xff, 0x41, 0x4d, 0x4e, 0x46, 0x01, ...field(0x0a, code), ...field(0x12, text(counter.name)),
    ...field(0x1a, text(counter.version)), ...field(0x22, text("inbox"))];
  if (manifest.length < prefix.length + 2 || prefix.some((byte, index) => manifest[index] !== byte)) return false;
  let offset = prefix.length;
  if (manifest[offset++] !== 0x32) return false;
  let length = 0;
  for (let shift = 0; ; shift += 7) {
    if (offset >= manifest.length || shift > 28) return false;
    const byte = manifest[offset++]!;
    length |= (byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) break;
  }
  return offset + length === manifest.length;
}

/** Every backend's conformance: `deploy` is the backend's real path from a deployed
 * JavaScript module to the Handler ABI its core runs. A public ActorDefinition must
 * answer the canonical delivery with the canonical transition, and its manifest
 * must carry the definition's codec and the deployment's subscription. */
export async function verifyDefinitionHost(
  deploy: (module: ReturnType<typeof conformanceDefinition>, deployment: { code: Uint8Array; subscriptions: readonly string[] }) => DefinitionHost | Promise<DefinitionHost>,
): Promise<void> {
  const host = await deploy(conformanceDefinition(), { code: CODE, subscriptions: ["inbox"] });
  if (typeof host?.manifest !== "function" || typeof host?.onRecord !== "function")
    throw new Error("ActorDefinition deployment does not expose the Handler ABI");
  if (!strictManifest(await host.manifest(), host.code))
    throw new Error("ActorDefinition manifest is not the deployment's strict-wire manifest");
  for (let attempt = 0; attempt < 2; attempt++) {
    const transition = await host.onRecord(DELIVERY.slice());
    if (transition.length !== TRANSITION.length || transition.some((byte, index) => byte !== TRANSITION[index]))
      throw new Error("ActorDefinition delivery differs from the canonical transition");
  }
}
