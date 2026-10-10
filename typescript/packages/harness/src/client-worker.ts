import { SnapshotStore, type ClientSnapshot } from "./client-snapshot.js";

/** Encode only the selected reference-oriented view. decode returns immutable data.
 * This is a same-host snapshot channel, never an evidence or journal transport.
 */
export interface SnapshotCodec<Value> {
  encode(value: Value): Uint8Array;
  decode(bytes: Uint8Array): Value;
}

type Frame = { kind: "snapshot"; sequence: number; bytes: Uint8Array };
function bound(maximumBytes: number): void {
  if (!Number.isSafeInteger(maximumBytes) || maximumBytes <= 0) throw new RangeError("snapshot byte bound required");
}

/** Explicitly attach a publisher to an owned MessagePort. At most one in-flight
 * snapshot plus the latest source reference is retained; intermediate UI snapshots
 * may coalesce. Commands and authoritative evidence stay with the single kernel.
 */
export function publishClientSnapshots<Value>(store: ClientSnapshot<Value>, port: MessagePort,
  codec: SnapshotCodec<Value>, maximumBytes: number, onError: (error: unknown) => void): () => void {
  bound(maximumBytes);
  let stopped = false;
  let sequence = 0;
  let inFlight = false;
  let latest = store.getSnapshot();
  let sent = latest;
  let ready = false;
  const send = (): void => {
    if (stopped || inFlight) return;
    try {
      const bytes = codec.encode(latest);
      if (bytes.byteLength > maximumBytes) throw new RangeError("selected snapshot exceeds byte bound");
      if (sequence === Number.MAX_SAFE_INTEGER) throw new RangeError("snapshot sequence exhausted");
      sent = latest;
      inFlight = true;
      port.postMessage({ kind: "snapshot", sequence: ++sequence, bytes } satisfies Frame, [bytes.buffer]);
    } catch (error) { stop(); onError(error); }
  };
  const receive = (event: MessageEvent): void => {
    if (event.data?.kind !== "ack" || event.data.sequence !== sequence || !inFlight) return;
    inFlight = false;
    if (!Object.is(latest, sent)) send();
  };
  let unsubscribe = () => {};
  const stop = (): void => {
    if (stopped) return;
    stopped = true;
    unsubscribe();
    port.removeEventListener("message", receive);
    port.close();
  };
  port.addEventListener("message", receive);
  port.start();
  try { unsubscribe = store.subscribe(() => { latest = store.getSnapshot(); if (ready && !Object.is(latest, sent)) send(); }); }
  catch (error) { stop(); throw error; }
  ready = true;
  latest = store.getSnapshot();
  send();
  return stop;
}

/** Explicitly attach a receiver, initialized with the same SSR/hydration snapshot.
 * No worker is created. Disposal closes the owned port and clears subscriptions.
 */
export function receiveClientSnapshots<Value>(initial: Value, port: MessagePort,
  codec: SnapshotCodec<Value>, maximumBytes: number, onError: (error: unknown) => void): ClientSnapshot<Value> & { dispose(): void } {
  bound(maximumBytes);
  const store = new SnapshotStore(initial);
  let sequence = 0;
  let stopped = false;
  const receive = (event: MessageEvent): void => {
    try {
      const frame = event.data as Frame;
      if (frame?.kind !== "snapshot" || !Number.isSafeInteger(frame.sequence) || frame.sequence !== sequence + 1 ||
        !(frame.bytes instanceof Uint8Array) || frame.bytes.byteLength > maximumBytes) {
        throw new Error("invalid snapshot frame");
      }
      const value = codec.decode(frame.bytes);
      sequence = frame.sequence;
      store.publish(value);
      port.postMessage({ kind: "ack", sequence });
    } catch (error) { dispose(); onError(error); }
  };
  const dispose = (): void => {
    if (stopped) return;
    stopped = true;
    port.removeEventListener("message", receive);
    port.close();
    store.dispose();
  };
  port.addEventListener("message", receive);
  port.start();
  return { getSnapshot: store.getSnapshot, subscribe: store.subscribe, dispose };
}
