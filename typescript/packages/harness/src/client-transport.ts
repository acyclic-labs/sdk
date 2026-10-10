import type { ClientCommand, Connection, ReplayCursor, Transport } from "./client.js";
import { RequestScheduler, requestAborted } from "./client-requests.js";

/** Provider-specific reservation, checked by the shared scheduler before work.
 * Include retained commands/cursors and provider buffers, not just wire bytes.
 * The underlying transport remains responsible for bounded inbound framing.
 */
export interface TransportReservations {
  connect(cursors: ReadonlyMap<string, ReplayCursor>): number;
  send(command: ClientCommand): number;
}

/** Composes existing transport correlation/replay with finite request admission.
 * Never retries, deduplicates writes, buffers delivery history or claims an
 * aborted send was rejected. Share its scheduler with foreground hydration.
 */
export class ScheduledTransport<Event> implements Transport<Event> {
  constructor(readonly transport: Transport<Event>, readonly scheduler: RequestScheduler,
    readonly reservations: TransportReservations,
    /** Late cancelled connects have no receiving owner; cleanup failures must
     * still reach the host. Delivered connections report close failures directly. */
    readonly onCleanupError: (error: unknown) => void = error => { queueMicrotask(() => { throw error; }); }) {}

  async connect(cursors: ReadonlyMap<string, ReplayCursor>, signal?: AbortSignal): Promise<Connection<Event>> {
    const controller = new AbortController();
    const cancel = (): void => controller.abort();
    signal?.addEventListener("abort", cancel, { once: true });
    let owned: Connection<Event> | undefined;
    let cleanup: Promise<void> | undefined;
    let detached = false;
    const closeOwned = (): Promise<void> => {
      if (owned === undefined) return Promise.resolve();
      cleanup ??= Promise.resolve().then(() => owned!.close()).catch(error => {
        if (detached) this.onCleanupError(error);
        throw error;
      });
      return cleanup;
    };
    let connection: Connection<Event>;
    try {
      connection = await this.scheduler.schedule(this.reservations.connect(cursors), async abort => {
        // The caller's signal remains live after connection admission settles.
        abort.addEventListener("abort", cancel, { once: true });
        try {
          const connected = await this.transport.connect(cursors, controller.signal);
          owned = connected;
          if (controller.signal.aborted) {
            await closeOwned();
            throw requestAborted();
          }
          return connected;
        } finally { abort.removeEventListener("abort", cancel); }
      }, signal);
      if (controller.signal.aborted) { await closeOwned(); throw requestAborted(); }
    } catch (error) {
      signal?.removeEventListener("abort", cancel);
      detached = owned === undefined;
      await closeOwned();
      throw error;
    }
    // Transfer cleanup ownership only after the cancellation-safe handoff.
    owned = undefined;
    let closed = false;
    let closing: Promise<void> | undefined;
    return {
      [Symbol.asyncIterator]: () => connection[Symbol.asyncIterator](),
      send: async command => {
        if (closed) return Promise.reject(new Error("connection closed; operation outcome may be indeterminate"));
        return this.scheduler.schedule(this.reservations.send(command), () => connection.send(command), controller.signal);
      },
      close: () => {
        if (closing !== undefined) return closing;
        closed = true;
        signal?.removeEventListener("abort", cancel);
        controller.abort();
        closing = Promise.resolve().then(() => connection.close());
        return closing;
      },
    };
  }
}
