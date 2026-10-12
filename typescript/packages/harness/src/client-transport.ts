import type { ClientCommand, Connection, ReplayCursor, Transport } from "./client.js";
import { RequestScheduler, requestAborted } from "./client-requests.js";

/** Reserve retained commands, cursors and buffers before work; the transport bounds inbound frames. */
export interface TransportReservations {
  connect(cursors: ReadonlyMap<string, ReplayCursor>): number;
  send(command: ClientCommand): number;
}

/** Share finite request admission with hydration; preserve transport correlation and replay. */
export class ScheduledTransport<Event> implements Transport<Event> {
  constructor(readonly transport: Transport<Event>, readonly scheduler: RequestScheduler,
    readonly reservations: TransportReservations,
    /** Report late cancelled connection cleanup failures to the host. */
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
    return {
      [Symbol.asyncIterator]: () => connection[Symbol.asyncIterator](),
      send: async command => {
        if (cleanup !== undefined) return Promise.reject(new Error("connection closed; operation outcome may be indeterminate"));
        return this.scheduler.schedule(this.reservations.send(command), () => connection.send(command), controller.signal);
      },
      close: () => {
        signal?.removeEventListener("abort", cancel);
        controller.abort();
        return closeOwned();
      },
    };
  }
}
