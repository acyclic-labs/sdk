import { expect, test } from "bun:test";
import { DemandLoader } from "../src/client-demand.js";
import { RequestCapacityError, RequestScheduler } from "../src/client-requests.js";
import { ScheduledTransport } from "../src/client-transport.js";
import type { ClientCommand, Connection } from "../src/client.js";

function deferred<Value>() {
  let resolve!: (value: Value) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<Value>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const turn = async (): Promise<void> => { for (let i = 0; i < 8; i++) await Promise.resolve(); };

test("pre-work admission and abort keep actual unsettled provider capacity reserved", async () => {
  const scheduler = new RequestScheduler({ concurrent: 1, requests: 2, bytes: 8 });
  const running = deferred<number>();
  const abort = new AbortController();
  let starts = 0;
  const first = scheduler.schedule(4, async () => { starts++; return running.promise; }, abort.signal);
  const failed = first.catch(error => error);
  const queuedAbort = new AbortController();
  const second = scheduler.schedule(4, async () => { starts++; return 2; }, queuedAbort.signal);
  const secondFailed = second.catch(error => error);
  await expect(scheduler.schedule(0, async () => 0)).rejects.toBeInstanceOf(RequestCapacityError);
  await turn();
  abort.abort(); queuedAbort.abort();
  expect((await failed).name).toBe("AbortError"); expect((await secondFailed).name).toBe("AbortError");
  expect(starts).toBe(1);
  expect(scheduler.residency).toEqual({ requests: 1, running: 1, bytes: 4 });
  const next = scheduler.schedule(4, async () => { starts++; throw new Error("provider fault"); });
  const nextFailed = next.catch(error => error);
  await turn(); expect(starts).toBe(1);
  running.resolve(1);
  expect((await nextFailed).message).toBe("provider fault");
  expect(scheduler.residency).toEqual({ requests: 0, running: 0, bytes: 0 });
  const immediateAbort = new AbortController();
  const never = scheduler.schedule(1, async () => { starts++; return 0; }, immediateAbort.signal);
  const neverFailed = never.catch(error => error);
  immediateAbort.abort(); expect((await neverFailed).name).toBe("AbortError"); await turn();
  expect(starts).toBe(2);
  const unsettled = deferred<number>();
  const disposedRunning = scheduler.schedule(4, () => unsettled.promise).catch(error => error);
  const disposedQueued = scheduler.schedule(4, async () => { starts++; return 3; }).catch(error => error);
  await turn(); scheduler.dispose();
  expect((await disposedRunning).name).toBe("AbortError");
  expect((await disposedQueued).name).toBe("AbortError");
  expect(scheduler.residency.requests).toBe(1);
  unsettled.resolve(0); await turn();
  expect(scheduler.residency.requests).toBe(0);
  expect(starts).toBe(2);
  scheduler.dispose();
});

test("shared selective hydration isolates caller abort, fences stale completion and bounds warm residency", async () => {
  const scheduler = new RequestScheduler({ concurrent: 2, requests: 2, bytes: 8 });
  let reads = 0;
  let selected = deferred<{ value: Readonly<{ ref: string }>; bytes: number }>();
  const loader = new DemandLoader({ reserve: () => 4, load: async () => { reads++; return selected.promise; } }, scheduler, 2, 1, 4);
  expect(reads).toBe(0);
  const abort = new AbortController();
  const first = loader.read("pin-1", abort.signal);
  const firstFailed = first.catch(error => error);
  const second = loader.read("pin-1");
  await expect(loader.read("pin-2")).rejects.toBeInstanceOf(RequestCapacityError);
  await turn(); expect(reads).toBe(1);
  // Count only attachments made synchronously inside read. An anchor keeps the
  // same provider pending while other readers churn through admission/cancel.
  const originalThen = Promise.prototype.then;
  let attachments = 0, retainedAttachments = 0;
  Promise.prototype.then = function(this: Promise<unknown>, ...args: Parameters<typeof originalThen>) {
    attachments++;
    return originalThen.apply(this, args);
  } as typeof originalThen;
  try {
    // Release the first interest to leave room for one transient reader.
    abort.abort();
    for (let i = 0; i < 1000; i++) {
      const churn = new AbortController();
      const before = attachments;
      const reader = loader.read("pin-1", churn.signal);
      retainedAttachments += attachments - before;
      void reader.catch(() => {});
      churn.abort();
    }
  } finally { Promise.prototype.then = originalThen; }
  expect(retainedAttachments).toBe(0);
  expect(loader.residency.demand).toBe(1);
  abort.abort(); expect((await firstFailed).name).toBe("AbortError");
  const value = Object.freeze({ ref: "pin-1" });
  selected.resolve({ value, bytes: 4 });
  expect(await second).toBe(value);
  let stable = true;
  for (let i = 0; i < 1000; i++) stable &&= loader.peek("pin-1")?.value === value;
  expect(stable).toBe(true);
  expect(reads).toBe(1);
  expect(loader.residency).toEqual({ demand: 0, pending: 0, entries: 1, bytes: 4 });
  selected = deferred();
  const stale = loader.read("pin-2");
  const staleFailed = stale.catch(error => error);
  await turn(); loader.invalidate("pin-2"); expect((await staleFailed).name).toBe("AbortError");
  selected.resolve({ value: Object.freeze({ ref: "stale" }), bytes: 4 });
  await turn(); expect(loader.peek("pin-2")).toBeUndefined();
  selected = deferred();
  const oversized = loader.read("pin-3");
  const oversizedFailed = oversized.catch(error => error);
  selected.resolve({ value, bytes: 5 }); expect((await oversizedFailed).message).toContain("reserved bytes"); await turn();
  expect(loader.peek("pin-3")).toBeUndefined();
  const transient = new DemandLoader({ reserve: () => 8, load: async () => ({ value, bytes: 4 }) }, scheduler, 1, 1, 4);
  expect(await transient.read("pin-4")).toBe(value);
  transient.dispose();
  class CompletionScheduler extends RequestScheduler {
    after: (() => void) | undefined;
    override schedule<Value>(bytes: number, work: (signal: AbortSignal, bytes: number) => Promise<Value>, signal?: AbortSignal): Promise<Value> {
      const promise = super.schedule(bytes, work, signal);
      void promise.then(() => this.after?.(), () => {});
      return promise;
    }
  }
  const boundaryScheduler = new CompletionScheduler({ concurrent: 1, requests: 1, bytes: 4 });
  const boundary = new DemandLoader({ reserve: () => 4, load: async () => ({ value, bytes: 4 }) }, boundaryScheduler, 1, 1, 4);
  boundaryScheduler.after = () => boundary.invalidate("completion-boundary");
  const invalidatedResult = await boundary.read("completion-boundary").catch(error => error);
  expect(invalidatedResult.name).toBe("AbortError");
  expect(boundary.peek("completion-boundary")).toBeUndefined();
  boundaryScheduler.after = () => boundary.dispose();
  const disposedResult = await boundary.read("dispose-boundary").catch(error => error);
  expect(disposedResult.name).toBe("AbortError");
  expect(boundary.residency.demand).toBe(0);
  boundary.dispose(); boundaryScheduler.dispose();
  loader.dispose(); scheduler.dispose();
});

test("transport shares request pressure and preserves original commands, delivery and lifecycle abort", async () => {
  const scheduler = new RequestScheduler({ concurrent: 1, requests: 2, bytes: 8 });
  const sent: ClientCommand[] = [];
  const blocked = deferred<void>();
  let closed = 0;
  let providerSignal: AbortSignal | undefined;
  const underlying: Connection<number> = {
    async *[Symbol.asyncIterator]() { /* no history buffering */ },
    send: async command => { sent.push(command); await blocked.promise; },
    close: () => { closed++; },
  };
  const transport = new ScheduledTransport({ connect: async (_cursors, signal) => { providerSignal = signal; return underlying; } }, scheduler,
    { connect: () => 1, send: () => 4 });
  const abort = new AbortController();
  const connected = await transport.connect(new Map(), abort.signal);
  const command = { operationId: "original-operation", authority: { kind: "task", id: "original-authority" }, kind: "task.cancel", payload: {} } as ClientCommand;
  const first = connected.send(command);
  const firstFailed = first.catch(error => error);
  const second = connected.send(command);
  const secondFailed = second.catch(error => error);
  await expect(scheduler.schedule(1, async () => 1)).rejects.toBeInstanceOf(RequestCapacityError);
  await turn(); expect(sent).toEqual([command]); expect(sent[0]).toBe(command);
  abort.abort(); expect(providerSignal?.aborted).toBe(true);
  expect((await firstFailed).name).toBe("AbortError"); expect((await secondFailed).name).toBe("AbortError");
  expect(scheduler.residency.requests).toBe(1);
  await connected.close(); await connected.close(); expect(closed).toBe(1);
  blocked.resolve(); await turn(); expect(scheduler.residency.requests).toBe(0);
  const late = deferred<Connection<number>>();
  const lateTransport = new ScheduledTransport({ connect: () => late.promise }, scheduler, { connect: () => 1, send: () => 4 });
  const lateAbort = new AbortController();
  const cancelledConnect = lateTransport.connect(new Map(), lateAbort.signal).catch(error => error);
  await turn(); lateAbort.abort();
  expect((await cancelledConnect).name).toBe("AbortError");
  expect(scheduler.residency.requests).toBe(1);
  late.resolve(underlying); await turn();
  expect(closed).toBe(2);
  expect(scheduler.residency.requests).toBe(0);
  // Cancellation in the microtask between provider completion and scheduler
  // handoff must still close a connection the caller never received.
  const handoffAbort = new AbortController();
  const handoff = new ScheduledTransport({ connect: async () => {
    queueMicrotask(() => queueMicrotask(() => handoffAbort.abort()));
    return underlying;
  } }, scheduler, { connect: () => 1, send: () => 4 });
  expect((await handoff.connect(new Map(), handoffAbort.signal).catch(error => error)).name).toBe("AbortError");
  await turn(); expect(closed).toBe(3);
  const closeFault = new Error("close failed");
  const failedClose = new ScheduledTransport({ connect: async () => ({ ...underlying, close: () => { throw closeFault; } }) },
    scheduler, { connect: () => 1, send: () => 4 });
  const faultConnection = await failedClose.connect(new Map());
  expect(await Promise.resolve(faultConnection.close()).catch(error => error)).toBe(closeFault);
  expect(await Promise.resolve(faultConnection.close()).catch(error => error)).toBe(closeFault);
  const lateFault = deferred<Connection<number>>();
  const cleanupErrors: unknown[] = [];
  const detachedFault = new ScheduledTransport({ connect: () => lateFault.promise }, scheduler,
    { connect: () => 1, send: () => 4 }, error => cleanupErrors.push(error));
  const detachedAbort = new AbortController();
  const detachedConnect = detachedFault.connect(new Map(), detachedAbort.signal).catch(error => error);
  await turn(); detachedAbort.abort();
  expect((await detachedConnect).name).toBe("AbortError");
  lateFault.resolve({ ...underlying, close: () => { throw closeFault; } });
  await turn(); expect(cleanupErrors).toEqual([closeFault]);
  scheduler.dispose();
});
