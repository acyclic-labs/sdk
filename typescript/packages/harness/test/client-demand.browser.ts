import { DemandLoader, RequestScheduler, Harness, PageWindow, IndexedDbClientStore, HydrationCache, ScheduledTransport,
  type FileRef, type OperationId } from "@acyclic-labs/harness";

const result = document.querySelector<HTMLElement>("#result")!;
const assert = (value: unknown, message: string): void => { if (!value) throw new Error(message); };
try {
  const harness = await Harness.create({ authority: { kind: "conversation", id: "demand-browser" }, issuerId: "demand-issuer", issuerKey: new Uint8Array(32).fill(7) });
  const scheduler = new RequestScheduler({ concurrent: 2, requests: 4, bytes: 8192 });
  const body = new TextEncoder().encode("Pinned selective body\n");
  const descriptor = harness.fileDescriptor(body, "text/plain");
  const volume = { provider: { namespace: "browser", family: "filesystem", version: "2" }, id: "demand", class: "project", owner: { kind: "project", id: "demand" } } as const;
  const refs = Array.from({ length: 4 }, (_, i) => harness.validateFileRef({ volume, path: `messages/${i}`, version: "pinned-1", descriptor, display_name: `${i}` }));
  const scope = harness.issueScope("body-read", [harness.volumeCapability(volume, "read")]);
  let reads = 0, transferred = 0;
  const source = {
    reserve: (file: FileRef) => file.descriptor.byte_length * 3 + 96,
    load: async (file: FileRef, signal: AbortSignal) => {
      harness.verifyContentRead(scope, file);
      reads++;
      const response = await fetch(`./client-demand-body.txt?reference=${file.path}`, { signal, cache: "no-store" });
      if (!response.ok) throw new Error(`body HTTP ${response.status}`);
      const bytes = new Uint8Array(await response.arrayBuffer());
      // This fixture's server only serves the finite body. Real providers must
      // enforce the descriptor bound before buffering their response.
      assert(bytes.buffer.byteLength <= file.descriptor.byte_length, "actual backing allocation");
      harness.verifyFileBytes(file, bytes);
      transferred += bytes.byteLength;
      const value = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      return { value, bytes: value.length * 2 + 96 };
    },
  };
  const demand = new DemandLoader(source, scheduler, 4, 2, 4096);
  const window = new PageWindow<{ id: string; content: FileRef }, number>(item => item.id,
    async () => ({ generation: "pinned-1", items: refs.map((content, i) => ({ id: String(i), content })), hasMoreBefore: false, hasMoreAfter: false }), 4);
  assert(reads === 0, "construction started IO");
  await window.load("after");
  assert(reads === 0 && window.items.length === 4, "reference pages loaded bodies");
  const selected = window.items[1]!.content;
  const [first, duplicate] = await Promise.all([demand.read(selected), demand.read(selected)]);
  assert(first === duplicate && reads === 1, "shared demand duplicated network");
  const coldBytes = transferred;
  const receipt = demand.peek(selected);
  const baselineCache = new HydrationCache<FileRef, typeof receipt>(2, 4096);
  baselineCache.set(selected, receipt, receipt!.bytes);
  const timing = (read: () => unknown): number => {
    const start = performance.now();
    for (let i = 0; i < 100000; i++) read();
    return performance.now() - start;
  };
  timing(() => baselineCache.get(selected)); timing(() => demand.peek(selected));
  const baselineMs = timing(() => baselineCache.get(selected));
  const warmMs = timing(() => demand.peek(selected));
  let stable = true;
  for (let i = 0; i < 10000; i++) stable &&= demand.peek(selected) === receipt;
  assert(stable, "warm receipt allocation");
  assert(warmMs <= Math.max(10, baselineMs * 5), "warm getter regression versus existing LRU baseline");
  assert(transferred === coldBytes, "warm work regression");
  const boundedBytes = demand.residency.bytes;
  for (const file of refs) await demand.read(file);
  assert(demand.residency.entries === 2 && demand.residency.bytes <= 4096, "retention grew with lifetime reads");
  // Actual baseline: the same authenticated reader hydrates the entire window.
  const beforeBaselineBytes = transferred, beforeBaselineReads = reads;
  for (const file of refs) await source.load(file, new AbortController().signal);
  const baselineBytes = transferred - beforeBaselineBytes;
  const baselineReads = reads - beforeBaselineReads;
  assert(coldBytes * 4 === baselineBytes && baselineReads === 4, "cold selective bound versus actual baseline");

  // The reviewed cancellation failures also run on the actual installed browser
  // path. Hold one real body read before IO while other readers join and cancel.
  let release!: () => void;
  const hold = new Promise<void>(resolve => { release = resolve; });
  const slow = new DemandLoader({ ...source, load: async (file: FileRef, signal: AbortSignal) => { await hold; return source.load(file, signal); } }, scheduler, 2, 1, 4096);
  const anchor = slow.read(selected);
  const originalThen = Promise.prototype.then;
  let attachments = 0, retainedAttachments = 0;
  Promise.prototype.then = function(this: Promise<unknown>, ...args: Parameters<typeof originalThen>) {
    attachments++;
    return originalThen.apply(this, args);
  } as typeof originalThen;
  try {
    for (let i = 0; i < 1000; i++) {
      const abort = new AbortController(), before = attachments;
      const reader = slow.read(selected, abort.signal);
      retainedAttachments += attachments - before;
      void reader.catch(() => {});
      abort.abort();
    }
  } finally { Promise.prototype.then = originalThen; }
  assert(retainedAttachments === 0 && slow.residency.demand === 1, "cancelled browser readers retained callbacks");
  release(); await anchor; slow.dispose();
  let closed = 0;
  const handoffAbort = new AbortController();
  const handoff = new ScheduledTransport({ connect: async () => {
    queueMicrotask(() => queueMicrotask(() => handoffAbort.abort()));
    return { async *[Symbol.asyncIterator]() {}, send: async () => {}, close: () => { closed++; } };
  } }, scheduler, { connect: () => 1, send: () => 1 });
  const handoffResult = await handoff.connect(new Map(), handoffAbort.signal).catch(error => error);
  for (let i = 0; i < 8; i++) await Promise.resolve();
  assert(handoffResult.name === "AbortError" && closed === 1, "cancelled browser connection lost cleanup");
  class CompletionScheduler extends RequestScheduler {
    after: (() => void) | undefined;
    override schedule<Value>(bytes: number, work: (signal: AbortSignal, bytes: number) => Promise<Value>, signal?: AbortSignal): Promise<Value> {
      const promise = super.schedule(bytes, work, signal);
      void promise.then(() => this.after?.(), () => {});
      return promise;
    }
  }
  const boundaryScheduler = new CompletionScheduler({ concurrent: 1, requests: 1, bytes: 4096 });
  const boundary = new DemandLoader(source, boundaryScheduler, 1, 1, 4096);
  boundaryScheduler.after = () => boundary.invalidate(selected);
  const boundaryResult = await boundary.read(selected).catch(error => error);
  assert(boundaryResult.name === "AbortError" && boundary.peek(selected) === undefined, "completed provider published an invalidated browser result");
  boundaryScheduler.after = () => boundary.dispose();
  const disposedResult = await boundary.read(selected).catch(error => error);
  assert(disposedResult.name === "AbortError" && boundary.residency.demand === 0, "completed provider published a disposed browser result");
  boundaryScheduler.dispose();

  // Exercise the existing atomic browser transaction, retaining its honest
  // cursor-only contract. This is not projection-checkpoint recovery evidence.
  const databaseName = `harness-cl3-${crypto.randomUUID()}`;
  const state = new IndexedDbClientStore({ databaseName, maximumCommands: 2, maximumBytes: 8192 });
  const operationId = harness.identity("operation", "01010101-0101-0101-0101-010101010101") as OperationId;
  const authority = { kind: "conversation", id: "demand-browser" } as const;
  await state.put({ operationId, authority, kind: "message.append", payload: { content: selected }, offlineSafe: true });
  await state.commit(authority, { generation: "pinned-1", revision: 1n }, operationId);
  const restarted = new IndexedDbClientStore({ databaseName });
  assert((await restarted.load()).length === 0 && (await restarted.loadCursors()).get("conversation:demand-browser")?.revision === 1n, "atomic cursor/outbox restart");
  demand.dispose(); baselineCache.clear(); scheduler.dispose(); harness.free();
  assert(scheduler.residency.requests === 0 && demand.residency.bytes === 0, "disposed residency");
  result.dataset.status = "passed";
  result.textContent = JSON.stringify({ coldReads: 1, coldBytes, baselineReads, baselineBytes, warmReads: 100000, baselineMs, warmMs, warmTransferredBytes: 0, warmReceiptAllocations: 0, demandResidentBytes: boundedBytes, retainedEntries: 2, cancelledReaderAttachments: retainedAttachments, cancelledConnectionCloses: closed });
} catch (error) {
  result.dataset.status = "failed";
  result.textContent = error instanceof Error ? error.stack ?? error.message : String(error);
  throw error;
}
