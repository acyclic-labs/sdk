import { DemandLoader, RequestScheduler, Harness, HarnessClient, ClientQuery, TaskVolume, WasmStream,
  initializeClientQueries, PageWindow, HydrationCache, type FileRef } from "@acyclic-labs/harness";
const result = document.querySelector<HTMLElement>("#result")!;
const assert = (value: unknown, message: string): void => { if (!value) throw new Error(message); };
try {
  const harness = await Harness.create({ authority: { kind: "conversation", id: "demand-browser" }, issuerId: "demand-issuer", issuerKey: new Uint8Array(32).fill(7) });
  const scheduler = new RequestScheduler({ concurrent: 2, requests: 4, bytes: 8192 });
  const body = new TextEncoder().encode("Pinned selective body\n");
  const descriptor = harness.contracts.fileDescriptor(body, "text/plain");
  const volume = { provider: { namespace: "browser", family: "filesystem", version: "2" }, id: "demand", class: "project", owner: { kind: "project", id: "demand" } } as const;
  const refs = Array.from({ length: 4 }, (_, i) => harness.contracts.validate("file_ref", { volume, path: `messages/${i}`, version: "pinned-1", descriptor, display_name: `${i}` }));
  const scope = harness.issueScope("body-read", [harness.volumeCapability(volume, "read")]);
  let reads = 0, transferred = 0, allocatedBytes = 0, mainThreadMs = 0;
  const source = {
    reserve: (file: FileRef) => file.descriptor.byte_length * 3 + 96,
    load: async (file: FileRef, signal: AbortSignal) => {
      const admissionStart = performance.now();
      harness.verifyContentRead(scope, file);
      reads++;
      const fetching = fetch(`./client-demand-body.txt?reference=${file.path}`, { signal, cache: "no-store" });
      mainThreadMs += performance.now() - admissionStart;
      const response = await fetching;
      if (!response.ok) throw new Error(`body HTTP ${response.status}`);
      const bytes = new Uint8Array(await response.arrayBuffer());
      const workStart = performance.now();
      assert(bytes.buffer.byteLength <= file.descriptor.byte_length, "actual backing allocation");
      harness.contracts.verifyFileBytes(file, bytes);
      transferred += bytes.byteLength;
      const value = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      allocatedBytes += bytes.byteLength + value.length * 2;
      mainThreadMs += performance.now() - workStart;
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
  const coldStart = performance.now();
  const [first, duplicate] = await Promise.all([demand.read(selected), demand.read(selected)]);
  const coldMs = performance.now() - coldStart;
  assert(first === duplicate && reads === 1, "shared demand duplicated network");
  const coldBytes = transferred;
  const coldAllocatedBytes = allocatedBytes, coldMainThreadMs = mainThreadMs;
  const nonSpeculative = new DemandLoader(source, scheduler, 4, 2, 4096);
  const baselineColdStart = performance.now();
  const nonSpeculativeValue = await nonSpeculative.read(selected);
  const nonSpeculativeColdMs = performance.now() - baselineColdStart;
  const nonSpeculativeMainThreadMs = mainThreadMs - coldMainThreadMs;
  assert(nonSpeculativeValue === first && transferred === coldBytes * 2 &&
    allocatedBytes === coldAllocatedBytes * 2 && nonSpeculative.residency.bytes === demand.residency.bytes,
    "non-speculative equivalent changed IO, allocation or residency");
  assert(coldMs <= Math.max(1000, nonSpeculativeColdMs * 10), "cold interaction exceeded recorded one-body/1s baseline");
  assert(coldMainThreadMs <= Math.max(20, nonSpeculativeMainThreadMs * 5), "cold main-thread work exceeded non-speculative baseline");
  nonSpeculative.dispose();
  const receipt = demand.peek(selected);
  const baselineCache = new HydrationCache<FileRef, typeof first>(2, 4096);
  baselineCache.set(selected, receipt!.value, receipt!.bytes);
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
  assert(transferred === coldBytes * 2 && allocatedBytes === coldAllocatedBytes * 2, "warm work regression");
  const boundedBytes = demand.residency.bytes;
  for (const file of refs) await demand.read(file);
  assert(demand.residency.entries === 2 && demand.residency.bytes <= 4096, "retention grew with lifetime reads");
  const beforeBaselineBytes = transferred, beforeBaselineReads = reads;
  for (const file of refs) await source.load(file, new AbortController().signal);
  const baselineBytes = transferred - beforeBaselineBytes;
  const baselineReads = reads - beforeBaselineReads;
  assert(coldBytes * 4 === baselineBytes && baselineReads === 4, "cold selective bound versus actual baseline");
  const beforeDeniedReads = reads;
  const foreign = harness.contracts.validate("file_ref", { ...selected, volume: { ...volume, id: "foreign" } });
  const denied = await demand.read(foreign).catch(error => error);
  assert(denied instanceof Error && reads === beforeDeniedReads && demand.peek(foreign) === undefined,
    "foreign authority fetched or retained a body");
  const excessive = new DemandLoader({ ...source, reserve: () => 8193 }, scheduler, 1, 1, 4096);
  const overBudget = await excessive.read(selected).catch(error => error);
  assert(overBudget instanceof Error && reads === beforeDeniedReads && excessive.residency.pending === 0,
    "work-bound mutation dispatched unreserved demand");
  excessive.dispose();

  let release!: () => void;
  const hold = new Promise<void>(resolve => { release = resolve; });
  const slow = new DemandLoader({ ...source, load: async (file: FileRef, signal: AbortSignal) => { await hold; return source.load(file, signal); } }, scheduler, 2, 1, 4096);
  const anchor = slow.read(selected);
  for (let i = 0; i < 1000; i++) {
    const abort = new AbortController();
    const reader = slow.read(selected, abort.signal);
    void reader.catch(() => {});
    abort.abort();
  }
  assert(slow.residency.demand === 1, "cancelled browser readers retained demand");
  release(); await anchor; slow.dispose();
  class CompletionScheduler extends RequestScheduler {
    after: (() => void) | undefined;
    override schedule<Value>(bytes: number, work: (signal: AbortSignal) => Promise<Value>,
      signal?: AbortSignal, capture?: () => void): Promise<Value> {
      const promise = super.schedule(bytes, work, signal, capture);
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
  await initializeClientQueries();
  const restartKey = "harness-file-client-restart";
  const previous = sessionStorage.getItem(restartKey);
  const databaseName = previous ?? `harness-file-client-${crypto.randomUUID()}`;
  const streams = await WasmStream.openBrowser(databaseName, 64, 65536n);
  const outbox = await TaskVolume.create(streams, "client/files");
  const target = await TaskVolume.create(streams, "agent/files");
  const client = await HarnessClient.open(outbox, target, { commands: 2n, bytes: 8192n, durability: "Durable" });
  const key = { Task: { task: "task", file: { Request: "offline.json" } } } as const;
  if (previous === null) {
    await client.enqueue(key, new TextEncoder().encode(JSON.stringify({ operationId: "offline", authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} })));
    assert(client.pending() === 1, "request did not reach the volume outbox");
    sessionStorage.setItem(restartKey, databaseName);
    client.free(); outbox.free(); target.free(); streams.free();
    location.reload();
    await new Promise<never>(() => {});
  }
  assert(client.pending() === 1, "document restart lost pending intent");
  const readerStreams = await WasmStream.openBrowser(databaseName, 64, 65536n);
  const readerVolume = await TaskVolume.open(readerStreams, "agent/files");
  const query = await ClientQuery.open(readerVolume, "Tasks", null, { files: 2n, segments: 2n, bytes: 8192n });
  const views = query.views();
  const stop = Promise.withResolvers<void>();
  let received = 0;
  const follow = query.follow(delivery => { if (delivery.file) received++; }, () => stop.resolve(), stop.promise);
  assert(await client.flush() === 1, "request was not admitted exactly once");
  await follow;
  assert(received === 1 && client.pending() === 0, "live publication or acknowledgement lost");
  assert(JSON.parse(new TextDecoder().decode(views.segments(key)![0])).operationId === "offline", "live view differs from persisted request");
  const cursor = client.cursor();
  views.free(); query.free(); readerVolume.free(); readerStreams.free(); client.free();
  const restored = await HarnessClient.open(outbox, target, { commands: 2n, bytes: 8192n, durability: "Durable" });
  assert(restored.pending() === 0 && restored.cursor().position === cursor.position, "acknowledged request replayed on reopen");
  restored.free(); outbox.free(); target.free(); streams.free();
  sessionStorage.removeItem(restartKey);
  demand.dispose(); baselineCache.clear(); scheduler.dispose(); harness.free();
  assert(scheduler.residency.requests === 0 && demand.residency.bytes === 0, "disposed residency");
  result.dataset.status = "passed";
  result.textContent = JSON.stringify({ coldReads: 1, coldMs, nonSpeculativeColdMs, coldAllocatedBytes, coldMainThreadMs, nonSpeculativeMainThreadMs, coldBytes, baselineReads, baselineBytes, warmReads: 100000, baselineMs, warmMs, warmTransferredBytes: 0, warmReceiptAllocations: 0, demandResidentBytes: boundedBytes, retainedEntries: 2, documentRestarts: 1, recoveredRequests: received, cursorPosition: String(cursor.position) });
} catch (error) {
  result.dataset.status = "failed";
  result.textContent = error instanceof Error ? error.stack ?? error.message : String(error);
  throw error;
}
