import { DemandLoader, RequestScheduler, Harness, HarnessClient, ProjectionStore, ClientViews,
  PageWindow, IndexedDbClientStore, HydrationCache, ScheduledTransport, ClientCheckpointConflict,
  type FileRef, type OperationId, type ReplayCursor, type ClientDomain, type ClientObservation,
  type ClientHypothesis } from "@acyclic-labs/harness";

const result = document.querySelector<HTMLElement>("#result")!;
const assert = (value: unknown, message: string): void => { if (!value) throw new Error(message); };
const parameters = new URLSearchParams(location.search);
if (parameters.get("role") === "stale-client") {
  const state = new IndexedDbClientStore({ databaseName: parameters.get("database")! });
  const authority = { kind: "conversation", id: "demand-browser" } as const;
  const operationId = "01010101-0101-0101-0101-010101010101" as OperationId;
  const delivery = Promise.withResolvers<void>();
  const receive = (event: MessageEvent): void => {
    if (event.source === parent && event.origin === location.origin && event.data === "deliver") delivery.resolve();
  };
  addEventListener("message", receive);
  const client = new HarnessClient<number>({ connect: async cursors => {
    assert(cursors.get("conversation:demand-browser")?.revision === 1n && projection.getSnapshot() === 1,
      "stale tab connected before restoration");
    parent.postMessage("restored", location.origin);
    return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {
      await delivery.promise;
      yield { authority, generation: "one", fromRevision: 1n, throughRevision: 2n, live: true,
        events: [{ authority, revision: 2n, operationId, event: 1 }] };
    } };
  } }, state);
  const projection = new ProjectionStore(client, 0, (value, event) => value + event.event, {
    authority, identity: "browser-race-v1", encode: value => value,
    decode: value => { if (typeof value !== "number") throw new Error("invalid race checkpoint"); return value; },
  });
  try {
    projection.start();
    const failure = await client.run().catch(error => error);
    parent.postMessage({ conflict: failure instanceof ClientCheckpointConflict, published: projection.getSnapshot() }, location.origin);
  } catch (error) {
    parent.postMessage({ error: String(error) }, location.origin);
  } finally {
    removeEventListener("message", receive);
    client.dispose(); projection.dispose();
  }
} else {
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
    override schedule<Value>(bytes: number, work: (signal: AbortSignal, bytes: number) => Promise<Value>,
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

  // The verified immutable reference, projection cut and local hypothesis share
  // one checkpoint. Storage does not authenticate facts or operation outcomes.
  const restartKey = "harness-client-durable-restart";
  const restartedDatabase = sessionStorage.getItem(restartKey);
  const databaseName = restartedDatabase ?? `harness-cl3-${crypto.randomUUID()}`;
  const state = new IndexedDbClientStore({ databaseName, maximumCommands: 2, maximumBytes: 8192 });
  const operationId = harness.identity("operation", "01010101-0101-0101-0101-010101010101") as OperationId;
  const authority = { kind: "conversation", id: "demand-browser" } as const;
  type Value = Readonly<{ content: FileRef }>;
  type Evidence = ClientObservation<Value>;
  const samePin = (file: FileRef): boolean => JSON.stringify(file) === JSON.stringify(selected);
  const basis = "conversation:demand-browser/pinned-1/1/browser/demand";
  const verified: Evidence = { fact: { key: "message", basis, value: { content: selected }, bytes: 2048 }, operation: null, work: 1 };
  const trusted = new WeakSet<object>([verified]);
  const domain: ClientDomain<Value, null, Evidence> = {
    identity: "17",
    validate: (_fact, predicted) => {
      if (!samePin(predicted.content)) throw new Error("foreign immutable pin");
      return { bytes: 2048, work: 1 };
    },
    observe: evidence => { if (!trusted.has(evidence)) throw new Error("untrusted fact"); return evidence; },
    corresponds: (predicted, canonical) => ({ matches: samePin(predicted.content) && samePin(canonical.content), work: 1 }),
    restore: (fact, branch) => {
      if (fact.basis !== basis || branch.basis !== basis || branch.adapter !== "17" ||
          branch.operation !== null || branch.prediction !== "Pending" || branch.outcome !== "Unknown" ||
          !samePin(branch.predicted.content)) throw new Error("untrusted hypothesis provenance");
      return { bytes: 2048, work: 1 };
    },
  };
  const limits = { records: 2, branches: 2, edges: 2, bytes: 32768, work: 64, retention: 10n, visible: 2 };
  const original = new ClientViews(domain, "72", 0n, limits);
  original.observe("message", verified);
  const branch = original.begin({ key: "message", basis, operation: null, predicted: { content: selected },
    assumption: null, dependencies: [], expires: 5n });
  type Cut = { content: FileRef | null; sequence: bigint; hypothesis: ClientHypothesis<Value, null> };
  const initial: Cut = { content: null, sequence: original.sequence(), hypothesis: original.checkpoint(branch) };
  const kernels: ClientViews<Value, null, Evidence>[] = [];
  const recovery = {
    authority, identity: "browser/demand/conversation:demand-browser/message-reducer-1/schema-1/adapter-17",
    encode: (cut: Cut) => cut,
    decode: (data: unknown, cursor: ReplayCursor): Cut => {
      const cut = data as Cut;
      if (cursor.generation !== "pinned-1" || cursor.revision !== 1n || cut.content === null ||
          !samePin(cut.content) || cut.sequence !== 1n || cut.hypothesis.branch !== branch) throw new Error("invalid projection cut");
      const kernel = new ClientViews(domain, "72", cut.sequence, limits);
      try {
        kernel.observe("message", verified);
        kernel.restore(cut.hypothesis);
        assert(samePin(kernel.view("message", [branch]).value.content), "restored speculation lost immutable pin");
        kernels.push(kernel);
      } catch (error) { kernel.dispose(); throw error; }
      return cut;
    },
  };
  const reduce = (cut: Cut, event: { event: FileRef }): Cut => {
    if (!samePin(event.event)) throw new Error("foreign authoritative event pin");
    return { ...cut, content: event.event };
  };
  if (restartedDatabase === null) {
  await state.put({ operationId, authority, kind: "message.append", payload: { content: selected }, offlineSafe: true });
  const stop = new AbortController();
  const client = new HarnessClient<FileRef>({ connect: async () => ({ send: async () => {}, close() {},
    async *[Symbol.asyncIterator]() {
      yield { authority, generation: "pinned-1", fromRevision: 0n, throughRevision: 1n, live: true,
        events: [{ authority, revision: 1n, operationId, event: selected }] };
      stop.abort();
    } }) }, state);
  const projection = new ProjectionStore(client, initial, reduce, recovery);
  projection.start(); await client.run(stop.signal);
    // Commit first, then destroy the whole document/JS heap and its IDB handles.
    // The second navigation must recover before its first network connection.
    projection.dispose(); client.dispose(); original.dispose();
    demand.dispose(); baselineCache.clear(); scheduler.dispose(); harness.free();
    sessionStorage.setItem(restartKey, databaseName);
    location.reload();
  } else {
  const restarted = new IndexedDbClientStore({ databaseName });
  assert((await restarted.load()).length === 0 && (await restarted.loadCursors()).get("conversation:demand-browser")?.revision === 1n, "atomic cursor/outbox restart");
  const resumedStop = new AbortController();
  let resumed!: ProjectionStore<Cut, FileRef>;
  const second = new HarnessClient<FileRef>({ connect: async cursors => {
    assert(cursors.get("conversation:demand-browser")?.revision === 1n && resumed.getSnapshot().content !== null && kernels.length === 1,
      "network resumed before validated projection and hypothesis recovery");
    resumedStop.abort(); return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {} };
  } }, restarted);
  resumed = new ProjectionStore(second, initial, reduce, recovery);
  resumed.start(); await second.run(resumedStop.signal);
  assert(kernels[0]!.checkpoint(branch).outcome === "Unknown", "persistence invented operation completion");
  resumed.dispose(); second.dispose(); original.dispose();
  for (const kernel of kernels) kernel.dispose();
  sessionStorage.removeItem(restartKey);

  // A separate browser realm restores the old cut, then loses a same-cut CAS
  // against this tab. A stale authoritative delivery must not ACK its operation.
  const raceName = `${databaseName}-race`;
  const race = new IndexedDbClientStore({ databaseName: raceName });
  const raceCursor = { generation: "one", revision: 1n };
  const initialVersion = await race.commit(authority, raceCursor, undefined,
    { identity: "browser-race-v1", state: 1 }, null, null);
  await race.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const actor = document.createElement("iframe");
  const ready = Promise.withResolvers<void>();
  const staleResult = Promise.withResolvers<{ conflict: boolean; published: number }>();
  const receive = (event: MessageEvent): void => {
    if (event.source !== actor.contentWindow || event.origin !== location.origin) return;
    if (event.data === "restored") ready.resolve();
    else if (event.data?.error !== undefined) { ready.reject(new Error(event.data.error)); staleResult.reject(new Error(event.data.error)); }
    else staleResult.resolve(event.data);
  };
  addEventListener("message", receive);
  try {
    actor.src = `${location.pathname}?role=stale-client&database=${encodeURIComponent(raceName)}`;
    document.body.append(actor);
    await ready.promise;
    const replacementVersion = await race.commit(authority, raceCursor, undefined,
      { identity: "browser-race-v1", state: 10 }, raceCursor, initialVersion);
    assert(replacementVersion > initialVersion, "same-cut checkpoint reused version");
    actor.contentWindow!.postMessage("deliver", location.origin);
    const stale = await staleResult.promise;
    assert(stale.conflict && stale.published === 1, "stale tab published its delivery");
    assert((await race.loadRecovery())[0]?.checkpoint?.state === 10 &&
      (await race.load())[0]?.operationId === operationId, "stale delivery changed checkpoint or ACK");
    await race.deleteCursor(authority);
    const reopenedRace = new IndexedDbClientStore({ databaseName: raceName });
    const recreated = await reopenedRace.commit(authority, raceCursor, undefined,
      { identity: "browser-race-v1", state: 20 }, null, null);
    const aba = await race.commit(authority, { ...raceCursor, revision: 2n }, operationId,
      { identity: "browser-race-v1", state: 2 }, raceCursor, initialVersion).catch(error => error);
    assert(recreated > replacementVersion && aba instanceof ClientCheckpointConflict &&
      (await race.load())[0]?.operationId === operationId, "delete/recreate revived stale tab");
  } finally {
    removeEventListener("message", receive); actor.remove();
  }

  // Existing cursor-only data is not permission to reconnect an empty UI.
  const cursorOnly = new IndexedDbClientStore({ databaseName: `${databaseName}-cursor-only` });
  await cursorOnly.putCursor(authority, { generation: "one", revision: 9n });
  const cursorStop = new AbortController();
  let cursorConnections = 0;
  const cursorClient = new HarnessClient({ connect: async () => {
    cursorConnections++; cursorStop.abort();
    return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {} };
  } }, cursorOnly);
  const cursorFailure = await cursorClient.run(cursorStop.signal).catch(error => error);
  assert(cursorFailure instanceof Error && cursorFailure.message.includes("matching recoverable projection") &&
    cursorConnections === 0 && (await cursorOnly.loadCursors()).get("conversation:demand-browser")?.revision === 9n,
    "cursor-only restart guessed state or deleted durable evidence");
  cursorClient.dispose();

  // Production IDB paged fixed cuts cannot shrink or include newly enqueued work
  // after another tab deletes the entire captured tail.
  const pages = new IndexedDbClientStore({ databaseName: `${databaseName}-pages`, maximumCommands: 80 });
  const ids = Array.from({ length: 70 }, (_, index) =>
    `${index.toString(16).padStart(8, "0")}-0000-0000-0000-000000000001` as OperationId);
  for (const id of ids) await pages.put({ operationId: id, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const firstPage = await pages.loadPage();
  assert(firstPage.commands.length === 64 && firstPage.throughSequence === 70, "unbounded or unordered browser outbox page");
  for (const id of ids.slice(64)) await pages.delete(id);
  const freshOperation = "ffffffff-ffff-ffff-ffff-ffffffffffff" as OperationId;
  await pages.put({ operationId: freshOperation, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const emptyTail = await pages.loadPage(firstPage.afterSequence, firstPage.throughSequence);
  assert(emptyTail.commands.length === 0 && emptyTail.afterSequence === 70, "deleted tail extended or stalled fixed cut");
  const nextCut = await pages.loadPage(70);
  assert(nextCut.commands.length === 1 && nextCut.commands[0]?.operationId === freshOperation &&
    nextCut.throughSequence === 71, "enqueue allocator recycled deleted sequence");

  // Listener failure is pre-ACK and at least once; the durable reducer is staged
  // on retry rather than applied twice to the visible projection.
  const listenerState = new IndexedDbClientStore({ databaseName: `${databaseName}-listeners` });
  await listenerState.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const listenerStop = new AbortController();
  let listenerConnections = 0, firstCalls = 0, laterCalls = 0;
  const listenerClient = new HarnessClient<number>({ connect: async cursors => {
    assert(++listenerConnections <= 2 && !cursors.has("conversation:demand-browser"), "listener failure was acknowledged");
    return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {
      yield { authority, generation: "one", fromRevision: 0n, throughRevision: 1n, live: true,
        events: [{ authority, revision: 1n, operationId, event: 1 }] };
      listenerStop.abort();
    } };
  } }, listenerState);
  const listenerProjection = new ProjectionStore(listenerClient, 0, (value, event) => value + event.event, {
    authority, identity: "listener-v1", encode: value => value,
    decode: value => { if (typeof value !== "number") throw new Error("invalid listener checkpoint"); return value; },
  });
  listenerProjection.start();
  listenerClient.subscribe(() => { if (++firstCalls === 1) throw new Error("retry listener"); });
  listenerClient.subscribe(() => { laterCalls++; });
  await listenerClient.run(listenerStop.signal);
  assert(firstCalls === 2 && laterCalls === 1 && listenerProjection.getSnapshot() === 1 &&
    (await listenerState.load()).length === 0 && (await listenerState.loadRecovery())[0]?.checkpoint?.state === 1,
    "listener ordering lost delivery or double-reduced projection");
  listenerClient.dispose(); listenerProjection.dispose();

  // Disposal cannot prepare the rest of a delivery while a real durable commit
  // is still settling. Restart sees precisely the single committed checkpoint.
  const commitEntered = Promise.withResolvers<void>();
  const releaseCommit = Promise.withResolvers<void>();
  const commitSettled = Promise.withResolvers<void>();
  class PausedBrowserStore extends IndexedDbClientStore {
    override async commit(...args: Parameters<IndexedDbClientStore["commit"]>): Promise<bigint> {
      commitEntered.resolve(); await releaseCommit.promise;
      try { return await super.commit(...args); } finally { commitSettled.resolve(); }
    }
  }
  const disposedName = `${databaseName}-disposed`;
  const disposedState = new PausedBrowserStore({ databaseName: disposedName });
  const secondOperation = "02020202-0202-0202-0202-020202020202" as OperationId;
  for (const id of [operationId, secondOperation]) await disposedState.put({
    operationId: id, authority, kind: "message.append", payload: {}, offlineSafe: true,
  });
  const disposedClient = new HarnessClient<number>({ connect: async () => ({
    send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {
      yield { authority, generation: "one", fromRevision: 0n, throughRevision: 2n, live: true,
        events: [operationId, secondOperation].map((id, index) => ({
          authority, revision: BigInt(index + 1), operationId: id, event: 1,
        })) };
    },
  }) }, disposedState);
  const disposedProjection = new ProjectionStore(disposedClient, 0, (value, event) => value + event.event, {
    authority, identity: "disposed-v1", encode: value => value,
    decode: value => { if (typeof value !== "number") throw new Error("invalid disposed checkpoint"); return value; },
  });
  disposedProjection.start();
  const disposing = disposedClient.run();
  await commitEntered.promise;
  disposedClient.dispose(); releaseCommit.resolve();
  await disposing; await commitSettled.promise;
  const recoveredDisposal = new IndexedDbClientStore({ databaseName: disposedName });
  const disposedCut = (await recoveredDisposal.loadRecovery())[0]!;
  assert(disposedCut.cursor.revision === 1n && disposedCut.checkpoint?.state === 1 &&
    disposedProjection.getSnapshot() === 0 && (await recoveredDisposal.load())[0]?.operationId === secondOperation,
    "disposal committed an unprepared batch suffix or lost the original retry");
  disposedProjection.dispose();
  demand.dispose(); baselineCache.clear(); scheduler.dispose(); harness.free();
  assert(scheduler.residency.requests === 0 && demand.residency.bytes === 0, "disposed residency");
  result.dataset.status = "passed";
  result.textContent = JSON.stringify({ coldReads: 1, coldBytes, baselineReads, baselineBytes, warmReads: 100000, baselineMs, warmMs, warmTransferredBytes: 0, warmReceiptAllocations: 0, demandResidentBytes: boundedBytes, retainedEntries: 2, cancelledReaderAttachments: retainedAttachments, cancelledConnectionCloses: closed, recoveredProjectionRevision: "1", recoveredBranch: branch, recoveredOutcome: "Unknown", documentRestarts: 1, staleTabFenced: true, abaFenced: true, fixedCut: 70, nextEnqueueCut: 71, listenerDeliveries: firstCalls });
  }
} catch (error) {
  result.dataset.status = "failed";
  result.textContent = error instanceof Error ? error.stack ?? error.message : String(error);
  throw error;
}
}
