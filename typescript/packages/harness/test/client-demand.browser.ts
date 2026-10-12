import { DemandLoader, RequestScheduler, Harness, HarnessClient, ProjectionStore, ClientViews,
  PageWindow, IndexedDbClientStore, HydrationCache, ScheduledTransport, ClientCheckpointConflict,
  type FileRef, type OperationId, type ReplayCursor, type ClientDomain, type ClientObservation,
  type ClientHypothesis } from "@acyclic-labs/harness";

const result = document.querySelector<HTMLElement>("#result")!;
const assert = (value: unknown, message: string): void => { if (!value) throw new Error(message); };
async function changeCheckpointBytes(databaseName: string, field: "fact" | "hypothesis"): Promise<void> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const open = indexedDB.open(databaseName);
    open.onsuccess = () => resolve(open.result); open.onerror = () => reject(open.error);
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction("cursors", "readwrite");
      transaction.oncomplete = () => resolve();
      transaction.onerror = transaction.onabort = () => reject(transaction.error ?? new Error("tamper transaction aborted"));
      const store = transaction.objectStore("cursors"), read = store.get("conversation:demand-browser");
      read.onsuccess = () => {
        const record = read.result;
        const before = [record.authority, record.generation, record.revision, record.checkpointVersion,
          record.checkpoint.identity, record.checkpoint.state.hypothesis.branch, record.checkpoint.state.hypothesis.basis];
        const file = field === "fact" ? record.checkpoint.state.content : record.checkpoint.state.hypothesis.predicted.content;
        file.descriptor.byte_length += 1;
        assert(before.every((value, index) => value === [record.authority, record.generation, record.revision,
          record.checkpointVersion, record.checkpoint.identity, record.checkpoint.state.hypothesis.branch,
          record.checkpoint.state.hypothesis.basis][index]), "fault injection changed checkpoint identity");
        store.put(record);
      };
    });
  } finally { database.close(); }
}
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
  const beforeBaselineBytes = transferred, beforeBaselineReads = reads;
  for (const file of refs) await source.load(file, new AbortController().signal);
  const baselineBytes = transferred - beforeBaselineBytes;
  const baselineReads = reads - beforeBaselineReads;
  assert(coldBytes * 4 === baselineBytes && baselineReads === 4, "cold selective bound versus actual baseline");

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
  class LostReplyBrowserStore extends IndexedDbClientStore {
    override async commit(...args: Parameters<IndexedDbClientStore["commit"]>): Promise<bigint> {
      await super.commit(...args);
      stop.abort();
      throw new Error("injected reply loss after actual IDB commit");
    }
  }
  const lostReplyState = new LostReplyBrowserStore({ databaseName });
  const client = new HarnessClient<FileRef>({ connect: async () => ({ send: async () => {}, close() {},
    async *[Symbol.asyncIterator]() {
      yield { authority, generation: "pinned-1", fromRevision: 0n, throughRevision: 1n, live: true,
        events: [{ authority, revision: 1n, operationId, event: selected }] };
    } }) }, lostReplyState);
  const projection = new ProjectionStore(client, initial, reduce, recovery);
  let lostReplyPublications = 0;
  projection.subscribe(() => { lostReplyPublications++; });
  projection.start(); await client.run(stop.signal);
  assert(projection.getSnapshot().content === null && lostReplyPublications === 0 &&
    (await state.load()).length === 0 && (await state.loadRecovery())[0]?.cursor.revision === 1n,
    "lost commit reply published or lost its durable exact cut");
  for (const field of ["fact", "hypothesis"] as const) {
    const tamperName = `${databaseName}-tamper-${field}`;
    const tamper = new IndexedDbClientStore({ databaseName: tamperName });
    await tamper.put({ operationId, authority, kind: "message.append", payload: { content: selected }, offlineSafe: true });
    await tamper.commit(authority, { generation: "pinned-1", revision: 1n }, undefined,
      { identity: recovery.identity, state: { ...initial, content: selected } }, null, null);
    await changeCheckpointBytes(tamperName, field);
  }
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
  for (const field of ["fact", "hypothesis"] as const) {
    const corrupt = new IndexedDbClientStore({ databaseName: `${databaseName}-tamper-${field}` });
    let connected = 0, publications = 0;
    const corruptClient = new HarnessClient<FileRef>({ connect: async () => {
      connected++; throw new Error("corrupt checkpoint reached network");
    } }, corrupt);
    const corruptProjection = new ProjectionStore(corruptClient, initial, reduce, recovery);
    corruptProjection.subscribe(() => { publications++; });
    corruptProjection.start();
    const failure = await corruptClient.run().catch(error => error);
    const persisted = (await corrupt.loadRecovery())[0]!;
    assert(failure instanceof Error && connected === 0 && publications === 0 &&
      corruptProjection.getSnapshot() === initial && persisted.cursor.revision === 1n &&
      persisted.checkpointVersion === 1n && persisted.checkpoint?.identity === recovery.identity &&
      (await corrupt.load())[0]?.operationId === operationId,
      `same-identity ${field} byte tamper advanced recovery, publication or ACK`);
    corruptClient.dispose(); corruptProjection.dispose();
  }
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
    assert(replacementVersion > initialVersion, "same-cut checkpoint allocator diverged from reference");
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
      (await race.load())[0]?.operationId === operationId &&
      (await race.loadRecovery())[0]?.checkpoint?.state === 20,
      "delete/recreate revived stale tab");
  } finally {
    removeEventListener("message", receive); actor.remove();
  }

  demand.dispose(); baselineCache.clear(); scheduler.dispose(); harness.free();
  assert(scheduler.residency.requests === 0 && demand.residency.bytes === 0, "disposed residency");
  result.dataset.status = "passed";
  result.textContent = JSON.stringify({ coldReads: 1, coldBytes, baselineReads, baselineBytes, warmReads: 100000, baselineMs, warmMs, warmTransferredBytes: 0, warmReceiptAllocations: 0, demandResidentBytes: boundedBytes, retainedEntries: 2, cancelledConnectionCloses: closed, recoveredProjectionRevision: "1", recoveredBranch: branch, recoveredOutcome: "Unknown", documentRestarts: 1, lostCommitReplyRecovered: true, changedBytesSameIdentityReloadCases: 2 });
  }
} catch (error) {
  result.dataset.status = "failed";
  result.textContent = error instanceof Error ? error.stack ?? error.message : String(error);
  throw error;
}
}
