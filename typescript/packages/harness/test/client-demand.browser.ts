import { DemandLoader, RequestScheduler, Harness, HarnessClient, ProjectionStore, ClientViews,
  PageWindow, IndexedDbClientStore, HydrationCache, ScheduledTransport, ClientCheckpointConflict, ReplayError,
  type FileRef, type OperationId, type ReplayCursor, type ClientDomain, type ClientObservation,
  type ClientHypothesis, type ClientView, type Delivery } from "@acyclic-labs/harness";

const result = document.querySelector<HTMLElement>("#result")!;
const assert = (value: unknown, message: string): void => { if (!value) throw new Error(message); };
const referenceTrace: string[] = JSON.parse(sessionStorage.getItem("harness-client-reference-trace") ?? "[]");
type GoldenCut = Readonly<{ generation: string; revision: bigint; checkpointVersion: bigint; identity: string; state: number }>;
// Independent arithmetic/CAS specification. The allocator survives deletion;
// old owner epochs have no publication or acknowledgement authority.
// Persistence is exercised only by the production IDB adapter below.
class GoldenBrowserRecovery {
  allocator = 0n;
  cut: GoldenCut | null = null;
  readonly outbox = new Set<string>();
  visible = 0;
  notifications = 0;
  owner = 0;
  disposed = false;
  constructor(readonly identity: string) {}
  commit(generation: string, revision: bigint, state: number, expected: GoldenCut | null, operation?: string): boolean {
    if (expected === null ? this.cut !== null : this.cut === null || expected.generation !== this.cut.generation ||
        expected.revision !== this.cut.revision || expected.checkpointVersion !== this.cut.checkpointVersion) return false;
    this.cut = { generation, revision, checkpointVersion: ++this.allocator, identity: this.identity, state };
    if (operation !== undefined) this.outbox.delete(operation);
    return true;
  }
  restore(): void {
    if (this.cut === null || this.cut.identity !== this.identity) throw new Error("reference checkpoint identity");
    if (this.visible !== this.cut.state) { this.visible = this.cut.state; this.notifications++; }
  }
  delivery(owner: number, generation: string, fromRevision: bigint, delta: number, operation: string): boolean {
    if (this.disposed || owner !== this.owner || this.cut === null || this.cut.generation !== generation ||
        this.cut.revision !== fromRevision) return false;
    const accepted = this.commit(generation, fromRevision + 1n, this.cut.state + delta, this.cut, operation);
    if (accepted) this.restore();
    return accepted;
  }
  fingerprint(): string {
    return JSON.stringify({ identity: this.identity, allocator: this.allocator, cut: this.cut,
      outbox: [...this.outbox].sort(), visible: this.visible, notifications: this.notifications,
      owner: this.owner, disposed: this.disposed }, (_, value) => typeof value === "bigint" ? value.toString() : value);
  }
}

// Fault injection edits real IDB data, preserving every checkpoint/CAS identity.
// It does NOT turn persisted bytes into authenticated facts: recovery below must
// still pass the existing immutable-pin and hypothesis-provenance validators.
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
  referenceTrace.push("enqueue original operation; trusted revision=1 FileRef receipt; actual IDB commit; lose reply; do not publish");
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
  // Two different trust boundaries, not repeated rows: altered authoritative
  // FileRef bytes versus altered prediction bytes under the same branch identity.
  for (const field of ["fact", "hypothesis"] as const) {
    referenceTrace.push(`persist valid ${field} checkpoint; change descriptor byte_length only; keep authority/cursor/version/checkpoint/branch identities`);
    const tamperName = `${databaseName}-tamper-${field}`;
    const tamper = new IndexedDbClientStore({ databaseName: tamperName });
    await tamper.put({ operationId, authority, kind: "message.append", payload: { content: selected }, offlineSafe: true });
    await tamper.commit(authority, { generation: "pinned-1", revision: 1n }, undefined,
      { identity: recovery.identity, state: { ...initial, content: selected } }, null, null);
    await changeCheckpointBytes(tamperName, field);
  }
    // Commit first, then destroy the whole document/JS heap and its IDB handles.
    // The second navigation must recover before its first network connection.
    projection.dispose(); client.dispose(); original.dispose();
    demand.dispose(); baselineCache.clear(); scheduler.dispose(); harness.free();
    sessionStorage.setItem(restartKey, databaseName);
    sessionStorage.setItem("harness-client-reference-trace", JSON.stringify(referenceTrace));
    location.reload();
  } else {
  referenceTrace.push("destroy original document/heap/handles; recover committed original cut before network");
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
  sessionStorage.removeItem("harness-client-reference-trace");

  // These corrupt records were written BEFORE the document reload. No original
  // heap, trusted WeakSet entry, or live IDB handle survives the recovery attempt.
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

  // A producer may retain its old installed transport listener after close.
  // Stop/dispose the old client, restore a replacement from REAL IDB, then invoke
  // that retained listener. Its stale event must neither publish nor ACK the
  // replacement's still-pending original operation.
  referenceTrace.push("IDB commit revision=1/value=1; enqueue original operation; retain old installed listener; stop owner; fresh restore");
  const retainedName = `${databaseName}-retained-listener`;
  const retainedState = new IndexedDbClientStore({ databaseName: retainedName });
  await retainedState.commit(authority, { generation: "one", revision: 1n }, undefined,
    { identity: "retained-v1", state: 1 }, null, null);
  await retainedState.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const retainedGolden = new GoldenBrowserRecovery("retained-v1");
  retainedGolden.commit("one", 1n, 1, null); retainedGolden.outbox.add(operationId); retainedGolden.restore();
  const oldOwner = retainedGolden.owner;
  const oldWaiting = Promise.withResolvers<void>(), oldDelivery = Promise.withResolvers<Delivery<number>>();
  const oldListener = (event: Event): void => oldDelivery.resolve((event as CustomEvent<Delivery<number>>).detail);
  let oldClosed = 0, oldPublications = 0;
  const oldClient = new HarnessClient<number>({ connect: async () => {
    addEventListener("client-reference-old-delivery", oldListener);
    return { send: async () => {}, close() { oldClosed++; removeEventListener("client-reference-old-delivery", oldListener); },
      async *[Symbol.asyncIterator]() { oldWaiting.resolve(); yield await oldDelivery.promise; } };
  } }, retainedState);
  const retainedRecovery = { authority, identity: "retained-v1", encode: (value: number) => value,
    decode: (value: unknown, cursor: ReplayCursor): number => {
      if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0 ||
          cursor.generation !== "one" || cursor.revision < 1n) throw new Error("invalid arithmetic checkpoint");
      return value;
    } };
  const oldProjection = new ProjectionStore(oldClient, 0, (value, event) => value + event.event, retainedRecovery);
  oldProjection.subscribe(() => { oldPublications++; });
  oldProjection.start();
  const oldRun = oldClient.run();
  await oldWaiting.promise;
  assert(oldProjection.getSnapshot() === 1 && oldPublications === 1, "old client failed to restore exact cut");
  oldClient.dispose();
  retainedGolden.owner++;
  const replacementReady = Promise.withResolvers<void>(), replacementDelivery = Promise.withResolvers<Delivery<number>>();
  const replacementStop = new AbortController();
  let replacementProjection!: ProjectionStore<number, number>, replacementPublications = 0;
  const replacementState = new IndexedDbClientStore({ databaseName: retainedName });
  const replacement = new HarnessClient<number>({ connect: async cursors => {
    assert(cursors.get("conversation:demand-browser")?.revision === 1n && replacementProjection.getSnapshot() === 1,
      "replacement connected before exact arithmetic restore");
    return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {
      replacementReady.resolve(); yield await replacementDelivery.promise; replacementStop.abort();
    } };
  } }, replacementState);
  replacementProjection = new ProjectionStore(replacement, 0, (value, event) => value + event.event, retainedRecovery);
  replacementProjection.subscribe(() => { replacementPublications++; });
  replacementProjection.start();
  const replacementRun = replacement.run(replacementStop.signal);
  await replacementReady.promise;
  referenceTrace.push("invoke retained old listener after replacement restore: revision=2/delta=1000/original operation");
  assert(!retainedGolden.delivery(oldOwner, "one", 1n, 1000, operationId), "reference old owner accepted delivery");
  oldListener(new CustomEvent("client-reference-old-delivery", { detail: {
    authority, generation: "one", fromRevision: 1n, throughRevision: 2n, live: true,
    events: [{ authority, revision: 2n, operationId, event: 1000 }],
  } }));
  await oldRun;
  const afterStale = (await replacementState.loadRecovery())[0]!;
  assert(oldClosed === 1 && oldProjection.getSnapshot() === retainedGolden.visible && oldPublications === 1 &&
    replacementProjection.getSnapshot() === retainedGolden.visible && replacementPublications === retainedGolden.notifications &&
    afterStale.cursor.revision === retainedGolden.cut?.revision &&
    afterStale.checkpointVersion === retainedGolden.cut?.checkpointVersion && afterStale.checkpoint?.state === retainedGolden.cut?.state &&
    (await replacementState.load())[0]?.operationId === [...retainedGolden.outbox][0],
    `retained old listener changed replacement cut, notification or original ACK: ${retainedGolden.fingerprint()}`);
  referenceTrace.push("replacement original operation delivery: revision=2/delta=2; commit exact 1+2 cut; publish once");
  assert(retainedGolden.delivery(retainedGolden.owner, "one", 1n, 2, operationId), "reference live owner rejected delivery");
  replacementDelivery.resolve({ authority, generation: "one", fromRevision: 1n, throughRevision: 2n, live: true,
    events: [{ authority, revision: 2n, operationId, event: 2 }] });
  await replacementRun;
  const afterReplacement = (await replacementState.loadRecovery())[0]!;
  assert(replacementProjection.getSnapshot() === retainedGolden.visible && replacementPublications === retainedGolden.notifications &&
    afterReplacement.cursor.revision === retainedGolden.cut?.revision && afterReplacement.checkpoint?.state === retainedGolden.cut?.state &&
    (await replacementState.load()).length === retainedGolden.outbox.size,
    `replacement did not publish the exact arithmetic cut: ${retainedGolden.fingerprint()}`);
  oldProjection.dispose(); replacement.dispose(); replacementProjection.dispose();

  // Unique delivery classes: a delayed old cut and a future gap. Neither is
  // silently buffered/applied or permitted to advance a recoverable exact cut.
  for (const fromRevision of [0n, 2n]) {
    referenceTrace.push(`restore revision=1/value=1; deliver ${fromRevision === 0n ? "delayed stale" : "future gap"} from=${fromRevision}; reject without publication/CAS/ACK`);
    const deliveryState = new IndexedDbClientStore({ databaseName: `${databaseName}-delivery-${fromRevision}` });
    await deliveryState.commit(authority, { generation: "one", revision: 1n }, undefined,
      { identity: retainedRecovery.identity, state: 1 }, null, null);
    await deliveryState.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
    let notifications = 0;
    const deliveryClient = new HarnessClient<number>({ connect: async () => ({
      send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {
        yield { authority, generation: "one", fromRevision, throughRevision: fromRevision + 1n, live: true,
          events: [{ authority, revision: fromRevision + 1n, operationId, event: 1000 }] };
      },
    }) }, deliveryState);
    const deliveryProjection = new ProjectionStore(deliveryClient, 0, (value, event) => value + event.event, retainedRecovery);
    deliveryProjection.subscribe(() => { notifications++; }); deliveryProjection.start();
    const failure = await deliveryClient.run().catch(error => error);
    const exact = (await deliveryState.loadRecovery())[0]!;
    assert(failure instanceof ReplayError && deliveryProjection.getSnapshot() === 1 && notifications === 1 &&
      exact.cursor.revision === 1n && exact.checkpointVersion === 1n && exact.checkpoint?.state === 1 &&
      (await deliveryState.load())[0]?.operationId === operationId, "out-of-order delivery changed the exact cut");
    deliveryClient.dispose(); deliveryProjection.dispose();
  }

  // Public production rebase owns an explicit authoritative snapshot gap. The
  // in-process adapter recognizes exact trusted cuts, not arbitrary saved data.
  referenceTrace.push("recover revision=1/value=1; reject foreign checkpoint identity; authoritative rebase revision=7/value=40; retain original pending operation");
  const rebaseState = new IndexedDbClientStore({ databaseName: `${databaseName}-reference-rebase` });
  const rebaseGolden = new GoldenBrowserRecovery("reference-rebase-v1");
  rebaseGolden.commit("one", 1n, 1, null); rebaseGolden.outbox.add(operationId);
  await rebaseState.commit(authority, { generation: "one", revision: 1n }, undefined,
    { identity: rebaseGolden.identity, state: 1 }, null, null);
  await rebaseState.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  let rebaseNotifications = 0, rebaseConnections = 0;
  const rebaseClient = new HarnessClient<number>({ connect: async () => {
    rebaseConnections++; throw new Error("rebase performs no network");
  } }, rebaseState);
  const rebaseRecovery = { authority, identity: rebaseGolden.identity, encode: (value: number) => value,
    decode: (value: unknown, cursor: ReplayCursor): number => {
      if (cursor.generation !== "one" || !((cursor.revision === 1n && value === 1) ||
          (cursor.revision === 7n && value === 40))) throw new Error("unrecognized authoritative arithmetic cut");
      return value as number;
    } };
  const rebaseProjection = new ProjectionStore(rebaseClient, 0, (value, event) => value + event.event, rebaseRecovery);
  rebaseProjection.subscribe(() => { rebaseNotifications++; }); rebaseProjection.start();
  const foreign = await rebaseClient.rebase(authority, { generation: "one", revision: 7n },
    { identity: "foreign-reducer", state: 40 }).catch(error => error);
  assert(foreign instanceof Error && rebaseProjection.getSnapshot() === 0 && rebaseNotifications === 0 &&
    (await rebaseState.loadRecovery())[0]?.cursor.revision === 1n && (await rebaseState.load())[0]?.operationId === operationId,
    "foreign snapshot identity changed a recoverable cut");
  rebaseGolden.commit("one", 7n, 40, rebaseGolden.cut); rebaseGolden.restore();
  await rebaseClient.rebase(authority, { generation: "one", revision: 7n }, { identity: rebaseGolden.identity, state: 40 });
  const rebasedCut = (await rebaseState.loadRecovery())[0]!;
  assert(rebaseProjection.getSnapshot() === rebaseGolden.visible && rebaseNotifications === rebaseGolden.notifications &&
    rebaseConnections === 0 && rebasedCut.cursor.revision === rebaseGolden.cut?.revision &&
    rebasedCut.checkpointVersion === rebaseGolden.cut?.checkpointVersion && rebasedCut.checkpoint?.state === rebaseGolden.cut?.state &&
    (await rebaseState.load())[0]?.operationId === [...rebaseGolden.outbox][0],
    `snapshot gap rebase changed projection identity, publication or original operation: ${rebaseGolden.fingerprint()}`);
  rebaseClient.dispose(); rebaseProjection.dispose();
  const rebaseRestartStop = new AbortController();
  let rebaseRestartProjection!: ProjectionStore<number, number>;
  const rebaseRestart = new HarnessClient<number>({ connect: async cursors => {
    assert(cursors.get("conversation:demand-browser")?.revision === 7n && rebaseRestartProjection.getSnapshot() === 40,
      "fresh client did not restore the exact authoritative snapshot gap");
    rebaseRestartStop.abort(); return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {} };
  } }, new IndexedDbClientStore({ databaseName: `${databaseName}-reference-rebase` }));
  rebaseRestartProjection = new ProjectionStore(rebaseRestart, 0, (value, event) => value + event.event, rebaseRecovery);
  rebaseRestartProjection.start(); await rebaseRestart.run(rebaseRestartStop.signal);
  rebaseRestart.dispose(); rebaseRestartProjection.dispose();

  // Actual WASM-backed browser selections: arithmetic expectations do not come
  // from a returned receipt, reducer, selected snapshot, or checkpoint.
  type ArithmeticEvidence = ClientObservation<number>;
  const arithmeticTrusted = new WeakSet<object>();
  const arithmeticDomain: ClientDomain<number, { original: number; delta: number }, ArithmeticEvidence> = {
    identity: "19",
    validate: (fact, prediction, assumption) => {
      if (fact.value !== assumption.original || prediction !== assumption.original + assumption.delta) throw new Error("invalid increment");
      return { bytes: 128, work: 1 };
    },
    observe: evidence => { if (!arithmeticTrusted.has(evidence)) throw new Error("untrusted arithmetic evidence"); return evidence; },
    corresponds: (prediction, canonical) => ({ matches: prediction === canonical, work: 1 }),
  };
  const arithmetic = new ClientViews(arithmeticDomain, "73", 0n, { ...limits, records: 2, branches: 2 });
  const arithmeticFact = (key: string, revision: number, value: number, operation: ArithmeticEvidence["operation"]): ArithmeticEvidence => {
    const evidence = { fact: { key, basis: `numbers/${key}/${revision}/${value}`, value, bytes: 128 }, operation, work: 1 };
    arithmeticTrusted.add(evidence); return evidence;
  };
  arithmetic.observe("a", arithmeticFact("a", 0, 10, null)); arithmetic.observe("b", arithmeticFact("b", 0, 20, null));
  const arithmeticRoot = arithmetic.begin({ key: "a", basis: "numbers/a/0/10", operation: "9", predicted: 11,
    assumption: { original: 10, delta: 1 }, dependencies: [], expires: 5n });
  const arithmeticChild = arithmetic.begin({ key: "b", basis: "numbers/b/0/20", operation: "10", predicted: 22,
    assumption: { original: 20, delta: 2 }, dependencies: [{ branch: arithmeticRoot, requirement: "Prediction" }], expires: 5n });
  const rootSelection = arithmetic.select("a", [arithmeticRoot]), childSelection = arithmetic.select("b", [arithmeticChild]);
  const rootBefore = rootSelection.getSnapshot(), childBefore = childSelection.getSnapshot();
  let rootNotifications = 0, childNotifications = 0;
  rootSelection.subscribe(() => {
    rootNotifications++;
    assert(childSelection.getSnapshot().hypotheses[0]?.prediction === "Invalidated", "related selections notified before staging full state");
  });
  childSelection.subscribe(() => { childNotifications++; });
  const replaced = arithmeticFact("a", 1, 12, ["9", "Completed"]);
  arithmetic.observe("a", replaced);
  const expectedRoot: ClientView<number> = { value: 12, basis: "numbers/a/1/12", branch: null, adapter: null, prediction: null, outcome: null,
    hypotheses: [{ branch: "73:1", prediction: "Replaced", outcome: "Completed" }] };
  const expectedChild: ClientView<number> = { value: 20, basis: "numbers/b/0/20", branch: null, adapter: null, prediction: null, outcome: null,
    hypotheses: [{ branch: "73:2", prediction: "Invalidated", outcome: "Unknown" }] };
  const fullView = (view: ClientView<number>): string => JSON.stringify([view.value, view.basis, view.branch,
    view.adapter, view.prediction, view.outcome, view.hypotheses.map(status => [status.branch, status.prediction, status.outcome])]);
  referenceTrace.push("arithmetic predictions 10+1 and 20+2/dependency; original completion=12 replaces root; invalidates child");
  assert(fullView(rootSelection.getSnapshot()) === fullView(expectedRoot) &&
    fullView(childSelection.getSnapshot()) === fullView(expectedChild) &&
    rootNotifications === 1 && childNotifications === 1 && rootBefore.value === 11 && childBefore.value === 22,
    "arithmetic refinement lost full view, identity, terminal outcome or immutable snapshot");
  const stableRoot = rootSelection.getSnapshot(), stableChild = childSelection.getSnapshot();
  arithmetic.observe("a", replaced);
  assert(rootSelection.getSnapshot() === stableRoot && childSelection.getSnapshot() === stableChild &&
    rootNotifications === 1 && childNotifications === 1, "duplicate evidence republished arithmetic state");
  rootSelection.dispose(); childSelection.dispose(); arithmetic.dispose();

  // A separate browser realm restores the old cut, then loses a same-cut CAS
  // against this tab. A stale authoritative delivery must not ACK its operation.
  const raceName = `${databaseName}-race`;
  const race = new IndexedDbClientStore({ databaseName: raceName });
  const raceCursor = { generation: "one", revision: 1n };
  const initialVersion = await race.commit(authority, raceCursor, undefined,
    { identity: "browser-race-v1", state: 1 }, null, null);
  await race.put({ operationId, authority, kind: "message.append", payload: {}, offlineSafe: true });
  const raceGolden = new GoldenBrowserRecovery("browser-race-v1");
  raceGolden.commit("one", 1n, 1, null); raceGolden.outbox.add(operationId);
  const oldCut = raceGolden.cut;
  referenceTrace.push("cross-realm restore old cut revision=1/version=1/value=1");
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
    raceGolden.commit("one", 1n, 10, raceGolden.cut);
    referenceTrace.push("same-cursor replacement revision=1/version=2/value=10; deliver old original operation");
    const replacementVersion = await race.commit(authority, raceCursor, undefined,
      { identity: "browser-race-v1", state: 10 }, raceCursor, initialVersion);
    assert(replacementVersion === raceGolden.allocator && initialVersion === oldCut?.checkpointVersion, "same-cut checkpoint allocator diverged from reference");
    actor.contentWindow!.postMessage("deliver", location.origin);
    const stale = await staleResult.promise;
    assert(stale.conflict && stale.published === 1, "stale tab published its delivery");
    assert((await race.loadRecovery())[0]?.checkpoint?.state === raceGolden.cut?.state &&
      (await race.load())[0]?.operationId === [...raceGolden.outbox][0], "stale delivery changed checkpoint or ACK");
    await race.deleteCursor(authority);
    raceGolden.cut = null;
    raceGolden.commit("one", 1n, 20, null);
    referenceTrace.push("delete/recreate same cursor version=3/value=20; stale version=1 CAS must fail without ACK");
    assert(!raceGolden.commit("one", 2n, 2, oldCut, operationId), "reference accepted ABA stale cut");
    const reopenedRace = new IndexedDbClientStore({ databaseName: raceName });
    const recreated = await reopenedRace.commit(authority, raceCursor, undefined,
      { identity: "browser-race-v1", state: 20 }, null, null);
    const aba = await race.commit(authority, { ...raceCursor, revision: 2n }, operationId,
      { identity: "browser-race-v1", state: 2 }, raceCursor, initialVersion).catch(error => error);
    assert(recreated === raceGolden.allocator && aba instanceof ClientCheckpointConflict &&
      (await race.load())[0]?.operationId === [...raceGolden.outbox][0] &&
      (await race.loadRecovery())[0]?.checkpoint?.state === raceGolden.cut?.state,
      `delete/recreate revived stale tab: ${raceGolden.fingerprint()}`);
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
  const disposedGolden = new GoldenBrowserRecovery("disposed-v1");
  disposedGolden.outbox.add(operationId); disposedGolden.outbox.add(secondOperation);
  referenceTrace.push("prepare first event; pause real IDB commit; dispose owner; settle first committed event; never prepare batch suffix");
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
  disposedGolden.disposed = true;
  disposedGolden.commit("one", 1n, 1, null, operationId);
  assert(!disposedGolden.delivery(disposedGolden.owner, "one", 1n, 1, secondOperation), "reference disposed owner advanced batch suffix");
  disposedClient.dispose(); releaseCommit.resolve();
  await disposing; await commitSettled.promise;
  const recoveredDisposal = new IndexedDbClientStore({ databaseName: disposedName });
  const disposedCut = (await recoveredDisposal.loadRecovery())[0]!;
  assert(disposedCut.cursor.revision === disposedGolden.cut?.revision && disposedCut.checkpoint?.state === disposedGolden.cut?.state &&
    disposedCut.checkpointVersion === disposedGolden.allocator && disposedProjection.getSnapshot() === disposedGolden.visible &&
    (await recoveredDisposal.load())[0]?.operationId === [...disposedGolden.outbox][0],
    `disposal committed an unprepared batch suffix or lost the original retry: ${disposedGolden.fingerprint()}`);
  disposedProjection.dispose();
  const disposalRestartStop = new AbortController();
  let disposalRestartProjection!: ProjectionStore<number, number>, disposalRestartNotifications = 0;
  const disposalRestart = new HarnessClient<number>({ connect: async cursors => {
    assert(cursors.get("conversation:demand-browser")?.revision === disposedGolden.cut?.revision &&
      disposalRestartProjection.getSnapshot() === disposedGolden.cut?.state && disposalRestartNotifications === 1,
      "disposed commit did not freshly restore before network");
    disposalRestartStop.abort(); return { send: async () => {}, close() {}, async *[Symbol.asyncIterator]() {} };
  } }, recoveredDisposal);
  disposalRestartProjection = new ProjectionStore(disposalRestart, 0, (value, event) => value + event.event, {
    authority, identity: disposedGolden.identity, encode: value => value,
    decode: (value, cursor) => {
      if (value !== 1 || cursor.generation !== "one" || cursor.revision !== 1n) throw new Error("invalid settled arithmetic cut");
      return 1;
    },
  });
  disposalRestartProjection.subscribe(() => { disposalRestartNotifications++; }); disposalRestartProjection.start();
  await disposalRestart.run(disposalRestartStop.signal);
  disposalRestart.dispose(); disposalRestartProjection.dispose();
  demand.dispose(); baselineCache.clear(); scheduler.dispose(); harness.free();
  assert(scheduler.residency.requests === 0 && demand.residency.bytes === 0, "disposed residency");
  result.dataset.status = "passed";
  result.textContent = JSON.stringify({ coldReads: 1, coldBytes, baselineReads, baselineBytes, warmReads: 100000, baselineMs, warmMs, warmTransferredBytes: 0, warmReceiptAllocations: 0, demandResidentBytes: boundedBytes, retainedEntries: 2, cancelledReaderAttachments: retainedAttachments, cancelledConnectionCloses: closed, recoveredProjectionRevision: "1", recoveredBranch: branch, recoveredOutcome: "Unknown", documentRestarts: 1, lostCommitReplyRecovered: true, changedBytesSameIdentityReloadCases: 2, retainedOldListenerFenced: true, outOfOrderExactCutCases: 2, fullArithmeticViews: 2, arithmeticNotifications: [rootNotifications, childNotifications], staleTabFenced: true, abaFenced: true, fixedCut: 70, nextEnqueueCut: 71, listenerDeliveries: firstCalls, referenceTrace });
  }
} catch (error) {
  sessionStorage.setItem("harness-client-reference-counterexample", JSON.stringify({ actions: referenceTrace,
    failure: error instanceof Error ? error.stack ?? error.message : String(error) }));
  result.dataset.status = "failed";
  result.textContent = error instanceof Error ? error.stack ?? error.message : String(error);
  throw error;
}
}
