import { BrowserStreamProvider, MemoryStreamProvider } from "/stream/dist/memory.js";
import { wireAppendRequest } from "/stream/dist/contract.js";
import { projectMemoryResponse } from "/stream/generated/wasm/acyclic_stream_wasm.js";

const query = new URL(location.href).searchParams;
const name = query.get("database") ?? `stream-${crypto.randomUUID()}`;
const channel = new BroadcastChannel(name);
const stream = new BrowserStreamProvider(name);
const bytes = value => new TextEncoder().encode(value);
const key = bytes;
function assert(value, message) { if (!value) throw new Error(message); }
const equal = (a, b) => JSON.stringify(a, (_, value) => typeof value === "bigint" ? `${value}n` : value)
  === JSON.stringify(b, (_, value) => typeof value === "bigint" ? `${value}n` : value);
if (query.has("actor")) {
  channel.onmessage = async ({ data }) => {
    if (data.id === undefined) return;
    if (data.action === "reload") { location.reload(); return; }
    try {
      const value = await stream.append("events", [Uint8Array.of(data.value)], { idempotencyKey: key(data.key), ifTail: data.tail });
      channel.postMessage({ id: data.id, value });
    } catch (error) { channel.postMessage({ id: data.id, error: String(error.stack ?? error) }); }
  };
  channel.postMessage({ ready: true });
} else {
  const result = document.querySelector("#result");
  let ready;
  let readyError;
  let sequence = 0;
  const pending = new Map();
  channel.onmessage = ({ data }) => {
    if (data.ready) ready?.();
    else if (data.startupError) readyError?.(new Error(data.startupError));
    else {
      const handler = pending.get(data.id);
      pending.delete(data.id);
      if (data.error) handler?.reject(new Error(data.error)); else handler?.resolve(data.value);
    }
  };
  const waitReady = () => new Promise((resolve, reject) => { ready = resolve; readyError = reject; });
  const call = args => new Promise((resolve, reject) => {
    const id = ++sequence;
    pending.set(id, { resolve, reject });
    channel.postMessage({ id, ...args });
  });
  const first = await stream.append("events", [Uint8Array.of(1)], { idempotencyKey: key("first") });
  assert(first.ok && equal(Array.from(first.commitId), [
    103, 88, 88, 43, 235, 47, 0, 131, 35, 251, 167, 33, 80, 169, 30, 73,
    72, 104, 186, 58, 197, 155, 218, 185, 49, 156, 25, 42, 172, 54, 132, 40,
  ]), "durable browser outcome diverged from native journal fixture");
  const memory = new MemoryStreamProvider();
  assert(equal(first, await memory.append("events", [Uint8Array.of(1)], { idempotencyKey: key("first") })), "durable provider diverged from canonical memory outcome");
  const url = new URL(`./browser-stream.html?actor=tab&database=${name}`, location.href);
  const started = waitReady();
  const tab = window.open(url);
  let worker;
  try {
    await started;
    result.dataset.waiting = "concurrent tabs";
    const [left, right] = await Promise.all([
      stream.append("events", [Uint8Array.of(2)], { idempotencyKey: key("race-left"), ifTail: 1n }),
      call({ value: 3, key: "race-right", tail: 1n }),
    ]);
    assert(Number(left.ok) + Number(right.ok) === 1, "concurrent stale owners both published");
    assert(await stream.tail("events") === 2n, "atomic CAS did not preserve one winner");
    const raceReplay = await call({ value: 3, key: "race-right", tail: 1n });
    assert(equal(raceReplay, right), "terminal conflict was reclassified after retry");
    const reloaded = waitReady();
    channel.postMessage({ id: ++sequence, action: "reload" });
    await reloaded;
    assert(equal(await call({ value: 3, key: "race-right", tail: 1n }), right), "reload lost exact outcome");

    // Interrupt between journal frame and metadata publication using an actual
    // IndexedDB abort. The same handle must discard its unpublished Rust state.
    const cancel = new AbortController();
    const cursor = stream.follow("events", { from: 0n, signal: cancel.signal })[Symbol.asyncIterator]();
    assert((await cursor.next()).value.sequence === 0n, "follow lost its initial history");
    result.dataset.waiting = "aborted publication";
    const put = IDBObjectStore.prototype.put;
    let aborted = false;
    IDBObjectStore.prototype.put = function(value, key) {
      if (!aborted && this.name === "journal" && key === "metadata") {
        aborted = true;
        this.transaction.abort();
      }
      return put.call(this, value, key);
    };
    try {
      let rejected = false;
      try { await stream.append("events", [Uint8Array.of(4)], { idempotencyKey: key("aborted") }); }
      catch (error) { rejected = error.code === "unavailable"; }
      assert(aborted && rejected, "publication abort did not fail closed");
    } finally { IDBObjectStore.prototype.put = put; }
    assert(await stream.tail("events") === 2n, "failed publication leaked disposable state");
    assert(await stream.inspectIdempotency(key("aborted")) === undefined, "failed publication retained a phantom receipt");

    result.dataset.waiting = "worker publication and durable follow";
    const workerUrl = new URL("./browser-stream-worker.mjs", location.href);
    workerUrl.searchParams.set("database", name);
    worker = new Worker(workerUrl, { type: "module" });
    const workerPending = new Map();
    await new Promise((resolve, reject) => {
      worker.onerror = error => reject(new Error(error.message));
      worker.onmessage = ({ data }) => {
        if (data.ready) resolve();
        else {
          const handler = workerPending.get(data.id);
          workerPending.delete(data.id);
          if (data.error) handler?.reject(new Error(data.error)); else handler?.resolve(data.value);
        }
      };
    });
    assert((await cursor.next()).value.sequence === 1n, "cache recovery reset the consumed follow cursor");
    const next = cursor.next();
    const workerResult = await new Promise((resolve, reject) => {
      const id = ++sequence;
      workerPending.set(id, { resolve, reject });
      worker.postMessage({ id, operation: "append", input: wireAppendRequest("events", [Uint8Array.of(5)], { idempotencyKey: key("worker") }) });
    });
    worker.terminate();
    const workerOutcome = projectMemoryResponse("append", workerResult);
    assert(workerOutcome.ok && (await next).value.value[0] === 5, "follow lost worker publication after worker termination");
    const waiting = cursor.next();
    cancel.abort();
    assert((await waiting).done, "cancelled durable follow did not end");
    const reopened = new BrowserStreamProvider(name);
    assert(equal(await reopened.append("events", [Uint8Array.of(5)], { idempotencyKey: key("worker") }), workerOutcome), "worker restart lost publication receipt");
    const records = [];
    for await (const record of reopened.read("events", { from: 0n, limit: 4 })) records.push(record);
    assert(records.length === 3 && records[2].value[0] === 5, "bounded recovery lost journal facts");
    const envelope = await reopened.readCommit(workerOutcome.commitId);
    assert(envelope.mutations.length === 1, "restart lost immutable envelope");

    result.dataset.waiting = "incremental backlog follow";
    for (let index = 0n; index < 128n; index++) {
      await stream.append("backlog", [Uint8Array.of(Number(index))], { ifTail: index });
    }
    const backlog = stream.follow("backlog", { from: 0n })[Symbol.asyncIterator]();
    try {
      for (let index = 0n; index <= 128n; index++) {
        if (index === 64n) await reopened.append("backlog", [Uint8Array.of(128)], { ifTail: 128n });
        const record = (await backlog.next()).value;
        assert(record.sequence === index && record.value[0] === Number(index), "backlog follow skipped or repeated history across publication");
      }
    } finally { await backlog.return(); }

    const request = { conditions: [{ path: "deadline", ifAbsent: true }], mutations: [{ append: { path: "deadline", values: [Uint8Array.of(1)] } }] };
    let deadlineRejected = false;
    try { await reopened.commit(request, { idempotencyKey: key("deadline"), deadlineUnixMillis: BigInt(Date.now()) - 1n }); }
    catch (error) { deadlineRejected = error.code === "deadline_elapsed"; }
    assert(deadlineRejected, "provider clock did not fence expiry");
    const committed = await reopened.commit(request, { idempotencyKey: key("deadline"), deadlineUnixMillis: BigInt(Date.now()) + 60_000n });
    assert(committed.ok, "valid coordinated commit failed");
    assert(equal(await reopened.commit(request, { idempotencyKey: key("deadline"), deadlineUnixMillis: 0n }), committed), "exact replay did not precede deadline fencing");
    const capped = new BrowserStreamProvider(`${name}-capacity`, { maximumCommands: 1 });
    const retained = await capped.append("one", [Uint8Array.of(1)], { idempotencyKey: key("one") });
    let capacityRejected = false;
    try { await capped.append("two", [Uint8Array.of(2)]); } catch (error) { capacityRejected = error.code === "capacity"; }
    assert(capacityRejected, "finite journal capacity admitted more history");
    assert(equal(await capped.append("one", [Uint8Array.of(1)], { idempotencyKey: key("one") }), retained), "capacity released an admitted identity");
    const conflict = await capped.append("one", [Uint8Array.of(9)], { ifTail: 0n });
    assert(!conflict.ok && conflict.actualTail === 1n, "journal capacity changed a non-publishing tail conflict");
    let fullDeadlineRejected = false;
    try { await capped.commit(request, { idempotencyKey: key("full-deadline"), deadlineUnixMillis: 0n }); }
    catch (error) { fullDeadlineRejected = error.code === "deadline_elapsed"; }
    assert(fullDeadlineRejected, "journal capacity changed canonical deadline precedence");
    result.dataset.status = "passed";
    result.dataset.waiting = "";
    result.textContent = "Rust correspondence, tabs/reload/worker durability, strict transaction abort, follow cancellation, deadline fencing, finite capacity and exact replay passed.";
  } finally { tab?.close(); worker?.terminate(); channel.close(); }
}
