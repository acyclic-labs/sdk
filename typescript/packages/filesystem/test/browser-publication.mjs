// Real tabs and a module worker share one authority. The transport only stages
// test actions; all window transitions and publication decisions run in Rust.
import initialize, { openBrowserFs } from "../generated/wasm/acyclic_fs_wasm.js";
import { DEFAULT_OBJECT_CACHE_OPTIONS } from "../generated/defaults.js";

await initialize();
const query = new URL(globalThis.location.href).searchParams;
const actor = query.get("actor");
const databaseName = query.get("database") ?? `publication-${crypto.randomUUID()}`;
const channel = new BroadcastChannel(databaseName);
const equal = (a, b) => a.length === b.length && a.every((v, i) => v === b[i]);
function assert(value, message) { if (!value) throw new Error(message); }
const options = { databaseName, maximumObjectBytes: 64 * 1024 * 1024,
  objectAcceleration: query.get("profile") ?? "indexeddb", objectCache: DEFAULT_OBJECT_CACHE_OPTIONS };
const fs = await openBrowserFs(options);

if (actor !== null) {
  const workspace = await fs.openWorkspace("main");
  const windows = fs.operationWindows();
  let transaction;
  channel.onmessage = async ({ data }) => {
    if (data.actor !== actor || data.id === undefined) return;
    try {
      let value;
      switch (data.action) {
        case "begin": value = await windows.begin(workspace.id, data.parent, actor, data.now, data.expiry); break;
        case "stage":
          transaction = await workspace.beginTransaction(data.key);
          await transaction.write(data.path, Uint8Array.of(42));
          value = true; break;
        case "commit": value = await transaction.commit(query.has("drop-permit") ? undefined : data.lease); break;
        case "read": value = await workspace.read(data.path, 1n); break;
        case "reload": globalThis.location.reload(); return;
        case "publish-without-reply":
          value = await transaction.commit(data.lease);
          assert(value.status === "committed", "interrupted acknowledgement did not publish");
          channel.postMessage({ published: actor });
          return; // The caller loses the terminal reply after durable publication.
        default: throw new Error(`unknown action ${data.action}`);
      }
      channel.postMessage({ reply: data.id, value });
    } catch (error) { channel.postMessage({ reply: data.id, error: String(error.stack ?? error) }); }
  };
  channel.postMessage({ ready: actor });
} else {
  const { adaptWasmFs, adaptWasmOperationWindows } = await import("../dist/wasm-adapter.js");
  const result = document.querySelector("#result");
  const workspace = await fs.createWorkspace("main");
  const windows = fs.operationWindows();
  const pending = new Map();
  const events = new Map();
  let sequence = 0;
  channel.onmessage = ({ data }) => {
    if (data.startupError !== undefined) events.get(`error:${data.startupError}`)?.(new Error(data.error));
    if (data.reply !== undefined) {
      const handler = pending.get(data.reply);
      pending.delete(data.reply);
      if (data.error !== undefined) handler?.reject(new Error(data.error));
      else handler?.resolve(data.value);
    }
    for (const kind of ["ready", "published"]) {
      if (data[kind] !== undefined) events.get(`${kind}:${data[kind]}`)?.();
    }
  };
  function event(kind, who) {
    return new Promise((resolve, reject) => {
      events.set(`${kind}:${who}`, resolve);
      events.set(`error:${who}`, reject);
    });
  }
  function call(who, action, args = {}) {
    result.dataset.waiting = `${who}: ${action}`;
    const id = ++sequence;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      channel.postMessage({ actor: who, id, action, ...args });
    });
  }
  const url = who => new URL(`./browser-publication.html?database=${databaseName}&actor=${who}&profile=${options.objectAcceleration}${query.has("drop-permit") ? "&drop-permit" : ""}`, location.href);
  const tabReady = event("ready", "tab");
  const tab = window.open(url("tab"));
  const workerReady = event("ready", "worker");
  const workerUrl = new URL("./browser-publication.mjs", location.href);
  workerUrl.search = url("worker").search;
  const worker = new Worker(workerUrl, { type: "module" });
  worker.onerror = error => events.get("error:worker")?.(new Error(error.message));
  try {
    await Promise.all([tabReady, workerReady]);
    const parent = await workspace.head();
    const now = BigInt(Date.now());
    const expiry = now + 120_000n;
    const [tabLease, initialWorkerLease] = await Promise.all([
      call("tab", "begin", { parent, now, expiry }),
      call("worker", "begin", { parent, now, expiry }),
    ]);
    const identity = new Uint8Array(16).fill(19);
    const mainLease = await windows.begin(workspace.id, parent, "main", now, expiry, identity);
    const beginReplay = await windows.begin(workspace.id, parent, "main", now, expiry, identity);
    assert(equal(mainLease.leaseId, beginReplay.leaseId), "deterministic begin lost retry identity");
    const phase = await windows.inspect(workspace.id);
    assert(phase.kind === "active" && phase.activeLeaseCount === 3, "concurrent leases lost a CAS update");
    assert(equal(tabLease.pinnedParent, parent) && equal(initialWorkerLease.pinnedParent, parent), "overlap did not pin one parent");
    let oversizedRejected = false;
    try { await windows.begin(workspace.id, parent, "x".repeat(65_536), now, expiry); }
    catch { oversizedRejected = true; }
    assert(oversizedRejected && (await windows.inspect(workspace.id)).activeLeaseCount === 3, "oversized control state crossed its hard bound");

    await call("tab", "stage", { path: "/stale", key: new Uint8Array(16).fill(1) });
    assert((await windows.finish(tabLease, now)).kind === "still-active", "premature reconciliation");
    assert((await call("tab", "commit", { lease: tabLease })).status === "fenced", "closed owner published");
    assert(equal(await workspace.head(), parent), "fenced publication moved authority");
    assert((await windows.finish(tabLease, now)).kind === "already-closed", "close replay reopened a lease");

    await call("worker", "stage", { path: "/worker", key: new Uint8Array(16).fill(2) });
    assert((await call("worker", "commit", { lease: { ...initialWorkerLease, expiresAtMillis: expiry + 1n } })).status === "fenced", "forged expiry published");
    const workerLease = await windows.renew(initialWorkerLease, BigInt(Date.now()), expiry + 60_000n);
    assert((await call("worker", "commit", { lease: initialWorkerLease })).status === "fenced", "superseded owner published");
    assert((await call("worker", "commit", { lease: workerLease })).status === "committed", "active worker could not publish");

    // Reload loses all tab heap state; the same durable window and head remain.
    const reloaded = event("ready", "tab");
    channel.postMessage({ actor: "tab", id: ++sequence, action: "reload" });
    await reloaded;
    assert((await call("tab", "read", { path: "/worker" }))[0] === 42, "reload lost worker publication");
    assert((await windows.inspect(workspace.id)).activeLeaseCount === 2, "reload changed lease authority");

    // Kill a worker after preparation, before any publication call.
    await call("worker", "stage", { path: "/abandoned", key: new Uint8Array(16).fill(3) });
    const beforeInterruption = await workspace.head();
    worker.terminate();
    assert(equal(await workspace.head(), beforeInterruption), "prepublication interruption changed authority");

    // Close the tab after durable publication, before its terminal reply.
    await call("tab", "stage", { path: "/durable", key: new Uint8Array(16).fill(4) });
    const published = event("published", "tab");
    channel.postMessage({ actor: "tab", id: ++sequence, action: "publish-without-reply", lease: mainLease });
    await published;
    tab.close();
    const reopened = await openBrowserFs(options);
    try {
      const recovered = await reopened.openWorkspace("main");
      assert((await recovered.read("/durable", 1n))[0] === 42, "acknowledgement interruption lost committed data");
      const expired = await recovered.beginTransaction(new Uint8Array(16).fill(5));
      await expired.write("/expired", Uint8Array.of(9));
      const expiredNow = BigInt(Date.now()) - 10n;
      const shortLease = await windows.begin(workspace.id, await workspace.head(), "expired", expiredNow - 10n, expiredNow);
      assert((await expired.commit(shortLease)).status === "fenced", "expired owner published");
      expired.free();
      const limits = { maximumGenerations: 16, maximumChanges: 16, maximumConflicts: 16 };
      assert((await windows.finish(workerLease, BigInt(Date.now()))).kind === "still-active", "worker close lost live lease");
      assert((await windows.finishWorkspace(workspace, mainLease, BigInt(Date.now()), limits)).kind === "reconciled", "final close did not reconcile expired and live leases");
      assert((await windows.inspect(workspace.id)).kind === "idle", "reconciliation did not restore idle authority");
      assert(await windows.recoverWorkspace(workspace, BigInt(Date.now()), limits) === undefined, "idle recovery reran reconciliation");

      // Equal workspace IDs never grant access to another database's workspace.
      const foreignFs = await openBrowserFs({ ...options, databaseName: `${databaseName}-foreign` });
      try {
        const foreign = await foreignFs.createWorkspace("main");
        assert(equal(foreign.id, workspace.id), "ownership control requires equal workspace IDs");
        const publicFs = adaptWasmFs(fs);
        const publicWorkspace = await publicFs.openWorkspace("main");
        const publicForeign = await adaptWasmFs(foreignFs).openWorkspace("main");
        const publicWindows = adaptWasmOperationWindows(publicFs);
        const ownedLease = await windows.begin(workspace.id, await workspace.head(), "ownership", BigInt(Date.now()), expiry);
        for (const [coordinator, other] of [[windows, foreign], [publicWindows, publicForeign]]) {
          for (const action of [
            () => coordinator.finishWorkspace(other, ownedLease, BigInt(Date.now()), limits),
            () => coordinator.recoverWorkspace(other, expiry + 1n, limits),
          ]) {
            let rejected = false;
            try { await action(); } catch (error) { rejected = String(error).includes("another browser filesystem"); }
            assert(rejected, "foreign workspace crossed the exact filesystem ownership boundary");
            assert((await windows.inspect(workspace.id)).kind === "active", "foreign reconciliation changed the live lease");
          }
        }
        await windows.finish(ownedLease, BigInt(Date.now()));
        const ticket = await windows.inspect(workspace.id);
        assert(ticket.kind === "reconciling", "ownership test did not retain a recovery ticket");
        let rejected = false;
        try { await publicWindows.recoverWorkspace(publicForeign, BigInt(Date.now()), limits); }
        catch (error) { rejected = String(error).includes("another browser filesystem"); }
        const retained = await windows.inspect(workspace.id);
        assert(rejected && retained.kind === "reconciling" && equal(ticket.ticket, retained.ticket), "foreign recovery consumed the owning database's ticket");
        await publicWindows.recoverWorkspace(publicWorkspace, BigInt(Date.now()), limits);
        assert((await windows.inspect(workspace.id)).kind === "idle", "owning workspace could not recover its ticket");
        foreign.free();
      } finally { foreignFs.close(); }
    } finally { reopened.close(); }
    result.dataset.status = "passed";
    result.dataset.waiting = "";
    result.textContent = "Concurrent tab/worker CAS, stale and forged/expired leases, reload, and publication interruption passed.";
  } finally { worker.terminate(); tab?.close(); channel.close(); fs.close(); }
}
