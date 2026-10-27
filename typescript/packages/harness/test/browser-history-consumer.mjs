import { BrowserAggregate, BrowserHistoryReader, BrowserAgentAuthority,
  initializeBrowserRuntime } from "../dist/browser.js";
import { encodeCanonicalJson } from "../generated/wasm/acyclic_harness_wasm.js";

// Canonical wire refusals; no retired protobuf transport dependency.
const CONFLICT = 2, INVALID = 4, UNAUTHORIZED = 5;
const equal = (left, right) => {
  const a = encodeCanonicalJson(left), b = encodeCanonicalJson(right);
  return a.length === b.length && a.every((byte, index) => byte === b[index]);
};

export async function exerciseBrowserHistory() {
  await initializeBrowserRuntime();
  await exerciseConversationHead();
  const options = { authority: { kind: "task", id: "browser-history-owner" },
    issuer_id: "browser-history-owner", issuer_key: new Uint8Array(32).fill(31),
    database: `harness-history-${crypto.randomUUID()}`, maximum_commands: 1024,
    maximum_journal_bytes: 8n * 1024n * 1024n,
    memory: { paths: 128, path_bytes: 65536, records: 128, payload_bytes: 1048576,
      commits: 128, idempotency_results: 128 } };
  const issuer = new BrowserAgentAuthority(options.authority, options.issuer_id, options.issuer_key, []);
  const scope = issuer.issueScope("history", ["lifecycle:manage"], undefined);
  const command = (operation, revision, to) => ({
    operation_id: operation, idempotency_key: `history:${operation}`, expected_revision: revision,
    scope, causal_parent: null, action: { kind: "transition_lifecycle", to, reason: null } });
  const first = command("61616161-6161-6161-6161-616161616161", 0n, "active");
  let aggregate, reader, cold, wrong, competing;
  try {
    aggregate = await BrowserAggregate.openBrowser(options, [], undefined);
    reader = aggregate.historyReader();
    const empty = await reader.pin(0n);
    const limits = { maximum_events: 1, maximum_bytes: 65536n };
    if (empty.through_revision !== 0n || (await reader.readPage(empty, limits)).events.length !== 0) {
      throw new Error("fresh archival reader did not expose an empty terminal cursor");
    }
    if ((await aggregate.execute(first)).result !== "applied") throw new Error("fresh indexed publication replayed");
    if ((await reader.readPage(empty, limits)).events.length !== 0) throw new Error("empty pinned history included its first later publication");
    const snapshot = aggregate.snapshot();
    const cursor = await reader.pin(0n);
    await aggregate.execute(command("62626262-6262-6262-6262-626262626262", 1n, "waiting"));
    const page = await reader.readPage(cursor, limits);
    if (page.events.length !== 1 || page.events[0].operation_id !== first.operation_id
        || page.cursor.after_revision !== 1n || page.cursor.through_revision !== 1n) {
      throw new Error("pinned archival page included a later append");
    }
    if ((await reader.readPage(page.cursor, limits)).events.length !== 0) throw new Error("terminal page is not empty");
    aggregate.free(); aggregate = undefined;
    reader.free(); reader = undefined;
    aggregate = await BrowserAggregate.openBrowser(options, [], snapshot);
    if (aggregate.head()[1] !== 2n || (await aggregate.execute(first)).result !== "replayed"
        || aggregate.head()[1] !== 2n) throw new Error("reopened old operation republished");
    if ((await aggregate.reconcile(first))?.event.revision !== 1n) throw new Error("exact reconciliation changed the original event");
    let changedRejected = false;
    try { await aggregate.execute({ ...first, idempotency_key: `${first.idempotency_key}:changed` }); }
    catch (error) { changedRejected = error.code === CONFLICT; }
    if (!changedRejected) throw new Error("changed retry identity accepted");
    competing = await BrowserAggregate.openBrowser(options, [], snapshot);
    await aggregate.execute(command("64646464-6464-6464-6464-646464646464", 2n, "active"));
    let staleRejected = false;
    try { await competing.execute(command("65656565-6565-6565-6565-656565656565", 2n, "active")); }
    catch (error) { staleRejected = error.code === CONFLICT; }
    if (!staleRejected || competing.head()[1] !== 2n) throw new Error("stale publication advanced its projection");
    if (!await competing.refreshThrough(3n, 1) || competing.head()[1] !== 3n) {
      throw new Error("bounded projection refresh did not admit the committed prefix");
    }
    await competing.execute(command("66666666-6666-6666-6666-666666666666", 3n, "waiting"));
    competing.free(); competing = undefined;
    cold = await BrowserHistoryReader.openBrowser(options);
    const pinned = await cold.pin(0n);
    const old = await cold.operationEvent(first.operation_id);
    if (old?.revision !== 1n || (await cold.operationEvent("63636363-6363-6363-6363-636363636363")) !== null) {
      throw new Error("cold operation lookup did not use the committed archive");
    }
    let position = pinned;
    for (let revision = 1n; revision <= 4n; revision++) {
      const next = await cold.readPage(position, limits);
      if (next.events.length !== 1 || next.events[0].revision !== revision
          || next.cursor.after_revision !== revision) throw new Error("cold bounded cursor traversal failed");
      position = next.cursor;
    }
    if (pinned.after_revision !== 0n || position.through_revision !== 4n) throw new Error("cursor boundary changed during traversal");
    let boundRejected = false;
    try { await cold.readPage(pinned, { ...limits, maximum_events: 0 }); }
    catch { boundRejected = true; }
    if (!boundRejected || !equal(pinned, { ...pinned, after_revision: 0n })) {
      throw new Error("failed page advanced the supplied cursor");
    }
    wrong = await BrowserHistoryReader.openBrowser({ ...options, issuer_key: new Uint8Array(32).fill(32) });
    let ownerRejected = false;
    try { await wrong.readPage(pinned, limits); }
    catch (error) { ownerRejected = error.code === UNAUTHORIZED; }
    if (!ownerRejected) throw new Error("wrong issuer read accepted an unattested event");
  } finally {
    competing?.free(); wrong?.free(); cold?.free(); reader?.free(); aggregate?.free(); issuer.free();
  }
}

async function exerciseConversationHead() {
  const agent = "71717171-7171-7171-7171-717171717171";
  const options = { authority: { kind: "conversation", id: "browser-message-head" },
    issuer_id: "browser-message-head", issuer_key: new Uint8Array(32).fill(41),
    database: `harness-message-head-${crypto.randomUUID()}`, maximum_commands: 128,
    maximum_journal_bytes: 1024n * 1024n };
  const issuer = new BrowserAgentAuthority(options.authority, options.issuer_id, options.issuer_key, []);
  let aggregate, reader, cold, wrong;
  try {
    aggregate = await BrowserAggregate.openBrowser(options, [], undefined);
    reader = aggregate.historyReader();
    if ((await reader.latestConversationMessage(65536n)) !== null) throw new Error("fresh message head is not empty");
    const scope = issuer.issueScope("bind-head", ["conversation:bind"], agent);
    await aggregate.execute({ operation_id: "72727272-7272-7272-7272-727272727272",
      idempotency_key: "bind-head", expected_revision: 0n, scope, causal_parent: null,
      action: { kind: "bind_conversation", agent } });
    if ((await reader.latestConversationMessage(65536n)) !== null) throw new Error("bound message head is not empty");
    let boundRejected = false;
    try { await reader.latestConversationMessage(0n); }
    catch (error) { boundRejected = error.code === INVALID; }
    if (!boundRejected) throw new Error("zero message-head byte bound accepted");
    reader.free(); reader = undefined;
    aggregate.free(); aggregate = undefined;
    cold = await BrowserHistoryReader.openBrowser(options);
    if ((await cold.latestConversationMessage(65536n)) !== null) throw new Error("reopened binding marker changed");
    wrong = await BrowserHistoryReader.openBrowser({ ...options, issuer_key: new Uint8Array(32).fill(42) });
    let ownerRejected = false;
    try { await wrong.latestConversationMessage(65536n); }
    catch (error) { ownerRejected = error.code === UNAUTHORIZED; }
    if (!ownerRejected) throw new Error("message head accepted another issuer");
  } finally {
    wrong?.free(); cold?.free(); reader?.free(); aggregate?.free(); issuer.free();
  }
}
