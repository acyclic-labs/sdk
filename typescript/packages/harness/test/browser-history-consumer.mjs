import { BrowserAggregate, BrowserHistoryReader, Harness, NativeContracts } from "../dist/index.js";
import { ErrorCode } from "../generated/proto/harness/v2/harness_pb.js";

export async function exerciseBrowserHistory() {
  const options = { authority: { kind: "task", id: "browser-history-owner" },
    issuerId: "browser-history-owner", issuerKey: new Uint8Array(32).fill(31),
    database: `harness-history-${crypto.randomUUID()}`, maximumCommands: 1024,
    maximumJournalBytes: 8n * 1024n * 1024n,
    memoryLimits: { paths: 128, path_bytes: 65536, records: 128, payload_bytes: 1048576,
      commits: 128, idempotency_results: 128 } };
  const issuer = await Harness.create(options);
  const contracts = await NativeContracts.create();
  const scope = issuer.issueScope("history", ["lifecycle:manage"]);
  const command = (operation, revision, to) => ({
    operation_id: operation, idempotency_key: `history:${operation}`, expected_revision: revision,
    scope, causal_parent: null, action: { kind: "transition_lifecycle", to, reason: null } });
  const first = command("61616161-6161-6161-6161-616161616161", 0n, "active");
  let aggregate, reader, cold, wrong, competing;
  try {
    aggregate = await BrowserAggregate.open(options);
    reader = aggregate.historyReader();
    const empty = await reader.pin();
    const limits = { maximum_events: 1, maximum_bytes: 65536n };
    if (empty.through_revision !== 0n || (await reader.readPage(empty, limits)).events.length !== 0) {
      throw new Error("fresh archival reader did not expose an empty terminal cursor");
    }
    if ((await aggregate.execute(first)).result !== "applied") throw new Error("fresh indexed publication replayed");
    if ((await reader.readPage(empty, limits)).events.length !== 0) throw new Error("empty pinned history included its first later publication");
    const snapshot = aggregate.snapshot();
    const cursor = await reader.pin();
    await aggregate.execute(command("62626262-6262-6262-6262-626262626262", 1n, "waiting"));
    const page = await reader.readPage(cursor, limits);
    if (page.events.length !== 1 || page.events[0].operation_id !== first.operation_id
        || page.cursor.after_revision !== 1n || page.cursor.through_revision !== 1n) {
      throw new Error("pinned archival page included a later append");
    }
    if ((await reader.readPage(page.cursor, limits)).events.length !== 0) throw new Error("terminal page is not empty");
    aggregate.free(); aggregate = undefined;
    reader.free(); reader = undefined;
    aggregate = await BrowserAggregate.open(options, snapshot);
    if (aggregate.head()[1] !== 2n || (await aggregate.execute(first)).result !== "replayed"
        || aggregate.head()[1] !== 2n) throw new Error("reopened old operation republished");
    if ((await aggregate.reconcile(first))?.event.revision !== 1n) throw new Error("exact reconciliation changed the original event");
    let changedRejected = false;
    try { await aggregate.execute({ ...first, idempotency_key: `${first.idempotency_key}:changed` }); }
    catch (error) { changedRejected = error.code === ErrorCode.CONFLICT; }
    if (!changedRejected) throw new Error("changed retry identity accepted");
    competing = await BrowserAggregate.open(options, snapshot);
    await aggregate.execute(command("64646464-6464-6464-6464-646464646464", 2n, "active"));
    let staleRejected = false;
    try { await competing.execute(command("65656565-6565-6565-6565-656565656565", 2n, "active")); }
    catch (error) { staleRejected = error.code === ErrorCode.CONFLICT; }
    if (!staleRejected || competing.head()[1] !== 2n) throw new Error("stale publication advanced its projection");
    if (!await competing.refreshThrough(3n, 1) || competing.head()[1] !== 3n) {
      throw new Error("bounded projection refresh did not admit the committed prefix");
    }
    await competing.execute(command("66666666-6666-6666-6666-666666666666", 3n, "waiting"));
    competing.free(); competing = undefined;
    cold = await BrowserHistoryReader.open(options);
    const pinned = await cold.pin();
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
    if (!boundRejected || !contracts.canonicalEqual(pinned, { ...pinned, after_revision: 0n })) {
      throw new Error("failed page advanced the supplied cursor");
    }
    wrong = await BrowserHistoryReader.open({ ...options, issuerKey: new Uint8Array(32).fill(32) });
    let ownerRejected = false;
    try { await wrong.readPage(pinned, limits); }
    catch (error) { ownerRejected = error.code === ErrorCode.UNAUTHORIZED; }
    if (!ownerRejected) throw new Error("wrong issuer read accepted an unattested event");
  } finally {
    competing?.free(); wrong?.free(); cold?.free(); reader?.free(); aggregate?.free(); issuer.free();
  }
}
