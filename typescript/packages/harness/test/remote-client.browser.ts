import { HarnessClient, ClientQuery, TaskVolume, WasmStream, initializeClientQueries } from "../src/client.js";

const assert = (value: unknown, message: string): void => { if (!value) throw new Error(message); };
const encoder = new TextEncoder(), decoder = new TextDecoder();
const request = (id: string) => ({ Task: { task: "task", file: { Request: `${id}.json` } } }) as const;
const command = (id: string) => encoder.encode(JSON.stringify({ operationId: id, authority: { kind: "task", id: "task" }, kind: "task.submit", offlineSafe: true, payload: {} }));
const limits = { commands: 8n, bytes: 4096n, durability: "Accepted" } as const;
const queryLimits = { files: 32n, segments: 64n, bytes: 16384n };

export async function test(endpoint: string) {
  await initializeClientQueries();
  const remote = WasmStream.connect(endpoint, () => "browser-test");
  const local = await WasmStream.openBrowser(`remote-client-${crypto.randomUUID()}`, 64, 131072n);
  const outbox = await TaskVolume.create(local, "client/files");
  const target = await TaskVolume.create(remote, "agent/files");
  const client = await HarnessClient.open(outbox, target, limits);
  const query = await ClientQuery.open(target, "Root", null, queryLimits);
  const views = query.views();
  const cancel = Promise.withResolvers<void>();
  const streamed = Promise.withResolvers<void>();
  const seen: string[] = [];
  let text = false;
  const running = query.follow(delivery => {
    const key = delivery.file?.key;
    if (key && typeof key === "object" && "Task" in key) {
      if (typeof key.Task.file === "object" && "Request" in key.Task.file) {
        const id = JSON.parse(decoder.decode(delivery.file!.bytes)).operationId;
        if (id !== "existing") seen.push(id);
      }
      if (typeof key.Task.file === "object" && "Chunk" in key.Task.file) {
        assert(decoder.decode(delivery.file!.bytes) === "streaming markdown", "streamed text delta changed");
        text = true;
      }
    }
  }, () => { if (text) streamed.resolve(); }, cancel.promise);
  await fetch(`${endpoint}/fixture/offline`, { method: "POST" });
  await client.enqueue(request("first"), command("first"));
  await client.enqueue(request("second"), command("second"));
  assert(client.pending() === 2, "indeterminate publication retired an offline request");
  const idle = client.whenIdle();
  await fetch(`${endpoint}/fixture/recover`, { method: "POST" });
  await idle;
  assert(client.pending() === 0, "Rust did not automatically drain after recovery");

  // The target already owns this immutable identity with different content.
  await target.write(await target.head(), request("rejected"), command("existing"), null);
  await fetch(`${endpoint}/fixture/offline`, { method: "POST" });
  await client.enqueue(request("rejected"), command("rejected"));
  await client.enqueue(request("valid"), command("valid"));
  const rejectedThenValid = client.whenIdle();
  await fetch(`${endpoint}/fixture/recover`, { method: "POST" });
  await rejectedThenValid;
  assert(client.lastError()?.kind === "Rejected", "terminal rejection was not reported and retired");
  assert(client.pending() === 0, "terminal rejection stranded the next request");
  client.free();
  const restored = await HarnessClient.open(outbox, target, limits);
  assert(restored.pending() === 0, "reopening resurrected retired requests");
  // A failed unary request cannot strand pending work behind an otherwise healthy socket.
  const beforeRetry = await (await fetch(`${endpoint}/fixture/connections`)).text();
  await fetch(`${endpoint}/fixture/fail-next`, { method: "POST" });
  await restored.enqueue(request("retry"), command("retry"));
  await restored.whenIdle();
  assert(await (await fetch(`${endpoint}/fixture/connections`)).text() === beforeRetry, "HTTP retry unnecessarily restarted the shared session");

  // Drain only on a pushed durability watermark, after the final data was consumed.
  const durableTarget = await TaskVolume.create(remote, "durable/files");
  const durableOutbox = await TaskVolume.create(local, "durable-outbox/files");
  const durableClient = await HarnessClient.open(durableOutbox, durableTarget, { ...limits, durability: "Durable" });
  await durableClient.enqueue(request("saved"), command("saved"));
  let awaitingDurability = false;
  try { await durableClient.flush(); } catch { awaitingDurability = true; }
  assert(awaitingDurability && durableClient.pending() === 1, "accepted data retired before durable confirmation");
  await durableTarget.write(await durableTarget.head(), "Goal", encoder.encode("last data before save"), null);
  await fetch(`${endpoint}/fixture/consumed`);
  const saved = durableClient.whenIdle();
  await fetch(`${endpoint}/fixture/save`, { method: "POST" });
  await saved;
  assert(durableClient.pending() === 0, "standalone durability did not wake the outbox");
  durableClient.free(); durableOutbox.free(); durableTarget.free();
  // A remote private outbox can advance durability while the target is silent.
  // Freeze each durable prefix: saving the request must not also save its later
  // checkpoint. Both waits must wake through the outbox's session follow.
  const remoteOutbox = await TaskVolume.create(remote, "remote-outbox/files");
  const localTarget = await TaskVolume.create(local, "local-target/files");
  const remoteOutboxClient = await HarnessClient.open(remoteOutbox, localTarget, { ...limits, durability: "Durable" });
  const checkpointQuery = await ClientQuery.open(remoteOutbox, "Root", null, queryLimits);
  const checkpointed = Promise.withResolvers<void>();
  const stopCheckpoint = Promise.withResolvers<void>();
  const watchingCheckpoint = checkpointQuery.follow(delivery => {
    const key = delivery.file?.key;
    if (key && typeof key === "object" && "Input" in key && key.Input === "workspace/client/outbox.json") checkpointed.resolve();
  }, () => {}, stopCheckpoint.promise);
  await remoteOutboxClient.enqueue(request("outbox-saved"), command("outbox-saved"));
  let pendingSave = false;
  try { await remoteOutboxClient.flush(); } catch { pendingSave = true; }
  assert(pendingSave && remoteOutboxClient.pending() === 1, "outbox request did not wait for its durable prefix");
  const requestCut = (await remoteOutbox.head()).position;
  await fetch(`${endpoint}/fixture/outbox-consumed/${requestCut}`);
  const outboxIdle = remoteOutboxClient.whenIdle();
  await fetch(`${endpoint}/fixture/save-outbox`, { method: "POST" });
  await checkpointed.promise;
  const checkpointCut = (await remoteOutbox.head()).position;
  assert(checkpointCut > requestCut, "outbox-only request save did not publish the checkpoint");
  await fetch(`${endpoint}/fixture/outbox-consumed/${checkpointCut}`);
  pendingSave = false;
  try { await remoteOutboxClient.flush(); } catch { pendingSave = true; }
  assert(pendingSave && remoteOutboxClient.pending() === 1, "checkpoint retired before its separate durable confirmation");
  await fetch(`${endpoint}/fixture/save-outbox`, { method: "POST" });
  await outboxIdle;
  assert(remoteOutboxClient.pending() === 0, "outbox-only checkpoint save did not resolve whenIdle");
  assert((await remoteOutbox.head()).position === checkpointCut, "durability retry appended another checkpoint");
  stopCheckpoint.resolve(); await watchingCheckpoint;
  remoteOutboxClient.free(); checkpointQuery.free();
  const reopenedOutbox = await HarnessClient.open(remoteOutbox, localTarget, { ...limits, durability: "Durable" });
  assert(reopenedOutbox.pending() === 0, "reopen resurrected a durably saved outbox request");
  reopenedOutbox.free(); remoteOutbox.free(); localTarget.free();
  const chunk = { Task: { task: "task", file: { Chunk: { turn: 9007199254740993n, sequence: 1n } } } } as const;
  await target.write(await target.head(), chunk, encoder.encode("streaming markdown"), "Text");
  await streamed.promise;
  const head = { Task: { task: "task", file: { TextHead: { turn: 9007199254740993n, channel: "Text" } } } } as const;
  assert(views.segments(head)?.map(segment => decoder.decode(segment)).join("") === "streaming markdown", "remote updates bypassed the shared Rust view fold");
  assert(JSON.stringify(seen) === JSON.stringify(["first", "second", "valid", "retry"]), `requests were skipped, reordered or duplicated: ${seen}`);
  cancel.resolve(); await running;
  restored.free(); views.free(); query.free(); outbox.free(); target.free(); local.free(); remote.free();
  return { requests: seen, textDeltas: 1, javascriptRetries: 0, durabilityOnly: true, remoteOutboxDurability: true };
}
