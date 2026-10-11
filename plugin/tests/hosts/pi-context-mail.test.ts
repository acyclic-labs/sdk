import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import type { SessionEntry } from "@earendil-works/pi-coding-agent";
import { installedNativeFixture, type InstalledNativeFixture } from "./pi-native-fixture.ts";

// The async faux response factory is Pi AI's actual provider API. All mailbox,
// admission, process and delivery behavior below belongs to the production extension.
const provider = `
import { VERSION } from "@earendil-works/pi-coding-agent";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { setTimeout as delay } from "node:timers/promises";
export default function nativeMailbox(pi) {
  if (VERSION !== "1.1.0") throw new Error("wrong native SDK");
  let native;
  let responses = 0;
  pi.on("session_start", (_event, ctx) => { native = ctx; });
  const model = fauxProvider({provider:"acyclic-native-qualification", models:[{id:"probe"}]});
  const response = async (context, options) => {
    let role;
    for (const message of context.messages) {
      if (message.role !== "user") continue;
      const text = typeof message.content === "string" ? message.content
        : message.content.filter(part => part.type === "text").map(part => part.text).join("");
      if (text.includes("ACYCLIC_MAIL_ROOT")) role = "ROOT";
      if (text.includes("ACYCLIC_MAIL_CHILD")) role = "CHILD";
    }
    if (!role) throw new Error("lost actual native child instruction");
    responses++;
    await delay(role === "CHILD" && responses === 1 ? 4000 : 750, undefined, {signal:options?.signal});
    const results = context.messages.filter(message => message.role === "toolResult");
    for (const result of results) if (result.isError) throw new Error("production mailbox control failed: " + JSON.stringify(result));
    const done = id => results.some(result => result.toolCallId === id);
    const call = (id, name, args) => fauxAssistantMessage(fauxToolCall(name, args, {id}), {stopReason:"toolUse"});
    if (role === "ROOT") {
      const steps = [
        ["spawn-mail-child", "acyclic_spawn", {id:"mail-child", prompt:"ACYCLIC_MAIL_CHILD"}],
        ["send-mail-child", "acyclic_send", {id:"mail-child", message:"original child follow-up"}],
        ["steer-mail-child", "acyclic_steer", {id:"mail-child", message:"original child steering"}],
        ["send-mail-root", "acyclic_send", {id:native.sessionManager.getSessionId(), message:"original root follow-up"}],
        ["steer-mail-root", "acyclic_steer", {id:native.sessionManager.getSessionId(), message:"original root steering"}],
        ["wait-mail-child", "acyclic_wait", {id:"mail-child"}],
      ];
      for (const [id,name,args] of steps) if (!done(id)) return call(id,name,args);
    }
    const ownMessages = native.sessionManager.getBranch().filter(entry => entry.type === "custom_message" && entry.customType === "acyclic_mail");
    const expected = role === "ROOT" ? ["original root follow-up", "original root steering"]
      : ["original child follow-up", "original child steering"];
    if (!expected.every(text => ownMessages.some(entry => entry.content === text))) {
      // An empty native assistant boundary lets Pi itself consume its queued
      // follow-up/steering messages. This does not restart or emulate the engine.
      return fauxAssistantMessage("");
    }
    return fauxAssistantMessage(JSON.stringify({role, sessionId:native.sessionManager.getSessionId(),
      mail:ownMessages, results:results.map(result => ({id:result.toolCallId,
        text:result.content.filter(part=>part.type==="text").map(part=>part.text).join("")}))}));
  };
  model.setResponses(Array.from({length:32}, () => response));
  pi.registerProvider(model.provider);
}
`;

function nativeMailDetails(entry: SessionEntry) {
  assert.equal(entry.type, "custom_message");
  if (entry.type !== "custom_message") throw new Error("not a native mailbox entry");
  const details = entry.details;
  assert.ok(details && typeof details === "object" &&
    "task_id" in details && typeof details.task_id === "string" &&
    "message_id" in details && typeof details.message_id === "string" &&
    "sequence" in details && typeof details.sequence === "number" && Number.isSafeInteger(details.sequence) &&
    "kind" in details && (details.kind === "message" || details.kind === "steer"));
  return { task_id: details.task_id, message_id: details.message_id, sequence: details.sequence, kind: details.kind };
}

test("installed root and child consume original mailbox messages through native SDK queues and persisted entries", { timeout: 240_000 }, async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-context-mail-", import.meta.url)));
  let fixture: InstalledNativeFixture | undefined;
  try {
    fixture = await installedNativeFixture(directory, provider, {});
    await fixture.start();
    const execution = await fixture.run("ACYCLIC_MAIL_ROOT");
    const report = JSON.parse(execution.stdout.trim());
    assert.equal(report.role, "ROOT");
    assert.equal(report.sessionId, fixture.manager.getSessionId());
    const spawn = JSON.parse(report.results.find((result: { id: string }) => result.id === "spawn-mail-child").text);
    const waited = JSON.parse(report.results.find((result: { id: string }) => result.id === "wait-mail-child").text);
    assert.equal(waited.state, "completed");
    assert.ok(waited.outcome, "waiting must retain the actual host terminal outcome");
    const root = SessionManager.open(fixture.session);
    const child = SessionManager.open(spawn.child.session_file);
    const childMail = child.getBranch().filter(entry => entry.type === "custom_message" && entry.customType === "acyclic_mail");
    const rootMail = root.getBranch().filter(entry => entry.type === "custom_message" && entry.customType === "acyclic_mail");
    for (const [entries, expected] of [
      [rootMail, ["original root follow-up", "original root steering"]],
      [childMail, ["original child follow-up", "original child steering"]],
    ] as const) {
      assert.equal(entries.length, 2, "queued original items must not be delivered twice");
      assert.deepEqual(entries.map(entry => entry.content), expected);
      const metadata = entries.map(nativeMailDetails);
      assert.deepEqual(metadata.map(entry => entry.kind), ["message", "steer"]);
      assert.equal(new Set(metadata.map(entry => entry.message_id)).size, 2);
      const sequences = metadata.map(entry => entry.sequence);
      assert.ok(sequences[0] < sequences[1]);
    }
    assert.ok(childMail.map(nativeMailDetails).every(entry => entry.task_id === spawn.child.task));
    assert.ok(rootMail.map(nativeMailDetails).every(entry => entry.task_id !== spawn.child.task));
    assert.equal(child.getSessionId(), spawn.child.native_session_id);
    assert.notEqual(child.getSessionId(), root.getSessionId());
    assert.ok(!JSON.stringify(report).includes("ACYCLIC_PI_CALLER"));
    assert.ok(!JSON.stringify(report).includes("caller_proof"));
  } finally {
    if (fixture) await fixture.close();
    rmSync(directory, { recursive: true });
  }
});
