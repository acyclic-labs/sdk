import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { test } from "bun:test";
import { fileURLToPath } from "node:url";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { installedNativeFixture, type InstalledNativeFixture } from "./pi-native-fixture.ts";

const rootOnly = "ROOT_ONLY_NATIVE_CONTEXT_0d20f1";
const foreignTranscript = "FOREIGN_NATIVE_TRANSCRIPT_19d2ae";
const provider = `
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function freshNativeContext(pi) {
  let native;
  let firstTurn;
  let initialNativeMessageCount;
  pi.on("session_start", (_event, ctx) => {
    native = ctx;
    initialNativeMessageCount = ctx.sessionManager.getBranch().filter(entry=>entry.type==="message" || entry.type==="custom_message").length;
  });
  const model = fauxProvider({provider:"acyclic-native-qualification", models:[{id:"probe"}]});
  const response = context => {
    let role;
    for (const message of context.messages) {
      if (message.role !== "user") continue;
      const text = typeof message.content === "string" ? message.content
        : message.content.filter(part=>part.type==="text").map(part=>part.text).join("");
      for (const candidate of ["ROOT", "SOURCE", "DEFAULT", "EXPLICIT"]) {
        if (text.startsWith("ACYCLIC_FRESH_" + candidate)) role = candidate;
      }
    }
    if (!role) throw new Error("lost original native task instruction");
    const fullContext = JSON.stringify(context.messages);
    if (!firstTurn) {
      firstTurn = {role, native_session_id:native.sessionManager.getSessionId(), initialNativeMessageCount,
        childOnlySeen:role!=="ROOT" && fullContext.includes("CHILD_ONLY_" + role + "_a937"),
        rootOnlySeen:fullContext.includes(${JSON.stringify(rootOnly)}),
        foreignTranscriptSeen:fullContext.includes(${JSON.stringify(foreignTranscript)})};
      if (role === "SOURCE" || role === "DEFAULT") {
        if (firstTurn.rootOnlySeen || initialNativeMessageCount !== 0) throw new Error("default native child inherited parent context instead of an empty native baseline");
      }
      if (role === "EXPLICIT" && !firstTurn.rootOnlySeen) throw new Error("explicit caller cut lost the actual caller context");
      if (role !== "ROOT" && !firstTurn.childOnlySeen) throw new Error("first native provider turn lost its actual child-only instruction");
      if (role !== "ROOT" && firstTurn.foreignTranscriptSeen) throw new Error("workspace selection cloned a foreign native transcript");
      if (role === "SOURCE") {
        pi.sendMessage({customType:"private_source_context",content:${JSON.stringify(foreignTranscript)},display:false}, {triggerTurn:false});
      }
    }
    const results = context.messages.filter(message=>message.role==="toolResult");
    for (const result of results) if (result.isError) throw new Error("actual context/workspace control failed: " + JSON.stringify(result));
    const done = id => results.some(result=>result.toolCallId===id);
    const call = (id,name,args) => fauxAssistantMessage(fauxToolCall(name,args,{id}),{stopReason:"toolUse"});
    if (role === "ROOT") {
      const steps = [
        ["spawn-source", "acyclic_spawn", {id:"source-agent",prompt:"ACYCLIC_FRESH_SOURCE CHILD_ONLY_SOURCE_a937"}],
        ["wait-source", "acyclic_wait", {id:"source-agent"}],
        ["spawn-default", "acyclic_spawn", {id:"fresh-agent",source:"source-agent",prompt:"ACYCLIC_FRESH_DEFAULT CHILD_ONLY_DEFAULT_a937"}],
        ["spawn-explicit", "acyclic_spawn", {id:"explicit-agent",source:"source-agent",context:"native_cut",prompt:"ACYCLIC_FRESH_EXPLICIT CHILD_ONLY_EXPLICIT_a937"}],
        ["wait-default", "acyclic_wait", {id:"fresh-agent"}],
        ["wait-explicit", "acyclic_wait", {id:"explicit-agent"}],
      ];
      for (const [id,name,args] of steps) if (!done(id)) return call(id,name,args);
      return fauxAssistantMessage(JSON.stringify({firstTurn,results:results.map(result=>({id:result.toolCallId,
        text:result.content.filter(part=>part.type==="text").map(part=>part.text).join("")}))}));
    }
    if (role === "SOURCE" && !done("write-source-workspace")) {
      return call("write-source-workspace","write",{path:"source-workspace.txt",content:"selected foreign workspace payload\\n"});
    }
    if ((role === "DEFAULT" || role === "EXPLICIT") && !done("read-selected-workspace")) {
      return call("read-selected-workspace","read",{path:"source-workspace.txt"});
    }
    if (role === "DEFAULT" || role === "EXPLICIT") {
      const result = results.find(result=>result.toolCallId==="read-selected-workspace");
      if (!JSON.stringify(result.content).includes("selected foreign workspace payload")) throw new Error("fresh context discarded the selected workspace instead of just native history");
    }
    return fauxAssistantMessage(JSON.stringify({firstTurn,
      sourcePrivateContextSeen:role==="SOURCE" && fullContext.includes(${JSON.stringify(foreignTranscript)})}));
  };
  model.setResponses(Array.from({length:32},()=>response));
  pi.registerProvider(model.provider);
}
`;

function lastNativeReport(session: SessionManager) {
  const assistant = session.getBranch().findLast(entry => entry.type === "message" && entry.message.role === "assistant");
  assert.ok(assistant && assistant.type === "message" && assistant.message.role === "assistant");
  const text = assistant.message.content.filter(part => part.type === "text").map(part => part.text).join("");
  return JSON.parse(text);
}

test("actual first native turns are fresh by default and explicit foreign-workspace context inherits only the caller", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-context-fresh-", import.meta.url)));
  let fixture: InstalledNativeFixture | undefined;
  try {
    fixture = await installedNativeFixture(directory, provider, {});
    await fixture.start();
    const execution = await fixture.run(`ACYCLIC_FRESH_ROOT ${rootOnly}`);
    const root = JSON.parse(execution.stdout.trim());
    assert.equal(root.firstTurn.rootOnlySeen, true);
    const children = new Map<string, { task: string; native_session_id: string; session_file: string }>();
    for (const id of ["spawn-source", "spawn-default", "spawn-explicit"]) {
      const result = root.results.find((value: { id: string }) => value.id === id);
      assert.ok(result);
      children.set(id, JSON.parse(result.text).child);
    }
    for (const id of ["wait-source", "wait-default", "wait-explicit"]) {
      const result = JSON.parse(root.results.find((value: { id: string }) => value.id === id).text);
      assert.equal(result.state, "completed");
      assert.ok(result.outcome);
    }
    const source = SessionManager.open(children.get("spawn-source")!.session_file);
    const fresh = SessionManager.open(children.get("spawn-default")!.session_file);
    const explicit = SessionManager.open(children.get("spawn-explicit")!.session_file);
    const sourceReport = lastNativeReport(source);
    const freshReport = lastNativeReport(fresh);
    const explicitReport = lastNativeReport(explicit);
    assert.equal(sourceReport.firstTurn.rootOnlySeen, false);
    assert.equal(sourceReport.sourcePrivateContextSeen, true, "foreign source really must have its own private native context");
    assert.equal(freshReport.firstTurn.rootOnlySeen, false);
    assert.equal(explicitReport.firstTurn.rootOnlySeen, true);
    assert.equal(sourceReport.firstTurn.initialNativeMessageCount, 0);
    assert.equal(freshReport.firstTurn.initialNativeMessageCount, 0);
    assert.ok(explicitReport.firstTurn.initialNativeMessageCount > 0);
    for (const child of [sourceReport, freshReport, explicitReport]) assert.equal(child.firstTurn.childOnlySeen, true);
    assert.equal(freshReport.firstTurn.foreignTranscriptSeen, false);
    assert.equal(explicitReport.firstTurn.foreignTranscriptSeen, false);
    assert.ok(JSON.stringify(source.getEntries()).includes(foreignTranscript));
    assert.ok(!JSON.stringify(fresh.getEntries()).includes(rootOnly));
    assert.ok(!JSON.stringify(fresh.getEntries()).includes(foreignTranscript));
    assert.ok(!JSON.stringify(explicit.getEntries()).includes(foreignTranscript));
    const identities = [fixture.manager.getSessionId(), source.getSessionId(), fresh.getSessionId(), explicit.getSessionId()];
    assert.equal(new Set(identities).size, 4, "each real SDK session retains its own generated identity");
  } finally {
    if (fixture) await fixture.close();
    rmSync(directory, { recursive: true });
  }
}, { timeout: 240_000 });
