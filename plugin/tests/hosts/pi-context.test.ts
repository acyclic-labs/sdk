import assert from "node:assert/strict";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "bun:test";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { discoverAndLoadExtensions, SessionManager, parseSessionEntries } from "@earendil-works/pi-coding-agent";
import type { PiInboxMessage } from "../../pi/index.ts";
import { loadPiContextHelpers } from "./pi-fixture.ts";

const {
  assertParentCutCurrent, captureParentCut, childSessionBytes,
  nativeChildSession, PiToolOperations, PiControlAcknowledgementError,
  visibleControlResult, observePiOperation, persistedPiInboxCursor, consumePiInbox, approvePiWriteback,
} = await loadPiContextHelpers();

test("the native Pi loader resolves and registers the extension outside the host package", async () => {
  const isolated = mkdtempSync(fileURLToPath(new URL(".pi-loader-", import.meta.url)));
  try {
    const loaded = await discoverAndLoadExtensions(
      [fileURLToPath(new URL("../../pi/index.ts", import.meta.url))], isolated, isolated);
    assert.deepEqual(loaded.errors, []);
    assert.equal(loaded.extensions.length, 1);
    assert.ok(loaded.extensions[0].commands.has("acyclic-doctor"));
    assert.deepEqual([...loaded.extensions[0].tools.keys()].sort(), [
      "acyclic_agents", "acyclic_cancel", "acyclic_context", "acyclic_diff", "acyclic_import",
      "acyclic_inspect", "acyclic_model", "acyclic_send", "acyclic_spawn", "acyclic_steer",
      "acyclic_wait", "acyclic_writeback", "edit", "read", "write",
    ]);
    assert.ok(loaded.extensions[0].handlers.has("session_start"));
    for (const event of ["agent_start", "agent_end", "session_shutdown"]) {
      assert.ok(loaded.extensions[0].handlers.has(event));
    }
    for (const registered of loaded.extensions[0].tools.values()) {
      const schema = registered.definition.parameters;
      for (const field of ["caller", "callerproof", "caller_proof", "signer", "environment", "tasklease"]) {
        assert.ok(!Object.hasOwn(schema.properties, field), `${registered.definition.name} must not expose ${field}`);
      }
    }
    const writeback = loaded.extensions[0].tools.get("acyclic_writeback");
    assert.ok(writeback);
    assert.ok(!Object.hasOwn(writeback.definition.parameters.properties, "approval_digest"), "model must not supply human approval");
  } finally {
    rmSync(isolated, { recursive: true });
  }
});

function spawnMessage(id: string) {
  return fauxAssistantMessage(fauxToolCall("acyclic_spawn", {}, { id }), { stopReason: "toolUse", timestamp: 1 });
}


test("bounds, missing invocation and digest corruption reject without changing parent", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage({ role: "user", content: "context", timestamp: 1 });
  parent.appendMessage(spawnMessage("spawn-1"));
  const before = JSON.stringify(parent.getEntries());
  assert.throws(() => captureParentCut(parent, "missing", 100, 100_000));
  assert.throws(() => captureParentCut(parent, "spawn-1", 1, 100_000));
  assert.throws(() => captureParentCut(parent, "spawn-1", 100, 1));
  assert.throws(() => captureParentCut(parent, "spawn-1", Infinity, 100_000));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  assert.throws(() => childSessionBytes({ ...cut, entriesJson: "[]" }, "child", process.cwd()));
  assert.equal(JSON.stringify(parent.getEntries()), before);
});

test("pre-admission revalidation rejects parent changes without replacing the pinned cut", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  const root = parent.appendMessage({ role: "user", content: "pinned instruction", timestamp: 1 });
  const owner = parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const before = JSON.stringify(cut);
  assertParentCutCurrent(parent, "spawn-1", cut, 100, 100_000);
  parent.appendMessage({ role: "user", content: "later history", timestamp: 2 });
  assert.throws(() => assertParentCutCurrent(parent, "spawn-1", cut, 100, 100_000), /changed before/);
  parent.branch(owner);
  assertParentCutCurrent(parent, "spawn-1", cut, 100, 100_000);
  const switched = SessionManager.inMemory(join(process.cwd(), "another-project"), { id: "parent" },
    structuredClone(parent.getEntries()));
  assert.throws(() => assertParentCutCurrent(switched, "spawn-1", cut, 100, 100_000), /changed before/);
  parent.branch(root);
  assert.throws(() => assertParentCutCurrent(parent, "spawn-1", cut, 100, 100_000), /absent/);
  assert.equal(JSON.stringify(cut), before);
});

test("a complete tool exchange survives; an omitted result rejects the native projection", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage(spawnMessage("completed"));
  const result = parent.appendMessage({ role: "toolResult", toolCallId: "completed", toolName: "acyclic_spawn",
    content: [{ type: "text", text: "completed result" }], isError: false, timestamp: 2 });
  const expected = parent.buildSessionContext();
  const boundary = parent.getLeafId();
  parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const child = SessionManager.inMemory(process.cwd(), undefined,
    parseSessionEntries(childSessionBytes(cut, "child", process.cwd())));
  assert.deepEqual(child.buildSessionContext(), expected);
  parent.branch(boundary!);
  parent.appendContextEdit(result, null);
  parent.appendMessage(spawnMessage("spawn-2"));
  assert.throws(() => childSessionBytes(captureParentCut(parent, "spawn-2", 100, 100_000),
    "child", process.cwd()), /incomplete tool exchange/);
});

test("a first-turn spawn produces a separate empty native child session", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 1, 2);
  assert.equal(cut.entriesJson, "[]");
  assert.equal(cut.parentLeafId, null);
  const child = SessionManager.inMemory(process.cwd(), undefined,
    parseSessionEntries(childSessionBytes(cut, "child", process.cwd())));
  assert.equal(child.getSessionId(), "child");
  assert.equal(child.getLeafId(), null);
  assert.deepEqual(child.buildSessionContext().messages, []);
  assert.throws(() => childSessionBytes(cut, "parent", process.cwd()), /own identity/);
});

test("compaction and context-edit provenance survive native child serialization", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage({ role: "user", content: "old history", timestamp: 1 });
  const retained = parent.appendMessage({ role: "user", content: "retained history", timestamp: 2 });
  const compact = parent.appendCompaction("native summary", retained, 20_000, { provenance: "original" });
  parent.appendContextEdit(retained, { content: "edited projection" });
  const expected = parent.buildSessionContext();
  const expectedCompaction = JSON.stringify(parent.getEntry(compact));
  parent.appendMessage(spawnMessage("spawn-1"));
  const before = JSON.stringify(parent.getEntries());
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const child = SessionManager.inMemory(process.cwd(), undefined,
    parseSessionEntries(childSessionBytes(cut, "child", process.cwd())));
  assert.deepEqual(child.buildSessionContext(), expected);
  assert.equal(JSON.stringify(child.getEntry(compact)), expectedCompaction);
  assert.equal(JSON.stringify(parent.getEntries()), before);
});

test("parallel sibling tool calls/results are excluded with their owning assistant", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  parent.appendMessage({ role: "user", content: "instruction", timestamp: 1 });
  const message = spawnMessage("spawn-1");
  message.content.push({ type: "toolCall", id: "sibling", name: "read", arguments: {} });
  parent.appendMessage(message);
  parent.appendMessage({ role: "toolResult", toolCallId: "sibling", toolName: "read",
    content: [{ type: "text", text: "sibling result" }], isError: false, timestamp: 2 });
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  assert.ok(!cut.entriesJson.includes("sibling"));
  assert.ok(!cut.entriesJson.includes("spawn-1"));
});

test("duplicate identities, branch changes and cyclic parent chains reject", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  const root = parent.appendMessage({ role: "user", content: "instruction", timestamp: 1 });
  const duplicate = spawnMessage("spawn-1");
  duplicate.content.push(duplicate.content[0]);
  parent.appendMessage(duplicate);
  assert.throws(() => captureParentCut(parent, "spawn-1", 100, 100_000), /duplicated/);
  parent.branch(root);
  assert.throws(() => captureParentCut(parent, "spawn-1", 100, 100_000), /absent/);
  parent.appendMessage(spawnMessage("spawn-2"));
  const entries = structuredClone(parent.getEntries());
  entries[0].parentId = entries.at(-1)!.id;
  const cyclic = SessionManager.inMemory(process.cwd(), { id: "cyclic" }, entries);
  assert.throws(() => captureParentCut(cyclic, "spawn-2", 100, 100_000), /cyclic/);
});

test("reloaded native parent selects the same cut; deep history observes finite bounds", () => {
  const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
  for (let index = 0; index < 50; index++) parent.appendCustomEntry("history", { index });
  parent.appendMessage(spawnMessage("spawn-1"));
  const cut = captureParentCut(parent, "spawn-1", 100, 100_000);
  const restored = SessionManager.inMemory(process.cwd(), undefined,
    parseSessionEntries([parent.getHeader(), ...parent.getEntries()].map(entry => JSON.stringify(entry)).join("\n")));
  assert.deepEqual(captureParentCut(restored, "spawn-1", 100, 100_000), cut);
  assert.throws(() => captureParentCut(restored, "spawn-1", 50, 100_000), /entry bound/);
});

test("bounded native branch histories preserve the exact pre-spawn projection", () => {
  for (let history = 1; history <= 4; history++) {
    for (let alternatives = 0; alternatives <= 2; alternatives++) {
      for (const exchange of [false, true]) {
        for (const compact of [false, true]) {
          const parent = SessionManager.inMemory(process.cwd(), { id: "parent" });
          let retained = "";
          for (let index = 0; index < history; index++) {
            retained = parent.appendMessage({ role: "user", content: `instruction ${index}`, timestamp: index });
          }
          if (exchange) {
            parent.appendMessage(spawnMessage("completed"));
            parent.appendMessage({ role: "toolResult", toolCallId: "completed", toolName: "acyclic_spawn",
              content: [{ type: "text", text: "completed result" }], isError: false, timestamp: 5 });
          }
          if (compact) parent.appendCompaction("native summary", retained, 20_000);
          const selected = parent.getLeafId()!;
          const expected = parent.buildSessionContext();
          for (let index = 0; index < alternatives; index++) {
            parent.appendMessage({ role: "user", content: `excluded branch ${index}`, timestamp: 6 + index });
          }
          parent.branch(selected);
          parent.appendMessage(spawnMessage("spawn"));
          const before = JSON.stringify([parent.getHeader(), parent.getLeafId(), parent.getEntries()]);
          const cut = captureParentCut(parent, "spawn", 100, 100_000);
          const bytes = childSessionBytes(cut, "child", process.cwd());
          const child = SessionManager.inMemory(process.cwd(), undefined, parseSessionEntries(bytes));
          assert.deepEqual(child.buildSessionContext(), expected);
          assert.equal(child.getLeafId(), selected);
          assert.ok(!bytes.includes("excluded branch"));
          assert.equal(JSON.stringify([parent.getHeader(), parent.getLeafId(), parent.getEntries()]), before);
          parent.appendMessage({ role: "user", content: "later mutation", timestamp: 10 });
          assert.equal(childSessionBytes(cut, "child", process.cwd()), bytes);
        }
      }
    }
  }
});

test("explicit native context comes only from the caller while workspace selection cannot supply foreign messages", () => {
  const caller = SessionManager.inMemory(process.cwd(), { id: "caller" });
  caller.appendMessage({ role: "user", content: "CALLER_NATIVE_CONTEXT", timestamp: 1 });
  const expected = caller.buildSessionContext();
  caller.appendMessage(spawnMessage("spawn-with-foreign-workspace"));
  const cut = captureParentCut(caller, "spawn-with-foreign-workspace", 100, 100_000);
  const native = nativeChildSession(caller, process.cwd(), cut);
  const child = SessionManager.inMemory(process.cwd(), undefined, parseSessionEntries(native.session_bytes));
  assert.equal(cut.mode, "caller_tool");
  assert.equal(cut.parentSessionId, "caller");
  assert.deepEqual(child.buildSessionContext(), expected);
  assert.ok(native.session_bytes.includes("CALLER_NATIVE_CONTEXT"));
  assert.ok(!native.session_bytes.includes("spawn-with-foreign-workspace"));
  assert.notEqual(child.getSessionId(), caller.getSessionId());
  const foreign = SessionManager.inMemory(process.cwd(), { id: "foreign" });
  foreign.appendMessage({ role: "user", content: "FOREIGN_TRANSCRIPT_ONLY", timestamp: 1 });
  foreign.appendMessage(spawnMessage("foreign-spawn"));
  assert.throws(() => nativeChildSession(caller, process.cwd(),
    captureParentCut(foreign, "foreign-spawn", 100, 100_000)), /actual caller native session/);
});



test("the same SDK operation retains uncertain acknowledgement and never reseeds its native session", async () => {
  const operations = new PiToolOperations();
  const caller = SessionManager.inMemory(process.cwd(), { id: "caller" });
  let seeded = 0;
  let nativeId: string | undefined;
  const invoke = async () => {
    seeded++;
    nativeId = nativeChildSession(caller, process.cwd()).native_session_id;
    throw new PiControlAcknowledgementError();
  };
  const first = operations.run("caller", "original-spawn-tool", { id: "child", context: "scratch" }, invoke);
  await assert.rejects(first, error => error instanceof PiControlAcknowledgementError &&
    error.operation === "original-spawn-tool" && /uncertain/.test(error.message));
  const retainedNativeId = nativeId;
  const second = operations.run("caller", "original-spawn-tool", { id: "child", context: "scratch" }, invoke);
  assert.equal(second, first);
  await assert.rejects(second, /original-spawn-tool/);
  assert.equal(seeded, 1);
  assert.equal(nativeId, retainedNativeId);
  assert.throws(() => operations.run("caller", "original-spawn-tool", { id: "replacement" }, invoke), /changed arguments/);
  assert.throws(() => operations.run("caller", "", {}, invoke), /original SDK tool call identity/);
});

test("successful concurrent SDK invocation is single-flight and child routing uses its own identity", async () => {
  const operations = new PiToolOperations();
  let effects = 0;
  const invoke = async () => ({ count: ++effects });
  const first = operations.run("root", "send-tool", { id: "child", message: "instruction" }, invoke);
  const same = operations.run("root", "send-tool", { id: "child", message: "instruction" }, invoke);
  assert.equal(first, same);
  assert.deepEqual(await first, { count: 1 });
  assert.deepEqual(await operations.run("child", "send-tool", { id: "grandchild", message: "instruction" }, invoke), { count: 2 });
});

test("headless model confirmation never publishes or supplies prepared root approval", async () => {
  const operations = new PiToolOperations();
  const fields = { source: "selected-source", root: "selected-root", paths: ["nested/file.txt"], confirm: true };
  const poisonedModelFields = { ...fields, approval_digest: "model-echo", operator: { scope: "model-echo" } };
  const requests: Readonly<Record<string, unknown>>[] = [];
  const context = {
    hasUI: false, sessionManager: SessionManager.inMemory(),
    ui: { confirm: async () => { throw new Error("headless native UI must not be called"); } },
  };
  const prepared = { status: "prepared", operation: "writeback-original", source: fields.source,
    root: fields.root, paths: fields.paths, approval_digest: "exact-prepared-digest" };
  const invoke = () => approvePiWriteback("writeback-original", poisonedModelFields, context, async request => {
    requests.push(request);
    return prepared;
  });
  const first = operations.run(context.sessionManager.getSessionId(), "writeback-original", fields, invoke);
  assert.deepEqual(await first, { operation: prepared.operation, source: fields.source, root: fields.root,
    paths: fields.paths, approval_digest: prepared.approval_digest, status: "pending", approval: "required" });
  assert.equal(operations.run(context.sessionManager.getSessionId(), "writeback-original", fields, invoke), first);
  assert.deepEqual(requests, [{ operation: "writeback-original", ...fields }]);
  assert.ok(!Object.hasOwn(requests[0], "approval_digest"), "model confirm must never stand in for human digest approval");
});

test("uncertain writeback prepare and mismatched approval operations never reach native confirmation", async () => {
  const fields = { source: "selected-source", paths: ["file.txt"], confirm: true };
  const context = {
    hasUI: true, sessionManager: SessionManager.inMemory(),
    ui: { confirm: async () => { throw new Error("invalid prepare must not reach native confirmation"); } },
  };
  const uncertain = { uncertain: { operation: "writeback-original", reason: "lost original acknowledgement" } };
  let requests = 0;
  assert.equal(await approvePiWriteback("writeback-original", fields, context, async () => {
    requests++;
    return uncertain;
  }), uncertain);
  assert.equal(requests, 1);
  await assert.rejects(approvePiWriteback("writeback-original", fields, context, async () => ({
    status: "prepared", operation: "replacement-operation", approval_digest: "digest",
  })), /original prepared approval digest/);
  const cancelled = new AbortController();
  cancelled.abort();
  await assert.rejects(approvePiWriteback("writeback-original", fields, context, async () => {
    throw new Error("cancelled native callback must not prepare");
  }, cancelled.signal), /abort/i);
});

test("headless writeback retains one promise through real host status and requires Applied evidence", async () => {
  const fields = { source: "child", root: "original-root", paths: ["selected.txt"], confirm: true };
  const prepared = { ...fields, operation: "original", status: "prepared", approval_digest: "exact" };
  const context = { hasUI: false, sessionManager: SessionManager.inMemory(),
    ui: { confirm: async () => { throw new Error("not UI"); } } };
  const operations = new PiToolOperations();
  const publication = Promise.withResolvers<Record<string, unknown>>();
  const observed = Promise.withResolvers<void>();
  let prepares = 0;
  let events = 0;
  let polls = 0;
  const invoke = () => approvePiWriteback("original", fields, context, async request => {
    prepares++;
    assert.ok(!Object.hasOwn(request, "approval_digest"));
    return { ...prepared, signer: "must-not-escape", operator_scope: "must-not-escape" };
  }, undefined, {
    prepared: descriptor => {
      events++;
      assert.deepEqual(descriptor, { operation: "original", source: "child", root: "original-root",
        paths: ["selected.txt"], approval_digest: "exact" });
    },
    observe: async request => {
      polls++;
      assert.deepEqual(request, { operation: "original", source: "child", root: "original-root", paths: ["selected.txt"] });
      observed.resolve();
      return publication.promise;
    },
    bounds: { maximumObservations: 2, timeoutMs: 1000, intervalMs: 0 },
  });
  const first = operations.run("root", "original", fields, invoke);
  await observed.promise;
  assert.equal(operations.run("root", "original", fields, invoke), first);
  publication.resolve({ ...prepared, status: "published", receipt: { phase: "Applied", operation: "native-original" } });
  assert.equal((await first).status, "published");
  assert.equal(prepares, 1);
  assert.equal(events, 1);
  assert.equal(polls, 1);
});

test("headless denial, pending bounds, mismatched proof and missing Applied receipts never dispatch publication", async () => {
  const fields = { source: "child", root: "root", paths: ["selected.txt"], confirm: true };
  const prepared = { ...fields, operation: "original", status: "prepared", approval_digest: "exact" };
  const context = { hasUI: false, sessionManager: SessionManager.inMemory(), ui: { confirm: async () => true } };
  for (const status of ["declined", "pending"]) {
    let prepares = 0;
    let polls = 0;
    const result = await approvePiWriteback("original", fields, context, async () => {
      prepares++; return prepared;
    }, undefined, { prepared: () => {}, observe: async () => { polls++; return { ...prepared, status }; },
      bounds: { maximumObservations: 2, timeoutMs: 1000, intervalMs: 0 } });
    assert.equal(result.status, status);
    assert.equal(prepares, 1);
    assert.equal(polls, status === "declined" ? 1 : 2);
    assert.ok(!Object.hasOwn(result, "receipt"));
  }
  for (const observation of [
    { ...prepared, operation: "foreign" }, { ...prepared, source: "foreign" },
    { ...prepared, root: "foreign" }, { ...prepared, paths: ["foreign.txt"] },
    { ...prepared, approval_digest: "wrong" }, { ...prepared, status: "published" },
    { ...prepared, status: "published", receipt: { phase: "Prepared" } },
  ]) {
    await assert.rejects(approvePiWriteback("original", fields, context, async () => prepared, undefined, {
      prepared: () => {}, observe: async () => observation,
    }), /original prepared selection|actual Applied receipt/);
  }
});

test("headless deadline and cancellation preserve the original prepare without claiming a response acknowledgement", async () => {
  const fields = { source: "child", root: "root", paths: ["selected.txt"], confirm: true };
  const prepared = { ...fields, operation: "original", status: "prepared", approval_digest: "exact" };
  const context = { hasUI: false, sessionManager: SessionManager.inMemory(), ui: { confirm: async () => true } };
  let prepares = 0;
  const prepare = async () => { prepares++; return prepared; };
  const observation = {
    prepared: () => {}, observe: async (_request: unknown, signal: AbortSignal) => {
      await delay(1000, undefined, { signal });
      throw new Error("unreachable");
    }, bounds: { maximumObservations: 2, timeoutMs: 20, intervalMs: 0 },
  };
  const result = await approvePiWriteback("original", fields, context, prepare, undefined, observation);
  assert.equal(result.status, "pending");
  assert.ok(!Object.hasOwn(result, "receipt"));
  assert.deepEqual(result.wait, { bounded: true, observations: 0 });
  const cancellation = new AbortController();
  await assert.rejects(approvePiWriteback("original", fields, context, prepare, cancellation.signal, {
    ...observation, prepared: () => cancellation.abort(),
  }), /abort/i);
  assert.equal(prepares, 2);
});

test("native UI confirms the exact digest through host authority and revalidates context after every await", async () => {
  const fields = { source: "child", root: "root", paths: ["selected.txt"], confirm: true };
  const prepared = { ...fields, operation: "original", status: "prepared", approval_digest: "exact" };
  for (const approved of [false, true]) {
    const requests: Readonly<Record<string, unknown>>[] = [];
    const decisions: { operation: string; digest: string; decision: string }[] = [];
    const context = { hasUI: true, sessionManager: SessionManager.inMemory(), ui: {
      confirm: async (_title: string, text: string) => {
        for (const exact of ["original", "child", "root", "selected.txt", "exact"]) assert.ok(text.includes(exact));
        return approved;
      },
    } };
    const result = await approvePiWriteback("original", fields, context, async request => {
      requests.push(request); return prepared;
    }, undefined, {
      prepared: () => { throw new Error("native UI is not headless"); },
      decide: async (operation, digest, decision) => { decisions.push({ operation, digest, decision }); },
      observe: async () => ({ ...prepared, status: approved ? "published" : "declined",
        ...(approved ? { receipt: { phase: "Applied" } } : {}) }),
    });
    assert.equal(requests.length, 1, "UI never dispatches publication with caller/model digest");
    assert.ok(!Object.hasOwn(requests[0], "approval_digest"));
    assert.deepEqual(decisions, [{ operation: "original", digest: "exact", decision: approved ? "approve" : "decline" }]);
    assert.equal(result.status, approved ? "published" : "declined");
  }
  for (const phase of ["prepare", "observe", "ui", "operator"]) {
    const sessionManager = SessionManager.inMemory();
    const context = { hasUI: phase === "ui" || phase === "operator", sessionManager, ui: {
      confirm: async () => { if (phase === "ui") sessionManager.appendCustomEntry("changed", {}); return true; },
    } };
    let calls = 0;
    await assert.rejects(approvePiWriteback("original", fields, context, async () => {
      calls++;
      if (phase === "prepare" && calls === 1) {
        sessionManager.appendCustomEntry("changed", {});
      }
      return prepared;
    }, undefined, { prepared: () => {},
      decide: async () => { if (phase === "operator") sessionManager.appendCustomEntry("changed", {}); },
      observe: async () => {
        sessionManager.appendCustomEntry("changed", {}); return { ...prepared, status: "declined" };
      },
    }), /session, branch or model changed/);
  }
});

test("normal tool output removes protected root and child proof fields without altering native text", () => {
  const privateResult = {
    session: "root", native_session: "child", caller: { signer: "root-key" },
    operator: { scope: "root-operator" }, operator_scope: "root-scope", operatorKey: "root-key",
    bearer: "root-bearer", authority: "root-authority",
    tasks: [{ task: "original", caller_proof: { scope: "secret" }, taskLease: "secret" }],
    child: { id: "child", environment: { ACYCLIC_PI_CALLER: "secret" }, signer: "child-key" },
    result: { text: "opaque native result", runtime: { environment: "secret" } },
  };
  assert.deepEqual(visibleControlResult(privateResult), {
    session: "root", native_session: "child", tasks: [{ task: "original" }],
    child: { id: "child" }, result: { text: "opaque native result" },
  });
  assert.ok(privateResult.caller.signer, "redaction must not mutate retained private routing");
});

test("native wait passively observes one original operation/task until an actual outcome", async () => {
  const original = Object.freeze({ session: "root", native_session: "caller", operation: "wait-original", task: "child" });
  const requests: typeof original[] = [];
  const outcome = Object.freeze({ originalHostOutcome: "opaque terminal payload" });
  const result = await observePiOperation(original.operation, async signal => {
    signal.throwIfAborted();
    requests.push(original);
    return requests.length < 3 ? { ...original, state: "pending" } : { ...original, state: "completed", outcome };
  }, undefined, { maximumObservations: 4, timeoutMs: 1000, intervalMs: 0 });
  assert.equal(requests.length, 3);
  assert.ok(requests.every(request => request === original));
  assert.equal(result.state, "completed");
  assert.equal(result.outcome, outcome, "native failed/cancelled/succeeded outcome is opaque and never rewritten");
});

test("native wait observation bound keeps pending explicit and does not dispatch replacements", async () => {
  let observations = 0;
  const result = await observePiOperation("wait-original", async () => {
    observations++;
    return { operation: "wait-original", task: "child", state: "pending" };
  }, undefined, { maximumObservations: 2, timeoutMs: 1000, intervalMs: 0 });
  assert.equal(observations, 2);
  assert.equal(result.state, "pending");
  assert.deepEqual(result.wait, { bounded: true, observations: 2 });
  assert.equal(result.operation, "wait-original");
  assert.equal(result.task, "child");
  assert.ok(!Object.hasOwn(result, "outcome"));
});

test("native wait stops immediately on uncertainty or malformed terminal evidence", async () => {
  let observations = 0;
  const uncertain = { state: "pending", uncertain: { operation: "wait-original", task: "child", reason: "lost acknowledgement" } };
  assert.equal(await observePiOperation("wait-original", async () => {
    observations++;
    return uncertain;
  }), uncertain);
  assert.equal(observations, 1);
  await assert.rejects(observePiOperation("wait-original", async () => {
    observations++;
    throw new PiControlAcknowledgementError("wait-original");
  }), /uncertain/);
  assert.equal(observations, 2, "a lost observation acknowledgement is never retried");
  await assert.rejects(observePiOperation("wait-original",
    async () => ({ state: "completed", task: "child" })), /omitted its actual terminal outcome/);
});

test("native wait honors the SDK abort signal and finite elapsed deadline", async () => {
  const cancellation = new AbortController();
  let observations = 0;
  await assert.rejects(observePiOperation("wait-original", async () => {
    observations++;
    cancellation.abort(new Error("native wait cancelled"));
    return { state: "pending", task: "child" };
  }, cancellation.signal), /native wait cancelled/);
  assert.equal(observations, 1);
  const bounded = await observePiOperation("wait-original", async signal => {
    observations++;
    if (observations > 2) await delay(10_000, undefined, { signal });
    return { state: "pending", task: "child", operation: "wait-original" };
  }, undefined, { maximumObservations: 120, timeoutMs: 25, intervalMs: 0 });
  assert.equal(bounded.state, "pending");
  assert.equal(bounded.operation, "wait-original");
  assert.equal(observations, 3);
  assert.deepEqual(bounded.wait, { bounded: true, observations: 1 });
});

function persistedNativeSession(directory: string, id: string) {
  const memory = SessionManager.inMemory(directory, { id });
  const file = join(directory, `${id}.jsonl`);
  writeFileSync(file, JSON.stringify(memory.getHeader()) + "\n");
  const session = SessionManager.open(file);
  session.appendMessage({
    ...spawnMessage("native-assistant"),
    content: [{ type: "text", text: "native assistant boundary" }], stopReason: "stop",
  });
  return session;
}

test("native inbox cursor ignores inherited parent task entries and follows only persisted own receipts", () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-inbox-", import.meta.url)));
  try {
    const parent = persistedNativeSession(directory, "parent");
    parent.appendCustomMessageEntry("acyclic_mail", "parent mailbox", true,
      { task_id: "parent-task", sequence: 99, message_id: "parent-message", kind: "message" });
    assert.equal(persistedPiInboxCursor(parent, "parent-task"), 99);
    parent.appendMessage(spawnMessage("inherit-native-context"));
    const native = nativeChildSession(parent, directory, captureParentCut(parent, "inherit-native-context", 100, 100_000));
    const file = join(directory, "child.jsonl");
    writeFileSync(file, native.session_bytes);
    const child = SessionManager.open(file);
    assert.equal(persistedPiInboxCursor(child, "child-task"), 0);
    const before = readFileSync(file, "utf8");
    child.appendCustomMessageEntry("acyclic_mail", "actual child steering", true,
      { task_id: "child-task", sequence: 1, message_id: "child-message", kind: "steer" });
    const accepted = readFileSync(file, "utf8");
    writeFileSync(file, before);
    assert.equal(persistedPiInboxCursor(child, "child-task"), 0, "SDK memory acceptance alone is not a persisted delivery receipt");
    writeFileSync(file, accepted);
    assert.equal(persistedPiInboxCursor(child, "child-task"), 1);
    assert.equal(persistedPiInboxCursor(parent, "parent-task"), 99);
  } finally {
    rmSync(directory, { recursive: true });
  }
});

test("native inbox queue acceptance never advances its service cursor or repeats a queued item", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-inbox-queue-", import.meta.url)));
  try {
    const session = persistedNativeSession(directory, "native");
    const pending: { item?: PiInboxMessage } = {};
    const requested: number[] = [];
    const queued: unknown[] = [];
    await consumePiInbox({
      sendMessage(message, options) { queued.push({ message, options }); },
    }, { sessionManager: session }, "own-task", async after => {
      requested.push(after);
      return {
        item: { task_id: "own-task", sequence: 1, message_id: "original-message", payload: { originalFile: true } },
        envelope: { kind: "steer", message: "native steering instruction" },
      };
    }, AbortSignal.timeout(40), pending);
    assert.deepEqual(requested, [0]);
    assert.equal(queued.length, 1);
    assert.deepEqual(queued[0], {
      message: {
        customType: "acyclic_mail", content: "native steering instruction", display: true,
        details: { task_id: "own-task", sequence: 1, message_id: "original-message", kind: "steer" },
      },
      options: { triggerTurn: true, deliverAs: "steer" },
    });
    assert.equal(persistedPiInboxCursor(session, "own-task"), 0);
    assert.equal(pending.item?.sequence, 1);
  } finally {
    rmSync(directory, { recursive: true });
  }
});

test("native inbox never retries lost acknowledgement or delivers a foreign task item", async () => {
  const session = SessionManager.inMemory(process.cwd(), { id: "native" });
  let receives = 0;
  let deliveries = 0;
  const pi = { sendMessage() { deliveries++; } };
  await assert.rejects(consumePiInbox(pi, { sessionManager: session }, "own-task", async () => {
    receives++;
    throw new PiControlAcknowledgementError();
  }, new AbortController().signal, {}), /uncertain/);
  assert.equal(receives, 1);
  await assert.rejects(consumePiInbox(pi, { sessionManager: session }, "own-task", async () => {
    receives++;
    return { item: { task_id: "foreign-task", sequence: 1, message_id: "forged" }, envelope: { kind: "message", message: "forged" } };
  }, new AbortController().signal, {}), /another task/);
  assert.equal(receives, 2);
  assert.equal(deliveries, 0);
});
