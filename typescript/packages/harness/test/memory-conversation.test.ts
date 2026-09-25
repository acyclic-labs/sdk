import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { ExecutionScope, GroupPolicies, Harness, MemoryConversation, NativeContracts, TaskDefinition, composeContentBindings,
  defineTool, descriptorFor, type AgentId, type FileRef, type OperationId } from "../src/index.js";

const wasm = readFileSync(fileURLToPath(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)));
const contracts = await NativeContracts.create();
const agent = "08080808-0808-0808-0808-080808080808" as AgentId;

test("typed tasks use owner-bound content grants and stable upload identities", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  await expect(host.stage(".system/forged.txt", new Uint8Array([1]), "text/plain", "forged.txt"))
    .rejects.toThrow("reserved");
  const content = host.contentBindings();
  const task = TaskDefinition.live<void, string>("content_task", "1", async context => {
    const file = await context.stageFile("upload-1", "task/output.txt", new TextEncoder().encode("owned"), "text/plain", "output.txt");
    await expect(context.stageFile("upload-1", "task/sibling.txt", new TextEncoder().encode("other"), "text/plain", "sibling.txt"))
      .rejects.toThrow("another file");
    await expect(context.stageFile("upload-1", "task/output.txt", new TextEncoder().encode("changed"), "text/plain", "output.txt"))
      .rejects.toThrow("another file");
    return new TextDecoder().decode(await context.readFile(file));
  }, { requirements: ["content:write"] });
  expect(() => Harness.builder(contracts).task(task).build()).toThrow("unsatisfied task requirement");
  const runtime = Harness.builder(contracts).content(content)
    .grant(content.volumeReadCapability(host.volume), content.writer!.writeCapability())
    .task(task).build();
  expect(await runtime.spawn(task, undefined).result()).toEqual({ kind: "succeeded", value: "owned" });
  expect(await runtime.scoped(ExecutionScope.create().onlyGrants()).spawn(task, undefined).result())
    .toMatchObject({ kind: "failed" });
  host.free();
});

test("resident file versions pin display name as well as bytes and media type", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  const file = await host.stage("notes/pinned.txt", new TextEncoder().encode("same"), "text/plain", "pinned.txt");
  expect(Object.isFrozen(file)).toBe(true);
  expect(Object.isFrozen(file.volume)).toBe(true);
  expect(Object.isFrozen(file.descriptor.sha256)).toBe(true);
  expect(Object.isFrozen(host.volume)).toBe(true);
  const exposedScope = host.scope;
  const originalProofByte = host.scope.proof[0];
  (exposedScope.proof as number[])[0] = (originalProofByte ?? 0) ^ 1;
  expect(host.scope.proof[0]).toBe(originalProofByte);
  const forged = { ...file, display_name: "renamed.txt" };
  await expect(host.read(forged)).rejects.toThrow("authorized owning-provider resolver");
  expect(() => host.delegateFileRead("11111111-1111-1111-1111-111111111111" as AgentId,
    "forged", forged)).toThrow("nonresident");
  const renamed = await host.stage("notes/pinned.txt", new TextEncoder().encode("same"), "text/plain", "renamed.txt");
  expect(renamed.version).not.toBe(file.version);
  expect(new TextDecoder().decode(await host.read(file))).toBe("same");
  expect(new TextDecoder().decode(await host.read(renamed))).toBe("same");
  host.free();
});

test("an attached reader can use a private-root directory grant without a write grant", async () => {
  const ownerAgent = "09090909-0909-0909-0909-090909090909" as AgentId;
  const owner = await MemoryConversation.create({ agent: ownerAgent, wasm });
  const foreign = await owner.stage("notes/nested/shared.txt", new TextEncoder().encode("owner bytes"),
    "text/plain", "shared.txt");
  const delegated = owner.delegateDirectoryRead(agent, "read-owner-root", "");
  await expect(owner.readAuthorized({ ...delegated, capabilities: [
    ...delegated.capabilities, "forged:write",
  ] }, foreign)).rejects.toThrow();
  const host = await MemoryConversation.create({ agent, wasm });
  const ownerBinding = owner.readBindings(delegated);
  const content = composeContentBindings(contracts, [
    { volume: host.volume, content: host.contentBindings() },
    { volume: owner.volume, content: ownerBinding },
  ]);
  (delegated.proof as number[])[0] = (delegated.proof[0] ?? 0) ^ 1;
  const task = TaskDefinition.live<void, string>("read_attached", "1", async context => {
    expect("harness" in context).toBe(false);
    expect("harness" in context.group(GroupPolicies.collectAll)).toBe(false);
    return new TextDecoder().decode(await context.readFile(foreign));
  });
  const blocked = Harness.builder(contracts).content(content).task(task).build();
  expect(await blocked.spawn(task, undefined).result()).toMatchObject({
    kind: "failed", error: { message: expect.stringContaining("cannot read this file") },
  });
  const runtime = Harness.builder(contracts).content(content)
    .grant(content.directoryReadCapability(foreign.volume, ""))
    .task(task).build();
  expect(await runtime.spawn(task, undefined).result()).toEqual({ kind: "succeeded", value: "owner bytes" });
  expect(content.writer?.volume).toEqual(host.volume);
  expect(runtime.scope.grants).not.toContain(content.writer!.writeCapability());
  host.free();
  owner.free();
});

test("local conversation publishes staged refs and pinned context before model dispatch", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  let calls = 0;
  const runtime = Harness.builder(contracts).model({
    async *generate() {
      calls += 1;
      expect(host.snapshot().events.some(event => typeof event === "object" && event !== null
        && "payload" in event && typeof event.payload === "object"
        && event.payload !== null && "kind" in event.payload
        && event.payload.kind === "model_context_selected")).toBe(true);
      yield { kind: "content" as const, delta: "answer" };
      yield { kind: "completed" as const, metadata: {} };
    },
    async reconcile() { return undefined; },
  }).build();
  const operation = "01010101-0101-0101-0101-010101010101" as OperationId;
  const content = await host.stage("turns/one/user.txt", new TextEncoder().encode("question"), "text/plain", "user.txt");
  const first = await host.runConversation(runtime, operation, content);
  const replay = await host.runConversation(runtime, operation, content);
  expect(first.text).toBe("answer");
  expect(replay).toEqual(first);
  expect(calls).toBe(1);
  const history = host.conversation();
  expect(history.agent).toBe(agent);
  expect(history.messages.map(message => message.kind)).toEqual(["user", "assistant"]);
  expect(typeof history.messages[0]!.sequence).toBe("bigint");
  expect(typeof history.messages[1]!.content.descriptor.byte_length).toBe("number");
  const eventJson = JSON.stringify(host.snapshot().events,
    (_key, value: unknown) => typeof value === "bigint" ? value.toString() : value);
  expect(eventJson).not.toContain("question");
  expect(eventJson).not.toContain("answer");
  expect(new TextDecoder().decode(await host.read(history.messages[1]!.content))).toBe("answer");
  host.free();
});

test("large attachment lists are manifest-backed and changed retry inputs are rejected", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  const runtime = Harness.builder(contracts).model({
    async *generate() { yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  const operation = "02020202-0202-0202-0202-020202020202" as OperationId;
  const content = await host.stage("turns/two/user.txt", new TextEncoder().encode("question"), "text/plain", "user.txt");
  const attachment = await host.stage("files/one.txt", new TextEncoder().encode("file"), "text/plain", "one.txt");
  const items = Array.from({ length: 129 }, () => ({ file: attachment, label: null }));
  await host.runConversation(runtime, operation, content, items);
  expect(host.conversation().messages[0]!.attachments.kind).toBe("manifest");
  await expect(host.runConversation(runtime, operation, content, [])).rejects.toThrow();
  host.free();
});

test("local conversation retains exact tool call and full result artifact", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  let step = 0;
  const tool = defineTool<number, string>({ name: "echo", revision: "1", description: "echo",
    inputSchema: {}, outputSchema: {},
    parseInput(value) { if (typeof value !== "number") throw new TypeError("expected number"); return value; },
    parseOutput(value) { if (typeof value !== "string") throw new TypeError("expected string"); return value; },
  }, async (_, value) => `value:${value}`);
  const runtime = Harness.builder(contracts).tool(tool).grant("tool:call:echo").model({
    async *generate() {
      if (step++ === 0) {
        yield { kind: "tool_call" as const, callId: "call-1", name: "echo", arguments: 7 };
        yield { kind: "completed" as const, metadata: {} };
      } else {
        yield { kind: "content" as const, delta: "done" };
        yield { kind: "completed" as const, metadata: {} };
      }
    },
    async reconcile() { return undefined; },
  }).build();
  await host.runPrompt(runtime, "start");
  const messages = host.conversation().messages;
  expect(messages.map(message => message.kind)).toEqual(["user", "tool_call", "tool_result", "assistant"]);
  expect(messages[2]!.reply_to).toBe(messages[1]!.id);
  expect(messages[2]!.tool_call_id).toBe("call-1");
  expect(JSON.parse(new TextDecoder().decode(await host.read(messages[2]!.content)))).toEqual({ value: "value:7" });
  host.free();
});

test("large canonical attachment lists produce a bounded model request", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  let projectedParts = 0;
  let omission = "";
  const runtime = Harness.builder(contracts).model({
    async *generate(request) {
      const content = request.messages.at(-1)?.content;
      if (!Array.isArray(content)) throw new Error("expected selected file parts");
      projectedParts = content.length;
      const last = content.at(-1);
      if (last?.kind === "text") omission = last.text;
      yield { kind: "completed" as const, metadata: {} };
    },
    async reconcile() { return undefined; },
  }).build();
  const operation = "03030303-0303-0303-0303-030303030303" as OperationId;
  const content = await host.stage("turns/three/user.txt", new TextEncoder().encode("inspect"), "text/plain", "user.txt");
  const attachment = await host.stage("files/one.txt", new TextEncoder().encode("file"), "text/plain", "one.txt");
  await host.runConversation(runtime, operation, content,
    Array.from({ length: 1_030 }, () => ({ file: attachment, label: null })));
  expect(projectedParts).toBe(1_024);
  expect(omission).toContain("8 additional attachments omitted");
  expect(host.conversation().messages[0]!.attachments.kind).toBe("manifest");
  host.free();
});

test("concurrent retries serialize before model dispatch", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  let calls = 0;
  const runtime = Harness.builder(contracts).model({
    async *generate() {
      calls += 1;
      await new Promise(resolve => setTimeout(resolve, 1));
      yield { kind: "completed" as const, metadata: {} };
    },
    async reconcile() { return undefined; },
  }).build();
  const operation = "04040404-0404-0404-0404-040404040404" as OperationId;
  const content = await host.stage("turns/four/user.txt", new TextEncoder().encode("question"), "text/plain", "user.txt");
  const outputs = await Promise.all([
    host.runConversation(runtime, operation, content),
    host.runConversation(runtime, operation, content),
  ]);
  expect(calls).toBe(1);
  expect(outputs[0]).toEqual(outputs[1]);
  expect(host.conversation().messages).toHaveLength(2);
  host.free();
});

test("different local turns serialize and inherit the prior committed assistant", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  const seen: number[] = [];
  const runtime = Harness.builder(contracts).model({
    async *generate(request) {
      seen.push(request.messages.length);
      yield { kind: "content" as const, delta: "ok" };
      yield { kind: "completed" as const, metadata: {} };
    },
    async reconcile() { return undefined; },
  }).build();
  await Promise.all([host.runPrompt(runtime, "first"), host.runPrompt(runtime, "second")]);
  expect(seen).toEqual([1, 3]);
  expect(host.conversation().messages.map(message => message.kind)).toEqual(["user", "assistant", "user", "assistant"]);
  host.free();
});

test("oversized attachment manifests never reach canonical admission", async () => {
  const host = await MemoryConversation.create({ agent, wasm });
  const runtime = Harness.builder(contracts).limits({ file_bytes: 128, render_bytes: 128 }).model({
    async *generate() { throw new Error("model must not dispatch"); },
    async reconcile() { return undefined; },
  }).build();
  const operation = "05050505-0505-0505-0505-050505050505" as OperationId;
  const content = await host.stage("turns/five/user.txt", new TextEncoder().encode("question"), "text/plain", "user.txt");
  const attachment = await host.stage("files/one.txt", new TextEncoder().encode("file"), "text/plain", "one.txt");
  await expect(host.runConversation(runtime, operation, content,
    Array.from({ length: 129 }, () => ({ file: attachment, label: null })))).rejects.toThrow("exceeds harness limits");
  expect(host.conversation().messages).toHaveLength(0);
  host.free();
});

test("foreign references require their owning provider to grant the reader", async () => {
  const bytes = new TextEncoder().encode("shared by its owner");
  const file = {
    volume: { provider: { namespace: "other", family: "filesystem", version: "2" },
      id: "private", class: "agent_private" as const,
      owner: { kind: "agent" as const, id: "09090909-0909-0909-0909-090909090909" as AgentId } },
    path: "notes/shared.txt", version: "generation-1",
    descriptor: await descriptorFor(bytes, "text/plain"), display_name: "shared.txt",
  };
  let authorized = false;
  const host = await MemoryConversation.create({ agent, wasm });
  const reader = { async read(reference: FileRef) {
    expect(reference).toEqual(file);
    if (!authorized) throw new TypeError("owner denied read");
    return bytes;
  } };
  host.attachReadVolume(file.volume, reader);
  reader.read = async () => bytes;
  const runtime = Harness.builder(contracts).model({
    async *generate() { yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  const operation = "06060606-0606-0606-0606-060606060606" as OperationId;
  await expect(host.runConversation(runtime, operation, file)).rejects.toThrow("owner denied read");
  expect(host.conversation().messages).toHaveLength(0);
  authorized = true;
  await host.runConversation(runtime, operation, file);
  expect(host.conversation().messages[0]!.content).toEqual(file);
  host.free();
});

test("a foreign provider cannot admit bytes that disagree with its pinned ref", async () => {
  const bytes = new TextEncoder().encode("correct");
  const file = {
    volume: { provider: { namespace: "other", family: "filesystem", version: "2" },
      id: "shared", class: "session_shared" as const,
      owner: { kind: "session" as const, id: "session" } },
    path: "notes/readme.txt", version: "generation-1",
    descriptor: await descriptorFor(bytes, "text/plain"), display_name: "readme.txt",
  };
  const host = await MemoryConversation.create({ agent, wasm });
  host.attachReadVolume(file.volume, {
    async read() { return new TextEncoder().encode("tampered"); },
  });
  const runtime = Harness.builder(contracts).model({
    async *generate() { throw new Error("model must not dispatch"); },
    async reconcile() { return undefined; },
  }).build();
  const operation = "07070707-0707-0707-0707-070707070707" as OperationId;
  await expect(host.runConversation(runtime, operation, file)).rejects.toThrow();
  expect(host.conversation().messages).toHaveLength(0);
  host.free();
});
