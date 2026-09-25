import { expect, test } from "bun:test";
import { composeContentBindings, DEFAULT_LIMITS,
  descriptorFor, NativeContracts, projectModelFile, verifiedContentResolver, selectModelContext,
  type AgentId, type Attachment, type ConversationMessage, type ConversationMessageId, type FileRef, type ProjectableConversation } from "../src/index.js";

const contracts = await NativeContracts.create();
const agent = "01010101-0101-0101-0101-010101010101" as AgentId;
const fixtureId = (value: string): ConversationMessageId => value as ConversationMessageId;
const id = fixtureId("03030303-0303-0303-0303-030303030303");
const conversationState = (...messages: ConversationMessage[]): ProjectableConversation =>
  Object.freeze({ agent, revision: BigInt(messages.length), messages: Object.freeze(messages) });

async function reference(path: string, bytes: Uint8Array, mediaType: string): Promise<FileRef<"agent_private", "filesystem">> {
  return contracts.validate("file_ref", {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "scratch", class: "agent_private", owner: { kind: "agent", id: agent } },
    path, version: "pinned-generation", descriptor: await descriptorFor(bytes, mediaType), display_name: path.split("/").at(-1) ?? path,
  });
}

test("verified resolver routes exact owner volumes through one content binding", async () => {
  const first = await reference("messages/first.txt", new TextEncoder().encode("first"), "text/plain");
  const second = contracts.validate("file_ref", { ...first, descriptor: await descriptorFor(new TextEncoder().encode("second"), "text/plain"), volume: {
    ...first.volume, id: "other-agent", owner: { kind: "agent", id: "02020202-0202-0202-0202-020202020202" as AgentId },
  } });
  const binding = (value: string) => ({
    validate: (file: FileRef) => { contracts.validate("file_ref", file); },
    verify: (_file: FileRef, bytes: Uint8Array) => {
      if (new TextDecoder().decode(bytes) !== value) throw new TypeError("file digest mismatch");
    },
    read: async () => new TextEncoder().encode(value),
    fileReadCapability: () => "file:read",
    volumeReadCapability: () => "volume:read",
    directoryReadCapability: () => "directory:read",
  });
  const resolve = verifiedContentResolver(composeContentBindings(contracts, [
    { volume: first.volume, content: binding("first") },
    { volume: second.volume, content: binding("second") },
  ]), DEFAULT_LIMITS);
  expect(new TextDecoder().decode(await resolve(first))).toBe("first");
  expect(new TextDecoder().decode(await resolve(second))).toBe("second");
  const mutable = binding("first");
  const pinned = verifiedContentResolver(mutable, DEFAULT_LIMITS);
  mutable.read = async () => new TextEncoder().encode("replacement");
  mutable.verify = () => { throw new Error("validator swapped"); };
  expect(new TextDecoder().decode(await pinned(first))).toBe("first");
  const forged = binding("second");
  await expect(verifiedContentResolver(composeContentBindings(contracts, [
    { volume: second.volume, content: { ...forged, read: async () => new TextEncoder().encode("forged") } },
  ]), DEFAULT_LIMITS)(second)).rejects.toThrow("file digest mismatch");
  expect(() => composeContentBindings(contracts, [
    { volume: first.volume, content: binding("first") },
    { volume: first.volume, content: binding("first") },
  ])).toThrow("registered twice");
  await expect(verifiedContentResolver(composeContentBindings(contracts, [
    { volume: first.volume, content: binding("first") },
  ]), DEFAULT_LIMITS)(second)).rejects.toThrow("not registered");
  await expect(verifiedContentResolver({
    validate: () => {}, verify: () => {}, read: async () => new TextEncoder().encode("forged"),
  }, DEFAULT_LIMITS)(first)).rejects.toThrow();
  await expect(projectModelFile({ kind: "file", file: first, policy: "bounded_full" }, {
    resolveFile: async () => new TextEncoder().encode("forged"), verifyFile: () => {},
  })).rejects.toThrow();
  let reads = 0;
  const untrusted = verifiedContentResolver({
    validate: () => {}, verify: () => {},
    read: async () => { reads++; return new Uint8Array(); },
  }, DEFAULT_LIMITS);
  await expect(untrusted({ ...first, path: "../escape" })).rejects.toThrow();
  expect(reads).toBe(0);
  await expect(projectModelFile({ kind: "file", file: { ...first, path: "../escape" }, policy: "reference" }, {}))
    .rejects.toThrow();
});

test("explicit model selection resolves complete manifest-backed attachments beyond inline limit", async () => {
  const content = await reference("messages/one.txt", new TextEncoder().encode("hello"), "text/plain");
  const image = await reference("attachments/image.png", new Uint8Array([137, 80, 78, 71]), "image/png");
  const items: Attachment[] = Array.from({ length: 130 }, () => ({ file: image, label: null }));
  const manifestBytes = new TextEncoder().encode(JSON.stringify(items));
  const manifest = await reference("manifests/attachments.json", manifestBytes, "application/vnd.acyclic.harness.attachments+json");
  const message: ConversationMessage = {
    id, sequence: 1n, kind: "user", content,
    attachments: { kind: "manifest", manifest, item_count: items.length },
    reply_to: null, tool_call_id: null, extensions: {},
  };
  const state = conversationState(message);
  const options = { resolveManifest: async (_: FileRef) => manifestBytes, maxAttachments: 130 };
  const projected = await selectModelContext(state, { conversationRevision: 1n, messageIds: [id] }, options);
  expect(projected.messages).toHaveLength(1);
  const parts = projected.messages[0]?.content;
  expect(Array.isArray(parts) ? parts.length : 0).toBe(131);
  expect(Array.isArray(parts) ? parts[0] : null).toEqual({ kind: "file", file: content, policy: "bounded_full" });
  expect(Array.isArray(parts) ? parts[1] : null).toEqual({ kind: "file", file: image, policy: "native" });
  await expect(selectModelContext(state, { conversationRevision: 1n, messageIds: [id] }, {
    ...options, decodeManifest: async () => [{ file: content, label: null }],
  })).rejects.toThrow("disagrees with canonical manifest");
  await expect(selectModelContext(state, { conversationRevision: 0n, messageIds: [id] }, options)).rejects.toThrow("stale");
  await expect(selectModelContext(state, { conversationRevision: 1n, messageIds: [id, id] }, options)).rejects.toThrow("ordered and unique");
  let oversizedManifestReads = 0;
  await expect(selectModelContext(state, { conversationRevision: 1n, messageIds: [id] }, {
    maxAttachments: 129,
    resolveManifest: async () => { oversizedManifestReads++; return manifestBytes; },
  })).rejects.toThrow("attachment projection limit");
  expect(oversizedManifestReads).toBe(0);
  await expect(selectModelContext(state, { conversationRevision: 1n, messageIds: [id] }, { resolveManifest: async () => new Uint8Array([0]) })).rejects.toThrow("file content does not match its descriptor");
  const noncanonicalBytes = new TextEncoder().encode(JSON.stringify(items, null, 2));
  const noncanonicalManifest = await reference("manifests/noncanonical.json", noncanonicalBytes, "application/vnd.acyclic.harness.attachments+json");
  const noncanonicalState = conversationState({
    ...message, attachments: { kind: "manifest", manifest: noncanonicalManifest, item_count: items.length },
  });
  await expect(selectModelContext(noncanonicalState, { conversationRevision: 1n, messageIds: [id] }, {
    resolveManifest: async () => noncanonicalBytes, maxAttachments: 130,
  })).rejects.toThrow("not canonical");
  const reorderedBytes = new TextEncoder().encode(JSON.stringify(items.map(item => ({
    label: item.label, file: item.file,
  }))));
  const reorderedManifest = await reference("manifests/reordered.json", reorderedBytes,
    "application/vnd.acyclic.harness.attachments+json");
  const reorderedState = conversationState({
    ...message, attachments: { kind: "manifest", manifest: reorderedManifest, item_count: items.length },
  });
  await expect(selectModelContext(reorderedState, { conversationRevision: 1n, messageIds: [id] }, {
    resolveManifest: async () => reorderedBytes, maxAttachments: 130,
  })).rejects.toThrow("not canonical");
});

test("tool projection links each result to its exact call, even when provider call IDs repeat", async () => {
  const encode = (value: unknown) => new TextEncoder().encode(JSON.stringify(value));
  const bytes = new Map<string, Uint8Array>();
  const artifact = async (path: string, value: unknown) => {
    const body = encode(value);
    bytes.set(path, body);
    return reference(path, body, "application/json");
  };
  const callIds = [fixtureId("10101010-1010-1010-1010-101010101010"), fixtureId("20202020-2020-2020-2020-202020202020")];
  const resultIds = [fixtureId("11111111-1111-1111-1111-111111111111"), fixtureId("22222222-2222-2222-2222-222222222222")];
  let state = conversationState();
  for (let index = 0; index < 2; index++) {
    const call = await artifact(`tool/call-${index}.json`, { call_id: "reused", name: `tool-${index}`, arguments: { index } });
    const result = await artifact(`tool/result-${index}.json`, { value: { index, full: "x".repeat(1024) } });
    const projection = await artifact(`tool/projection-${index}.json`, { index });
    state = conversationState(...state.messages, { id: callIds[index]!, sequence: BigInt(index * 2 + 1),
      kind: "tool_call", content: call, attachments: { kind: "inline", items: [] },
      reply_to: null, tool_call_id: "reused", extensions: {} });
    state = conversationState(...state.messages, { id: resultIds[index]!, sequence: BigInt(index * 2 + 2),
      kind: "tool_result", content: result,
      attachments: { kind: "inline", items: [{ file: projection, label: "model_projection" }] },
      reply_to: callIds[index]!, tool_call_id: "reused", extensions: {} });
  }
  const selection = { conversationRevision: 4n, messageIds: [callIds[0]!, resultIds[0]!, callIds[1]!, resultIds[1]!] };
  const options = { resolveManifest: async () => new Uint8Array(),
    resolveFile: async (file: FileRef) => bytes.get(file.path) ?? new Uint8Array(), maxRenderBytes: 256 };
  const projected = await selectModelContext(state, selection, options);
  expect(projected.messages[1]?.content).toEqual({ kind: "tool_result", callId: "reused", name: "tool-0", value: { index: 0 } });
  expect(projected.messages[3]?.content).toEqual({ kind: "tool_result", callId: "reused", name: "tool-1", value: { index: 1 } });
  await expect(selectModelContext(state, { ...selection, messageIds: [resultIds[0]!] }, options))
    .rejects.toThrow("lacks its call");
  const sourceResult = state.messages[3]!;
  if (sourceResult.kind !== "tool_result") throw new TypeError("fixture has no tool result");
  const mismatched = conversationState(...state.messages, { ...sourceResult,
    id: fixtureId("33333333-3333-3333-3333-333333333333"), sequence: 5n,
    reply_to: callIds[1]!, tool_call_id: "other" });
  await expect(selectModelContext(mismatched, { conversationRevision: 5n,
    messageIds: [...selection.messageIds, fixtureId("33333333-3333-3333-3333-333333333333")] }, options))
    .rejects.toThrow("lacks its call");
});

test("large primary text stays a reference while a primary image stays native", async () => {
  const text = await reference("messages/large.txt", new TextEncoder().encode("longer than eight"), "text/plain");
  const image = await reference("messages/photo.png", new Uint8Array([137, 80, 78, 71]), "image/png");
  const empty = { kind: "inline" as const, items: [] };
  let state = conversationState();
  state = conversationState(...state.messages, { id, sequence: 1n, kind: "user", content: text,
    attachments: empty, reply_to: null, tool_call_id: null, extensions: {} });
  state = conversationState(...state.messages, { id: fixtureId("04040404-0404-0404-0404-040404040404"), sequence: 2n,
    kind: "user", content: image, attachments: empty, reply_to: null, tool_call_id: null, extensions: {} });
  const projected = await selectModelContext(state, { conversationRevision: 2n,
    messageIds: [id, fixtureId("04040404-0404-0404-0404-040404040404")] }, { maxRenderBytes: 8 });
  expect(projected.messages[0]?.content).toEqual([{ kind: "file", file: text, policy: "reference" }]);
  expect(projected.messages[1]?.content).toEqual([{ kind: "file", file: image, policy: "native" }]);
});

test("a compacted remote view uses its authoritative revision, not its loaded message count", async () => {
  const content = await reference("messages/retained.txt", new TextEncoder().encode("retained"), "text/plain");
  const view: ProjectableConversation = {
    agent, revision: (1n << 53n) + 7n,
    messages: [{ id, sequence: (1n << 53n) + 6n, kind: "user", content,
      attachments: { kind: "inline", items: [] }, reply_to: null, tool_call_id: null, extensions: {} }],
  };
  const selected = await selectModelContext(view, { conversationRevision: view.revision,
    messageIds: [id] }, {});
  expect(selected.selection.conversationRevision).toBe(view.revision);
  await expect(selectModelContext(view, { conversationRevision: view.revision - 1n,
    messageIds: [id] }, {})).rejects.toThrow("stale");
});
