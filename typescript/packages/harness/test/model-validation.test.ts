import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  DEFAULT_LIMITS, jsonToolProjection, Harness, NativeContracts, descriptorFor, type AgentId, type FileRef, type PinnedContextPath,
} from "../src/index.js";
import {
  prepareModelRequest, encodeModelPrefix, WasmReducer, validateModelContent, validateUserInput,
} from "../generated/wasm/acyclic_harness_wasm.js";
import * as harnessWasm from "../generated/wasm/acyclic_harness_wasm.js";
import { publicModelContent } from "../src/projection.js";
import type { NativeModelContent } from "../src/native-contracts.js";
import type { WasmModelRequestWire } from "../generated/wasm/acyclic_harness_wasm.js";
import initWasm from "../generated/wasm/acyclic_harness_wasm.js";
import { assertHarnessWasmExports, ensureHarnessWasm } from "../src/wasm-runtime.js";

const contracts = await NativeContracts.create();
const jsonProjection = await jsonToolProjection({});
const rawWasmExports = await initWasm();
const agent = "07070707-0707-0707-0707-070707070707" as AgentId;

test("generated skill parser and immutable projection share Rust validation and bounds", () => {
  const provider = { namespace: "discovery", family: "filesystem", version: "2" };
  const source: PinnedContextPath = {
    root: { volume: { provider, id: "skills", class: "session_shared", owner: { kind: "session", id: "test" } }, directory: "" },
    generation: { kind: "generation", provider, key: [1], version: "pinned" }, path: "inspect/SKILL.md",
  };
  const prefix = new TextEncoder().encode("---\r\nname: inspect\r\ndescription: >-\r\n  Inspect selected\r\n  files\r\nmetadata: {category: review}\r\n---\r\nSECRET BODY");
  const skill = contracts.parseSkillMetadata(prefix, source);
  expect(skill.description).toBe("Inspect selected files");
  expect(skill.fields).toEqual({ metadata: { category: "review" } });
  expect(skill.source).toEqual(source);
  expect(Object.isFrozen(skill)).toBe(true);
  const snapshot = { instructions: [], skills: [skill] };
  const base = { messages: [{ role: "user" as const, content: "inspect" }], metadata: {} };
  const projected = contracts.projectDiscoveredContext(snapshot, base, "prepend", DEFAULT_LIMITS);
  expect(base.messages).toHaveLength(1);
  expect(projected.messages).toHaveLength(2);
  expect(JSON.stringify(projected)).toContain("Inspect selected files");
  expect(JSON.stringify(projected)).not.toContain("SECRET BODY");
  expect(() => contracts.projectDiscoveredContext(snapshot, base, "prepend", { ...DEFAULT_LIMITS, render_bytes: 1 })).toThrow();
  expect(() => contracts.projectDiscoveredContext({ ...snapshot, skills: [{ ...skill, name: "Invalid" }] }, base, "prepend", DEFAULT_LIMITS)).toThrow();
  expect(() => contracts.parseSkillMetadata(new TextEncoder().encode("---\nname: inspect\ndescription: &x hidden\nmetadata: *x\n---\n"), source)).toThrow();
  expect(() => contracts.parseSkillMetadata(prefix, { ...source, path: ".system/private" })).toThrow();
  const name = "a".repeat(128);
  const description = "d".repeat(2048);
  const payload = "x".repeat(70000);
  const items = Array(3000).fill("0").join(",");
  const admitted = contracts.parseSkillMetadata(new TextEncoder().encode(
    `---\nname: ${name}\ndescription: ${description}\npayload: ${payload}\nitems: [${items}]\nnested: ${"[".repeat(32)}0${"]".repeat(32)}\n---\n`,
  ), source);
  expect(admitted.name).toBe(name);
  expect(admitted.description).toBe(description);
  expect(admitted.fields.payload).toBe(payload);
  expect(admitted.fields.items).toHaveLength(3000);
  expect(() => contracts.projectDiscoveredContext({ instructions: [], skills: [admitted] },
    base, "prepend", DEFAULT_LIMITS)).not.toThrow();
});

test("generated context selections and updates use native schemas, placement and bounds", () => {
  const selection = { source: { kind: "attribute" as const, attribute: {
    type_name: "example.role", type_revision: "1", state_revision: "2",
    schema: { type: "string" }, value: "reviewer",
  } }, extent: { kind: "whole" as const }, representation: "full" as const };
  contracts.validateContextSelection(selection, DEFAULT_LIMITS);
  expect(() => contracts.validateContextSelection({ ...selection, source: {
    kind: "attribute", attribute: { ...selection.source.attribute, value: 3 },
  } }, DEFAULT_LIMITS)).toThrow();
  expect(() => contracts.validateContextSelection(selection, {
    ...DEFAULT_LIMITS, render_bytes: 1,
  })).toThrow();
  const base = { messages: [{ role: "user" as const, content: "task" }], metadata: {} };
  const rendered = [{ role: "system" as const, content: "reviewer" }];
  const rebuilt = contracts.applyContextProjection(base, rendered, "prompt", "prepend", DEFAULT_LIMITS);
  const update = contracts.applyContextProjection(base, rendered, "update", "prepend", DEFAULT_LIMITS);
  expect(rebuilt.messages[0]).toEqual(update.messages[1]);
  expect(base.messages).toHaveLength(1);
  expect(() => contracts.applyContextProjection(base, rendered, "prompt", "prepend", {
    ...DEFAULT_LIMITS, render_bytes: 1,
  })).toThrow();
});

test("generated context composition preserves existing file wires without reinterpreting metadata", async () => {
  const reference = await file();
  const part = { kind: "file" as const, file: reference, policy: "reference" as const };
  const opaque = Object.fromEntries([["sha256", "note"], ["byte_length", 7n], ["media_type", "note"],
    ["__proto__", { marker: "literal" }]]);
  const base = { messages: [{ role: "user" as const, content: part },
    { role: "user" as const, content: [{ kind: "text" as const, text: "attached" }, part] },
    { role: "assistant" as const, content: { kind: "tool_call" as const, call_id: "call", name: "inspect", arguments: opaque } },
    { role: "tool" as const, content: { kind: "tool_result" as const, call_id: "call", name: "inspect", content: { kind: "json" as const, value: opaque } } }],
  metadata: Object.fromEntries([["attachment", reference], ["__proto__", reference]]) };
  for (const placement of ["prepend", "append"] as const) {
    const projections = [
      contracts.projectDiscoveredContext({ instructions: [reference], skills: [] }, base, placement, DEFAULT_LIMITS),
      contracts.applyContextProjection(base, [{ role: "system", content: part }], "prompt", placement, DEFAULT_LIMITS),
    ];
    for (const projected of projections) {
      expect(projected.metadata).toEqual(base.metadata);
      expect(Object.hasOwn(projected.metadata, "__proto__")).toBe(true);
      const calls = projected.messages.filter(message => !Array.isArray(message.content)
        && typeof message.content !== "string" && message.content.kind === "tool_call");
      expect(calls[0]?.content).toMatchObject({ arguments: opaque });
      const files = projected.messages.flatMap(message => {
        const parts = typeof message.content === "string" ? []
          : Array.isArray(message.content) ? message.content : [message.content];
        return parts.flatMap(part => part.kind === "file" ? [part.file] : []);
      });
      expect(files).toHaveLength(3);
      for (const file of files) expect(typeof file.descriptor.byte_length).toBe("number");
      expect(() => prepareModelRequest({ model: { provider: "mock", name: "files", revision: "1", options: {} },
        messages: projected.messages, tools: [{ name: "inspect", revision: "1", description: "Inspect files",
          inputSchema: {}, outputSchema: {}, projectionSchema: jsonProjection.schema }], maxOutputTokens: 4096 }, DEFAULT_LIMITS)).not.toThrow();
    }
  }
});

test("declared context binding pins metadata and preserves the prior binding on invalid replacement", async () => {
  const provider = { namespace: "discovery", family: "filesystem", version: "2" };
  const source: PinnedContextPath = {
    root: { volume: { provider, id: "skills", class: "session_shared", owner: { kind: "session", id: "test" } }, directory: "" },
    generation: { kind: "generation", provider, key: [1], version: "pinned" }, path: "inspect/SKILL.md",
  };
  const skill = contracts.parseSkillMetadata(new TextEncoder().encode(
    "---\nname: inspect\ndescription: Inspect files\n---\nSECRET BODY"), source);
  const snapshot = { instructions: [], skills: [skill] };
  const admitted: Uint8Array[] = [];
  const builder = Harness.builder(contracts)
    .context({ async build() { return [{ role: "user", content: "custom context" }]; } })
    .declaredContext(snapshot)
    .model({ provider: "mock", name: "discovery", revision: "pinned", options: {} }, {
      async *generate(request) {
        admitted.push(request.serializedInput.slice());
        yield { kind: "completed" as const, metadata: {} };
      }, async reconcile() { return undefined; },
    });
  expect(() => builder.declaredContext({ ...snapshot,
    skills: [{ ...skill, name: "Invalid" }] })).toThrow();
  snapshot.skills = [];
  await (await builder.build()).run("go");
  const first = new TextDecoder().decode(admitted[0]);
  expect(first).toContain("Inspect files");
  expect(first).toContain("custom context");
  expect(first).not.toContain("SECRET BODY");
  builder.context({ async build() { return [{ role: "user", content: "replacement" }]; } });
  await (await builder.build()).run("go");
  const second = new TextDecoder().decode(admitted[1]);
  expect(second).toContain("replacement");
  expect(second).not.toContain("Inspect files");
});

test("native and WASM request construction preserve exact Unicode and paired tool bytes", async () => {
  const fixture = await readFile(new URL("../../../../rust/crates/harness/fixtures/model-request.json", import.meta.url));
  const wire = contracts.decodeModelJson(fixture) as unknown as WasmModelRequestWire;
  const request = {
    model: wire.model,
    messages: wire.messages,
    tools: wire.tools.map((tool) => ({
      name: tool.name, revision: tool.revision, description: tool.description,
      inputSchema: tool.input_schema, outputSchema: tool.output_schema, projectionSchema: tool.projection_schema,
    })),
    maxOutputTokens: wire.max_output_tokens,
  };
  const bytes = prepareModelRequest(request, DEFAULT_LIMITS);
  expect(bytes).toEqual(Uint8Array.from(fixture));
  const prefix = await readFile(new URL("../../../../rust/crates/harness/fixtures/model-prefix.json", import.meta.url));
  expect(encodeModelPrefix(bytes, null, undefined, DEFAULT_LIMITS)).toEqual(Uint8Array.from(prefix));
  expect(() => prepareModelRequest({ ...request, maxOutputTokens: 0 }, DEFAULT_LIMITS)).toThrow();
  expect(() => prepareModelRequest({ ...request, messages: request.messages.slice(0, 2) }, DEFAULT_LIMITS)).toThrow("incomplete");
  expect(() => prepareModelRequest({ ...request, tools: [...request.tools, ...request.tools] }, DEFAULT_LIMITS)).toThrow("repeated");
});

test("the actual task provider receives the admitted serialized input", async () => {
  const prompt = "é\0🦀\r\n";
  let calls = 0;
  const runtime = await Harness.builder(contracts).model({ provider: "mock", name: "exact", revision: "pinned", options: {} }, {
    async *generate(request) {
      calls += 1;
      expect(contracts.decodeModelJson(request.serializedInput)).toEqual({
        model: { provider: "mock", name: "exact", revision: "pinned", options: {} }, messages: [{ role: "user", content: prompt }],
        tools: [], max_output_tokens: null,
      });
      expect(request.messages).toEqual([{ role: "user", content: prompt }]);
      yield { kind: "completed" as const, metadata: {} };
    },
    async reconcile() { return undefined; },
  }).build();
  await runtime.run(prompt);
  expect(calls).toBe(1);
});

test("WASM direct-parent prefixes preserve exact provider bytes across depth three and siblings", async () => {
  const fixture = await readFile(new URL("../../../../rust/crates/harness/fixtures/model-request.json", import.meta.url));
  const root = contracts.decodeModelJson(fixture) as unknown as WasmModelRequestWire;
  const core = new WasmReducer({ kind: "conversation", id: "model-prefix" }, "model-prefix", new Uint8Array(32).fill(19), []);
  const files = new Map<string, Uint8Array>();
  const references: FileRef[] = [];
  const stage = async (bytes: Uint8Array, path: string, mediaType: string): Promise<FileRef> => {
    const reference = contracts.validate("file_ref", {
      volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
        id: "prefix-parent", class: "agent_private", owner: { kind: "agent", id: agent } },
      path, version: `immutable-${path}`, descriptor: await descriptorFor(bytes, mediaType), display_name: path,
    });
    files.set(new TextDecoder().decode(contracts.encodeCanonicalJson(reference)), bytes.slice());
    references.push(reference);
    return reference;
  };
  try {
    const attachment = await stage(new TextEncoder().encode("attachment é\0🦀\r\n"), "attachment.txt", "text/plain");
    const tools = root.tools.map(tool => ({ name: tool.name, revision: tool.revision, description: tool.description,
      inputSchema: tool.input_schema, outputSchema: tool.output_schema, projectionSchema: tool.projection_schema }));
    let parentRequest = prepareModelRequest({ model: root.model, messages: [...root.messages,
      { role: "user", content: [{ kind: "file", file: attachment, policy: "reference" }] }],
      tools, maxOutputTokens: 4096 }, DEFAULT_LIMITS);
    let head = await stage(encodeModelPrefix(parentRequest, null, undefined, DEFAULT_LIMITS), "prefix-0.json",
      "application/vnd.acyclic.harness.model-prefix+json");
    for (let depth = 1; depth <= 3; depth++) {
      const scope = core.issueScopeForAgent(agent, `reader-${depth}`, references.map(ref => core.fileReadCapability(ref)));
      const parentWire = contracts.decodeModelJson(parentRequest) as unknown as WasmModelRequestWire;
      const children = depth === 1 ? ["primary", "sibling"] : ["primary"];
      let primary: Uint8Array | undefined;
      for (const child of children) {
        const prompt = `notification ${depth}; task ${child}; identity ${child}; workspace ${child}; fresh scratch ${child} é\0🦀\r\n`;
        let calls = 0;
        const runtime = await Harness.builder(contracts).inheritedModelPrefix({ core, scope, head, files })
          .model(root.model, { async *generate(request) {
            calls++;
            const expected = prepareModelRequest({ model: root.model, tools,
              messages: [...parentWire.messages, { role: "user", content: prompt }] }, DEFAULT_LIMITS);
            expect(request.serializedInput).toEqual(expected);
            if (child === "primary") primary = request.serializedInput.slice();
            yield { kind: "completed" as const, metadata: {} };
          }, async reconcile() { return undefined; } })
          .tool({ ...tools[0]!, inputSchema: { type: "string" }, outputSchema: { type: "string" },
            parseInput: value => value, projection: jsonProjection, parseOutput: value => value }, {
            async execute(invocation) { return { value: invocation.arguments }; }, async reconcile() { return undefined; },
          }).grant("tool:call:echo").build();
        await runtime.run(prompt);
        expect(calls).toBe(1);
      }
      const denied = core.issueScopeForAgent(agent, "missing-attachment", references.filter(ref => ref !== attachment).map(ref => core.fileReadCapability(ref)));
      await expect(core.prepareInheritedModelRequest(denied, { model: root.model, tools, maxOutputTokens: 4096,
        messages: [{ role: "user", content: "denied" }] }, head, files, DEFAULT_LIMITS)).rejects.toThrow();
      const segment = encodeModelPrefix(primary!, head, parentRequest, DEFAULT_LIMITS);
      expect((contracts.decodeModelJson(segment) as { messages: unknown[] }).messages).toHaveLength(1);
      head = await stage(segment, `prefix-${depth}.json`, "application/vnd.acyclic.harness.model-prefix+json");
      parentRequest = primary!;
    }
  } finally { core.free(); }
});

test("tool results retain their pinned string schema and enforce the exact render bound", async () => {
  for (const length of [38, 39]) {
    const value = "x".repeat(length);
    let calls = 0;
    const runtime = await Harness.builder(contracts).limits({ render_bytes: 64 })
      .model({ provider: "mock", name: "bounded", revision: "1", options: {} }, {
        async *generate(request) {
          calls++;
          if (calls === 1) yield { kind: "tool_call" as const, callId: "read", name: "read", arguments: "go" };
          else {
            expect(request.messages.at(-1)?.content).toEqual({ kind: "tool_result", callId: "read", name: "read", content: { kind: "json", value } });
            expect((contracts.decodeModelJson(request.serializedInput) as unknown as WasmModelRequestWire).messages.at(-1)?.content)
              .toEqual({ kind: "tool_result", call_id: "read", name: "read", content: { kind: "json", value } });
          }
          yield { kind: "completed" as const, metadata: {} };
        }, async reconcile() { return undefined; },
      }).tool({ name: "read", revision: "1", description: "read", inputSchema: { type: "string" },
        outputSchema: { type: "string" }, parseInput: value => value, projection: jsonProjection, parseOutput: value => value,
        handler: async () => value }).grant("tool:call:read").build();
    if (length === 38) {
      const result = await runtime.run("go");
      expect(result.receipts.find(receipt => receipt.kind === "tool")).toMatchObject({ value, projection: { kind: "json", value } });
      expect(calls).toBe(2);
    } else {
      await expect(runtime.run("go")).rejects.toThrow("render limit");
      expect(calls).toBe(1);
    }
  }
});

test("inherited dispatch captures fresh pinned local files after builder construction", async () => {
  const core = new WasmReducer({ kind: "conversation", id: "fresh-prefix" }, "fresh-prefix", new Uint8Array(32).fill(23), []);
  const model = { provider: "mock", name: "fresh", revision: "1", options: {} };
  const volume = contracts.validate("volume_ref", { provider: { namespace: "test", family: "filesystem", version: "2" },
    id: "child-scratch", class: "agent_private", owner: { kind: "agent", id: agent } });
  const resident = new Map<string, Uint8Array>();
  const key = (file: FileRef) => new TextDecoder().decode(contracts.encodeCanonicalJson(file));
  const stage = async (path: string, version: string, bytes: Uint8Array, mediaType: string) => {
    const file = contracts.validate("file_ref", { volume, path, version, descriptor: await descriptorFor(bytes, mediaType), display_name: path });
    resident.set(key(file), bytes.slice());
    return file;
  };
  try {
    const parent = prepareModelRequest({ model, messages: [{ role: "user", content: "parent" }], tools: [], maxOutputTokens: 4096 }, DEFAULT_LIMITS);
    const head = await stage("prefix.json", "prefix-1", encodeModelPrefix(parent, null, undefined, DEFAULT_LIMITS),
      "application/vnd.acyclic.harness.model-prefix+json");
    const files = new Map([[key(head), resident.get(key(head))!]]);
    const volumeGrant = core.volumeCapability(volume, "read");
    const scope = core.issueScopeForAgent(agent, "local-reader", [volumeGrant]);
    let fresh: FileRef;
    let prefixOnly = false;
    let calls = 0;
    let reads = 0;
    const content = {
      validate(file: FileRef) { validateModelContent({ kind: "file", file, policy: "reference" }, DEFAULT_LIMITS); },
      verify(file: FileRef, bytes: Uint8Array) { contracts.verifyFileBytes(file, bytes); },
      async read(file: FileRef) { reads++; return resident.get(key(file))!.slice(); },
      fileReadCapability: (file: FileRef) => core.fileReadCapability(file),
      volumeReadCapability: (volume: FileRef["volume"]) => core.volumeCapability(volume, "read"),
      directoryReadCapability: (volume: FileRef["volume"], prefix: string) => core.directoryReadCapability(volume, prefix),
    };
    const context = { async build() { return prefixOnly ? []
      : [{ role: "user" as const, content: { kind: "file" as const, file: fresh, policy: "reference" as const } }]; } };
    const provider = { async *generate(request: import("../src/model.js").ModelRequest & { serializedInput: Uint8Array }) {
      calls++;
      expect(request.maxOutputTokens).toBeUndefined();
      expect(request.serializedInput).toEqual(prepareModelRequest({ model, tools: [],
        messages: [{ role: "user", content: "parent" }, ...(prefixOnly ? []
          : [{ role: "user" as const, content: { kind: "file" as const, file: fresh, policy: "reference" as const } }])] }, DEFAULT_LIMITS));
      yield { kind: "completed" as const, metadata: {} };
    }, async reconcile() { return undefined; } };
    const runtime = await Harness.builder(contracts).inheritedModelPrefix({ core, scope, head, files })
      .content(content).context(context).model(model, provider).grant(volumeGrant).build();
    for (const version of ["first", "later"]) {
      fresh = await stage("current.txt", version, new TextEncoder().encode(`${version} é\0🦀\r\n`), "text/plain");
      await runtime.run("go");
      expect(files.has(key(fresh))).toBe(false);
    }
    expect(calls).toBe(2);
    expect(reads).toBe(2);
    prefixOnly = true;
    await runtime.run("go");
    expect(calls).toBe(3);
    expect(reads).toBe(2);
    prefixOnly = false;
    const denied = core.issueScopeForAgent(agent, "head-only", [core.fileReadCapability(head)]);
    const deniedRuntime = await Harness.builder(contracts).inheritedModelPrefix({ core, scope: denied, head, files })
      .content(content).context(context).model(model, provider).grant(volumeGrant).build();
    await expect(deniedRuntime.run("go")).rejects.toThrow();
    expect(calls).toBe(3);
    expect(reads).toBe(2);
    resident.set(key(fresh!), new TextEncoder().encode("corrupt"));
    await expect(runtime.run("go")).rejects.toThrow();
    expect(calls).toBe(3);
  } finally { core.free(); }
});

test("stale WASM modules fail compatibility checks before model dispatch", () => {
  expect(() => assertHarnessWasmExports(harnessWasm)).not.toThrow();
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateModelContent: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateContract: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateUserInput: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, admitModelEvent: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, WasmContentStore: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports(rawWasmExports)).not.toThrow();
  expect(() => assertHarnessWasmExports({ ...rawWasmExports, wasmcontentstore_stage: undefined })).toThrow("required validators");
});

test("fresh WASM initialization accepts the raw content-store ABI", async () => {
  await ensureHarnessWasm();
});

async function file(): Promise<FileRef> {
  return contracts.validate("file_ref", {
    volume: {
      provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: agent },
    },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: await descriptorFor(new TextEncoder().encode("input"), "text/plain"),
    display_name: "input.txt",
  });
}

async function nativeOptionPart() {
  const media = await file();
  const options = { ...media, path: "messages/options.json", display_name: "options.json",
    descriptor: await descriptorFor(new TextEncoder().encode("{}"), "application/json") };
  const configuration = {
    source: { authority: { kind: "agent" as const, id: agent }, revision: 9_007_199_254_740_993n },
    configuration: { extension: { name: "fixture.options", version: 1 },
      schema_digest: Array(32).fill(1), content: options },
    implementation_digest: Array(32).fill(2),
  };
  return { kind: "file" as const, file: media, policy: { native: {
    intent: { kind: "image" as const, detail: "auto" as const },
    maximum_bytes: 64, maximum_work: 8, configuration,
  } } };
}

test("Rust inventory includes nested tool media and options while preserving original revisions", async () => {
  const part = await nativeOptionPart();
  const inventory = contracts.modelContentInventory({ kind: "tool_result", callId: "call", name: "inspect",
    content: { kind: "parts", parts: [{ kind: "text", text: "attachment" }, part] } }, DEFAULT_LIMITS);
  expect(inventory.files).toEqual([part.file, part.policy.native.configuration.configuration.content]);
  expect(inventory.nativeConfigurations).toEqual([part.policy.native.configuration]);
  expect(inventory.nativeConfigurations[0]?.source.revision).toBe(9_007_199_254_740_993n);
  expect(Object.isFrozen(inventory.nativeConfigurations[0])).toBe(true);
  expect(contracts.modelContentInventory("plain text", DEFAULT_LIMITS)).toEqual({ files: [], nativeConfigurations: [] });
});

test("public model content converts only typed portable quantities and preserves original option revisions", async () => {
  const part = await nativeOptionPart();
  contracts.modelContentInventory(part, DEFAULT_LIMITS);
  const raw = contracts.decodeModelJson(contracts.encodeCanonicalJson(part)) as NativeModelContent;
  expect(publicModelContent(raw)).toEqual(part);
  const result = { kind: "tool_result", call_id: "call", name: "inspect",
    content: { kind: "parts", parts: [part] } };
  expect(publicModelContent(contracts.decodeModelJson(contracts.encodeCanonicalJson(result)) as NativeModelContent))
    .toEqual({ kind: "tool_result", callId: "call", name: "inspect", content: { kind: "parts", parts: [part] } });
});

test("local model dispatch rejects claimed native options before invoking a custom provider", async () => {
  let calls = 0;
  const runtime = await Harness.builder(contracts).model(
    { provider: "mock", name: "native-options", revision: "pinned", options: {} }, {
      async *generate() { calls += 1; yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; },
    }).build();
  await expect(runtime.run({ prompt: "", content: [await nativeOptionPart()] })).rejects.toThrow(
    "original native option admission is unavailable");
  expect(calls).toBe(0);
});

test("WASM model validators preserve Rust limits and tool-name checks", async () => {
  const content = await file();
  expect(() => validateModelContent("hello", DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "file", file: content, policy: "reference" }, DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "lookup", arguments: { value: 1 } }, DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "bad name", arguments: {} }, DEFAULT_LIMITS)).toThrow();
  expect(() => validateModelContent({ kind: "text", text: "x".repeat(DEFAULT_LIMITS.render_bytes + 1) }, DEFAULT_LIMITS)).toThrow();
  expect(() => validateModelContent({ kind: "unknown", value: true } as never, DEFAULT_LIMITS)).toThrow();
});

test("WASM user-input validator rejects empty, tool, and malformed content", async () => {
  const content = await file();
  expect(() => validateUserInput("hello")).not.toThrow();
  expect(() => validateUserInput({ kind: "file", file: content, policy: { native: {
    intent: { kind: "image", detail: "auto" }, maximum_bytes: 64, maximum_work: 8, configuration: null,
  } } })).not.toThrow();
  expect(() => validateUserInput("")).toThrow();
  expect(() => validateUserInput([])).toThrow();
  expect(() => validateUserInput({ kind: "tool_result", callId: "call", name: "lookup", content: { kind: "json", value: {} } })).toThrow();
  expect(() => validateUserInput({ kind: "file", file: { ...content, path: "../escape" }, policy: "reference" })).toThrow();
});

test("Rust model event admission preserves per-step bounds and cumulative UTF-8 output bytes", () => {
  const limits = { ...DEFAULT_LIMITS, model_events_per_step: 2, tool_calls_per_step: 2, file_bytes: 3, render_bytes: 3 };
  let admitted = contracts.admitModelEvent({ kind: "content", delta: "é" }, limits);
  let state = admitted.state;
  expect(admitted.event).toEqual({ kind: "content", delta: "é" });
  expect(state).toEqual({ count: 1, calls: [], completed: false, text_bytes: 2 });
  const completedLimits = { ...limits, model_events_per_step: 3 };
  admitted = contracts.admitModelEvent({ kind: "completed", metadata: {} }, completedLimits, state);
  state = admitted.state;
  expect(state.completed).toBe(true);
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "x" }, completedLimits, state)).toThrow("after completion");
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "é" }, limits,
    { count: 0, calls: [], completed: false, text_bytes: 2 })).toThrow("assistant output exceeds file limit");
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "x" }, limits,
    { count: 2, calls: [], completed: false, text_bytes: 0 })).toThrow("model event limit exceeded");
});

test("Rust model event admission rejects duplicate tool identities and accepts camelCase call IDs", () => {
  const limits = { ...DEFAULT_LIMITS, tool_calls_per_step: 1 };
  const first = contracts.admitModelEvent({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} }, limits);
  expect(first.event).toEqual({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} });
  expect(first.state.calls).toEqual(["call-1"]);
  expect(() => contracts.admitModelEvent({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} }, limits, first.state)).toThrow("repeated");
  expect(() => contracts.admitModelEvent({ kind: "tool_call", callId: "bad/name", name: "lookup", arguments: {} }, limits)).toThrow("identity");
});

test("model event admission detaches getter-backed provider events", () => {
  let reads = 0;
  const event = {
    kind: "content" as const,
    get delta(): string {
      reads += 1;
      return reads === 1 ? "ok" : "x".repeat(DEFAULT_LIMITS.file_bytes + 1);
    },
  };
  const admitted = contracts.admitModelEvent(event, DEFAULT_LIMITS);
  expect(admitted.event).toEqual({ kind: "content", delta: "ok" });
  expect(reads).toBe(1);
});

test("model event admission rejects unknown event fields", () => {
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "ok", extra: true } as never, DEFAULT_LIMITS)).toThrow();
});

test("model event admission preserves full-width BigInts in provider JSON", () => {
  const value = 9_007_199_254_740_993n;
  const toolCall = contracts.admitModelEvent({
    kind: "tool_call", callId: "bigint-call", name: "lookup", arguments: { tokens: 1n, cursor: value },
  }, DEFAULT_LIMITS);
  expect(toolCall.event).toEqual({
    kind: "tool_call", callId: "bigint-call", name: "lookup", arguments: { tokens: 1, cursor: value },
  });

  const completed = contracts.admitModelEvent({ kind: "completed", metadata: { tokens: 1n, cursor: value } }, DEFAULT_LIMITS);
  expect(completed.event).toEqual({ kind: "completed", metadata: { tokens: 1, cursor: value } });
});
