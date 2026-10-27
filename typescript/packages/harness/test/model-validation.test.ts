import { syntheticAccounting, scriptedPreparation } from "./support/model-accounting.mjs";
import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import {
  DEFAULT_LIMITS, jsonToolProjection, Harness, NativeContracts, descriptorFor, type AgentId, type FileRef, type PinnedContextPath,
} from "../src/index.js";
import {
  prepareModelRequest, encodeModelPrefix, WasmReducer, validateModelContent, validateUserInput,
} from "../generated/wasm/acyclic_harness_wasm.js";
import type { ModelRequest as ModelRequestWire } from "../generated/wasm/acyclic_harness_wasm.js";

const contracts = await NativeContracts.create();
const jsonProjection = await jsonToolProjection({});
const agent = "07070707-0707-0707-0707-070707070707" as AgentId;

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
      ...syntheticAccounting(contracts), prepare: scriptedPreparation(contracts, async function* (request) {
        admitted.push(request.serializedInput.slice());
        yield { kind: "completed" as const, metadata: {} };
      }), async reconcile() { return undefined; },
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

test("the actual task provider receives the admitted serialized input", async () => {
  const prompt = "é\0🦀\r\n";
  let calls = 0;
  const runtime = await Harness.builder(contracts).model({ provider: "mock", name: "exact", revision: "pinned", options: {} }, {
    ...syntheticAccounting(contracts), prepare: scriptedPreparation(contracts, async function* (request) {
      calls += 1;
      expect(contracts.decodeModelJson(request.serializedInput)).toEqual({
        model: { provider: "mock", name: "exact", revision: "pinned", options: {} }, messages: [{ role: "user", content: prompt }],
        tools: [], max_output_tokens: null,
      });
      expect(request.messages).toEqual([{ role: "user", content: prompt }]);
      yield { kind: "completed" as const, metadata: {} };
    }),
    async reconcile() { return undefined; },
  }).build();
  await runtime.run(prompt);
  expect(calls).toBe(1);
});

test("WASM direct-parent prefixes preserve exact provider bytes across depth three and siblings", async () => {
  const fixture = await readFile(new URL("../../../../rust/crates/harness/fixtures/model-request.json", import.meta.url));
  const root = contracts.decodeModelJson(fixture) as unknown as ModelRequestWire;
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
      const scope = core.issueScope(`reader-${depth}`, references.map(ref => core.fileReadCapability(ref)), agent);
      const parentWire = contracts.decodeModelJson(parentRequest) as unknown as ModelRequestWire;
      const children = depth === 1 ? ["primary", "sibling"] : ["primary"];
      let primary: Uint8Array | undefined;
      for (const child of children) {
        const prompt = `notification ${depth}; task ${child}; identity ${child}; workspace ${child}; fresh scratch ${child} é\0🦀\r\n`;
        let calls = 0;
        const runtime = await Harness.builder(contracts).inheritedModelPrefix({ core, scope, head, files })
          .model(root.model, { ...syntheticAccounting(contracts), prepare: scriptedPreparation(contracts, async function* (request) {
            calls++;
            const expected = prepareModelRequest({ model: root.model, tools,
              messages: [...parentWire.messages, { role: "user", content: prompt }] }, DEFAULT_LIMITS);
            expect(request.serializedInput).toEqual(expected);
            if (child === "primary") primary = request.serializedInput.slice();
            yield { kind: "completed" as const, metadata: {} };
          }), async reconcile() { return undefined; } })
          .tool({ ...tools[0]!, inputSchema: { type: "string" }, outputSchema: { type: "string" },
            parseInput: value => value, projection: await jsonToolProjection({ type: "string" }), parseOutput: value => value }, {
            async execute(invocation) { return { value: invocation.arguments }; }, async reconcile() { return undefined; },
          }).grant("tool:call:echo").build();
        await runtime.run(prompt);
        expect(calls).toBe(1);
      }
      const denied = core.issueScope("missing-attachment", references.filter(ref => ref !== attachment).map(ref => core.fileReadCapability(ref)), agent);
      await expect(core.prepareInheritedModelRequest(denied, { model: root.model, tools, maxOutputTokens: 4096,
        messages: [{ role: "user", content: "denied" }] }, head, files, DEFAULT_LIMITS)).rejects.toThrow();
      const segment = encodeModelPrefix(primary!, head, parentRequest, DEFAULT_LIMITS);
      expect((contracts.decodeModelJson(segment) as { messages: unknown[] }).messages).toHaveLength(1);
      head = await stage(segment, `prefix-${depth}.json`, "application/vnd.acyclic.harness.model-prefix+json");
      parentRequest = primary!;
    }
  } finally { core.free(); }
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
    const scope = core.issueScope("local-reader", [volumeGrant], agent);
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
    const provider = { ...syntheticAccounting(contracts), prepare: scriptedPreparation(contracts, async function* (request: import("../src/model.js").ModelRequest & { serializedInput: Uint8Array }) {
      calls++;
      expect(request.maxOutputTokens).toBeUndefined();
      expect(request.serializedInput).toEqual(prepareModelRequest({ model, tools: [],
        messages: [{ role: "user", content: "parent" }, ...(prefixOnly ? []
          : [{ role: "user" as const, content: { kind: "file" as const, file: fresh, policy: "reference" as const } }])] }, DEFAULT_LIMITS));
      yield { kind: "completed" as const, metadata: {} };
    }), async reconcile() { return undefined; } };
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
    const denied = core.issueScope("head-only", [core.fileReadCapability(head)], agent);
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

test("synthetic accounting counts nested media and native options with wide revisions", async () => {
  const part = await nativeOptionPart();
  const request: import("../src/model.js").ModelRequest = {
    model: { provider: "mock", name: "nested-accounting", revision: "1", options: {} },
    messages: [
      { role: "assistant", content: { kind: "tool_call", callId: "call", name: "inspect", arguments: {} } },
      { role: "tool", content: { kind: "tool_result", callId: "call", name: "inspect",
        content: { kind: "parts", parts: [part] } } },
    ],
    tools: [{ name: "inspect", revision: "1", description: "inspect", inputSchema: {}, outputSchema: {},
      projectionSchema: {} }],
  };
  const serializedInput = prepareModelRequest(request, DEFAULT_LIMITS);
  const count = await syntheticAccounting(contracts).countTokens({ ...request, serializedInput });
  expect(count.requestDigest).toEqual(contracts.digestCanonicalJson(contracts.decodeCanonicalJson(serializedInput)));
  expect(count.fixedTokens).toBe(serializedInput.byteLength + 512);
  expect(count.messageTokens).toEqual([512, 512 + part.file.descriptor.byte_length
    + part.policy.native.configuration.configuration.content.descriptor.byte_length]);
});

test("local model dispatch rejects claimed native options before invoking a custom provider", async () => {
  let calls = 0;
  const runtime = await Harness.builder(contracts).model(
    { provider: "mock", name: "native-options", revision: "pinned", options: {} }, {
      ...syntheticAccounting(contracts),
      prepare: scriptedPreparation(contracts, async function* () { calls += 1; yield { kind: "completed" as const, metadata: {} }; }),
      async reconcile() { return undefined; },
    }).build();
  await expect(runtime.run({ prompt: "", content: [await nativeOptionPart()] })).rejects.toThrow(
    );
  expect(calls).toBe(0);
});
