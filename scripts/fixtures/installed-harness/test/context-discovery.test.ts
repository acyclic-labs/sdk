import { expect, test } from "bun:test";
import {
  initializeContextDiscovery, captureDiscoveredContext, contextForRequest, readPinnedContextPath,
  projectDiscoveredContext, applyContextProjection, fileDescriptor, encodeCanonicalJson, decodeClientJson,
  type ContextDiscovery, type ContextDiscoveryReader,
} from "@acyclic-labs/harness";

// Finite fixture inputs, not product defaults or a fabricated read capability.
const fixtureLimits = { file_bytes: 65_536, path_bytes: 4_096, attachments: 16, render_bytes: 65_536,
  model_steps: 8, model_events_per_step: 64, tool_calls_per_step: 16, context_messages: 128 };

test("installed generated discovery callbacks retain lazy bounded reads and exact file wire", async () => {
  await initializeContextDiscovery();
  const provider = { namespace: "installed", family: "filesystem", version: "2" };
  const volume = { provider, id: "skills", class: "session_shared" as const,
    owner: { kind: "session" as const, id: "installed" } };
  const generation = { kind: "generation" as const, provider, key: [1], version: "pinned" };
  const declaration: ContextDiscovery = { instructions: [{ root: { volume, directory: "project" }, active_directory: "src" }], skills: [{ volume, directory: "" }],
    policy: { instruction_names: ["AGENTS.md", "CLAUDE.md"], skill_name: "SKILL.md" },
    limits: { entries: 16, directories: 8, header_bytes: 256, instruction_bytes: 512 } };
  const bytes = new TextEncoder().encode("---\nname: inspect\ndescription: Inspect files\nrank: 7\nexample: {sha256: note, byte_length: 7, media_type: note}\n---\nSECRET BODY");
  const instructionText = new Map([
    ["project/AGENTS.md", "root instruction"],
    ["project/src/CLAUDE.md", "nested instruction"],
  ]);
  let reads = 0;
  const reader: ContextDiscoveryReader = {
    async list(query) {
      expect(query.maximum_entries).toBeGreaterThan(0);
      return { generation, hasMore: false, entries: query.path === "project"
        ? [{ name: "AGENTS.md", kind: "file" }, { name: "sibling", kind: "directory" }, { name: "src", kind: "directory" }]
        : query.path === "project/src"
        ? [{ name: "CLAUDE.md", kind: "file" }, { name: "child", kind: "directory" }]
        : query.path === "" ? [{ name: "inspect", kind: "directory" }] : [{ name: "SKILL.md", kind: "file" }] };
    },
    async prefix(query) {
      expect(query.maximum_bytes).toBe(256n);
      return bytes.slice();
    },
    async read(query) {
      reads++;
      expect(query.maximum_bytes).toBe(512n);
      const content = instructionText.has(query.source.path)
        ? new TextEncoder().encode(instructionText.get(query.source.path)!) : bytes.slice();
      return { bytes: content, file: { volume, path: query.source.path, version: "pinned",
        descriptor: fileDescriptor(content, "text/plain"), display_name: query.source.path.split("/").at(-1)! } };
    },
  };
  const snapshot = await captureDiscoveredContext(declaration, reader);
  expect(reads).toBe(2);
  expect(snapshot.instructions).toMatchObject([...instructionText.keys()].map(path => ({ path })));
  const fields = decodeClientJson(encodeCanonicalJson(snapshot.skills[0]!.fields)) as Record<string, unknown>;
  expect(fields.rank).toBe(7n);
  expect(fields.example).toEqual({ sha256: "note", byte_length: 7n, media_type: "note" });
  // The deleted TS facade's Object.freeze policy is retired. Rust returns owned data.
  let bindings = 0;
  const inactiveReader: ContextDiscoveryReader = {
    get list() { bindings++; return reader.list; },
    get prefix() { bindings++; return reader.prefix; },
    get read() { bindings++; return reader.read; },
  };
  const inactiveDeclaration = { ...declaration, limits: { ...declaration.limits, instruction_bytes: 0 } };
  expect(await contextForRequest(inactiveDeclaration, inactiveReader, snapshot, "explicit")).toEqual(snapshot);
  expect(bindings).toBe(0);
  const projected = projectDiscoveredContext(snapshot, { messages: [], metadata: {} }, "prepend", fixtureLimits);
  expect(JSON.stringify(projected)).not.toContain("SECRET BODY");
  const body = await readPinnedContextPath(snapshot.skills[0]!.source, reader, 512);
  expect(body.bytes).toEqual(bytes);
  expect(typeof body.file.descriptor.byte_length).toBe("number");
  expect(reads).toBe(3);
  const part = { kind: "file" as const, file: body.file, policy: "reference" as const };
  const base = { messages: [{ role: "user" as const, content: part },
    { role: "user" as const, content: [part] }],
  metadata: Object.fromEntries([["attachment", body.file], ["__proto__", body.file]]) };
  const composed = projectDiscoveredContext(snapshot, base, "prepend", fixtureLimits);
  expect(composed.messages.slice(3)).toEqual(base.messages);
  expect(composed.metadata.attachment).toEqual(body.file);
  expect(Object.hasOwn(composed.metadata, "__proto__")).toBe(true);
  expect(typeof composed.metadata.attachment!.descriptor.byte_length).toBe("number");
  expect(applyContextProjection(base, [], "prompt", "append", fixtureLimits)).toEqual(base);
  await expect(captureDiscoveredContext(declaration, { ...reader,
    async prefix() { return new Uint8Array(257); },
  })).rejects.toThrow();
  await expect(captureDiscoveredContext(declaration, { ...reader,
    async list(query) {
      const page = await reader.list(query);
      return { ...page, entries: page.entries.map(entry => entry.name === "SKILL.md"
        ? { ...entry, kind: "directory" as const } : entry) };
    },
  })).rejects.toThrow();
  await expect(readPinnedContextPath(snapshot.skills[0]!.source, { ...reader,
    async read(query) {
      const result = await reader.read(query);
      return { ...result, bytes: Uint8Array.of(0) };
    },
  }, 512)).rejects.toThrow();
});
