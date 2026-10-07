import { expect, test } from "bun:test";
import {
  DEFAULT_LIMITS, NativeContracts, descriptorFor,
  type ContextDiscovery, type ContextDiscoveryReader,
} from "@acyclic-labs/harness";

test("installed generated discovery callbacks retain lazy bounded reads and exact file wire", async () => {
  const contracts = await NativeContracts.create();
  const provider = { namespace: "installed", family: "filesystem", version: "2" };
  const volume = { provider, id: "skills", class: "session_shared" as const,
    owner: { kind: "session" as const, id: "installed" } };
  const generation = { kind: "generation" as const, provider, key: [1], version: "pinned" };
  const declaration: ContextDiscovery = { instructions: [], skills: [{ volume, directory: "" }],
    policy: { instruction_names: ["AGENTS.md"], skill_name: "SKILL.md" },
    limits: { entries: 8, directories: 4, header_bytes: 256, instruction_bytes: 512 } };
  const bytes = new TextEncoder().encode("---\nname: inspect\ndescription: Inspect files\nrank: 7\nexample: {sha256: note, byte_length: 7, media_type: note}\n---\nSECRET BODY");
  let reads = 0;
  const reader: ContextDiscoveryReader = {
    async list(query) {
      expect(query.maximum_entries).toBeGreaterThan(0);
      return { generation, hasMore: false, entries: query.path === ""
        ? [{ name: "inspect", kind: "directory" }] : [{ name: "SKILL.md", kind: "file" }] };
    },
    async prefix(query) {
      expect(query.maximum_bytes).toBe(256n);
      return bytes.slice();
    },
    async read(query) {
      reads++;
      expect(query.maximum_bytes).toBe(512n);
      return { bytes: bytes.slice(), file: { volume, path: query.source.path, version: "pinned",
        descriptor: await descriptorFor(bytes, "text/plain"), display_name: "SKILL.md" } };
    },
  };
  const snapshot = await contracts.captureDiscoveredContext(declaration, reader);
  expect(reads).toBe(0);
  expect(snapshot.skills[0]?.fields.rank).toBe(7n);
  expect(snapshot.skills[0]?.fields.example).toEqual({ sha256: "note", byte_length: 7n, media_type: "note" });
  expect(Object.isFrozen(snapshot.skills)).toBe(true);
  let bindings = 0;
  const inactiveReader: ContextDiscoveryReader = {
    get list() { bindings++; return reader.list; },
    get prefix() { bindings++; return reader.prefix; },
    get read() { bindings++; return reader.read; },
  };
  const inactiveDeclaration = { ...declaration, limits: { ...declaration.limits, instruction_bytes: 0 } };
  expect(await contracts.contextForRequest(inactiveDeclaration, inactiveReader, snapshot, "explicit")).toEqual(snapshot);
  expect(bindings).toBe(0);
  const projected = contracts.projectDiscoveredContext(snapshot, { messages: [], metadata: {} }, "prepend", DEFAULT_LIMITS);
  expect(JSON.stringify(projected)).not.toContain("SECRET BODY");
  const body = await contracts.readPinnedContextPath(snapshot.skills[0]!.source, reader, 512);
  expect(body.bytes).toEqual(bytes);
  expect(typeof body.file.descriptor.byte_length).toBe("number");
  expect(reads).toBe(1);
  const part = { kind: "file" as const, file: body.file, policy: "reference" as const };
  const base = { messages: [{ role: "user" as const, content: part },
    { role: "user" as const, content: [part] }],
  metadata: Object.fromEntries([["attachment", body.file], ["__proto__", body.file]]) };
  const composed = contracts.projectDiscoveredContext(snapshot, base, "prepend", DEFAULT_LIMITS);
  expect(composed.messages.slice(1)).toEqual(base.messages);
  expect(composed.metadata.attachment).toEqual(body.file);
  expect(Object.hasOwn(composed.metadata, "__proto__")).toBe(true);
  expect(typeof composed.metadata.attachment!.descriptor.byte_length).toBe("number");
  expect(contracts.applyContextProjection(base, [], "prompt", "append", DEFAULT_LIMITS)).toEqual(base);
  await expect(contracts.captureDiscoveredContext(declaration, { ...reader,
    async prefix() { return new Uint8Array(257); },
  })).rejects.toThrow();
  await expect(contracts.captureDiscoveredContext(declaration, { ...reader,
    async list(query) {
      const page = await reader.list(query);
      return { ...page, entries: page.entries.map(entry => entry.name === "SKILL.md"
        ? { ...entry, kind: "directory" as const } : entry) };
    },
  })).rejects.toThrow();
  await expect(contracts.readPinnedContextPath(snapshot.skills[0]!.source, { ...reader,
    async read(query) {
      const result = await reader.read(query);
      return { ...result, bytes: Uint8Array.of(0) };
    },
  }, 512)).rejects.toThrow();
});
