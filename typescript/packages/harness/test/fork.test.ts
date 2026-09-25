import { expect, test } from "bun:test";
import { MAX_FORK_INHERITED_MESSAGES, NativeContracts, forkReadableReferences, forkSeed, resourceRef, validateForkReport, validateForkSeed,
  type AgentId, type ForkReport, type ForkSeed, type OperationId, type ResourceRevision, type ReferenceGrant } from "../src/index.js";

const filesystem = { namespace: "test", family: "filesystem", version: "2" } as const;
const stream = { namespace: "test", family: "stream", version: "2" } as const;
const owner = { kind: "project", id: "project" } as const;
const parentVolume = { provider: filesystem, id: "parent", class: "project", owner } as const;
const childVolume = { provider: filesystem, id: "child", class: "project", owner } as const;
const agent = "22222222-2222-2222-2222-222222222222" as AgentId;
const privateVolume = { provider: filesystem, id: "private", class: "agent_private", owner: { kind: "agent", id: agent } } as const;
const history: ResourceRevision = {
  kind: "history",
  reference: {
    kind: "stream", provider: stream,
    key: [...new TextEncoder().encode("harness/v2/conversations/parent")], version: "3",
  },
};
const parentProject: ResourceRevision = {
  kind: "project", reference: {
    volume: parentVolume,
    generation: { kind: "generation", provider: filesystem, key: [1], version: null },
  },
};
const childProject: ResourceRevision = {
  kind: "project", reference: {
    volume: childVolume,
    generation: { kind: "generation", provider: filesystem, key: [2], version: null },
  },
};

test("Rust and TypeScript share the canonical v2 resource revision fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/resource-revision.json", import.meta.url)).text()).trim();
  const revision = JSON.parse(fixture) as ResourceRevision;
  if (revision.kind !== "artifact") throw new TypeError("fixture kind changed");
  const admitted = await resourceRef(revision.reference);
  expect(JSON.stringify({ kind: revision.kind, reference: admitted })).toBe(fixture);
  expect(Object.isFrozen(admitted.provider)).toBe(true);
  expect(Object.isFrozen(admitted.key)).toBe(true);
  await expect(resourceRef({ ...revision.reference, key: [] })).rejects.toThrow();
  const unknownField = { ...revision.reference, unexpected: true };
  await expect(resourceRef(unknownField)).rejects.toThrow();
});

test("Rust and TypeScript share the canonical v2 fork seed fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/fork-seed.json", import.meta.url)).text()).trim();
  const decoded = JSON.parse(fixture) as Omit<ForkSeed, "parent_revision"> & { parent_revision: number };
  const seed = { ...decoded, parent_revision: BigInt(decoded.parent_revision) };
  await validateForkSeed(seed);
  expect(JSON.stringify(seed, (_key, value: unknown) => typeof value === "bigint" ? Number(value) : value)).toBe(fixture);
});

test("Rust and TypeScript share the canonical v2 reference grant fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/reference-grant.json", import.meta.url)).text()).trim();
  const grant = JSON.parse(fixture) as ReferenceGrant;
  (await NativeContracts.create()).validate("file_ref", grant.file);
  expect(JSON.stringify(grant)).toBe(fixture);
});

function report(): ForkReport {
  return {
    request: {
      operation_id: "11111111111111111111111111111111" as OperationId,
      parent: { kind: "conversation", id: "parent" }, parent_revision: 3n,
      child: { kind: "conversation", id: "child" }, child_agent: agent,
      attached_agents: [],
      selections: [
        { required: true, revision: history },
        { required: true, revision: parentProject },
      ],
      boundary: null,
    },
    captures: [
      { kind: "captured", value: { source: history, revision: history } },
      { kind: "captured", value: { source: parentProject, revision: childProject } },
    ],
    child_private_volume: privateVolume,
    child_private_generation: { kind: "generation", provider: filesystem, key: [3], version: null },
    inherited_context: [],
    inherited_through_sequence: 0n,
    shared_grants: [],
    reference_grants: [],
    attachment_manifests: [],
  };
}

test("parent-controlled fork seed pins exact history and isolates project/private volumes", async () => {
  const seed = await forkSeed(report());
  expect(seed.resources).toHaveLength(2);
  expect(seed.omissions).toHaveLength(0);
  expect(seed.child_private_volume.owner).toEqual({ kind: "agent", id: agent });
  expect(seed.child_private_generation).toEqual(report().child_private_generation);
  await expect(validateForkSeed(seed)).resolves.toBeUndefined();
  await expect(validateForkSeed({ ...seed, inherited_through_sequence: -1n })).rejects.toThrow();

  const stale = { ...seed, parent_revision: 4n };
  await expect(validateForkSeed(stale)).rejects.toThrow();
  const inheritedPrivate = { ...seed, child_private_volume: { ...privateVolume, owner: { kind: "agent" as const, id: "33333333333333333333333333333333" as AgentId } } };
  await expect(validateForkSeed(inheritedPrivate)).rejects.toThrow();
  await expect(validateForkSeed({ ...seed, child_private_generation: {
    ...seed.child_private_generation, provider: { namespace: "other", family: "filesystem", version: "2" },
  } })).rejects.toThrow();
});

test("fork reports and seeds reject inherited prefixes above the protocol hard cap", async () => {
  await expect(validateForkReport(report())).resolves.toBeUndefined();
  const oversized = MAX_FORK_INHERITED_MESSAGES + 1n;
  await expect(validateForkReport({ ...report(), inherited_through_sequence: oversized }))
    .rejects.toThrow("inherited message limit");
  await expect(forkSeed({ ...report(), inherited_through_sequence: oversized }))
    .rejects.toThrow("inherited message limit");
  const seed = await forkSeed(report());
  await expect(validateForkSeed({ ...seed, inherited_through_sequence: oversized }))
    .rejects.toThrow("protocol limits");
});

test("required capture failure never publishes a child; optional failures remain visible", async () => {
  const required = report();
  await expect(forkSeed({ ...required, captures: [{ kind: "unsupported", value: "not available" }, required.captures[1]!] }))
    .rejects.toThrow();
  const optional: ForkReport = {
    ...required,
    request: {
      ...required.request,
      selections: [
        ...required.request.selections,
        { required: false, revision: { kind: "artifact", reference: { kind: "artifact", provider: filesystem, key: [9], version: null } } },
      ],
    },
    captures: [...required.captures, { kind: "indeterminate", value: "44444444444444444444444444444444" as OperationId }],
  };
  const seed = await forkSeed(optional);
  expect(seed.omissions).toHaveLength(1);
  expect(seed.omissions[0]?.outcome.kind).toBe("indeterminate");
  const invalidOmission = { ...seed, omissions: [{ ...seed.omissions[0]!, outcome: { kind: "indeterminate" as const, value: "not-an-operation" as OperationId } }] };
  await expect(validateForkSeed(invalidOmission)).rejects.toThrow();
  await expect(validateForkSeed({ ...seed, resources: [...seed.resources, seed.resources[0]!] }))
    .rejects.toThrow();
  await expect(resourceRef({ kind: "artifact", provider: filesystem, key: [1], version: null }))
    .resolves.toBeDefined();
});

test("project capture cannot retain the parent's project volume", async () => {
  const invalid = report();
  await expect(forkSeed({ ...invalid, captures: [invalid.captures[0]!, { kind: "captured", value: { source: parentProject, revision: parentProject } }] }))
    .rejects.toThrow();
});

test("fork seeds reject noncanonical omissions and unknown fields", async () => {
  const seed = await forkSeed(report());
  await expect(validateForkSeed({ ...seed, unexpected: true } as unknown as ForkSeed))
    .rejects.toThrow();
  const capturedOmission = {
    ...seed,
    omissions: [{ selection: { required: false, revision: history }, outcome: { kind: "captured", value: seed.resources[0]! } }],
  } as unknown as ForkSeed;
  await expect(validateForkSeed(capturedOmission)).rejects.toThrow();
});

test("published seeds are detached and immutable, including nested resources", async () => {
  const prepared = report();
  const seed = await forkSeed(prepared);
  (prepared.child_private_volume as { id: string }).id = "changed";
  expect(seed.child_private_volume.id).toBe("private");
  expect(Object.isFrozen(seed)).toBe(true);
  expect(Object.isFrozen(seed.resources[0]?.revision)).toBe(true);
});

test("native fork conversion keeps child-owned file lengths as exact TS numbers", async () => {
  const inherited = {
    volume: privateVolume, path: ".system/inherited-conversation/prefix.txt", version: "one",
    descriptor: { sha256: Array(32).fill(0), byte_length: 7, media_type: "text/plain" },
    display_name: "prefix.txt",
  };
  const seed = await forkSeed({ ...report(), inherited_context: [inherited] });
  expect(seed.inherited_context[0]?.descriptor.byte_length).toBe(7);
  expect(typeof seed.inherited_context[0]?.descriptor.byte_length).toBe("number");
});

test("inherited context cannot select the same child path twice", async () => {
  const seed = await forkSeed(report());
  const file = {
    volume: privateVolume, path: ".system/inherited-conversation/a.txt", version: "one",
    descriptor: { sha256: Array(32).fill(0) as number[], byte_length: 0, media_type: "text/plain" },
    display_name: "a.txt",
  };
  await expect(validateForkSeed({ ...seed, inherited_context: [file, { ...file, version: "two" }] }))
    .rejects.toThrow();
});

test("a selected shared volume requires one exact child-bound grant", async () => {
  const seed = await forkSeed(report());
  const sharedVolume = { provider: filesystem, id: "shared", class: "session_shared", owner: { kind: "session", id: "session" } } as const;
  const sharedRevision: ResourceRevision = { kind: "shared_volume", reference: sharedVolume };
  const resources = [...seed.resources, { source: sharedRevision, revision: sharedRevision }];
  await expect(validateForkSeed({ ...seed, resources })).rejects.toThrow();
  const granted = { ...seed, resources, shared_grants: [{ volume: sharedVolume, child_agent: agent, operations: ["read"] as const }] };
  await expect(validateForkSeed(granted)).resolves.toBeUndefined();
  await expect(validateForkSeed({ ...granted, shared_grants: [{ ...granted.shared_grants[0]!, child_agent: "33333333333333333333333333333333" as AgentId }] }))
    .rejects.toThrow();
  const reader = "33333333333333333333333333333333" as AgentId;
  await expect(validateForkSeed({ ...granted, attached_agents: [reader], shared_grants: [...granted.shared_grants, { volume: sharedVolume, child_agent: reader, operations: ["read"] as const }] }))
    .resolves.toBeUndefined();
});

test("attached fork agents receive exact read-only references", async () => {
  const seed = await forkSeed(report());
  const reader = "33333333333333333333333333333333" as AgentId;
  const parentFile = {
    volume: { ...privateVolume, owner: { kind: "agent" as const, id: "44444444444444444444444444444444" as AgentId } },
    path: "attachments/evidence.bin", version: "one",
    descriptor: { sha256: Array(32).fill(0) as number[], byte_length: 0, media_type: "application/octet-stream" },
    display_name: "evidence.bin",
  };
  const attached = { ...seed, attached_agents: [reader], reference_grants: [{ file: parentFile, reader }] };
  await expect(validateForkSeed(attached)).resolves.toBeUndefined();
  const inherited = { ...parentFile, volume: privateVolume, path: ".system/inherited-conversation/evidence.bin" };
  const readable = await forkReadableReferences({ ...attached, inherited_context: [inherited] }, reader);
  const canonicalParentFile = { ...parentFile, volume: { ...parentFile.volume,
    owner: { kind: "agent" as const, id: "44444444-4444-4444-4444-444444444444" as AgentId } } };
  expect(readable).toEqual([inherited, canonicalParentFile]);
  await expect(forkReadableReferences(attached, "55555555555555555555555555555555" as AgentId)).rejects.toThrow();
  await expect(validateForkSeed({ ...seed, attached_agents: [parentFile.volume.owner.id as AgentId], reference_grants: [] })).resolves.toBeUndefined();
  await expect(validateForkSeed({ ...attached, attached_agents: [] })).rejects.toThrow();
  await expect(validateForkSeed({ ...attached, reference_grants: [...attached.reference_grants, attached.reference_grants[0]!] }))
    .rejects.toThrow();
  await expect(validateForkSeed({ ...attached, reference_grants: [{ file: parentFile, reader: parentFile.volume.owner.id as AgentId }] }))
    .rejects.toThrow();
  const projectFile = { ...parentFile, volume: parentVolume };
  await expect(validateForkSeed({ ...seed, reference_grants: [{ file: projectFile, reader: agent }] }))
    .resolves.toBeUndefined();
  const manifest = { ...parentFile, path: "attachments/list.json", descriptor: {
    ...parentFile.descriptor, media_type: "application/vnd.acyclic.harness.attachments+json",
  } };
  await expect(validateForkSeed({ ...attached,
    attachment_manifests: [manifest],
    reference_grants: [{ file: parentFile, reader, attachment_manifest: manifest }],
  })).resolves.toBeUndefined();
  await expect(validateForkSeed({ ...attached,
    reference_grants: [{ file: parentFile, reader, attachment_manifest: manifest }],
  })).rejects.toThrow();
});

test("extension fork revisions pin a nonzero implementation digest and numeric version", async () => {
  const seed = await forkSeed(report());
  const extension: ResourceRevision = {
    kind: "extension",
    reference: {
      name: "example.message", version: 1, implementation_digest: Array(32).fill(9),
      reference: { kind: "artifact", provider: filesystem, key: [4], version: "immutable-1" },
    },
  };
  const resources = [...seed.resources, { source: extension, revision: extension }];
  await expect(validateForkSeed({ ...seed, resources })).resolves.toBeUndefined();
  const resetRevision: ResourceRevision = {
    ...extension,
    reference: { ...extension.reference, reference: { ...extension.reference.reference, key: [5] } },
  };
  await expect(validateForkSeed({ ...seed, resources: [...seed.resources, { source: extension, revision: resetRevision }] })).resolves.toBeUndefined();
  await expect(validateForkSeed({ ...seed, resources: [...seed.resources, {
    source: extension,
    revision: { ...extension, reference: { ...extension.reference, implementation_digest: Array(32).fill(0) } },
  }] })).rejects.toThrow();
  await expect(validateForkSeed({ ...seed, resources: [...seed.resources, {
    source: extension,
    revision: { ...extension, reference: { ...extension.reference, version: 0 } },
  }] })).rejects.toThrow();
});
