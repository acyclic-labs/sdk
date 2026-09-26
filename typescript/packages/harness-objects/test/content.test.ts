import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Harness, NativeContracts, TaskDefinition, type AgentId } from "@acyclic-labs/harness";
import { MemoryObjectsProvider } from "@acyclic-labs/objects";
import { ObjectContentStore, type ObjectVolumeRef } from "../src/index.js";

const wasm = readFileSync(fileURLToPath(new URL("../../harness/generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)));
const contracts = await NativeContracts.create();
const owner = "10101010-1010-1010-1010-101010101010" as AgentId;
const reader = "11111111-1111-1111-1111-111111111111" as AgentId;

test("Objects content is exact-version, owner-written, and delegably read", async () => {
  const objects = new MemoryObjectsProvider();
  const bucket = await objects.createBucket("harness-objects-test");
  const volume: ObjectVolumeRef = {
    provider: { namespace: "local", family: "objects", version: "1" },
    id: bucket.bucketId, class: "agent_private", owner: { kind: "agent", id: owner },
  };
  const authority = await Harness.create({
    authority: { kind: "conversation", id: crypto.randomUUID() },
    issuerId: "objects-test", issuerKey: crypto.getRandomValues(new Uint8Array(32)), wasm,
  });
  const ownerScope = authority.issueScopeForAgent(owner, "owner", [
    authority.volumeCapability(volume, "read"), authority.volumeCapability(volume, "write"),
  ]);
  const store = await ObjectContentStore.create({ objects, bucket, volume, expectedProvider: volume.provider,
    authority, ownerScope,
    scope: ownerScope, maximumBytes: 4_096 });
  await expect(store.stage("internal", ".system/forged.txt", new Uint8Array([1]), "text/plain", "forged.txt"))
    .rejects.toThrow("reserved");
  const file = await store.stage("upload-1", "notes/one.txt", new TextEncoder().encode("owned"),
    "text/plain", "one.txt");
  expect(new TextDecoder().decode(await store.read(file))).toBe("owned");
  const manifestBytes = new TextEncoder().encode(JSON.stringify([{ file, label: null }]));
  const manifest = await store.stage("manifest-1", "lists/one.json", manifestBytes,
    "application/vnd.acyclic.harness.attachments+json", "one.json");
  expect(await store.loadManifest(manifest, 1)).toEqual([{ file, label: null }]);
  await expect(store.loadManifest(manifest, 2)).rejects.toThrow();
  const task = TaskDefinition.live<void, string>("object_file", "1", async context =>
    new TextDecoder().decode(await context.readFile(file)), { requirements: ["content"] });
  const runtime = Harness.builder(contracts).content(store.bindings())
    .grant(authority.volumeCapability(volume, "read")).task(task).build();
  expect(await runtime.spawn(task, undefined).result()).toEqual({ kind: "succeeded", value: "owned" });

  const delegated = authority.delegatePrivateFileRead(ownerScope, reader, "reader", file);
  const attached = await ObjectContentStore.create({ objects, bucket, volume, expectedProvider: volume.provider,
    authority, ownerScope,
    scope: delegated, maximumBytes: 4_096 });
  expect(attached.bindings().writer).toBeUndefined();
  expect(new TextDecoder().decode(await attached.read(file))).toBe("owned");
  await expect(attached.stage("denied", "notes/two.txt", new Uint8Array(), "text/plain", "two.txt"))
    .rejects.toThrow("original owner");
  await expect(ObjectContentStore.create({ objects, bucket, volume, expectedProvider: volume.provider,
    authority, ownerScope,
    scope: { ...delegated, capabilities: [...delegated.capabilities, authority.volumeCapability(volume, "write")] },
    maximumBytes: 4_096 })).rejects.toThrow();
  await expect(store.stage("upload-1", "notes/one.txt", new TextEncoder().encode("owned"),
    "text/plain", "changed.txt")).rejects.toThrow();
  await expect(store.stage("upload-1", "notes/other.txt", new TextEncoder().encode("owned"),
    "text/plain", "one.txt")).rejects.toThrow();
  await expect(store.stage("upload-1", "notes/one.txt", new TextEncoder().encode("other"),
    "text/plain", "one.txt")).rejects.toThrow();
  const foreign = await Harness.create({ authority: { kind: "conversation", id: crypto.randomUUID() },
    issuerId: "foreign", issuerKey: crypto.getRandomValues(new Uint8Array(32)), wasm });
  const foreignScope = foreign.issueScopeForAgent(reader, "foreign", [foreign.volumeCapability(volume, "read")]);
  await expect(ObjectContentStore.create({ objects, bucket, volume, expectedProvider: volume.provider,
    authority: foreign, ownerScope, scope: foreignScope, maximumBytes: 4_096 })).rejects.toThrow();
  (ownerScope.proof as number[])[0] ^= 1;
  (delegated.proof as number[])[0] ^= 1;
  objects.get = async () => { throw new Error("provider callback swapped"); };
  expect(Object.isFrozen(store.options.scope.proof)).toBe(true);
  expect(Object.isFrozen(attached.options.scope.proof)).toBe(true);
  expect(new TextDecoder().decode(await store.read(file))).toBe("owned");
  expect(new TextDecoder().decode(await attached.read(file))).toBe("owned");
  foreign.free();
  authority.free();
});
