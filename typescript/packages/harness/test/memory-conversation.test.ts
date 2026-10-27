import { expect, test } from "bun:test";
import { DEFAULT_LIMITS, MemoryConversation, type AgentId } from "../src/index.js";
import { HARNESS_PRIVATE_DIRECTORY_PAGE_MAXIMUM } from "../src/private-directory-page-contract.js";

const agent = "08080808-0808-0808-0808-080808080808" as AgentId;

test("ephemeral private volumes never alias another conversation of the same agent", async () => {
  const first = await MemoryConversation.create({ agent });
  const second = await MemoryConversation.create({ agent });
  expect(first.volume).not.toEqual(second.volume);
  const file = await first.stage("notes/one.txt", new TextEncoder().encode("private"), "text/plain", "one.txt");
  await expect(second.read(file)).rejects.toThrow();
  first.free();
  second.free();
});

test("local staging applies its Rust-validated bound before retaining bytes", async () => {
  const host = await MemoryConversation.create({ agent,
    limits: { ...DEFAULT_LIMITS, file_bytes: 8, render_bytes: 8, path_bytes: 24 } });
  const empty = await host.stage("files/empty", new Uint8Array(), "text/plain", "empty.txt");
  expect((await host.read(empty)).byteLength).toBe(0);
  await expect(host.stage("files/too-large", new Uint8Array(9), "text/plain", "large.txt"))
    .rejects.toThrow();
  await expect(host.stage("files/a-path-that-is-too-long", new Uint8Array(1), "text/plain", "long.txt"))
    .rejects.toThrow();
  host.free();
});

test("ephemeral content retention bounds bytes and zero-byte versions without discarding pinned refs", async () => {
  await expect(MemoryConversation.create({ agent, maxResidentBytes: Number.POSITIVE_INFINITY }))
    .rejects.toThrow();
  await expect(MemoryConversation.create({ agent, maxResidentFiles: 0 }))
    .rejects.toThrow();
  const host = await MemoryConversation.create({ agent, maxResidentBytes: 3, maxResidentFiles: 3 });
  const first = await host.stage("files/one", new Uint8Array([1, 2]), "application/octet-stream", "one");
  const second = await host.stage("files/two", new Uint8Array([3]), "application/octet-stream", "two");
  await expect(host.stage("files/three", new Uint8Array([4]), "application/octet-stream", "three"))
    .rejects.toThrow();
  const empty = await host.stage("files/empty", new Uint8Array(), "application/octet-stream", "empty");
  await expect(host.stage("files/another-empty", new Uint8Array(), "application/octet-stream", "empty"))
    .rejects.toThrow();
  expect(await host.stage("files/one", new Uint8Array([1, 2]), "application/octet-stream", "one"))
    .toEqual(first);
  expect(await host.read(first)).toEqual(new Uint8Array([1, 2]));
  expect(await host.read(second)).toEqual(new Uint8Array([3]));
  expect(await host.read(empty)).toEqual(new Uint8Array());
  host.free();

  const deduplicated = await MemoryConversation.create({ agent, maxResidentBytes: 2, maxResidentFiles: 3 });
  const a = await deduplicated.stage("files/a", new Uint8Array([5, 6]), "text/plain", "a");
  const b = await deduplicated.stage("files/b", new Uint8Array([5, 6]), "application/octet-stream", "b");
  expect(await deduplicated.read(a)).toEqual(await deduplicated.read(b));
  await expect(deduplicated.stage("files/c", new Uint8Array([7]), "text/plain", "c"))
    .rejects.toThrow();
  deduplicated.free();
});

test("resident file versions pin display name as well as bytes and media type", async () => {
  const host = await MemoryConversation.create({ agent });
  const file = await host.stage("notes/pinned.txt", new TextEncoder().encode("same"), "text/plain", "pinned.txt");
  const exposedScope = host.scope;
  const originalProofByte = host.scope.proof[0];
  (exposedScope.proof as number[])[0] = (originalProofByte ?? 0) ^ 1;
  expect(host.scope.proof[0]).toBe(originalProofByte);
  const forged = { ...file, display_name: "renamed.txt" };
  await expect(host.read(forged)).rejects.toThrow();
  expect(() => host.delegateFileRead("11111111-1111-1111-1111-111111111111" as AgentId,
    "forged", forged)).toThrow();
  const renamed = await host.stage("notes/pinned.txt", new TextEncoder().encode("same"), "text/plain", "renamed.txt");
  expect(renamed.version).not.toBe(file.version);
  expect(new TextDecoder().decode(await host.read(file))).toBe("same");
  expect(new TextDecoder().decode(await host.read(renamed))).toBe("same");
  host.free();
});

test("unseen owner volumes mount lazily without granting writes", async () => {
  const bytes = new TextEncoder().encode("lazy owner bytes");
  const owner = await MemoryConversation.create({ agent: "0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a" as AgentId });
  const file = await owner.stage("notes/lazy.txt", bytes, "text/plain", "lazy.txt");
  const scope = owner.delegateDirectoryRead(agent, "lazy-reader", "notes");
  const host = await MemoryConversation.create({ agent });
  let granted = false;
  let resolutions = 0;
  host.mountReadVolumes({ mount(volume) {
    expect(volume).toEqual(file.volume);
    resolutions++;
    if (!granted) throw new TypeError("owner denied mount");
    return { owner, scope };
  } });
  await expect(host.read(file)).rejects.toThrow();
  granted = true;
  expect(new TextDecoder().decode(await host.read(file))).toBe("lazy owner bytes");
  const page = await host.listPrivateDirectory(owner.volume, "notes", "notes");
  expect(page.entries).toEqual([{ name: "lazy.txt", kind: "file" }]);
  expect(page.generation.kind).toBe("generation");
  expect(page.generation.provider).toEqual(owner.volume.provider);
  const first = await host.listPrivateDirectory(owner.volume, "notes", "notes", page.generation, null, 1);
  expect(first.entries).toEqual(page.entries);
  expect(first.hasMore).toBe(false);
  await expect(host.listPrivateDirectory(owner.volume, "notes", "notes", page.generation, null,
    HARNESS_PRIVATE_DIRECTORY_PAGE_MAXIMUM + 1)).rejects.toThrow();
  expect(new TextDecoder().decode((await host.readPrivatePath(owner.volume, "notes",
    "notes/lazy.txt", page.generation)).bytes)).toBe("lazy owner bytes");
  await expect(host.listPrivateDirectory(owner.volume, "notes", "")).rejects.toThrow();
  await owner.stage("notes/later.txt", new TextEncoder().encode("later"), "text/plain", "later.txt");
  const pinned = await host.listPrivateDirectory(owner.volume, "notes", "notes", page.generation);
  expect(pinned).toEqual(page);
  const forgedGeneration = { ...page.generation,
    key: page.generation.key.map((byte, index) => index === 0 ? byte ^ 1 : byte) };
  await expect(host.listPrivateDirectory(owner.volume, "notes", "notes", forgedGeneration))
    .rejects.toThrow();
  expect(new TextDecoder().decode((await host.readPrivatePath(owner.volume, "notes",
    "notes/lazy.txt", page.generation)).bytes)).toBe("lazy owner bytes");
  expect((await host.listPrivateDirectory(owner.volume, "notes", "notes")).entries)
    .toEqual([{ name: "later.txt", kind: "file" }, { name: "lazy.txt", kind: "file" }]);
  await owner.stage("notes/lazy.txt", new TextEncoder().encode("revised"), "text/plain", "lazy.txt");
  expect(new TextDecoder().decode((await host.readPrivatePath(owner.volume, "notes",
    "notes/lazy.txt", page.generation)).bytes)).toBe("lazy owner bytes");
  expect(new TextDecoder().decode((await host.readPrivatePath(owner.volume, "notes",
    "notes/lazy.txt")).bytes)).toBe("revised");
  granted = false;
  await expect(host.read(file)).rejects.toThrow();
  expect(resolutions).toBeGreaterThan(3);
  host.free();
  owner.free();
});
