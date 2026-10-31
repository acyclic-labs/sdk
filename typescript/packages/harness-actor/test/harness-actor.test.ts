import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { GUEST_ABI_VERSION, materializeActorState, openGuestResponse, type GuestResponseFrame } from "@acyclic-labs/actors";
import { ActorStateStorage, initializeHosting } from "@acyclic-labs/harness/hosting";
import initializeManifest, { validate, verifyArtifact } from "../../workers/generated/wasm/acyclic_manifest.js";
import { ACCOUNT_LEAF, HARNESS_BINDINGS, HARNESS_MODULE, MACHINES_ENDPOINT, MODEL_FETCH, OBJECTS_ENDPOINT } from "../src/bindings.js";
import { actor } from "../src/harness-actor.js";
import { harnessManifest, HarnessManifestError, MODEL_SECRET } from "../src/manifest.js";
import owner from "./owner.json";

await initializeManifest({ module_or_path: await readFile(new URL("../../workers/generated/wasm/acyclic_manifest_bg.wasm", import.meta.url)) });
await initializeHosting({ module_or_path: await readFile(new URL("../../harness/generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)) });
const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const entry = new TextEncoder().encode("export const actor = {};");
const module = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
const asset = { path: `harness.${sha256(module)}.wasm`, sha256: sha256(module), size: module.length,
  mediaType: "application/wasm", role: "module" } as const;
const spec = owner.model.spec as Parameters<typeof harnessManifest>[1];
const deployment = owner.deployment as Parameters<typeof harnessManifest>[0];
const volumes = owner.options.filesystem_provider;
const manifest = (assets = [asset]) => harnessManifest(deployment, spec, volumes, sha256(entry), assets);

test("the generated manifest selects every descriptor binding and passes Rust admission", () => {
  validate(JSON.stringify(manifest()));
  const [declared] = manifest().environments[0]!.entries;
  expect(declared!.module).toBe(HARNESS_MODULE.id);
  expect(declared!.bindings).toEqual(Object.values(HARNESS_BINDINGS));
  // The platform reads the Machine's selected-volume provider from the manifest, never the bundle.
  expect(declared!.runtime).toMatchObject({ actorMachine: { binding: HARNESS_BINDINGS.workspace, volumeProvider: volumes } });
  expect(declared!.fetch).toEqual({ name: MODEL_FETCH, egress: [{ host: "api.anthropic.com", port: 443 }] });
  expect(manifest().environments[0]!.secrets).toEqual([{ name: MODEL_SECRET, reference: "secrets:/models/anthropic" }]);
});

test("the artifact is exactly the entry and its declared assets", () => {
  const verify = (assets = [asset], files = [module]) => verifyArtifact(JSON.stringify(manifest(assets)),
    deployment.environment, deployment.entry, entry, files.map(() => asset.path), files);
  verify();
  // Negative control: a manifest declaring no assets cannot carry the harness module.
  expect(() => verify([])).toThrow(": undeclared-asset");
  expect(() => verify([asset], [module.slice(4)])).toThrow(": asset-size");
  // A file without a path is refused, never dropped unchecked.
  expect(() => verifyArtifact(JSON.stringify(manifest()), deployment.environment, deployment.entry, entry,
    [asset.path], [module, module])).toThrow(": undeclared-asset");
  expect(() => verify([{ ...asset, mediaType: "application/javascript" } as never])).toThrow(": entry-asset");
});

test("a deployment without the platform authority binding is refused before admission", () => {
  const partial = { ...deployment, bindings: deployment.bindings.filter(binding => binding.name !== HARNESS_BINDINGS.authority) };
  expect(() => harnessManifest(partial, spec, volumes, sha256(entry), [asset])).toThrow(HarnessManifestError);
  expect(() => harnessManifest(partial, spec, volumes, sha256(entry), [asset])).toThrow("platformAuthority: harness-binding");
});

test("an anonymous provider declares and selects no credential", () => {
  const anonymous = harnessManifest(deployment, { ...spec, authentication: "optional" }, volumes, sha256(entry), [asset]);
  validate(JSON.stringify(anonymous));
  expect(anonymous.environments[0]!.secrets).toEqual([]);
  expect(anonymous.environments[0]!.entries[0]!.secrets).toEqual([]);
});

test("the Machines and Objects endpoints and their credential stay host-only", () => {
  const [declared] = manifest().environments[0]!.entries;
  const cell = { host: "machines.example.com", port: 443 };
  expect(declared!.hostFetch).toEqual([{ name: MACHINES_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF },
    { name: OBJECTS_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF }]);
  // The guest selects the `objects` binding, never its endpoint or the leaf.
  expect(declared!.bindings).toContain(HARNESS_BINDINGS.objects);
  expect(declared!.bindings).not.toContain(OBJECTS_ENDPOINT);
  expect(manifest().environments[0]!.secrets.map(secret => secret.reference)).not.toContain(ACCOUNT_LEAF.reference);
  // A Machines bearer leaves the Objects endpoint on the account leaf.
  const bearer = { scheme: "bearer", reference: "secrets:/machines/token" } as const;
  const own = harnessManifest({ ...deployment, machines: { ...deployment.machines, credential: bearer } },
    spec, volumes, sha256(entry), [asset]);
  validate(JSON.stringify(own));
  expect(own.environments[0]!.entries[0]!.hostFetch).toEqual([{ name: MACHINES_ENDPOINT, origin: cell, credential: bearer },
    { name: OBJECTS_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF }]);
  const exposed = (machines: Parameters<typeof harnessManifest>[0]["machines"]) =>
    () => validate(JSON.stringify(harnessManifest({ ...deployment, machines }, spec, volumes, sha256(entry), [asset])));
  // Negative controls: the guest's model egress reaches the origin, or its model secret is the bearer.
  expect(exposed({ ...deployment.machines, origin: "https://api.anthropic.com" })).toThrow(": host-fetch-exposed");
  for (const scheme of ["bearer", "account-leaf"] as const) {
    expect(exposed({ ...deployment.machines, credential: { scheme, reference: "secrets:/models/anthropic" } }))
      .toThrow(": host-fetch-exposed");
  }
  expect(() => harnessManifest({ ...deployment, machines: { ...deployment.machines, origin: "http://machines.example.com" } },
    spec, volumes, sha256(entry), [asset])).toThrow("https-origin");
});

test("an inline credential is refused by the Rust secret-reference rule", () => {
  const inline = { ...spec, authentication: { api_key_secret: { secret_reference: "sk-ant-api03-inline" } } };
  expect(() => validate(JSON.stringify(harnessManifest(deployment, inline, volumes, sha256(entry), [asset]))))
    .toThrow(": secret-reference");
});

test("the entry is a public guest ABI definition", async () => {
  expect(actor.abiVersion).toBe(GUEST_ABI_VERSION);
  expect(await actor.checkpoint(undefined as never, undefined as never))
    .toEqual({ codec: actor.codec.identity, bytes: new Uint8Array() });
});

/** The invocation's environment: its admission projects the caller's credential. */
const actorState = {};
const caller = Uint8Array.of(9, 9);
const env = { ...Object.fromEntries(Object.values(HARNESS_BINDINGS).map(name => [name, {}])),
  [HARNESS_BINDINGS.storage]: actorState,
  [HARNESS_BINDINGS.streams]: { register() {}, dispatch() {}, open() {} },
  [HARNESS_BINDINGS.access]: { credential: caller }, [MODEL_FETCH]: {} };

/** One opened Actor whose host serves the given session frames after the upload ends,
 * and records each routed agent call. */
function serving(outputs: (Uint8Array | Error)[], path = "/acyclic.stream.v1.StreamSessionService/Session",
  body: Uint8Array = new Uint8Array(5)) {
  const calls: [string, Uint8Array, Uint8Array][] = [];
  const host = {
    async grpcSession(next: (signal: AbortSignal) => Promise<Uint8Array | null>) {
      while (await next(new AbortController().signal) !== null);
      return { async next() { const output = outputs.shift() ?? null; if (output instanceof Error) throw output; return output; }, close() {} };
    },
    async fetchCall(url: string, command: Uint8Array, credential: Uint8Array) {
      calls.push([url, command, credential]);
      return Uint8Array.of(7);
    },
  };
  const opened = { with(mutate: (value: unknown) => undefined) { mutate({ host }); } };
  const request = new Request(`https://harness.invalid${path}`,
    { method: "POST", headers: { "content-type": "application/grpc" }, body });
  return { calls, response: actor.fetch!(opened as never, request, { env } as never) };
}

test("Bind and Command routed from an ActorCall reach the same call with the exact body and the caller's credential", async () => {
  for (const [path, command] of [["/agents/bind", '{"kind":"bind"}'], ["/agents/command", '{ "kind" : "root" }']] as const) {
    const body = new TextEncoder().encode(command);
    const { calls, response } = serving([], path, body);
    const reply = await response as Response;
    expect(reply.status).toBe(200);
    expect(new Uint8Array(await reply.arrayBuffer())).toEqual(Uint8Array.of(7));
    expect(calls).toEqual([[`https://harness.invalid${path}`, body, caller]]);
  }
});

test("session fetch publishes terminal gRPC trailers after the body", async () => {
  const frame = Uint8Array.of(0, 0, 0, 0, 1, 7);
  const frames: GuestResponseFrame[] = [];
  for await (const part of openGuestResponse(await serving([frame]).response).frames) frames.push(part);
  expect(frames[0]).toEqual({ data: frame });
  expect(frames).toHaveLength(2);
  expect((frames[1] as { trailers: Headers }).trailers.get("grpc-status")).toBe("0");
});

test("a session failure mid-read surfaces its original error code, in the body and the trailers", async () => {
  const response = await serving([Uint8Array.of(0, 0, 0, 0, 0), Object.assign(new Error("access_denied"), { code: "access_denied" })]).response;
  const trailers = "trailers" in response ? response.trailers : Promise.reject(new Error("no trailers"));
  const frames = openGuestResponse(response).frames[Symbol.asyncIterator]();
  expect((await frames.next()).value).toEqual({ data: Uint8Array.of(0, 0, 0, 0, 0) });
  await expect(frames.next()).rejects.toMatchObject({ code: "access_denied" });
  expect((await trailers).get("grpc-message")).toBe("access_denied");
});

test("host metadata, wake rows and journal frames commit as one actor state packet", async () => {
  const retained: Uint8Array[] = [], commits: { writes: { key: string }[]; journalFrom: string; journal: Uint8Array[]; alarm: unknown }[] = [];
  const binding = materializeActorState({
    seed: { metadata: [], journalTail: "0" },
    async commit(request) { commits.push(request); retained.push(...request.journal); },
    async readJournal(from, limit) { return retained.slice(Number(from), Number(from) + limit); },
    fence() {},
  });
  const state = new ActorStateStorage(binding);
  const wakes = state.domain("wakes");
  const journal = state.journal(changes => wakes.put("changed", JSON.stringify(changes)));
  expect(await journal.configure(Uint8Array.of(1), 64, Uint8Array.of(2))).toEqual(Uint8Array.of(2));
  const frame = new Uint8Array(18);
  expect(await journal.append(0, () => [frame], [["inbox", false]])).toBe(1);
  expect(commits).toHaveLength(2);
  expect(commits[1]!.journalFrom).toBe("0");
  expect(commits[1]!.journal).toEqual([frame]);
  expect(commits[1]!.writes.map(write => write.key)).toEqual(["journal/bytes", "wakes/changed"]);
  expect(wakes.get("changed")).toBe(JSON.stringify([["inbox", false]]));
  const [header, ...frames] = await journal.read(0, 8);
  expect(new DataView(header!.buffer).getBigUint64(0, true)).toBe(1n);
  expect(new DataView(header!.buffer).getBigUint64(8, true)).toBe(18n);
  expect(frames).toEqual([frame]);
  // A stale index admits nothing and reports the current head.
  expect(await journal.append(0, () => { throw new Error("admitted at a stale index"); }, [])).toBe(1);
  await expect(journal.configure(Uint8Array.of(9), 64, Uint8Array.of(2))).rejects.toThrow("configuration differs");
  // A reopened adapter keeps every domain's deadline: one domain's alarm cannot drop another's.
  await wakes.alarm(100);
  await new ActorStateStorage(binding).domain("actor").alarm(200);
  expect(commits.at(-1)).toMatchObject({ alarm: { type: "set", deadline: "100" } });
});
