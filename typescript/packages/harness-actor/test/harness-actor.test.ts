import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import initializeManifest, { validate, verifyArtifact } from "../../workers/generated/wasm/acyclic_manifest.js";
import { ACCOUNT_LEAF, HARNESS_BINDINGS, HARNESS_MODULE, MACHINES_ENDPOINT, MODEL_FETCH, OBJECTS_ENDPOINT,
  STREAMS_ENDPOINT } from "../src/bindings.js";
import { harnessManifest, HarnessManifestError, MODEL_SECRET } from "../src/manifest.js";
import owner from "./owner.json";

await initializeManifest({ module_or_path: await readFile(new URL("../../workers/generated/wasm/acyclic_manifest_bg.wasm", import.meta.url)) });
const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const entry = new TextEncoder().encode("export function manifest() {} export function onRecord() {}");
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
  expect(manifest().environments[0]!.secrets).toEqual([{ name: MODEL_SECRET, reference: { scope: "models", name: "anthropic", version: "1" } }]);
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

test("the Machines, Objects and Streams endpoints and their credential stay host-only", () => {
  const [declared] = manifest().environments[0]!.entries;
  const cell = { host: "machines.example.com", port: 443 };
  expect(declared!.hostFetch).toEqual([{ name: MACHINES_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF },
    { name: OBJECTS_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF },
    { name: STREAMS_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF }]);
  // The guest selects the `objects` and `sharedStreams` bindings, never their endpoints or the leaf.
  expect(declared!.bindings).toContain(HARNESS_BINDINGS.objects);
  expect(declared!.bindings).toContain(HARNESS_BINDINGS.streams);
  expect(declared!.bindings).not.toContain(OBJECTS_ENDPOINT);
  expect(declared!.bindings).not.toContain(STREAMS_ENDPOINT);
  expect(manifest().environments[0]!.secrets.map(secret => secret.reference)).not.toContain(ACCOUNT_LEAF.reference);
  // A Machines bearer leaves the Objects and Streams endpoints on the account leaf.
  const bearer = { scheme: "bearer", reference: "secrets:/machines/token" } as const;
  const own = harnessManifest({ ...deployment, machines: { ...deployment.machines, credential: bearer } },
    spec, volumes, sha256(entry), [asset]);
  validate(JSON.stringify(own));
  expect(own.environments[0]!.entries[0]!.hostFetch).toEqual([{ name: MACHINES_ENDPOINT, origin: cell, credential: bearer },
    { name: OBJECTS_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF },
    { name: STREAMS_ENDPOINT, origin: cell, credential: ACCOUNT_LEAF }]);
  const exposed = (machines: Parameters<typeof harnessManifest>[0]["machines"]) =>
    () => validate(JSON.stringify(harnessManifest({ ...deployment, machines }, spec, volumes, sha256(entry), [asset])));
  // Negative controls: the guest's model egress reaches the origin, or its model
  // secret is the bearer's variable at any version.
  expect(exposed({ ...deployment.machines, origin: "https://api.anthropic.com" })).toThrow(": host-fetch-exposed");
  expect(exposed({ ...deployment.machines,
    credential: { scheme: "bearer", reference: { scope: "models", name: "anthropic", version: "2" } } }))
    .toThrow(": host-fetch-exposed");
  const leaf = { ...spec, authentication: { api_key_secret: { secret_reference: { scope: "account", name: "leaf", version: "3" } } } };
  expect(() => validate(JSON.stringify(harnessManifest(deployment, leaf, volumes, sha256(entry), [asset]))))
    .toThrow(": host-fetch-exposed");
  expect(() => harnessManifest({ ...deployment, machines: { ...deployment.machines, origin: "http://machines.example.com" } },
    spec, volumes, sha256(entry), [asset])).toThrow("https-origin");
});

test("an inline credential never decodes as a secret reference", () => {
  const inline = { ...spec, authentication: { api_key_secret: { secret_reference: "sk-ant-api03-inline" } } };
  expect(() => validate(JSON.stringify(harnessManifest(deployment, inline as never, volumes, sha256(entry), [asset]))))
    .toThrow(": invalid-json-or-contract");
});
