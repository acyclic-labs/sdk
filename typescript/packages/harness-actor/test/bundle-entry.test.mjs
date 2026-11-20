import { afterAll, beforeAll, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { bundleHarnessActor } from "../../../../scripts/build-harness-actor.mjs";
import { loadHostedArtifact } from "../../../../scripts/artifact-assets.mjs";
import { fixtureAuthority, initializeFixtureAuthority } from "../../../../rust/crates/conformance/cloudflare/fixture-authority.mjs";
import { HARNESS_BINDINGS, HARNESS_MODULE, MODEL_FETCH } from "../src/bindings.js";

// Assembly consumes prepared bindings; no compiler runs in this test.
const directory = mkdtempSync(join(tmpdir(), "harness-entry-"));
beforeAll(() => bundleHarnessActor(fileURLToPath(new URL("./owner.json", import.meta.url)), directory));
afterAll(() => rmSync(directory, { recursive: true, force: true }));
test("built namespace bundle loads through manifest, onRecord and acceptsInterrupt with one admitted heap", async () => {
  const deployment = JSON.parse(readFileSync(join(directory, "agent-deploy.json"), "utf8"));
  const assets = deployment.environments[0].entries[0].assets;
  const files = new Map(assets.map(asset => [asset.path, readFileSync(join(directory, asset.path))]));
  const module = await loadHostedArtifact(join(directory, HARNESS_MODULE.file), assets, files);
  // The host distinguishes products and obtains readiness through these exports.
  expect(typeof module.onRecord).toBe("function");
  expect(typeof module.manifest).toBe("function");
  expect(typeof module.acceptsInterrupt).toBe("function");
  expect("actor" in module).toBe(false);
  await expect(module.manifest()).rejects.toMatchObject({ code: "failed_precondition", subject: "environment" });
  await expect(module.acceptsInterrupt(Uint8Array.of(0), new AbortController().signal))
    .rejects.toMatchObject({ code: "failed_precondition", subject: "environment" });
  await initializeFixtureAuthority();
  const owner = fixtureAuthority("00112233-4455-6677-8899-aabbccddeeff");
  const unexpected = () => { throw new Error("readiness performed a delivery operation"); };
  const env = {
    [HARNESS_BINDINGS.authority]: owner.capability,
    [HARNESS_BINDINGS.objects]: { open: unexpected },
    [HARNESS_BINDINGS.streams]: { dispatch: unexpected, open: unexpected },
    [HARNESS_BINDINGS.workspace]: { admit: unexpected },
    [MODEL_FETCH]: () => ({ credentialScope: new Uint8Array(32).fill(1), fetch: unexpected }),
  };
  try {
    const readiness = await module.manifest({ env });
    expect(readiness).toBeInstanceOf(Uint8Array);
    expect(readiness.byteLength).toBeGreaterThan(0);
    expect(await module.manifest()).toEqual(readiness);
    // A second readiness call cannot substitute a different owner or guest.
    expect(await module.manifest({ get env() { throw new Error("heap reopened"); } })).toEqual(readiness);
    // The actual built entry reaches the shared Rust strict-wire decoder.
    await expect(module.onRecord(Uint8Array.of(0), new AbortController().signal)).rejects.toThrow("non-canonical guest delivery");
    await expect(module.acceptsInterrupt(Uint8Array.of(0), new AbortController().signal)).rejects.toThrow("non-canonical guest interrupt");
  } finally { owner.free(); }
});

test("the deployed scripted configuration opens without a model credential or egress binding", async () => {
  const fixture = new URL("../../../../rust/crates/conformance/fixtures/", import.meta.url);
  const config = fileURLToPath(new URL("owner.scripted.json", fixture));
  const content = readFileSync(new URL("scripted-swarm.v1.json", fixture));
  expect(JSON.parse(readFileSync(config, "utf8")).model.spec.script.sha256)
    .toBe(createHash("sha256").update(content).digest("hex"));
  const artifact = join(directory, "swarm-harness");
  await bundleHarnessActor(config, artifact);
  const deployment = JSON.parse(readFileSync(join(artifact, "agent-deploy.json"), "utf8"));
  const environment = deployment.environments[0];
  const entry = environment.entries[0];
  expect(entry.fetch).toBeNull();
  expect(entry.secrets).toEqual([]);
  expect(environment.secrets).toEqual([]);
  const files = new Map(entry.assets.map(asset => [asset.path, readFileSync(join(artifact, asset.path))]));
  const module = await loadHostedArtifact(join(artifact, HARNESS_MODULE.file), entry.assets, files);
  await initializeFixtureAuthority();
  const owner = fixtureAuthority("00112233-4455-6677-8899-aabbccddeeff");
  const unexpected = () => { throw new Error("readiness performed a delivery operation"); };
  try {
    const readiness = await module.manifest({ env: {
      [HARNESS_BINDINGS.authority]: owner.capability,
      [HARNESS_BINDINGS.objects]: { open: unexpected },
      [HARNESS_BINDINGS.streams]: { dispatch: unexpected, open: unexpected },
      [HARNESS_BINDINGS.workspace]: { admit: unexpected },
      get [MODEL_FETCH]() { return unexpected(); },
    } });
    expect(readiness.byteLength).toBeGreaterThan(0);
  } finally { owner.free(); }
});
