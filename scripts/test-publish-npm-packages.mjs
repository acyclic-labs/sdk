import assert from "node:assert/strict";
import test from "node:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { registryFixture } from "./fixtures/registry.mjs";
import { PublicationPending, requirePublication } from "./publication-receipt.mjs";
import { parseNpmView, publishExact, publishNativeAssembly, verifyPublicationAttempt, reconcilePublishedExact } from "./publish-npm-packages.mjs";

test("native parent publication waits for every exact companion and stops on failure", async () => {
  const assembly = { companions: ["linux", "mac", "windows"].map(name => ({ name, version: "0.2.0", asset: `${name}.tgz` })), parent: { name: "parent", version: "0.2.0", asset: "parent.tgz" } };
  const published = [];
  await publishNativeAssembly(assembly, "/qualified", async name => { published.push(name); });
  assert.deepEqual(published, ["linux", "mac", "windows", "parent"]);
  for (const failed of ["linux", "mac", "windows", "parent"]) {
    const attempted = [];
    await assert.rejects(publishNativeAssembly(assembly, "/qualified", async name => {
      attempted.push(name);
      if (name === failed) throw new Error(`exact publication failed: ${name}`);
    }), /exact publication failed/);
    assert.deepEqual(attempted, published.slice(0, published.indexOf(failed) + 1));
  }
});

test("npm view tolerates an unpublished or partially visible field", () => {
  assert.equal(parseNpmView({ status: 0, stdout: "", stderr: "" }, "package"), null);
  assert.equal(parseNpmView({ status: 1, stdout: "", stderr: "npm ERR! E404" }, "package"), null);
  assert.equal(parseNpmView({ status: 0, stdout: '"sha512-exact"', stderr: "" }, "package"), "sha512-exact");
  assert.throws(
    () => parseNpmView({ status: 1, stdout: "", stderr: "npm ERR! E500" }, "package"),
    /could not inspect package/,
  );
});

test("real npm observations retain pending receipts and reconcile once without republishing", { timeout: 20_000 }, async t => {
  const registry = await registryFixture(t);
  const directory = mkdtempSync(join(tmpdir(), "acyclic-registry-receipt-"));
  const before = { registry: process.env.NPM_CONFIG_REGISTRY, cache: process.env.NPM_CONFIG_CACHE };
  process.env.NPM_CONFIG_REGISTRY = registry.url;
  process.env.NPM_CONFIG_CACHE = join(directory, "cache");
  t.after(() => {
    for (const [name, value] of [["NPM_CONFIG_REGISTRY", before.registry], ["NPM_CONFIG_CACHE", before.cache]]) {
      if (value === undefined) delete process.env[name]; else process.env[name] = value;
    }
    rmSync(directory, { recursive: true, force: true });
  });
  const receipts = join(directory, "receipts");
  const readsOnly = async () => {
    const observations = await registry.observations();
    assert.ok(observations.length > 0);
    assert.ok(observations.every(observation => observation.method === "GET"), JSON.stringify(observations));
    t.diagnostic(`one explicit observation: ${JSON.stringify(observations)}`);
  };
  const inspect = () => reconcilePublishedExact("example", "0.1.5", "sha512-exact", { sourceSha: "a".repeat(40) });
  await registry.set({ error: "not found" }, 404);
  const pending = await inspect();
  assert.equal(pending.state, "Pending");
  await readsOnly();
  assert.throws(() => requirePublication(pending, receipts, "explicit reconcile"), PublicationPending);
  const files = readdirSync(receipts);
  assert.equal(files.length, 1);
  assert.deepEqual(JSON.parse(readFileSync(join(receipts, files[0]), "utf8")), pending);

  const manifest = (digest, latest) => ({ name: "example", "dist-tags": { latest }, versions: {
    "0.1.5": { name: "example", version: "0.1.5", dist: { integrity: digest } },
  } });
  await registry.set(manifest("sha512-exact", "0.1.4"));
  const staleTag = await inspect();
  assert.equal(staleTag.state, "Pending");
  await readsOnly();
  assert.throws(() => requirePublication(staleTag, receipts, "explicit reconcile"), PublicationPending);
  await registry.set(manifest("sha512-other", "0.1.5"));
  await assert.rejects(inspect(), /exists with different bytes/);
  await readsOnly();
  assert.equal(JSON.parse(readFileSync(join(receipts, files[0]), "utf8")).state, "Pending");
  await registry.set(manifest("sha512-exact", "0.1.5"));
  const complete = await inspect();
  assert.equal(complete.state, "Complete");
  await readsOnly();
  requirePublication(complete, receipts, "explicit reconcile");
  assert.deepEqual(readdirSync(receipts), files);
  assert.deepEqual(JSON.parse(readFileSync(join(receipts, files[0]), "utf8")), complete);

  // Exercise the same archive-hashing entry point used by both CLI modes.
  // Reconciliation of an absent version must never submit an npm publication.
  const archive = join(directory, "qualified.tgz");
  const bytes = Buffer.from("exact original qualified archive");
  writeFileSync(archive, bytes);
  const expectedDigest = `sha512-${createHash("sha512").update(bytes).digest("base64")}`;
  const policy = { sourceSha: "b".repeat(40), reconcileOnly: true, directory: receipts, command: "explicit reconcile" };
  await registry.set({ error: "not found" }, 404);
  await assert.rejects(publishExact("example", "0.1.5", archive, policy), PublicationPending);
  await readsOnly();
  await registry.set(manifest(expectedDigest, "0.1.5"));
  await publishExact("example", "0.1.5", archive, policy);
  await readsOnly();
});

test("a duplicate publish response requires explicit reconciliation of a pending archive", async () => {
  let reads = 0;
  const pending = await verifyPublicationAttempt(
    { status: 1, stdout: "", stderr: "npm error You cannot publish over the previously published versions: 0.1.5." },
    "@acyclic-labs/harness", "0.1.5", "sha512-exact",
    {
      readIntegrity: () => { reads += 1; return null; },
      readLatest: () => assert.fail("an absent archive cannot have a verified tag"),
    },
  );
  assert.equal(reads, 1);
  assert.equal(pending.state, "Pending");
  await assert.rejects(
    verifyPublicationAttempt({ status: 1, stderr: "npm error E401 Unauthorized" }, "package", "0.1.5", "sha512-exact"),
    /npm publication failed/,
  );
});
