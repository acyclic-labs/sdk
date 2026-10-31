import assert from "node:assert/strict";
import test from "node:test";
import { parseNpmView, publishNativeAssembly } from "./publish-npm-packages.mjs";

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

// These are the deleted source mechanisms, not fabricated registry responses.
// Reintroducing either cadence must fail even behind its old helper name.
test("the source rule rejects the deleted registry visibility windows", async () => {
  const { newPollingSites } = await import("./no-polling.mjs");
  const { readFileSync } = await import("node:fs");
  for (const [file, removed] of [
    ["publish-npm-packages.mjs", `
async function waitForPublishedExact(name, version) {
  const deadline = Date.now() + 300_000;
  while (true) {
    if (await visible(name, version)) return;
    if (Date.now() >= deadline) throw new Error("not visible");
    await delay(5_000);
  }
}`],
    ["publish-cargo-crates.mjs", `
async function waitForRegistry(name, version) {
  for (let attempt = 0; attempt < 90; attempt += 1) {
    if (await registryVersion(name, version)) return;
    await new Promise(resolveDelay => setTimeout(resolveDelay, 2_000));
  }
}`],
  ]) {
    const path = `scripts/${file}`;
    const current = readFileSync(new URL(file, import.meta.url), "utf8");
    const sites = newPollingSites(path, current, `${current}\n${removed}`);
    assert.ok(sites.some(site => site.kind === "timer-or-readiness" || site.kind === "loop"), file);
    assert.doesNotMatch(current, /waitForPublishedExact|waitForRegistry|setTimeout|node:timers|timeoutMs/);
  }
});
