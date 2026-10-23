import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { stageArtifacts } from "./artifacts.mjs";
import { Processes } from "./process.mjs";

test("Cargo-selected artifacts retain custody after target-cache eviction", async () => {
  const root = await mkdtemp(join(tmpdir(), "e2e-artifact-custody-"));
  const input = join(root, "actual cargo artifact.mjs"), code = 'console.log("actual artifact survived");\n';
  const record = { reason: "compiler-artifact", target: { name: "host-proof" },
    profile: { test: true }, executable: input };
  const spec = { name: "proof.mjs", target: "host-proof", test: true, records: JSON.stringify(record) };
  const processes = new Processes();
  try {
    await writeFile(input, code);
    const manifest = await stageArtifacts([spec], join(root, "bundle"));
    await rm(input); // Real cache eviction after publication, before execution.
    const before = await processes.run(process.execPath, [input]);
    assert.notEqual(before.code, 0, "old cache-path handoff unexpectedly survived eviction");
    const artifact = manifest.artifacts["proof.mjs"];
    const result = await processes.run(process.execPath, [artifact.path]);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(result.stdout, "actual artifact survived\n");
    assert.equal(artifact.sha256, createHash("sha256").update(code).digest("hex"));
    assert.deepEqual(JSON.parse(await readFile(join(root, "bundle", "artifacts.json"), "utf8")), manifest);
    await assert.rejects(stageArtifacts([spec], join(root, "bundle")), /EEXIST/);
    for (const records of ["", spec.records + "\n" + spec.records,
      JSON.stringify({ ...record, executable: "relative-artifact" }),
      JSON.stringify({ ...record, profile: { test: false } })]) {
      await assert.rejects(stageArtifacts([{ ...spec, records }], join(root, "invalid")));
    }
  } finally { await processes.close(); await rm(root, { recursive: true, force: true }); }
});
