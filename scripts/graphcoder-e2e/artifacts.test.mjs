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

test("one Cargo admission stages native libraries on every OS without guessing target paths", async () => {
  const root = await mkdtemp(join(tmpdir(), "e2e-library-custody-"));
  try {
    for (const extension of [".so", ".dylib", ".dll"]) {
      const source = join(root, `actual-library${extension}`), bytes = `library bytes ${extension}`;
      await writeFile(source, bytes);
      const record = { reason: "compiler-artifact", target: { name: "native_library", crate_types: ["cdylib"] },
        profile: { test: false }, executable: null,
        filenames: [source, `${source}.pdb`, `${source}.lib`, `${source}.a`] };
      const spec = { name: "binding.node", target: "native_library", test: false,
        kind: "cdylib", records: JSON.stringify(record) };
      const manifest = await stageArtifacts([spec], join(root, `bundle${extension}`));
      await rm(source);
      const artifact = manifest.artifacts["binding.node"];
      assert.equal(await readFile(artifact.path, "utf8"), bytes);
      assert.equal(artifact.sha256, createHash("sha256").update(bytes).digest("hex"));
      assert.equal(artifact.kind, "cdylib");
      assert.deepEqual(JSON.parse(await readFile(join(root, `bundle${extension}`, "artifacts.json"), "utf8")), manifest);
      for (const records of ["", spec.records + "\n" + spec.records,
        JSON.stringify({ ...record, target: { ...record.target, crate_types: ["rlib"] } }),
        JSON.stringify({ ...record, filenames: ["relative-library" + extension] }),
        JSON.stringify({ ...record, filenames: [source + ".lib"] }),
        JSON.stringify({ ...record, filenames: [source, source + extension] }),
        JSON.stringify({ ...record, profile: { test: true } })]) {
        await assert.rejects(stageArtifacts([{ ...spec, records }], join(root, "invalid")));
      }
      await assert.rejects(stageArtifacts([{ ...spec, kind: "guessed-library" }], join(root, "invalid")), /unknown Cargo artifact kind/);
    }
  } finally { await rm(root, { recursive: true, force: true }); }
});
