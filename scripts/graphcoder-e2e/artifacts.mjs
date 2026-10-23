import assert from "node:assert/strict";
import { constants, createWriteStream } from "node:fs";
import { mkdir, open, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { isAbsolute, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import { Transform } from "node:stream";
import { fileURLToPath } from "node:url";

// Cargo JSON is the only selector. Once selected, an open descriptor owns each
// input while it is copied out of the disposable target cache. Consumers receive
// the manifest only after every immutable job-local artifact is complete.
export async function stageArtifacts(specifications, destination) {
  assert.ok(isAbsolute(destination), "artifact destination must be absolute");
  assert.equal(new Set(specifications.map(spec => spec.name)).size, specifications.length,
    "artifact identities must be unique");
  assert.ok(specifications.length > 0, "artifact selection is empty");
  const selected = specifications.map(spec => {
    assert.match(spec.name, /^[a-zA-Z0-9_.-]+$/, "unsafe artifact identity");
    assert.ok(spec.name !== "." && spec.name !== ".." && spec.name !== "artifacts.json",
      "reserved artifact identity");
    const records = spec.records.trim().split(/\r?\n/).filter(Boolean).map(JSON.parse);
    const matches = records.filter(record => record.reason === "compiler-artifact"
      && record.target.name === spec.target && record.profile.test === spec.test && record.executable);
    assert.equal(matches.length, 1, `need one Cargo executable for ${spec.name}`);
    assert.ok(isAbsolute(matches[0].executable), "Cargo executable must be absolute");
    return { name: spec.name, source: matches[0].executable };
  });
  await mkdir(destination); // Reusing a previous job's bundle is forbidden.
  const artifacts = {};
  for (const { name, source } of selected) {
    const path = join(destination, name), digest = createHash("sha256");
    const input = await open(source, constants.O_RDONLY);
    try {
      await pipeline(input.createReadStream({ autoClose: false }), new Transform({
        transform(bytes, encoding, callback) { digest.update(bytes); callback(null, bytes); },
      }), createWriteStream(path, { flags: "wx", mode: 0o500 }));
    } finally { await input.close(); }
    artifacts[name] = { path, source, sha256: digest.digest("hex") };
  }
  const manifest = { artifacts };
  await writeFile(join(destination, "artifacts.json"), JSON.stringify(manifest, null, 2) + "\n", { flag: "wx" });
  return manifest;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const [specification, destination, ...extra] = process.argv.slice(2);
  assert.ok(specification && destination && !extra.length,
    "usage: node artifacts.mjs SPECIFICATION.json FRESH_ABSOLUTE_DIRECTORY");
  const inputs = JSON.parse(await readFile(specification, "utf8"));
  for (const input of inputs) input.records = await readFile(input.cargo_json, "utf8");
  console.log(JSON.stringify(await stageArtifacts(inputs, destination), null, 2));
}
