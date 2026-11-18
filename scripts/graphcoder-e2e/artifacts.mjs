import assert from "node:assert/strict";
import { constants, createWriteStream } from "node:fs";
import { mkdir, open, readFile, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { extname, isAbsolute, join } from "node:path";
import { pipeline } from "node:stream/promises";
import { Transform } from "node:stream";
import { fileURLToPath } from "node:url";
import { verifyNativeImage } from "../verify-release-binary.mjs";

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
    const kind = spec.kind ?? "executable";
    assert.ok(["executable", "cdylib"].includes(kind), "unknown Cargo artifact kind");
    const matches = records.filter(record => record.reason === "compiler-artifact"
      && record.target.name === spec.target && record.profile.test === spec.test
      && (kind === "executable" ? record.executable : record.target.crate_types?.includes("cdylib")));
    assert.equal(matches.length, 1, `need one Cargo ${kind} for ${spec.name}`);
    const sources = kind === "executable" ? [matches[0].executable]
      : matches[0].filenames.filter(path => [".so", ".dylib", ".dll"].includes(extname(path)));
    assert.equal(sources.length, 1, `need one Cargo ${kind} file for ${spec.name}`);
    assert.ok(isAbsolute(sources[0]), "Cargo artifact must be absolute");
    return { name: spec.name, source: sources[0], kind };
  });
  await mkdir(destination); // Reusing a previous job's bundle is forbidden.
  const artifacts = {};
  for (const { name, source, kind } of selected) {
    const path = join(destination, name), digest = createHash("sha256");
    const input = await open(source, constants.O_RDONLY);
    try {
      await pipeline(input.createReadStream({ autoClose: false }), new Transform({
        transform(bytes, encoding, callback) { digest.update(bytes); callback(null, bytes); },
      }), createWriteStream(path, { flags: "wx", mode: 0o500 }));
    } finally { await input.close(); }
    const sha256 = digest.digest("hex");
    let nativeTarget;
    try { nativeTarget = verifyNativeImage(await readFile(path)); }
    catch (error) {
      // Retain the staged bytes for diagnosis. No runnable manifest exists.
      const verifier = fileURLToPath(new URL("../verify-release-binary.mjs", import.meta.url));
      const recovery = [process.execPath, verifier, "--native", path];
      throw new Error(`kind=ARTIFACT_IMAGE step=staged-native-image name=${name} source=${JSON.stringify(source)} retained=${JSON.stringify(path)} sha256=${sha256} detail=${JSON.stringify(error.message)} recovery=${JSON.stringify(recovery)}`, { cause: error });
    }
    artifacts[name] = { path, source, kind, sha256, nativeTarget };
  }
  const manifest = { artifacts };
  await writeFile(join(destination, "artifacts.json"), JSON.stringify(manifest, null, 2) + "\n", { flag: "wx" });
  return manifest;
}

if (import.meta.main) {
  const [specification, destination, ...extra] = process.argv.slice(2);
  assert.ok(specification && destination && !extra.length,
    "usage: node artifacts.mjs SPECIFICATION.json FRESH_ABSOLUTE_DIRECTORY");
  const inputs = JSON.parse(await readFile(specification, "utf8"));
  for (const input of inputs) input.records = await readFile(input.cargo_json, "utf8");
  console.log(JSON.stringify(await stageArtifacts(inputs, destination), null, 2));
}
