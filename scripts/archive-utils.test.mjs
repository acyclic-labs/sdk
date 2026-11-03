import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createHash } from "node:crypto";
import { gzipSync, gunzipSync } from "node:zlib";
import { fuzz, fuzzGzip, fuzzDeclared } from "./archive-utils.fuzz.mjs";
import { tarEntries } from "./archive-utils.mjs";

const seed = readFileSync(new URL("./test-support/archive-corpus/native-ustar.tar", import.meta.url));

test("native package inventory survives byte mutations and rejects removed admission checks", async () => {
  const directory = mkdtempSync(join(tmpdir(), "archive-inventory-"));
  const path = join(directory, "input.tar.gz");
  const compressed = readFileSync(new URL("./test-support/archive-corpus/native-flat.tar.gz", import.meta.url));
  const files = JSON.parse(readFileSync(new URL("./test-support/archive-corpus/native-flat.files.json", import.meta.url), "utf8"));
  const hash = bytes => createHash("sha256").update(bytes).digest("hex");
  const digest = hash(compressed);
  const wrong = "0".repeat(64);
  try {
    const accepted = fuzzDeclared(compressed, path, files, digest, seed.length);
    const listed = readFileSync(new URL("./test-support/archive-corpus/native-flat.list", import.meta.url), "utf8");
    assert.deepEqual([...accepted.payload.keys()], listed.trimEnd().split(/\r?\n/));
    for (let offset = 0; offset < compressed.length; offset++) {
      const bytes = Buffer.from(compressed);
      bytes[offset] ^= 0x80;
      fuzzDeclared(bytes, path, files, hash(bytes), seed.length);
    }
    const mutations = [
      ['if (sha256(compressed) !== expectedDigest) throw new Error("archive digest differs");', compressed, files, wrong],
      ['if (sha256(entry.body) !== files[entry.path]) throw new Error("archive payload digest differs");', compressed, { ...files, "body.txt": wrong }, digest],
      ['if (payload.size !== Object.keys(files).length) throw new Error("archive payload inventory differs");', compressed, { ...files, "absent.txt": wrong }, digest],
    ];
    const memberCheck = 'if (entry.type !== "0" || payload.has(entry.path) || !Object.hasOwn(files, entry.path)) {\n      throw new Error("unsafe, duplicate or unexpected archive member");\n    }';
    const directoryBytes = gzipSync(seed);
    const directoryFiles = Object.fromEntries(tarEntries(seed).map(entry => [entry.path, hash(entry.body)]));
    mutations.push([memberCheck, directoryBytes, directoryFiles, hash(directoryBytes)]);
    const flat = gunzipSync(compressed);
    // Duplicate the original first member without changing its authenticated
    // header or body; only the negative input differs from the native capture.
    const duplicate = gzipSync(Buffer.concat([flat.subarray(0, 1024), flat]));
    mutations.push([memberCheck, duplicate, files, hash(duplicate)]);
    for (const [removed, bytes, inventory, expected] of mutations) {
      assert.equal(fuzzDeclared(bytes, path, inventory, expected, seed.length + 1024), false);
      const mutant = await planted(removed);
      assert.throws(() => mutant.fuzzDeclared(bytes, path, inventory, expected, seed.length + 1024), assert.AssertionError);
    }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("native gzip corpus exercises actual file admission and both byte bounds", async () => {
  const directory = mkdtempSync(join(tmpdir(), "archive-admission-"));
  const path = join(directory, "input.tar.gz");
  try {
    const compressed = readFileSync(new URL("./test-support/archive-corpus/native-ustar.tar.gz", import.meta.url));
    assert.deepEqual(fuzzGzip(compressed, path, seed.length).expanded, seed);
    for (let offset = 0; offset < compressed.length; offset++) {
      fuzzGzip(compressed.subarray(0, offset), path, seed.length);
      const bytes = Buffer.from(compressed);
      bytes[offset] ^= 0x80;
      fuzzGzip(bytes, path, seed.length);
    }
    for (const removed of [
      'if (size <= 0 || size > maxArchiveBytes) throw new Error("archive exceeds its size bound");',
      'if (expanded.length > maxExpandedBytes) throw new Error("archive expands beyond its size bound");',
    ]) {
      const mutant = await planted(removed);
      assert.throws(() => mutant.fuzzGzip(compressed, path, seed.length), assert.AssertionError);
    }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("native TAR corpus and byte mutations exercise the production decoder", () => {
  assert.equal(fuzz(seed), true);
  const listed = readFileSync(new URL("./test-support/archive-corpus/native-ustar.list", import.meta.url), "utf8");
  assert.deepEqual(tarEntries(seed).map(entry => entry.path), listed.trimEnd().split(/\r?\n/));
  // Every header byte and body/trailer boundary is reachable from the real
  // producer output, including the final member and a one-block end marker.
  for (let offset = 0; offset < seed.length; offset += 512) {
    assert.equal(fuzz(seed.subarray(0, offset)), false);
    for (let field = 0; field < 512; field++) {
      const bytes = Buffer.from(seed);
      bytes[offset + field] ^= 0x80;
      fuzz(bytes);
    }
  }
  // Keep header authentication valid to reach size/name/type branches rather
  // than having every field mutation stop at the checksum boundary.
  for (const [start, length, value] of [[124, 12, ""], [124, 12, "invalid"],
    [124, 12, "00000010000"], [0, 100, "a".repeat(100)], [156, 1, ""]]) {
    const bytes = Buffer.from(seed);
    bytes.fill(0, start, start + length);
    bytes.write(value, start, length, "ascii");
    seal(bytes);
    fuzz(bytes);
  }
});

function seal(bytes) {
  bytes.fill(32, 148, 156);
  const sum = bytes.subarray(0, 512).reduce((total, byte) => total + byte, 0);
  bytes.write(`${sum.toString(8).padStart(6, "0")}\0 `, 148, 8, "ascii");
}

test("the unchanged fuzz oracle rejects removed checksum and end-marker checks", async () => {
  const mutations = [
    [
      'if (checksum !== Number.parseInt(rawChecksum, 8)) throw new Error("archive member checksum mismatch");',
      bytes => { bytes[0] ^= 1; return bytes; },
    ],
    [
      'if (offset + 1024 > tar.length || !tar.subarray(offset).every(byte => byte === 0)) {\n        throw new Error("archive has an invalid end marker");\n      }',
      bytes => bytes.subarray(0, bytes.length - 512),
    ],
  ];
  for (const [removed, corrupt] of mutations) {
    const mutant = await planted(removed);
    assert.throws(() => mutant.fuzz(corrupt(Buffer.from(seed))), assert.AssertionError);
  }
});

async function planted(removed) {
  const source = readFileSync(new URL("./archive-utils.mjs", import.meta.url), "utf8").replaceAll("\r\n", "\n");
  const target = readFileSync(new URL("./archive-utils.fuzz.mjs", import.meta.url), "utf8");
  assert.ok(source.includes(removed), "planted defect no longer matches production");
  const production = `data:text/javascript;base64,${Buffer.from(source.replace(removed, "")).toString("base64")}`;
  return import(`data:text/javascript;base64,${Buffer.from(target.replace('"./archive-utils.mjs"', JSON.stringify(production))).toString("base64")}`);
}
