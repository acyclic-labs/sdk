import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fuzz } from "./archive-utils.fuzz.mjs";
import { tarEntries } from "./archive-utils.mjs";

const seed = readFileSync(new URL("./test-support/archive-corpus/native-ustar.tar", import.meta.url));

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
  const source = readFileSync(new URL("./archive-utils.mjs", import.meta.url), "utf8");
  const target = readFileSync(new URL("./archive-utils.fuzz.mjs", import.meta.url), "utf8");
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
    const normalized = source.replaceAll("\r\n", "\n");
    assert.ok(normalized.includes(removed), "planted defect no longer matches production");
    const production = `data:text/javascript;base64,${Buffer.from(normalized.replace(removed, "")).toString("base64")}`;
    const module = target.replace('"./archive-utils.mjs"', JSON.stringify(production));
    const mutant = await import(`data:text/javascript;base64,${Buffer.from(module).toString("base64")}`);
    assert.throws(() => mutant.fuzz(corrupt(Buffer.from(seed))), assert.AssertionError);
  }
});
