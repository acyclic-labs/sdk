import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { gunzipSync } from "node:zlib";
import { collectBody, isCanonicalRelativePath, readArchivePayload, readPayloadEntries, readTarGzip, visitTar, wasmArtifactFiles, wasmArtifactInventory } from "./archive-utils.mjs";

// The caller records its actual file writes independently of enumeration.
// Invalid trees must refuse, including aliases whose target is a valid tree.
export function fuzzArtifactTree(directory, expected, options) {
  if (expected === null) {
    assert.throws(() => [...wasmArtifactFiles(directory, options)], /WASM artifact admission failed .*; recovery:/);
    return;
  }
  const files = [...wasmArtifactFiles(directory, options)];
  const order = (a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
  assert.deepEqual([...files].sort(order), [...expected].sort(order));
  const inventory = wasmArtifactInventory(files);
  assert.deepEqual(inventory, [...expected].sort(order).map(([path, bytes]) => ({
    path, bytes: bytes.length, sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
  })));
  assert.deepEqual(wasmArtifactInventory(files.toReversed()), inventory);
  return inventory;
}

export async function fuzzPayload(data, caseInsensitive) {
  const bytes = Buffer.from(data);
  let result;
  try {
    result = await readPayloadEntries(visit => visitTar([bytes], visit), { canonical: isCanonicalRelativePath, caseInsensitive });
  } catch (error) { return refused(error); }
  assert.equal(await fuzz(bytes), true);
  const entries = [];
  await visitTar([bytes], async entry => entries.push({ ...entry, body: await collectBody(entry) }));
  assert.ok(result.payload.size > 0, "an admitted payload must contain a file");
  assert.deepEqual([...result.payload.keys()], entries.filter(entry => entry.type === "0").map(entry => entry.path));
  const names = entries.map(entry => entry.type === "5" ? entry.path.replace(/\/$/, "") : entry.path);
  assert.ok(names.every(isCanonicalRelativePath), "each admitted name must satisfy the caller's policy");
  assert.equal(new Set(caseInsensitive ? names.map(name => name.toLowerCase()) : names).size, entries.length);
  for (const entry of entries) {
    assert.ok(["0", "5"].includes(entry.type));
    if (entry.type === "0") {
      const actual = result.payload.get(entry.path);
      assert.deepEqual(actual, entry.body);
      if (actual.length) {
        assert.equal(actual.buffer, bytes.buffer, "a contiguous body must retain its input");
        assert.equal(actual.byteOffset, entry.body.byteOffset);
      }
    }
  }
  return result.payload;
}

export async function fuzzDeclared(data, path, files, digest) {
  writeFileSync(path, data);
  let result;
  try {
    result = await readArchivePayload(path, { canonical: isCanonicalRelativePath, declared: files, expected: digest });
  } catch (error) { return refused(error); }
  const hash = bytes => createHash("sha256").update(bytes).digest("hex");
  assert.equal(hash(data), digest);
  assert.equal(result.digest, digest);
  assert.deepEqual([...result.payload.keys()].sort(), Object.keys(files).sort());
  const expanded = gunzipSync(data);
  assert.equal(await fuzz(expanded), true);
  let count = 0;
  await visitTar([expanded], async entry => {
    count++;
    assert.equal(entry.type, "0");
    const body = await collectBody(entry);
    assert.deepEqual(result.payload.get(entry.path), body);
    assert.equal(hash(body), files[entry.path]);
  });
  assert.equal(count, result.payload.size);
  return result;
}

export async function fuzzGzip(data, path) {
  writeFileSync(path, data);
  let result;
  try { result = await readTarGzip(path, () => {}); }
  catch (error) { return refused(error); }
  const expanded = gunzipSync(data);
  assert.equal(await fuzz(expanded), true);
  assert.equal(result.digest, createHash("sha256").update(data).digest("hex"));
  return { ...result, expanded };
}

function refused(error) {
  assert.equal(error.constructor, Error);
  assert.ok(["Z_DATA_ERROR", "Z_BUF_ERROR"].includes(error.code)
    || /^archive (member |has |is |offset |must |inventory |digest |payload |evidence )/.test(error.message)
    || error.message === "unsafe or duplicate archive member", error);
  return false;
}

// Exercise the production streaming decoder with native TAR captures and their
// mutations. Body chunks must borrow the input and advance within its length.
export async function fuzz(data) {
  const bytes = Buffer.from(data);
  let offset = 0, visited = 0;
  try {
    await visitTar([bytes], async ({ body, size, path, type }) => {
      const header = Buffer.from(bytes.subarray(offset, offset + 512));
      const expected = Number.parseInt(header.subarray(148, 156).toString("utf8").replace(/\0/g, "").trim(), 8);
      header.fill(32, 148, 156);
      assert.equal(header.reduce((sum, byte) => sum + byte, 0), expected, "unauthenticated header");
      assert.ok(++visited <= Math.floor(bytes.length / 512));
      assert.equal(typeof path, "string");
      assert.equal(type.length, 1);
      assert.ok(Number.isSafeInteger(size) && size >= 0);
      let consumed = 0;
      for await (const chunk of body) {
        assert.ok(chunk.length > 0);
        assert.equal(chunk.buffer, bytes.buffer);
        assert.equal(chunk.byteOffset, bytes.byteOffset + offset + 512 + consumed);
        consumed += chunk.length;
        assert.ok(consumed <= size);
      }
      assert.equal(consumed, size);
      offset += 512 + Math.ceil(size / 512) * 512;
    });
  } catch (error) {
    assert.equal(error.constructor, Error);
    assert.match(error.message, /^archive (member |has an invalid end marker|is truncated or lacks an end marker|offset is not representable)/);
    return false;
  }
  assert.ok(offset + 1024 <= bytes.length);
  assert.ok(bytes.subarray(offset).every(byte => byte === 0));
  return true;
}
