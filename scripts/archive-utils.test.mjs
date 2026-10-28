import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import { collectBody, hashBody, readPayloadEntries, readTarGzip, sha256, visitTar } from "./archive-utils.mjs";
import { validateArchive } from "./validate-npm-package.mjs";

test("digest-only archive admission streams every chunk and rejects names before reading bodies", async () => {
  const chunk = Buffer.alloc(65536, 7), expected = createHash("sha256");
  let visited = 0;
  const result = await readPayloadEntries(async visit => {
    await visit({ path: "large", type: "0", size: chunk.length * 1025, body: (async function* () {
      for (let index = 0; index < 1025; index++) {
        visited++;
        expected.update(chunk);
        yield chunk;
      }
    })() });
  }, { canonical: name => name === "large", retain: () => false });
  assert.equal(visited, 1025);
  assert.equal(result.payload.size, 0);
  assert.equal(result.digests.get("large"), expected.digest("hex"));
  let bodyRead = false;
  await assert.rejects(readPayloadEntries(visit => visit({
    path: "../outside", type: "0", size: 1,
    body: (async function* () { bodyRead = true; yield Buffer.from("x"); })(),
  }), { canonical: () => false }), /unsafe or duplicate/);
  assert.equal(bodyRead, false);
});

function header(path, size) {
  const result = Buffer.alloc(512);
  result.write(path, 0, 100);
  result.write(`${size.toString(8).padStart(11, "0")}\0`, 124, 12);
  result[156] = 48;
  result.fill(32, 148, 156);
  const checksum = result.reduce((sum, byte) => sum + byte, 0);
  result.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148, 8);
  return result;
}

function tar(entries) {
  return Buffer.concat([...entries.flatMap(([path, payload]) => {
    const body = Buffer.from(payload);
    return [header(path, body.length), body, Buffer.alloc((512 - body.length % 512) % 512)];
  }), Buffer.alloc(1024)]);
}

async function* chunks(bytes, size) {
  for (let offset = 0; offset < bytes.length; offset += size) yield bytes.subarray(offset, offset + size);
}

test("tar parsing tolerates every header split and drains unread or partly read bodies", async () => {
  const bytes = tar([["first", "one"], ["ignored", Buffer.alloc(32769, 7)], ["last", "three"]]);
  for (const size of [1, 511, 512, 513, 65536]) {
    const seen = [];
    await visitTar(chunks(bytes, size), async entry => {
      seen.push(entry.path);
      if (entry.path === "first") assert.equal((await collectBody(entry)).toString(), "one");
      if (entry.path === "ignored") {
        for await (const part of entry.body) { assert.equal(part[0], 7); break; }
      }
      if (entry.path === "last") assert.equal(await hashBody(entry), sha256(Buffer.from("three")));
    });
    assert.deepEqual(seen, ["first", "ignored", "last"]);
  }
});

test("tar validation rejects corrupt checksums, truncation and invalid end markers", async () => {
  const valid = tar([["file", "payload"]]);
  const checksum = Buffer.from(valid); checksum[0] ^= 1;
  const nonzeroTail = Buffer.concat([valid, Buffer.from([1])]);
  const badEnd = Buffer.from(valid); badEnd[badEnd.length - 1] = 1;
  for (const bytes of [checksum, valid.subarray(0, 515), valid.subarray(0, -513), nonzeroTail, badEnd]) {
    await assert.rejects(visitTar(chunks(bytes, 7), () => {}), /checksum|truncated|end marker/);
  }
});

test("gzip reader authenticates compressed bytes and propagates CRC failures", async () => {
  const directory = await mkdtemp(join(tmpdir(), "acyclic-streamed-archive-"));
  try {
    const archive = join(directory, "archive.tgz");
    const compressed = gzipSync(tar([["file", "content"]]));
    await writeFile(archive, compressed);
    assert.equal((await readTarGzip(archive, async entry => {
      assert.equal((await collectBody(entry)).toString(), "content");
    })).digest, sha256(compressed));
    compressed[compressed.length - 8] ^= 1;
    await writeFile(archive, compressed);
    await assert.rejects(readTarGzip(archive, () => {}), /data check|CRC|checksum/i);
    await assert.rejects(readTarGzip(directory, () => {}), /regular file/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("npm qualification accepts large metadata and retains unsafe-member and identity negatives", async () => {
  const directory = await mkdtemp(join(tmpdir(), "acyclic-streamed-npm-"));
  try {
    const archive = join(directory, "package.tgz");
    const name = "@acyclic-labs/test";
    const version = "1.0.0";
    const source = "typescript/packages/test";
    const metadata = { name, version, private: false, license: "Apache-2.0", repository: { type: "git", url: "git+https://github.com/acyclic-labs/sdk.git", directory: source }, description: "x".repeat(1048577) };
    const entries = [["package/package.json", JSON.stringify(metadata)], ["package/README.md", `# ${name}\n${"x".repeat(262145)}`], ["package/CHANGELOG.md", `# ${name} changelog\n## ${version} release\n${"x".repeat(1048577)}`], ["package/dist/index.js", "export {};"], ["package/dist/index.d.ts", "export {};"]];
    const compressed = gzipSync(tar(entries));
    await writeFile(archive, compressed);
    assert.equal(await validateArchive(archive, name, version, source), sha256(compressed));
    await assert.rejects(validateArchive(archive, `${name}-other`, version, source), /manifest does not match/);
    await writeFile(archive, gzipSync(tar([...entries, ["package/../escape", "bad"]])));
    await assert.rejects(validateArchive(archive, name, version, source), /unsafe path/);
    await writeFile(archive, gzipSync(tar([...entries, entries[0]])));
    await assert.rejects(validateArchive(archive, name, version, source), /unsafe path/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("plugin qualification streams large manifests and binds the original archive evidence", async () => {
  const directory = await mkdtemp(join(tmpdir(), "acyclic-streamed-plugin-"));
  try {
    const archive = join(directory, "acyclic-labs-plugin-1.0.0.tgz");
    const receipt = join(directory, "QUALIFICATION.json");
    const program = fileURLToPath(new URL("./plugin-qualification.mjs", import.meta.url));
    const source = "a".repeat(40);
    await writeFile(archive, gzipSync(tar([["package/package.json", JSON.stringify({ name: "@acyclic-labs/plugin", version: "1.0.0", description: "x".repeat(1048577) })]])));
    const create = spawnSync(process.execPath, [program, "create", archive, receipt, source], { encoding: "utf8" });
    assert.equal(create.status, 0, create.stderr);
    const verify = () => spawnSync(process.execPath, [program, "verify", receipt, source, archive, "1.0.0"], { encoding: "utf8" });
    assert.equal(verify().status, 0);
    await writeFile(archive, gzipSync(tar([["package/package.json", JSON.stringify({ name: "@acyclic-labs/plugin", version: "1.0.0" })]])));
    const changed = verify();
    assert.notEqual(changed.status, 0);
    assert.match(changed.stderr, /bytes differ/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});
