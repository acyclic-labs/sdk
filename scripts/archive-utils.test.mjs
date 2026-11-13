import assert from "node:assert/strict";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { validateArchive } from "./validate-npm-package.mjs";
import { createHash } from "node:crypto";
import { gzipSync, gunzipSync } from "node:zlib";
import { fuzz, fuzzGzip, fuzzDeclared, fuzzPayload, fuzzArtifactTree } from "./archive-utils.fuzz.mjs";
import { collectBody, hashBody, isCanonicalRelativePath, readPayloadEntries, readTarGzip, sha256, visitTar, copyWasmArtifacts, preserveWasmArtifacts, wasmArtifactFiles, wasmArtifactInventory } from "./archive-utils.mjs";

const seed = readFileSync(new URL("./test-support/archive-corpus/native-ustar.tar", import.meta.url));

test("artifact inventory matches real file writes and refuses aliases before replacing output", async () => {
  const temporary = mkdtempSync(join(tmpdir(), "wasm-artifact-tree-"));
  const source = join(temporary, "source"), output = join(temporary, "output");
  const expected = new Map();
  const bodies = (await entries(seed)).filter(entry => entry.type === "0").map(entry => entry.body);
  const write = (path, bytes) => {
    const parts = path.split("/");
    mkdirSync(join(source, ...parts.slice(0, -1)), { recursive: true });
    writeFileSync(join(source, path), bytes);
    expected.set(`generated/wasm/${path}`, bytes);
  };
  try {
    mkdirSync(source);
    assert.deepEqual(copyWasmArtifacts(source, output), fuzzArtifactTree(source, expected));
    // The bytes are native TAR payloads, not compiler or receipt fixtures:
    // tree admission treats them as opaque files and never decodes them.
    for (const [index, path] of ["module.js", "module_bg.wasm", "module.d.ts",
      "snippets/inline.js", "snippets/nested/é.js"].entries()) {
      write(path, bodies[index % bodies.length]);
      const receipt = fuzzArtifactTree(source, expected);
      mkdirSync(join(output, "snippets", "stale"), { recursive: true });
      const stale = join(output, "snippets", "stale", "old.js");
      writeFileSync(stale, bodies[0]);
      assert.deepEqual(copyWasmArtifacts(source, output), receipt);
      assert.deepEqual(fuzzArtifactTree(output, expected), receipt);
      assert.throws(() => readFileSync(stale), { code: "ENOENT" });
    }
    for (const present of [true, false]) {
      const selected = present ? source : join(temporary, "absent");
      const original = present ? wasmArtifactInventory(wasmArtifactFiles(selected)) : [];
      for (const outcome of ["built", "refused", "no-output"]) {
        const custody = preserveWasmArtifacts(selected, "node scripts/build-wasm.mjs harness");
        let candidate;
        try {
          if (outcome === "no-output") throw new Error("qualification refused");
          if (present) {
            mkdirSync(selected, { recursive: true });
            cpSync(custody.source(join(selected, "module.js")), join(selected, "module.js"));
            assert.deepEqual(readFileSync(join(selected, "module.js")), expected.get("generated/wasm/module.js"));
          }
          mkdirSync(join(selected, "snippets", "candidate"), { recursive: true });
          // Derived source bytes exercise real directory custody, not a compiler fixture.
          writeFileSync(join(selected, "module.js"), readFileSync(new URL("./build-wasm.mjs", import.meta.url)));
          writeFileSync(join(selected, "snippets", "candidate", "inline.js"), readFileSync(import.meta.filename));
          candidate = wasmArtifactInventory(wasmArtifactFiles(selected));
          if (outcome === "refused") throw new Error("qualification refused");
        } catch (error) {
          if (outcome === "built" || error.message !== "qualification refused") throw error;
        } finally { custody.restore(); }
        if (candidate) assert.deepEqual(wasmArtifactInventory(wasmArtifactFiles(custody.archive)), candidate,
          "temporary build was not retained separately");
        else assert.equal(existsSync(custody.archive), false, "absent temporary build was fabricated");
        if (candidate) assert.deepEqual(readFileSync(custody.retained(join(selected, "module.js"))),
          readFileSync(new URL("./build-wasm.mjs", import.meta.url)), "receipt names replaced output bytes");
        const outside = join(temporary, "outside", "module.js");
        assert.equal(custody.source(outside), outside);
        assert.equal(custody.retained(outside), outside);
        assert.equal(existsSync(selected), present, "temporary build replaced original output presence");
        if (present) assert.deepEqual(wasmArtifactInventory(wasmArtifactFiles(selected)), original,
          "temporary build replaced original output bytes");
      }
    }
    write("producer-receipt.json", bodies[0]);
    fuzzArtifactTree(source, expected, { receipt: true });
    const withoutReceipt = new Map(expected);
    withoutReceipt.delete("generated/wasm/producer-receipt.json");
    fuzzArtifactTree(source, withoutReceipt);
    const receiptLeak = await planted('if (receipt || path !== "producer-receipt.json") yield path;', "yield path;");
    assert.throws(() => receiptLeak.fuzzArtifactTree(source, withoutReceipt), assert.AssertionError);
    const changedDigest = await planted('bytes: bytes.length, sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}`',
      'bytes: bytes.length, sha256: "sha256:" + "0".repeat(64)');
    assert.throws(() => changedDigest.fuzzArtifactTree(source, withoutReceipt), assert.AssertionError);
    const unsorted = await planted(
      ")).sort((a, b) => a.path < b.path ? -1 : a.path > b.path ? 1 : 0)", "))");
    assert.throws(() => unsorted.fuzzArtifactTree(source, withoutReceipt), assert.AssertionError);
    renameSync(join(source, "snippets"), join(source, "other"));
    fuzzArtifactTree(source, null);
    renameSync(join(source, "other"), join(source, "snippets"));

    const alias = join(temporary, "alias");
    symlinkSync(source, alias, process.platform === "win32" ? "junction" : "dir");
    fuzzArtifactTree(alias, null);
    const rootAlias = await planted("const root = lstatSync(directory);", "const root = statSync(directory);");
    assert.throws(() => rootAlias.fuzzArtifactTree(alias, null), assert.AssertionError);
    fuzzArtifactTree(join(source, "module.js"), null);

    const linked = join(source, "snippets", "linked");
    symlinkSync(output, linked, process.platform === "win32" ? "junction" : "dir");
    fuzzArtifactTree(source, null);
    const followed = await planted("entry.isDirectory() && !entry.isSymbolicLink()",
      "(entry.isDirectory() || entry.isSymbolicLink())");
    assert.throws(() => followed.fuzzArtifactTree(source, null), assert.AssertionError);
    const original = bodies[2];
    writeFileSync(join(output, "module.js"), original);
    assert.throws(() => fuzzArtifactTree(output, withoutReceipt), assert.AssertionError);
    const protectedFile = join(output, "previous.js");
    writeFileSync(protectedFile, original);
    assert.throws(() => copyWasmArtifacts(source, output), /non-regular artifact; recovery:/);
    assert.deepEqual(readFileSync(join(output, "module.js")), original);
    assert.deepEqual(readFileSync(protectedFile), original);
    rmSync(linked, { recursive: true });
    symlinkSync(source, join(output, "snippets", "linked"), process.platform === "win32" ? "junction" : "dir");
    assert.throws(() => copyWasmArtifacts(source, output), /non-regular artifact; recovery:/);
    assert.deepEqual(readFileSync(join(output, "module.js")), original);
    assert.deepEqual(readFileSync(protectedFile), original);
  } finally { rmSync(temporary, { recursive: true, force: true }); }
});

test("a build never removes or rewrites source-owned package metadata beside its artifacts", () => {
  const temporary = mkdtempSync(join(tmpdir(), "wasm-artifact-metadata-"));
  const source = join(temporary, "source"), output = join(temporary, "output");
  const tracked = { "package.json": readFileSync(new URL("../package.json", import.meta.url)),
    ".npmignore": readFileSync(new URL("./build-wasm.mjs", import.meta.url)) };
  try {
    mkdirSync(join(source, "snippets"), { recursive: true });
    mkdirSync(output);
    for (const [name, bytes] of Object.entries(tracked)) writeFileSync(join(output, name), bytes);
    writeFileSync(join(output, "stale_bg.wasm"), seed);
    // A producer that also emits package metadata (wasm-pack style) must not reach the output.
    writeFileSync(join(source, "module.js"), seed.subarray(0, 512));
    writeFileSync(join(source, "module_bg.wasm"), seed.subarray(512, 1024));
    writeFileSync(join(source, "snippets", "inline.js"), seed.subarray(1024, 1536));
    writeFileSync(join(source, "package.json"), "{}");
    writeFileSync(join(source, ".gitignore"), "*");
    const inventory = copyWasmArtifacts(source, output);
    assert.deepEqual(inventory.map(item => item.path),
      ["generated/wasm/module.js", "generated/wasm/module_bg.wasm", "generated/wasm/snippets/inline.js"]);
    for (const [name, bytes] of Object.entries(tracked)) assert.deepEqual(readFileSync(join(output, name)), bytes, name);
    assert.equal(existsSync(join(output, ".gitignore")), false);
    assert.equal(existsSync(join(output, "stale_bg.wasm")), false);
    assert.deepEqual(wasmArtifactInventory(wasmArtifactFiles(output)), inventory);
  } finally { rmSync(temporary, { recursive: true, force: true }); }
});

test("native payload framing preserves caller path and case policies without copying members", async () => {
  const flat = gunzipSync(readFileSync(new URL("./test-support/archive-corpus/native-flat.tar.gz", import.meta.url)));
  // The third member in the retained native capture is a directory header.
  const withDirectory = Buffer.concat([flat.subarray(0, flat.length - 1024), seed.subarray(1536, 2048), flat.subarray(flat.length - 1024)]);
  for (const insensitive of [false, true]) {
    assert.equal((await fuzzPayload(flat, insensitive)).size, 2);
    assert.deepEqual([...(await fuzzPayload(withDirectory, insensitive))], [...(await fuzzPayload(flat, insensitive))]);
    assert.notEqual(await fuzzPayload(seed, insensitive), false);
    for (let offset = 0; offset < flat.length; offset += 512) await fuzzPayload(flat.subarray(0, offset), insensitive);
  }
  const unsafe = Buffer.from(flat);
  unsafe.fill(0, 0, 100);
  unsafe.write("../body.txt");
  seal(unsafe);
  const link = Buffer.from(flat);
  link[156] = "2".charCodeAt(0);
  seal(link);
  const first = Buffer.from(flat.subarray(0, 1024));
  first.write("BODY.txt");
  seal(first);
  const folded = Buffer.concat([first, flat]);
  assert.equal((await fuzzPayload(folded, false)).size, 3);
  const memberCheck = 'if (!canonical(name) || seen.has(alias) || (entry.type !== "0" && (declared || regularOnly || entry.type !== "5"))) throw new Error("unsafe or duplicate archive member");';
  for (const [removed, replacement, bytes, insensitive] of [
    [memberCheck, "", unsafe, false],
    [memberCheck, "", link, false],
    [memberCheck, "", Buffer.concat([flat.subarray(0, 1024), flat]), false],
    ["name.toLowerCase()", "name", folded, true],
    ['if (!files || (declared && files !== Object.keys(declared).length)) throw new Error("archive inventory differs");', "", flat.subarray(flat.length - 1024), false],
  ]) {
    assert.equal(await fuzzPayload(bytes, insensitive), false);
    const mutant = await planted(removed, replacement);
    await assert.rejects(mutant.fuzzPayload(bytes, insensitive), assert.AssertionError);
  }
  const copying = await planted("payload.set(name, body)", "payload.set(name, Buffer.from(body))");
  await assert.rejects(copying.fuzzPayload(flat, false), assert.AssertionError);
});

test("native package inventory survives byte mutations and rejects removed admission checks", async () => {
  const directory = mkdtempSync(join(tmpdir(), "archive-inventory-"));
  const path = join(directory, "input.tar.gz");
  const compressed = readFileSync(new URL("./test-support/archive-corpus/native-flat.tar.gz", import.meta.url));
  const files = JSON.parse(readFileSync(new URL("./test-support/archive-corpus/native-flat.files.json", import.meta.url), "utf8"));
  const hash = bytes => createHash("sha256").update(bytes).digest("hex");
  const digest = hash(compressed);
  const wrong = "0".repeat(64);
  try {
    const accepted = await fuzzDeclared(compressed, path, files, digest);
    const listed = readFileSync(new URL("./test-support/archive-corpus/native-flat.list", import.meta.url), "utf8");
    assert.deepEqual([...accepted.payload.keys()], listed.trimEnd().split(/\r?\n/));
    for (let offset = 0; offset < compressed.length; offset++) {
      const bytes = Buffer.from(compressed);
      bytes[offset] ^= 0x80;
      await fuzzDeclared(bytes, path, files, hash(bytes));
    }
    const mutations = [
      ['if (expected !== undefined && evidence.digest !== expected) throw new Error("archive digest differs");', compressed, files, wrong],
      ['if (declared && digest !== declared[name]) throw new Error("archive payload digest differs");', compressed, { ...files, "body.txt": wrong }, digest],
      ['if (!files || (declared && files !== Object.keys(declared).length)) throw new Error("archive inventory differs");', compressed, { ...files, "absent.txt": wrong }, digest],
    ];
    const memberCheck = 'if (!canonical(name) || seen.has(alias) || (entry.type !== "0" && (declared || regularOnly || entry.type !== "5"))) throw new Error("unsafe or duplicate archive member");';
    const directoryBytes = gzipSync(seed);
    const directoryFiles = Object.fromEntries((await entries(seed)).filter(entry => entry.type === "0").map(entry => [entry.path, hash(entry.body)]));
    mutations.push([memberCheck, directoryBytes, directoryFiles, hash(directoryBytes)]);
    const flat = gunzipSync(compressed);
    // Duplicate the original first member without changing its authenticated
    // header or body; only the negative input differs from the native capture.
    const duplicate = gzipSync(Buffer.concat([flat.subarray(0, 1024), flat]));
    mutations.push([[memberCheck, 'if (!files || (declared && files !== Object.keys(declared).length)) throw new Error("archive inventory differs");'], duplicate, files, hash(duplicate)]);
    for (const [removed, bytes, inventory, expected] of mutations) {
      assert.equal(await fuzzDeclared(bytes, path, inventory, expected), false);
      const mutant = await planted(removed);
      await assert.rejects(mutant.fuzzDeclared(bytes, path, inventory, expected), assert.AssertionError);
    }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("native gzip corpus authenticates actual file admission and CRC failures", async () => {
  const directory = mkdtempSync(join(tmpdir(), "archive-admission-"));
  const path = join(directory, "input.tar.gz");
  try {
    const compressed = readFileSync(new URL("./test-support/archive-corpus/native-ustar.tar.gz", import.meta.url));
    assert.deepEqual((await fuzzGzip(compressed, path)).expanded, seed);
    for (let offset = 0; offset < compressed.length; offset++) {
      await fuzzGzip(compressed.subarray(0, offset), path);
      const bytes = Buffer.from(compressed);
      bytes[offset] ^= 0x80;
      await fuzzGzip(bytes, path);
    }
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("native TAR corpus and byte mutations exercise the production decoder", async () => {
  assert.equal(await fuzz(seed), true);
  const listed = readFileSync(new URL("./test-support/archive-corpus/native-ustar.list", import.meta.url), "utf8");
  assert.deepEqual((await entries(seed)).map(entry => entry.path), listed.trimEnd().split(/\r?\n/));
  // Every header byte and body/trailer boundary is reachable from the real
  // producer output, including the final member and a one-block end marker.
  for (let offset = 0; offset < seed.length; offset += 512) {
    assert.equal(await fuzz(seed.subarray(0, offset)), false);
    for (let field = 0; field < 512; field++) {
      const bytes = Buffer.from(seed);
      bytes[offset + field] ^= 0x80;
      await fuzz(bytes);
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
    await fuzz(bytes);
  }
});

function seal(bytes) {
  bytes.fill(32, 148, 156);
  const sum = bytes.subarray(0, 512).reduce((total, byte) => total + byte, 0);
  bytes.write(`${sum.toString(8).padStart(6, "0")}\0 `, 148, 8, "ascii");
}

test("the fuzz oracle rejects removed checksum and end-marker checks", async () => {
  const mutations = [
    [
      'if (checksum !== Number.parseInt(rawChecksum, 8)) throw new Error("archive member checksum mismatch");',
      bytes => { bytes[0] ^= 1; return bytes; },
    ],
    [
      'if (!(await reader.header()).every(byte => byte === 0)) throw new Error("archive has an invalid end marker");',
      bytes => bytes.subarray(0, bytes.length - 512),
    ],
  ];
  for (const [removed, corrupt] of mutations) {
    const mutant = await planted(removed);
    await assert.rejects(mutant.fuzz(corrupt(Buffer.from(seed))), assert.AssertionError);
  }
});

async function planted(removed, replacement = "") {
  let source = readFileSync(new URL("./archive-utils.mjs", import.meta.url), "utf8").replaceAll("\r\n", "\n");
  const target = readFileSync(new URL("./archive-utils.fuzz.mjs", import.meta.url), "utf8");
  for (const defect of Array.isArray(removed) ? removed : [removed]) {
    assert.ok(source.includes(defect), "planted defect no longer matches production");
    source = source.replace(defect, replacement);
  }
  const production = `data:text/javascript;base64,${Buffer.from(source).toString("base64")}`;
  return import(`data:text/javascript;base64,${Buffer.from(target.replace('"./archive-utils.mjs"', JSON.stringify(production))).toString("base64")}`);
}

async function entries(bytes) {
  const result = [];
  await visitTar([bytes], async entry => result.push({ ...entry, body: await collectBody(entry) }));
  return result;
}

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
  }, { canonical: name => name === "large", retain: () => false, collectDigests: true });
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
    const observed = [];
    const evidence = await validateArchive(archive, name, version, source, async entry => {
      observed.push([entry.path, sha256(await collectBody(entry))]);
    });
    assert.deepEqual(evidence, { digest: sha256(compressed), size: compressed.length });
    assert.deepEqual(observed, entries.map(([path, body]) => [path, sha256(Buffer.from(body))]));
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
