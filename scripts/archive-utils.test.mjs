import assert from "node:assert/strict";
import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { createHash } from "node:crypto";
import { gzipSync, gunzipSync } from "node:zlib";
import { fuzz, fuzzGzip, fuzzDeclared, fuzzPayload, fuzzArtifactTree } from "./archive-utils.fuzz.mjs";
import { copyWasmArtifacts, preserveWasmArtifacts, tarEntries, wasmArtifactFiles, wasmArtifactInventory } from "./archive-utils.mjs";

const seed = readFileSync(new URL("./test-support/archive-corpus/native-ustar.tar", import.meta.url));

test("artifact inventory matches real file writes and refuses aliases before replacing output", async () => {
  const temporary = mkdtempSync(join(tmpdir(), "wasm-artifact-tree-"));
  const source = join(temporary, "source"), output = join(temporary, "output");
  const expected = new Map();
  const bodies = [...tarEntries(seed)].filter(entry => entry.type === "0").map(entry => entry.body);
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

test("native payload framing preserves caller path and case policies without copying members", async () => {
  const flat = gunzipSync(readFileSync(new URL("./test-support/archive-corpus/native-flat.tar.gz", import.meta.url)));
  // The third member in the retained native capture is a directory header.
  const withDirectory = Buffer.concat([flat.subarray(0, flat.length - 1024), seed.subarray(1536, 2048), flat.subarray(flat.length - 1024)]);
  const snippets = Buffer.from(seed);
  let headerOffset = 0;
  for (const entry of tarEntries(seed)) {
    const header = snippets.subarray(headerOffset, headerOffset + 512);
    if (entry.type === "5" || entry.path.includes("/")) {
      const start = entry.type === "5" ? 0 : 345;
      header.fill(0, start, start + (entry.type === "5" ? 100 : 155));
      header.write(entry.type === "5" ? "snippets/" : "snippets", start);
      seal(header);
    }
    headerOffset += 512 + Math.ceil(entry.size / 512) * 512;
  }
  for (const insensitive of [false, true]) {
    assert.equal(fuzzPayload(flat, insensitive).size, 2);
    assert.deepEqual([...fuzzPayload(withDirectory, insensitive)], [...fuzzPayload(flat, insensitive)]);
    assert.notEqual(fuzzPayload(snippets, insensitive), false);
    for (let offset = 0; offset < flat.length; offset += 512) fuzzPayload(flat.subarray(0, offset), insensitive);
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
  assert.equal(fuzzPayload(folded, false).size, 3);
  const memberCheck = 'if (!isCanonical(name) || seen.has(key) || !["0", "5"].includes(entry.type)) throw new Error("unsafe or duplicate archive member");';
  for (const [removed, replacement, bytes, insensitive] of [
    [memberCheck, "", unsafe, false],
    [memberCheck, "", link, false],
    [memberCheck, "", Buffer.concat([flat.subarray(0, 1024), flat]), false],
    ["name.toLowerCase()", "name", folded, true],
    ['if (!payload.size) throw new Error("empty archive payload");', "", flat.subarray(flat.length - 1024), false],
  ]) {
    assert.equal(fuzzPayload(bytes, insensitive), false);
    const mutant = await planted(removed, replacement);
    assert.throws(() => mutant.fuzzPayload(bytes, insensitive), assert.AssertionError);
  }
  const copying = await planted("payload.set(name, entry.body)", "payload.set(name, Buffer.from(entry.body))");
  assert.throws(() => copying.fuzzPayload(flat, false), assert.AssertionError);
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
    const directoryFiles = Object.fromEntries(Array.from(tarEntries(seed), entry => [entry.path, hash(entry.body)]));
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
  assert.deepEqual(Array.from(tarEntries(seed), entry => entry.path), listed.trimEnd().split(/\r?\n/));
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

async function planted(removed, replacement = "") {
  const source = readFileSync(new URL("./archive-utils.mjs", import.meta.url), "utf8").replaceAll("\r\n", "\n");
  const target = readFileSync(new URL("./archive-utils.fuzz.mjs", import.meta.url), "utf8");
  assert.ok(source.includes(removed), "planted defect no longer matches production");
  const production = `data:text/javascript;base64,${Buffer.from(source.replace(removed, replacement)).toString("base64")}`;
  return import(`data:text/javascript;base64,${Buffer.from(target.replace('"./archive-utils.mjs"', JSON.stringify(production))).toString("base64")}`);
}
