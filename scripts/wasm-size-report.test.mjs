import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { reportWasm } from "./wasm-size-report.mjs";

const header = [0, 97, 115, 109, 1, 0, 0, 0];
const leb = value => {
  const bytes = [];
  do {
    const rest = Math.floor(value / 128);
    bytes.push(value % 128 + (rest ? 128 : 0));
    value = rest;
  } while (value);
  return bytes;
};
const section = (id, bytes) => [id, ...leb(bytes.length), ...bytes];
const string = value => [...leb(Buffer.byteLength(value)), ...Buffer.from(value)];
const moduleBytes = (...sections) => Uint8Array.from([...header, ...sections.flat()]);
const type = section(1, [1, 0x60, 0, 0]);
const declarations = section(3, [1, 0]);
const code = section(10, [1, 2, 0, 0x0b]);
const nameSection = entries => section(0, [...string("name"), ...section(1,
  [entries.length, ...entries.flatMap(([index, name]) => [...leb(index), ...string(name)])])]);

test("empty module conserves bytes and has no code", () => {
  const report = reportWasm(moduleBytes());
  assert.equal(report.moduleBytes, 8);
  assert.equal(report.code.sectionBytes, 0);
  assert.equal(report.nameCoverage, "complete");
  assert.deepEqual(report.functions, []);
});

test("imported function indices, aliases, names and overlapping matches are exact", () => {
  const imports = section(2, [1, ...string("env"), ...string("f"), 0, 0]);
  const exports = section(7, [2, ...string("a"), 0, 1, ...string("b"), 0, 1]);
  const report = reportWasm(moduleBytes(type, imports, declarations, exports, code,
    nameSection([[0, "imported"], [1, "future::poll"]])), ["future", "poll", "imported"]);
  assert.equal(report.importedFunctions, 1);
  assert.equal(report.nameCoverage, "complete");
  assert.equal(report.functions[0].index, 1);
  assert.deepEqual(report.functions[0].exports, ["a", "b"]);
  assert.equal(report.functions[0].bodyBytes, 2);
  assert.equal(report.functions[0].encodedBytes, 3);
  assert.deepEqual(report.matches.map(match => match.encodedBytes), [3, 3, 0]);
  assert.equal(report.code.payloadBytes, 4);
  assert.equal(report.code.sectionBytes, 6);
});

test("leading BOMs remain part of function, export and custom section names", () => {
  const name = "\uFEFFfuture::poll";
  const exports = section(7, [1, ...string(name), 0, 0]);
  const report = reportWasm(moduleBytes(type, declarations, exports, code,
    nameSection([[0, name]])), ["^future", "^\uFEFFfuture"]);
  assert.equal(report.functions[0].name, name);
  assert.deepEqual(report.functions[0].exports, [name]);
  assert.deepEqual(report.matches.map(match => match.functions), [0, 1]);
  // An unknown custom section can contain arbitrary bytes, even invalid name metadata.
  const unknown = reportWasm(moduleBytes(type, declarations, code,
    section(0, [...string("\uFEFFname"), 1, 0xff])));
  assert.equal(unknown.sections.at(-1).name, "\uFEFFname");
  assert.equal(unknown.nameCoverage, "unavailable");
});

test("multi-byte size prefixes and padded valid LEB encodings conserve actual bytes", () => {
  // 130 byte body: empty locals, 128 nops and end; padded function count.
  const body = [0, ...Array(128).fill(1), 0x0b];
  const report = reportWasm(moduleBytes(type, declarations,
    section(10, [0x81, 0, ...leb(body.length), ...body])));
  assert.equal(report.code.vectorBytes, 2);
  assert.equal(report.code.bodyBytes, 130);
  assert.equal(report.code.sizePrefixBytes, 2);
  assert.equal(report.code.payloadBytes, 134);
});

test("function vectors cross the one-byte count boundary without index drift", () => {
  const count = 128;
  const report = reportWasm(moduleBytes(type,
    section(3, [...leb(count), ...Array(count).fill(0)]),
    section(10, [...leb(count), ...Array.from({ length: count }, () => [2, 0, 0x0b]).flat()]),
    nameSection([[127, "last"]])), ["last"]);
  assert.equal(report.definedFunctions, count);
  assert.equal(report.code.vectorBytes, 2);
  assert.equal(report.code.bodyBytes, 256);
  assert.equal(report.code.functionEncodedBytes, 384);
  assert.equal(report.functions[127].index, 127);
  assert.equal(report.nameCoverage, "partial");
  assert.equal(report.matches[0].encodedBytes, 3);
});

test("every single-byte core mutation agrees with the independent engine validator", () => {
  const original = moduleBytes(type, declarations, code);
  let rejected = 0;
  let accepted = 0;
  for (let offset = 0; offset < original.length; offset++) {
    for (const value of [0, 1, 0x7f, 0x80, 0xff]) {
      const bytes = original.slice();
      bytes[offset] = value;
      if (WebAssembly.validate(bytes)) {
        const report = reportWasm(bytes);
        assert.equal(report.moduleBytes, bytes.length);
        accepted++;
      } else {
        assert.throws(() => reportWasm(bytes));
        rejected++;
      }
    }
  }
  assert.ok(accepted > 0 && rejected > 100);
});

test("malformed core modules fail closed", () => {
  const valid = moduleBytes(type, declarations, code);
  for (let length = 0; length < valid.length; length++) {
    const truncated = valid.subarray(0, length);
    // A section boundary can itself form a valid minimal module.
    if (!WebAssembly.validate(truncated)) assert.throws(() => reportWasm(truncated));
  }
  for (const broken of [
    moduleBytes([1, 0x80]),
    moduleBytes([1, 0xff, 0xff, 0xff, 0xff, 0x10]),
    moduleBytes(type, declarations),
    moduleBytes(type, declarations, section(10, [0])),
    moduleBytes(type, declarations, section(10, [1, 2, 0, 0xff])),
    moduleBytes(type, type),
  ]) assert.throws(() => reportWasm(broken));
});

test("ignored custom-name corruption cannot silently misattribute bytes", () => {
  for (const names of [
    nameSection([[1, "invalid-index"]]),
    nameSection([[0, "a"], [0, "b"]]),
    section(0, [...string("name"), ...section(1, [1, 0, 2, 0x61])]),
    section(0, [...string("name"), ...section(1, [1, 0, 1, 0xff])]),
    section(0, [...string("name"), ...section(1, [0, 0])]),
  ]) {
    const bytes = moduleBytes(type, declarations, code, names);
    assert.equal(WebAssembly.validate(bytes), true);
    assert.throws(() => reportWasm(bytes));
  }
  assert.throws(() => reportWasm(moduleBytes(type, declarations, code,
    nameSection([[0, "a"]]), nameSection([[0, "b"]]))));
});

test("CLI emits parseable reports and fails without partial stdout", () => {
  const temporary = mkdtempSync(join(tmpdir(), "wasm-size-report-"));
  try {
    const valid = join(temporary, "minimal.wasm");
    const invalid = join(temporary, "broken.wasm");
    writeFileSync(valid, moduleBytes());
    writeFileSync(invalid, Uint8Array.of(0));
    const run = args => spawnSync(process.execPath,
      [fileURLToPath(new URL("./wasm-size-report.mjs", import.meta.url)), ...args], { encoding: "utf8" });
    const success = run(["--match", "future", valid]);
    assert.equal(success.status, 0, success.stderr);
    const output = JSON.parse(success.stdout);
    assert.equal(output.runtime.version, process.versions.bun ?? process.version);
    assert.equal(output.runtime.name, process.versions.bun ? "bun" : process.release.name);
    assert.equal(output.reports[0].moduleBytes, 8);
    assert.equal(output.reports[0].matches[0].encodedBytes, 0);
    for (const args of [[], ["--match"], ["--unknown", valid], ["--match", "[", valid],
      [valid, invalid], [join(temporary, "missing.wasm")]]) {
      const failure = run(args);
      assert.equal(failure.status, 1, failure.stderr);
      assert.equal(failure.stdout, "");
      assert.ok(failure.stderr.length > 0);
    }
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
});
