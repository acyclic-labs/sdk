import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Usage: node scripts/wasm-size-report.mjs [--match REGEX] FILE.wasm ...
// Redirect stdout to retain a deterministic report bound to each input SHA-256.
// Run against symbol-preserving bindgen output for names; shipped output is
// stripped. Never transfer function indices between raw and bindgen modules.
// Byte attribution, not an instruction decoder or source-level cost model.
// Engine validation rejects malformed core modules; unsupported proposals fail
// closed. Function-name subsections are checked separately because engines ignore
// them; other name metadata and unknown extensions are skipped without validation.
class Reader {
  constructor(bytes, start = 0, end = bytes.length) {
    this.bytes = bytes;
    this.position = start;
    this.end = end;
  }
  byte() {
    if (this.position >= this.end) throw new Error("truncated WASM field");
    return this.bytes[this.position++];
  }
  u32() {
    let value = 0;
    for (let i = 0; i < 5; i++) {
      const byte = this.byte();
      if (i === 4 && (byte & 0xf0)) throw new Error("u32 LEB overflow");
      value += (byte & 0x7f) * 2 ** (7 * i);
      if (!(byte & 0x80)) return value;
    }
    throw new Error("unterminated u32 LEB");
  }
  take(length) {
    if (length > this.end - this.position) throw new Error("truncated WASM payload");
    const result = new Reader(this.bytes, this.position, this.position + length);
    this.position += length;
    return result;
  }
  string() {
    const part = this.take(this.u32());
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
      this.bytes.subarray(part.position, part.end),
    );
  }
  done() {
    if (this.position !== this.end) throw new Error("unconsumed WASM field bytes");
  }
}

export function reportWasm(bytes, patterns = []) {
  const module = new WebAssembly.Module(bytes);
  const importedFunctions = WebAssembly.Module.imports(module).filter(x => x.kind === "function").length;
  const reader = new Reader(bytes, 8);
  const sections = [];
  const functions = [];
  const names = new Map();
  const exports = new Map();
  let declaredFunctions = 0;
  let codeVectorBytes = 0;
  let hasFunctionNames = false;
  while (reader.position < reader.end) {
    const offset = reader.position;
    const id = reader.byte();
    const payloadBytes = reader.u32();
    const payloadOffset = reader.position;
    const part = reader.take(payloadBytes);
    let name;
    if (id === 0) {
      name = part.string();
      if (name === "name") {
        while (part.position < part.end) {
          const subsection = part.byte();
          const contents = part.take(part.u32());
          if (subsection === 1) {
            if (hasFunctionNames) throw new Error("duplicate function name subsection");
            hasFunctionNames = true;
            const count = contents.u32();
            for (let i = 0; i < count; i++) {
              const index = contents.u32();
              if (names.has(index)) throw new Error("duplicate function name index");
              names.set(index, contents.string());
            }
            contents.done();
          }
        }
      }
    } else if (id === 3) {
      declaredFunctions = part.u32();
      for (let i = 0; i < declaredFunctions; i++) part.u32();
      part.done();
    } else if (id === 7) {
      const count = part.u32();
      for (let i = 0; i < count; i++) {
        const exportName = part.string();
        const kind = part.byte();
        const index = part.u32();
        if (kind === 0) exports.set(index, [...(exports.get(index) ?? []), exportName]);
      }
      part.done();
    } else if (id === 10) {
      const count = part.u32();
      codeVectorBytes = part.position - payloadOffset;
      for (let i = 0; i < count; i++) {
        const entryOffset = part.position;
        const bodyBytes = part.u32();
        const bodyOffset = part.position;
        part.take(bodyBytes);
        functions.push({ index: importedFunctions + i, entryOffset, bodyOffset, bodyBytes,
          sizePrefixBytes: bodyOffset - entryOffset, encodedBytes: part.position - entryOffset });
      }
      part.done();
    }
    sections.push({ id, ...(name === undefined ? {} : { name }), offset, payloadOffset,
      headerBytes: payloadOffset - offset, payloadBytes, encodedBytes: reader.position - offset });
  }
  if (functions.length !== declaredFunctions) throw new Error("function/code count mismatch");
  for (const index of names.keys()) {
    if (index >= importedFunctions + declaredFunctions) throw new Error("function name index out of range");
  }
  const matchers = patterns.map(pattern => new RegExp(pattern));
  const rows = functions.map(fn => ({ ...fn, name: names.get(fn.index) ?? null,
    exports: exports.get(fn.index) ?? [],
    matches: patterns.filter((_, i) => names.has(fn.index) && matchers[i].test(names.get(fn.index))) }));
  const sum = (items, key) => items.reduce((total, item) => total + item[key], 0);
  const codeSection = sections.find(section => section.id === 10);
  const functionEncodedBytes = sum(rows, "encodedBytes");
  const namedFunctions = rows.filter(row => row.name !== null).length;
  const nameCoverage = namedFunctions === rows.length ? "complete" : namedFunctions === 0 ? "unavailable" : "partial";
  if (8 + sum(sections, "encodedBytes") !== bytes.length ||
      (codeSection?.payloadBytes ?? 0) !== codeVectorBytes + functionEncodedBytes) {
    throw new Error("byte conservation failed");
  }
  return { schemaVersion: 1, sha256: createHash("sha256").update(bytes).digest("hex"),
    moduleBytes: bytes.length, headerBytes: 8, importedFunctions, definedFunctions: rows.length,
    namedFunctions, nameCoverage,
    code: { sectionBytes: codeSection?.encodedBytes ?? 0, payloadBytes: codeSection?.payloadBytes ?? 0,
      vectorBytes: codeVectorBytes, functionEncodedBytes, bodyBytes: sum(rows, "bodyBytes"),
      sizePrefixBytes: sum(rows, "sizePrefixBytes") },
    matches: patterns.map(pattern => {
      const selected = rows.filter(row => row.matches.includes(pattern));
      return { pattern, functions: nameCoverage === "unavailable" ? null : selected.length,
        encodedBytes: nameCoverage === "unavailable" ? null : sum(selected, "encodedBytes"),
        bodyBytes: nameCoverage === "unavailable" ? null : sum(selected, "bodyBytes") };
    }),
    sections, functions: rows,
    scope: "Exact physical bytes. Bodies include locals and instructions. Name matches can overlap; unnamed/inlined/shared code is unattributed. Matches are not removable bytes or total async state-machine cost.",
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const files = [];
    const patterns = [];
    const args = process.argv.slice(2);
    for (let i = 0; i < args.length; i++) {
      if (args[i] === "--match") {
        if (!args[i + 1]) throw new Error("--match requires a regular expression");
        patterns.push(args[++i]);
      } else if (args[i].startsWith("--")) throw new Error(`unknown option: ${args[i]}`);
      else files.push(args[i]);
    }
    if (!files.length) throw new Error("usage: node scripts/wasm-size-report.mjs [--match REGEX] FILE.wasm ...");
    const runtime = process.versions.bun
      ? { name: "bun", version: process.versions.bun, nodeCompatibilityVersion: process.version }
      : { name: process.release.name, version: process.version, v8: process.versions.v8 };
    console.log(JSON.stringify({ runtime, reports: files.map(file => ({
      file, ...reportWasm(readFileSync(file), patterns),
    })) }, null, 2));
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
