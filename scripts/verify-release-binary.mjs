#!/usr/bin/env node

import { readFileSync } from "node:fs";

function fail(message) {
  throw new Error(message);
}

function expect(condition, message) {
  if (!condition) fail(message);
}

function verifyElf(bytes, target) {
  expect(bytes.length >= 64 && bytes.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46])), "binary is not ELF");
  expect(bytes[4] === 2 && bytes[5] === 1, "release ELF must be 64-bit little-endian");
  const expectedMachine = target.includes("arm64") ? 0xb7 : 0x3e;
  expect(bytes.readUInt16LE(18) === expectedMachine, `ELF machine does not match ${target}`);
  const programOffset = Number(bytes.readBigUInt64LE(32));
  const entrySize = bytes.readUInt16LE(54);
  const entryCount = bytes.readUInt16LE(56);
  expect(entrySize >= 56 && programOffset + entrySize * entryCount <= bytes.length, "ELF program headers are invalid");
  let interpreter = null;
  for (let index = 0; index < entryCount; index += 1) {
    const offset = programOffset + index * entrySize;
    if (bytes.readUInt32LE(offset) !== 3) continue;
    const valueOffset = Number(bytes.readBigUInt64LE(offset + 8));
    const valueSize = Number(bytes.readBigUInt64LE(offset + 32));
    expect(valueOffset + valueSize <= bytes.length, "ELF interpreter is out of bounds");
    interpreter = bytes.subarray(valueOffset, valueOffset + valueSize).toString("utf8").replace(/\0.*$/s, "");
  }
  if (target.endsWith("-gnu")) expect(interpreter?.includes("ld-linux"), "GNU release does not use the glibc loader");
  if (target.endsWith("-musl")) expect(interpreter === null || interpreter.includes("ld-musl"), "musl release uses a non-musl loader");
}

function verifyMachO(bytes, target) {
  expect(bytes.length >= 32 && bytes.readUInt32LE(0) === 0xfeedfacf, "binary is not 64-bit little-endian Mach-O");
  const expectedCpu = target === "darwin-arm64" ? 0x0100000c : 0x01000007;
  expect(bytes.readUInt32LE(4) === expectedCpu, `Mach-O CPU does not match ${target}`);
}

function verifyPe(bytes, target) {
  expect(bytes.length >= 64 && bytes[0] === 0x4d && bytes[1] === 0x5a, "binary is not PE/COFF");
  const header = bytes.readUInt32LE(0x3c);
  expect(header + 6 <= bytes.length && bytes.readUInt32LE(header) === 0x00004550, "PE header is invalid");
  const expectedMachine = target === "win32-arm64" ? 0xaa64 : 0x8664;
  expect(bytes.readUInt16LE(header + 4) === expectedMachine, `PE machine does not match ${target}`);
}

const [target, path] = process.argv.slice(2);
if (!target || !path) fail("usage: verify-release-binary.mjs TARGET BINARY");
const bytes = readFileSync(path);
if (/^linux-(?:x64|arm64)-(?:gnu|musl)$/.test(target)) verifyElf(bytes, target);
else if (/^darwin-(?:x64|arm64)$/.test(target)) verifyMachO(bytes, target);
else if (/^win32-(?:x64|arm64)$/.test(target)) verifyPe(bytes, target);
else fail(`unsupported release target: ${target}`);
