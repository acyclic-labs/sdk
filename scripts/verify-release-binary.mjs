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
  /** @type {string | null} */
  let interpreter = null;
  for (let index = 0; index < entryCount; index += 1) {
    const offset = programOffset + index * entrySize;
    const segmentOffset = Number(bytes.readBigUInt64LE(offset + 8));
    const segmentSize = Number(bytes.readBigUInt64LE(offset + 32));
    expect(segmentOffset + segmentSize <= bytes.length, "ELF segment is out of bounds");
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
  const count = bytes.readUInt32LE(16), commandsSize = bytes.readUInt32LE(20);
  expect(32 + commandsSize <= bytes.length, "Mach-O load commands are out of bounds");
  let offset = 32;
  for (let i = 0; i < count; i += 1) {
    expect(offset + 8 <= 32 + commandsSize, "Mach-O load command is truncated");
    const command = bytes.readUInt32LE(offset), size = bytes.readUInt32LE(offset + 4);
    expect(size >= 8 && offset + size <= 32 + commandsSize, "Mach-O load command size is invalid");
    if (command === 0x19) {
      expect(size >= 72, "Mach-O segment is truncated");
      expect(Number(bytes.readBigUInt64LE(offset + 40)) + Number(bytes.readBigUInt64LE(offset + 48)) <= bytes.length,
        "Mach-O segment is out of bounds");
    }
    offset += size;
  }
  expect(offset === 32 + commandsSize, "Mach-O load command inventory is incomplete");
}

function verifyPe(bytes, target) {
  expect(bytes.length >= 64 && bytes[0] === 0x4d && bytes[1] === 0x5a, "binary is not PE/COFF");
  const header = bytes.readUInt32LE(0x3c);
  expect(header + 6 <= bytes.length && bytes.readUInt32LE(header) === 0x00004550, "PE header is invalid");
  const expectedMachine = target === "win32-arm64" ? 0xaa64 : 0x8664;
  expect(bytes.readUInt16LE(header + 4) === expectedMachine, `PE machine does not match ${target}`);
  expect(header + 24 <= bytes.length, "PE COFF header is truncated");
  const count = bytes.readUInt16LE(header + 6), optionalSize = bytes.readUInt16LE(header + 20);
  const sections = header + 24 + optionalSize;
  expect(sections + count * 40 <= bytes.length, "PE section table is out of bounds");
  for (let i = 0; i < count; i += 1) {
    const offset = sections + i * 40;
    expect(bytes.readUInt32LE(offset + 20) + bytes.readUInt32LE(offset + 16) <= bytes.length,
      "PE section is out of bounds");
  }
}

// Release qualification and job-local artifact admission use this same parser.
// With no target, infer the actual supported native format/architecture; release
// callers still impose their exact OS/architecture/loader contract.
export function verifyNativeImage(bytes, target) {
  if (bytes.length >= 8 && [0xcafebabe, 0xcafebabf, 0xbebafeca, 0xbfbafeca].includes(bytes.readUInt32BE(0))) {
    const magic = bytes.readUInt32BE(0), little = [0xbebafeca, 0xbfbafeca].includes(magic);
    const wide = [0xcafebabf, 0xbfbafeca].includes(magic);
    const u32 = offset => little ? bytes.readUInt32LE(offset) : bytes.readUInt32BE(offset);
    const u64 = offset => Number(little ? bytes.readBigUInt64LE(offset) : bytes.readBigUInt64BE(offset));
    const count = u32(4), size = wide ? 32 : 20;
    expect(count > 0 && 8 + count * size <= bytes.length, "Mach-O universal table is invalid");
    const targets = [];
    for (let i = 0; i < count; i += 1) {
      const entry = 8 + i * size, offset = wide ? u64(entry + 8) : u32(entry + 8);
      const length = wide ? u64(entry + 16) : u32(entry + 12);
      expect(offset >= 8 + count * size && length > 0 && offset + length <= bytes.length,
        "Mach-O universal image is out of bounds");
      const image = bytes.subarray(offset, offset + length);
      expect(image.length >= 32 && image.readUInt32LE(0) === 0xfeedfacf, "Mach-O universal member is not supported native Mach-O");
      expect(image.readUInt32LE(4) === u32(entry), "Mach-O universal CPU disagrees with member");
      targets.push(verifyNativeImage(image));
    }
    if (target) expect(targets.includes(target), `Mach-O universal image does not contain ${target}`);
    return target ?? (new Set(targets).size === 1 ? targets[0] : "darwin-universal");
  }
  if (!target) {
    if (bytes.length >= 64 && bytes.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) {
      const machine = bytes.readUInt16LE(18);
      expect([0x3e, 0xb7].includes(machine), "unsupported ELF machine");
      target = machine === 0xb7 ? "linux-arm64" : "linux-x64";
    } else if (bytes.length >= 32 && bytes.readUInt32LE(0) === 0xfeedfacf) {
      const cpu = bytes.readUInt32LE(4);
      expect([0x01000007, 0x0100000c].includes(cpu), "unsupported Mach-O CPU");
      target = cpu === 0x0100000c ? "darwin-arm64" : "darwin-x64";
    } else if (bytes.length >= 64 && bytes[0] === 0x4d && bytes[1] === 0x5a) {
      const header = bytes.readUInt32LE(0x3c);
      expect(header + 6 <= bytes.length, "PE header is invalid");
      const machine = bytes.readUInt16LE(header + 4);
      expect([0x8664, 0xaa64].includes(machine), "unsupported PE machine");
      target = machine === 0xaa64 ? "win32-arm64" : "win32-x64";
    } else fail("artifact is not a supported native ELF, Mach-O or PE image");
  }
  if (/^linux-(?:x64|arm64)(?:-(?:gnu|musl))?$/.test(target)) verifyElf(bytes, target);
  else if (/^darwin-(?:x64|arm64)$/.test(target)) verifyMachO(bytes, target);
  else if (/^win32-(?:x64|arm64)$/.test(target)) verifyPe(bytes, target);
  else fail(`unsupported release target: ${target}`);
  return target;
}

if (import.meta.main) {
  const [target, path, ...extra] = process.argv.slice(2);
  if (!target || !path || extra.length) fail("usage: verify-release-binary.mjs TARGET|--native BINARY");
  verifyNativeImage(readFileSync(path), target === "--native" ? undefined : target);
}
