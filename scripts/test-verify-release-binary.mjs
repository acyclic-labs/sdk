#!/usr/bin/env node

import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const directory = mkdtempSync(join(tmpdir(), "acyclic-binary-contract-"));
const verifier = fileURLToPath(new URL("./verify-release-binary.mjs", import.meta.url));

function elf(machine, interpreter) {
  const bytes = Buffer.alloc(256);
  Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1]).copy(bytes);
  bytes.writeUInt16LE(machine, 18);
  bytes.writeBigUInt64LE(64n, 32);
  bytes.writeUInt16LE(56, 54);
  bytes.writeUInt16LE(interpreter ? 1 : 0, 56);
  if (interpreter) {
    const value = Buffer.from(`${interpreter}\0`);
    bytes.writeUInt32LE(3, 64);
    bytes.writeBigUInt64LE(128n, 72);
    bytes.writeBigUInt64LE(BigInt(value.length), 96);
    value.copy(bytes, 128);
  }
  return bytes;
}

function mach(cpu) {
  const bytes = Buffer.alloc(32);
  bytes.writeUInt32LE(0xfeedfacf, 0);
  bytes.writeUInt32LE(cpu, 4);
  return bytes;
}

function pe(machine) {
  const bytes = Buffer.alloc(128);
  bytes.write("MZ", 0, "ascii");
  bytes.writeUInt32LE(64, 0x3c);
  bytes.writeUInt32LE(0x00004550, 64);
  bytes.writeUInt16LE(machine, 68);
  return bytes;
}

const cases = [
  ["linux-x64-gnu", elf(0x3e, "/lib64/ld-linux-x86-64.so.2")],
  ["linux-x64-musl", elf(0x3e, null)],
  ["linux-arm64-gnu", elf(0xb7, "/lib/ld-linux-aarch64.so.1")],
  ["linux-arm64-musl", elf(0xb7, "/lib/ld-musl-aarch64.so.1")],
  ["darwin-x64", mach(0x01000007)],
  ["darwin-arm64", mach(0x0100000c)],
  ["win32-x64", pe(0x8664)],
  ["win32-arm64", pe(0xaa64)],
];

for (const [target, bytes] of cases) {
  const path = join(directory, target);
  writeFileSync(path, bytes);
  const result = spawnSync(process.execPath, [verifier, target, path], { stdio: "inherit" });
  if (result.status !== 0) throw new Error(`valid ${target} fixture was rejected`);
}

const mismatch = spawnSync(process.execPath, [verifier, "win32-arm64", join(directory, "win32-x64")]);
if (mismatch.status === 0) throw new Error("architecture mismatch was accepted");
