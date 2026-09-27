import { expect, test } from "bun:test";
import type {
  ConflictKey,
  ConflictValue,
  FilesystemConflict,
  FilesystemConflictKind,
  MergeDriver,
  MergeDriverMode,
  MergeResolution,
} from "../src/compat.js";

const id = new Uint8Array(16);
const generation = new Uint8Array(32);

const conflictKeys = [
  { kind: "file", fileId: id },
  { kind: "binding", directoryId: id, name: new Uint8Array([1]) },
  { kind: "content-range", fileId: id, offset: 2n, length: 3n },
  { kind: "metadata", fileId: id },
  { kind: "hard-links", fileId: id },
  { kind: "directory", fileId: id },
  { kind: "rename", fileId: id, from: "a", to: "b" },
  { kind: "special-payload", fileId: id },
] satisfies readonly ConflictKey[];

const conflictValues = [
  { kind: "absent" },
  { kind: "text", text: "text" },
  { kind: "binary", bytes: new Uint8Array([1]) },
  { kind: "metadata", bytes: new Uint8Array([2]) },
  { kind: "binding", fileId: undefined },
  { kind: "record", generation, fileId: id },
  { kind: "record-reference", generation, fileId: id },
  { kind: "binding-reference", generation, directoryId: id, name: new Uint8Array([3]) },
] satisfies readonly ConflictValue[];

const resolutions = [
  { kind: "select", side: "base" },
  { kind: "select", side: "ours" },
  { kind: "select", side: "theirs" },
  { kind: "text", text: "merged" },
  { kind: "binary", bytes: new Uint8Array([1]) },
  { kind: "metadata", bytes: new Uint8Array([2]) },
  { kind: "binding", fileId: undefined },
  { kind: "unresolved" },
] satisfies readonly MergeResolution[];

const conflict: FilesystemConflict = {
  key: conflictKeys[0],
  path: undefined,
  kind: "record" satisfies FilesystemConflictKind,
  base: conflictValues[0],
  ours: conflictValues[1],
  theirs: conflictValues[2],
};

const driver: MergeDriver = {
  fingerprint: () => new Uint8Array([1]),
  mode: () => "deterministic" satisfies MergeDriverMode,
  resolve: () => ({ kind: "unresolved" }),
};

test("merge-driver contracts expose every Rust-owned variant", () => {
  expect(conflictKeys).toHaveLength(8);
  expect(conflictValues).toHaveLength(8);
  expect(resolutions).toHaveLength(8);
  expect(conflict.kind).toBe("record");
  expect(driver.mode?.()).toBe("deterministic");
  expect(driver.resolve(conflict)).toEqual({ kind: "unresolved" });
});
