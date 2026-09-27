import { describe, expect, test } from "bun:test";
import { copyBatchLookupEntries, copyBindingChange, copyDirectoryPage, copyDirectoryRecordPage,
  copyFileRecord, copyGenerationDiff, copyNamedAttributePage, copyNamedAttributeResult, copyStatResult } from "../src/binding-results.js";
import type { WorkCounters } from "../src/contracts.js";

const id = (byte: number) => Buffer.alloc(16, byte);

describe("filesystem binding byte results", () => {
  test("file records share one native and WASM normalization boundary", () => {
    const fileId = id(1);
    const metadataObject = id(2);
    const payloadObject = id(3);
    const inlineBytes = Buffer.from([4]);
    const wire = {
      fileId, fileKind: "regular", linkCount: "5", metadataObject,
      payloadKind: "inline-regular", logicalBytes: "1", payloadObject, inlineBytes,
      deviceMajor: undefined, deviceMinor: undefined,
    };
    const decoded = copyFileRecord(wire);
    const native = copyFileRecord({ ...decoded, linkCount: 5n });

    expect(decoded.linkCount).toBe(5n);
    expect(decoded.logicalBytes).toBe(1n);
    expect(native.linkCount).toBe(5n);
    decoded.fileId[0] = 9;
    decoded.metadataObject[0] = 9;
    decoded.payloadObject![0] = 9;
    decoded.inlineBytes![0] = 9;
    expect([fileId[0], metadataObject[0], payloadObject[0], inlineBytes[0]])
      .toEqual([1, 2, 3, 4]);
  });

  test("batch lookup entries own their file identities", () => {
    const fileId = id(1);
    const [entry] = copyBatchLookupEntries([
      { exists: true, fileId, fileKind: "regular", resolvedComponents: 1 },
    ]);

    expect(Buffer.isBuffer(entry?.fileId)).toBe(false);
    expect(entry?.fileId).toEqual(new Uint8Array(fileId));
    entry!.fileId![0] = 9;
    expect(fileId[0]).toBe(1);
  });

  test("binding changes own top-level and nested snapshot bytes", () => {
    const directoryId = id(1);
    const name = Buffer.from([2]);
    const beforeId = id(3);
    const beforeName = Buffer.from([4]);
    const afterId = id(5);
    const afterName = Buffer.from([6]);
    const result = copyBindingChange({
      directoryId,
      name: { encoding: "posix-bytes", bytes: name },
      before: { fileId: beforeId, name: { encoding: "posix-bytes", bytes: beforeName }, fileKind: "regular" },
      after: { fileId: afterId, name: { encoding: "posix-bytes", bytes: afterName }, fileKind: "regular" },
    });

    expect(Buffer.isBuffer(result.before?.fileId)).toBe(false);
    result.directoryId[0] = 9;
    result.name.bytes[0] = 9;
    result.before!.fileId[0] = 9;
    result.before!.name.bytes[0] = 9;
    result.after!.fileId[0] = 9;
    result.after!.name.bytes[0] = 9;
    expect([directoryId[0], name[0], beforeId[0], beforeName[0], afterId[0], afterName[0]])
      .toEqual([1, 2, 3, 4, 5, 6]);
  });

  test("generation diffs copy native and WASM file changes through one boundary", () => {
    const fileId = id(1);
    const metadataObject = id(2);
    const before = {
      fileId, fileKind: "regular", linkCount: "1", metadataObject,
      payloadKind: "inline-regular", logicalBytes: "1", payloadObject: undefined,
      inlineBytes: Buffer.from([3]), deviceMajor: undefined, deviceMinor: undefined,
    };
    const work = {} as WorkCounters;
    const result = copyGenerationDiff({
      files: [{ fileId, before, after: undefined }], bindings: [], truncated: false,
    }, work);
    expect(result.files[0]?.before?.linkCount).toBe(1n);
    expect(result.work).toBe(work);
    result.files[0]!.fileId[0] = 9;
    result.files[0]!.before!.metadataObject[0] = 9;
    result.files[0]!.before!.inlineBytes![0] = 9;
    expect([fileId[0], metadataObject[0], before.inlineBytes[0]]).toEqual([1, 2, 3]);
  });

  test("checkout pages and attributes own native Buffer bytes", () => {
    const work = {} as WorkCounters;
    const fileId = id(1);
    const name = Buffer.from([2]);
    const metadataBytes = Buffer.from([3]);
    const record = { fileId, fileKind: "regular", linkCount: "1", metadataObject: id(4),
      payloadKind: "inline-regular", logicalBytes: "1", payloadObject: undefined,
      inlineBytes: undefined, deviceMajor: undefined, deviceMinor: undefined };
    const stat = copyStatResult({ exists: true, record, metadataCanonicalBytes: metadataBytes }, work);
    const attributes = copyNamedAttributePage({ entries: [{ attributeClass: "posix-xattr", name }],
      hasMore: false }, work);
    const attribute = copyNamedAttributeResult({ exists: true, bytes: name }, work);
    const directory = copyDirectoryPage({ entries: [{ name, fileId, fileKind: "regular" }],
      hasMore: false }, work);
    const records = copyDirectoryRecordPage({ entries: [{ name, record, metadataCanonicalBytes: metadataBytes }],
      hasMore: false }, work);
    stat.record!.fileId[0] = 9;
    stat.metadataCanonicalBytes![0] = 9;
    attributes.entries[0]!.name[0] = 9;
    attribute.bytes![0] = 9;
    directory.entries[0]!.fileId[0] = 9;
    records.entries[0]!.record.metadataObject[0] = 9;
    expect([fileId[0], name[0], metadataBytes[0], record.metadataObject[0]])
      .toEqual([1, 2, 3, 4]);
  });
});
