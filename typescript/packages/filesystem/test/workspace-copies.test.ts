import { describe, expect, test } from "bun:test";
import type { TransactionRebaseResult, WorkCounters, WorkspaceStat } from "../src/contracts.js";
import {
  copyCheckoutCommit, copyFileExtentPlan, copyLiveMutation, copyLiveTransaction,
  copyRebaseResult, copyTransactionRebase, copyTransactionResult,
  copyWorkspaceDirectoryPage, copyWorkspaceExtentPlan, copyWorkspaceStat,
} from "../src/workspace-copies.js";

describe("workspace binding result copies", () => {
  test("normalizes wire integers and copies stat and directory bytes", () => {
    const fileId = Buffer.alloc(16, 1);
    const name = Buffer.from([97]);
    const stat = copyWorkspaceStat({
      fileId, kind: "regular", linkCount: "2", logicalBytes: "3",
      metadata: {
        posixMode: 0o644, posixUid: 1, posixGid: 2, posixFlags: "4",
        windowsAttributes: undefined, createdNs: "5", modifiedNs: undefined,
        accessedNs: undefined, changedNs: undefined,
        hasNamedAttributes: false, hasAcl: false, hasSecurityDescriptor: false,
      },
    } as unknown as WorkspaceStat);
    const page = copyWorkspaceDirectoryPage({
      hasMore: false,
      entries: [{ name: { encoding: "posix-bytes", bytes: name }, fileId, kind: "regular" }],
    });

    expect(stat.linkCount).toBe(2n);
    expect(stat.metadata.posixFlags).toBe(4n);
    expect(page.entries[0]?.name.bytes).toEqual(new Uint8Array(name));
    stat.fileId[0] = 9;
    page.entries[0]!.fileId[0] = 9;
    page.entries[0]!.name.bytes[0] = 9;
    expect(fileId[0]).toBe(1);
    expect(name[0]).toBe(97);
  });

  test("normalizes extent and transaction integer fields while copying conflict bytes", () => {
    const extent = copyWorkspaceExtentPlan({
      spans: [{ offset: "1", length: "2", sourceEnd: "3", kind: "content" }],
    } as unknown as Parameters<typeof copyWorkspaceExtentPlan>[0]);
    const generationId = Buffer.alloc(16, 1);
    const expected = Buffer.from([2]);
    const rebase = copyTransactionRebase({
      status: "conflicted", generationId, truncated: false,
      conflicts: [{
        region: "file-record", fileId: generationId, directoryId: undefined,
        offset: "4", length: "5", sparseTarget: undefined, name: undefined,
        maximumEntries: undefined, usage: "observation", expected, actual: undefined,
      }],
    } as unknown as TransactionRebaseResult);

    expect(extent.spans[0]?.sourceEnd).toBe(3n);
    expect(rebase.conflicts[0]?.offset).toBe(4n);
    rebase.generationId![0] = 9;
    rebase.conflicts[0]!.fileId![0] = 9;
    rebase.conflicts[0]!.expected![0] = 9;
    expect(generationId[0]).toBe(1);
    expect(expected[0]).toBe(2);
  });

  test("normalizes native and WASM file extents through one result boundary", () => {
    const objectId = Buffer.alloc(33, 3);
    const work = {} as WorkCounters;
    const sparse = copyFileExtentPlan({ kind: "sparse", spans: [{
      kind: "content", offset: "1", length: "2", sourceEnd: "3", objectId, objectOffset: "4",
    }], retainedAllocationBytes: "5" }, work, "WASM");
    if (sparse.kind !== "sparse") throw new Error("expected sparse extent plan");
    expect(sparse.work).toBe(work);
    expect(sparse.spans[0]?.sourceEnd).toBe(3n);
    expect(sparse.retainedAllocationBytes).toBe(5n);
    const span = sparse.spans[0];
    if (span?.kind !== "content") throw new Error("expected content span");
    span.objectId[0] = 9;
    expect(objectId[0]).toBe(3);
    expect(copyFileExtentPlan({ kind: "inline" }, work, "native binding")).toEqual({ kind: "inline", work });
    const wasmSpan = copyFileExtentPlan({ kind: "sparse", spans: [{
      kind: "content", offset: "1", length: "2", sourceEnd: "3",
      objectId: Array.from(objectId), objectOffset: "4",
    }] }, work, "WASM");
    expect(wasmSpan.kind === "sparse" && wasmSpan.spans[0]?.kind === "content"
      ? wasmSpan.spans[0].objectId : undefined).toEqual(new Uint8Array(objectId));
    expect(() => copyFileExtentPlan({ kind: "sparse", spans: [{
      kind: "content", offset: 0n, length: 1n, sourceEnd: 1n,
    }] }, work, "native binding")).toThrow("malformed content extent");
    expect(() => copyFileExtentPlan({ kind: "sparse", spans: [{
      kind: "content", offset: 0n, length: 1n, sourceEnd: 1n,
      objectId: Buffer.alloc(32), objectOffset: 0n,
    }] }, work, "native binding")).toThrow("content object identity must be 33 bytes");
    expect(() => copyFileExtentPlan({ kind: "sparse", spans: [{
      kind: "content", offset: 0n, length: 1n, sourceEnd: 1n,
      objectId: Array(33).fill(256), objectOffset: 0n,
    }] }, work, "WASM")).toThrow("content object identity must be 33 bytes");
    expect(() => copyFileExtentPlan({ kind: "sparse", spans: [{
      kind: "content", offset: 0n, length: 1n, sourceEnd: 1n,
      objectId, objectOffset: null,
    }] } as unknown as Parameters<typeof copyFileExtentPlan>[0], work, "WASM"))
      .toThrow("malformed content extent");
    expect(() => copyFileExtentPlan({ kind: "sparse" }, work, "native binding"))
      .toThrow("malformed file extent plan");
    expect(copyFileExtentPlan({ kind: "sparse", retainedAllocationBytes: "0" }, work, "WASM", true))
      .toEqual({ kind: "sparse", spans: [], retainedAllocationBytes: 0n, work });
    expect(() => copyFileExtentPlan({ kind: "unknown", spans: [] } as unknown as Parameters<typeof copyFileExtentPlan>[0],
      work, "WASM")).toThrow("malformed file extent plan");
    expect(() => copyFileExtentPlan({ kind: "sparse", spans: [{
      kind: "unknown", offset: 0n, length: 1n, sourceEnd: 1n,
    }] } as unknown as Parameters<typeof copyFileExtentPlan>[0], work, "WASM"))
      .toThrow("malformed file extent span");
  });

  test("normalizes checkout commit and live results without retaining binding buffers", () => {
    const generationId = Buffer.alloc(32, 1);
    const fingerprint = Buffer.alloc(32, 2);
    const fileId = Buffer.alloc(16, 3);
    const work = {} as WorkCounters;
    const fields = { status: "committed" as const, generationId, epoch: "4", sequence: "5",
      committedFingerprint: fingerprint };
    const commit = copyCheckoutCommit(fields, work);
    const live = copyLiveMutation({ ...fields, conflictCount: 0, truncated: false }, work);
    const transaction = copyLiveTransaction({ ...fields, conflictCount: 0, truncated: false,
      createdFileIds: [fileId, undefined] }, work);
    const wasmTransaction = copyLiveTransaction({ ...fields,
      generationId: Array.from(generationId), committedFingerprint: Array.from(fingerprint),
      conflictCount: 0, truncated: false, createdFileIds: [Array.from(fileId), null] }, work);
    expect(wasmTransaction.generationId).toEqual(new Uint8Array(generationId));
    expect(wasmTransaction.createdFileIds).toEqual([new Uint8Array(fileId), undefined]);
    expect([commit.epoch, live.sequence, transaction.createdFileIds.length]).toEqual([4n, 5n, 2]);
    for (const result of [commit, live, transaction]) {
      result.generationId![0] = 9;
      result.committedFingerprint![0] = 9;
    }
    transaction.createdFileIds[0]![0] = 9;
    expect([generationId[0], fingerprint[0], fileId[0]]).toEqual([1, 2, 3]);
    expect(() => copyCheckoutCommit({ ...fields, status: "unknown" } as unknown as Parameters<typeof copyCheckoutCommit>[0], work))
      .toThrow("invalid status");
    const invalidLive = { ...fields, status: "unknown", conflictCount: 0, truncated: false } as unknown as Parameters<typeof copyLiveMutation>[0];
    expect(() => copyLiveMutation(invalidLive, work)).toThrow("invalid status");
    expect(() => copyCheckoutCommit({ ...fields, generationId: Buffer.alloc(31) }, work))
      .toThrow("generation identity must be 32 bytes");
    expect(() => copyCheckoutCommit({ ...fields, committedFingerprint: Buffer.alloc(31) }, work))
      .toThrow("committed fingerprint must be 32 bytes");
    expect(() => copyLiveTransaction({ ...fields, conflictCount: 0, truncated: false,
      createdFileIds: [Buffer.alloc(15)] }, work)).toThrow("created file identity must be 16 bytes");
    const ordinary = copyTransactionResult({ createdFileIds: [fileId, undefined] }, work);
    ordinary.createdFileIds[0]![0] = 8;
    expect(fileId[0]).toBe(3);
    expect(() => copyTransactionResult({ createdFileIds: [Buffer.alloc(15)] }, work))
      .toThrow("created file identity must be 16 bytes");
  });

  test("validates and copies checkout rebase identities", () => {
    const generationId = Buffer.alloc(32, 4);
    const work = {} as WorkCounters;
    const rebase = copyRebaseResult({ status: "safe", generationId, conflictCount: 0,
      truncated: false }, work);
    rebase.generationId![0] = 9;
    expect(generationId[0]).toBe(4);
    expect(() => copyRebaseResult({ status: "safe", generationId: Buffer.alloc(31),
      conflictCount: 0, truncated: false }, work)).toThrow("generation identity must be 32 bytes");
    expect(() => copyRebaseResult({ status: "invalid", generationId: undefined,
      conflictCount: 0, truncated: false } as unknown as Parameters<typeof copyRebaseResult>[0], work))
      .toThrow("invalid status");
  });
});
