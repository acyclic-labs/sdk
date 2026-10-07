import { describe, expect, test } from "bun:test";
import type { FsGeneration, MergeConflictSelection, WorkspaceRebaseResult } from "../src/contracts.js";
import { adaptJoinPlanBase, adaptResolvableJoinPlan, workspaceOperations } from "../src/workspace-operations.js";
import { openMemoryFs } from "../src/memory-node.js";

describe("shared workspace operations", () => {
  test("retains Rust binding admission for names, identities, and bounds", async () => {
    const engine = await openMemoryFs();
    try {
      await expect(engine.createWorkspace("")).rejects.toThrow("workspace name is empty");
      const workspace = await engine.createWorkspace("rust-admission");
      await expect(workspace.delete(new Uint8Array(15))).rejects.toThrow(
        "identity must be exactly 16 bytes",
      );
      await expect(workspace.liveRebase({
        maximumGenerations: 0, maximumChanges: 1, maximumConflicts: 1,
      }, new Uint8Array(16))).rejects.toThrow("workspace join exceeds its configured bound");
      await expect(workspace.liveRebase({
        maximumGenerations: 1.5, maximumChanges: 1, maximumConflicts: 1,
      }, new Uint8Array(16))).rejects.toThrow();
    } finally {
      await engine.close();
    }
  });

  test("join plan copies binding heads and preserves raw validation errors", async () => {
    const targetHead = Buffer.alloc(32, 1);
    const commonAncestor = Buffer.alloc(32, 2);
    let applies = 0;
    let parses = 0;
    const raw = {
      targetHead, commonAncestor,
      async apply(ifTarget: Uint8Array, idempotencyKey?: Uint8Array) {
        applies += 1;
        if (ifTarget.byteLength !== 32) throw new Error("target generation identity must be exactly 32 bytes");
        if (idempotencyKey !== undefined && idempotencyKey.byteLength !== 16) {
          throw new Error("idempotency key must be exactly 16 bytes");
        }
        return { status: "applied", generationId: Buffer.alloc(32, 3), conflicts: [], truncated: false };
      },
    };
    const plan = adaptJoinPlanBase(raw, (value) => {
      parses += 1;
      return { status: "applied", generationId: value.generationId, conflicts: [], truncated: false };
    });

    plan.targetHead[0] = 9;
    plan.commonAncestor[0] = 9;
    expect(targetHead[0]).toBe(1);
    expect(commonAncestor[0]).toBe(2);
    await expect(plan.apply(new Uint8Array(31))).rejects.toThrow("generation identity");
    await expect(plan.apply(new Uint8Array(32), new Uint8Array(15))).rejects.toThrow("idempotency key");
    expect(applies).toBe(2);
    const applied = await plan.apply(new Uint8Array(32), new Uint8Array(16));
    expect(applied.status).toBe("applied");
    expect(applied.generationId?.[0]).toBe(3);
    expect([applies, parses]).toEqual([3, 1]);
  });

  test("native conflict-side selections are copied before forwarding", async () => {
    const fileId = Buffer.alloc(16, 1);
    const directoryId = Buffer.alloc(16, 2);
    const name = Buffer.from([3]);
    let forwarded: readonly MergeConflictSelection[] = [];
    const raw = {
      targetHead: Buffer.alloc(32), commonAncestor: Buffer.alloc(32),
      async apply() { throw new Error("not used"); },
      async applySides(_target: Uint8Array, _key: Uint8Array | undefined, selections: readonly MergeConflictSelection[]) {
        forwarded = selections;
        return { status: "applied", generationId: Buffer.alloc(32, 4), conflicts: [], truncated: false };
      },
    };
    const plan = adaptResolvableJoinPlan(raw, (value) => ({
      status: "applied", generationId: value.generationId, conflicts: [], truncated: false,
    }));
    const result = await plan.applySides(new Uint8Array(32), [
      { kind: "file", fileId, side: "ours" },
      { kind: "binding", directoryId, name: { encoding: "posix-bytes", bytes: name }, side: "theirs" },
    ]);

    expect(result.generationId?.[0]).toBe(4);
    expect(forwarded).toHaveLength(2);
    const forwardedFile = forwarded[0];
    const forwardedBinding = forwarded[1];
    if (forwardedFile?.kind !== "file" || forwardedBinding?.kind !== "binding") throw new Error("unexpected selection kind");
    forwardedFile.fileId[0] = 9;
    forwardedBinding.directoryId[0] = 9;
    forwardedBinding.name.bytes[0] = 9;
    expect([fileId[0], directoryId[0], name[0]]).toEqual([1, 2, 3]);
  });

  test("copies raw bytes and forwards raw request errors", async () => {
    const head = Buffer.alloc(32, 1);
    const committed = Buffer.alloc(32, 2);
    let reads = 0;
    let deletes = 0;
    const raw = {
      async head() { return head; },
      async read() { reads += 1; return Buffer.from([3]); },
      async write() { return { status: "committed", generationId: committed }; },
      async delete(idempotencyKey?: Uint8Array) {
        deletes += 1;
        if (idempotencyKey !== undefined && idempotencyKey.byteLength !== 16) {
          throw new Error("idempotency key must be exactly 16 bytes");
        }
        return "deleted";
      },
    } as unknown as Parameters<typeof workspaceOperations>[0];
    const operations = workspaceOperations(
      raw,
      (generation) => generation as unknown as FsGeneration,
      (value) => value as unknown as WorkspaceRebaseResult,
    );

    await expect(operations.read("/item", 0n)).rejects.toThrow("maximum read bytes");
    expect(reads).toBe(0);
    const result = await operations.write("/item", new Uint8Array([1]));
    expect(result.status).toBe("committed");
    result.generationId![0] = 9;
    expect(committed[0]).toBe(2);
    const observed = await operations.head();
    observed[0] = 9;
    expect(head[0]).toBe(1);
    await expect(operations.delete(new Uint8Array(15))).rejects.toThrow("idempotency key");
    expect(deletes).toBe(1);
  });
});
