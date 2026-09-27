import { describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  WorkspaceContextDiscardSchema, WorkspaceContextRootSchema, WorkspaceContextRootsSchema,
  WorkspaceContextSnapshotSchema, WorkspaceContextState,
  type WorkspaceContextRoot as WireRoot,
} from "../generated/proto/filesystem/v2/filesystem_pb.js";

import type {
  RawWorkspaceContextRegistry,
  WasmRawWorkspaceContextRegistry,
  WorkCounters,
} from "../src/contracts.js";
import * as GeneratedWasm from "../generated/wasm/acyclic_fs_wasm.js";
import {
  adaptWasmWorkspaceContextRegistry,
} from "../src/wasm-adapter.js";
import { openBrowserOperationWindowCoordinator } from "../src/browser.js";
import { DEFAULT_MEMORY_FS_OPTIONS, openMemoryFs } from "../src/memory-node.js";
import { adaptWorkspaceContextRegistry } from "../src/workspace-context.js";
import { copyMergeConflict, decodeMergeConflict, parseJoinResult, parseMergePreparation, parseWorkspaceCommit, parseWorkspaceRebaseResult } from "../src/workspace-results.js";

const identity = (byte: number): number[] => Array.from({ length: 16 }, () => byte);
const id = (byte: number): Uint8Array => Uint8Array.from(identity(byte));
const snapshot = (revision: bigint, options: { contextId?: Uint8Array; parentContextId?: Uint8Array; roots?: WireRoot[] } = {}) =>
  toBinary(WorkspaceContextSnapshotSchema, create(WorkspaceContextSnapshotSchema, {
    version: 1, revision, contextId: options.contextId ?? id(1),
    ...(options.parentContextId === undefined ? {} : { parentContextId: options.parentContextId }),
    roots: options.roots ?? [], state: WorkspaceContextState.ACTIVE,
  }));

describe("WASM adapter canonical boundaries", () => {
  test("preserves u64 context revisions beyond JavaScript's safe integer range", async () => {
    const raw = {
      async resolve() { return snapshot(9007199254740993n); },
    } as unknown as WasmRawWorkspaceContextRegistry;

    const context = await adaptWasmWorkspaceContextRegistry(raw).resolve(Uint8Array.from(identity(1)));
    expect(context.revision).toBe(9007199254740993n);
  });

  test("rejects out-of-range identity bytes instead of wrapping them", async () => {
    const raw = {
      async resolve() { return snapshot(1n, { contextId: id(1).slice(1) }); },
    } as unknown as WasmRawWorkspaceContextRegistry;

    await expect(
      adaptWasmWorkspaceContextRegistry(raw).resolve(Uint8Array.from(identity(1))),
    ).rejects.toThrow("context identity must be a 16-byte identity");
  });

  test("round-trips registry roots and discarded identities through the shared binding contract", async () => {
    let encodedRoots: Uint8Array | undefined;
    const raw = {
      async registerRoot(_contextId: Uint8Array, rootsWire: Uint8Array) {
        encodedRoots = rootsWire;
        return snapshot(2n, { roots: [create(WorkspaceContextRootSchema, {
          rootId: id(2), sourcePath: "/source", workspaceId: id(3), workspaceName: "primary",
        })] });
      },
      async discardSubtree() {
        return toBinary(WorkspaceContextDiscardSchema,
          create(WorkspaceContextDiscardSchema, { contextIds: [id(4)] }));
      },
    } as unknown as WasmRawWorkspaceContextRegistry;
    const registry = adaptWasmWorkspaceContextRegistry(raw);
    const context = await registry.registerRoot(Uint8Array.from(identity(1)), [{
      rootId: Uint8Array.from(identity(2)), sourcePath: "/source",
      workspaceId: Uint8Array.from(identity(3)), workspaceName: "primary",
      parentWorkspaceId: undefined, mountPath: undefined,
    }]);
    expect(fromBinary(WorkspaceContextRootsSchema, encodedRoots!).roots[0]).toMatchObject({
      rootId: id(2), sourcePath: "/source", workspaceId: id(3), workspaceName: "primary",
    });
    expect(context.roots[0]?.workspaceId).toEqual(Uint8Array.from(identity(3)));
    expect(await registry.discardSubtree(Uint8Array.from(identity(1)), Uint8Array.from(identity(4)), 1))
      .toEqual([Uint8Array.from(identity(4))]);
  });

  test("accepts native Buffer identities and forwards optional parent bindings", async () => {
    const child = Buffer.from(identity(1));
    const parent = Buffer.from(identity(2));
    let encodedRoot: Uint8Array | undefined;
    const raw = {
      async registerChild(contextId: Uint8Array, parentContextId: Uint8Array, rootsWire: Uint8Array) {
        expect(contextId).toBe(child);
        expect(parentContextId).toBe(parent);
        encodedRoot = rootsWire;
        return snapshot(3n, { parentContextId: id(2) });
      },
    } as unknown as RawWorkspaceContextRegistry;
    const context = await adaptWorkspaceContextRegistry(raw).registerChild(child, parent, [{
      rootId: Buffer.from(identity(3)), sourcePath: "/native", workspaceId: Buffer.from(identity(4)),
      workspaceName: "child", parentWorkspaceId: Buffer.from(identity(5)), mountPath: "/mount",
    }]);
    const root = fromBinary(WorkspaceContextRootsSchema, encodedRoot!).roots[0];
    expect(root?.parentWorkspaceId).toEqual(id(5));
    expect(root?.mountPath).toBe("/mount");
    expect(context.parentContextId).toEqual(Uint8Array.from(identity(2)));
  });

  test("copies native Buffer generation identities at shared result boundaries", () => {
    const generationId = Buffer.alloc(32, 6);
    const join = parseJoinResult({ status: "applied", generationId, conflicts: [], truncated: false }, value => value);
    const rebase = parseWorkspaceRebaseResult({ status: "rebased", generationId, conflicts: [], truncated: false }, value => value);
    const commit = parseWorkspaceCommit({ status: "committed", generationId });
    for (const result of [join, rebase, commit]) {
      expect(result.generationId).toBeInstanceOf(Uint8Array);
      expect(result.generationId).not.toBeInstanceOf(Buffer);
      result.generationId![0] = 9;
      expect(generationId[0]).toBe(6);
    }
    const malformedGenerationId = id(6);
    expect(() => parseJoinResult(
      { status: "applied", generationId: malformedGenerationId, conflicts: [], truncated: false }, value => value,
    )).toThrow("invalid generation identity");
    expect(() => parseWorkspaceRebaseResult(
      { status: "rebased", generationId: malformedGenerationId, conflicts: [], truncated: false }, value => value,
    )).toThrow("invalid generation identity");
    expect(() => parseWorkspaceCommit({ status: "committed", generationId: malformedGenerationId }))
      .toThrow("invalid generation identity");
  });

  test("copies native Buffer merge conflict identities and names", () => {
    const directoryId = Buffer.from(identity(6));
    const name = Buffer.from([97]);
    const conflict = decodeMergeConflict({ kind: "binding", fileId: undefined, directoryId,
      name: { encoding: "utf8", bytes: name } }, "native merge");
    expect(conflict.kind).toBe("binding");
    if (conflict.kind !== "binding") throw new Error("expected binding conflict");
    conflict.directoryId[0] = 9;
    conflict.name.bytes[0] = 98;
    expect(directoryId[0]).toBe(6);
    expect(name[0]).toBe(97);
  });

  test("uses one merge-preparation status and byte ownership boundary", () => {
    const work = {} as WorkCounters;
    const generationId = Buffer.alloc(32, 7);
    const prepared = parseMergePreparation(
      { status: "prepared", generationId, conflicts: [], truncated: false, work },
      copyMergeConflict,
    );
    expect(prepared.status).toBe("prepared");
    prepared.generationId?.fill(9);
    expect(generationId[0]).toBe(7);

    const directoryId = Buffer.from(identity(8));
    const name = Buffer.from([97]);
    const conflicted = parseMergePreparation(
      { status: "conflicted", generationId: undefined, truncated: true, work,
        conflicts: [{ kind: "binding" as const, directoryId, name: { encoding: "utf8" as const, bytes: name } }] },
      copyMergeConflict,
    );
    expect(conflicted.status).toBe("conflicted");
    const conflict = conflicted.conflicts[0];
    if (conflict?.kind !== "binding") throw new Error("expected binding conflict");
    conflict.directoryId[0] = 9;
    conflict.name.bytes[0] = 9;
    expect(directoryId[0]).toBe(8);
    expect(name[0]).toBe(97);
    expect(() => parseMergePreparation(
      { status: "prepared", generationId, conflicts: [], truncated: true, work }, copyMergeConflict,
    )).toThrow("malformed merge preparation");
    expect(() => parseMergePreparation(
      { status: "prepared", generationId: id(7), conflicts: [], truncated: false, work }, copyMergeConflict,
    )).toThrow("invalid generation identity");
    for (const malformed of [
      { kind: "file", fileId: id(1).slice(1) },
      { kind: "file", fileId: id(1), directoryId: id(2) },
      { kind: "binding", directoryId: id(2).slice(1), name: { encoding: "utf8", bytes: id(3) } },
      { kind: "binding", directoryId: id(2), name: { encoding: "utf8", bytes: "invalid" } },
      { kind: "binding", directoryId: id(2), name: null },
    ]) {
      expect(() => decodeMergeConflict(malformed, "WASM merge")).toThrow("malformed conflict");
    }
  });

  test("copies context identities decoded from native Buffer wire payloads", async () => {
    const wire = Buffer.from(snapshot(7n, { contextId: id(1), parentContextId: id(2) }));
    const raw = { async resolve() { return wire; } } as unknown as RawWorkspaceContextRegistry;
    const context = await adaptWorkspaceContextRegistry(raw).resolve(id(1));
    context.contextId[0] = 9;
    context.parentContextId![0] = 9;
    const again = await adaptWorkspaceContextRegistry(raw).resolve(id(1));
    expect(again.contextId[0]).toBe(1);
    expect(again.parentContextId?.[0]).toBe(2);
  });

  test("rejects unsupported browser operation windows explicitly", async () => {
    await expect(openBrowserOperationWindowCoordinator()).rejects.toThrow(
      "browser operation windows are unsupported by the persistent authority",
    );
  });

  test("preserves typed WASM workspace stat, directory, extent, and join results", async () => {
    const engine = await openMemoryFs();
    try {
      const workspace = await engine.createWorkspace("typed-results");
      const payload = Uint8Array.of(97, 98, 99);
      expect((await workspace.write("/typed", payload)).status).toBe("committed");

      const stat = await workspace.stat("/typed");
      expect(stat.fileId).toBeInstanceOf(Uint8Array);
      expect(stat.linkCount).toBe(1n);
      expect(stat.logicalBytes).toBe(3n);
      expect(stat.metadata.createdNs === undefined || typeof stat.metadata.createdNs === "bigint").toBe(true);
      const fileId = stat.fileId.slice();
      stat.fileId.fill(0);
      expect((await workspace.stat("/typed")).fileId).toEqual(fileId);

      const generation = await workspace.sync();
      const page = await generation.listDirectory("/", undefined, 8);
      const entry = page.entries.find(item => new TextDecoder().decode(item.name.bytes) === "typed");
      expect(entry).toBeDefined();
      expect(entry?.name.bytes).toBeInstanceOf(Uint8Array);
      expect(entry?.fileId).toBeInstanceOf(Uint8Array);
      expect(entry?.kind).toBe("regular");

      const extentPlan = await generation.planExtents("/typed", 0n, 3n, 8);
      expect(extentPlan.spans.length).toBeGreaterThan(0);
      expect(extentPlan.spans[0]?.offset).toBe(0n);
      expect(extentPlan.spans[0]?.length).toBe(3n);
      expect(typeof extentPlan.spans[0]?.sourceEnd).toBe("bigint");

      const target = await workspace.fork("typed-target");
      expect((await workspace.write("/joined", Uint8Array.of(4))).status).toBe("committed");
      const joinPlan = await workspace.joinInto(target, {
        history: "merge", maximumGenerations: 8, maximumChanges: 16, maximumConflicts: 8,
      });
      const joined = await joinPlan.apply(joinPlan.targetHead);
      expect(joined.status).toBe("applied");
      expect(joined.generationId).toBeInstanceOf(Uint8Array);
      expect(await target.read("/joined", 1n)).toEqual(Uint8Array.of(4));
    } finally {
      await engine.close();
    }
  });

  test("keeps large extent coordinates as bigint in the raw WASM ABI", async () => {
    const engine = await openMemoryFs();
    engine.close();
    const rawFs = GeneratedWasm.openMemoryFs(DEFAULT_MEMORY_FS_OPTIONS);
    try {
      const rawWorkspace = await rawFs.createWorkspace("raw-bigint");
      await rawWorkspace.write("/large", Uint8Array.of(1));
      const transaction = await rawWorkspace.beginTransaction();
      await transaction.resize("/large", 2n ** 54n);
      await transaction.commit();
      const generation = await rawWorkspace.sync();
      const plan = await generation.planExtents("/large", (2n ** 54n) - 1n, 1n, 8);
      expect(plan.spans.some(span => typeof span.offset === "bigint" && span.offset > 2n ** 53n)).toBe(true);
    } finally {
      rawFs.free();
    }
  });

  test("preserves conflicting join and rebase IDs and names as byte arrays", async () => {
    const engine = await openMemoryFs();
    try {
      const source = await engine.createWorkspace("conflict-source");
      await source.write("/shared", Uint8Array.of(1));
      const target = await source.fork("conflict-target");
      await source.write("/shared", Uint8Array.of(2));
      await target.write("/shared", Uint8Array.of(3));
      const join = await source.joinInto(target, {
        history: "merge", maximumGenerations: 8, maximumChanges: 16, maximumConflicts: 8,
      });
      const joined = await join.apply(join.targetHead);
      expect(joined.status).toBe("conflicted");
      expect(joined.generationId).toBeUndefined();
      const fileConflict = joined.conflicts.find(conflict => conflict.kind === "file");
      expect(fileConflict).toBeDefined();
      if (fileConflict?.kind === "file") expect(fileConflict.fileId).toBeInstanceOf(Uint8Array);

      const rebaseSource = await engine.createWorkspace("rebase-conflict-source");
      await rebaseSource.write("/shared", Uint8Array.of(1));
      const rebaseTarget = await rebaseSource.fork("rebase-conflict-target");
      await rebaseTarget.write("/shared", Uint8Array.of(3));
      await rebaseSource.write("/shared", Uint8Array.of(2));
      const rebased = await rebaseTarget.liveRebase({
        maximumGenerations: 8, maximumChanges: 16, maximumConflicts: 8,
      });
      expect(rebased.status).toBe("conflicted");
      expect(rebased.generationId).toBeUndefined();
      const rebaseConflict = rebased.conflicts.find(conflict => conflict.kind === "file");
      expect(rebaseConflict).toBeDefined();
      if (rebaseConflict?.kind === "file") expect(rebaseConflict.fileId).toBeInstanceOf(Uint8Array);

      const bindingSource = await engine.createWorkspace("binding-conflict-source");
      const bindingTarget = await bindingSource.fork("binding-conflict-target");
      const sourceTx = await bindingSource.beginTransaction();
      await sourceTx.createDirectory("/same-directory");
      await sourceTx.commit();
      const targetTx = await bindingTarget.beginTransaction();
      await targetTx.createDirectory("/same-directory");
      await targetTx.commit();
      const bindingJoin = await bindingSource.joinInto(bindingTarget, {
        history: "merge", maximumGenerations: 8, maximumChanges: 16, maximumConflicts: 8,
      });
      const bindingResult = await bindingJoin.apply(bindingJoin.targetHead);
      expect(bindingResult.status).toBe("conflicted");
      const bindingConflict = bindingResult.conflicts.find(conflict => conflict.kind === "binding");
      expect(bindingConflict).toBeDefined();
      if (bindingConflict?.kind === "binding") {
        expect(bindingConflict.directoryId).toBeInstanceOf(Uint8Array);
        expect(bindingConflict.name.bytes).toBeInstanceOf(Uint8Array);
      }
    } finally {
      await engine.close();
    }
  });
});
