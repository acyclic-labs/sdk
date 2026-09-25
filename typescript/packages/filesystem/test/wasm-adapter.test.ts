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
import {
  adaptWasmWorkspaceContextRegistry,
} from "../src/wasm-adapter.js";
import { openBrowserOperationWindowCoordinator } from "../src/browser.js";
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
});
