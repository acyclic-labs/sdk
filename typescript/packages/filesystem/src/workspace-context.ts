import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  WorkspaceContextDiscardSchema, WorkspaceContextRootSchema, WorkspaceContextRootsSchema,
  WorkspaceContextSnapshotSchema, WorkspaceContextState,
  type WorkspaceContextRoot as WireRoot,
} from "../generated/proto/filesystem/v2/filesystem_pb.js";
import type { RawWorkspaceContextRegistry } from "./contracts.js";
import type { WorkspaceContext, WorkspaceContextRegistry, WorkspaceContextRoot } from "./compat.js";

function identity(bytes: Uint8Array, name: string): Uint8Array {
  if (bytes.byteLength !== 16) throw new TypeError(`${name} must be a 16-byte identity`);
  return Uint8Array.from(bytes);
}

function encodeRoot(root: WorkspaceContextRoot): WireRoot {
  return create(WorkspaceContextRootSchema, {
    rootId: root.rootId,
    sourcePath: root.sourcePath,
    workspaceId: root.workspaceId,
    workspaceName: root.workspaceName,
    ...(root.parentWorkspaceId === undefined ? {} : { parentWorkspaceId: root.parentWorkspaceId }),
    ...(root.mountPath === undefined ? {} : { mountPath: root.mountPath }),
  });
}

function encodeRoots(roots: readonly WorkspaceContextRoot[]): Uint8Array {
  const ordered = [...roots].sort((left, right) => {
    for (let index = 0; index < Math.min(left.rootId.length, right.rootId.length); index += 1) {
      const delta = left.rootId[index]! - right.rootId[index]!;
      if (delta !== 0) return delta;
    }
    return left.rootId.length - right.rootId.length;
  });
  return toBinary(WorkspaceContextRootsSchema, create(WorkspaceContextRootsSchema, { roots: ordered.map(encodeRoot) }));
}

function decodeRoot(root: WireRoot): WorkspaceContextRoot {
  return {
    rootId: identity(root.rootId, "root identity"),
    sourcePath: root.sourcePath,
    workspaceId: identity(root.workspaceId, "workspace identity"),
    workspaceName: root.workspaceName,
    parentWorkspaceId: root.parentWorkspaceId === undefined ? undefined
      : identity(root.parentWorkspaceId, "parent workspace identity"),
    mountPath: root.mountPath,
  };
}

function decodeContext(bytes: Uint8Array): WorkspaceContext {
  const value = fromBinary(WorkspaceContextSnapshotSchema, bytes);
  const state = (() => {
    switch (value.state) {
      case WorkspaceContextState.ACTIVE: return "active";
      case WorkspaceContextState.FROZEN: return "frozen";
      case WorkspaceContextState.DISCARDED: return "discarded";
      default: throw new TypeError("unknown workspace context state");
    }
  })();
  return {
    version: value.version,
    revision: value.revision,
    contextId: identity(value.contextId, "context identity"),
    parentContextId: value.parentContextId === undefined ? undefined
      : identity(value.parentContextId, "parent context identity"),
    roots: value.roots.map(decodeRoot),
    state,
  };
}

/** The native and WASM bindings expose the same Rust-owned protobuf contract. */
export function adaptWorkspaceContextRegistry(raw: RawWorkspaceContextRegistry): WorkspaceContextRegistry {
  return {
    async registerRoot(contextId, roots) {
      return decodeContext(await raw.registerRoot(contextId, encodeRoots(roots)));
    },
    async registerChild(contextId, parentContextId, roots) {
      return decodeContext(await raw.registerChild(contextId, parentContextId, encodeRoots(roots)));
    },
    async adoptRoot(contextId, root) {
      return decodeContext(await raw.adoptRoot(contextId, toBinary(WorkspaceContextRootSchema, encodeRoot(root))));
    },
    async removeRoot(contextId, rootId) {
      return decodeContext(await raw.removeRoot(contextId, rootId));
    },
    async resolve(contextId) {
      return decodeContext(await raw.resolve(contextId));
    },
    async setActive(contextId, active) {
      return decodeContext(await raw.setActive(contextId, active));
    },
    async setWorkspace(contextId, rootId, workspaceId, workspaceName, parentWorkspaceId) {
      return decodeContext(await raw.setWorkspace(contextId, rootId, workspaceId, workspaceName, parentWorkspaceId));
    },
    async discardSubtree(parentContextId, childContextId, maximum) {
      const value = fromBinary(WorkspaceContextDiscardSchema,
        await raw.discardSubtree(parentContextId, childContextId, maximum));
      return value.contextIds.map(id => identity(id, "discarded context identity"));
    },
  };
}
