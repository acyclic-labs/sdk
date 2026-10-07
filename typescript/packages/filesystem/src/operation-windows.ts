import type { FsWorkspace, WorkspaceRebaseOptions, WorkspaceRebaseResult, NativeRawOperationWindowCoordinator, NativeRawOperationWindowLease, NativeRawOperationWindowPhase, NativeRawOperationWindowClose } from "./contracts.js";
import type { OperationWindowCoordinator, OperationWindowLease, OperationWindowPhase, OperationWindowClose } from "./compat.js";
import type { BrowserOperationWindowPhase, BrowserOperationWindowClose } from "../generated/wasm/acyclic_fs_wasm.js";
import { copyBytes, copyOptionalBytes, requireIdentity, requireGenerationIdentity } from "./binding-values.js";

type RawOperationWindows<Workspace, Rebase> =
  Pick<NativeRawOperationWindowCoordinator, "begin" | "renew" | "observeParent"> & {
    finish(lease: OperationWindowLease, nowMillis: bigint): Promise<NativeRawOperationWindowClose | BrowserOperationWindowClose>;
    inspect(workspaceId: Uint8Array): Promise<NativeRawOperationWindowPhase | BrowserOperationWindowPhase>;
    finishWorkspace(workspace: Workspace, lease: OperationWindowLease, nowMillis: bigint, options: WorkspaceRebaseOptions): Promise<{ kind: string; remaining?: number | undefined; rebase?: Rebase | undefined }>;
    recoverWorkspace(workspace: Workspace, nowMillis: bigint, options: WorkspaceRebaseOptions): Promise<Rebase | undefined | null>;
  };

function copyOperationWindowLease(
  lease: NativeRawOperationWindowLease,
): OperationWindowLease {
  return {
    workspaceId: copyBytes(lease.workspaceId),
    leaseId: copyBytes(lease.leaseId),
    pinnedParent: copyBytes(lease.pinnedParent),
    expiresAtMillis: lease.expiresAtMillis,
  };
}

function validateOperationWindowLease(
  lease: OperationWindowLease,
): NativeRawOperationWindowLease {
  requireIdentity(lease.workspaceId, "workspace identity");
  requireIdentity(lease.leaseId, "lease identity");
  requireGenerationIdentity(lease.pinnedParent, "pinned parent");
  return lease;
}

function parseOperationWindowPhase(
  phase: NativeRawOperationWindowPhase | BrowserOperationWindowPhase,
): OperationWindowPhase {
  if (phase.kind === "idle") return { kind: "idle" };
  if (phase.kind === "active" && phase.pinnedParent !== undefined) {
    return {
      kind: "active",
      pinnedParent: copyBytes(phase.pinnedParent),
      pendingParent: copyOptionalBytes(phase.pendingParent),
      activeLeaseCount: phase.activeLeaseCount ?? 0,
    };
  }
  if (
    phase.kind === "reconciling" && phase.ticket !== undefined &&
    phase.pinnedParent !== undefined
  ) {
    return {
      kind: "reconciling",
      ticket: copyBytes(phase.ticket),
      pinnedParent: copyBytes(phase.pinnedParent),
      pendingParent: copyOptionalBytes(phase.pendingParent),
    };
  }
  throw new TypeError("operation window returned a malformed phase");
}

function parseOperationWindowClose(
  close: NativeRawOperationWindowClose | BrowserOperationWindowClose,
): OperationWindowClose {
  if (close.kind === "still-active" && close.remaining !== undefined) {
    return { kind: "still-active", remaining: close.remaining };
  }
  if (close.kind === "already-closed") return { kind: "already-closed" };
  if (
    close.kind === "reconcile" && close.ticket !== undefined &&
    close.pinnedParent !== undefined
  ) {
    return {
      kind: "reconcile",
      ticket: copyBytes(close.ticket),
      pinnedParent: copyBytes(close.pinnedParent),
      pendingParent: copyOptionalBytes(close.pendingParent),
    };
  }
  throw new TypeError("operation window returned a malformed close result");
}

export function adaptOperationWindowCoordinator<Workspace, Rebase>(
  raw: RawOperationWindows<Workspace, Rebase>,
  rawWorkspace: (workspace: FsWorkspace) => Workspace,
  parseRebase: (value: Rebase) => WorkspaceRebaseResult,
): OperationWindowCoordinator {
  return {
    async begin(workspaceId, parent, owner, nowMillis, expiresAtMillis, leaseId) {
      requireIdentity(workspaceId, "workspace identity");
      requireGenerationIdentity(parent, "parent");
      if (leaseId !== undefined) requireIdentity(leaseId, "lease identity");
      if (owner.length === 0) throw new RangeError("operation owner must be non-empty");
      return copyOperationWindowLease(
        await raw.begin(workspaceId, parent, owner, nowMillis, expiresAtMillis, leaseId),
      );
    },
    async renew(lease, nowMillis, expiresAtMillis) {
      return copyOperationWindowLease(await raw.renew(validateOperationWindowLease(lease), nowMillis, expiresAtMillis));
    },
    async observeParent(workspaceId, parent) {
      requireIdentity(workspaceId, "workspace identity");
      requireGenerationIdentity(parent, "parent");
      return raw.observeParent(workspaceId, parent);
    },
    async finish(lease, nowMillis) {
      return parseOperationWindowClose(
        await raw.finish(validateOperationWindowLease(lease), nowMillis),
      );
    },
    async inspect(workspaceId) {
      requireIdentity(workspaceId, "workspace identity");
      return parseOperationWindowPhase(await raw.inspect(workspaceId));
    },
    async finishWorkspace(workspace, lease, nowMillis, options) {
      const result = await raw.finishWorkspace(
        rawWorkspace(workspace),
        validateOperationWindowLease(lease),
        nowMillis,
        options,
      );
      if (result.kind === "still-active" && result.remaining !== undefined) {
        return { kind: "still-active", remaining: result.remaining };
      }
      if (result.kind === "already-closed") return { kind: "already-closed" };
      if (result.kind === "reconciled" && result.rebase !== undefined) {
        return { kind: "reconciled", rebase: parseRebase(result.rebase) };
      }
      throw new TypeError("operation window returned a malformed workspace close result");
    },
    async recoverWorkspace(workspace, nowMillis, options) {
      const result = await raw.recoverWorkspace(rawWorkspace(workspace), nowMillis, options);
      return result == null ? undefined : parseRebase(result);
    },
  };
}
