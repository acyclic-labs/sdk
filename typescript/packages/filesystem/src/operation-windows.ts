import type { FsWorkspace } from "./contracts.js";
import type { WasmRawWorkspace } from "./binding-types.js";
import { parseWorkspaceRebaseResult } from "./workspace-results.js";
import type { OperationWindowCoordinator, OperationWindowLease, OperationWindowPhase, OperationWindowClose } from "./compat.js";
import type { BrowserOperationWindowCoordinator, BrowserOperationWindowLease, BrowserOperationWindowPhase, BrowserOperationWindowClose } from "../generated/wasm/acyclic_fs_wasm.js";
import { copyOptionalBytes, requireIdentity } from "./binding-values.js";

function copyOperationWindowLease(
  lease: BrowserOperationWindowLease,
): OperationWindowLease {
  return {
    workspaceId: new Uint8Array(lease.workspaceId),
    leaseId: new Uint8Array(lease.leaseId),
    pinnedParent: new Uint8Array(lease.pinnedParent),
    expiresAtMillis: lease.expiresAtMillis,
  };
}

function validateOperationWindowLease(
  lease: OperationWindowLease,
): BrowserOperationWindowLease {
  requireIdentity(lease.workspaceId, 16, "workspace identity");
  requireIdentity(lease.leaseId, 16, "lease identity");
  requireIdentity(lease.pinnedParent, 32, "pinned parent");
  return lease;
}

function parseOperationWindowPhase(
  phase: BrowserOperationWindowPhase,
): OperationWindowPhase {
  if (phase.kind === "idle") return { kind: "idle" };
  if (phase.kind === "active" && phase.pinnedParent !== undefined) {
    return {
      kind: "active",
      pinnedParent: new Uint8Array(phase.pinnedParent),
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
      ticket: new Uint8Array(phase.ticket),
      pinnedParent: new Uint8Array(phase.pinnedParent),
      pendingParent: copyOptionalBytes(phase.pendingParent),
    };
  }
  throw new TypeError("operation window returned a malformed phase");
}

function parseOperationWindowClose(
  close: BrowserOperationWindowClose,
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
      ticket: new Uint8Array(close.ticket),
      pinnedParent: new Uint8Array(close.pinnedParent),
      pendingParent: copyOptionalBytes(close.pendingParent),
    };
  }
  throw new TypeError("operation window returned a malformed close result");
}

export function adaptOperationWindowCoordinator(
  raw: BrowserOperationWindowCoordinator,
  rawWorkspace: (workspace: FsWorkspace) => WasmRawWorkspace,
): OperationWindowCoordinator {
  return {
    async begin(workspaceId, parent, owner, nowMillis, expiresAtMillis, leaseId) {
      requireIdentity(workspaceId, 16, "workspace identity");
      requireIdentity(parent, 32, "parent");
      if (leaseId !== undefined) requireIdentity(leaseId, 16, "lease identity");
      if (owner.length === 0) throw new RangeError("operation owner must be non-empty");
      return copyOperationWindowLease(
        await raw.begin(workspaceId, parent, owner, nowMillis, expiresAtMillis, leaseId),
      );
    },
    async renew(lease, nowMillis, expiresAtMillis) {
      return copyOperationWindowLease(await raw.renew(validateOperationWindowLease(lease), nowMillis, expiresAtMillis));
    },
    async observeParent(workspaceId, parent) {
      requireIdentity(workspaceId, 16, "workspace identity");
      requireIdentity(parent, 32, "parent");
      return raw.observeParent(workspaceId, parent);
    },
    async finish(lease, nowMillis) {
      return parseOperationWindowClose(
        await raw.finish(validateOperationWindowLease(lease), nowMillis),
      );
    },
    async inspect(workspaceId) {
      requireIdentity(workspaceId, 16, "workspace identity");
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
        return { kind: "reconciled", rebase: parseWorkspaceRebaseResult(result.rebase) };
      }
      throw new TypeError("operation window returned a malformed workspace close result");
    },
    async recoverWorkspace(workspace, nowMillis, options) {
      const result = await raw.recoverWorkspace(rawWorkspace(workspace), nowMillis, options);
      return result == null ? undefined : parseWorkspaceRebaseResult(result);
    },
  };
}
