/** Ref-only durable interaction values. Rust owns admission and schema validation. */
import type { FileRef } from "./conversation.js";
import type { OperationId, Scope } from "./index.js";
import type { InteractionKind } from "./enums.js";
export type { InteractionKind } from "./enums.js";

declare const interactionBrand: unique symbol;
export type InteractionId = string & { readonly [interactionBrand]: "InteractionId" };

export interface ApprovalBinding {
  readonly operation_id: OperationId;
  readonly action_digest: readonly number[];
}

interface InteractionTicketBase {
  readonly id: InteractionId;
  readonly request: FileRef;
  readonly deadline_unix_ms: bigint | null;
}
export type InteractionTicket = InteractionTicketBase & (
  | Readonly<{ kind: "approval"; approval: ApprovalBinding }>
  | Readonly<{ kind: Exclude<InteractionKind, "approval">; approval: null }>
);

export type InteractionOutcome =
  | Readonly<{ kind: "answered"; answer: FileRef }>
  | Readonly<{ kind: "approved" | "declined" | "cancelled" | "expired" | "denied" }>
  | Readonly<{ kind: "indeterminate"; operation_id: OperationId }>;

export interface InteractionResolution {
  readonly id: InteractionId;
  readonly expected_version: bigint;
  readonly outcome: InteractionOutcome;
  readonly detail?: FileRef;
}

/** The owner retains answer bytes and publishes only their immutable file ref. */
export type InteractionResponse =
  | Readonly<{ type: "question" | "form"; value: unknown }>
  | Readonly<{ type: "choice"; option_ids: readonly string[] }>
  | Readonly<{ type: "approval"; approved: boolean; reason: string | null }>;

export interface ResolutionReceipt {
  readonly id: InteractionId;
  readonly version: bigint;
  readonly operation_id: OperationId;
  readonly conversation_revision: bigint;
  readonly replayed: boolean;
  readonly outcome: InteractionOutcome;
}

/** Presentation and answer authority remain independent replaceable bindings. */
export interface InteractionResolver {
  resolveAnswer(operationId: OperationId, scope: Scope, id: InteractionId, expectedVersion: bigint,
    response: Exclude<InteractionResponse, { type: "approval" }>): Promise<ResolutionReceipt>;
  resolveApproval(operationId: OperationId, scope: Scope, id: InteractionId, expectedVersion: bigint,
    approved: boolean, reason: string | null): Promise<ResolutionReceipt>;
}
