/** Ref-only durable interaction values. Rust owns admission and schema validation. */
import type { FileRef } from "./conversation.js";
import { NativeContracts } from "./native-contracts.js";
import type { OperationId } from "./index.js";

export type InteractionKind = "question" | "choice" | "form" | "approval";
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

/** Rust-canonical non-nil UUID for an addressable interaction. */
export async function interactionId(value: string): Promise<InteractionId> {
  return (await NativeContracts.create()).validateIdentity("interaction", value);
}

/** Validates and detaches one exact approval intent. */
export async function approvalBinding(value: ApprovalBinding): Promise<ApprovalBinding> {
  return (await NativeContracts.create()).validate("approval_binding", value);
}

export function responderGrant(ticket: Pick<InteractionTicket, "id">): string {
  return `interaction:respond:${ticket.id}`;
}

/** Validates and detaches the metadata envelope; request bytes are checked by the owner. */
export async function interactionTicket(value: InteractionTicket): Promise<InteractionTicket> {
  return (await NativeContracts.create()).validate("interaction_ticket", value);
}

/** Validates a CAS resolution envelope; Rust checks the current version and grant. */
export async function interactionResolution(value: InteractionResolution, ticket: InteractionTicket): Promise<InteractionResolution> {
  return (await NativeContracts.create()).validate("interaction_resolution", value, ticket);
}
