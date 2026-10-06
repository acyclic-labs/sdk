/** Public Harness enum spellings derived from generated Rust/protobuf types. */
import type {
  WasmGroupPolicy,
  WasmResourceRefWire,
  WasmVolumeRefWire,
  AuthorityLevel as WasmAuthorityLevel,
} from "../generated/wasm/acyclic_harness_wasm.js";
import {
  AggregateKind as WireAggregateKind,
  ConversationKind as WireConversationKind,
  InteractionKind as WireInteractionKind,
  ExtensionForkPolicy as WireExtensionForkPolicy,
  SharedVolumeOperation as WireVolumeOperation,
} from "../generated/proto/harness/v2/harness_pb.js";

type WireNames<Wire extends object> = Exclude<keyof Wire, "UNSPECIFIED"> & string;
type SnakeCase<Value extends string> = Value extends `${infer Head}_${infer Tail}`
  ? `${Lowercase<Head>}_${SnakeCase<Tail>}`
  : Lowercase<Value>;
type PublicEnum<Wire extends object> = SnakeCase<WireNames<Wire>>;
type AssertNever<Value extends never> = Value;
/** Members present in only one of two unions; `never` when they match exactly. */
type Mismatch<Actual, Expected> = Exclude<Actual, Expected> | Exclude<Expected, Actual>;

/** Exact string unions emitted by the Rust WASM DTO generator. */
export type VolumeClass = WasmVolumeRefWire["class"];
export type ResourceKind = WasmResourceRefWire["kind"];
export type GroupPolicy = WasmGroupPolicy;
export type AuthorityLevel = WasmAuthorityLevel;

/** Lower snake-case names are derived directly from protobuf enum members. */
export type MessageKind = PublicEnum<typeof WireConversationKind>;
export type VolumeOperation = PublicEnum<typeof WireVolumeOperation>;
export type InteractionKind = PublicEnum<typeof WireInteractionKind>;
export type ExtensionForkPolicy = PublicEnum<typeof WireExtensionForkPolicy>;
export type AggregateKind = PublicEnum<typeof WireAggregateKind>;

type _MessageKinds = AssertNever<Mismatch<MessageKind, "user" | "assistant" | "system" | "tool_call" | "tool_result"
  | "interaction" | "permission" | "fork" | "merge">>;
type _VolumeOperations = AssertNever<Mismatch<VolumeOperation, "read" | "write">>;
type _InteractionKinds = AssertNever<Mismatch<InteractionKind, "question" | "choice" | "form" | "approval">>;
type _ExtensionForkPolicies = AssertNever<Mismatch<ExtensionForkPolicy, "inherit" | "reset" | "reject">>;
type _AggregateKinds = AssertNever<Mismatch<AggregateKind, "agent" | "conversation" | "session" | "turn" | "task">>;

/** Aggregate identity is encoded in every Harness authority envelope. */
export const aggregateKindToWire = Object.freeze({
  agent: WireAggregateKind.AGENT,
  conversation: WireAggregateKind.CONVERSATION,
  session: WireAggregateKind.SESSION,
  turn: WireAggregateKind.TURN,
  task: WireAggregateKind.TASK,
} satisfies Record<AggregateKind, WireAggregateKind>);
const aggregateKindFromWire = Object.freeze({
  [WireAggregateKind.AGENT]: "agent",
  [WireAggregateKind.CONVERSATION]: "conversation",
  [WireAggregateKind.SESSION]: "session",
  [WireAggregateKind.TURN]: "turn",
  [WireAggregateKind.TASK]: "task",
} as const satisfies Partial<Record<WireAggregateKind, AggregateKind>>);

export function decodeAggregateKind(value: number): AggregateKind {
  const decoded = (aggregateKindFromWire as Partial<Record<number, AggregateKind>>)[value];
  if (decoded === undefined) throw new TypeError(`unknown aggregate kind enum value: ${value}`);
  return decoded;
}

const collectAllPolicy = Object.freeze({ kind: "collect-all" } as const);
const cancelOnFailurePolicy = Object.freeze({ kind: "cancel-on-failure" } as const);
/** Stable object values retained by the existing GroupPolicies API. */
export const groupPolicies = Object.freeze({
  collectAll: collectAllPolicy,
  cancelOnFailure: cancelOnFailurePolicy,
});
