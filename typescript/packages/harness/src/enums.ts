/** Public Harness enum spellings derived from generated Rust/protobuf types. */
import type {
  WasmResourceRefWire,
  WasmVolumeRefWire,
} from "../generated/wasm/acyclic_harness_wasm.js";
export type { WasmGroupPolicy as GroupPolicy, AuthorityLevel } from "../generated/wasm/acyclic_harness_wasm.js";
import {
  AggregateKind as WireAggregateKind,
  ConversationKind as WireConversationKind,
  InteractionKind as WireInteractionKind,
  ExtensionForkPolicy as WireExtensionForkPolicy,
  SharedVolumeOperation as WireVolumeOperation,
} from "../generated/proto/harness/v2/harness_pb.js";

type PublicEnum<Wire extends object> = Lowercase<Exclude<keyof Wire, "UNSPECIFIED"> & string>;
type AssertNever<Value extends never> = Value;
/** Members present in only one of two unions; `never` when they match exactly. */
type Mismatch<Actual, Expected> = Exclude<Actual, Expected> | Exclude<Expected, Actual>;

/** Exact string unions emitted by the Rust WASM DTO generator. */
export type VolumeClass = WasmVolumeRefWire["class"];
export type ResourceKind = WasmResourceRefWire["kind"];

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
export function decodeAggregateKind(value: number): AggregateKind {
  for (const [kind, wire] of Object.entries(aggregateKindToWire)) {
    if (wire === value) return kind as AggregateKind;
  }
  throw new TypeError(`unknown aggregate kind enum value: ${value}`);
}
