import type {
  ModelContentPart as ModelContentPartWire, ModelEvent as ModelEventWire,
  ModelRole as ModelRoleWire, ModelTokenCount, ReasoningBlock,
} from "../generated/wasm/acyclic_harness_wasm.js";

type AssertNever<Value extends never> = Value;
type Assert<Condition extends true> = Condition;
type Equal<Left, Right> = (<Value>() => Value extends Left ? 1 : 2) extends
  (<Value>() => Value extends Right ? 1 : 2) ? true : false;

type _ModelRolesHaveNoUntrackedRustVariants = AssertNever<Exclude<ModelRoleWire,
  "system" | "user" | "assistant" | "tool">>;
type _ModelRolesHaveNoMissingPublicVariants = AssertNever<Exclude<
  "system" | "user" | "assistant" | "tool", ModelRoleWire>>;
type _ModelPartsHaveNoUntrackedRustVariants = AssertNever<Exclude<ModelContentPartWire["kind"],
  "text" | "file" | "reasoning" | "tool_call" | "tool_result">>;
type _ModelPartsHaveNoMissingPublicVariants = AssertNever<Exclude<
  "text" | "file" | "reasoning" | "tool_call" | "tool_result", ModelContentPartWire["kind"]>>;
type _ModelEventsHaveNoUntrackedRustVariants = AssertNever<Exclude<ModelEventWire["kind"],
  "content" | "reasoning" | "reasoning_block" | "tool_call" | "completed">>;
type _ModelEventsHaveNoMissingPublicVariants = AssertNever<Exclude<
  "content" | "reasoning" | "reasoning_block" | "tool_call" | "completed", ModelEventWire["kind"]>>;

type _VerifiedAdmissionCountIsLossless = Assert<Equal<ModelTokenCount["input_tokens"], bigint>>;
type _CompletedReasoningRetainsTheCanonicalBlock = Assert<Equal<
  Extract<ModelEventWire, { kind: "reasoning_block" }>["block"], ReasoningBlock
>>;
type _ProjectedReasoningRetainsTheCanonicalBlock = Assert<Equal<
  Extract<ModelContentPartWire, { kind: "reasoning" }>["block"], ReasoningBlock
>>;
