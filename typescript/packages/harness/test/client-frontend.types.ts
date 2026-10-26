import type {
  AgentClient, AgentEvent, AgentWindow, ApprovalBinding, ModelContent,
  ModelContentPart, ModelDataPart, StatusRecord,
} from "../src/index.js";
import type {
  ApprovalBinding as RustApprovalBinding, ModelContent as RustModelContent,
  ModelContentPart as RustModelContentPart, ModelDataPart as RustModelDataPart,
} from "../generated/wasm/acyclic_harness_wasm.js";

type Assert<Condition extends true> = Condition;
type Equal<Left, Right> = (<Value>() => Value extends Left ? 1 : 2) extends
  (<Value>() => Value extends Right ? 1 : 2) ? true : false;

type _AgentViewCursorsStayExact = Assert<Equal<Parameters<AgentClient["setView"]>[1], BigUint64Array>>;
type _AgentWindowUsesTheRustContract = Assert<Equal<ReturnType<AgentClient["agentWindow"]>, AgentWindow>>;
type _AgentWindowCountersStayExact = Assert<Equal<AgentWindow[keyof AgentWindow], bigint>>;
type _AgentEventsUseTheRustContract = Assert<Equal<Awaited<ReturnType<AgentClient["next"]>>, AgentEvent>>;
type _AgentRecordsExposeAbsence = Assert<Equal<ReturnType<AgentClient["agentRecord"]>, AgentEvent | null>>;
type _ApprovalStatusesUseTheRustContract = Assert<Equal<ReturnType<AgentClient["agentStatus"]>, StatusRecord | null>>;
type _ApprovalUsesOneRustDefinition = Assert<Equal<ApprovalBinding, RustApprovalBinding>>;
type _ModelContentUsesOneRustDefinition = Assert<Equal<
  [ModelContent, ModelContentPart, ModelDataPart],
  [RustModelContent, RustModelContentPart, RustModelDataPart]
>>;

// Public encoding is the existing Rust boundary, not a frontend JSON codec.
type _CanonicalEncoderIsRust = Assert<Equal<
  typeof import("../src/index.js").encodeCanonicalJson,
  typeof import("../generated/wasm/acyclic_harness_wasm.js").encodeCanonicalJson
>>;
