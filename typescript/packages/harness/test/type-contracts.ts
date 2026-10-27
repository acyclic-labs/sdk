import type { AgentClient, AgentEvent, AgentWindow, StatusRecord } from "../src/client.js";
import type {
  ModelContent as ModelContentWire, ModelEvent as ModelEventWire,
  SummaryForkSelection as RustSummaryForkSelection, WasmNativeLimitsWire,
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

type _SummaryLimitsUseExistingNativeWire = Assert<Equal<RustSummaryForkSelection["limits"], WasmNativeLimitsWire>>;
type _SummaryLimitsPreserveEveryBigint = Assert<Equal<
  RustSummaryForkSelection["limits"][keyof RustSummaryForkSelection["limits"]], bigint
>>;

const generatedModelContent: ModelContentWire = {
  kind: "tool_call", call_id: "call", name: "lookup", arguments: { value: 1 },
};
const generatedModelEvent: ModelEventWire = {
  kind: "tool_call", call_id: "call", name: "lookup", arguments: { value: 1 },
};
void [generatedModelContent, generatedModelEvent];
