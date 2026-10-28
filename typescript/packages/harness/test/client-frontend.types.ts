import type {
  AgentClient, AgentCommand, AgentControls, AgentEvent, AgentWindow, ApprovalBinding, ModelContent,
  ModelContentPart, ModelDataPart, StatusRecord,
  PreparedAgentInvocation,
  DiscoveredAgentControls,
} from "../src/index.js";
import type {
  ApprovalBinding as RustApprovalBinding, ModelContent as RustModelContent,
  ModelContentPart as RustModelContentPart, ModelDataPart as RustModelDataPart,
  WasmExtensionAdmissionWire, WasmFileRefWire,
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

// Retained extension configuration still requires an immutable generated ref.
function extensionConfigurationUsesAFileRef(extension: WasmExtensionAdmissionWire): void {
  const content: WasmFileRefWire = extension.configurations[0]!.content;
  // @ts-expect-error extension configuration content cannot be an opaque route
  const invalidContent: WasmFileRefWire = "bad";
  void [content, invalidContent];
}

// Public encoding is the existing Rust boundary, not a frontend JSON codec.
type _CanonicalEncoderIsRust = Assert<Equal<
  typeof import("../src/index.js").encodeCanonicalJson,
  typeof import("../generated/wasm/acyclic_harness_wasm.js").encodeCanonicalJson
>>;

type _PreparationUsesTheRustCommand = Assert<Equal<
  Parameters<typeof import("../src/index.js").prepareAgentInvocation>,
  [controls: AgentControls, value: AgentCommand]
>>;
type _PreparationKeepsCallerOwnedInvocation = Assert<Equal<
  ReturnType<typeof import("../src/index.js").prepareAgentInvocation>,
  PreparedAgentInvocation
>>;

type _DiscoveryReturnsTheRustResult = Assert<Equal<
  Awaited<ReturnType<typeof import("../src/client.js").discoverAgentControls>>,
  DiscoveredAgentControls
>>;
type _DiscoveredControlsUseTheRustContract = Assert<Equal<DiscoveredAgentControls["controls"], AgentControls>>;
