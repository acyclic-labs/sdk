import type { BrowserAgentRegistry } from "../src/browser.js";
import type { CompactionPolicy, ModelContextCapacity, ModelTokenCount,
  ModelEvent, ModelEventInput, ModelContentInput } from "../src/index.js";

type Assert<Condition extends true> = Condition;
type Equal<Left, Right> = (<Value>() => Value extends Left ? 1 : 2) extends
  (<Value>() => Value extends Right ? 1 : 2) ? true : false;
type ConfigureModel = Parameters<BrowserAgentRegistry["configureModel"]>;

// These replace the deleted task facade's compaction/capacity/count aliases.
// The public types describe the actual host callback boundary, in Rust fields.
type _CompactionIsTheHostContract = Assert<Equal<ConfigureModel[5], CompactionPolicy>>;
type _CapacityIsTheHostContract = Assert<Equal<ReturnType<NonNullable<ConfigureModel[3]>>, ModelContextCapacity>>;
type _AccountingIsTheHostContract = Assert<Equal<Awaited<ReturnType<NonNullable<ConfigureModel[4]>>>, ModelTokenCount>>;

const providerEvent: ModelEvent = { kind: "tool_call", call_id: "call", name: "lookup", arguments: {}, metadata: null };
const eventInput: ModelEventInput = { kind: "tool_call", callId: "call", name: "lookup", arguments: {}, metadata: null };
const contentInput: ModelContentInput = eventInput;
// @ts-expect-error canonical provider events use the Rust call_id field
const facadeEvent: ModelEvent = { kind: "tool_call", callId: "call", name: "lookup", arguments: {} };
void [providerEvent, eventInput, contentInput, facadeEvent];
