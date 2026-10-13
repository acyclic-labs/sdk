import type { ToolContext } from "./task-context.js";
import type { FileRef } from "./conversation.js";
import type { SelectedModelContext } from "./projection.js";
import type { WasmMachineIdentityWire } from "./native-contracts.js";
import type {
  ModelContextCapacity as ModelContextCapacityWire,
  ModelTokenCount as ModelTokenCountWire,
  ModelContentPart as ModelContentPartWire,
  ModelDataPart as ModelDataPartWire,
  ToolResultContent as ToolResultContentWire,
  ModelEvent as ModelEventWire,
  ModelRequest as ModelRequestWire,
  ToolDefinition as ToolDefinitionWire,
  Model as ModelWire,
  ModelDispatch as ModelDispatchWire,
  ModelRole,
  WasmToolJsonSchema as ToolJsonSchema,
  WasmToolJsonValue as ToolJsonValue,
} from "../generated/wasm/acyclic_harness_wasm.js";
export type { ModelRole, WasmToolJsonSchema as ToolJsonSchema, WasmToolJsonValue as ToolJsonValue } from "../generated/wasm/acyclic_harness_wasm.js";
import { NativeContracts } from "./native-contracts.js";
import { validateComponentLabel } from "./rust-policy.js";
export { validateComponentLabel } from "./rust-policy.js";

/** Public model options retain the Rust model's JSON boundary. */
export type Model = Readonly<Omit<ModelWire, "options"> & { options: unknown }>;

type PublicModelDataPart<Part extends ModelDataPartWire> =
  Part extends Readonly<{ kind: "file" }>
    ? Readonly<Omit<Part, "file"> & { file: FileRef }>
    : Readonly<Part>;
export type ModelDataPart = PublicModelDataPart<ModelDataPartWire>;
type PublicToolResultContent<Content extends ToolResultContentWire, Value> =
  Content extends Readonly<{ kind: "json" }>
    ? Readonly<Omit<Content, "value"> & { value: Value }>
    : Readonly<Omit<Content, "parts"> & { parts: readonly ModelDataPart[] }>;
/** Complete, nonrecursive result envelope derived from the Rust content contract. */
export type ToolResultContent<Value = unknown> = PublicToolResultContent<ToolResultContentWire, Value>;
/** Explicit identity projection using the Rust-owned JSON envelope schema. */
export async function jsonToolProjection<Output>(schema: ToolJsonSchema): Promise<ToolProjection<unknown, Output, Output>> {
  const contracts = await NativeContracts.create();
  return Object.freeze({ schema: contracts.jsonToolProjectionSchema(schema),
    project: (_invocation: ToolInvocation, result: ToolResult<Output>): ToolResultContent<Output> => ({ kind: "json", value: result.value }) });
}

type PublicModelContentPart<Part extends ModelContentPartWire> =
  Part extends Readonly<{ kind: "tool_call" }>
    ? Readonly<Omit<Part, "call_id" | "arguments"> & { callId: string; arguments: unknown }>
    : Part extends Readonly<{ kind: "tool_result" }>
      ? Readonly<Omit<Part, "call_id" | "content"> & { callId: string; content: ToolResultContent }>
      : Part extends Readonly<{ kind: "file" }>
        ? Readonly<Omit<Part, "file"> & { file: FileRef }>
        : Readonly<Part>;

/** CamelCase facade derived from the Rust serde content union. */
export type ModelContentPart = PublicModelContentPart<ModelContentPartWire>;
export type UserContentPart = Extract<ModelContentPart, { kind: "text" | "file" }>;
export type ModelContent = string | ModelContentPart | readonly ModelContentPart[];
export interface ModelMessage {
  readonly role: ModelRole;
  readonly content: ModelContent;
}
export interface ToolDefinition<Input = unknown, Output = unknown> {
  readonly name: string;
  readonly revision: string;
  readonly description: string;
  readonly inputSchema: ToolJsonSchema;
  readonly outputSchema: ToolJsonSchema;
  readonly projection: ToolProjection<Input, Output>;
  /** Converts only schema-admitted JSON into the handler's input type. */
  readonly parseInput: (value: unknown) => Input;
  /** Verifies the executor's schema-admitted output before typed publication. */
  readonly parseOutput: (value: unknown) => Output;
  readonly handler?: (context: ToolContext, input: Input) => Output | Promise<Output>;
}
/** Only the declarative schema crosses the model boundary; executors stay private. */
export type ModelToolDefinition =
  Readonly<Omit<ToolDefinitionWire, "input_schema" | "output_schema" | "projection_schema"> & {
    inputSchema: ToolJsonSchema;
    outputSchema: ToolJsonSchema;
    projectionSchema: ToolJsonSchema;
  }>;
export type ModelRequest =
  Readonly<Omit<ModelRequestWire, "model" | "messages" | "tools" | "max_output_tokens"> & {
    model: Model;
    messages: readonly ModelMessage[];
    tools: readonly ModelToolDefinition[];
    maxOutputTokens?: number;
    signal?: AbortSignal;
  }>;
type PublicModelEvent<Event extends ModelEventWire> =
  Event extends Readonly<{ kind: "tool_call" }>
    ? Readonly<Omit<Event, "call_id" | "arguments"> & { callId: string; arguments: unknown }>
    : Event extends Readonly<{ kind: "completed" }>
      ? Readonly<Omit<Event, "metadata"> & { metadata: unknown }>
      : Readonly<Event>;
export type ModelEvent = PublicModelEvent<ModelEventWire>;
export type ModelAttempt =
  Readonly<Omit<ModelDispatchWire, "operation_id" | "request_digest"> & {
    operationId: string;
    requestDigest: Uint8Array;
    observed: readonly ModelEvent[];
  }>;
/** Provider capacity for the exact immutable model, derived from the Rust proof. */
export type ModelContextCapacity = Readonly<Omit<ModelContextCapacityWire, "context_tokens" | "output_tokens"> & {
  contextTokens: ModelContextCapacityWire["context_tokens"];
  outputTokens: ModelContextCapacityWire["output_tokens"];
}>;
/** Additive upper bounds bound to the exact prepared bytes and ordered messages. */
export type ModelTokenCount = Readonly<Omit<ModelTokenCountWire, "request_digest" | "fixed_tokens" | "message_tokens"> & {
  requestDigest: Uint8Array;
  fixedTokens: ModelTokenCountWire["fixed_tokens"];
  messageTokens: readonly ModelTokenCountWire["message_tokens"][number][];
}>;
export interface ModelProvider {
  /** Synchronous, without dispatch effects; unsupported models must reject explicitly. */
  contextCapacity(model: Model): ModelContextCapacity;
  /** Synchronous; serializedInput is a defensive copy of the exact canonical request. */
  countTokens(request: ModelRequest & { readonly serializedInput: Uint8Array }): ModelTokenCount;
  generate(request: ModelRequest & { readonly serializedInput: Uint8Array }): AsyncIterable<ModelEvent>;
  reconcile(attempt: ModelAttempt): Promise<readonly ModelEvent[] | undefined>;
}

export interface ToolInvocation<Input = unknown> {
  /** Runtime-owned reconciliation identity; provider call IDs can recur across turns. */
  readonly operationId: string;
  readonly callId: string;
  readonly name: string;
  readonly arguments: Input;
}
export interface ToolResult<Output = unknown> { readonly value: Output }
export interface ToolExecutor<Input = unknown, Output = unknown> { execute(invocation: ToolInvocation<Input>): Promise<ToolResult<Output>>; reconcile(invocation: ToolInvocation<Input>): Promise<ToolResult<Output> | undefined> }
/** Pure model-facing projection with its independently pinned envelope schema. */
export interface ToolProjection<Input = unknown, Output = unknown, Projected = unknown> {
  readonly schema: ToolJsonSchema;
  project(invocation: ToolInvocation<Input>, result: ToolResult<Output>): ToolResultContent<Projected>;
}
declare const toolRefBrand: unique symbol;
/** Opaque registered handle: implementation and executor never enter task code. */
export interface ToolRef<Input, Output> {
  readonly definition: ModelToolDefinition;
  /** Pinned owner-host machine for a durable-only resumable tool. */
  readonly machine?: WasmMachineIdentityWire;
  readonly [toolRefBrand]: (input: Input) => Output;
}
export async function defineTool<Input, Output>(definition: Omit<ToolDefinition<Input, Output>, "handler">, handler: (context: ToolContext, input: Input) => Output | Promise<Output>): Promise<ToolDefinition<Input, Output>> {
  await validateComponentLabel(definition.name, "tool name");
  await validateComponentLabel(definition.revision, "tool revision");
  if (typeof definition.parseInput !== "function" || typeof definition.parseOutput !== "function") {
    throw new TypeError("typed tools require input and output parsers");
  }
  if (typeof definition.projection?.project !== "function") {
    throw new TypeError("typed tools require an explicit result projection");
  }
  return Object.freeze({ ...definition, handler });
}
export interface ContextBuilder { build(input: RuntimeAgentInput, messages: readonly ModelMessage[]): Promise<readonly ModelMessage[]> }
export interface AgentInput { readonly prompt: string; readonly content?: readonly UserContentPart[] }
/** Runtime input after an owning conversation has committed a context selection. */
export interface RuntimeAgentInput extends AgentInput { readonly selectedContext?: SelectedModelContext }
