import type { TaskContext, ToolContext } from "./runtime.js";
import type { Attachment, FileRef } from "./conversation.js";
import type { SelectedModelContext } from "./projection.js";
import type { MachineIdentityWire } from "./native-contracts.js";
import type {
  WasmModelContentPart,
  WasmModelEvent,
  WasmModelJsonSchema,
  WasmModelJsonValue,
  WasmModelRequestWire,
  WasmModelToolDefinitionWire,
  WasmModelWire,
  WasmModelAttemptWire,
  WasmModelRole,
  WasmToolJsonSchema,
  WasmToolJsonValue,
} from "../generated/wasm/acyclic_harness_wasm.js";
import { isValidComponentLabel } from "./component-label-contract.js";

/** Public model options retain the Rust model's JSON boundary while allowing provider-specific typing. */
export type Model<Options = unknown> = Readonly<Omit<WasmModelWire, "options"> & { options: Options }>;
export type ModelRole = WasmModelRole;

type PublicModelContentPart<Part extends WasmModelContentPart, Arguments, Result> =
  Part extends Readonly<{ kind: "tool_call" }>
    ? Readonly<Omit<Part, "call_id" | "arguments"> & { callId: string; arguments: Arguments }>
    : Part extends Readonly<{ kind: "tool_result" }>
      ? Readonly<Omit<Part, "call_id" | "value"> & { callId: string; value: Result }>
      : Part extends Readonly<{ kind: "file" }>
        ? Readonly<Omit<Part, "file"> & { file: FileRef }>
        : Readonly<Part>;

/** CamelCase facade derived from the Rust serde content union. */
export type ModelContentPart<Arguments = unknown, Result = unknown> =
  PublicModelContentPart<WasmModelContentPart, Arguments, Result>;
export type UserContentPart = Extract<ModelContentPart, { kind: "text" | "file" }>;
export type ModelContent<Arguments = unknown, Result = unknown> =
  string | ModelContentPart<Arguments, Result> | readonly ModelContentPart<Arguments, Result>[];
export interface ModelMessage<Content = ModelContent, Role extends string = ModelRole> {
  readonly role: Role;
  readonly content: Content;
}
export type ToolJsonValue = WasmToolJsonValue;
export type ToolJsonSchema = WasmToolJsonSchema;
export type ModelJsonValue = WasmModelJsonValue;
export type ModelJsonSchema = WasmModelJsonSchema;
export interface ToolDefinition<Input = unknown, Output = unknown, InputSchema extends ToolJsonSchema = ToolJsonSchema, OutputSchema extends ToolJsonSchema = ToolJsonSchema> {
  readonly name: string;
  readonly revision: string;
  readonly description: string;
  readonly inputSchema: InputSchema;
  readonly outputSchema: OutputSchema;
  /** JSON Schema for the model-visible value after projection. Defaults to outputSchema. */
  readonly modelOutputSchema?: OutputSchema;
  /** Converts the typed owner result into the model-visible projection. */
  readonly projectOutput?: (value: Output) => unknown;
  /** Converts only schema-admitted JSON into the handler's input type. */
  readonly parseInput: (value: unknown) => Input;
  /** Verifies the executor's schema-admitted output before typed publication. */
  readonly parseOutput: (value: unknown) => Output;
  readonly handler?: LiveTool<Input, Output>;
}
/** Only the declarative schema crosses the model boundary; executors stay private. */
export type ModelToolDefinition<InputSchema extends ToolJsonSchema = ToolJsonSchema, OutputSchema extends ToolJsonSchema = ToolJsonSchema> =
  Readonly<Omit<WasmModelToolDefinitionWire, "input_schema" | "output_schema" | "model_output_schema"> & {
    inputSchema: InputSchema;
    outputSchema: OutputSchema;
    modelOutputSchema?: OutputSchema;
  }>;
/** Immutable evidence for the exact Rust-admitted request sent to a provider. */
export interface ModelRequestEvidence {
  readonly requestJson: string;
  readonly manifestJson: string;
  readonly requestDigest: readonly number[];
}
export type ModelRequest<ModelOptions = unknown, Content = ModelContent> =
  Readonly<Omit<WasmModelRequestWire, "model" | "messages" | "tools" | "max_output_tokens"> & {
    model: Model<ModelOptions>;
    messages: readonly ModelMessage<Content>[];
    tools: readonly ModelToolDefinition[];
    maxOutputTokens?: number;
    signal?: AbortSignal;
    /** Transport metadata; adapters must keep it out of model-visible content. */
    canonical?: ModelRequestEvidence;
  }>;
type PublicModelEvent<Event extends WasmModelEvent, Arguments, Metadata> =
  Event extends Readonly<{ kind: "tool_call" }>
    ? Readonly<Omit<Event, "call_id" | "arguments"> & { callId: string; arguments: Arguments }>
    : Event extends Readonly<{ kind: "completed" }>
      ? Readonly<Omit<Event, "metadata"> & { metadata: Metadata }>
      : Readonly<Event>;
export type ModelEvent<Arguments = unknown, Metadata = unknown> =
  PublicModelEvent<WasmModelEvent, Arguments, Metadata>;
export type ModelAttempt<Event extends ModelEvent = ModelEvent> =
  Readonly<Omit<WasmModelAttemptWire, "operation_id" | "request_digest" | "observed"> & {
    operationId: string;
    requestDigest: Uint8Array;
    observed: readonly Event[];
  }>;
export interface ModelProvider<Request extends ModelRequest = ModelRequest, Event extends ModelEvent = ModelEvent> {
  /** Admits the provider-registered, model-visible options before Rust/WASM request construction. */
  readonly admitModel?: (model: Model) => void;
  generate(request: Request): AsyncIterable<Event>;
  reconcile(attempt: ModelAttempt<Event>): Promise<readonly Event[] | undefined>;
}

/** Safe default for providers that expose no registered public model options. */
export function validateDefaultModelOptions(options: unknown): void {
  if (options === null || (typeof options === "object" && !Array.isArray(options)
    && Object.keys(options as Record<string, unknown>).length === 0)) return;
  throw new TypeError("model options require an explicitly registered provider policy");
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
export interface ToolProjection<Input = unknown, Output = unknown, Projected = unknown> { project(invocation: ToolInvocation<Input>, result: ToolResult<Output>): Projected }
export interface Tool<Input = unknown, Output = unknown, Projected = unknown> { readonly definition: ToolDefinition<Input, Output>; readonly executor: ToolExecutor<Input, Output>; readonly projection: ToolProjection<Input, Output, Projected> }
declare const toolRefBrand: unique symbol;
/** Opaque registered handle: implementation and executor never enter task code. */
export interface ToolRef<Input, Output> {
  readonly definition: ModelToolDefinition;
  /** Pinned owner-host machine for a durable-only resumable tool. */
  readonly machine?: MachineIdentityWire;
  readonly [toolRefBrand]: (input: Input) => Output;
}
export type LiveTool<Input, Output> = (context: ToolContext, input: Input) => Output | Promise<Output>;
export function validateComponentLabel(value: string, field: string): void {
  if (typeof value !== "string" || !isValidComponentLabel(value)) throw new TypeError(`${field} is invalid`);
}
export function validateToolName(name: string): void {
  validateComponentLabel(name, "tool name");
}
export function defineTool<Input, Output>(definition: Omit<ToolDefinition<Input, Output>, "handler">, handler: LiveTool<Input, Output>): ToolDefinition<Input, Output> {
  validateToolName(definition.name);
  validateComponentLabel(definition.revision, "tool revision");
  if (typeof definition.parseInput !== "function" || typeof definition.parseOutput !== "function") {
    throw new TypeError("typed tools require input and output parsers");
  }
  return Object.freeze({ ...definition, handler });
}
export interface ContextStage { readonly name: string; apply(input: unknown, messages: readonly ModelMessage[]): Promise<readonly ModelMessage[]> }
export interface ContextBuilder<Input = AgentLoopInput, Message extends ModelMessage = ModelMessage> { build(input: Input, messages: readonly Message[]): Promise<readonly Message[]> }
export interface AgentInput<Content = UserContentPart> { readonly prompt: string; readonly content?: readonly Content[] }
/** Runtime input after an owning conversation has committed a context selection. */
export interface AgentLoopInput extends AgentInput { readonly selectedContext?: SelectedModelContext }
export interface AgentOutput<Content = unknown, Artifact = unknown> { readonly text: string; readonly content?: readonly Content[]; readonly artifacts?: readonly Artifact[]; readonly attachments?: readonly Attachment[] }
export interface AgentLoop<Input extends AgentInput = AgentLoopInput, Output extends AgentOutput = AgentOutput> { run(context: TaskContext, input: Input): Promise<Output> }
