import type { TaskContext, ToolContext } from "./runtime.js";
import type { Attachment, FileRef } from "./conversation.js";
import type { SelectedModelContext } from "./projection.js";
import type { MachineIdentityWire } from "./native-contracts.js";

export interface Model<Options = unknown> { readonly provider: string; readonly name: string; readonly revision: string; readonly options: Options }
export type ModelContentPart =
  | Readonly<{ kind: "text"; text: string }>
  | Readonly<{ kind: "file"; file: FileRef; policy: "reference" | "bounded_full" | "native" }>
  | Readonly<{ kind: "tool_call"; callId: string; name: string; arguments: unknown }>
  | Readonly<{ kind: "tool_result"; callId: string; name: string; value: unknown }>;
export type UserContentPart = Extract<ModelContentPart, { kind: "text" | "file" }>;
export type ModelContent = string | ModelContentPart | readonly ModelContentPart[];
export interface ModelMessage<Content = ModelContent, Role extends string = "system" | "user" | "assistant" | "tool"> { readonly role: Role; readonly content: Content }
export type ToolJsonValue = null | string | number | boolean | readonly ToolJsonValue[] | Readonly<{ [key: string]: ToolJsonValue }>;
export type ToolJsonSchema = boolean | Readonly<{ [key: string]: ToolJsonValue }>;
export interface ToolDefinition<Input = unknown, Output = unknown, InputSchema extends ToolJsonSchema = ToolJsonSchema, OutputSchema extends ToolJsonSchema = ToolJsonSchema> {
  readonly name: string;
  readonly revision: string;
  readonly description: string;
  readonly inputSchema: InputSchema;
  readonly outputSchema: OutputSchema;
  /** Converts only schema-admitted JSON into the handler's input type. */
  readonly parseInput: (value: unknown) => Input;
  /** Verifies the executor's schema-admitted output before typed publication. */
  readonly parseOutput: (value: unknown) => Output;
  readonly handler?: LiveTool<Input, Output>;
}
/** Only the declarative schema crosses the model boundary; executors stay private. */
export type ModelToolDefinition<InputSchema extends ToolJsonSchema = ToolJsonSchema, OutputSchema extends ToolJsonSchema = ToolJsonSchema> = Pick<ToolDefinition<unknown, unknown, InputSchema, OutputSchema>, "name" | "revision" | "description" | "inputSchema" | "outputSchema">;
export interface ModelRequest<ModelOptions = unknown, Content = ModelContent> { readonly model: Model<ModelOptions>; readonly messages: readonly ModelMessage<Content>[]; readonly tools: readonly ModelToolDefinition[]; readonly maxOutputTokens?: number; readonly signal?: AbortSignal }
export type ModelEvent<Arguments = unknown, Metadata = unknown> =
  | { readonly kind: "content"; readonly delta: string }
  | { readonly kind: "reasoning"; readonly delta: string }
  | { readonly kind: "tool_call"; readonly callId: string; readonly name: string; readonly arguments: Arguments }
  | { readonly kind: "completed"; readonly metadata: Metadata };
export interface ModelAttempt<Event extends ModelEvent = ModelEvent> { readonly operationId: string; readonly step: number; readonly requestDigest: Uint8Array; readonly observed: readonly Event[] }
export interface ModelProvider<Request extends ModelRequest = ModelRequest, Event extends ModelEvent = ModelEvent> { generate(request: Request): AsyncIterable<Event>; reconcile(attempt: ModelAttempt<Event>): Promise<readonly Event[] | undefined> }

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
  if (typeof value !== "string" || !value || new TextEncoder().encode(value).byteLength > 255 || /[\s\p{Cc}\\/]/u.test(value)
    || value === "." || value === "..") throw new TypeError(`${field} is invalid`);
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
