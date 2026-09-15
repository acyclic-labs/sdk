import type { TaskContext, ToolContext } from "./runtime.js";

export interface Model<Options = unknown> { readonly provider: string; readonly name: string; readonly revision: string; readonly options: Options }
export interface ModelMessage<Content = unknown, Role extends string = "system" | "user" | "assistant" | "tool"> { readonly role: Role | (string & {}); readonly content: Content }
export interface ToolDefinition<Input = unknown, Output = unknown, InputSchema = unknown, OutputSchema = unknown> { readonly name: string; readonly revision: string; readonly description: string; readonly inputSchema: InputSchema; readonly outputSchema: OutputSchema; readonly handler?: LiveTool<Input, Output> }
export interface ModelRequest<ModelOptions = unknown, Content = unknown> { readonly model: Model<ModelOptions>; readonly messages: readonly ModelMessage<Content>[]; readonly tools: readonly ToolDefinition[]; readonly maxOutputTokens?: number; readonly signal?: AbortSignal }
export type ModelEvent<Arguments = unknown, Metadata = unknown> =
  | { readonly kind: "content"; readonly delta: string }
  | { readonly kind: "reasoning"; readonly delta: string }
  | { readonly kind: "tool_call"; readonly callId: string; readonly name: string; readonly arguments: Arguments }
  | { readonly kind: "completed"; readonly metadata: Metadata };
export interface ModelAttempt<Event extends ModelEvent = ModelEvent> { readonly operationId: string; readonly step: number; readonly requestDigest: Uint8Array; readonly observed: readonly Event[] }
export interface ModelProvider<Request extends ModelRequest = ModelRequest, Event extends ModelEvent = ModelEvent> { generate(request: Request): AsyncIterable<Event>; reconcile(attempt: ModelAttempt<Event>): Promise<readonly Event[] | undefined> }

export interface ToolInvocation<Input = unknown> { readonly callId: string; readonly name: string; readonly arguments: Input }
export interface ToolResult<Output = unknown> { readonly value: Output }
export interface ToolExecutor<Input = unknown, Output = unknown> { execute(invocation: ToolInvocation<Input>): Promise<ToolResult<Output>>; reconcile(invocation: ToolInvocation<Input>): Promise<ToolResult<Output> | undefined> }
export interface ToolProjection<Input = unknown, Output = unknown, Projected = unknown> { project(invocation: ToolInvocation<Input>, result: ToolResult<Output>): Projected }
export interface Tool<Input = unknown, Output = unknown, Projected = unknown> { readonly definition: ToolDefinition<Input, Output>; readonly executor: ToolExecutor<Input, Output>; readonly projection: ToolProjection<Input, Output, Projected> }
export interface ToolRef<Input, Output> { readonly definition: ToolDefinition<Input, Output>; readonly executor?: ToolExecutor<Input, Output> }
export type LiveTool<Input, Output> = (context: ToolContext, input: Input) => Output | Promise<Output>;
export function defineTool<Input, Output>(definition: Omit<ToolDefinition<Input, Output>, "handler">, handler: LiveTool<Input, Output>): ToolDefinition<Input, Output> { if (!definition.name.trim() || !definition.revision.trim()) throw new TypeError("tool name and revision are required"); return Object.freeze({ ...definition, handler }); }
export interface ContextStage { readonly name: string; apply(input: unknown, messages: readonly ModelMessage[]): Promise<readonly ModelMessage[]> }
export interface ContextBuilder<Input = AgentInput, Message extends ModelMessage = ModelMessage> { build(input: Input, messages: readonly Message[]): Promise<readonly Message[]> }
export interface AgentInput<Content = unknown> { readonly prompt: string; readonly content?: readonly Content[] }
export interface AgentOutput<Content = unknown, Artifact = unknown> { readonly text: string; readonly content?: readonly Content[]; readonly artifacts?: readonly Artifact[] }
export interface AgentLoop<Input extends AgentInput = AgentInput, Output extends AgentOutput = AgentOutput> { run(context: TaskContext, input: Input): Promise<Output> }
