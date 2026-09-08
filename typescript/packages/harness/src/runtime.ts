export interface Model {
  readonly provider: string;
  readonly name: string;
  readonly revision: string;
  readonly options: unknown;
}

export interface ModelMessage {
  readonly role: "system" | "user" | "assistant" | "tool" | (string & {});
  readonly content: unknown;
}

export interface ToolDefinition {
  readonly name: string;
  readonly revision: string;
  readonly description: string;
  readonly inputSchema: unknown;
  readonly outputSchema: unknown;
}

export interface ModelRequest {
  readonly model: Model;
  readonly messages: readonly ModelMessage[];
  readonly tools: readonly ToolDefinition[];
  readonly maxOutputTokens?: number;
}

export type ModelEvent =
  | { readonly kind: "content"; readonly delta: string }
  | { readonly kind: "reasoning"; readonly delta: string }
  | { readonly kind: "tool_call"; readonly callId: string; readonly name: string; readonly arguments: unknown }
  | { readonly kind: "completed"; readonly metadata: unknown };

export interface ModelAttempt {
  readonly operationId: string;
  readonly step: number;
  readonly requestDigest: Uint8Array;
  readonly observed: readonly ModelEvent[];
}

export interface ModelProvider {
  generate(request: ModelRequest): AsyncIterable<ModelEvent>;
  reconcile(attempt: ModelAttempt): Promise<readonly ModelEvent[] | undefined>;
}

export interface ToolInvocation {
  readonly callId: string;
  readonly name: string;
  readonly arguments: unknown;
}

export interface ToolResult { readonly value: unknown }
export interface ToolExecutor {
  execute(invocation: ToolInvocation): Promise<ToolResult>;
  reconcile(invocation: ToolInvocation): Promise<ToolResult | undefined>;
}
export interface ToolProjection { project(invocation: ToolInvocation, result: ToolResult): unknown }

/** One tool assembled from independently replaceable public values. */
export interface Tool {
  readonly definition: ToolDefinition;
  readonly executor: ToolExecutor;
  readonly projection: ToolProjection;
}

export interface ContextStage {
  readonly name: string;
  apply(input: unknown, messages: readonly ModelMessage[]): Promise<readonly ModelMessage[]>;
}
