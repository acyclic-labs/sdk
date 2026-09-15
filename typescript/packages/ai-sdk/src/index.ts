import type { ModelAttempt, ModelEvent, ModelProvider, ModelRequest } from "@acyclic-labs/harness";

export interface AiSdkUsage { readonly inputTokens?: number; readonly outputTokens?: number; readonly totalTokens?: number }
export type AiSdkStreamPart =
  | { readonly type: "text-delta"; readonly text: string }
  | { readonly type: "reasoning-delta"; readonly text: string }
  | { readonly type: "tool-call"; readonly toolCallId: string; readonly toolName: string; readonly input: unknown }
  | { readonly type: "finish"; readonly finishReason: string; readonly usage?: AiSdkUsage; readonly providerMetadata?: unknown }
  | { readonly type: "error"; readonly error: unknown };
export interface AiSdkStreamResult<Part = AiSdkStreamPart> { readonly fullStream: AsyncIterable<Part> }
export interface AiSdkGenerateRequest<Model> { readonly request: ModelRequest; readonly model: Model; readonly abortSignal?: AbortSignal }
export type AiSdkStreamText<Model, Part = AiSdkStreamPart> = (request: AiSdkGenerateRequest<Model>) => AiSdkStreamResult<Part> | Promise<AiSdkStreamResult<Part>>;
export interface AiSdkBridge<Model, Part = AiSdkStreamPart> {
  readonly model: (request: ModelRequest) => Model;
  readonly streamText: AiSdkStreamText<Model, Part>;
  readonly project: (part: Part) => ModelEvent | undefined;
  readonly reconcile: (attempt: ModelAttempt) => Promise<readonly ModelEvent[] | undefined>;
}

/** Converts an AI SDK full stream into the runtime's complete typed event model. */
export function aiSdkProvider<Model>(bridge: Omit<AiSdkBridge<Model, AiSdkStreamPart>, "project"> & { readonly project?: AiSdkBridge<Model>["project"] }): ModelProvider;
export function aiSdkProvider<Model, Part>(bridge: AiSdkBridge<Model, Part>): ModelProvider;
export function aiSdkProvider<Model>(bridge: unknown): ModelProvider {
  const normalized = bridge as Omit<AiSdkBridge<Model, unknown>, "project"> & {
    readonly project?: (part: unknown) => ModelEvent | undefined;
  };
  const project = normalized.project ?? (defaultProject as (part: unknown) => ModelEvent | undefined);
  return {
    async *generate(request) {
      request.signal?.throwIfAborted();
      const result = await normalized.streamText({ request, model: normalized.model(request), ...(request.signal ? { abortSignal: request.signal } : {}) });
      for await (const part of result.fullStream) {
        request.signal?.throwIfAborted();
        const event = project(part);
        if (event !== undefined) yield event;
      }
    },
    reconcile: normalized.reconcile,
  };
}

export function defaultProject(part: AiSdkStreamPart): ModelEvent | undefined {
  switch (part.type) {
    case "text-delta": return { kind: "content", delta: part.text };
    case "reasoning-delta": return { kind: "reasoning", delta: part.text };
    case "tool-call": return { kind: "tool_call", callId: part.toolCallId, name: part.toolName, arguments: part.input };
    case "finish": return { kind: "completed", metadata: { finishReason: part.finishReason, usage: part.usage, providerMetadata: part.providerMetadata } };
    case "error": throw new AiSdkProviderError(part.error);
  }
}

export class AiSdkProviderError extends Error { constructor(readonly cause: unknown) { super(cause instanceof Error ? cause.message : String(cause)); } }
