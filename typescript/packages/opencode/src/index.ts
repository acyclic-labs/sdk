import type { ModelAttempt, ModelEvent, ModelProvider, ModelRequest } from "@acyclic-labs/harness";

export type OpenCodePart<Metadata = unknown> =
  | { readonly type: "text"; readonly delta: string }
  | { readonly type: "reasoning"; readonly delta: string }
  | { readonly type: "tool"; readonly callId: string; readonly tool: string; readonly input: unknown }
  | { readonly type: "done"; readonly metadata: Metadata }
  | { readonly type: "error"; readonly error: unknown };
export interface OpenCodeBridge<Request, Part, Metadata = unknown> {
  readonly request: (request: ModelRequest) => Request;
  readonly stream: (request: Request, options: { readonly signal?: AbortSignal }) => AsyncIterable<Part>;
  readonly part?: (part: Part) => OpenCodePart<Metadata>;
  readonly reconcile: (attempt: ModelAttempt) => Promise<readonly Part[] | undefined>;
}
export function openCodeProvider<Request, Metadata = unknown>(bridge: OpenCodeBridge<Request, OpenCodePart<Metadata>, Metadata>): ModelProvider;
export function openCodeProvider<Request, Part, Metadata = unknown>(bridge: OpenCodeBridge<Request, Part, Metadata> & { readonly part: (part: Part) => OpenCodePart<Metadata> }): ModelProvider;
export function openCodeProvider(bridge: unknown): ModelProvider {
  const typed = bridge as OpenCodeBridge<unknown, unknown>;
  const project = typed.part ?? ((part: unknown) => part as OpenCodePart);
  return { async *generate(request) { request.signal?.throwIfAborted(); for await (const source of typed.stream(typed.request(request), request.signal ? { signal: request.signal } : {})) { request.signal?.throwIfAborted(); yield projectOpenCodePart(project(source)); } }, async reconcile(attempt) { const parts = await typed.reconcile(attempt); return parts?.map(part => projectOpenCodePart(project(part))); } };
}
export function projectOpenCodePart<Metadata>(part: OpenCodePart<Metadata>): ModelEvent {
  switch (part.type) { case "text": return { kind: "content", delta: part.delta }; case "reasoning": return { kind: "reasoning", delta: part.delta }; case "tool": return { kind: "tool_call", callId: part.callId, name: part.tool, arguments: part.input }; case "done": return { kind: "completed", metadata: part.metadata }; case "error": throw new OpenCodeProviderError(part.error); }
}
export class OpenCodeProviderError extends Error { constructor(readonly cause: unknown) { super(cause instanceof Error ? cause.message : String(cause)); } }
