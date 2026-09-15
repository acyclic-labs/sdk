import type { ModelAttempt, ModelEvent, ModelProvider, ModelRequest } from "@acyclic-labs/harness";

export type PiEvent<Metadata = unknown> =
  | { readonly type: "text_delta"; readonly text: string }
  | { readonly type: "thinking_delta"; readonly text: string }
  | { readonly type: "tool_call"; readonly id: string; readonly name: string; readonly arguments: unknown }
  | { readonly type: "complete"; readonly metadata: Metadata }
  | { readonly type: "error"; readonly error: unknown };
export interface PiBridge<Request, Event, Metadata = unknown> {
  readonly request: (request: ModelRequest) => Request;
  readonly run: (request: Request, options: { readonly signal?: AbortSignal }) => AsyncIterable<Event>;
  readonly event?: (event: Event) => PiEvent<Metadata>;
  readonly reconcile: (attempt: ModelAttempt) => Promise<readonly Event[] | undefined>;
}
export function piProvider<Request, Metadata = unknown>(bridge: PiBridge<Request, PiEvent<Metadata>, Metadata>): ModelProvider;
export function piProvider<Request, Event, Metadata = unknown>(bridge: PiBridge<Request, Event, Metadata> & { readonly event: (event: Event) => PiEvent<Metadata> }): ModelProvider;
export function piProvider(bridge: unknown): ModelProvider {
  const typed = bridge as PiBridge<unknown, unknown>;
  const project = typed.event ?? ((event: unknown) => event as PiEvent);
  return { async *generate(request) { request.signal?.throwIfAborted(); for await (const source of typed.run(typed.request(request), request.signal ? { signal: request.signal } : {})) { request.signal?.throwIfAborted(); yield projectPiEvent(project(source)); } }, async reconcile(attempt) { const events = await typed.reconcile(attempt); return events?.map(event => projectPiEvent(project(event))); } };
}
export function projectPiEvent<Metadata>(event: PiEvent<Metadata>): ModelEvent {
  switch (event.type) { case "text_delta": return { kind: "content", delta: event.text }; case "thinking_delta": return { kind: "reasoning", delta: event.text }; case "tool_call": return { kind: "tool_call", callId: event.id, name: event.name, arguments: event.arguments }; case "complete": return { kind: "completed", metadata: event.metadata }; case "error": throw new PiProviderError(event.error); }
}
export class PiProviderError extends Error { constructor(readonly cause: unknown) { super(cause instanceof Error ? cause.message : String(cause)); } }
