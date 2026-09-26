import { projectModelFile, type FileProjectionOptions, type ModelAttempt, type ModelContentPart,
  type ModelEvent, type ModelProvider, type ModelRequest } from "@acyclic-labs/harness";

export type PiEvent<Metadata = unknown> =
  | { readonly type: "text_delta"; readonly text: string }
  | { readonly type: "thinking_delta"; readonly text: string }
  | { readonly type: "tool_call"; readonly id: string; readonly name: string; readonly arguments: unknown }
  | { readonly type: "complete"; readonly metadata: Metadata }
  | { readonly type: "error"; readonly error: unknown };

/** Pi owns model-wire projection; Harness only supplies typed, provenance-bearing model input. */
export interface PiBridge<Request, Event, Metadata = unknown> {
  readonly project: (request: ModelRequest) => Request | Promise<Request>;
  readonly run: (request: Request, options: { readonly signal?: AbortSignal }) => AsyncIterable<Event>;
  readonly event?: (event: Event) => PiEvent<Metadata>;
  readonly reconcile: (attempt: ModelAttempt) => Promise<readonly Event[] | undefined>;
}

export function piProvider<Request, Metadata = unknown>(bridge: PiBridge<Request, PiEvent<Metadata>, Metadata>): ModelProvider;
export function piProvider<Request, Event, Metadata = unknown>(bridge: PiBridge<Request, Event, Metadata> & { readonly event: (event: Event) => PiEvent<Metadata> }): ModelProvider;
export function piProvider<Request, Event, Metadata>(bridge: PiBridge<Request, Event, Metadata>): ModelProvider {
  const projectEvent = bridge.event ?? ((event: unknown) => event);
  return {
    async *generate(request) {
      request.signal?.throwIfAborted();
      const projected = await bridge.project(request);
      for await (const source of bridge.run(projected, request.signal ? { signal: request.signal } : {})) {
        request.signal?.throwIfAborted();
        yield projectPiEvent(projectEvent(source));
      }
    },
    async reconcile(attempt) {
      const events = await bridge.reconcile(attempt);
      return events?.map(event => projectPiEvent(projectEvent(event)));
    },
  };
}

export type PiProjectedPart =
  | Readonly<{ type: "text"; text: string }>
  | Readonly<{ type: "image"; mediaType: string; bytes: Uint8Array }>
  | Readonly<{ type: "tool_call"; callId: string; name: string; arguments: unknown }>
  | Readonly<{ type: "tool_result"; callId: string; name: string; value: unknown }>;
type PiTextOrImage = Extract<PiProjectedPart, { type: "text" | "image" }>;
type PiToolCall = Extract<PiProjectedPart, { type: "tool_call" }>;
type PiToolResult = Extract<PiProjectedPart, { type: "tool_result" }>;
export type PiProjectedMessage =
  | Readonly<{ role: "system" | "user"; content: readonly PiTextOrImage[] }>
  | Readonly<{ role: "assistant"; content: readonly (PiTextOrImage | PiToolCall)[] }>
  | Readonly<{ role: "tool"; content: readonly (PiTextOrImage | PiToolResult)[] }>;
export interface PiProjectedRequest {
  readonly model: ModelRequest["model"];
  readonly messages: readonly PiProjectedMessage[];
  readonly tools: ModelRequest["tools"];
  readonly maxOutputTokens?: number;
}

/** Safe default projection; custom Pi bridges can still supply their own. */
export async function projectPiRequest(request: ModelRequest, options: FileProjectionOptions = {}): Promise<PiProjectedRequest> {
  const messages: PiProjectedMessage[] = [];
  for (const message of request.messages) {
    const parts = typeof message.content === "string" ? [{ kind: "text", text: message.content }]
      : Array.isArray(message.content) ? message.content : [message.content];
    const content: PiProjectedPart[] = [];
    for (const part of parts) {
      if (typeof part !== "object" || part === null || typeof part.kind !== "string") {
        throw new TypeError("unsupported Pi content part");
      }
      switch (part.kind) {
        case "text":
          if (typeof part.text !== "string") throw new TypeError("invalid Pi text part");
          content.push({ type: "text", text: part.text });
          break;
        case "file": {
          const projected = await projectModelFile(part as Extract<ModelContentPart, { kind: "file" }>, options);
          content.push(projected.kind === "image"
            ? { type: "image", mediaType: projected.mediaType, bytes: projected.bytes }
            : { type: "text", text: projected.text });
          break;
        }
        case "tool_call":
          if (message.role !== "assistant" || typeof part.callId !== "string" || !part.callId
            || typeof part.name !== "string" || !part.name) throw new TypeError("invalid Pi tool call");
          content.push({ type: "tool_call", callId: part.callId, name: part.name, arguments: part.arguments });
          break;
        case "tool_result":
          if (message.role !== "tool" || typeof part.callId !== "string" || !part.callId
            || typeof part.name !== "string" || !part.name) throw new TypeError("invalid Pi tool result");
          content.push({ type: "tool_result", callId: part.callId, name: part.name, value: part.value });
          break;
        default: throw new TypeError("unsupported Pi content part");
      }
    }
    messages.push(projectedMessage(message.role, content));
  }
  return { model: request.model, messages, tools: request.tools,
    ...(request.maxOutputTokens === undefined ? {} : { maxOutputTokens: request.maxOutputTokens }) };
}

function projectedMessage(role: ModelRequest["messages"][number]["role"], content: PiProjectedPart[]): PiProjectedMessage {
  const isTextOrImage = (part: PiProjectedPart): part is PiTextOrImage => part.type === "text" || part.type === "image";
  const isAssistantPart = (part: PiProjectedPart): part is PiTextOrImage | PiToolCall => part.type !== "tool_result";
  const isToolPart = (part: PiProjectedPart): part is PiTextOrImage | PiToolResult => part.type !== "tool_call";
  switch (role) {
    case "system":
    case "user":
      if (!content.every(isTextOrImage)) throw new TypeError("invalid Pi message content");
      return { role, content };
    case "assistant":
      if (!content.every(isAssistantPart)) throw new TypeError("invalid Pi assistant content");
      return { role, content };
    case "tool":
      if (!content.every(isToolPart)) throw new TypeError("invalid Pi tool content");
      return { role, content };
    default: throw new TypeError("unsupported Pi message role");
  }
}

export type PiDefaultBridge<Metadata = unknown> = Omit<PiBridge<PiProjectedRequest, PiEvent<Metadata>, Metadata>, "project"> & FileProjectionOptions;

/** Pi adapter with the shared bounded text/image/reference defaults. */
export function piDefaultProvider<Metadata = unknown>(bridge: PiDefaultBridge<Metadata>): ModelProvider {
  return piProvider({
    project: request => projectPiRequest(request, bridge),
    run: bridge.run,
    reconcile: bridge.reconcile,
    ...(bridge.event === undefined ? {} : { event: bridge.event }),
  });
}

export function projectPiEvent(event: unknown): ModelEvent {
  if (typeof event !== "object" || event === null || !("type" in event) || typeof event.type !== "string") {
    throw new TypeError("Pi bridge returned an invalid event");
  }
  switch (event.type) {
    case "text_delta":
      if (!("text" in event) || typeof event.text !== "string") throw new TypeError("Pi text delta is invalid");
      return { kind: "content", delta: event.text };
    case "thinking_delta":
      if (!("text" in event) || typeof event.text !== "string") throw new TypeError("Pi thinking delta is invalid");
      return { kind: "reasoning", delta: event.text };
    case "tool_call":
      if (!("id" in event) || typeof event.id !== "string" || !event.id ||
          !("name" in event) || typeof event.name !== "string" || !event.name || !("arguments" in event)) {
        throw new TypeError("Pi tool call identity is invalid");
      }
      return { kind: "tool_call", callId: event.id, name: event.name, arguments: event.arguments };
    case "complete":
      if (!("metadata" in event)) throw new TypeError("Pi completion metadata is missing");
      return { kind: "completed", metadata: event.metadata };
    case "error":
      if (!("error" in event)) throw new TypeError("Pi error detail is missing");
      throw new PiProviderError(event.error);
    default: throw new TypeError("unsupported Pi bridge event");
  }
}

export class PiProviderError extends Error {
  constructor(readonly cause: unknown) { super(cause instanceof Error ? cause.message : String(cause)); }
}
