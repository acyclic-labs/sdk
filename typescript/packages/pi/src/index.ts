import { projectModelFile, type FileProjectionOptions, type ModelAttempt, type ModelContentPart,
  type ModelEvent, type ModelProvider, type ModelRequest } from "@acyclic-labs/harness";

export type PiEvent =
  | { readonly type: "text_delta"; readonly text: string }
  | { readonly type: "thinking_delta"; readonly text: string }
  | { readonly type: "tool_call"; readonly id: string; readonly name: string; readonly arguments: unknown }
  | { readonly type: "complete"; readonly metadata: unknown }
  | { readonly type: "error"; readonly error: unknown };

/** Pi owns model-wire projection; Harness only supplies typed, provenance-bearing model input. */
export interface PiBridge<Request, Event> {
  /** Exact selected-model capacity; required when accounting is requested. */
  readonly contextCapacity?: ModelProvider["contextCapacity"];
  /** Request-bound additive token bounds, without projection or dispatch. */
  readonly countTokens?: ModelProvider["countTokens"];
  readonly project: (request: ModelRequest) => Request | Promise<Request>;
  readonly run: (request: Request, options: { readonly signal?: AbortSignal }) => AsyncIterable<Event>;
  readonly event?: (event: Event) => PiEvent;
  readonly reconcile: (attempt: ModelAttempt) => Promise<readonly Event[] | undefined>;
}

export function piProvider<Request>(bridge: PiBridge<Request, PiEvent>): ModelProvider;
export function piProvider<Request, Event>(bridge: PiBridge<Request, Event> & { readonly event: (event: Event) => PiEvent }): ModelProvider;
export function piProvider<Request, Event>(bridge: PiBridge<Request, Event>): ModelProvider {
  const projectEvent = bridge.event ?? ((event: unknown) => event);
  return {
    contextCapacity(model) {
      if (bridge.contextCapacity === undefined) {
        throw new TypeError("Pi provider requires explicit model capacity accounting");
      }
      return bridge.contextCapacity(model);
    },
    countTokens(request) {
      if (bridge.countTokens === undefined) {
        throw new TypeError("Pi provider requires explicit token accounting");
      }
      return bridge.countTokens(request);
    },
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
  | Readonly<{ type: "tool_call"; callId: string; name: string; arguments: unknown }>
  | Readonly<{ type: "tool_result"; callId: string; name: string; value: unknown }>;
type PiText = Extract<PiProjectedPart, { type: "text" }>;
type PiToolCall = Extract<PiProjectedPart, { type: "tool_call" }>;
type PiToolResult = Extract<PiProjectedPart, { type: "tool_result" }>;
export type PiProjectedMessage =
  | Readonly<{ role: "system" | "user"; content: readonly PiText[] }>
  | Readonly<{ role: "assistant"; content: readonly (PiText | PiToolCall)[] }>
  | Readonly<{ role: "tool"; content: readonly (PiText | PiToolResult)[] }>;
export interface PiProjectedRequest {
  readonly model: ModelRequest["model"];
  readonly messages: readonly PiProjectedMessage[];
  readonly tools: ModelRequest["tools"];
  readonly maxOutputTokens?: number;
}

/** Safe default projection; custom Pi bridges can still supply their own. */
export async function projectPiRequest(request: ModelRequest, options: FileProjectionOptions = {}): Promise<PiProjectedRequest> {
  for (const message of request.messages) {
    const parts = Array.isArray(message.content) ? message.content : [message.content];
    for (const part of parts) {
      if (typeof part !== "object" || part === null) continue;
      const data = part.kind === "tool_result" && part.content?.kind === "parts" ? part.content.parts : [part];
      for (const item of data) {
        if (item.kind === "file" && item.policy !== "reference" && item.policy !== "bounded_full") {
          throw new TypeError("unsupported native media policy for Pi adapter");
        }
      }
    }
  }
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
        case "file":
          content.push({ type: "text", text: await projectModelFile(part as Extract<ModelContentPart, { kind: "file" }>, options) });
          break;
        case "tool_call":
          if (message.role !== "assistant" || typeof part.callId !== "string" || !part.callId
            || typeof part.name !== "string" || !part.name) throw new TypeError("invalid Pi tool call");
          content.push({ type: "tool_call", callId: part.callId, name: part.name, arguments: part.arguments });
          break;
        case "tool_result":
          if (message.role !== "tool" || typeof part.callId !== "string" || !part.callId
            || typeof part.name !== "string" || !part.name) throw new TypeError("invalid Pi tool result");
          if (part.content?.kind === "json") {
            content.push({ type: "tool_result", callId: part.callId, name: part.name, value: part.content.value });
          } else if (part.content?.kind === "parts") {
            const data: PiText[] = [];
            for (const item of part.content.parts) {
              if (item.kind === "text") data.push({ type: "text", text: item.text });
              else if (item.kind === "file") {
                data.push({ type: "text", text: await projectModelFile(item, options) });
              } else throw new TypeError("unsupported Pi tool result data");
            }
            content.push({ type: "tool_result", callId: part.callId, name: part.name, value: data });
          } else throw new TypeError("invalid Pi tool result projection");
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
  const isText = (part: PiProjectedPart): part is PiText => part.type === "text";
  const isAssistantPart = (part: PiProjectedPart): part is PiText | PiToolCall => part.type !== "tool_result";
  const isToolPart = (part: PiProjectedPart): part is PiText | PiToolResult => part.type !== "tool_call";
  switch (role) {
    case "system":
    case "user":
      if (!content.every(isText)) throw new TypeError("invalid Pi message content");
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
      throw new Error(event.error instanceof Error ? event.error.message : String(event.error), { cause: event.error });
    default: throw new TypeError("unsupported Pi bridge event");
  }
}
