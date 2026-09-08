import type { ModelAttempt, ModelEvent, ModelProvider, ModelRequest } from "./runtime.js";

export interface OpenAiCompatibleOptions {
  readonly baseUrl: string;
  readonly apiKey?: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly fetcher?: typeof fetch;
}

/** Native dependency-free adapter for OpenAI-compatible chat-completions streams. */
export class OpenAiCompatibleProvider implements ModelProvider {
  readonly #baseUrl: string;
  readonly #headers: Readonly<Record<string, string>>;
  readonly #fetcher: typeof fetch;

  constructor(options: OpenAiCompatibleOptions) {
    this.#baseUrl = options.baseUrl.endsWith("/") ? options.baseUrl : `${options.baseUrl}/`;
    this.#headers = Object.freeze({
      ...options.headers,
      ...(options.apiKey === undefined ? {} : { authorization: `Bearer ${options.apiKey}` }),
    });
    this.#fetcher = options.fetcher ?? fetch;
  }

  async *generate(request: ModelRequest): AsyncIterable<ModelEvent> {
    const response = await this.#fetcher(new URL("chat/completions", this.#baseUrl), {
      method: "POST",
      headers: { "content-type": "application/json", ...this.#headers },
      body: JSON.stringify({
        ...(isRecord(request.model.options) ? request.model.options : {}),
        model: request.model.name,
        messages: request.messages,
        tools: request.tools.map(tool => ({
          type: "function",
          function: {
            name: tool.name,
            description: tool.description,
            parameters: tool.inputSchema,
          },
        })),
        stream: true,
        ...(request.maxOutputTokens === undefined ? {} : { max_completion_tokens: request.maxOutputTokens }),
      }),
    });
    if (!response.ok || response.body === null) {
      throw new Error(`OpenAI-compatible request failed: ${response.status}`);
    }
    const calls = new Map<number, { id: string; name: string; arguments: string }>();
    const completion: Record<string, unknown> = {};
    let terminal = false;
    for await (const data of sse(response.body)) {
      if (data === "[DONE]") {
        terminal = true;
        break;
      }
      const value: unknown = JSON.parse(data);
      if (!isRecord(value)) continue;
      if (value.id !== undefined) completion.id = value.id;
      if (value.model !== undefined) completion.model = value.model;
      if (value.usage !== undefined) completion.usage = value.usage;
      const choice = Array.isArray(value.choices) && isRecord(value.choices[0]) ? value.choices[0] : undefined;
      if (choice?.finish_reason !== undefined && choice.finish_reason !== null) terminal = true;
      const delta = choice !== undefined && isRecord(choice.delta) ? choice.delta : undefined;
      if (delta === undefined) continue;
      if (typeof delta.content === "string") yield { kind: "content", delta: delta.content };
      if (typeof delta.reasoning_content === "string") {
        yield { kind: "reasoning", delta: delta.reasoning_content };
      }
      if (Array.isArray(delta.tool_calls)) {
        for (const raw of delta.tool_calls) {
          if (!isRecord(raw) || typeof raw.index !== "number") continue;
          const existing = calls.get(raw.index) ?? { id: "", name: "", arguments: "" };
          if (typeof raw.id === "string") existing.id = raw.id;
          if (isRecord(raw.function)) {
            if (typeof raw.function.name === "string") existing.name += raw.function.name;
            if (typeof raw.function.arguments === "string") existing.arguments += raw.function.arguments;
          }
          calls.set(raw.index, existing);
        }
      }
    }
    if (!terminal) throw new Error("OpenAI-compatible stream ended without a terminal marker");
    for (const call of [...calls.entries()].sort(([left], [right]) => left - right).map(([, call]) => call)) {
      yield {
        kind: "tool_call",
        callId: call.id,
        name: call.name,
        arguments: JSON.parse(call.arguments || "{}") as unknown,
      };
    }
    yield { kind: "completed", metadata: completion };
  }

  async reconcile(_: ModelAttempt): Promise<undefined> {
    return undefined;
  }
}

async function* sse(body: ReadableStream<Uint8Array>): AsyncIterable<string> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffered = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      buffered += value === undefined ? "" : decoder.decode(value, { stream: !done });
      const blocks = buffered.split(/\r?\n\r?\n/);
      buffered = blocks.pop() ?? "";
      for (const block of blocks) {
        const data = block
          .split(/\r?\n/)
          .filter(line => line.startsWith("data:"))
          .map(line => line.slice(5).trimStart())
          .join("\n");
        if (data.length > 0) yield data;
      }
      if (done) break;
    }
  } finally {
    reader.releaseLock();
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
