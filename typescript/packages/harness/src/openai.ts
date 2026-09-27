import type { ModelAttempt, ModelEvent, ModelProvider, ModelRequest } from "./runtime.js";
import type { ModelContentPart } from "./model.js";
import type { FileRef } from "./conversation.js";
import { projectModelFile } from "./projection.js";
import { NativeContracts } from "./native-contracts.js";
import type { HttpFetcher } from "./wire-transport.js";

export type OpenAiFilePolicy = "reference" | "bounded_full" | "native";
export type OpenAiContentPart = ModelContentPart;
type OpenAiProjectedPart =
  | Readonly<{ type: "text"; text: string }>
  | Readonly<{ type: "image_url"; image_url: Readonly<{ url: string }> }>;

export interface OpenAiCompatibleOptions {
  readonly baseUrl: string;
  readonly apiKey?: string;
  readonly headers?: Readonly<Record<string, string>>;
  readonly fetcher?: HttpFetcher;
  /** Must enforce the caller's owner-mediated read grant before returning bytes. */
  readonly resolveFile?: (file: FileRef) => Promise<Uint8Array>;
  /** Optional owner check in addition to Rust's mandatory descriptor verification. */
  readonly verifyFile?: (file: FileRef, bytes: Uint8Array) => void | Promise<void>;
  readonly maxResolvedBytes?: number;
  readonly maxEventBytes?: number;
}

/** Native dependency-free adapter for OpenAI-compatible chat-completions streams. */
export class OpenAiCompatibleProvider implements ModelProvider {
  readonly #endpoint: string;
  readonly #headers: Readonly<Record<string, string>>;
  readonly #fetcher: HttpFetcher;
  readonly #resolveFile: ((file: FileRef) => Promise<Uint8Array>) | undefined;
  readonly #verifyFile: OpenAiCompatibleOptions["verifyFile"];
  readonly #maxResolvedBytes: number;
  readonly #maxEventBytes: number;

  constructor(options: OpenAiCompatibleOptions) {
    const base = new URL(options.baseUrl);
    const loopback = base.hostname === "localhost" || base.hostname === "127.0.0.1" || base.hostname === "[::1]";
    if ((base.protocol !== "https:" && !(base.protocol === "http:" && loopback))
      || !base.hostname || base.username || base.password
      || base.href.includes("?") || base.href.includes("#")) {
      throw new TypeError("OpenAI-compatible base URL must be HTTPS (or local HTTP) without credentials, query, or fragment");
    }
    if (!base.pathname.endsWith("/")) base.pathname += "/";
    this.#endpoint = new URL("chat/completions", base).href;
    this.#headers = Object.freeze({
      ...options.headers,
      ...(options.apiKey === undefined ? {} : { authorization: `Bearer ${options.apiKey}` }),
    });
    this.#fetcher = options.fetcher ?? fetch;
    this.#resolveFile = options.resolveFile;
    this.#verifyFile = options.verifyFile;
    this.#maxResolvedBytes = options.maxResolvedBytes ?? 1_048_576;
    this.#maxEventBytes = options.maxEventBytes ?? 1_048_576;
    if (!Number.isSafeInteger(this.#maxResolvedBytes) || this.#maxResolvedBytes < 0
      || !Number.isSafeInteger(this.#maxEventBytes) || this.#maxEventBytes <= 0) {
      throw new TypeError("OpenAI-compatible projection and event limits must be safe integers");
    }
  }

  async *generate(request: ModelRequest): AsyncIterable<ModelEvent> {
    const contracts = await NativeContracts.create();
    const encoder = new TextEncoder();
    const decoder = new TextDecoder();
    const messages = await Promise.all(request.messages.map(async message => {
      if (!["system", "user", "assistant", "tool"].includes(message.role)) {
        throw new TypeError("unsupported model message role");
      }
      if (isRecord(message.content) && message.content.kind === "tool_call") {
        if (message.role !== "assistant" || typeof message.content.callId !== "string"
          || typeof message.content.name !== "string" || !message.content.callId || !message.content.name) {
          throw new TypeError("invalid tool call message");
        }
        return { role: "assistant", content: null, tool_calls: [{ id: message.content.callId, type: "function", function: { name: message.content.name, arguments: decoder.decode(contracts.encodeCanonicalJson(message.content.arguments)) } }] };
      }
      if (isRecord(message.content) && message.content.kind === "tool_result") {
        if (message.role !== "tool" || typeof message.content.callId !== "string" || !message.content.callId) {
          throw new TypeError("invalid tool result message");
        }
        return { role: "tool", tool_call_id: message.content.callId, content: decoder.decode(contracts.encodeCanonicalJson(message.content.value)) };
      }
      return { role: message.role, content: await this.#projectContent(message.content) };
    }));
    const response = await this.#fetcher(this.#endpoint, {
      method: "POST",
      ...(request.signal === undefined ? {} : { signal: request.signal }),
      headers: { "content-type": "application/json", ...this.#headers },
      body: decoder.decode(contracts.encodeCanonicalJson({
        ...(isRecord(request.model.options) ? request.model.options : {}),
        model: request.model.name,
        messages,
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
      })),
    });
    if (!response.ok || response.body === null) {
      throw new Error(`OpenAI-compatible request failed: ${response.status}`);
    }
    const calls = new Map<number, { id: string; name: string; arguments: string }>();
    const completion: Record<string, unknown> = {};
    let terminal = false;
    for await (const data of sse(response.body, this.#maxEventBytes)) {
      if (data === "[DONE]") {
        terminal = true;
        break;
      }
      const value: unknown = contracts.decodeModelJson(encoder.encode(data));
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
          if (!isRecord(raw) || typeof raw.index !== "number" || !Number.isSafeInteger(raw.index) || raw.index < 0
            || (raw.type !== undefined && raw.type !== "function")) {
            throw new TypeError("invalid OpenAI-compatible tool call delta");
          }
          const existing = calls.get(raw.index) ?? { id: "", name: "", arguments: "" };
          if (raw.id !== undefined) {
            if (typeof raw.id !== "string" || !raw.id.trim() || (existing.id && existing.id !== raw.id)) {
              throw new TypeError("invalid OpenAI-compatible tool call identity");
            }
            existing.id = raw.id;
          }
          if (raw.function !== undefined) {
            if (!isRecord(raw.function) || (raw.function.name !== undefined && typeof raw.function.name !== "string")
              || (raw.function.arguments !== undefined && typeof raw.function.arguments !== "string")) {
              throw new TypeError("invalid OpenAI-compatible tool call function");
            }
            if (typeof raw.function.name === "string") existing.name += raw.function.name;
            if (typeof raw.function.arguments === "string") existing.arguments += raw.function.arguments;
          }
          calls.set(raw.index, existing);
        }
      }
    }
    if (!terminal) throw new Error("OpenAI-compatible stream ended without a terminal marker");
    const seenIds = new Set<string>();
    const validatedCalls: Extract<ModelEvent, { kind: "tool_call" }>[] = [];
    for (const call of [...calls.entries()].sort(([left], [right]) => left - right).map(([, call]) => call)) {
      if (!call.id.trim() || !call.name.trim() || !call.arguments.trim() || seenIds.has(call.id)) {
        throw new TypeError("incomplete OpenAI-compatible tool call");
      }
      seenIds.add(call.id);
      let argumentsValue: unknown;
      try {
        argumentsValue = contracts.decodeModelJson(encoder.encode(call.arguments));
      } catch {
        throw new TypeError("invalid OpenAI-compatible tool call arguments");
      }
      if (!isRecord(argumentsValue) || Array.isArray(argumentsValue)) {
        throw new TypeError("OpenAI-compatible tool call arguments must be an object");
      }
      validatedCalls.push({
        kind: "tool_call",
        callId: call.id,
        name: call.name,
        arguments: argumentsValue,
      });
    }
    for (const call of validatedCalls) yield call;
    yield { kind: "completed", metadata: completion };
  }

  async reconcile(_: ModelAttempt): Promise<undefined> {
    return undefined;
  }

  async #projectContent(content: unknown): Promise<string | readonly OpenAiProjectedPart[]> {
    if (typeof content === "string") return content;
    const parts = Array.isArray(content) ? content : [content];
    const projected: OpenAiProjectedPart[] = [];
    for (const part of parts) {
      if (!isRecord(part)) throw new TypeError("unsupported model content part");
      if (part.kind === "text" && typeof part.text === "string") {
        projected.push({ type: "text", text: part.text });
        continue;
      }
      if (part.kind !== "file" || !isRecord(part.file)) {
        throw new TypeError("unsupported model content part");
      }
      const file = await projectModelFile(part as Extract<ModelContentPart, { kind: "file" }>, {
        ...(this.#resolveFile === undefined ? {} : { resolveFile: this.#resolveFile }),
        ...(this.#verifyFile === undefined ? {} : { verifyFile: this.#verifyFile }),
        maxResolvedBytes: this.#maxResolvedBytes,
      });
      if (file.kind === "image") {
        projected.push({ type: "image_url", image_url: { url: `data:${file.mediaType};base64,${base64(file.bytes)}` } });
      } else {
        projected.push({ type: "text", text: file.text });
      }
    }
    return projected;
  }
}

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < bytes.length; offset += 32_768) {
    binary += String.fromCharCode(...bytes.subarray(offset, offset + 32_768));
  }
  return btoa(binary);
}

async function* sse(body: ReadableStream<Uint8Array>, maxEventBytes: number): AsyncIterable<string> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffered = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      buffered += value === undefined ? "" : decoder.decode(value, { stream: !done });
      const blocks = buffered.split(/\r?\n\r?\n/);
      buffered = blocks.pop() ?? "";
      if (new TextEncoder().encode(buffered).byteLength > maxEventBytes) {
        throw new RangeError("OpenAI-compatible event exceeds its configured byte limit");
      }
      for (const block of blocks) {
        if (new TextEncoder().encode(block).byteLength > maxEventBytes) {
          throw new RangeError("OpenAI-compatible event exceeds its configured byte limit");
        }
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
