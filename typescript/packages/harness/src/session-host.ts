import { is_stream_error_code, grpcErrorStatus } from "../generated/wasm/acyclic_harness_wasm.js";
import type { HostedSession } from "./event-host.js";

/** HTTP/2 response parts. The deployment runtime must publish trailers after
 * the body; WHATWG Response headers cannot substitute for terminal trailers. */
export interface GrpcSessionResponse {
  readonly status: 200;
  readonly headers: Headers;
  readonly body: ReadableStream<Uint8Array>;
  readonly trailers: Promise<Headers>;
  close(): void;
}

interface Server {
  grpcSession(next: (signal: AbortSignal) => Promise<Uint8Array | null>, signal?: AbortSignal): Promise<HostedSession>;
}

function failure(code: string): Error & { readonly code: string } {
  return Object.assign(new Error(code), { code });
}

function cause(error: unknown): Error & { readonly code: string } {
  const code = error instanceof Error && "code" in error ? error.code : undefined;
  return failure(typeof code === "string" && is_stream_error_code(code) ? code : "unavailable");
}

/** Ordinary Actor/Worker entry for StreamSessionService.Session. The owning
 * platform adapts these response parts to its public HTTP/2 response primitive.
 * Rust owns all protobuf, session authorization, credit and ActorCall dispatch. */
export async function serveGrpcSession(host: Server, request: Request): Promise<GrpcSessionResponse> {
  if (new URL(request.url).pathname !== "/acyclic.stream.v1.StreamSessionService/Session"
    || request.method !== "POST" || !request.body) throw failure("invalid_argument");
  const contentType = request.headers.get("content-type")?.split(";", 1)[0]?.trim();
  if (contentType !== "application/grpc" && contentType !== "application/grpc+proto") throw failure("unsupported");
  const encoding = request.headers.get("grpc-encoding");
  if (encoding && encoding !== "identity") throw failure("unsupported");

  const reader = request.body.getReader();
  let cursor: HostedSession | undefined;
  let terminal: Error | undefined;
  let ended = false;
  let resolveTrailers!: (headers: Headers) => void;
  const trailers = new Promise<Headers>(resolve => { resolveTrailers = resolve; });
  const finish = (error?: Error) => {
    if (ended) return;
    ended = true;
    terminal = error;
    request.signal.removeEventListener("abort", abort);
    cursor?.close();
    void reader.cancel(error).catch(() => undefined);
    const code = error ? cause(error).code : undefined;
    const status = String(code === undefined ? 0 : grpcErrorStatus(code));
    resolveTrailers(new Headers({ "grpc-status": status, ...(code ? { "grpc-message": code } : {}) }));
  };
  const abort = () => finish(failure("unavailable"));
  request.signal.addEventListener("abort", abort, { once: true });
  try {
    if (request.signal.aborted) abort();
    if (terminal) throw terminal;
    cursor = await host.grpcSession(async signal => {
      const cancel = () => { void reader.cancel(failure("unavailable")).catch(() => undefined); };
      signal.addEventListener("abort", cancel, { once: true });
      try {
        if (signal.aborted) cancel();
        const part = await reader.read();
        if (signal.aborted || terminal) throw terminal ?? failure("unavailable");
        return part.done ? null : part.value;
      } finally { signal.removeEventListener("abort", cancel); }
    }, request.signal);
    if (ended) { cursor.close(); throw terminal ?? failure("unavailable"); }
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          if (terminal) throw terminal;
          const frame = await cursor!.next();
          if (terminal) throw terminal;
          if (frame === null) { finish(); controller.close(); return; }
          controller.enqueue(frame);
        } catch (error) {
          const terminal = cause(error);
          finish(terminal);
          controller.error(terminal);
        }
      },
      cancel() { abort(); },
    }, { highWaterMark: 0 });
    return { status: 200, headers: new Headers({ "content-type": "application/grpc+proto", "trailer": "grpc-status, grpc-message" }),
      body, trailers, close: abort };
  } catch (error) { finish(cause(error)); throw error; }
}
