/** Browser platform I/O for the Rust MCP transport; no protocol semantics here. */
import type { McpBrowserHttpProvider, McpBrowserResponseHead }
  from "../generated/wasm/acyclic_harness_wasm.js";
import { deadlineSignal } from "@acyclic-labs/actors/deadline";

/** Supplies host-selected HTTP I/O for journal-admitted MCP operations. */
export function createBrowserMcpHttpProvider(fetcher: typeof fetch = globalThis.fetch,
  signal?: AbortSignal): McpBrowserHttpProvider {
  return (_operation, request) => {
    const deadline = deadlineSignal(request.timeout_ms ?? undefined,
      () => new DOMException("MCP exchange deadline elapsed", "TimeoutError"), signal);
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let pending = false;
    const finish = () => {
      deadline.signal.removeEventListener("abort", cancel);
      deadline.cleanup();
    };
    const cancel = () => {
      finish();
      if (!pending) reader?.releaseLock();
    };
    deadline.signal.addEventListener("abort", cancel, { once: true });
    if (deadline.signal.aborted) cancel();
    const response = Promise.resolve().then(() => {
      deadline.signal.throwIfAborted();
      return fetcher(request.endpoint, {
        method: request.method,
        headers: new Headers(Array.from(request.headers)),
        ...(request.body.length === 0 ? {} : { body: new Uint8Array(request.body) }),
        signal: deadline.signal,
        credentials: "omit",
        redirect: "manual",
      });
    }).then(value => {
      reader = value.body?.getReader();
      return { status: value.status, headers: Object.fromEntries(value.headers) } satisfies McpBrowserResponseHead;
    });
    return {
      response,
      async read() {
        pending = true;
        try {
          await response;
          if (!reader) { finish(); return null; }
          const result = await reader.read();
          if (result.done) { finish(); reader.releaseLock(); return null; }
          return result.value;
        } finally {
          pending = false;
          if (deadline.signal.aborted) reader?.releaseLock();
        }
      },
      cancel,
    };
  };
}
