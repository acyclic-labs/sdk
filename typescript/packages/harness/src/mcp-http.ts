/** Browser platform I/O for the Rust MCP transport; no protocol semantics here. */
import type { McpBrowserHttpProvider, McpBrowserResponseHead }
  from "../generated/wasm/acyclic_harness_wasm.js";

/** Creates an inert provider. An embedding host supplies its selected fetch
 * capability; credentials are omitted unless that host explicitly wraps it.
 * The owning Harness journal must admit each operation before invoking it. */
export function createBrowserMcpHttpProvider(fetcher: typeof fetch = globalThis.fetch,
  signal?: AbortSignal): McpBrowserHttpProvider {
  return (_operation, request) => {
    const controller = new AbortController();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    let pending = false;
    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const finish = () => { clearTimeout(timer); signal?.removeEventListener("abort", cancel); };
    const cancel = () => {
      cancelled = true;
      finish();
      controller.abort();
      if (!pending) reader?.releaseLock();
    };
    timer = setTimeout(cancel, request.timeout_ms);
    signal?.addEventListener("abort", cancel, { once: true });
    if (signal?.aborted) cancel();
    const response = Promise.resolve().then(() => {
      controller.signal.throwIfAborted();
      return fetcher(request.endpoint, {
        method: request.method,
        headers: new Headers(Array.from(request.headers)),
        ...(request.body.length === 0 ? {} : { body: new Uint8Array(request.body) }),
        signal: controller.signal,
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
          if (cancelled) reader?.releaseLock();
        }
      },
      cancel,
    };
  };
}
