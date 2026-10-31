import { expect, test } from "bun:test";
import { SecretsClient } from "../src/secrets.js";

for (const phase of ["headers", "body"] as const) {
  test(`WASM secrets enforce the shared whole-request deadline during ${phase}`, async () => {
    const planted = "PLANTED_DEADLINE_REASON";
    const originalFetch = globalThis.fetch;
    const originalTimeout = AbortSignal.timeout;
    let releaseStarted!: () => void;
    const started = new Promise<void>(resolve => { releaseStarted = resolve; });
    let controller: AbortController | undefined;
    let calls = 0;
    let rejectTransport: (() => void) | undefined;
    let result: Promise<string> | undefined;
    AbortSignal.timeout = (milliseconds: number) => {
      expect(milliseconds).toBe(30_000);
      controller = new AbortController();
      return controller.signal;
    };
    globalThis.fetch = (async (request: Request) => {
      calls++;
      const signal = request.signal;
      expect(signal.aborted).toBe(false);
      if (phase === "headers") {
        return new Promise<Response>((_resolve, reject) => {
          rejectTransport = () => reject(new Error(planted));
          signal.addEventListener("abort", rejectTransport, { once: true });
          releaseStarted();
        });
      }
      return new Response(new ReadableStream<Uint8Array>({
        start(body) {
          body.enqueue(new TextEncoder().encode('{"generation":0,'));
          rejectTransport = () => body.error(new Error(planted));
          signal.addEventListener("abort", rejectTransport, { once: true });
        },
        pull() { releaseStarted(); },
      }), { status: 200 });
    }) as typeof fetch;
    const client = new SecretsClient("http://127.0.0.1:1234", () => "fixture");
    try {
      const pending = client.list({ scope: "fixture" });
      // Attach rejection handling before advancing the controlled deadline.
      result = pending.then(() => "unexpected success", error => String(error));
      await started;
      if (!controller) throw new Error("missing canonical request deadline");
      controller.abort(planted);
      const failure = await result;
      expect(failure).toContain("secrets: transport failed");
      expect(failure).not.toContain(planted);
      expect(calls).toBe(1);
    } finally {
      rejectTransport?.();
      controller?.abort();
      await result;
      client.free();
      globalThis.fetch = originalFetch;
      AbortSignal.timeout = originalTimeout;
    }
  });
}
