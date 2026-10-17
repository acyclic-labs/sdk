import { plugin } from "bun";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
const fault = process.env.STREAM_NATIVE_FAULT === "missing-signal";
const scenario = process.env.STREAM_NATIVE_SCENARIO ?? "cancel-renewal";
plugin({ name: "native-children-cancellation-model", setup(build) {
  build.onResolve({ filter: /^@acyclic-labs\/actors\/credential$/ }, () => ({
    path: fileURLToPath(new URL("../../actors/src/credential.ts", import.meta.url)),
  }));
  build.onLoad({ filter: /[\\/]src[\\/]native\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    const load = "const module = await loadNativeModule();";
    assert(source.includes(load));
    source = source.replaceAll(load, "const module = (globalThis as any).nativeChildrenBinding as typeof GeneratedNative;");
    return { contents: source, loader: "ts" };
  });
  build.onLoad({ filter: /[\\/]src[\\/]native-credentials\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    if (fault) {
      assert(source.includes("credential(controller.signal)"));
      source = source.replace("credential(controller.signal)", "credential(undefined)");
    }
    return { contents: source, loader: "ts" };
  });
} });
const controller = new AbortController();
let observed: AbortSignal | undefined;
let stopped = 0;
let sent = 0;
let start!: () => void;
const started = new Promise<void>(resolve => { start = resolve; });
let finish!: (value: string) => void;
const credential = (signal?: AbortSignal): Promise<string> => {
  observed = signal;
  start();
  return new Promise((resolve, reject) => {
    const abort = () => { stopped++; reject(signal?.reason); };
    finish = value => { signal?.removeEventListener("abort", abort); resolve(value); };
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
  });
};
class Cancellation {
  release!: () => void;
  cancelled = new Promise<void>(resolve => { this.release = resolve; });
  cancel(): void { this.release(); }
}
class Client {
  constructor(readonly resolve: () => Promise<string>) {}
  abandonSessionAttempts(): void {}
  static async connectWithCredentialResult(_endpoint: string, resolve: () => Promise<string>) {
    return { client: new Client(resolve) };
  }
  withCredentialProvider(resolve: () => Promise<string>): Client { return new Client(resolve); }
  async openChildrenResult(_request: Uint8Array, cancellation: Cancellation) {
    return Promise.race([
      this.resolve().then(() => {
        if (controller.signal.aborted) return { error: { code: "cancelled", message: "cancelled" } };
        sent++;
        return { follow: { async nextResult() {
          return scenario === "terminal-error" ? { error: { code: "unavailable", message: "reconnect failed" } } : {};
        }, async close() {} } };
      }, () => ({ error: { code: "cancelled", message: "cancelled" } })),
      cancellation.cancelled.then(() => ({ error: { code: "cancelled", message: "cancelled" } })),
    ]);
  }
}
Object.assign(globalThis, { nativeChildrenBinding: { NativeStreamClient: Client, NativeStreamCancellation: Cancellation } });
const { NativeStreamProvider } = await import("../src/native.js");
const provider = await NativeStreamProvider.connect("https://fixture.invalid", undefined, undefined, undefined, credential);
if (scenario === "cancel-before-open") controller.abort();
const cursor = provider.children({ parent: "agents", limit: 1, signal: controller.signal })[Symbol.asyncIterator]();
const pending = cursor.next();
if (scenario !== "cancel-before-open") {
  await started;
  assert.notEqual(observed, undefined, "credential renewal must receive discovery's cancellation signal");
  if (scenario === "terminal-error") {
    finish("fixture");
    await assert.rejects(pending, (error: any) => error.code === "unavailable");
  } else if (scenario === "finish-before-cancel") {
    finish("fixture");
    await pending;
    controller.abort();
    assert.equal(stopped, 0);
  } else {
    controller.abort();
    await pending;
    assert.equal(observed?.aborted, true);
    assert.equal(stopped, 1, "cancelled discovery must stop credential work");
    assert.equal(sent, 0);
  }
} else {
  assert.equal((await pending).done, true);
  assert.equal(sent, 0);
  assert.equal(observed, undefined);
}
await cursor.return?.();
console.log("native children cancellation model", scenario, "passed");
