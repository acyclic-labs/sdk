import { createServer, type Socket } from "node:net";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { expect, test } from "bun:test";

type NativeBinding = {
  readonly NativeInferenceCancellation: new () => { readonly cancelled: boolean; cancel(): void };
  readonly NativeInferenceClient: {
    connect(endpoint: string, token: string, cancellation: object): Promise<unknown>;
  };
};

const packageDirectory = process.env.ACYCLIC_INFERENCE_NATIVE_PACKAGE_DIR?.trim() || undefined;
const nativeTest = packageDirectory === undefined ? test.skip : test;

async function loadBinding(): Promise<NativeBinding> {
  if (packageDirectory === undefined) throw new Error("native package fixture is not configured");
  const namespace = await import(pathToFileURL(join(packageDirectory, "index.js")).href) as NativeBinding & { readonly default?: NativeBinding };
  return namespace.NativeInferenceClient === undefined ? namespace.default as NativeBinding : namespace;
}

async function withTimeout<T>(operation: Promise<T>, label: string): Promise<T> {
  return Promise.race([
    operation,
    new Promise<T>((_, reject) => setTimeout(() => reject(new Error(label)), 2_000)),
  ]);
}

nativeTest("installed native companion cancels a pending Rust connection future", async () => {
  const binding = await loadBinding();
  expect(typeof binding.NativeInferenceCancellation).toBe("function");
  expect(typeof binding.NativeInferenceClient?.connect).toBe("function");

  const sockets = new Set<Socket>();
  let resolveAccepted!: () => void;
  let rejectAccepted!: (error: unknown) => void;
  const accepted = new Promise<void>((resolve, reject) => {
    resolveAccepted = resolve;
    rejectAccepted = reject;
  });
  const server = createServer((socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
    resolveAccepted();
  });
  server.once("error", rejectAccepted);

  try {
    const port = await new Promise<number>((resolve, reject) => {
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        if (address === null || typeof address === "string") {
          reject(new Error("native cancellation fixture did not expose a TCP port"));
        } else {
          resolve(address.port);
        }
      });
      server.once("error", reject);
    });
    const cancellation = new binding.NativeInferenceCancellation();
    const pending = binding.NativeInferenceClient.connect(
      `http://127.0.0.1:${port}`,
      "fixture-token",
      cancellation,
    );
    const outcome = pending.then(
      () => ({ kind: "resolved" as const }),
      (error: unknown) => ({ kind: "rejected" as const, error }),
    );
    await withTimeout(accepted, "native companion did not start the pending handshake");
    cancellation.cancel();
    expect(cancellation.cancelled).toBeTrue();
    const result = await withTimeout(outcome, "native connection ignored cancellation");
    expect(result.kind).toBe("rejected");
    if (result.kind === "rejected") expect(String(result.error)).toMatch(/cancel/i);
  } finally {
    for (const socket of sockets) socket.destroy();
    if (server.listening) {
      await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    }
  }
});
