import { create } from "@bufbuild/protobuf";
import { expect, mock, test } from "bun:test";
import {
  fromEnv,
  HttpInferenceTransport,
  InferenceClient,
  RustInferenceTransport,
  INFERENCE_REMOTE_POLICY,
} from "../src/index.js";
import { NativeInferenceTransport, nativeCompanionTarget } from "../src/native.js";
import { InspectRunRequestSchema } from "../generated/proto/inference/v1/inference_pb.js";

type MockNativeClient = {
  inspectRun(request: Uint8Array, cancellation?: MockNativeInferenceCancellation): Promise<Uint8Array>;
};

class MockNativeInferenceCancellation {
  cancelled = false;
  constructor() { nativeHandles.push(this); }
  cancel(): void { this.cancelled = true; }
}

const nativeHandles: MockNativeInferenceCancellation[] = [];
const nativeMock: {
  connectCalls: number;
  connectPromise: Promise<MockNativeClient>;
  connectCancellation: MockNativeInferenceCancellation | undefined;
  inspectResponse: Promise<Uint8Array>;
} = {
  connectCalls: 0,
  connectPromise: Promise.resolve(undefined as unknown as MockNativeClient),
  connectCancellation: undefined,
  inspectResponse: Promise.resolve(new Uint8Array()),
};
const nativeClient: MockNativeClient = {
  inspectRun: (_request, _cancellation) => nativeMock.inspectResponse,
};
nativeMock.connectPromise = Promise.resolve(nativeClient);
const nativeBridge = {
  connect: async (_endpoint: string, _token: string, cancellation?: MockNativeInferenceCancellation) => {
    nativeMock.connectCalls += 1;
    nativeMock.connectCancellation = cancellation;
    return nativeMock.connectPromise;
  },
};
mock.module(`@acyclic-labs/inference-${nativeCompanionTarget()}`, () => ({
  NativeInferenceCancellation: MockNativeInferenceCancellation,
  NativeInferenceClient: nativeBridge,
}));

function resetNativeMock(): void {
  nativeHandles.length = 0;
  nativeMock.connectCalls = 0;
  nativeMock.connectPromise = Promise.resolve(nativeClient);
  nativeMock.connectCancellation = undefined;
  nativeMock.inspectResponse = Promise.resolve(new Uint8Array());
}

function deferred<T>(): { readonly promise: Promise<T>; readonly resolve: (value: T) => void; readonly reject: (reason: unknown) => void } {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => { resolve = resolvePromise; reject = rejectPromise; });
  return { promise, resolve, reject };
}

async function waitForConnectionHandle(): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (nativeMock.connectCancellation !== undefined) return;
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  throw new Error("native connection did not start");
}

async function waitForOperationHandle(): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (nativeHandles.length >= 2) return;
    await new Promise(resolve => setTimeout(resolve, 0));
  }
  throw new Error("native operation did not start");
}

test("Rust-owned policy selects the best transport without consumer flags", () => {
  expect(INFERENCE_REMOTE_POLICY.transport.native.map(option => option.kind)).toEqual(["grpc", "http"]);
  expect(INFERENCE_REMOTE_POLICY.transport.browser.map(option => option.kind)).toEqual(["http"]);
  const client = fromEnv({ endpoint: "https://inference.example", token: "fixture" });
  expect(client).toBeInstanceOf(InferenceClient);
  expect(client.transport).not.toBeInstanceOf(RustInferenceTransport);
  expect(client.transport).not.toBeInstanceOf(HttpInferenceTransport);
});

test("unavailable transport settings are rejected by the generated policy", () => {
  expect(() => fromEnv({ endpoint: "https://inference.example", token: "fixture", transport: "grpc" }))
    .not.toThrow();
  expect(() => fromEnv({ endpoint: "https://inference.example", token: "fixture", fetcher: globalThis.fetch }))
    .toThrow("do not support a custom fetcher");
  const compatibility = new HttpInferenceTransport("https://inference.example", "fixture");
  expect(compatibility).toBeInstanceOf(RustInferenceTransport);
});

test("native companion target follows the host libc", () => {
  expect(nativeCompanionTarget("linux", "x64", { getReport: () => ({ header: { glibcVersionRuntime: "2.39" } }) }))
    .toBe("linux-x64-gnu");
  expect(nativeCompanionTarget("linux", "x64", { getReport: () => ({ header: { platform: "linux", arch: "x64" } }) }))
    .toBe("linux-x64-musl");
  expect(nativeCompanionTarget("linux", "x64", { getReport: () => ({ header: {} }) }))
    .toBe("linux-x64-gnu");
  expect(nativeCompanionTarget("linux", "x64", {})).toBe("linux-x64-gnu");
  expect(nativeCompanionTarget("darwin", "arm64")).toBe("darwin-arm64");
});

test("native calls cancel pending Rust promises without unhandled rejection", async () => {
  resetNativeMock();
  const transport = new NativeInferenceTransport({ endpoint: "https://inference.example", token: "fixture" });
  await transport.inspectRun(create(InspectRunRequestSchema));
  const late = deferred<Uint8Array>();
  nativeMock.inspectResponse = late.promise;
  const controller = new AbortController();
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => unhandled.push(reason);
  process.on("unhandledRejection", onUnhandled);
  try {
    const pending = transport.inspectRun(create(InspectRunRequestSchema), controller.signal);
    await waitForOperationHandle();
    controller.abort();
    await expect(pending)
      .rejects.toMatchObject({ name: "AbortError" });
    expect(nativeHandles.at(-1)?.cancelled).toBeTrue();
    late.reject(new Error("native cancellation"));
    await new Promise(resolve => setTimeout(resolve, 0));
    expect(unhandled).toHaveLength(0);
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
});

test("one aborted native waiter does not cancel another waiter", async () => {
  resetNativeMock();
  const connection = deferred<MockNativeClient>();
  nativeMock.connectPromise = connection.promise;
  const transport = new NativeInferenceTransport({ endpoint: "https://inference.example", token: "fixture" });
  const firstController = new AbortController();
  const first = transport.inspectRun(create(InspectRunRequestSchema), firstController.signal);
  const second = transport.inspectRun(create(InspectRunRequestSchema));
  await waitForConnectionHandle();
  firstController.abort();
  await expect(first).rejects.toMatchObject({ name: "AbortError" });
  expect(nativeMock.connectCancellation?.cancelled).toBeFalse();
  connection.resolve(nativeClient);
  await expect(second).resolves.toBeDefined();
  expect(nativeMock.connectCalls).toBe(1);
});

test("last aborted native waiter cancels the connection and clears stale state", async () => {
  resetNativeMock();
  const connection = deferred<MockNativeClient>();
  nativeMock.connectPromise = connection.promise;
  const transport = new NativeInferenceTransport({ endpoint: "https://inference.example", token: "fixture" });
  const firstController = new AbortController();
  const secondController = new AbortController();
  const first = transport.inspectRun(create(InspectRunRequestSchema), firstController.signal);
  const second = transport.inspectRun(create(InspectRunRequestSchema), secondController.signal);
  await waitForConnectionHandle();
  const firstOutcome = first.then(() => { throw new Error("first waiter unexpectedly resolved"); }, error => { expect(error).toMatchObject({ name: "AbortError" }); });
  const secondOutcome = second.then(() => { throw new Error("second waiter unexpectedly resolved"); }, error => { expect(error).toMatchObject({ name: "AbortError" }); });
  firstController.abort();
  secondController.abort();
  await firstOutcome;
  await secondOutcome;
  await new Promise(resolve => setTimeout(resolve, 0));
  expect(nativeMock.connectCancellation?.cancelled).toBeTrue();
  connection.reject(new Error("native connection cancelled"));
  nativeMock.connectPromise = Promise.resolve(nativeClient);
  await transport.inspectRun(create(InspectRunRequestSchema));
  expect(nativeMock.connectCalls).toBe(2);
});

test("failed native connection promises reset for a later caller", async () => {
  resetNativeMock();
  const transport = new NativeInferenceTransport({ endpoint: "https://inference.example", token: "fixture" });
  nativeMock.connectPromise = Promise.reject(new Error("first connection failed"));
  await expect(transport.inspectRun(create(InspectRunRequestSchema))).rejects.toThrow("first connection failed");
  nativeMock.connectPromise = Promise.resolve(nativeClient);
  await transport.inspectRun(create(InspectRunRequestSchema));
  expect(nativeMock.connectCalls).toBe(2);
});
