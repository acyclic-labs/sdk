import { expect, test } from "bun:test";
import { createMachinesGrpcClient, invokeWithAbort, MACHINES_GRPC_METHODS, type RustOwnedGrpcInvoker } from "../src/generated-client.ts";

test("Rust-generated Machines gRPC factory exposes every native RPC", async () => {
  const calls: string[] = [];
  const invoker: RustOwnedGrpcInvoker = {
    async invokeGrpc(method) {
      calls.push(method.rpcName);
      return {};
    },
    invokeGrpcStream(method) {
      calls.push(method.rpcName);
      return (async function* () { yield {}; })();
    },
  };
  const client = createMachinesGrpcClient(invoker);
  expect(Object.keys(MACHINES_GRPC_METHODS)).toHaveLength(19);
  await client.inspectMachine({} as never);
  for await (const _value of client.watchOperation({} as never)) break;
  expect(calls).toEqual(["InspectMachine", "WatchOperation"]);
});

test("Rust-generated Machines gRPC factory forwards AbortSignal to the invoker", async () => {
  let observed: AbortSignal | undefined;
  const reason = new Error("caller aborted");
  const invoker: RustOwnedGrpcInvoker = {
    async invokeGrpc(_method, _request, signal) {
      observed = signal;
      if (signal?.aborted) throw signal.reason;
      return {};
    },
    invokeGrpcStream() {
      return (async function* () { yield {}; })();
    },
  };
  const client = createMachinesGrpcClient(invoker);
  const controller = new AbortController();
  controller.abort(reason);
  await expect(client.inspectMachine({} as never, controller.signal)).rejects.toBe(reason);
  expect(observed).toBe(controller.signal);
});

test("Rust-generated await helper rejects with the caller abort reason", async () => {
  const reason = new Error("caller aborted");
  const controller = new AbortController();
  const pending = invokeWithAbort(() => new Promise<void>(() => undefined), controller.signal);
  controller.abort(reason);
  await expect(pending).rejects.toBe(reason);
});
