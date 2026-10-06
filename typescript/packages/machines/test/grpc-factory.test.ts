import { expect, test } from "bun:test";
import { createMachinesGrpcClient, MACHINES_GRPC_METHODS, type RustOwnedGrpcInvoker } from "../src/generated-client.ts";

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
