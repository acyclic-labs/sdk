import { fromBinary, toBinary } from "@bufbuild/protobuf";
import { Code, ConnectError, type Transport } from "@connectrpc/connect";
import { MachinesService } from "../generated/proto/machines/v1/machines_pb.js";

type RpcMethod = Parameters<Transport["unary"]>[0] | Parameters<Transport["stream"]>[0];

/** Binary projection for the same-module Rust provider. Authentication, duplex
 * flow control and HTTP ownership remain with the original public Transport.
 * No lifecycle, command validation or retry mechanism is implemented here.
 */
export function createMachinesRpcTransport(transport: Transport) {
  return (name: keyof typeof MachinesService.method, pull: () => Promise<Uint8Array | undefined>, timeout?: string) => {
    const method: RpcMethod = MachinesService.method[name];
    if (!method) throw new ConnectError("unknown public Machines method", Code.Unimplemented);
    const controller = new AbortController();
    let closed = false;
    const header = new Headers();
    if (timeout !== undefined) header.set("grpc-timeout", timeout);
    const input = {
      async *[Symbol.asyncIterator]() {
        while (!closed) {
          const bytes = await pull();
          if (closed || bytes === undefined) return;
          yield fromBinary(method.input, bytes);
        }
      },
    };
    let output: AsyncIterator<Uint8Array>;
    // Zero disables a transport default timer. Rust retains the ONE original
    // deadline event; the original gRPC deadline metadata is forwarded intact.
    const ready = (async () => {
      if (method.methodKind === "unary") {
        const first = await input[Symbol.asyncIterator]().next();
        if (first.done) throw new ConnectError("unary request omitted its message", Code.InvalidArgument);
        const response = await transport.unary(method, controller.signal, 0, header, first.value);
        output = (async function* () { yield toBinary(method.output, response.message); })();
      } else {
        const response = await transport.stream(method, controller.signal, 0, header, input);
        const messages = response.message[Symbol.asyncIterator]();
        output = {
          async next() {
            const result = await messages.next();
            return result.done ? { done: true, value: undefined } : { done: false, value: toBinary(method.output, result.value) };
          },
        };
      }
    })();
    return {
      ready,
      async next(): Promise<Uint8Array | undefined> {
        await ready;
        if (closed) return undefined;
        const result = await output.next();
        return result.done ? undefined : result.value;
      },
      cancel() { closed = true; controller.abort(); },
    };
  };
}
