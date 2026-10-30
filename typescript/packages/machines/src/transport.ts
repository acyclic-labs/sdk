import { ownedStream } from "./owned-stream.js";
import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { Code, ConnectError, createClient, type CallOptions, type Transport } from "@connectrpc/connect";
import * as wire from "../generated/proto/machines/v1/machines_pb.js";
export { createMachinesRpcTransport } from "./rpc-transport.js";

export const MAX_OUTPUT_READ_BYTES = 65_536;
const MAX_OFFSET = (1n << 64n) - 1n;

export type MachinesExecRequest = MessageInitShape<typeof wire.ExecStartSchema> & {
  readonly argv: readonly string[];
  readonly stdin?: Uint8Array;
};

/** Public Machines client over an authenticated transport with duplex streaming.
 * The host owns connection/authentication; this core owns framing, validation,
 * original deadlines and cancellation. No Node APIs or transport retry owner.
 */
export function createMachinesClient(transport: Transport) {
  const machines = createClient(wire.MachinesService, transport);
  return {
    machines: {
      ...machines,
      async readFile(request: MessageInitShape<typeof wire.ReadFileRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.ReadFileRequestSchema, request);
        const access = checkFileAccess(query.access, query.limit);
        if (query.limit < 1) throw new RangeError("file read chunk must be positive");
        const result = await fileCompletion(signal => machines.readFile(oneChunk(query), { ...fileOptions(access, callOptions), signal }), callOptions?.signal);
        checkFileResult(access, result);
        if (result.result.case === "data") {
          if (result.result.value.byteLength > query.limit) throw new ConnectError("file read exceeds requested chunk", Code.DataLoss);
        } else if (result.result.case !== "errno" || result.result.value === 0) throw new ConnectError("file read terminal result is invalid", Code.DataLoss);
        return result;
      },
      async writeFile(request: MessageInitShape<typeof wire.WriteFileRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.WriteFileRequestSchema, request);
        const access = checkFileAccess(query.access, query.data.byteLength);
        if (query.truncate && access.offset !== 0n) throw new RangeError("file truncation requires offset zero");
        const result = await fileCompletion(signal => machines.writeFile(oneChunk(query), { ...fileOptions(access, callOptions), signal }), callOptions?.signal);
        checkFileResult(access, result);
        if (result.result.case === "written") {
          if (result.result.value > query.data.byteLength) throw new ConnectError("file write exceeds supplied chunk", Code.DataLoss);
        } else if (result.result.case !== "errno" || result.result.value === 0) throw new ConnectError("file write terminal result is invalid", Code.DataLoss);
        return result;
      },
      exec(request: MachinesExecRequest, callOptions?: CallOptions) {
        const { argv, stdin = new Uint8Array(), ...admission } = request;
        const query = create(wire.ExecStartSchema, admission);
        if (!argv.length || !argv[0] || argv.some(value => value.includes("\0"))) throw new RangeError("invalid exec argv");
        if (typeof request.timeoutMs !== "bigint" || request.timeoutMs < 1n || request.timeoutMs > 0xffffffffffffffffn) throw new RangeError("timeoutMs must be a positive uint64 bigint");
        if (query.cwd !== undefined && (!query.cwd || query.cwd.includes("\0"))) throw new RangeError("cwd must be nonempty and NUL-free");
        return ownedStream(async function* (signal) {
          let remaining = query.timeoutMs + 1000n;
          if (query.expiryUnixMs !== undefined) {
            const expiry = query.expiryUnixMs - BigInt(Date.now());
            if (expiry < remaining) remaining = expiry;
          }
          for await (const frame of machines.exec(execInput(query, argv, stdin), { ...deadlineOptions(remaining, callOptions), signal })) {
            checkIdentity(query.machine, frame.machine, "machine");
            if (frame.output.case === "stopped") {
              if (![wire.ExecStopKind.TIMEOUT, wire.ExecStopKind.EXPIRED, wire.ExecStopKind.OUTPUT_LIMIT, wire.ExecStopKind.CONTROL_STOPPED, wire.ExecStopKind.CAPTURE_FAILED].includes(frame.output.value.kind)) throw new ConnectError("invalid exec stop kind", Code.DataLoss);
            } else if (frame.output.case !== "exitCode" && (frame.output.case !== "data" || ![wire.ExecStream.STDOUT, wire.ExecStream.STDERR].includes(frame.output.value.stream) || frame.output.value.bytes.byteLength < 1 || frame.output.value.bytes.byteLength > MAX_OUTPUT_READ_BYTES)) throw new ConnectError("invalid exec output frame", Code.DataLoss);
            yield frame;
          }
          throw new ConnectError("exec stream ended without exit code", Code.DataLoss);
        }, callOptions?.signal, frame => frame.output.case === "exitCode" || frame.output.case === "stopped");
      },
      watchOperation(request: MessageInitShape<typeof wire.OperationRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.OperationRequestSchema, request);
        return ownedStream(async function* (signal) {
          for await (const frame of machines.watchOperation(query, { ...callOptions, signal })) {
            checkIdentity(query.operation, frame.operation, "operation");
            if (![wire.OperationStatus.PENDING, wire.OperationStatus.SUCCEEDED, wire.OperationStatus.FAILED, wire.OperationStatus.CANCELLED, wire.OperationStatus.INDETERMINATE].includes(frame.status)) throw new ConnectError("invalid operation status", Code.DataLoss);
            yield frame;
          }
          throw new ConnectError("operation watch ended without terminal status", Code.DataLoss);
        }, callOptions?.signal, frame => frame.status !== wire.OperationStatus.PENDING);
      },
      async readOutput(request: MessageInitShape<typeof wire.ReadOutputRequestSchema>, callOptions?: CallOptions) {
        const query = create(wire.ReadOutputRequestSchema, request);
        if (query.offset < 0n || query.offset > MAX_OFFSET) throw new RangeError("offset must be uint64");
        if (!Number.isInteger(query.limit) || query.limit < 1 || query.limit > MAX_OUTPUT_READ_BYTES) throw new RangeError("limit must be 1..65536");
        const result = await machines.readOutput(query, callOptions);
        checkIdentity(query.machine, result.machine, "machine");
        if (result.offset < query.offset || result.data.byteLength > query.limit || result.offset + BigInt(result.data.byteLength) > MAX_OFFSET) throw new ConnectError("invalid console output range", Code.DataLoss);
        return result;
      },
    },
  };
}

async function* oneChunk<T>(value: T): AsyncIterable<T> { yield value; }

// A native chunk has one completion. Finish on that receipt rather than waiting
// for EOF; cancellation also interrupts a pending read and closes the request.
async function fileCompletion<T>(open: (signal: AbortSignal) => AsyncIterable<T>, parent?: AbortSignal): Promise<T> {
  const output = ownedStream(open, parent, () => true);
  try {
    const result = await output.next();
    if (result.done) throw new ConnectError("file stream ended without native completion", Code.DataLoss);
    return result.value;
  } finally { await output.return(); }
}

// Transport pulls only the current frame. Argument boundaries and process stdin
// are separate; commandComplete alone authorizes launch on the original session.
async function* execInput(start: wire.ExecStart, argv: readonly string[], stdin: Uint8Array): AsyncIterable<wire.ExecInput> {
  const frame = (init: MessageInitShape<typeof wire.ExecInputSchema>) => create(wire.ExecInputSchema, init);
  yield frame({ input: { case: "start", value: start } });
  const encoder = new TextEncoder();
  for (const argument of argv) {
    let offset = 0;
    do {
      let end = Math.min(argument.length, offset + 65_536);
      if (end < argument.length && argument.charCodeAt(end - 1) >= 0xd800 && argument.charCodeAt(end - 1) <= 0xdbff) end--;
      const bytes = new Uint8Array(Math.min(65_536, (end - offset) * 3));
      const { read, written } = encoder.encodeInto(argument.slice(offset, end), bytes);
      offset += read;
      yield frame({ input: { case: "argument", value: { data: bytes.subarray(0, written), end: offset === argument.length } } });
    } while (offset < argument.length);
  }
  yield frame({ input: { case: "commandComplete", value: {} } });
  for (let offset = 0; offset < stdin.byteLength; offset += 65_536) yield frame({ input: { case: "stdin", value: stdin.subarray(offset, offset + 65_536) } });
  yield frame({ input: { case: "stdinEof", value: {} } });
}

function checkIdentity(request: { value: Uint8Array } | undefined, response: { value: Uint8Array } | undefined, kind: string): void {
  if (!request || !response || request.value.length !== response.value.length || request.value.some((byte, index) => byte !== response.value[index])) throw new ConnectError(`response ${kind} identity differs from request`, Code.DataLoss);
}


function checkFileAccess(access: wire.FileAccess | undefined, length: number): wire.FileAccess {
  if (!access || !access.machine || !/^(?:\/|\\\\|[A-Za-z]:[\/\\])/.test(access.path) || access.path.includes("\0") || access.offset < 0n || access.offset > MAX_OFFSET || access.deadlineUnixMs < 1n || access.deadlineUnixMs > MAX_OFFSET) throw new RangeError("invalid absolute guest file access");
  if (!Number.isInteger(length) || length < 0 || length > MAX_OUTPUT_READ_BYTES || access.offset + BigInt(length) > MAX_OFFSET) throw new RangeError("invalid guest file chunk range");
  return access;
}

function fileOptions(access: wire.FileAccess, options?: CallOptions): CallOptions {
  return deadlineOptions(access.deadlineUnixMs - BigInt(Date.now()), options);
}

function deadlineOptions(remaining: bigint, options?: CallOptions): CallOptions {
  if (remaining <= 0n || remaining > BigInt(Number.MAX_SAFE_INTEGER)) throw new RangeError("guest deadline expired or not representable by this transport");
  const caller = options?.timeoutMs;
  return { ...options, timeoutMs: Math.min(Number(remaining), caller === undefined || caller === 0 ? Infinity : caller) };
}

function checkFileResult(access: wire.FileAccess, result: { machine?: wire.MachineId | undefined; offset: bigint }): void {
  checkIdentity(access.machine, result.machine, "machine");
  if (access.offset !== result.offset) throw new ConnectError("file result offset substituted", Code.DataLoss);
}
