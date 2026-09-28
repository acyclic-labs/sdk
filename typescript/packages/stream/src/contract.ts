import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import initStreamWasm, { encodeHttpRequest as encodeHttpRequestWire, initSync as initStreamWasmSync, normalizeCommitRequest, validateAppendRequest, validatePath, validateRequest, validateSequence } from "../generated/wasm/acyclic_stream_wasm.js";
import {
  AbsentConditionSchema, AppendMutationSchema, AppendRequestSchema, CommitConditionSchema,
  CommitMutationSchema, CommitRequestSchema,
  ForkMutationSchema, ForkRequestSchema, TailConditionSchema, TailRequestSchema,
  ReadRequestSchema, FollowRequestSchema,
  ChildrenRequestSchema, ChildrenPageRequestSchema, InspectIdempotencyRequestSchema, ReadCommitRequestSchema,
  CreateTokenRequestSchema,
} from "../generated/proto/stream/v2/stream_pb.js";
import type { AppendOptions, CommitOptions, CreateTokenRequest, ForkOptions, IdempotencyKey, ProviderCommitRequest } from "./types.js";
import { StreamError, commitId, idempotencyKey } from "./types.js";
import type { HttpRoute } from "./http-contract.js";

let wasmReady: Promise<void> | undefined;
export async function ensureStreamWasm(): Promise<void> {
  wasmReady ??= (async () => {
    const wasmUrl = new URL("../generated/wasm/acyclic_stream_wasm_bg.wasm", import.meta.url);
    if (wasmUrl.protocol === "file:") {
      const { readFile } = await import("node:fs/promises");
      const instance = await initStreamWasm({ module_or_path: Uint8Array.from(await readFile(wasmUrl)) });
      verifyExports(instance);
    } else {
      const instance = await initStreamWasm({ module_or_path: wasmUrl });
      verifyExports(instance);
    }
  })();
  try { await wasmReady; }
  catch (error) { wasmReady = undefined; throw error; }
}

// The public Stream handle constructors are intentionally synchronous. Load the
// packaged module synchronously when the runtime provides a built-in filesystem
// API (Node and Bun), which also makes this safe under test workers that do not
// await module-level promises before running synchronous test callbacks.
if (!initializeStreamWasmSync()) await ensureStreamWasm();

function initializeStreamWasmSync(): boolean {
  const runtime = globalThis as typeof globalThis & {
    process?: { getBuiltinModule?: (name: string) => unknown };
  };
  const getBuiltinModule = runtime.process?.getBuiltinModule;
  if (getBuiltinModule === undefined) return false;
  const filesystem = getBuiltinModule("node:fs") as { readFileSync?: (path: URL) => Uint8Array } | undefined;
  if (filesystem?.readFileSync === undefined) return false;
  const wasmUrl = new URL("../generated/wasm/acyclic_stream_wasm_bg.wasm", import.meta.url);
  const instance = initStreamWasmSync({ module: filesystem.readFileSync(wasmUrl) });
  verifyExports(instance);
  return true;
}

function verifyExports(instance: { readonly decodeHttpResponse: unknown; readonly encodeHttpRequest: unknown; readonly is_stream_error_code: unknown; readonly normalizeCommitRequest: unknown; readonly projectMemoryResponse: unknown; readonly validateAppendRequest: unknown; readonly validatePath: unknown; readonly validateRequest: unknown; readonly validateSequence: unknown; readonly __wbindgen_free: unknown }): void {
  if (typeof instance.decodeHttpResponse !== "function" || typeof instance.encodeHttpRequest !== "function" || typeof instance.is_stream_error_code !== "function" || typeof instance.validateAppendRequest !== "function" || typeof instance.normalizeCommitRequest !== "function" || typeof instance.projectMemoryResponse !== "function" || typeof instance.validatePath !== "function" || typeof instance.validateRequest !== "function" || typeof instance.validateSequence !== "function" || typeof instance.__wbindgen_free !== "function") {
    throw new StreamError("configuration", "stream WASM exports do not match the packaged contract");
  }
}

/** Validates one path through the canonical Rust Stream parser. */
export function validatePathValue(value: unknown): void {
  if (typeof value !== "string") throw new StreamError("invalid_path", "path must be text");
  const error = validatePath(value);
  if (error) validationError(error, "path");
}

/** Validates one sequence through Rust's canonical unsigned 64-bit boundary. */
export function validateSequenceValue(value: unknown): bigint {
  if (typeof value !== "bigint") throw new RangeError("sequence must be an unsigned 64-bit integer");
  if (validateSequence(value.toString())) throw new RangeError("sequence must be an unsigned 64-bit integer");
  return value;
}

function validationError(error: unknown, operation: "append" | "commit" | "request" | "path"): never {
  const code = String(error);
  if (code === "invalid_path" || code === "invalid_argument" || code === "limit_exceeded") {
    const message = code === "limit_exceeded" ? `${operation} command exceeds contract limits`
      : code === "invalid_argument" && operation === "commit" ? "commit requires valid, matching conditions and mutations"
      : code.replaceAll("_", " ");
    throw new StreamError(code, message);
  }
  throw error;
}

type WireRequest =
  | { readonly kind: "tail"; readonly path: string }
  | { readonly kind: "fork"; readonly source: string; readonly destination: string; readonly options?: ForkOptions }
  | { readonly kind: "read"; readonly path: string; readonly from: bigint; readonly limit: number }
  | { readonly kind: "follow"; readonly path: string; readonly from: bigint }
  | { readonly kind: "children"; readonly parent?: string; readonly limit: number }
  | { readonly kind: "children_page"; readonly parent?: string; readonly after?: string; readonly hierarchyVersion?: Uint8Array; readonly limit: number };

export type HttpRequestRoute = HttpRoute;

/** Encode the hosted token request through the canonical protobuf/Rust route. */
export function wireCreateTokenRequest(request: CreateTokenRequest): Uint8Array {
  try {
    if (request === null || typeof request !== "object" || !Array.isArray(request.allow)) {
      throw new StreamError("invalid_argument", "token request must contain an allow array");
    }
    return toBinary(CreateTokenRequestSchema, create(CreateTokenRequestSchema, {
      expiresIn: request.expiresIn,
      allow: request.allow.map(grant => {
        if (grant === null || typeof grant !== "object") {
          throw new StreamError("invalid_argument", "token grant must be an object");
        }
        return {
          path: grant.path,
          ...(grant.subtree === undefined ? {} : { subtree: grant.subtree }),
          operations: [...grant.operations],
        };
      }),
    }));
  } catch (error) {
    if (error instanceof StreamError) throw error;
    throw new StreamError("invalid_argument", `token request fields are malformed: ${error instanceof Error ? error.message : String(error)}`);
  }
}

export function wireRequest(request: WireRequest): Uint8Array {
  switch (request.kind) {
      case "tail":
        requirePathType(request.path);
        return toBinary(TailRequestSchema, create(TailRequestSchema, { path: request.path }));
      case "fork":
        requirePathType(request.source);
        requirePathType(request.destination);
        if (request.options?.atTail !== undefined) requireSequenceType(request.options.atTail);
        if (request.options?.idempotencyKey !== undefined) requireBytesType(request.options.idempotencyKey);
        return toBinary(ForkRequestSchema, create(ForkRequestSchema, {
          source: request.source, destination: request.destination,
          ...(request.options?.atTail === undefined ? {} : { atTail: request.options.atTail }),
          ...(request.options?.idempotencyKey === undefined ? {} : { idempotencyKey: request.options.idempotencyKey }),
        }));
      case "read":
        requirePathType(request.path);
        requireSequenceType(request.from);
        requireLimitType(request.limit);
        return toBinary(ReadRequestSchema, create(ReadRequestSchema, { path: request.path, from: request.from, limit: request.limit }));
      case "follow":
        requirePathType(request.path);
        requireSequenceType(request.from);
        return toBinary(FollowRequestSchema, create(FollowRequestSchema, { path: request.path, from: request.from }));
      case "children":
        if (request.parent !== undefined) requirePathType(request.parent);
        requireLimitType(request.limit);
        return toBinary(ChildrenRequestSchema, create(ChildrenRequestSchema, {
          limit: request.limit, ...(request.parent === undefined ? {} : { parent: request.parent }),
        }));
      case "children_page":
        if (request.parent !== undefined) requirePathType(request.parent);
        if (request.after !== undefined) requirePathType(request.after);
        if (request.hierarchyVersion !== undefined) requireBytesType(request.hierarchyVersion);
        requireLimitType(request.limit);
        return toBinary(ChildrenPageRequestSchema, create(ChildrenPageRequestSchema, {
          limit: request.limit,
          ...(request.parent === undefined ? {} : { parent: request.parent }),
          ...(request.after === undefined ? {} : { after: request.after }),
          ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion }),
        }));
      default: throw new StreamError("invalid_argument", "stream request kind is invalid");
  }
}

export async function validateWireRequest(request: WireRequest): Promise<void> {
  let input: Uint8Array;
  try {
    input = wireRequest(request);
  } catch (error) {
    if (error instanceof StreamError) throw error;
    throw new StreamError("invalid_argument", `stream request fields are malformed: ${error instanceof Error ? error.message : String(error)}`);
  }
  await ensureStreamWasm();
  const error = validateRequest(request.kind, input);
  if (error) validationError(error, "request");
}

function requirePathType(value: unknown): void {
  if (typeof value !== "string") throw new StreamError("invalid_argument", "stream path must be text");
}
function requireSequenceType(value: unknown): void {
  if (typeof value !== "bigint") throw new StreamError("invalid_argument", "stream position must be a bigint");
}
function requireBytesType(value: unknown): void {
  if (!(value instanceof Uint8Array)) throw new StreamError("invalid_argument", "stream identity must be bytes");
}
function requireLimitType(value: unknown): void {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) throw new StreamError("invalid_argument", "stream limit must be a safe integer");
}

export function wireAppendRequest(path: string, records: readonly Uint8Array[], options?: AppendOptions): Uint8Array {
  requirePathType(path);
  if (!Array.isArray(records)) throw new StreamError("invalid_argument", "append records must be an array");
  for (const record of records) requireBytesType(record);
  if (options?.ifTail !== undefined) requireSequenceType(options.ifTail);
  if (options?.idempotencyKey !== undefined) requireBytesType(options.idempotencyKey);
  return toBinary(AppendRequestSchema, create(AppendRequestSchema, {
    path, records: [...records],
    ...(options?.ifTail === undefined ? {} : { ifTail: options.ifTail }),
    ...(options?.idempotencyKey === undefined ? {} : { idempotencyKey: options.idempotencyKey }),
  }));
}

export async function validateWireAppend(path: string, records: readonly Uint8Array[], options?: AppendOptions): Promise<void> {
  const input = wireAppendRequest(path, records, options);
  await ensureStreamWasm();
  const error = validateAppendRequest(input);
  if (error) validationError(error, "append");
}

export function wireInspectIdempotencyRequest(key: IdempotencyKey): Uint8Array {
  return toBinary(InspectIdempotencyRequestSchema, create(InspectIdempotencyRequestSchema, { idempotencyKey: idempotencyKey(key) }));
}

export function wireReadCommitRequest(value: Uint8Array): Uint8Array {
  return toBinary(ReadCommitRequestSchema, create(ReadCommitRequestSchema, { commitId: commitId(value) }));
}

export async function encodeHttpRequest(route: HttpRequestRoute, input: Uint8Array): Promise<string> {
  await ensureStreamWasm();
  try { return encodeHttpRequestWire(route, input); }
  catch (error) { validationError(error, "request"); }
}

export async function normalizeWireCommitBytes(request: ProviderCommitRequest, options: CommitOptions): Promise<Uint8Array> {
  let input: Uint8Array;
  try {
    const conditions = request.conditions.map(condition => {
      if ("ifTail" in condition) return create(CommitConditionSchema, {
        condition: { case: "tail", value: create(TailConditionSchema, { path: condition.path, expected: condition.ifTail }) },
      });
      if (condition.ifAbsent !== true) throw new StreamError("invalid_argument", "absence condition must be true");
      return create(CommitConditionSchema, {
        condition: { case: "absent", value: create(AbsentConditionSchema, { path: condition.path }) },
      });
    });
    const mutations = request.mutations.map(mutation => {
      if ("append" in mutation) return create(CommitMutationSchema, {
        mutation: { case: "append", value: create(AppendMutationSchema, { path: mutation.append.path, records: [...mutation.append.values] }) },
      });
      if ("fork" in mutation) return create(CommitMutationSchema, {
        mutation: { case: "fork", value: create(ForkMutationSchema, {
          source: mutation.fork.source,
          destination: mutation.fork.destination,
          atTail: mutation.fork.atTail,
          records: mutation.fork.values.map((value: Uint8Array) => value.slice()),
        }) },
      });
      throw new StreamError("invalid_argument", "commit mutation is invalid");
    });
    const wire = create(CommitRequestSchema, { conditions, mutations, idempotencyKey: options.idempotencyKey, ...(options.deadlineUnixMillis === undefined ? {} : { deadlineUnixMillis: options.deadlineUnixMillis }) });
    input = toBinary(CommitRequestSchema, wire);
  } catch (error) {
    if (error instanceof StreamError) throw error;
    throw new StreamError("invalid_argument", `commit request fields are malformed: ${error instanceof Error ? error.message : String(error)}`);
  }
  await ensureStreamWasm();
  let canonical: Uint8Array;
  try { canonical = normalizeCommitRequest(input); }
  catch (error) { validationError(error, "commit"); }
  return canonical;
}

export async function normalizeWireCommit(request: ProviderCommitRequest, options: CommitOptions): Promise<ProviderCommitRequest> {
  const canonical = await normalizeWireCommitBytes(request, options);
  const normalized = fromBinary(CommitRequestSchema, canonical);
  return {
    conditions: normalized.conditions.map(condition => {
      switch (condition.condition.case) {
        case "tail": return { path: condition.condition.value.path, ifTail: condition.condition.value.expected };
        case "absent": return { path: condition.condition.value.path, ifAbsent: true };
        default: throw new StreamError("invalid_argument", "normalized condition is missing its kind");
      }
    }),
    mutations: normalized.mutations.map(mutation => {
      switch (mutation.mutation.case) {
        case "append": return {
          append: {
            path: mutation.mutation.value.path,
            values: mutation.mutation.value.records.map(record => record.slice()),
          },
        };
        case "fork": {
          const value = mutation.mutation.value;
          return { fork: { source: value.source, destination: value.destination, atTail: value.atTail, values: value.records.map(record => record.slice()) } };
        }
        default: throw new StreamError("invalid_argument", "normalized mutation is missing its kind");
      }
    }),
  };
}
