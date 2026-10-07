import { Buffer } from "node:buffer";
import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { fromBinary } from "@bufbuild/protobuf";
import { projectMemoryResponse } from "../generated/wasm/acyclic_stream_wasm.js";
import * as wire from "../generated/proto/stream/v2/stream_pb.js";
import type * as GeneratedNative from "../generated/native/binding.js";
import { normalizeWireCommitBytes, validateWireRequest, wireAppendRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import { validateAppend } from "./client.js";
import { StreamError, commitId, type StreamFailureCode } from "./types.js";
import type { AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommitId, CommittedEnvelope, CommitOptions, CommitResult, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey, IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence, StreamProvider } from "./types.js";

type NativeModule = typeof GeneratedNative;
type NativeClient = InstanceType<NativeModule["NativeStreamClient"]>;
type NativeFollow = InstanceType<NativeModule["NativeStreamFollow"]>;
type NativeCancellation = InstanceType<NativeModule["NativeStreamCancellation"]>;
type NativeConnectResult = Awaited<ReturnType<NativeModule["NativeStreamClient"]["connectResult"]>>;

let modulePromise: Promise<NativeModule> | undefined;
const requireNative = createRequire(import.meta.url);
const nativeBindingPath = fileURLToPath(new URL("../generated/native/binding.cjs", import.meta.url));
const nativeTargetMetadataPath = fileURLToPath(new URL("../generated/native/native-targets.json", import.meta.url));

type NativeTargetMetadata = { readonly selected_target?: unknown };

function selectedNativeTarget(): string | undefined {
  try {
    const parsed = JSON.parse(readFileSync(nativeTargetMetadataPath, "utf8")) as NativeTargetMetadata;
    return typeof parsed.selected_target === "string" ? parsed.selected_target : undefined;
  } catch (error) {
    if (typeof error === "object" && error !== null && "code" in error && (error as { readonly code?: unknown }).code === "ENOENT") return undefined;
    throw error;
  }
}

function targetMatchesRuntime(target: string): boolean {
  const runtime = globalThis.process;
  if (runtime === undefined) return false;
  const match = /^(?:x86_64|aarch64|armv7|i686)-([^-]+)-/.exec(target);
  if (match === null) return false;
  const arch = target.startsWith("x86_64-") ? "x64" : target.startsWith("aarch64-") ? "arm64" : target.startsWith("i686-") ? "ia32" : "arm";
  const platform = target.includes("windows") ? "win32" : target.includes("apple-darwin") ? "darwin" : target.includes("linux") ? "linux" : target.includes("freebsd") ? "freebsd" : undefined;
  return platform !== undefined && runtime.platform === platform && runtime.arch === arch;
}

async function loadNativeModule(): Promise<NativeModule> {
  modulePromise ??= Promise.resolve().then(() => {
    const namespace = requireNative(nativeBindingPath) as NativeModule & { readonly default?: NativeModule };
    const binding = namespace.NativeStreamClient === undefined ? namespace.default : namespace;
    if (binding?.NativeStreamClient === undefined || binding.NativeStreamCancellation === undefined) {
      throw new Error("Stream native companion did not export the generated Rust N-API binding");
    }
    return binding;
  }).catch((error: unknown) => {
    modulePromise = undefined;
    throw error;
  });
  return modulePromise;
}

/** Creates a cancellation handle from the generated Rust N-API binding. */
export async function createNativeStreamCancellation(): Promise<NativeCancellation> {
  const module = await loadNativeModule();
  return new module.NativeStreamCancellation();
}

function missingNativeBinding(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const code = "code" in error ? (error as { readonly code?: unknown }).code : undefined;
  if (code !== "MODULE_NOT_FOUND") return false;
  const message = "message" in error && typeof (error as { readonly message?: unknown }).message === "string"
    ? (error as { readonly message: string }).message : String(error);
  const firstLine = message.split(/\r?\n/, 1)[0] ?? message;
  return /Cannot find module ['"][^'"]*[\\/]generated[\\/]native[\\/]binding\.cjs['"]/.test(firstLine);
}

function expectedNativeAbsence(error: unknown): boolean {
  if (missingNativeBinding(error)) return true;
  if (typeof error !== "object" || error === null) return false;
  const message = "message" in error && typeof (error as { readonly message?: unknown }).message === "string"
    ? (error as { readonly message: string }).message : "";
  if (!message.startsWith("Cannot find native binding.")) return false;
  let cause: unknown = "cause" in error ? (error as { readonly cause?: unknown }).cause : undefined;
  let sawCause = false;
  while (cause !== null && cause !== undefined) {
    sawCause = true;
    if (typeof cause !== "object") return false;
    const causeMessage = "message" in cause && typeof (cause as { readonly message?: unknown }).message === "string"
      ? (cause as { readonly message: string }).message : "";
    if (!/^Cannot find module ['"](?:\.\/index\.[^'"]+\.node|\.\/index\.wasi\.cjs|@acyclic-labs\/stream-[^'"]+|\.\/index\.wasm[^'"]*)['"]/.test(causeMessage.split(/\r?\n/, 1)[0] ?? causeMessage)) return false;
    cause = "cause" in cause ? (cause as { readonly cause?: unknown }).cause : undefined;
  }
  return sawCause;
}

/** Whether the staged native binding is installed for this runtime. */
export async function nativeStreamAvailable(): Promise<boolean> {
  const target = selectedNativeTarget();
  if (target !== undefined && !targetMatchesRuntime(target)) return false;
  try { await loadNativeModule(); return true; }
  catch (error) { if (expectedNativeAbsence(error)) return false; throw error; }
}

/** Thin provider over the Rust-owned Stream N-API bridge. */
export class NativeStreamProvider implements StreamProvider {
  private constructor(private readonly client: NativeClient, private readonly module: NativeModule) {}

  static async connect(endpoint: string, token: string, caCertificate?: Uint8Array, cancellation?: NativeCancellation): Promise<NativeStreamProvider> {
    const module = await loadNativeModule();
    const result: NativeConnectResult = caCertificate === undefined
      ? await module.NativeStreamClient.connectResult(endpoint, token, cancellation)
      : await module.NativeStreamClient.connectWithCaResult(endpoint, token, Buffer.from(caCertificate), cancellation);
    if (result.error !== undefined) throw nativeError(result.error, "connect");
    if (result.client === undefined) throw new StreamError("transport", "Stream native companion returned no client");
    return new NativeStreamProvider(result.client, module);
  }

  get transport(): string { return this.client.transport(); }

  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const value = await this.operation("inspect_idempotency", this.client.inspectIdempotencyResult(Buffer.from(wireInspectIdempotencyRequest(key))));
    return this.project("inspect_idempotency", value) as IdempotencyObservation | undefined;
  }

  async tail(path: string): Promise<Sequence> {
    const request = await this.request("tail", path);
    const value = await this.operation("tail", this.client.tailResult(Buffer.from(request)));
    try { return fromBinary(wire.TailResponseSchema, value).tail; }
    catch (error) { throw invalidResponse("tail", error); }
  }

  async append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult> {
    const records = values.map(value => value.slice());
    const authored = options === undefined ? undefined : structuredClone(options);
    await validateAppend(path, records, authored);
    const value = await this.operation("append", this.client.appendResult(Buffer.from(wireAppendRequest(path, records, authored))));
    return this.project("append", value) as AppendResult;
  }

  async fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt> {
    const request = { kind: "fork" as const, source, destination, ...(options === undefined ? {} : { options: structuredClone(options) }) };
    await validateWireRequest(request);
    const value = await this.operation("fork", this.client.forkResult(Buffer.from(wireRequest(request))));
    return this.project("fork", value) as ForkReceipt;
  }

  async *read(path: string, options: ReadOptions): AsyncIterable<EncodedRecord> {
    const request = { kind: "read" as const, path, from: options.from, limit: options.limit };
    await validateWireRequest(request);
    const result = await this.sequence("read", this.client.readResult(Buffer.from(wireRequest(request))));
    let next = request.from;
    let count = 0;
    for (const value of result) {
      if (++count > request.limit) throw new StreamError("invalid_response", "read exceeds requested limit");
      let response: wire.ReadResponse;
      try { response = fromBinary(wire.ReadResponseSchema, value); }
      catch (error) { throw invalidResponse("read", error); }
      const record = checkedRecord(response.record, next);
      next = record.sequence + 1n;
      yield record;
    }
  }

  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const request = { kind: "follow" as const, path, from: options.from };
    await validateWireRequest(request);
    if (options.signal?.aborted) return;
    const cancellation = new this.module.NativeStreamCancellation();
    let follow: NativeFollow | undefined;
    const abort = () => { cancellation.cancel(); if (follow !== undefined) void follow.close(); };
    options.signal?.addEventListener("abort", abort, { once: true });
    if (options.signal?.aborted) {
      cancellation.cancel();
      options.signal.removeEventListener("abort", abort);
      return;
    }
    let next = request.from;
    try {
      let opened: Awaited<ReturnType<NativeClient["openFollowResult"]>>;
      try {
        opened = await this.client.openFollowResult(Buffer.from(wireRequest(request)), cancellation);
      } catch (error) {
        if (options.signal?.aborted) return;
        throw nativeError(error, "follow");
      }
      if (opened.error !== undefined) {
        if (options.signal?.aborted) return;
        throw nativeError(opened.error, "follow");
      }
      if (opened.follow === undefined) {
        if (options.signal?.aborted) return;
        throw new StreamError("invalid_response", "native follow omitted its cursor");
      }
      follow = opened.follow;
      if (options.signal?.aborted) return;
      while (!options.signal?.aborted) {
        const result = await follow.nextResult();
        if (result.error !== undefined) {
          if (options.signal?.aborted) return;
          throw nativeError(result.error, "follow");
        }
        if (result.value === undefined) return;
        let response: wire.ReadResponse;
        try { response = fromBinary(wire.ReadResponseSchema, result.value); }
        catch (error) { throw invalidResponse("follow", error); }
        const record = checkedRecord(response.record, next);
        next = record.sequence + 1n;
        yield record;
      }
    } finally {
      options.signal?.removeEventListener("abort", abort);
      if (follow !== undefined) await follow.close();
    }
  }

  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    const authored = { kind: "children_page" as const, limit: request.limit,
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion.slice() }),
    };
    await validateWireRequest(authored);
    const value = await this.operation("children_page", this.client.childrenPageResult(Buffer.from(wireRequest(authored))));
    return this.project("children_page", value) as ChildrenPage;
  }

  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> {
    const value = await this.operation("commit", this.client.commitResult(Buffer.from(await normalizeWireCommitBytes(structuredClone(request), structuredClone(options)))));
    return this.project("commit", value) as CommitResult;
  }

  async readCommit(id: CommitId): Promise<CommittedEnvelope> {
    const value = await this.operation("read_commit", this.client.readCommitResult(Buffer.from(wireReadCommitRequest(id))));
    return this.project("read_commit", value) as CommittedEnvelope;
  }

  private async request(kind: "tail", path: string): Promise<Uint8Array> {
    await validateWireRequest({ kind, path });
    return wireRequest({ kind, path });
  }

  private async operation(operation: string, result: Promise<GeneratedNative.NativeStreamOperationResult>): Promise<Uint8Array> {
    try {
      const value = await result;
      if (value.error !== undefined) throw nativeError(value.error, operation);
      if (value.value === undefined) throw new StreamError("invalid_response", `native ${operation} response omitted its value`);
      return new Uint8Array(value.value);
    } catch (error) {
      if (error instanceof StreamError) throw error;
      throw nativeError(error, operation);
    }
  }

  private async sequence(operation: string, result: Promise<GeneratedNative.NativeStreamSequenceResult>): Promise<readonly Buffer[]> {
    try {
      const value = await result;
      if (value.error !== undefined) throw nativeError(value.error, operation);
      return value.values.map(item => Buffer.from(item));
    } catch (error) {
      if (error instanceof StreamError) throw error;
      throw nativeError(error, operation);
    }
  }

  private project(operation: string, value: Uint8Array): unknown {
    try { return projectMemoryResponse(operation, value); }
    catch (error) { throw invalidResponse(operation, error); }
  }
}

function checkedRecord(record: wire.Record | undefined, expected: bigint): EncodedRecord {
  if (record === undefined || record.sequence !== expected || record.value.length > wire.StreamLimit.MAX_RECORD_BYTES) {
    throw new StreamError("invalid_response", "stream response contains an invalid record cursor or body");
  }
  try { return { sequence: record.sequence, value: record.value, commitId: commitId(record.commitId), committedAtMicros: record.committedAtMicros }; }
  catch { throw new StreamError("invalid_response", "stream record omitted its canonical commit identity"); }
}

function invalidResponse(operation: string, error: unknown): StreamError {
  const message = error instanceof Error ? error.message : String(error);
  return new StreamError("invalid_response", `invalid ${operation} response${message ? `: ${message}` : ""}`);
}

function nativeError(error: unknown, operation: string): Error {
  if (error instanceof StreamError) return error;
  let code: unknown;
  let message: string;
  if (typeof error === "object" && error !== null) {
    code = "code" in error ? (error as { readonly code?: unknown }).code : undefined;
    message = "message" in error && typeof (error as { readonly message?: unknown }).message === "string"
      ? (error as { readonly message: string }).message : String(error);
  } else message = String(error);
  if (typeof code !== "string") return new StreamError("unavailable", message);
  let mappedCode = code;
  if (mappedCode === "not_found") mappedCode = operation === "read_commit" ? "commit_not_found" : "stream_not_found";
  else if (mappedCode === "already_exists") mappedCode = "destination_exists";
  else if (mappedCode === "capacity") mappedCode = "capacity_exhausted";
  else if (mappedCode === "prefix_not_retained" && operation === "commit") mappedCode = "invalid_argument";
  const knownCodes = new Set<StreamFailureCode>([
    "invalid_path", "invalid_argument", "limit_exceeded", "not_found", "already_exists",
    "prefix_not_retained", "out_of_range", "idempotency_mismatch", "capacity", "access_denied",
    "unavailable", "hierarchy_changed", "deadline_elapsed", "unsupported", "stream_not_found",
    "commit_not_found", "destination_exists", "capacity_exhausted", "configuration", "invalid_cursor",
    "invalid_page", "invalid_response", "provider_mismatch", "response_too_large", "transport",
  ]);
  return new StreamError(knownCodes.has(mappedCode as StreamFailureCode) ? mappedCode as StreamFailureCode : "unavailable", message);
}
