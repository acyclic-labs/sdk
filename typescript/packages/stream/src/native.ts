import { SessionOpenedSchema } from "../generated/proto/stream/v1/session_pb.js";
import { Opened, streamOpening } from "./opened.js";
import { recordsFromFrames } from "./record.js";
import { wireWhenAbsent } from "./contract.js";
import { Buffer } from "node:buffer";
import { credentialProvider, validateCredential, type CredentialProvider } from "@acyclic-labs/actors/credential";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { projectMemoryResponse } from "../generated/wasm/acyclic_stream_wasm.js";
import * as wire from "../generated/proto/stream/v1/stream_pb.js";
import type * as GeneratedNative from "../generated/native/binding.js";
import { normalizeWireCommitBytes, validateWireRequest, wireInspectIdempotencyRequest, wireReadCommitRequest, wireRequest } from "./contract.js";
import { validateAppend } from "./client.js";
import { checkedChild } from "./child.js";
import { nativeCredentials } from "./native-credentials.js";
import { StreamError, type StreamFailureCode } from "./types.js";
import type { AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommitId, CommittedEnvelope, CommitOptions, CommitResult, FollowOptions, ForkOptions, ForkReceipt, IdempotencyKey, IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence, StreamProvider } from "./types.js";

let modulePromise: Promise<typeof GeneratedNative> | undefined;
const requireNative = createRequire(import.meta.url);
const nativeBindingPath = fileURLToPath(new URL("../generated/native/binding.cjs", import.meta.url));

async function loadNativeModule(): Promise<typeof GeneratedNative> {
  modulePromise ??= Promise.resolve().then(() => {
    const namespace = requireNative(nativeBindingPath) as typeof GeneratedNative & { readonly default?: typeof GeneratedNative };
    const binding = namespace.NativeStreamClient === undefined ? namespace.default : namespace;
    if (typeof binding?.NativeStreamClient !== "function" || typeof binding.NativeStreamCancellation !== "function"
      || typeof binding.NativeStreamClient.connectResult !== "function" || typeof binding.NativeStreamClient.connectWithCaResult !== "function"
      || typeof binding.NativeStreamClient.prototype?.tailResult !== "function"
      || typeof binding.NativeStreamClient.prototype?.transport !== "function") {
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
export async function createNativeStreamCancellation(): Promise<GeneratedNative.NativeStreamCancellation> {
  const module = await loadNativeModule();
  return new module.NativeStreamCancellation();
}

function isOwnNativeLoader(path: string): boolean {
  return (path.startsWith("file:") ? fileURLToPath(path) : path).replace(/\\/g, "/") === nativeBindingPath.replace(/\\/g, "/");
}

function importedByNativeLoader(message: string): boolean {
  const lines = message.split(/\r?\n/);
  const marker = lines.findIndex(line => line.trim() === "Require stack:");
  const frame = marker >= 0 ? lines[marker + 1]?.trim().replace(/^-\s*/, "")
    : lines[0]?.match(/\s(?:imported )?from (?:['"]([^'"]+)['"]|(.+))$/i);
  const importer = typeof frame === "string" ? frame : frame?.[1] ?? frame?.[2];
  return importer !== undefined && isOwnNativeLoader(importer);
}

function expectedNativeAbsence(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  const message = "message" in error && typeof error.message === "string" ? error.message : undefined;
  if ("code" in error && error.code === "MODULE_NOT_FOUND") {
    const firstLine = (message ?? String(error)).split(/\r?\n/, 1)[0];
    const requested = firstLine?.match(/^Cannot find module ['"]([^'"]+)['"]/)?.[1];
    if (requested !== undefined && isOwnNativeLoader(requested)) return true;
  }
  if (!message?.startsWith("Cannot find native binding.")) return false;
  let cause: unknown = "cause" in error ? (error as { readonly cause?: unknown }).cause : undefined;
  if (cause === null || cause === undefined) return false;
  while (cause !== null && cause !== undefined) {
    if (typeof cause !== "object") return false;
    const code = "code" in cause ? cause.code : undefined;
    if (code !== undefined && code !== "MODULE_NOT_FOUND" && code !== "ERR_MODULE_NOT_FOUND") return false;
    const causeMessage = "message" in cause && typeof (cause as { readonly message?: unknown }).message === "string"
      ? (cause as { readonly message: string }).message : "";
    if (!/^Cannot find module ['"](?:\.\/index\.[^'"]+\.node|\.\/index\.wasi\.cjs|@acyclic-labs\/stream-[^'"]+|\.\/index\.wasm[^'"]*)['"]/.test(causeMessage.split(/\r?\n/, 1)[0] ?? causeMessage)) return false;
    if (!importedByNativeLoader(causeMessage)) return false;
    const companion = causeMessage.match(/^Cannot find module ['"](@acyclic-labs\/stream-[^'"]+)['"]/)?.[1];
    if (companion !== undefined) {
      try { requireNative.resolve(`${companion}/package.json`); return false; }
      catch (error) {
        if (typeof error !== "object" || error === null || !("code" in error) || error.code !== "MODULE_NOT_FOUND") throw error;
      }
    }
    cause = "cause" in cause ? (cause as { readonly cause?: unknown }).cause : undefined;
  }
  return true;
}

/** Whether the staged native binding is installed for this runtime. */
export async function nativeStreamAvailable(): Promise<boolean> {
  try { await loadNativeModule(); return true; }
  catch (error) { if (expectedNativeAbsence(error)) return false; throw error; }
}

/** Thin provider over the Rust-owned Stream N-API bridge. */
export class NativeStreamProvider implements StreamProvider {
  private constructor(private readonly client: GeneratedNative.NativeStreamClient, private readonly module: typeof GeneratedNative, private readonly credential?: ReturnType<typeof nativeCredentials>) {}

  static async connect(endpoint: string, token: string | undefined, caCertificate?: Uint8Array, cancellation?: GeneratedNative.NativeStreamCancellation, credential?: CredentialProvider): Promise<NativeStreamProvider> {
    const certificate = caCertificate === undefined ? undefined : Buffer.from(caCertificate);
    let abandon = (): void => {};
    const resolve = credential === undefined ? undefined : nativeCredentials(credentialProvider({ credential }), () => abandon());
    if (resolve === undefined) validateCredential(token);
    const module = await loadNativeModule();
    let result: GeneratedNative.NativeStreamConnectResult;
    if (resolve !== undefined) {
      const binding = module.NativeStreamClient as typeof GeneratedNative.NativeStreamClient & {
        connectWithCredentialResult(endpoint: string, callback: () => Promise<string>, ca?: Buffer, cancellation?: GeneratedNative.NativeStreamCancellation): Promise<GeneratedNative.NativeStreamConnectResult>;
      };
      if (typeof binding.connectWithCredentialResult !== "function") throw new StreamError("configuration", "Stream native companion does not support credential providers");
      const release = resolve.retain();
      try { result = await binding.connectWithCredentialResult(endpoint, () => resolve.resolve(), certificate, cancellation); }
      finally { release(); }
    } else {
      validateCredential(token);
      result = certificate === undefined
        ? await module.NativeStreamClient.connectResult(endpoint, token, cancellation)
        : await module.NativeStreamClient.connectWithCaResult(endpoint, token, certificate, cancellation);
    }
    if (result.error !== undefined) throw nativeError(result.error, "connect");
    if (result.client === undefined) throw new StreamError("transport", "Stream native companion returned no client");
    const client = result.client;
    if (resolve !== undefined) {
      if (typeof client.abandonSessionAttempts !== "function") throw new StreamError("configuration", "Stream native companion does not support credential cancellation");
      abandon = () => client.abandonSessionAttempts();
    }
    return new NativeStreamProvider(result.client, module, resolve);
  }

  private async request<T>(operation: (client: GeneratedNative.NativeStreamClient) => Promise<T>): Promise<T> {
    const release = this.credential?.retain();
    try { return await operation(this.client); }
    finally { release?.(); }
  }

  async registerCredential(slot: number, credential: Uint8Array): Promise<void> {
    try { this.client.registerCredential(slot, Buffer.from(credential)); }
    catch (error) { throw nativeError(error, "session credential"); }
  }

  get transport(): string { return this.client.transport(); }

  async inspectIdempotency(key: IdempotencyKey): Promise<IdempotencyObservation | undefined> {
    const bytes = Buffer.from(wireInspectIdempotencyRequest(key));
    const value = await this.operation("inspect_idempotency", this.request(client => client.inspectIdempotencyResult(bytes)));
    return this.project("inspect_idempotency", value) as IdempotencyObservation | undefined;
  }

  async heads(options: import("./types.js").HeadsRequest): Promise<import("./types.js").Heads> {
    const request = { kind: "heads" as const, ...structuredClone(options) };
    await validateWireRequest(request);
    const value = await this.operation("heads", this.request(client => client.headsResult(Buffer.from(wireRequest(request)))));
    return this.project("heads", value) as import("./types.js").Heads;
  }
  async head(path: string): Promise<import("./types.js").PathHead> {
    const request = { kind: "head" as const, path };
    await validateWireRequest(request);
    const value = await this.operation("head", this.request(client => client.headResult(Buffer.from(wireRequest(request)))));
    return this.project("head", value) as import("./types.js").PathHead;
  }
  async tail(path: string): Promise<Sequence> {
    const request = { kind: "tail" as const, path };
    await validateWireRequest(request);
    const value = await this.operation("tail", this.request(client => client.tailResult(Buffer.from(wireRequest(request)))));
    try { return fromBinary(wire.TailResponseSchema, value).tail; }
    catch (error) { throw invalidResponse("tail", error); }
  }

  async append(path: string, values: readonly Uint8Array[], options?: AppendOptions): Promise<AppendResult> {
    const records = values.map(value => value.slice());
    const authored = options === undefined ? undefined : structuredClone(options);
    const input = await validateAppend(path, records, authored);
    const value = await this.operation("append", this.request(client => client.appendResult(Buffer.from(input))));
    return this.project("append", value) as AppendResult;
  }

  async fork(source: string, destination: string, options?: ForkOptions): Promise<ForkReceipt> {
    const request = { kind: "fork" as const, source, destination, ...(options === undefined ? {} : { options: structuredClone(options) }) };
    await validateWireRequest(request);
    const value = await this.operation("fork", this.request(client => client.forkResult(Buffer.from(wireRequest(request)))));
    return this.project("fork", value) as ForkReceipt;
  }

  read(path: string, options: ReadOptions): Opened {
    return this.records("read", path, structuredClone(options));
  }

  follow(path: string, options: FollowOptions): Opened {
    const authored = { ...options, ...(options.filter === undefined ? {} : { filter: structuredClone(options.filter) }) };
    return this.records("follow", path, authored, options.signal);
  }

  private records(operation: "read" | "follow", path: string, options: ReadOptions | FollowOptions, signal?: AbortSignal): Opened {
    return new Opened(async signal => {
      const input = operation === "read"
        ? await validateWireRequest({ kind: "read", path, ...(options as ReadOptions) })
        : (await validateWireRequest({ kind: "follow", path, from: options.from }),
          toBinary(wire.FollowRequestSchema, create(wire.FollowRequestSchema, { path, from: options.from,
            whenAbsent: wireWhenAbsent((options as FollowOptions).whenAbsent),
            ...((options as FollowOptions).filter === undefined ? {} : { filter: (options as FollowOptions).filter }),
          })));
      signal.throwIfAborted();
      const cancellation = new this.module.NativeStreamCancellation();
      let cursor: GeneratedNative.NativeStreamFollow | undefined;
      let release: (() => void) | undefined;
      const abort = (): void => { cancellation.cancel(); };
      const close = async (): Promise<void> => {
        signal.removeEventListener("abort", abort);
        cancellation.cancel();
        release?.(); release = undefined;
        const retained = cursor; cursor = undefined;
        await retained?.close();
      };
      signal.addEventListener("abort", abort, { once: true });
      try {
        release = this.credential?.retain(signal);
        const result = operation === "read"
          ? await this.client.readResult(Buffer.from(input), cancellation)
          : await this.client.openFollowResult(Buffer.from(input), cancellation);
        if (result.error !== undefined) throw nativeError(result.error, operation);
        cursor = result.follow;
        if (cursor === undefined || result.opened === undefined) throw new StreamError("invalid_response", "native open omitted its receipt or cursor");
        signal.throwIfAborted();
        const opening = streamOpening(fromBinary(SessionOpenedSchema, result.opened));
        const retained = cursor;
        const frames = async function* (): AsyncGenerator<Uint8Array> {
          for (;;) {
            const result = await retained.nextResult();
            if (signal.aborted) return;
            if (result.error !== undefined) throw nativeError(result.error, operation);
            if (result.value === undefined) return;
            yield result.value;
          }
        };
        return { opening, records: recordsFromFrames(frames(), path, opening.selection.from,
          operation === "read" ? options as ReadOptions : undefined, signal), close };
      } catch (error) {
        try { await close(); }
        catch (cleanup) { throw new AggregateError([error, cleanup], "native stream open failed during cleanup"); }
        if (error instanceof StreamError || signal.aborted) throw error;
        throw nativeError(error, operation);
      }
    }, signal);
  }
  async childrenPage(request: ChildrenPageRequest): Promise<ChildrenPage> {
    const authored = { kind: "children_page" as const,
      ...(request.tailStream === undefined ? {} : { tailStream: request.tailStream }), limit: request.limit,
      ...(request.parent === undefined ? {} : { parent: request.parent }),
      ...(request.after === undefined ? {} : { after: request.after }),
      ...(request.hierarchyVersion === undefined ? {} : { hierarchyVersion: request.hierarchyVersion.slice() }),
    };
    await validateWireRequest(authored);
    const value = await this.operation("children_page", this.request(client => client.childrenPageResult(Buffer.from(wireRequest(authored)))));
    return this.project("children_page", value) as ChildrenPage;
  }

  async commit(request: ProviderCommitRequest, options: CommitOptions): Promise<CommitResult> {
    const bytes = Buffer.from(await normalizeWireCommitBytes(structuredClone(request), structuredClone(options)));
    const value = await this.operation("commit", this.request(client => client.commitResult(bytes)));
    return this.project("commit", value) as CommitResult;
  }

  async readCommit(id: CommitId): Promise<CommittedEnvelope> {
    const bytes = Buffer.from(wireReadCommitRequest(id));
    const value = await this.operation("read_commit", this.request(client => client.readCommitResult(bytes)));
    return this.project("read_commit", value) as CommittedEnvelope;
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

  private project(operation: string, value: Uint8Array): unknown {
    try { return projectMemoryResponse(operation, value); }
    catch (error) { throw invalidResponse(operation, error); }
  }
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
    "invalid_page", "invalid_response", "provider_mismatch", "response_too_large", "transport", "revision_foreign",
  ]);
  return new StreamError(knownCodes.has(mappedCode as StreamFailureCode) ? mappedCode as StreamFailureCode : "unavailable", message);
}
