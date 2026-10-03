import { arch, platform } from "node:process";
import { Buffer } from "node:buffer";
import { GrpcStreamProvider } from "./grpc.js";
import { commitId, StreamError } from "./types.js";
import type {
  AppendOptions, AppendResult, ChildrenPage, ChildrenPageRequest, CommitId, CommittedEnvelope,
  CommitOptions, CommitResult, EncodedRecord, FollowOptions, ForkOptions, ForkReceipt,
  IdempotencyKey, IdempotencyObservation, ProviderCommitRequest, ReadOptions, Sequence,
  StreamProvider,
} from "./types.js";

export interface NativeStreamOptions {
  readonly endpoints: readonly string[];
  readonly token: string;
  readonly caCertificate?: string;
  readonly maximumMessageBytes?: number;
}

interface NativeAppendResult {
  readonly committed: boolean;
  readonly start?: string;
  readonly end?: string;
  readonly tail?: string;
  readonly actualTail?: string;
  readonly actual_tail?: string;
  readonly commitId?: Uint8Array;
  readonly commit_id?: Uint8Array;
}
interface NativeRecord {
  readonly sequence: string;
  readonly value: Uint8Array;
  readonly commitId?: Uint8Array;
  readonly commit_id?: Uint8Array;
  readonly committedAtMicros?: string;
  readonly committed_at_micros?: string;
}
interface NativeRecordBatch { readonly records: readonly NativeRecord[]; readonly cancelled: boolean }
interface NativeChild { readonly path: string }
interface NativeForkReceipt {
  readonly source: string;
  readonly destination: string;
  readonly forkedAt?: string;
  readonly forked_at?: string;
  readonly tail: string;
  readonly commitId?: Uint8Array;
  readonly commit_id?: Uint8Array;
}
interface NativeCancellation { cancel(): void; readonly cancelled: boolean }
interface NativeStreamClient {
  tail(path: string): Promise<string>;
  append(path: string, records: readonly Uint8Array[], ifTail?: string, idempotencyKey?: Uint8Array): Promise<NativeAppendResult>;
  read(path: string, from: string, limit: number): Promise<NativeRecordBatch>;
  follow(path: string, from: string, cancellation?: NativeCancellation): Promise<NativeRecordBatch>;
  fork(source: string, destination: string, atTail?: string, idempotencyKey?: Uint8Array): Promise<NativeForkReceipt>;
  children(parent: string | undefined, limit: number): Promise<readonly NativeChild[]>;
}
interface NativeStreamModule {
  readonly NativeStreamClient: { connect(options: {
    endpoints: readonly string[];
    bearerToken: string;
    caCertificatePem?: Uint8Array;
  }): Promise<NativeStreamClient> };
  readonly NativeStreamCancellation: new () => NativeCancellation;
}

const TARGETS = new Set(["win32-x64", "win32-arm64", "linux-x64", "linux-arm64", "darwin-x64", "darwin-arm64"]);
let bindingPromise: Promise<NativeStreamModule> | undefined;

async function binding(): Promise<NativeStreamModule> {
  const target = `${platform}-${arch}`;
  if (!TARGETS.has(target)) throw new Error(`@acyclic-labs/stream has no native companion for ${target}`);
  bindingPromise ??= import(`@acyclic-labs/stream-${target}`).then((module) => {
    const namespace = module as NativeStreamModule & { readonly default?: NativeStreamModule };
    const candidate = namespace.NativeStreamClient === undefined ? namespace.default : namespace;
    if (candidate?.NativeStreamClient === undefined || candidate.NativeStreamCancellation === undefined) {
      throw new Error("native companion did not export the Stream bridge");
    }
    return candidate;
  }).catch((error: unknown) => {
    bindingPromise = undefined;
    throw error;
  });
  return bindingPromise;
}

function bytes(value: Uint8Array | undefined, name: string): Uint8Array {
  if (value === undefined) throw new StreamError("invalid_response", `native response omitted ${name}`);
  return Uint8Array.from(value);
}
function decimal(value: string | undefined, name: string): bigint {
  if (value === undefined || !/^\d+$/.test(value)) throw new StreamError("invalid_response", `native response contains an invalid ${name}`);
  return BigInt(value);
}
function record(value: NativeRecord): EncodedRecord {
  const identity = value.commitId ?? value.commit_id;
  const micros = value.committedAtMicros ?? value.committed_at_micros;
  return { sequence: decimal(value.sequence, "record sequence"), value: Uint8Array.from(value.value), commitId: commitId(bytes(identity, "record commit ID")), committedAtMicros: decimal(micros, "record commit time") };
}
function nativeError(operation: string, error: unknown): Error {
  if (error instanceof StreamError) return error;
  return new StreamError("unavailable", `native Stream ${operation} failed: ${error instanceof Error ? error.message : String(error)}`);
}

/** Stream provider using the Rust N-API bridge for the operations it exposes. */
export class NativeStreamProvider implements StreamProvider {
  readonly #client: NativeStreamClient;
  readonly #fallback: GrpcStreamProvider;

  private constructor(client: NativeStreamClient, fallback: GrpcStreamProvider) {
    this.#client = client;
    this.#fallback = fallback;
  }

  static async connect(options: NativeStreamOptions): Promise<NativeStreamProvider> {
    const module = await binding();
    const client = await module.NativeStreamClient.connect({
      endpoints: [...options.endpoints],
      bearerToken: options.token,
      ...(options.caCertificate === undefined ? {} : { caCertificatePem: Buffer.from(options.caCertificate) }),
    });
    return new NativeStreamProvider(client, new GrpcStreamProvider({ endpoint: options.endpoints[0]!, token: options.token, ...(options.caCertificate === undefined ? {} : { caCertificate: options.caCertificate }), ...(options.maximumMessageBytes === undefined ? {} : { maximumMessageBytes: options.maximumMessageBytes }) }));
  }

  async inspectIdempotency(key: IdempotencyKey, _signal?: AbortSignal): Promise<IdempotencyObservation | undefined> { return this.#fallback.inspectIdempotency(key); }
  async tail(path: string, _signal?: AbortSignal): Promise<Sequence> { try { return decimal(await this.#client.tail(path), "tail"); } catch (error) { throw nativeError("tail", error); } }
  async append(path: string, values: readonly Uint8Array[], options?: AppendOptions, _signal?: AbortSignal): Promise<AppendResult> {
    try {
      const result = await this.#client.append(path, values.map(value => Buffer.from(value)), options?.ifTail?.toString(), options?.idempotencyKey === undefined ? undefined : Buffer.from(options.idempotencyKey));
      if (!result.committed) return { ok: false, code: "tail_conflict", actualTail: decimal(result.actualTail ?? result.actual_tail, "actual tail") };
      const commit = bytes(result.commitId ?? result.commit_id, "commit ID");
      if (result.start === undefined || result.end === undefined || result.tail === undefined) throw new StreamError("invalid_response", "native append response omitted its receipt");
      return { ok: true, start: decimal(result.start, "append start"), end: decimal(result.end, "append end"), tail: decimal(result.tail, "append tail"), commitId: commitId(commit) };
    } catch (error) { throw nativeError("append", error); }
  }
  async fork(source: string, destination: string, options?: ForkOptions, _signal?: AbortSignal): Promise<ForkReceipt> {
    try {
      const result = await this.#client.fork(source, destination, options?.atTail?.toString(), options?.idempotencyKey === undefined ? undefined : Buffer.from(options.idempotencyKey));
      return { source: result.source, destination: result.destination, forkedAt: decimal(result.forkedAt ?? result.forked_at, "forked-at sequence"), tail: decimal(result.tail, "fork tail"), commitId: commitId(bytes(result.commitId ?? result.commit_id, "fork commit ID")) };
    } catch (error) { throw nativeError("fork", error); }
  }
  async *read(path: string, options: ReadOptions, _signal?: AbortSignal): AsyncIterable<EncodedRecord> {
    try { for (const value of (await this.#client.read(path, options.from.toString(), options.limit)).records) yield record(value); }
    catch (error) { throw nativeError("read", error); }
  }
  async *follow(path: string, options: FollowOptions): AsyncIterable<EncodedRecord> {
    const cancellation = new (await binding()).NativeStreamCancellation();
    const cancel = () => cancellation.cancel();
    if (options.signal?.aborted) return;
    options.signal?.addEventListener("abort", cancel, { once: true });
    try {
      const batch = await this.#client.follow(path, options.from.toString(), cancellation);
      for (const value of batch.records) {
        if (options.signal?.aborted) return;
        yield record(value);
      }
    } catch (error) { if (!options.signal?.aborted) throw nativeError("follow", error); }
    finally { options.signal?.removeEventListener("abort", cancel); }
  }
  async childrenPage(request: ChildrenPageRequest, _signal?: AbortSignal): Promise<ChildrenPage> { return this.#fallback.childrenPage(request); }
  async commit(request: ProviderCommitRequest, options: CommitOptions, _signal?: AbortSignal): Promise<CommitResult> { return this.#fallback.commit(request, options); }
  async readCommit(commitIdValue: CommitId, _signal?: AbortSignal): Promise<CommittedEnvelope> { return this.#fallback.readCommit(commitIdValue); }
}
