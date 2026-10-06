import { arch, platform, report } from "node:process";
import type {
  CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, Image,
  ImageQualification, MachineEventPage, MachineId, MachineObservation, MachinesProvider,
  MachinePage, MutationOutcome, OperationId, OperationObservation, SuspensionPolicy,
  UsageReceipt,
} from "./index.js";
import { MACHINES_NATIVE_COMPANION_TARGETS } from "./generated-client.js";

/** mTLS material passed to the Rust-owned native Machines bridge. */
export interface NativeMachinesOptions {
  readonly endpoint: string;
  readonly caCertificate: string;
  readonly certificate: string;
  readonly privateKey: string;
}

interface NativeMachinesClient {
  qualifyImage(requestJson: string): Promise<string>;
  create(requestJson: string): Promise<string>;
  inspectMachine(requestJson: string): Promise<string>;
  listMachines(requestJson: string): Promise<string>;
  events(requestJson: string): Promise<string>;
  usage(requestJson: string): Promise<string>;
  checkpoint(requestJson: string): Promise<string>;
  inspectCheckpoint(requestJson: string): Promise<string>;
  fork(requestJson: string): Promise<string>;
  forkMachine(requestJson: string): Promise<string>;
  suspend(requestJson: string): Promise<string>;
  wake(requestJson: string): Promise<string>;
  setSuspensionPolicy(requestJson: string): Promise<string>;
  destroyMachine(requestJson: string): Promise<string>;
  destroyCheckpoint(requestJson: string): Promise<string>;
  recover(requestJson: string): Promise<string>;
  recoverOperation(requestJson: string): Promise<string>;
  inspectOperation(requestJson: string): Promise<string>;
  cancel(requestJson: string): Promise<string>;
  watchOperation(requestJson: string): Promise<string>;
  assurance(): string;
}

interface NativeMachinesModule {
  readonly MachinesNativeClient: {
    connect(options: NativeMachinesOptions): Promise<NativeMachinesClient>;
    connectFromEnv(): Promise<NativeMachinesClient>;
  };
}

let bindingPromise: Promise<NativeMachinesModule> | undefined;

/** Resolve the companion name from the runtime ABI and the package matrix. */
export function nativeCompanionTarget(
  currentPlatform: string = platform,
  currentArch: string = arch,
  currentReport: { getReport?: () => { header?: { glibcVersionRuntime?: unknown } } } | undefined = report,
): string {
  const base = `${currentPlatform}-${currentArch}`;
  if (currentPlatform !== "linux") return base;
  let libc = "gnu";
  try {
    const header = currentReport?.getReport?.().header;
    if (currentReport !== undefined && typeof header?.glibcVersionRuntime !== "string") libc = "musl";
  } catch {
    // Keep the common glibc default when a runtime report is unavailable.
  }
  return `${base}-${libc}`;
}

async function binding(): Promise<NativeMachinesModule> {
  const target = nativeCompanionTarget();
  if (!(MACHINES_NATIVE_COMPANION_TARGETS as readonly string[]).includes(target)) throw new Error(`@acyclic-labs/machines has no native companion for ${target}`);
  bindingPromise ??= import(`@acyclic-labs/machines-${target}`).then((module) => {
    const namespace = module as NativeMachinesModule & { readonly default?: NativeMachinesModule };
    const candidate = namespace.MachinesNativeClient === undefined ? namespace.default : namespace;
    if (candidate?.MachinesNativeClient === undefined) throw new Error("native companion did not export MachinesNativeClient");
    return candidate;
  }).catch((error: unknown) => {
    bindingPromise = undefined;
    throw error;
  });
  return bindingPromise;
}

function decode<T>(value: string): T {
  try {
    return JSON.parse(value) as T;
  } catch (error) {
    throw new TypeError(`native Machines response is not valid JSON: ${String(error)}`);
  }
}

function encode(value: unknown): string {
  return JSON.stringify(value, (_key, nested) => typeof nested === "bigint" ? nested.toString() : nested);
}

/**
 * The current Rust N-API methods accept only the serialized request and do
 * not expose a cancellation hook. Abort therefore stops awaiting the bridge
 * from JavaScript while preserving the accepted native operation's lifetime;
 * it does not cancel work already running in Rust.
 */
function invokeNative(operation: (requestJson: string) => Promise<string>, requestJson: string, signal?: AbortSignal): Promise<string> {
  if (signal === undefined) return operation(requestJson);
  const aborted = () => signal.reason ?? new DOMException("The operation was aborted", "AbortError");
  if (signal.aborted) return Promise.reject(aborted());
  return new Promise<string>((resolve, reject) => {
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const onAbort = () => { cleanup(); reject(aborted()); };
    signal.addEventListener("abort", onAbort, { once: true });
    let pending: Promise<string>;
    try {
      pending = operation(requestJson);
    } catch (error) {
      cleanup();
      reject(error);
      return;
    }
    pending.then(
      (value) => { cleanup(); resolve(value); },
      (error: unknown) => { cleanup(); reject(error); },
    );
  });
}

/** Machines provider backed by the Rust N-API domain and transport boundary. */
export class NativeMachinesProvider implements MachinesProvider {
  readonly assurance = "customer-hosted" as const;
  readonly #client: NativeMachinesClient;

  private constructor(client: NativeMachinesClient) { this.#client = client; }

  static async connect(options: NativeMachinesOptions): Promise<NativeMachinesProvider> {
    const module = await binding();
    return new NativeMachinesProvider(await module.MachinesNativeClient.connect(options));
  }

  static async connectFromEnv(): Promise<NativeMachinesProvider> {
    const module = await binding();
    return new NativeMachinesProvider(await module.MachinesNativeClient.connectFromEnv());
  }

  qualifyImage(image: Image, signal?: AbortSignal): Promise<ImageQualification> { return invokeNative(this.#client.qualifyImage.bind(this.#client), encode(image), signal).then(decode<ImageQualification>); }
  create(request: CreateMachine, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.create.bind(this.#client), encode(request), signal).then(decode<MutationOutcome>); }
  inspectMachine(machineId: MachineId, signal?: AbortSignal): Promise<MachineObservation> { return invokeNative(this.#client.inspectMachine.bind(this.#client), encode(machineId), signal).then(decode<MachineObservation>); }
  listMachines(after: MachineId | null, limit: number, signal?: AbortSignal): Promise<MachinePage> { return invokeNative(this.#client.listMachines.bind(this.#client), encode({ after, limit }), signal).then(decode<MachinePage>); }
  checkpoint(machineId: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.checkpoint.bind(this.#client), encode({ machineId, idempotencyKey }), signal).then(decode<MutationOutcome>); }
  inspectCheckpoint(checkpointId: CheckpointId, signal?: AbortSignal): Promise<CheckpointObservation> { return invokeNative(this.#client.inspectCheckpoint.bind(this.#client), encode(checkpointId), signal).then(decode<CheckpointObservation>); }
  fork(checkpointId: CheckpointId, count: number, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.fork.bind(this.#client), encode({ checkpointId, count, idempotencyKey }), signal).then(decode<MutationOutcome>); }
  forkMachine(machineId: MachineId, count: number, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.forkMachine.bind(this.#client), encode({ machineId, count, idempotencyKey }), signal).then(decode<MutationOutcome>); }
  suspend(machineId: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.suspend.bind(this.#client), encode({ machineId, idempotencyKey }), signal).then(decode<MutationOutcome>); }
  wake(machineId: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.wake.bind(this.#client), encode({ machineId, idempotencyKey }), signal).then(decode<MutationOutcome>); }
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.setSuspensionPolicy.bind(this.#client), encode({ machineId, policy, idempotencyKey }), signal).then(decode<MutationOutcome>); }
  destroyMachine(machineId: MachineId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.destroyMachine.bind(this.#client), encode({ machineId, idempotencyKey }), signal).then(decode<MutationOutcome>); }
  destroyCheckpoint(checkpointId: CheckpointId, idempotencyKey: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.destroyCheckpoint.bind(this.#client), encode({ checkpointId, idempotencyKey }), signal).then(decode<MutationOutcome>); }
  recover(key: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return invokeNative(this.#client.recover.bind(this.#client), encode(key), signal).then(decode<MutationOutcome>); }
  recoverOperation(key: IdempotencyKey, signal?: AbortSignal): Promise<OperationId> { return invokeNative(this.#client.recoverOperation.bind(this.#client), encode(key), signal).then(decode<OperationId>); }
  inspectOperation(operationId: OperationId, signal?: AbortSignal): Promise<OperationObservation> { return invokeNative(this.#client.inspectOperation.bind(this.#client), encode(operationId), signal).then(decode<OperationObservation>); }
  cancel(operationId: OperationId, signal?: AbortSignal): Promise<OperationObservation> { return invokeNative(this.#client.cancel.bind(this.#client), encode(operationId), signal).then(decode<OperationObservation>); }
  events(machineId: MachineId, afterSequence: number | null, limit: number, signal?: AbortSignal): Promise<MachineEventPage> { return invokeNative(this.#client.events.bind(this.#client), encode({ machineId, afterSequence, limit }), signal).then(decode<MachineEventPage>); }
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number, signal?: AbortSignal): Promise<UsageReceipt> { return invokeNative(this.#client.usage.bind(this.#client), encode({ machineId, startUnixMs, endUnixMs }), signal).then(decode<UsageReceipt>); }
  async *watchOperation(operationId: OperationId, signal?: AbortSignal): AsyncIterable<OperationObservation> {
    for (const observation of decode<readonly OperationObservation[]>(await invokeNative(this.#client.watchOperation.bind(this.#client), encode(operationId), signal))) yield observation;
  }
}
