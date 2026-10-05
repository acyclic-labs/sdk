import { arch, platform, report } from "node:process";
import type {
  CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, Image,
  ImageQualification, MachineEventPage, MachineId, MachineObservation, MachinesProvider,
  MachinePage, MutationOutcome, OperationId, OperationObservation, SuspensionPolicy,
  UsageReceipt,
} from "./index.js";

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

const TARGETS = new Set([
  "win32-x64", "win32-arm64", "linux-x64-gnu", "linux-arm64-gnu", "darwin-x64", "darwin-arm64",
]);

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
  if (!TARGETS.has(target)) throw new Error(`@acyclic-labs/machines has no native companion for ${target}`);
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

  qualifyImage(image: Image): Promise<ImageQualification> { return this.#client.qualifyImage(encode(image)).then(decode<ImageQualification>); }
  create(request: CreateMachine): Promise<MutationOutcome> { return this.#client.create(encode(request)).then(decode<MutationOutcome>); }
  inspectMachine(machineId: MachineId): Promise<MachineObservation> { return this.#client.inspectMachine(encode(machineId)).then(decode<MachineObservation>); }
  listMachines(after: MachineId | null, limit: number): Promise<MachinePage> { return this.#client.listMachines(encode({ after, limit })).then(decode<MachinePage>); }
  checkpoint(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#client.checkpoint(encode({ machineId, idempotencyKey })).then(decode<MutationOutcome>); }
  inspectCheckpoint(checkpointId: CheckpointId): Promise<CheckpointObservation> { return this.#client.inspectCheckpoint(encode(checkpointId)).then(decode<CheckpointObservation>); }
  fork(checkpointId: CheckpointId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#client.fork(encode({ checkpointId, count, idempotencyKey })).then(decode<MutationOutcome>); }
  forkMachine(machineId: MachineId, count: number, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#client.forkMachine(encode({ machineId, count, idempotencyKey })).then(decode<MutationOutcome>); }
  suspend(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#client.suspend(encode({ machineId, idempotencyKey })).then(decode<MutationOutcome>); }
  wake(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#client.wake(encode({ machineId, idempotencyKey })).then(decode<MutationOutcome>); }
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#client.setSuspensionPolicy(encode({ machineId, policy, idempotencyKey })).then(decode<MutationOutcome>); }
  destroyMachine(machineId: MachineId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#client.destroyMachine(encode({ machineId, idempotencyKey })).then(decode<MutationOutcome>); }
  destroyCheckpoint(checkpointId: CheckpointId, idempotencyKey: IdempotencyKey): Promise<MutationOutcome> { return this.#client.destroyCheckpoint(encode({ checkpointId, idempotencyKey })).then(decode<MutationOutcome>); }
  recover(key: IdempotencyKey): Promise<MutationOutcome> { return this.#client.recover(encode(key)).then(decode<MutationOutcome>); }
  recoverOperation(key: IdempotencyKey): Promise<OperationId> { return this.#client.recoverOperation(encode(key)).then(decode<OperationId>); }
  inspectOperation(operationId: OperationId): Promise<OperationObservation> { return this.#client.inspectOperation(encode(operationId)).then(decode<OperationObservation>); }
  cancel(operationId: OperationId): Promise<OperationObservation> { return this.#client.cancel(encode(operationId)).then(decode<OperationObservation>); }
  events(machineId: MachineId, afterSequence: number | null, limit: number): Promise<MachineEventPage> { return this.#client.events(encode({ machineId, afterSequence, limit })).then(decode<MachineEventPage>); }
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number): Promise<UsageReceipt> { return this.#client.usage(encode({ machineId, startUnixMs, endUnixMs })).then(decode<UsageReceipt>); }
  async *watchOperation(operationId: OperationId): AsyncIterable<OperationObservation> {
    for (const observation of decode<readonly OperationObservation[]>(await this.#client.watchOperation(encode(operationId)))) yield observation;
  }
}
