import type {
  CheckpointId, CheckpointObservation, CreateMachine, ForkFidelity, IdempotencyKey, MachineEvent, MachineId,
  MachineObservation, MachinesProvider, MutationOutcome, OperationId, OperationObservation,
  SuspensionPolicy, UsageReceipt,
} from "./index.js";
import { MANAGED_OCI_CONTRACT } from "./managed-oci-contract.js";
import type { NativeMachinesOptions } from "./native.js";
import { HttpMachinesProvider } from "./http.js";
import { MACHINES_REMOTE_POLICY, isRustOwnedTransportUnavailable, selectRustOwnedTransport, validateRustOwnedCredentialPolicy, type RustOwnedRuntime } from "./generated-client.js";
import { ensureMachinesWasm } from "./wasm-runtime.js";

export interface MachinesEnvironment {
  readonly endpoint?: string;
  /** Retained for source compatibility; native Machines authentication is mTLS. */
  readonly token?: string;
  readonly caCertificate?: string;
  readonly certificate?: string;
  readonly privateKey?: string;
}

export interface MachineListOptions {
  /** Exclusive cursor. */
  readonly after?: MachineId;
  /** Per-request page size. */
  readonly pageSize?: number;
  /** Hard bound on the number of yielded Machines. */
  readonly maximum?: number;
  /** Stops awaiting the current page request. */
  readonly signal?: AbortSignal;
}

export class Machines {
  constructor(readonly provider: MachinesProvider) {}
  /** Connects through the Rust-owned native provider selected for this runtime. */
  static async fromEnv(environment: Partial<MachinesEnvironment> = {}): Promise<Machines> {
    const runtime: RustOwnedRuntime = isNativeRuntime() ? "native" : "browser";
    const selected = selectRustOwnedTransport(MACHINES_REMOTE_POLICY, runtime);
    await ensureMachinesWasm();
    if (selected === "grpc-web") {
      if (environment.endpoint === undefined || environment.token === undefined) {
        throw new TypeError("Machines browser transport requires endpoint and token");
      }
      validateRustOwnedCredentialPolicy(environment.token);
      const { RemoteMachines } = await import("./remote.js");
      return new Machines(await RemoteMachines.connect(environment.endpoint, environment.token));
    }
    if (selected !== "grpc" || runtime !== "native") throw new TypeError("Machines transport is unavailable for this runtime");
    // Keep the native companion outside browser bundles; this path is reached only
    // after the runtime check above and is resolved by the Node conditional export.
    const nativeModule = "./native.js";
    const { NativeMachinesProvider } = await import(nativeModule);
    const endpoint = environment.endpoint;
    const caCertificate = environment.caCertificate;
    const certificate = environment.certificate;
    const privateKey = environment.privateKey;
    const hasMutualTls = caCertificate !== undefined || certificate !== undefined || privateKey !== undefined;
    if (environment.token !== undefined) {
      if (hasMutualTls) throw new TypeError("Machines native transport accepts either token or complete mutual-TLS credentials");
      if (endpoint === undefined) throw new TypeError("Machines native bearer transport requires endpoint and token");
      validateRustOwnedCredentialPolicy(environment.token);
      const options: NativeMachinesOptions = { endpoint, token: environment.token };
      try {
        return new Machines(await NativeMachinesProvider.connect(options));
      } catch (error) {
        if (!isRustOwnedTransportUnavailable(error)) throw error;
        return new Machines(new HttpMachinesProvider({ endpoint, token: environment.token }));
      }
    }
    if (endpoint === undefined && caCertificate === undefined && certificate === undefined && privateKey === undefined) {
      return new Machines(await NativeMachinesProvider.connectFromEnv());
    }
    if (endpoint === undefined || caCertificate === undefined || certificate === undefined || privateKey === undefined) {
      throw new TypeError("Machines native transport requires endpoint and either token or complete mutual-TLS credentials");
    }
    const options: NativeMachinesOptions = { endpoint, caCertificate, certificate, privateKey };
    try {
      return new Machines(await NativeMachinesProvider.connect(options));
    } catch (error) {
      if (!isRustOwnedTransportUnavailable(error)) throw error;
      throw new TypeError("Machines mutual-TLS transport is unavailable because its native companion is not installed", { cause: error });
    }
  }
  qualifyImage(image: import("./index.js").Image, signal?: AbortSignal): Promise<import("./index.js").ImageQualification> { return this.provider.qualifyImage(image, signal); }
  async create(request: CreateMachine, signal?: AbortSignal): Promise<Machine> { const outcome = await this.provider.create(request, signal); return new Machine(this.provider, expectOutcome(outcome, "created").machine.id); }
  async attach(id: MachineId, signal?: AbortSignal): Promise<Machine> { await this.provider.inspectMachine(id, signal); return new Machine(this.provider, id); }
  machine(id: MachineId): Machine { return new Machine(this.provider, id); }
  checkpoint(id: CheckpointId): Checkpoint { return new Checkpoint(this.provider, id); }
  operation(id: OperationId): Operation { return new Operation(this.provider, id); }
  /** Observes a durable operation by its exact retained operation identity. */
  recover(id: OperationId, signal?: AbortSignal): Promise<OperationObservation> { return this.provider.inspectOperation(id, signal); }
  /** Recovers the mutation outcome associated with an idempotency key. */
  recoverMutation(key: IdempotencyKey, signal?: AbortSignal): Promise<MutationOutcome> { return this.provider.recover(key, signal); }
  async recoverOperation(key: IdempotencyKey, signal?: AbortSignal): Promise<Operation> { return this.operation(await this.provider.recoverOperation(key, signal)); }
  async *list(options: MachineListOptions = {}): AsyncIterable<Machine> {
    const pageSize = options.pageSize ?? MANAGED_OCI_CONTRACT.maxPageSize;
    const maximum = options.maximum ?? 1024;
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > MANAGED_OCI_CONTRACT.maxPageSize) throw new RangeError(`pageSize must be 1..=${MANAGED_OCI_CONTRACT.maxPageSize}`);
    if (!Number.isSafeInteger(maximum) || maximum < 0) throw new RangeError("maximum must be a nonnegative safe integer");
    let cursor = options.after ?? null;
    let yielded = 0;
    while (yielded < maximum) {
      const page = await this.provider.listMachines(cursor, Math.min(pageSize, maximum - yielded), options.signal);
      for (const observation of page.machines) {
        yield new Machine(this.provider, observation.id);
        yielded += 1;
        if (yielded === maximum) return;
      }
      if (page.next === null) return;
      if (page.next === cursor || page.machines.length === 0) throw new Error("machine listing did not advance its cursor");
      cursor = page.next;
    }
  }
}

export class Machine {
  constructor(readonly provider: MachinesProvider, readonly id: MachineId) {}
  inspect(signal?: AbortSignal): Promise<MachineObservation> { return this.provider.inspectMachine(this.id, signal); }
  async checkpoint(key: IdempotencyKey, signal?: AbortSignal): Promise<Checkpoint> { return new Checkpoint(this.provider, expectOutcome(await this.provider.checkpoint(this.id, key, signal), "checkpointed").checkpoint.id); }
  /** Forks this running machine into `count` fresh children; see `MachinesProvider.forkMachine` for the exact semantics. */
  async fork(count: number, key: IdempotencyKey, signal?: AbortSignal): Promise<MachineFork> { const outcome = expectOutcome(await this.provider.forkMachine(this.id, count, key, signal), "machine-forked"); if (outcome.source !== this.id) throw new MachineOutcomeError("machine-forked", outcome); return { fidelity: outcome.fidelity, children: outcome.children.map(value => new Machine(this.provider, value.id)) }; }
  suspend(key: IdempotencyKey, signal?: AbortSignal): Promise<Extract<MutationOutcome, { kind: "suspended" }>> { return this.#outcome("suspended", this.provider.suspend(this.id, key, signal)); }
  wake(key: IdempotencyKey, signal?: AbortSignal): Promise<Extract<MutationOutcome, { kind: "woken" }>> { return this.#outcome("woken", this.provider.wake(this.id, key, signal)); }
  setSuspensionPolicy(policy: SuspensionPolicy, key: IdempotencyKey, signal?: AbortSignal): Promise<Extract<MutationOutcome, { kind: "suspension-policy-set" }>> { return this.#outcome("suspension-policy-set", this.provider.setSuspensionPolicy(this.id, policy, key, signal)); }
  destroy(key: IdempotencyKey, signal?: AbortSignal): Promise<Extract<MutationOutcome, { kind: "machine-destroyed" }>> { return this.#outcome("machine-destroyed", this.provider.destroyMachine(this.id, key, signal)); }
  events(afterSequence: number | null = null, limit = MANAGED_OCI_CONTRACT.maxEventPageSize, signal?: AbortSignal): Promise<{ readonly events: readonly MachineEvent[]; readonly nextSequence: number | null }> { return this.provider.events(this.id, afterSequence, limit, signal); }
  usage(startUnixMs: number, endUnixMs: number, signal?: AbortSignal): Promise<UsageReceipt> { return this.provider.usage(this.id, startUnixMs, endUnixMs, signal); }
  async #outcome<Kind extends MutationOutcome["kind"]>(kind: Kind, value: Promise<MutationOutcome>): Promise<Extract<MutationOutcome, { kind: Kind }>> { return expectOutcome(await value, kind); }
}

/** Children of one `Machine.fork` and the fidelity they were forked at. */
export interface MachineFork { readonly fidelity: ForkFidelity; readonly children: readonly Machine[] }

export class Checkpoint {
  constructor(readonly provider: MachinesProvider, readonly id: CheckpointId) {}
  inspect(signal?: AbortSignal): Promise<CheckpointObservation> { return this.provider.inspectCheckpoint(this.id, signal); }
  async fork(count: number, key: IdempotencyKey, signal?: AbortSignal): Promise<readonly Machine[]> { return expectOutcome(await this.provider.fork(this.id, count, key, signal), "forked").machines.map(value => new Machine(this.provider, value.id)); }
  destroy(key: IdempotencyKey, signal?: AbortSignal): Promise<Extract<MutationOutcome, { kind: "checkpoint-destroyed" }>> { return this.provider.destroyCheckpoint(this.id, key, signal).then(value => expectOutcome(value, "checkpoint-destroyed")); }
}

export class Operation {
  constructor(readonly provider: MachinesProvider, readonly id: OperationId) {}
  inspect(signal?: AbortSignal): Promise<OperationObservation> { return this.provider.inspectOperation(this.id, signal); }
  cancel(signal?: AbortSignal): Promise<OperationObservation> { return this.provider.cancel(this.id, signal); }
  watch(signal?: AbortSignal): AsyncIterable<OperationObservation> { return this.provider.watchOperation(this.id, signal); }
}

function expectOutcome<Kind extends MutationOutcome["kind"]>(outcome: MutationOutcome, kind: Kind): Extract<MutationOutcome, { kind: Kind }> { if (outcome.kind !== kind) throw new MachineOutcomeError(kind, outcome); return outcome as Extract<MutationOutcome, { kind: Kind }>; }
export class MachineOutcomeError extends Error { constructor(readonly expected: MutationOutcome["kind"], readonly outcome: MutationOutcome) { super(`expected ${expected} outcome, received ${outcome.kind}`); } }
function isNativeRuntime(): boolean {
  const value = globalThis as typeof globalThis & { process?: { versions?: { node?: string; bun?: string } } };
  return typeof value.process?.versions?.node === "string" || typeof value.process?.versions?.bun === "string";
}
