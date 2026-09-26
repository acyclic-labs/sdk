import type { CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey, Image, ImageQualification, MachineEvent, MachineId, MachineObservation, MachinesProvider, MutationOutcome, OperationId, OperationObservation, Performance, SuspensionPolicy, UsageReceipt } from "./index.js";
import { digestHex as canonicalDigestHex } from "./digest.js";

export interface HttpMachinesOptions { readonly endpoint: string; readonly token: string; readonly fetcher?: typeof fetch; readonly maximumResponseBytes?: number }

/** Managed-service transport with bounded responses and no implicit mutation retries. */
export class HttpMachinesProvider implements MachinesProvider {
  readonly assurance = "managed-service" as const;
  readonly #endpoint: string; readonly #token: string; readonly #fetcher: typeof fetch; readonly #maximum: number;
  constructor(options: HttpMachinesOptions) { const endpoint = new URL(options.endpoint); if (endpoint.protocol !== "https:" || endpoint.username || endpoint.password || endpoint.search || endpoint.hash) throw new TypeError("endpoint must be an absolute HTTPS URL without credentials, query, or fragment"); if (!options.token.trim()) throw new TypeError("token is required"); const maximum = options.maximumResponseBytes ?? 8 * 1024 * 1024; if (!Number.isSafeInteger(maximum) || maximum <= 0) throw new RangeError("maximumResponseBytes must be a positive safe integer"); this.#endpoint = endpoint.href.endsWith("/") ? endpoint.href : `${endpoint.href}/`; this.#token = options.token; this.#fetcher = options.fetcher ?? fetch; this.#maximum = maximum; }
  async qualifyImage(image: Image) { const expected = inputImage(image); return this.#call("images/qualify", { image: expected }, value => qualification(value, expected)); }
  async create(request: CreateMachine) { return this.#call("machines/create", { ...request, image: inputImage(request.image) }, value => checkedMutation(mutation(value), { kind: "created" })); }
  inspectMachine(machineId: MachineId) { return this.#call("machines/inspect", { machineId }, value => machineFor(value, machineId)); }
  listMachines(after: MachineId | null, limit: number) { return this.#call("machines/list", { after, limit }, machinePage); }
  checkpoint(machineId: MachineId, idempotencyKey: IdempotencyKey) { return this.#call("machines/checkpoint", { machineId, idempotencyKey }, value => checkedMutation(mutation(value), { kind: "checkpointed", source: machineId })); }
  inspectCheckpoint(checkpointId: CheckpointId) { return this.#call("checkpoints/inspect", { checkpointId }, value => checkpointFor(value, checkpointId)); }
  fork(checkpointId: CheckpointId, count: number, performance: Performance, idempotencyKey: IdempotencyKey) { return this.#call("checkpoints/fork", { checkpointId, count, performance, idempotencyKey }, value => checkedMutation(mutation(value), { kind: "forked", checkpointId, count, performance })); }
  forkMachine(machineId: MachineId, count: number, idempotencyKey: IdempotencyKey) { return this.#call("machines/fork", { machineId, count, idempotencyKey }, value => checkedMutation(mutation(value), { kind: "machine-forked", machineId, count })); }
  suspend(machineId: MachineId, idempotencyKey: IdempotencyKey) { return this.#call("machines/suspend", { machineId, idempotencyKey }, value => checkedMutation(mutation(value), { kind: "suspended", machineId })); }
  wake(machineId: MachineId, idempotencyKey: IdempotencyKey) { return this.#call("machines/wake", { machineId, idempotencyKey }, value => checkedMutation(mutation(value), { kind: "woken", machineId })); }
  setSuspensionPolicy(machineId: MachineId, policy: SuspensionPolicy, idempotencyKey: IdempotencyKey) { return this.#call("machines/suspension-policy", { machineId, policy, idempotencyKey }, value => checkedMutation(mutation(value), { kind: "suspension-policy-set", machineId, policy })); }
  destroyMachine(machineId: MachineId, idempotencyKey: IdempotencyKey) { return this.#call("machines/destroy", { machineId, idempotencyKey }, value => checkedMutation(mutation(value), { kind: "machine-destroyed", machineId })); }
  destroyCheckpoint(checkpointId: CheckpointId, idempotencyKey: IdempotencyKey) { return this.#call("checkpoints/destroy", { checkpointId, idempotencyKey }, value => checkedMutation(mutation(value), { kind: "checkpoint-destroyed", checkpointId })); }
  events(machineId: MachineId, afterSequence: number | null, limit: number) { return this.#call("machines/events", { machineId, afterSequence, limit }, value => eventPageFor(value, machineId, afterSequence)); }
  usage(machineId: MachineId, startUnixMs: number, endUnixMs: number) { return this.#call("machines/usage", { machineId, startUnixMs, endUnixMs }, value => usageFor(value, machineId, startUnixMs, endUnixMs)); }
  recover(idempotencyKey: IdempotencyKey) { return this.#call("operations/recover", { idempotencyKey }, mutation); }
  recoverOperation(idempotencyKey: IdempotencyKey) { return this.#call("operations/recover-id", { idempotencyKey }, value => text(value, "operationId") as OperationId); }
  inspectOperation(operationId: OperationId) { return this.#call("operations/inspect", { operationId }, value => operationFor(value, operationId)); }
  cancel(operationId: OperationId) { return this.#call("operations/cancel", { operationId }, value => operationFor(value, operationId)); }
  async *watchOperation(operationId: OperationId): AsyncIterable<OperationObservation> { for (const observation of await this.#call("operations/watch", { operationId }, value => array(value, item => operationFor(item, operationId)))) yield observation; }
  async #call<Value>(route: string, request: unknown, project: Decoder<Value>): Promise<Value> { const response = await this.#fetcher(new URL(`v1/machines/${route}`, this.#endpoint), { method: "POST", headers: { authorization: `Bearer ${this.#token}`, "content-type": "application/json" }, body: encode(request) }); const bytes = await boundedBytes(response, this.#maximum); if (!response.ok) throw new MachinesTransportError(`HTTP ${response.status}`, response.status); try { return project(decode(new TextDecoder().decode(bytes))); } catch { throw new MachinesTransportError(`invalid ${route} response`, response.status); } }
}

export class MachinesTransportError extends Error { constructor(message: string, readonly status: number) { super(message); } }
async function boundedBytes(response: Response, maximum: number): Promise<Uint8Array> {
  const reader = response.body?.getReader();
  if (reader === undefined) return new Uint8Array();
  const chunks: Uint8Array[] = []; let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maximum) { await reader.cancel().catch(() => undefined); throw new MachinesTransportError("response exceeds configured bound", response.status); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(total); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return bytes;
}
function encode(value: unknown): string { return JSON.stringify(value, (_key, item) => typeof item === "bigint" ? { $bigint: item.toString() } : item instanceof Uint8Array ? { $bytes: bytes(item) } : item); }
function decode(value: string): unknown { return JSON.parse(value, (_key, item: unknown) => { if (item !== null && typeof item === "object" && "$bigint" in item && typeof item.$bigint === "string") return BigInt(item.$bigint); if (item !== null && typeof item === "object" && "$bytes" in item && typeof item.$bytes === "string") return fromBytes(item.$bytes); return item; }); }
function bytes(value: Uint8Array): string { let binary = ""; for (const byte of value) binary += String.fromCharCode(byte); return btoa(binary); }
function fromBytes(value: string): Uint8Array { return Uint8Array.from(atob(value), character => character.charCodeAt(0)); }
type Decoder<Value> = (value: unknown) => Value;
const states = ["starting", "running", "suspending", "suspended", "waking", "destroying", "destroyed", "failed", "indeterminate"] as const;
const capabilities = ["elastic-cpu", "elastic-memory", "live-checkpoint", "live-fork", "suspend-resume", "live-movement", "disk-fork"] as const;
function record(value: unknown): Record<string, unknown> { if (value === null || typeof value !== "object" || Array.isArray(value)) throw new TypeError("expected object"); return value as Record<string, unknown>; }
function text(value: unknown, name: string): string { if (typeof value !== "string" || !value) throw new TypeError(`${name} must be a non-empty string`); return value; }
function integer(value: unknown, name: string): number { if (!Number.isSafeInteger(value)) throw new TypeError(`${name} must be a safe integer`); return value as number; }
function positiveInteger(value: unknown, name: string): number { const parsed = integer(value, name); if (parsed <= 0) throw new TypeError(`${name} must be positive`); return parsed; }
function bool(value: unknown, name: string): boolean { if (typeof value !== "boolean") throw new TypeError(`${name} must be boolean`); return value; }
function big(value: unknown, name: string): bigint { if (typeof value !== "bigint") throw new TypeError(`${name} must be bigint`); return value; }
function bytesValue(value: unknown, name: string): Uint8Array { if (!(value instanceof Uint8Array)) throw new TypeError(`${name} must be bytes`); return value; }
function array<Value>(value: unknown, item: Decoder<Value>): readonly Value[] { if (!Array.isArray(value)) throw new TypeError("expected array"); return value.map(item); }
function member<const Value extends readonly string[]>(value: unknown, values: Value, name: string): Value[number] { if (typeof value !== "string" || !values.includes(value)) throw new TypeError(`${name} is invalid`); return value as Value[number]; }
function digestHex(value: unknown, name: string): string { const parsed = text(value, name); if (!/^[0-9a-f]{64}$/.test(parsed) || /^0+$/.test(parsed)) throw new TypeError(`${name} must be a non-zero lowercase SHA-256 digest`); return parsed; }
function unique<Value extends string>(values: readonly Value[], name: string): readonly Value[] { if (new Set(values).size !== values.length) throw new TypeError(`${name} contains duplicates`); return values; }
function sameImage(left: Image, right: Image): boolean { return left.kind === right.kind && (left.kind === "checkpoint" ? right.kind === "checkpoint" && left.checkpointId === right.checkpointId : right.kind !== "checkpoint" && left.digestHex === right.digestHex); }
function inputImage(value: Image): Image { return value.kind === "checkpoint" ? value : { ...value, digestHex: canonicalDigestHex(value.digestHex) }; }
function image(value: unknown): Image { const item = record(value); const kind = member(item.kind, ["managed-oci", "custom", "checkpoint"] as const, "image.kind"); return kind === "checkpoint" ? { kind, checkpointId: text(item.checkpointId, "checkpointId") as CheckpointId } : { kind, digestHex: digestHex(item.digestHex, "digestHex") }; }
function suspension(value: unknown): SuspensionPolicy { const item = record(value); const kind = member(item.kind, ["manual", "after-idle"] as const, "suspension.kind"); return kind === "manual" ? { kind } : { kind, milliseconds: positiveInteger(item.milliseconds, "milliseconds") }; }
function contract(value: unknown) { const item = record(value); const parsedCapabilities = unique(array(item.capabilities, value => member(value, capabilities, "capability")), "capabilities"); const compatibility = record(item.compatibility); const compatibilityKind = member(compatibility.kind, ["best-effort", "require"] as const, "compatibility.kind"); const required = compatibilityKind === "best-effort" ? [] : unique(array(compatibility.capabilities, value => member(value, capabilities, "capability")), "required capabilities"); if ((compatibilityKind === "best-effort" && compatibility.capabilities !== undefined && (!Array.isArray(compatibility.capabilities) || compatibility.capabilities.length !== 0)) || (compatibilityKind === "require" && (required.length === 0 || required.some(value => !parsedCapabilities.includes(value))))) throw new TypeError("compatibility policy is contradictory"); const expiration = record(item.expiration); const expirationKind = member(expiration.kind, ["never", "max-age", "at", "idle"] as const, "expiration.kind"); const spendMicros = big(record(item.budgets).spendMicros, "spendMicros"); const concurrency = integer(record(item.budgets).concurrency, "concurrency"); if (spendMicros < 0n || concurrency < 0) throw new TypeError("budgets cannot be negative"); return { image: image(item.image), compatibility: compatibilityKind === "best-effort" ? { kind: compatibilityKind } : { kind: compatibilityKind, capabilities: required }, performance: member(item.performance, ["elastic", "dedicated"] as const, "performance"), suspension: suspension(item.suspension), expiration: expirationKind === "never" ? { kind: expirationKind } : { kind: expirationKind, milliseconds: positiveInteger(expiration.milliseconds, "expiration.milliseconds") }, networkPolicyDigestHex: digestHex(item.networkPolicyDigestHex, "networkPolicyDigestHex"), budgets: { spendMicros, concurrency }, capabilities: parsedCapabilities, compatibilityRevisionHex: digestHex(item.compatibilityRevisionHex, "compatibilityRevisionHex") } as const; }
function machine(value: unknown): MachineObservation { const item = record(value); const endpoints = array(item.endpoints, value => { const endpoint = record(value); return { name: text(endpoint.name, "endpoint.name"), uri: text(endpoint.uri, "endpoint.uri") }; }); if (new Set(endpoints.map(value => value.name)).size !== endpoints.length) throw new TypeError("machine endpoints contain duplicate names"); const createdAtUnixMs = positiveInteger(item.createdAtUnixMs, "createdAtUnixMs"); const changedAtUnixMs = positiveInteger(item.changedAtUnixMs, "changedAtUnixMs"); if (changedAtUnixMs < createdAtUnixMs) throw new TypeError("machine timestamps are reversed"); return { id: text(item.id, "machine.id") as MachineId, state: member(item.state, states, "machine.state"), contract: contract(item.contract), endpoints, lastCheckpoint: item.lastCheckpoint === null ? null : text(item.lastCheckpoint, "lastCheckpoint") as CheckpointId, createdAtUnixMs, changedAtUnixMs }; }
function machineFor(value: unknown, expected: MachineId): MachineObservation { const parsed = machine(value); if (parsed.id !== expected) throw new TypeError("machine identity was substituted"); return parsed; }
function checkpoint(value: unknown): CheckpointObservation { const item = record(value); return { id: text(item.id, "checkpoint.id") as CheckpointId, source: text(item.source, "checkpoint.source") as MachineId, contract: contract(item.contract), forkable: bool(item.forkable, "forkable"), createdAtUnixMs: positiveInteger(item.createdAtUnixMs, "createdAtUnixMs") }; }
function checkpointFor(value: unknown, expected: CheckpointId): CheckpointObservation { const parsed = checkpoint(value); if (parsed.id !== expected) throw new TypeError("checkpoint identity was substituted"); return parsed; }
function qualification(value: unknown, expected: Image): ImageQualification { const item = record(value); const parsedImage = image(item.image); if (!sameImage(parsedImage, expected)) throw new TypeError("qualified image was substituted"); return { image: parsedImage, capabilities: unique(array(item.capabilities, value => member(value, capabilities, "capability")), "capabilities"), compatibilityRevisionHex: digestHex(item.compatibilityRevisionHex, "compatibilityRevisionHex") }; }
function mutation(value: unknown): MutationOutcome {
  const item = record(value);
  switch (item.kind) {
    case "created": return { kind: item.kind, machine: machine(item.machine) };
    case "checkpointed": return { kind: item.kind, checkpoint: checkpoint(item.checkpoint) };
    case "forked": return { kind: item.kind, machines: array(item.machines, machine) };
    case "machine-forked": {
      const children = array(item.children, machine);
      if (children.length === 0 || new Set(children.map(child => child.id)).size !== children.length) throw new TypeError("fork children are empty or duplicated");
      return { kind: item.kind, source: text(item.source, "source") as MachineId, fidelity: member(item.fidelity, ["memory-and-disk", "disk-only"] as const, "fidelity"), children };
    }
    case "suspended": case "woken": case "machine-destroyed": return { kind: item.kind, machineId: text(item.machineId, "machineId") as MachineId };
    case "suspension-policy-set": return { kind: item.kind, machineId: text(item.machineId, "machineId") as MachineId, policy: suspension(item.policy) };
    case "checkpoint-destroyed": return { kind: item.kind, checkpointId: text(item.checkpointId, "checkpointId") as CheckpointId };
    default: throw new TypeError("mutation kind is invalid");
  }
}
type MutationExpectation =
  | { readonly kind: "created" }
  | { readonly kind: "checkpointed"; readonly source: MachineId }
  | { readonly kind: "forked"; readonly checkpointId: CheckpointId; readonly count: number; readonly performance: Performance }
  | { readonly kind: "machine-forked"; readonly machineId: MachineId; readonly count: number }
  | { readonly kind: "suspended" | "woken" | "machine-destroyed"; readonly machineId: MachineId }
  | { readonly kind: "suspension-policy-set"; readonly machineId: MachineId; readonly policy: SuspensionPolicy }
  | { readonly kind: "checkpoint-destroyed"; readonly checkpointId: CheckpointId };
function sameSuspension(left: SuspensionPolicy, right: SuspensionPolicy): boolean {
  return left.kind === right.kind && (left.kind === "manual" || (right.kind === "after-idle" && left.milliseconds === right.milliseconds));
}
function checkedMutation(outcome: MutationOutcome, expected: MutationExpectation): MutationOutcome {
  if (outcome.kind !== expected.kind) throw new TypeError("machine mutation kind was substituted");
  switch (expected.kind) {
    case "created": return outcome;
    case "checkpointed":
      if (outcome.kind === "checkpointed" && outcome.checkpoint.source === expected.source) return outcome;
      break;
    case "forked":
      if (outcome.kind === "forked" && outcome.machines.length === expected.count && outcome.machines.every(machine =>
        machine.lastCheckpoint === expected.checkpointId && machine.contract.image.kind === "checkpoint" &&
        machine.contract.image.checkpointId === expected.checkpointId && machine.contract.performance === expected.performance)) return outcome;
      break;
    case "machine-forked":
      if (outcome.kind === "machine-forked" && outcome.source === expected.machineId && outcome.children.length === expected.count &&
          outcome.children.every(child => child.id !== expected.machineId &&
            (child.contract.capabilities.includes("live-fork") ? "memory-and-disk" : child.contract.capabilities.includes("disk-fork") ? "disk-only" : null) === outcome.fidelity)) return outcome;
      break;
    case "suspended": case "woken": case "machine-destroyed":
      if ((outcome.kind === "suspended" || outcome.kind === "woken" || outcome.kind === "machine-destroyed") && outcome.machineId === expected.machineId) return outcome;
      break;
    case "suspension-policy-set":
      if (outcome.kind === "suspension-policy-set" && outcome.machineId === expected.machineId && sameSuspension(outcome.policy, expected.policy)) return outcome;
      break;
    case "checkpoint-destroyed":
      if (outcome.kind === "checkpoint-destroyed" && outcome.checkpointId === expected.checkpointId) return outcome;
      break;
  }
  throw new TypeError("machine mutation identity or policy was substituted");
}
function operation(value: unknown): OperationObservation { const item = record(value); return { id: text(item.id, "operation.id") as OperationId, phase: member(item.phase, ["pending", "succeeded", "cancelled", "indeterminate", "failed"] as const, "operation.phase") }; }
function operationFor(value: unknown, expected: OperationId): OperationObservation { const parsed = operation(value); if (parsed.id !== expected) throw new TypeError("operation identity was substituted"); return parsed; }
function event(value: unknown): MachineEvent { const item = record(value); const fact = record(item.fact); const kind = member(fact.kind, ["state", "pressure", "capacity-changed"] as const, "event.fact.kind"); const parsedFact = kind === "state" ? { kind, state: member(fact.state, states, "event.state") } : kind === "pressure" ? { kind, pressure: member(fact.pressure, ["customer-budget", "machine-limit", "service-saturation"] as const, "event.pressure") } : { kind }; return { machine: text(item.machine, "event.machine") as MachineId, sequence: positiveInteger(item.sequence, "event.sequence"), observedAtUnixMs: positiveInteger(item.observedAtUnixMs, "observedAtUnixMs"), fact: parsedFact }; }
function usage(value: unknown): UsageReceipt { const item = record(value); return { machine: text(item.machine, "usage.machine") as MachineId, startUnixMs: integer(item.startUnixMs, "startUnixMs"), endUnixMs: integer(item.endUnixMs, "endUnixMs"), elasticCpuNs: big(item.elasticCpuNs, "elasticCpuNs"), dedicatedCpuNs: big(item.dedicatedCpuNs, "dedicatedCpuNs"), privateResidentByteSeconds: big(item.privateResidentByteSeconds, "privateResidentByteSeconds"), durablePrivateBytes: big(item.durablePrivateBytes, "durablePrivateBytes"), lineageReceiptSha256: bytesValue(item.lineageReceiptSha256, "lineageReceiptSha256"), egressBytes: big(item.egressBytes, "egressBytes"), receipt: bytesValue(item.receipt, "receipt") }; }
function machinePage(value: unknown) { const item = record(value); return { machines: array(item.machines, machine), next: item.next === null ? null : text(item.next, "next") as MachineId }; }
function eventPageFor(value: unknown, expected: MachineId, after: number | null) { const item = record(value); const events = array(item.events, event); let previous = after; for (const item of events) { if (item.machine !== expected || (previous !== null && item.sequence <= previous)) throw new TypeError("event identity or ordering is invalid"); previous = item.sequence; } const nextSequence = item.nextSequence === null ? null : positiveInteger(item.nextSequence, "nextSequence"); if (nextSequence !== null && previous !== nextSequence) throw new TypeError("event continuation does not match the page"); return { events, nextSequence }; }
function usageFor(value: unknown, expected: MachineId, start: number, end: number): UsageReceipt { const parsed = usage(value); if (parsed.machine !== expected || parsed.startUnixMs !== start || parsed.endUnixMs !== end || start >= end) throw new TypeError("usage identity or interval was substituted"); if (parsed.elasticCpuNs < 0n || parsed.dedicatedCpuNs < 0n || parsed.privateResidentByteSeconds < 0n || parsed.durablePrivateBytes < 0n || parsed.egressBytes < 0n || parsed.lineageReceiptSha256.byteLength !== 32) throw new TypeError("usage receipt is malformed"); return parsed; }

/** Shared result validators for the hosted JSON and Rust WASM adapters. */
export const machinesResponseValidation = { qualification, mutation, checkedMutation, machineFor, machinePage, checkpointFor, operationFor, eventPageFor, usageFor } as const;
