/** Version-pinned, ref-only extension state shared with the Rust reducer. */
import type { FileRef } from "./conversation.js";
import type { Authority, Command, EventReference, OperationId } from "./index.js";
import type {
  ContentBindings,
  HarnessExecutionProvider,
  HarnessRuntimeHost,
  HarnessRuntimeSpawner,
  HarnessRuntimeState,
  InteractionHandler,
} from "./runtime.js";
import { NativeContracts } from "./native-contracts.js";

export type ExtensionForkPolicy = "inherit" | "reset" | "reject";

/** Exact installed revision selected for new admissions or required by another extension. */
export interface ExtensionDependency {
  readonly name: string;
  readonly version: number;
}

/** Exact process-local executable identity, matching Rust's `ExtensionIdentity`.
 *
 * This is deliberately separate from the event-sourced dependency. The
 * implementation digest is what prevents a host from linking different code
 * under an already-admitted logical extension/version.
 */
export interface ExtensionIdentity extends ExtensionDependency {
  readonly digest: readonly number[];
}

/** Validates an executable identity through the native dependency contract. */
export async function extensionIdentity(value: ExtensionIdentity): Promise<ExtensionIdentity> {
  // The durable dependency contract intentionally has no executable digest;
  // validate only its exact logical identity before checking native bytes.
  const dependency = await extensionDependency({ name: value.name, version: value.version });
  if (!Array.isArray(value.digest) || value.digest.length !== 32
    || value.digest.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)
    || value.digest.every(byte => byte === 0)) {
    throw new TypeError("extension implementation digest must be a nonzero 32-byte value");
  }
  return Object.freeze({ ...dependency, digest: Object.freeze([...value.digest]) });
}

/**
 * The provider-neutral linker surface. Native implementations are linked by
 * Rust; TypeScript hosts only carry this boundary and never reimplement the
 * registries or reducer. Existing bindings remain authoritative: each slot
 * may be filled only when it is absent.
 */
export interface ExtensionLinker {
  readonly tasks: unknown;
  readonly tools: unknown;
  readonly resumableTools: unknown;
  durableHost(host: HarnessRuntimeHost): void | Promise<void>;
  state(state: HarnessRuntimeState): void | Promise<void>;
  spawner(spawner: HarnessRuntimeSpawner): void | Promise<void>;
  execution(execution: HarnessExecutionProvider): void | Promise<void>;
  interactions(interactions: InteractionHandler): void | Promise<void>;
  interactionResolver(resolver: unknown): void | Promise<void>;
  content(content: ContentBindings): void | Promise<void>;
  artifacts(artifacts: ContentBindings): void | Promise<void>;
  scope(): unknown;
}

/** Process-local implementation loaded by a host, not durable event state. */
export interface NativeExtension {
  identity(): ExtensionIdentity;
  dependencies?(): readonly ExtensionDependency[];
  link?(linker: ExtensionLinker): void | Promise<void>;
  /** Called after the registry has removed the final retained lease. */
  dispose?(): void | Promise<void>;
}

/** A retained exact implementation lease. Hosts must release it explicitly. */
export interface ExtensionLease {
  identity(): ExtensionIdentity;
  dispose(): void | Promise<void>;
}

/** Leases held by one admitted task or operation. */
export interface ExtensionLeases {
  identities(): readonly ExtensionIdentity[];
  dispose(): void | Promise<void>;
}

/** Exact-version, dependency-aware registry owned by the native host. */
export interface ExtensionRegistry {
  install(extension: NativeExtension): void | Promise<void>;
  /** Natural registration spelling for composition builders. */
  register(extension: NativeExtension): void | Promise<void>;
  disable(name: string): void | Promise<void>;
  pin(name: string): ExtensionLease | Promise<ExtensionLease>;
  pinExact(identity: ExtensionIdentity): ExtensionLease | Promise<ExtensionLease>;
  remove(identity: ExtensionIdentity): void | Promise<void>;
  contains(identity: ExtensionIdentity): boolean | Promise<boolean>;
  accepting(name: string): boolean | Promise<boolean>;
  current(name: string): ExtensionIdentity | null | Promise<ExtensionIdentity | null>;
  exact(name: string, version: number): ExtensionIdentity | null | Promise<ExtensionIdentity | null>;
}

/** A linked composition with version-pinned executable implementations. */
export interface ExtensionRuntime {
  selected(): readonly ExtensionIdentity[];
  disable(): void | Promise<void>;
  acceptsNewAdmissions(): boolean;
  linkInto(bindings: ExtensionLinker): void | Promise<void>;
  retainForTask(requirements: Iterable<string>): ExtensionLeases | Promise<ExtensionLeases>;
  retainForExisting(requirements: Iterable<string>): ExtensionLeases | Promise<ExtensionLeases>;
  validateAdmission(admission: ExtensionAdmission | null): void | Promise<void>;
}

/** Builder-owned bundle retaining its runtime for the lifetime of a harness. */
export interface NativeExtensionBundle {
  runtime(): ExtensionRuntime;
  linkInto(bindings: ExtensionLinker): void | Promise<void>;
}

/** Host operations needed to construct a linked runtime without a TS registry. */
export interface ExtensionLifecycleHost {
  activate(registry: ExtensionRegistry, roots: readonly ExtensionIdentity[]): ExtensionRuntime | Promise<ExtensionRuntime>;
  fromAdmission(registry: ExtensionRegistry, admission: ExtensionAdmission): ExtensionRuntime | Promise<ExtensionRuntime>;
  fromReducer?(registry: ExtensionRegistry, reducer: unknown): ExtensionRuntime | Promise<ExtensionRuntime>;
  bundle?(runtime: ExtensionRuntime): NativeExtensionBundle;
}

/** Validated, immutable configuration revision for one installed version. */
export interface ExtensionConfiguration {
  readonly extension: ExtensionDependency;
  readonly schema_digest: readonly number[];
  readonly content: FileRef;
}

/** Committed selection snapshot retained by a task across later activations. */
export interface ExtensionAdmission {
  readonly source: Readonly<{ authority: Authority<"agent">; revision: bigint }>;
  readonly selected: readonly ExtensionDependency[];
  readonly configurations: readonly ExtensionConfiguration[];
}

/** Exact source event retained by an extension state migration. */
export type ExtensionStateReference = Readonly<Pick<EventReference, "authority" | "revision">>;

export type ConfigureExtensionAction = Readonly<{
  kind: "configure_extension";
  extension: ExtensionDependency;
  content: FileRef;
}>;

/** Agent-owned event-sourced selection; old operations retain their admitted version. */
export type SelectExtensionsAction = Readonly<{
  kind: "select_extensions";
  roots: readonly ExtensionDependency[];
}>;

/** Extension command shapes; migration publication additionally requires the native content-admission host. */
export type ExtensionAction =
  | ConfigureExtensionAction
  | SelectExtensionsAction
  | MigrateExtensionStateAction;

/** Complete extension command envelope. Fresh migrations are not executable by the bare reducer. */
export type ExtensionCommand<Action extends ExtensionAction = ExtensionAction> = Command<Action>;

export interface ExtensionRecord {
  readonly name: string;
  readonly version: number;
  readonly schema_digest: readonly number[];
  readonly implementation_digest: readonly number[];
  readonly fork_policy: ExtensionForkPolicy;
  readonly content: FileRef;
}

/** Retained source and new state revision; admission verifies the exact latest source. */
export interface ExtensionStateMigration {
  readonly previous: ExtensionStateReference;
  readonly record: ExtensionRecord;
}

export type MigrateExtensionStateAction = Readonly<{
  kind: "migrate_extension_state";
  name: string;
  previous: ExtensionStateReference;
  to_version: number;
  content: FileRef;
}>;

/** Caller-retained identity used by a host that executes a migration before applying it. */
export interface ExtensionMigrationRequest {
  readonly operation_id: OperationId;
  readonly expected_revision: bigint;
  readonly previous: ExtensionStateReference;
  readonly name: string;
  readonly to_version: number;
}

export async function extensionRecord(value: ExtensionRecord): Promise<ExtensionRecord> {
  return (await NativeContracts.create()).validate("extension_record", value);
}

export async function extensionStateMigration(value: ExtensionStateMigration): Promise<ExtensionStateMigration> {
  return (await NativeContracts.create()).validate("extension_state_migration", value);
}

export async function extensionDependency(value: ExtensionDependency): Promise<ExtensionDependency> {
  return (await NativeContracts.create()).validate("extension_dependency", value);
}

export async function extensionConfiguration(value: ExtensionConfiguration): Promise<ExtensionConfiguration> {
  return (await NativeContracts.create()).validate("extension_configuration", value);
}

export async function extensionAdmission(value: ExtensionAdmission): Promise<ExtensionAdmission> {
  return (await NativeContracts.create()).validate("extension_admission", value);
}
