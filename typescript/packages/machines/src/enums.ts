import type { DescEnum } from "@bufbuild/protobuf";
import {
  CapabilitySchema, CompatibilityModeSchema, EventKindSchema, ExpirationKindSchema, ImageKindSchema,
  MachineStatusSchema, OperationStatusSchema, PressureKindSchema,
} from "../generated/proto/machines/v1/machines_pb.js";
import type {
  Capability as WireCapability, CompatibilityMode as WireCompatibilityMode,
  EventKind as WireEventKind, ExpirationKind as WireExpirationKind, ImageKind as WireImageKind,
  MachineStatus as WireMachineStatus, OperationStatus as WireOperationStatus,
  PressureKind as WirePressureKind,
} from "../generated/proto/machines/v1/machines_pb.js";

type Kebab<Value extends string> = Value extends `${infer Head}_${infer Tail}`
  ? `${Lowercase<Head>}-${Kebab<Tail>}`
  : Lowercase<Value>;

/** Public spellings are derived from the generated protobuf enum members. */
export type PublicEnum<Wire extends object> = Kebab<Exclude<keyof Wire, "UNSPECIFIED"> & string>;
export type EventKind = PublicEnum<typeof WireEventKind> extends infer Kind extends string
  ? Kind extends "capacity" ? `${Kind}-changed` : Kind
  : never;

type AssertNever<Value extends never> = Value;
/** Members present in only one of two unions; `never` when they match exactly. */
type Mismatch<Actual, Expected> = Exclude<Actual, Expected> | Exclude<Expected, Actual>;
type _ImageKinds = AssertNever<Mismatch<PublicEnum<typeof WireImageKind>, "managed-oci" | "custom" | "checkpoint">>;
type _CompatibilityModes = AssertNever<Mismatch<PublicEnum<typeof WireCompatibilityMode>, "best-effort" | "require">>;
type _ExpirationKinds = AssertNever<Mismatch<PublicEnum<typeof WireExpirationKind>, "never" | "max-age" | "at" | "idle">>;
type _EventKinds = AssertNever<Mismatch<EventKind, "state" | "pressure" | "capacity-changed">>;

function values<Wire extends object>(schema: DescEnum): readonly PublicEnum<Wire>[] {
  return schema.values
    .filter((value) => value.number !== 0)
    .map((value) => value.localName.replaceAll("_", "-").toLowerCase()) as PublicEnum<Wire>[];
}

export const imageKinds = values<typeof WireImageKind>(ImageKindSchema);
export const capabilities = values<typeof WireCapability>(CapabilitySchema);
export const compatibilityModes = values<typeof WireCompatibilityMode>(CompatibilityModeSchema);
export const expirationKinds = values<typeof WireExpirationKind>(ExpirationKindSchema);
export const operationPhases = values<typeof WireOperationStatus>(OperationStatusSchema);
export const machineStates = values<typeof WireMachineStatus>(MachineStatusSchema);
export const pressures = values<typeof WirePressureKind>(PressureKindSchema);
export const eventKinds = values<typeof WireEventKind>(EventKindSchema)
  .map((kind) => kind === "capacity" ? "capacity-changed" : kind) as readonly EventKind[];
