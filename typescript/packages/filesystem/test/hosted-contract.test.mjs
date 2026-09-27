import { expect, test } from "bun:test";
import {
  ConflictUse,
  ExtentKind,
  FileKind,
  FilesystemProfile,
  JoinHistory,
  JoinStatus,
  MutationStatus,
  NameEncoding,
  RebaseStatus,
  SourceInvalidationReason,
  SourceState,
  SparseTarget,
} from "../generated/proto/filesystem/v2/filesystem_pb.js";
import {
  CONFLICT_USE_TO_USAGE,
  EXTENT_KIND_TO_KIND,
  FILE_KIND_TO_KIND,
  FILESYSTEM_PROFILE_TO_PROFILE,
  JOIN_HISTORY_FROM_PUBLIC,
  JOIN_HISTORY_TO_PUBLIC,
  JOIN_STATUS_TO_STATUS,
  MUTATION_STATUS_TO_COMMIT,
  MUTATION_STATUS_TO_DELETE,
  NAME_ENCODING_TO_PUBLIC,
  REBASE_STATUS_TO_STATUS,
  SOURCE_INVALIDATION_REASON_TO_REASON,
  SOURCE_STATE_TO_STATUS,
  SPARSE_TARGET_TO_TARGET,
} from "../generated/hosted-contract.js";
import { DEFAULT_HOSTED_OPTIONS } from "../generated/defaults.js";

const numericEnumValues = enumObject => Object.values(enumObject).filter(value => typeof value === "number");
const assertComplete = (enumObject, mapping) => {
  for (const value of numericEnumValues(enumObject)) expect(mapping).toHaveProperty(String(value));
};

test("hosted mappings cover every protobuf enum value", () => {
  assertComplete(FilesystemProfile, FILESYSTEM_PROFILE_TO_PROFILE);
  assertComplete(FileKind, FILE_KIND_TO_KIND);
  assertComplete(MutationStatus, MUTATION_STATUS_TO_COMMIT);
  assertComplete(MutationStatus, MUTATION_STATUS_TO_DELETE);
  assertComplete(JoinHistory, JOIN_HISTORY_TO_PUBLIC);
  assertComplete(ConflictUse, CONFLICT_USE_TO_USAGE);
  assertComplete(SparseTarget, SPARSE_TARGET_TO_TARGET);
  assertComplete(ExtentKind, EXTENT_KIND_TO_KIND);
  assertComplete(SourceState, SOURCE_STATE_TO_STATUS);
  assertComplete(SourceInvalidationReason, SOURCE_INVALIDATION_REASON_TO_REASON);
  assertComplete(NameEncoding, NAME_ENCODING_TO_PUBLIC);
  assertComplete(RebaseStatus, REBASE_STATUS_TO_STATUS);
  assertComplete(JoinStatus, JOIN_STATUS_TO_STATUS);
  expect(MUTATION_STATUS_TO_DELETE[MutationStatus.COMMITTED]).toBe("deleted");
  expect(MUTATION_STATUS_TO_DELETE[MutationStatus.ALREADY_COMMITTED]).toBe("already-deleted");
  expect(MUTATION_STATUS_TO_COMMIT[MutationStatus.INDETERMINATE]).toBeUndefined();
  expect(SOURCE_STATE_TO_STATUS[SourceState.NEEDS_RESCAN]).toBe("needs-rescan");
  expect(SOURCE_INVALIDATION_REASON_TO_REASON[SourceInvalidationReason.ROOT_CHANGED]).toBe("root-changed");
  for (const value of ["merge", "rebase", "squash", "cherry-pick"]) {
    expect(JOIN_HISTORY_FROM_PUBLIC).toHaveProperty(value);
  }
  expect(JOIN_HISTORY_FROM_PUBLIC.merge).toBe(JoinHistory.MERGE);
  expect(JOIN_HISTORY_FROM_PUBLIC["cherry-pick"]).toBe(JoinHistory.CHERRY_PICK);
  expect(JOIN_HISTORY_FROM_PUBLIC["toString"]).toBeUndefined();
  expect(JOIN_HISTORY_FROM_PUBLIC["__proto__"]).toBeUndefined();
  expect(Object.getPrototypeOf(JOIN_HISTORY_FROM_PUBLIC)).toBeNull();
});

test("hosted transaction conflict default follows Rust hosted page default", () => {
  expect(DEFAULT_HOSTED_OPTIONS.maximumPageItems).toBe(1024);
});
