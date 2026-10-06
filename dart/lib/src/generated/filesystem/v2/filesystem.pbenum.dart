// This is a generated file - do not edit.
//
// Generated from filesystem/v2/filesystem.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class WorkspaceContextState extends $pb.ProtobufEnum {
  static const WorkspaceContextState WORKSPACE_CONTEXT_STATE_UNSPECIFIED =
      WorkspaceContextState._(
          0, _omitEnumNames ? '' : 'WORKSPACE_CONTEXT_STATE_UNSPECIFIED');
  static const WorkspaceContextState WORKSPACE_CONTEXT_STATE_ACTIVE =
      WorkspaceContextState._(
          1, _omitEnumNames ? '' : 'WORKSPACE_CONTEXT_STATE_ACTIVE');
  static const WorkspaceContextState WORKSPACE_CONTEXT_STATE_FROZEN =
      WorkspaceContextState._(
          2, _omitEnumNames ? '' : 'WORKSPACE_CONTEXT_STATE_FROZEN');
  static const WorkspaceContextState WORKSPACE_CONTEXT_STATE_DISCARDED =
      WorkspaceContextState._(
          3, _omitEnumNames ? '' : 'WORKSPACE_CONTEXT_STATE_DISCARDED');

  static const $core.List<WorkspaceContextState> values =
      <WorkspaceContextState>[
    WORKSPACE_CONTEXT_STATE_UNSPECIFIED,
    WORKSPACE_CONTEXT_STATE_ACTIVE,
    WORKSPACE_CONTEXT_STATE_FROZEN,
    WORKSPACE_CONTEXT_STATE_DISCARDED,
  ];

  static final $core.List<WorkspaceContextState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static WorkspaceContextState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const WorkspaceContextState._(super.value, super.name);
}

class FilesystemProfile extends $pb.ProtobufEnum {
  static const FilesystemProfile FILESYSTEM_PROFILE_UNSPECIFIED =
      FilesystemProfile._(
          0, _omitEnumNames ? '' : 'FILESYSTEM_PROFILE_UNSPECIFIED');
  static const FilesystemProfile FILESYSTEM_PROFILE_PORTABLE =
      FilesystemProfile._(
          1, _omitEnumNames ? '' : 'FILESYSTEM_PROFILE_PORTABLE');
  static const FilesystemProfile FILESYSTEM_PROFILE_POSIX =
      FilesystemProfile._(2, _omitEnumNames ? '' : 'FILESYSTEM_PROFILE_POSIX');
  static const FilesystemProfile FILESYSTEM_PROFILE_WINDOWS =
      FilesystemProfile._(
          3, _omitEnumNames ? '' : 'FILESYSTEM_PROFILE_WINDOWS');
  static const FilesystemProfile FILESYSTEM_PROFILE_BROWSER =
      FilesystemProfile._(
          4, _omitEnumNames ? '' : 'FILESYSTEM_PROFILE_BROWSER');

  static const $core.List<FilesystemProfile> values = <FilesystemProfile>[
    FILESYSTEM_PROFILE_UNSPECIFIED,
    FILESYSTEM_PROFILE_PORTABLE,
    FILESYSTEM_PROFILE_POSIX,
    FILESYSTEM_PROFILE_WINDOWS,
    FILESYSTEM_PROFILE_BROWSER,
  ];

  static final $core.List<FilesystemProfile?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 4);
  static FilesystemProfile? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const FilesystemProfile._(super.value, super.name);
}

class FileKind extends $pb.ProtobufEnum {
  static const FileKind FILE_KIND_UNSPECIFIED =
      FileKind._(0, _omitEnumNames ? '' : 'FILE_KIND_UNSPECIFIED');
  static const FileKind FILE_KIND_REGULAR =
      FileKind._(1, _omitEnumNames ? '' : 'FILE_KIND_REGULAR');
  static const FileKind FILE_KIND_DIRECTORY =
      FileKind._(2, _omitEnumNames ? '' : 'FILE_KIND_DIRECTORY');
  static const FileKind FILE_KIND_SYMBOLIC_LINK =
      FileKind._(3, _omitEnumNames ? '' : 'FILE_KIND_SYMBOLIC_LINK');
  static const FileKind FILE_KIND_FIFO =
      FileKind._(4, _omitEnumNames ? '' : 'FILE_KIND_FIFO');
  static const FileKind FILE_KIND_SOCKET =
      FileKind._(5, _omitEnumNames ? '' : 'FILE_KIND_SOCKET');
  static const FileKind FILE_KIND_CHARACTER_DEVICE =
      FileKind._(6, _omitEnumNames ? '' : 'FILE_KIND_CHARACTER_DEVICE');
  static const FileKind FILE_KIND_BLOCK_DEVICE =
      FileKind._(7, _omitEnumNames ? '' : 'FILE_KIND_BLOCK_DEVICE');
  static const FileKind FILE_KIND_REPARSE_POINT =
      FileKind._(8, _omitEnumNames ? '' : 'FILE_KIND_REPARSE_POINT');
  static const FileKind FILE_KIND_MOUNT_BOUNDARY =
      FileKind._(9, _omitEnumNames ? '' : 'FILE_KIND_MOUNT_BOUNDARY');

  static const $core.List<FileKind> values = <FileKind>[
    FILE_KIND_UNSPECIFIED,
    FILE_KIND_REGULAR,
    FILE_KIND_DIRECTORY,
    FILE_KIND_SYMBOLIC_LINK,
    FILE_KIND_FIFO,
    FILE_KIND_SOCKET,
    FILE_KIND_CHARACTER_DEVICE,
    FILE_KIND_BLOCK_DEVICE,
    FILE_KIND_REPARSE_POINT,
    FILE_KIND_MOUNT_BOUNDARY,
  ];

  static final $core.List<FileKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 9);
  static FileKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const FileKind._(super.value, super.name);
}

class MutationStatus extends $pb.ProtobufEnum {
  static const MutationStatus MUTATION_STATUS_UNSPECIFIED =
      MutationStatus._(0, _omitEnumNames ? '' : 'MUTATION_STATUS_UNSPECIFIED');
  static const MutationStatus MUTATION_STATUS_COMMITTED =
      MutationStatus._(1, _omitEnumNames ? '' : 'MUTATION_STATUS_COMMITTED');
  static const MutationStatus MUTATION_STATUS_ALREADY_COMMITTED =
      MutationStatus._(
          2, _omitEnumNames ? '' : 'MUTATION_STATUS_ALREADY_COMMITTED');
  static const MutationStatus MUTATION_STATUS_CONFLICT =
      MutationStatus._(3, _omitEnumNames ? '' : 'MUTATION_STATUS_CONFLICT');
  static const MutationStatus MUTATION_STATUS_FENCED =
      MutationStatus._(4, _omitEnumNames ? '' : 'MUTATION_STATUS_FENCED');
  static const MutationStatus MUTATION_STATUS_IDEMPOTENCY_CONFLICT =
      MutationStatus._(
          5, _omitEnumNames ? '' : 'MUTATION_STATUS_IDEMPOTENCY_CONFLICT');
  static const MutationStatus MUTATION_STATUS_INDETERMINATE = MutationStatus._(
      6, _omitEnumNames ? '' : 'MUTATION_STATUS_INDETERMINATE');

  static const $core.List<MutationStatus> values = <MutationStatus>[
    MUTATION_STATUS_UNSPECIFIED,
    MUTATION_STATUS_COMMITTED,
    MUTATION_STATUS_ALREADY_COMMITTED,
    MUTATION_STATUS_CONFLICT,
    MUTATION_STATUS_FENCED,
    MUTATION_STATUS_IDEMPOTENCY_CONFLICT,
    MUTATION_STATUS_INDETERMINATE,
  ];

  static final $core.List<MutationStatus?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 6);
  static MutationStatus? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const MutationStatus._(super.value, super.name);
}

class JoinHistory extends $pb.ProtobufEnum {
  static const JoinHistory JOIN_HISTORY_UNSPECIFIED =
      JoinHistory._(0, _omitEnumNames ? '' : 'JOIN_HISTORY_UNSPECIFIED');
  static const JoinHistory JOIN_HISTORY_MERGE =
      JoinHistory._(1, _omitEnumNames ? '' : 'JOIN_HISTORY_MERGE');
  static const JoinHistory JOIN_HISTORY_REBASE =
      JoinHistory._(2, _omitEnumNames ? '' : 'JOIN_HISTORY_REBASE');
  static const JoinHistory JOIN_HISTORY_SQUASH =
      JoinHistory._(3, _omitEnumNames ? '' : 'JOIN_HISTORY_SQUASH');
  static const JoinHistory JOIN_HISTORY_CHERRY_PICK =
      JoinHistory._(4, _omitEnumNames ? '' : 'JOIN_HISTORY_CHERRY_PICK');

  static const $core.List<JoinHistory> values = <JoinHistory>[
    JOIN_HISTORY_UNSPECIFIED,
    JOIN_HISTORY_MERGE,
    JOIN_HISTORY_REBASE,
    JOIN_HISTORY_SQUASH,
    JOIN_HISTORY_CHERRY_PICK,
  ];

  static final $core.List<JoinHistory?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 4);
  static JoinHistory? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const JoinHistory._(super.value, super.name);
}

class ConflictUse extends $pb.ProtobufEnum {
  static const ConflictUse CONFLICT_USE_UNSPECIFIED =
      ConflictUse._(0, _omitEnumNames ? '' : 'CONFLICT_USE_UNSPECIFIED');
  static const ConflictUse CONFLICT_USE_OBSERVATION =
      ConflictUse._(1, _omitEnumNames ? '' : 'CONFLICT_USE_OBSERVATION');
  static const ConflictUse CONFLICT_USE_MUTATION =
      ConflictUse._(2, _omitEnumNames ? '' : 'CONFLICT_USE_MUTATION');
  static const ConflictUse CONFLICT_USE_OBSERVATION_AND_MUTATION =
      ConflictUse._(
          3, _omitEnumNames ? '' : 'CONFLICT_USE_OBSERVATION_AND_MUTATION');

  static const $core.List<ConflictUse> values = <ConflictUse>[
    CONFLICT_USE_UNSPECIFIED,
    CONFLICT_USE_OBSERVATION,
    CONFLICT_USE_MUTATION,
    CONFLICT_USE_OBSERVATION_AND_MUTATION,
  ];

  static final $core.List<ConflictUse?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static ConflictUse? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ConflictUse._(super.value, super.name);
}

class SparseTarget extends $pb.ProtobufEnum {
  static const SparseTarget SPARSE_TARGET_UNSPECIFIED =
      SparseTarget._(0, _omitEnumNames ? '' : 'SPARSE_TARGET_UNSPECIFIED');
  static const SparseTarget SPARSE_TARGET_DATA =
      SparseTarget._(1, _omitEnumNames ? '' : 'SPARSE_TARGET_DATA');
  static const SparseTarget SPARSE_TARGET_HOLE =
      SparseTarget._(2, _omitEnumNames ? '' : 'SPARSE_TARGET_HOLE');

  static const $core.List<SparseTarget> values = <SparseTarget>[
    SPARSE_TARGET_UNSPECIFIED,
    SPARSE_TARGET_DATA,
    SPARSE_TARGET_HOLE,
  ];

  static final $core.List<SparseTarget?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static SparseTarget? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const SparseTarget._(super.value, super.name);
}

class ExtentKind extends $pb.ProtobufEnum {
  static const ExtentKind EXTENT_KIND_UNSPECIFIED =
      ExtentKind._(0, _omitEnumNames ? '' : 'EXTENT_KIND_UNSPECIFIED');
  static const ExtentKind EXTENT_KIND_HOLE =
      ExtentKind._(1, _omitEnumNames ? '' : 'EXTENT_KIND_HOLE');
  static const ExtentKind EXTENT_KIND_ALLOCATED_ZERO =
      ExtentKind._(2, _omitEnumNames ? '' : 'EXTENT_KIND_ALLOCATED_ZERO');
  static const ExtentKind EXTENT_KIND_CONTENT =
      ExtentKind._(3, _omitEnumNames ? '' : 'EXTENT_KIND_CONTENT');

  static const $core.List<ExtentKind> values = <ExtentKind>[
    EXTENT_KIND_UNSPECIFIED,
    EXTENT_KIND_HOLE,
    EXTENT_KIND_ALLOCATED_ZERO,
    EXTENT_KIND_CONTENT,
  ];

  static final $core.List<ExtentKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static ExtentKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ExtentKind._(super.value, super.name);
}

class SourceState extends $pb.ProtobufEnum {
  static const SourceState SOURCE_STATE_UNSPECIFIED =
      SourceState._(0, _omitEnumNames ? '' : 'SOURCE_STATE_UNSPECIFIED');
  static const SourceState SOURCE_STATE_CLEAN =
      SourceState._(1, _omitEnumNames ? '' : 'SOURCE_STATE_CLEAN');
  static const SourceState SOURCE_STATE_PENDING_CAPTURE =
      SourceState._(2, _omitEnumNames ? '' : 'SOURCE_STATE_PENDING_CAPTURE');
  static const SourceState SOURCE_STATE_NEEDS_RESCAN =
      SourceState._(3, _omitEnumNames ? '' : 'SOURCE_STATE_NEEDS_RESCAN');
  static const SourceState SOURCE_STATE_CONFLICT =
      SourceState._(4, _omitEnumNames ? '' : 'SOURCE_STATE_CONFLICT');
  static const SourceState SOURCE_STATE_SEALED =
      SourceState._(5, _omitEnumNames ? '' : 'SOURCE_STATE_SEALED');

  static const $core.List<SourceState> values = <SourceState>[
    SOURCE_STATE_UNSPECIFIED,
    SOURCE_STATE_CLEAN,
    SOURCE_STATE_PENDING_CAPTURE,
    SOURCE_STATE_NEEDS_RESCAN,
    SOURCE_STATE_CONFLICT,
    SOURCE_STATE_SEALED,
  ];

  static final $core.List<SourceState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static SourceState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const SourceState._(super.value, super.name);
}

class SourceInvalidationReason extends $pb.ProtobufEnum {
  static const SourceInvalidationReason SOURCE_INVALIDATION_REASON_UNSPECIFIED =
      SourceInvalidationReason._(
          0, _omitEnumNames ? '' : 'SOURCE_INVALIDATION_REASON_UNSPECIFIED');
  static const SourceInvalidationReason
      SOURCE_INVALIDATION_REASON_INITIAL_SNAPSHOT_REQUIRED =
      SourceInvalidationReason._(
          1,
          _omitEnumNames
              ? ''
              : 'SOURCE_INVALIDATION_REASON_INITIAL_SNAPSHOT_REQUIRED');
  static const SourceInvalidationReason
      SOURCE_INVALIDATION_REASON_QUEUE_OVERFLOW = SourceInvalidationReason._(
          2, _omitEnumNames ? '' : 'SOURCE_INVALIDATION_REASON_QUEUE_OVERFLOW');
  static const SourceInvalidationReason
      SOURCE_INVALIDATION_REASON_NATIVE_RESCAN_REQUIRED =
      SourceInvalidationReason._(
          3,
          _omitEnumNames
              ? ''
              : 'SOURCE_INVALIDATION_REASON_NATIVE_RESCAN_REQUIRED');
  static const SourceInvalidationReason
      SOURCE_INVALIDATION_REASON_BACKEND_ERROR = SourceInvalidationReason._(
          4, _omitEnumNames ? '' : 'SOURCE_INVALIDATION_REASON_BACKEND_ERROR');
  static const SourceInvalidationReason
      SOURCE_INVALIDATION_REASON_UNREPRESENTABLE_PATH =
      SourceInvalidationReason._(
          5,
          _omitEnumNames
              ? ''
              : 'SOURCE_INVALIDATION_REASON_UNREPRESENTABLE_PATH');
  static const SourceInvalidationReason
      SOURCE_INVALIDATION_REASON_AMBIGUOUS_RENAME = SourceInvalidationReason._(
          6,
          _omitEnumNames ? '' : 'SOURCE_INVALIDATION_REASON_AMBIGUOUS_RENAME');
  static const SourceInvalidationReason
      SOURCE_INVALIDATION_REASON_ROOT_CHANGED = SourceInvalidationReason._(
          7, _omitEnumNames ? '' : 'SOURCE_INVALIDATION_REASON_ROOT_CHANGED');

  static const $core.List<SourceInvalidationReason> values =
      <SourceInvalidationReason>[
    SOURCE_INVALIDATION_REASON_UNSPECIFIED,
    SOURCE_INVALIDATION_REASON_INITIAL_SNAPSHOT_REQUIRED,
    SOURCE_INVALIDATION_REASON_QUEUE_OVERFLOW,
    SOURCE_INVALIDATION_REASON_NATIVE_RESCAN_REQUIRED,
    SOURCE_INVALIDATION_REASON_BACKEND_ERROR,
    SOURCE_INVALIDATION_REASON_UNREPRESENTABLE_PATH,
    SOURCE_INVALIDATION_REASON_AMBIGUOUS_RENAME,
    SOURCE_INVALIDATION_REASON_ROOT_CHANGED,
  ];

  static final $core.List<SourceInvalidationReason?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 7);
  static SourceInvalidationReason? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const SourceInvalidationReason._(super.value, super.name);
}

class NameEncoding extends $pb.ProtobufEnum {
  static const NameEncoding NAME_ENCODING_UNSPECIFIED =
      NameEncoding._(0, _omitEnumNames ? '' : 'NAME_ENCODING_UNSPECIFIED');
  static const NameEncoding NAME_ENCODING_UTF8 =
      NameEncoding._(1, _omitEnumNames ? '' : 'NAME_ENCODING_UTF8');
  static const NameEncoding NAME_ENCODING_POSIX_BYTES =
      NameEncoding._(2, _omitEnumNames ? '' : 'NAME_ENCODING_POSIX_BYTES');
  static const NameEncoding NAME_ENCODING_WINDOWS_UTF16LE =
      NameEncoding._(3, _omitEnumNames ? '' : 'NAME_ENCODING_WINDOWS_UTF16LE');

  static const $core.List<NameEncoding> values = <NameEncoding>[
    NAME_ENCODING_UNSPECIFIED,
    NAME_ENCODING_UTF8,
    NAME_ENCODING_POSIX_BYTES,
    NAME_ENCODING_WINDOWS_UTF16LE,
  ];

  static final $core.List<NameEncoding?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static NameEncoding? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const NameEncoding._(super.value, super.name);
}

class RebaseStatus extends $pb.ProtobufEnum {
  static const RebaseStatus REBASE_STATUS_UNSPECIFIED =
      RebaseStatus._(0, _omitEnumNames ? '' : 'REBASE_STATUS_UNSPECIFIED');
  static const RebaseStatus REBASE_STATUS_REBASED =
      RebaseStatus._(1, _omitEnumNames ? '' : 'REBASE_STATUS_REBASED');
  static const RebaseStatus REBASE_STATUS_ALREADY_REBASED =
      RebaseStatus._(2, _omitEnumNames ? '' : 'REBASE_STATUS_ALREADY_REBASED');
  static const RebaseStatus REBASE_STATUS_CURRENT =
      RebaseStatus._(3, _omitEnumNames ? '' : 'REBASE_STATUS_CURRENT');
  static const RebaseStatus REBASE_STATUS_STALE =
      RebaseStatus._(4, _omitEnumNames ? '' : 'REBASE_STATUS_STALE');
  static const RebaseStatus REBASE_STATUS_CONFLICTED =
      RebaseStatus._(5, _omitEnumNames ? '' : 'REBASE_STATUS_CONFLICTED');
  static const RebaseStatus REBASE_STATUS_FENCED =
      RebaseStatus._(6, _omitEnumNames ? '' : 'REBASE_STATUS_FENCED');
  static const RebaseStatus REBASE_STATUS_IDEMPOTENCY_CONFLICT = RebaseStatus._(
      7, _omitEnumNames ? '' : 'REBASE_STATUS_IDEMPOTENCY_CONFLICT');

  static const $core.List<RebaseStatus> values = <RebaseStatus>[
    REBASE_STATUS_UNSPECIFIED,
    REBASE_STATUS_REBASED,
    REBASE_STATUS_ALREADY_REBASED,
    REBASE_STATUS_CURRENT,
    REBASE_STATUS_STALE,
    REBASE_STATUS_CONFLICTED,
    REBASE_STATUS_FENCED,
    REBASE_STATUS_IDEMPOTENCY_CONFLICT,
  ];

  static final $core.List<RebaseStatus?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 7);
  static RebaseStatus? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const RebaseStatus._(super.value, super.name);
}

class JoinStatus extends $pb.ProtobufEnum {
  static const JoinStatus JOIN_STATUS_UNSPECIFIED =
      JoinStatus._(0, _omitEnumNames ? '' : 'JOIN_STATUS_UNSPECIFIED');
  static const JoinStatus JOIN_STATUS_APPLIED =
      JoinStatus._(1, _omitEnumNames ? '' : 'JOIN_STATUS_APPLIED');
  static const JoinStatus JOIN_STATUS_ALREADY_APPLIED =
      JoinStatus._(2, _omitEnumNames ? '' : 'JOIN_STATUS_ALREADY_APPLIED');
  static const JoinStatus JOIN_STATUS_NO_CHANGES =
      JoinStatus._(3, _omitEnumNames ? '' : 'JOIN_STATUS_NO_CHANGES');
  static const JoinStatus JOIN_STATUS_STALE_TARGET =
      JoinStatus._(4, _omitEnumNames ? '' : 'JOIN_STATUS_STALE_TARGET');
  static const JoinStatus JOIN_STATUS_CONFLICTED =
      JoinStatus._(5, _omitEnumNames ? '' : 'JOIN_STATUS_CONFLICTED');
  static const JoinStatus JOIN_STATUS_FENCED =
      JoinStatus._(6, _omitEnumNames ? '' : 'JOIN_STATUS_FENCED');
  static const JoinStatus JOIN_STATUS_IDEMPOTENCY_CONFLICT =
      JoinStatus._(7, _omitEnumNames ? '' : 'JOIN_STATUS_IDEMPOTENCY_CONFLICT');

  static const $core.List<JoinStatus> values = <JoinStatus>[
    JOIN_STATUS_UNSPECIFIED,
    JOIN_STATUS_APPLIED,
    JOIN_STATUS_ALREADY_APPLIED,
    JOIN_STATUS_NO_CHANGES,
    JOIN_STATUS_STALE_TARGET,
    JOIN_STATUS_CONFLICTED,
    JOIN_STATUS_FENCED,
    JOIN_STATUS_IDEMPOTENCY_CONFLICT,
  ];

  static final $core.List<JoinStatus?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 7);
  static JoinStatus? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const JoinStatus._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
