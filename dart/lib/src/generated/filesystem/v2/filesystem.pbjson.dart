// This is a generated file - do not edit.
//
// Generated from filesystem/v2/filesystem.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports
// ignore_for_file: unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use workspaceContextStateDescriptor instead')
const WorkspaceContextState$json = {
  '1': 'WorkspaceContextState',
  '2': [
    {'1': 'WORKSPACE_CONTEXT_STATE_UNSPECIFIED', '2': 0},
    {'1': 'WORKSPACE_CONTEXT_STATE_ACTIVE', '2': 1},
    {'1': 'WORKSPACE_CONTEXT_STATE_FROZEN', '2': 2},
    {'1': 'WORKSPACE_CONTEXT_STATE_DISCARDED', '2': 3},
  ],
};

/// Descriptor for `WorkspaceContextState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List workspaceContextStateDescriptor = $convert.base64Decode(
    'ChVXb3Jrc3BhY2VDb250ZXh0U3RhdGUSJwojV09SS1NQQUNFX0NPTlRFWFRfU1RBVEVfVU5TUE'
    'VDSUZJRUQQABIiCh5XT1JLU1BBQ0VfQ09OVEVYVF9TVEFURV9BQ1RJVkUQARIiCh5XT1JLU1BB'
    'Q0VfQ09OVEVYVF9TVEFURV9GUk9aRU4QAhIlCiFXT1JLU1BBQ0VfQ09OVEVYVF9TVEFURV9ESV'
    'NDQVJERUQQAw==');

@$core.Deprecated('Use filesystemProfileDescriptor instead')
const FilesystemProfile$json = {
  '1': 'FilesystemProfile',
  '2': [
    {'1': 'FILESYSTEM_PROFILE_UNSPECIFIED', '2': 0},
    {'1': 'FILESYSTEM_PROFILE_PORTABLE', '2': 1},
    {'1': 'FILESYSTEM_PROFILE_POSIX', '2': 2},
    {'1': 'FILESYSTEM_PROFILE_WINDOWS', '2': 3},
    {'1': 'FILESYSTEM_PROFILE_BROWSER', '2': 4},
  ],
};

/// Descriptor for `FilesystemProfile`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List filesystemProfileDescriptor = $convert.base64Decode(
    'ChFGaWxlc3lzdGVtUHJvZmlsZRIiCh5GSUxFU1lTVEVNX1BST0ZJTEVfVU5TUEVDSUZJRUQQAB'
    'IfChtGSUxFU1lTVEVNX1BST0ZJTEVfUE9SVEFCTEUQARIcChhGSUxFU1lTVEVNX1BST0ZJTEVf'
    'UE9TSVgQAhIeChpGSUxFU1lTVEVNX1BST0ZJTEVfV0lORE9XUxADEh4KGkZJTEVTWVNURU1fUF'
    'JPRklMRV9CUk9XU0VSEAQ=');

@$core.Deprecated('Use fileKindDescriptor instead')
const FileKind$json = {
  '1': 'FileKind',
  '2': [
    {'1': 'FILE_KIND_UNSPECIFIED', '2': 0},
    {'1': 'FILE_KIND_REGULAR', '2': 1},
    {'1': 'FILE_KIND_DIRECTORY', '2': 2},
    {'1': 'FILE_KIND_SYMBOLIC_LINK', '2': 3},
    {'1': 'FILE_KIND_FIFO', '2': 4},
    {'1': 'FILE_KIND_SOCKET', '2': 5},
    {'1': 'FILE_KIND_CHARACTER_DEVICE', '2': 6},
    {'1': 'FILE_KIND_BLOCK_DEVICE', '2': 7},
    {'1': 'FILE_KIND_REPARSE_POINT', '2': 8},
    {'1': 'FILE_KIND_MOUNT_BOUNDARY', '2': 9},
  ],
};

/// Descriptor for `FileKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List fileKindDescriptor = $convert.base64Decode(
    'CghGaWxlS2luZBIZChVGSUxFX0tJTkRfVU5TUEVDSUZJRUQQABIVChFGSUxFX0tJTkRfUkVHVU'
    'xBUhABEhcKE0ZJTEVfS0lORF9ESVJFQ1RPUlkQAhIbChdGSUxFX0tJTkRfU1lNQk9MSUNfTElO'
    'SxADEhIKDkZJTEVfS0lORF9GSUZPEAQSFAoQRklMRV9LSU5EX1NPQ0tFVBAFEh4KGkZJTEVfS0'
    'lORF9DSEFSQUNURVJfREVWSUNFEAYSGgoWRklMRV9LSU5EX0JMT0NLX0RFVklDRRAHEhsKF0ZJ'
    'TEVfS0lORF9SRVBBUlNFX1BPSU5UEAgSHAoYRklMRV9LSU5EX01PVU5UX0JPVU5EQVJZEAk=');

@$core.Deprecated('Use mutationStatusDescriptor instead')
const MutationStatus$json = {
  '1': 'MutationStatus',
  '2': [
    {'1': 'MUTATION_STATUS_UNSPECIFIED', '2': 0},
    {'1': 'MUTATION_STATUS_COMMITTED', '2': 1},
    {'1': 'MUTATION_STATUS_ALREADY_COMMITTED', '2': 2},
    {'1': 'MUTATION_STATUS_CONFLICT', '2': 3},
    {'1': 'MUTATION_STATUS_FENCED', '2': 4},
    {'1': 'MUTATION_STATUS_IDEMPOTENCY_CONFLICT', '2': 5},
    {'1': 'MUTATION_STATUS_INDETERMINATE', '2': 6},
  ],
};

/// Descriptor for `MutationStatus`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List mutationStatusDescriptor = $convert.base64Decode(
    'Cg5NdXRhdGlvblN0YXR1cxIfChtNVVRBVElPTl9TVEFUVVNfVU5TUEVDSUZJRUQQABIdChlNVV'
    'RBVElPTl9TVEFUVVNfQ09NTUlUVEVEEAESJQohTVVUQVRJT05fU1RBVFVTX0FMUkVBRFlfQ09N'
    'TUlUVEVEEAISHAoYTVVUQVRJT05fU1RBVFVTX0NPTkZMSUNUEAMSGgoWTVVUQVRJT05fU1RBVF'
    'VTX0ZFTkNFRBAEEigKJE1VVEFUSU9OX1NUQVRVU19JREVNUE9URU5DWV9DT05GTElDVBAFEiEK'
    'HU1VVEFUSU9OX1NUQVRVU19JTkRFVEVSTUlOQVRFEAY=');

@$core.Deprecated('Use joinHistoryDescriptor instead')
const JoinHistory$json = {
  '1': 'JoinHistory',
  '2': [
    {'1': 'JOIN_HISTORY_UNSPECIFIED', '2': 0},
    {'1': 'JOIN_HISTORY_MERGE', '2': 1},
    {'1': 'JOIN_HISTORY_REBASE', '2': 2},
    {'1': 'JOIN_HISTORY_SQUASH', '2': 3},
    {'1': 'JOIN_HISTORY_CHERRY_PICK', '2': 4},
  ],
};

/// Descriptor for `JoinHistory`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List joinHistoryDescriptor = $convert.base64Decode(
    'CgtKb2luSGlzdG9yeRIcChhKT0lOX0hJU1RPUllfVU5TUEVDSUZJRUQQABIWChJKT0lOX0hJU1'
    'RPUllfTUVSR0UQARIXChNKT0lOX0hJU1RPUllfUkVCQVNFEAISFwoTSk9JTl9ISVNUT1JZX1NR'
    'VUFTSBADEhwKGEpPSU5fSElTVE9SWV9DSEVSUllfUElDSxAE');

@$core.Deprecated('Use conflictUseDescriptor instead')
const ConflictUse$json = {
  '1': 'ConflictUse',
  '2': [
    {'1': 'CONFLICT_USE_UNSPECIFIED', '2': 0},
    {'1': 'CONFLICT_USE_OBSERVATION', '2': 1},
    {'1': 'CONFLICT_USE_MUTATION', '2': 2},
    {'1': 'CONFLICT_USE_OBSERVATION_AND_MUTATION', '2': 3},
  ],
};

/// Descriptor for `ConflictUse`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List conflictUseDescriptor = $convert.base64Decode(
    'CgtDb25mbGljdFVzZRIcChhDT05GTElDVF9VU0VfVU5TUEVDSUZJRUQQABIcChhDT05GTElDVF'
    '9VU0VfT0JTRVJWQVRJT04QARIZChVDT05GTElDVF9VU0VfTVVUQVRJT04QAhIpCiVDT05GTElD'
    'VF9VU0VfT0JTRVJWQVRJT05fQU5EX01VVEFUSU9OEAM=');

@$core.Deprecated('Use sparseTargetDescriptor instead')
const SparseTarget$json = {
  '1': 'SparseTarget',
  '2': [
    {'1': 'SPARSE_TARGET_UNSPECIFIED', '2': 0},
    {'1': 'SPARSE_TARGET_DATA', '2': 1},
    {'1': 'SPARSE_TARGET_HOLE', '2': 2},
  ],
};

/// Descriptor for `SparseTarget`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List sparseTargetDescriptor = $convert.base64Decode(
    'CgxTcGFyc2VUYXJnZXQSHQoZU1BBUlNFX1RBUkdFVF9VTlNQRUNJRklFRBAAEhYKElNQQVJTRV'
    '9UQVJHRVRfREFUQRABEhYKElNQQVJTRV9UQVJHRVRfSE9MRRAC');

@$core.Deprecated('Use extentKindDescriptor instead')
const ExtentKind$json = {
  '1': 'ExtentKind',
  '2': [
    {'1': 'EXTENT_KIND_UNSPECIFIED', '2': 0},
    {'1': 'EXTENT_KIND_HOLE', '2': 1},
    {'1': 'EXTENT_KIND_ALLOCATED_ZERO', '2': 2},
    {'1': 'EXTENT_KIND_CONTENT', '2': 3},
  ],
};

/// Descriptor for `ExtentKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List extentKindDescriptor = $convert.base64Decode(
    'CgpFeHRlbnRLaW5kEhsKF0VYVEVOVF9LSU5EX1VOU1BFQ0lGSUVEEAASFAoQRVhURU5UX0tJTk'
    'RfSE9MRRABEh4KGkVYVEVOVF9LSU5EX0FMTE9DQVRFRF9aRVJPEAISFwoTRVhURU5UX0tJTkRf'
    'Q09OVEVOVBAD');

@$core.Deprecated('Use sourceStateDescriptor instead')
const SourceState$json = {
  '1': 'SourceState',
  '2': [
    {'1': 'SOURCE_STATE_UNSPECIFIED', '2': 0},
    {'1': 'SOURCE_STATE_CLEAN', '2': 1},
    {'1': 'SOURCE_STATE_PENDING_CAPTURE', '2': 2},
    {'1': 'SOURCE_STATE_NEEDS_RESCAN', '2': 3},
    {'1': 'SOURCE_STATE_CONFLICT', '2': 4},
    {'1': 'SOURCE_STATE_SEALED', '2': 5},
  ],
};

/// Descriptor for `SourceState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List sourceStateDescriptor = $convert.base64Decode(
    'CgtTb3VyY2VTdGF0ZRIcChhTT1VSQ0VfU1RBVEVfVU5TUEVDSUZJRUQQABIWChJTT1VSQ0VfU1'
    'RBVEVfQ0xFQU4QARIgChxTT1VSQ0VfU1RBVEVfUEVORElOR19DQVBUVVJFEAISHQoZU09VUkNF'
    'X1NUQVRFX05FRURTX1JFU0NBThADEhkKFVNPVVJDRV9TVEFURV9DT05GTElDVBAEEhcKE1NPVV'
    'JDRV9TVEFURV9TRUFMRUQQBQ==');

@$core.Deprecated('Use sourceInvalidationReasonDescriptor instead')
const SourceInvalidationReason$json = {
  '1': 'SourceInvalidationReason',
  '2': [
    {'1': 'SOURCE_INVALIDATION_REASON_UNSPECIFIED', '2': 0},
    {'1': 'SOURCE_INVALIDATION_REASON_INITIAL_SNAPSHOT_REQUIRED', '2': 1},
    {'1': 'SOURCE_INVALIDATION_REASON_QUEUE_OVERFLOW', '2': 2},
    {'1': 'SOURCE_INVALIDATION_REASON_NATIVE_RESCAN_REQUIRED', '2': 3},
    {'1': 'SOURCE_INVALIDATION_REASON_BACKEND_ERROR', '2': 4},
    {'1': 'SOURCE_INVALIDATION_REASON_UNREPRESENTABLE_PATH', '2': 5},
    {'1': 'SOURCE_INVALIDATION_REASON_AMBIGUOUS_RENAME', '2': 6},
    {'1': 'SOURCE_INVALIDATION_REASON_ROOT_CHANGED', '2': 7},
  ],
};

/// Descriptor for `SourceInvalidationReason`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List sourceInvalidationReasonDescriptor = $convert.base64Decode(
    'ChhTb3VyY2VJbnZhbGlkYXRpb25SZWFzb24SKgomU09VUkNFX0lOVkFMSURBVElPTl9SRUFTT0'
    '5fVU5TUEVDSUZJRUQQABI4CjRTT1VSQ0VfSU5WQUxJREFUSU9OX1JFQVNPTl9JTklUSUFMX1NO'
    'QVBTSE9UX1JFUVVJUkVEEAESLQopU09VUkNFX0lOVkFMSURBVElPTl9SRUFTT05fUVVFVUVfT1'
    'ZFUkZMT1cQAhI1CjFTT1VSQ0VfSU5WQUxJREFUSU9OX1JFQVNPTl9OQVRJVkVfUkVTQ0FOX1JF'
    'UVVJUkVEEAMSLAooU09VUkNFX0lOVkFMSURBVElPTl9SRUFTT05fQkFDS0VORF9FUlJPUhAEEj'
    'MKL1NPVVJDRV9JTlZBTElEQVRJT05fUkVBU09OX1VOUkVQUkVTRU5UQUJMRV9QQVRIEAUSLwor'
    'U09VUkNFX0lOVkFMSURBVElPTl9SRUFTT05fQU1CSUdVT1VTX1JFTkFNRRAGEisKJ1NPVVJDRV'
    '9JTlZBTElEQVRJT05fUkVBU09OX1JPT1RfQ0hBTkdFRBAH');

@$core.Deprecated('Use nameEncodingDescriptor instead')
const NameEncoding$json = {
  '1': 'NameEncoding',
  '2': [
    {'1': 'NAME_ENCODING_UNSPECIFIED', '2': 0},
    {'1': 'NAME_ENCODING_UTF8', '2': 1},
    {'1': 'NAME_ENCODING_POSIX_BYTES', '2': 2},
    {'1': 'NAME_ENCODING_WINDOWS_UTF16LE', '2': 3},
  ],
};

/// Descriptor for `NameEncoding`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List nameEncodingDescriptor = $convert.base64Decode(
    'CgxOYW1lRW5jb2RpbmcSHQoZTkFNRV9FTkNPRElOR19VTlNQRUNJRklFRBAAEhYKEk5BTUVfRU'
    '5DT0RJTkdfVVRGOBABEh0KGU5BTUVfRU5DT0RJTkdfUE9TSVhfQllURVMQAhIhCh1OQU1FX0VO'
    'Q09ESU5HX1dJTkRPV1NfVVRGMTZMRRAD');

@$core.Deprecated('Use rebaseStatusDescriptor instead')
const RebaseStatus$json = {
  '1': 'RebaseStatus',
  '2': [
    {'1': 'REBASE_STATUS_UNSPECIFIED', '2': 0},
    {'1': 'REBASE_STATUS_REBASED', '2': 1},
    {'1': 'REBASE_STATUS_ALREADY_REBASED', '2': 2},
    {'1': 'REBASE_STATUS_CURRENT', '2': 3},
    {'1': 'REBASE_STATUS_STALE', '2': 4},
    {'1': 'REBASE_STATUS_CONFLICTED', '2': 5},
    {'1': 'REBASE_STATUS_FENCED', '2': 6},
    {'1': 'REBASE_STATUS_IDEMPOTENCY_CONFLICT', '2': 7},
  ],
};

/// Descriptor for `RebaseStatus`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List rebaseStatusDescriptor = $convert.base64Decode(
    'CgxSZWJhc2VTdGF0dXMSHQoZUkVCQVNFX1NUQVRVU19VTlNQRUNJRklFRBAAEhkKFVJFQkFTRV'
    '9TVEFUVVNfUkVCQVNFRBABEiEKHVJFQkFTRV9TVEFUVVNfQUxSRUFEWV9SRUJBU0VEEAISGQoV'
    'UkVCQVNFX1NUQVRVU19DVVJSRU5UEAMSFwoTUkVCQVNFX1NUQVRVU19TVEFMRRAEEhwKGFJFQk'
    'FTRV9TVEFUVVNfQ09ORkxJQ1RFRBAFEhgKFFJFQkFTRV9TVEFUVVNfRkVOQ0VEEAYSJgoiUkVC'
    'QVNFX1NUQVRVU19JREVNUE9URU5DWV9DT05GTElDVBAH');

@$core.Deprecated('Use joinStatusDescriptor instead')
const JoinStatus$json = {
  '1': 'JoinStatus',
  '2': [
    {'1': 'JOIN_STATUS_UNSPECIFIED', '2': 0},
    {'1': 'JOIN_STATUS_APPLIED', '2': 1},
    {'1': 'JOIN_STATUS_ALREADY_APPLIED', '2': 2},
    {'1': 'JOIN_STATUS_NO_CHANGES', '2': 3},
    {'1': 'JOIN_STATUS_STALE_TARGET', '2': 4},
    {'1': 'JOIN_STATUS_CONFLICTED', '2': 5},
    {'1': 'JOIN_STATUS_FENCED', '2': 6},
    {'1': 'JOIN_STATUS_IDEMPOTENCY_CONFLICT', '2': 7},
  ],
};

/// Descriptor for `JoinStatus`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List joinStatusDescriptor = $convert.base64Decode(
    'CgpKb2luU3RhdHVzEhsKF0pPSU5fU1RBVFVTX1VOU1BFQ0lGSUVEEAASFwoTSk9JTl9TVEFUVV'
    'NfQVBQTElFRBABEh8KG0pPSU5fU1RBVFVTX0FMUkVBRFlfQVBQTElFRBACEhoKFkpPSU5fU1RB'
    'VFVTX05PX0NIQU5HRVMQAxIcChhKT0lOX1NUQVRVU19TVEFMRV9UQVJHRVQQBBIaChZKT0lOX1'
    'NUQVRVU19DT05GTElDVEVEEAUSFgoSSk9JTl9TVEFUVVNfRkVOQ0VEEAYSJAogSk9JTl9TVEFU'
    'VVNfSURFTVBPVEVOQ1lfQ09ORkxJQ1QQBw==');

@$core.Deprecated('Use workspaceRefDescriptor instead')
const WorkspaceRef$json = {
  '1': 'WorkspaceRef',
  '2': [
    {'1': 'workspace_id', '3': 1, '4': 1, '5': 12, '10': 'workspaceId'},
    {'1': 'name', '3': 2, '4': 1, '5': 9, '10': 'name'},
  ],
};

/// Descriptor for `WorkspaceRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workspaceRefDescriptor = $convert.base64Decode(
    'CgxXb3Jrc3BhY2VSZWYSIQoMd29ya3NwYWNlX2lkGAEgASgMUgt3b3Jrc3BhY2VJZBISCgRuYW'
    '1lGAIgASgJUgRuYW1l');

@$core.Deprecated('Use generationRefDescriptor instead')
const GenerationRef$json = {
  '1': 'GenerationRef',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {'1': 'generation_id', '3': 2, '4': 1, '5': 12, '10': 'generationId'},
  ],
};

/// Descriptor for `GenerationRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List generationRefDescriptor = $convert.base64Decode(
    'Cg1HZW5lcmF0aW9uUmVmEkEKCXdvcmtzcGFjZRgBIAEoCzIjLmFjeWNsaWMuZmlsZXN5c3RlbS'
    '52Mi5Xb3Jrc3BhY2VSZWZSCXdvcmtzcGFjZRIjCg1nZW5lcmF0aW9uX2lkGAIgASgMUgxnZW5l'
    'cmF0aW9uSWQ=');

@$core.Deprecated('Use workspaceContextRootDescriptor instead')
const WorkspaceContextRoot$json = {
  '1': 'WorkspaceContextRoot',
  '2': [
    {'1': 'root_id', '3': 1, '4': 1, '5': 12, '10': 'rootId'},
    {'1': 'source_path', '3': 2, '4': 1, '5': 9, '10': 'sourcePath'},
    {'1': 'workspace_id', '3': 3, '4': 1, '5': 12, '10': 'workspaceId'},
    {'1': 'workspace_name', '3': 4, '4': 1, '5': 9, '10': 'workspaceName'},
    {
      '1': 'parent_workspace_id',
      '3': 5,
      '4': 1,
      '5': 12,
      '9': 0,
      '10': 'parentWorkspaceId',
      '17': true
    },
    {
      '1': 'mount_path',
      '3': 6,
      '4': 1,
      '5': 9,
      '9': 1,
      '10': 'mountPath',
      '17': true
    },
  ],
  '8': [
    {'1': '_parent_workspace_id'},
    {'1': '_mount_path'},
  ],
};

/// Descriptor for `WorkspaceContextRoot`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workspaceContextRootDescriptor = $convert.base64Decode(
    'ChRXb3Jrc3BhY2VDb250ZXh0Um9vdBIXCgdyb290X2lkGAEgASgMUgZyb290SWQSHwoLc291cm'
    'NlX3BhdGgYAiABKAlSCnNvdXJjZVBhdGgSIQoMd29ya3NwYWNlX2lkGAMgASgMUgt3b3Jrc3Bh'
    'Y2VJZBIlCg53b3Jrc3BhY2VfbmFtZRgEIAEoCVINd29ya3NwYWNlTmFtZRIzChNwYXJlbnRfd2'
    '9ya3NwYWNlX2lkGAUgASgMSABSEXBhcmVudFdvcmtzcGFjZUlkiAEBEiIKCm1vdW50X3BhdGgY'
    'BiABKAlIAVIJbW91bnRQYXRoiAEBQhYKFF9wYXJlbnRfd29ya3NwYWNlX2lkQg0KC19tb3VudF'
    '9wYXRo');

@$core.Deprecated('Use workspaceContextRootsDescriptor instead')
const WorkspaceContextRoots$json = {
  '1': 'WorkspaceContextRoots',
  '2': [
    {
      '1': 'roots',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceContextRoot',
      '10': 'roots'
    },
  ],
};

/// Descriptor for `WorkspaceContextRoots`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workspaceContextRootsDescriptor = $convert.base64Decode(
    'ChVXb3Jrc3BhY2VDb250ZXh0Um9vdHMSQQoFcm9vdHMYASADKAsyKy5hY3ljbGljLmZpbGVzeX'
    'N0ZW0udjIuV29ya3NwYWNlQ29udGV4dFJvb3RSBXJvb3Rz');

@$core.Deprecated('Use workspaceContextSnapshotDescriptor instead')
const WorkspaceContextSnapshot$json = {
  '1': 'WorkspaceContextSnapshot',
  '2': [
    {'1': 'version', '3': 1, '4': 1, '5': 13, '10': 'version'},
    {'1': 'revision', '3': 2, '4': 1, '5': 4, '10': 'revision'},
    {'1': 'context_id', '3': 3, '4': 1, '5': 12, '10': 'contextId'},
    {
      '1': 'parent_context_id',
      '3': 4,
      '4': 1,
      '5': 12,
      '9': 0,
      '10': 'parentContextId',
      '17': true
    },
    {
      '1': 'roots',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceContextRoot',
      '10': 'roots'
    },
    {
      '1': 'state',
      '3': 6,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.WorkspaceContextState',
      '10': 'state'
    },
  ],
  '8': [
    {'1': '_parent_context_id'},
  ],
};

/// Descriptor for `WorkspaceContextSnapshot`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workspaceContextSnapshotDescriptor = $convert.base64Decode(
    'ChhXb3Jrc3BhY2VDb250ZXh0U25hcHNob3QSGAoHdmVyc2lvbhgBIAEoDVIHdmVyc2lvbhIaCg'
    'hyZXZpc2lvbhgCIAEoBFIIcmV2aXNpb24SHQoKY29udGV4dF9pZBgDIAEoDFIJY29udGV4dElk'
    'Ei8KEXBhcmVudF9jb250ZXh0X2lkGAQgASgMSABSD3BhcmVudENvbnRleHRJZIgBARJBCgVyb2'
    '90cxgFIAMoCzIrLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5Xb3Jrc3BhY2VDb250ZXh0Um9vdFIF'
    'cm9vdHMSQgoFc3RhdGUYBiABKA4yLC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuV29ya3NwYWNlQ2'
    '9udGV4dFN0YXRlUgVzdGF0ZUIUChJfcGFyZW50X2NvbnRleHRfaWQ=');

@$core.Deprecated('Use workspaceContextDiscardDescriptor instead')
const WorkspaceContextDiscard$json = {
  '1': 'WorkspaceContextDiscard',
  '2': [
    {'1': 'context_ids', '3': 1, '4': 3, '5': 12, '10': 'contextIds'},
  ],
};

/// Descriptor for `WorkspaceContextDiscard`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workspaceContextDiscardDescriptor =
    $convert.base64Decode(
        'ChdXb3Jrc3BhY2VDb250ZXh0RGlzY2FyZBIfCgtjb250ZXh0X2lkcxgBIAMoDFIKY29udGV4dE'
        'lkcw==');

@$core.Deprecated('Use operationOptionsDescriptor instead')
const OperationOptions$json = {
  '1': 'OperationOptions',
  '2': [
    {'1': 'idempotency_key', '3': 1, '4': 1, '5': 12, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `OperationOptions`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List operationOptionsDescriptor = $convert.base64Decode(
    'ChBPcGVyYXRpb25PcHRpb25zEicKD2lkZW1wb3RlbmN5X2tleRgBIAEoDFIOaWRlbXBvdGVuY3'
    'lLZXk=');

@$core.Deprecated('Use pageOptionsDescriptor instead')
const PageOptions$json = {
  '1': 'PageOptions',
  '2': [
    {'1': 'maximum_items', '3': 1, '4': 1, '5': 13, '10': 'maximumItems'},
    {
      '1': 'after',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.LogicalName',
      '10': 'after'
    },
  ],
};

/// Descriptor for `PageOptions`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List pageOptionsDescriptor = $convert.base64Decode(
    'CgtQYWdlT3B0aW9ucxIjCg1tYXhpbXVtX2l0ZW1zGAEgASgNUgxtYXhpbXVtSXRlbXMSOAoFYW'
    'Z0ZXIYAiABKAsyIi5hY3ljbGljLmZpbGVzeXN0ZW0udjIuTG9naWNhbE5hbWVSBWFmdGVy');

@$core.Deprecated('Use byteRangeDescriptor instead')
const ByteRange$json = {
  '1': 'ByteRange',
  '2': [
    {'1': 'offset', '3': 1, '4': 1, '5': 4, '10': 'offset'},
    {'1': 'length', '3': 2, '4': 1, '5': 4, '10': 'length'},
  ],
};

/// Descriptor for `ByteRange`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List byteRangeDescriptor = $convert.base64Decode(
    'CglCeXRlUmFuZ2USFgoGb2Zmc2V0GAEgASgEUgZvZmZzZXQSFgoGbGVuZ3RoGAIgASgEUgZsZW'
    '5ndGg=');

@$core.Deprecated('Use optionalU32Descriptor instead')
const OptionalU32$json = {
  '1': 'OptionalU32',
  '2': [
    {'1': 'present', '3': 1, '4': 1, '5': 13, '9': 0, '10': 'present'},
    {'1': 'unavailable', '3': 2, '4': 1, '5': 8, '9': 0, '10': 'unavailable'},
  ],
  '8': [
    {'1': 'value'},
  ],
};

/// Descriptor for `OptionalU32`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List optionalU32Descriptor = $convert.base64Decode(
    'CgtPcHRpb25hbFUzMhIaCgdwcmVzZW50GAEgASgNSABSB3ByZXNlbnQSIgoLdW5hdmFpbGFibG'
    'UYAiABKAhIAFILdW5hdmFpbGFibGVCBwoFdmFsdWU=');

@$core.Deprecated('Use optionalU64Descriptor instead')
const OptionalU64$json = {
  '1': 'OptionalU64',
  '2': [
    {'1': 'present', '3': 1, '4': 1, '5': 4, '9': 0, '10': 'present'},
    {'1': 'unavailable', '3': 2, '4': 1, '5': 8, '9': 0, '10': 'unavailable'},
  ],
  '8': [
    {'1': 'value'},
  ],
};

/// Descriptor for `OptionalU64`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List optionalU64Descriptor = $convert.base64Decode(
    'CgtPcHRpb25hbFU2NBIaCgdwcmVzZW50GAEgASgESABSB3ByZXNlbnQSIgoLdW5hdmFpbGFibG'
    'UYAiABKAhIAFILdW5hdmFpbGFibGVCBwoFdmFsdWU=');

@$core.Deprecated('Use optionalI64Descriptor instead')
const OptionalI64$json = {
  '1': 'OptionalI64',
  '2': [
    {'1': 'present', '3': 1, '4': 1, '5': 18, '9': 0, '10': 'present'},
    {'1': 'unavailable', '3': 2, '4': 1, '5': 8, '9': 0, '10': 'unavailable'},
  ],
  '8': [
    {'1': 'value'},
  ],
};

/// Descriptor for `OptionalI64`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List optionalI64Descriptor = $convert.base64Decode(
    'CgtPcHRpb25hbEk2NBIaCgdwcmVzZW50GAEgASgSSABSB3ByZXNlbnQSIgoLdW5hdmFpbGFibG'
    'UYAiABKAhIAFILdW5hdmFpbGFibGVCBwoFdmFsdWU=');

@$core.Deprecated('Use metadataDescriptor instead')
const Metadata$json = {
  '1': 'Metadata',
  '2': [
    {
      '1': 'posix_mode',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU32',
      '10': 'posixMode'
    },
    {
      '1': 'posix_uid',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU32',
      '10': 'posixUid'
    },
    {
      '1': 'posix_gid',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU32',
      '10': 'posixGid'
    },
    {
      '1': 'posix_flags',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU64',
      '10': 'posixFlags'
    },
    {
      '1': 'windows_attributes',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU32',
      '10': 'windowsAttributes'
    },
    {
      '1': 'created_ns',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalI64',
      '10': 'createdNs'
    },
    {
      '1': 'modified_ns',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalI64',
      '10': 'modifiedNs'
    },
    {
      '1': 'accessed_ns',
      '3': 8,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalI64',
      '10': 'accessedNs'
    },
    {
      '1': 'changed_ns',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalI64',
      '10': 'changedNs'
    },
    {
      '1': 'has_named_attributes',
      '3': 10,
      '4': 1,
      '5': 8,
      '10': 'hasNamedAttributes'
    },
    {'1': 'has_acl', '3': 11, '4': 1, '5': 8, '10': 'hasAcl'},
    {
      '1': 'has_security_descriptor',
      '3': 12,
      '4': 1,
      '5': 8,
      '10': 'hasSecurityDescriptor'
    },
  ],
};

/// Descriptor for `Metadata`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List metadataDescriptor = $convert.base64Decode(
    'CghNZXRhZGF0YRJBCgpwb3NpeF9tb2RlGAEgASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk'
    '9wdGlvbmFsVTMyUglwb3NpeE1vZGUSPwoJcG9zaXhfdWlkGAIgASgLMiIuYWN5Y2xpYy5maWxl'
    'c3lzdGVtLnYyLk9wdGlvbmFsVTMyUghwb3NpeFVpZBI/Cglwb3NpeF9naWQYAyABKAsyIi5hY3'
    'ljbGljLmZpbGVzeXN0ZW0udjIuT3B0aW9uYWxVMzJSCHBvc2l4R2lkEkMKC3Bvc2l4X2ZsYWdz'
    'GAQgASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk9wdGlvbmFsVTY0Ugpwb3NpeEZsYWdzEl'
    'EKEndpbmRvd3NfYXR0cmlidXRlcxgFIAEoCzIiLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5PcHRp'
    'b25hbFUzMlIRd2luZG93c0F0dHJpYnV0ZXMSQQoKY3JlYXRlZF9ucxgGIAEoCzIiLmFjeWNsaW'
    'MuZmlsZXN5c3RlbS52Mi5PcHRpb25hbEk2NFIJY3JlYXRlZE5zEkMKC21vZGlmaWVkX25zGAcg'
    'ASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk9wdGlvbmFsSTY0Ugptb2RpZmllZE5zEkMKC2'
    'FjY2Vzc2VkX25zGAggASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk9wdGlvbmFsSTY0Ugph'
    'Y2Nlc3NlZE5zEkEKCmNoYW5nZWRfbnMYCSABKAsyIi5hY3ljbGljLmZpbGVzeXN0ZW0udjIuT3'
    'B0aW9uYWxJNjRSCWNoYW5nZWROcxIwChRoYXNfbmFtZWRfYXR0cmlidXRlcxgKIAEoCFISaGFz'
    'TmFtZWRBdHRyaWJ1dGVzEhcKB2hhc19hY2wYCyABKAhSBmhhc0FjbBI2ChdoYXNfc2VjdXJpdH'
    'lfZGVzY3JpcHRvchgMIAEoCFIVaGFzU2VjdXJpdHlEZXNjcmlwdG9y');

@$core.Deprecated('Use fileStatDescriptor instead')
const FileStat$json = {
  '1': 'FileStat',
  '2': [
    {'1': 'file_id', '3': 1, '4': 1, '5': 12, '10': 'fileId'},
    {
      '1': 'kind',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.FileKind',
      '10': 'kind'
    },
    {'1': 'link_count', '3': 3, '4': 1, '5': 4, '10': 'linkCount'},
    {
      '1': 'logical_bytes',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU64',
      '10': 'logicalBytes'
    },
    {
      '1': 'metadata',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Metadata',
      '10': 'metadata'
    },
  ],
};

/// Descriptor for `FileStat`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List fileStatDescriptor = $convert.base64Decode(
    'CghGaWxlU3RhdBIXCgdmaWxlX2lkGAEgASgMUgZmaWxlSWQSMwoEa2luZBgCIAEoDjIfLmFjeW'
    'NsaWMuZmlsZXN5c3RlbS52Mi5GaWxlS2luZFIEa2luZBIdCgpsaW5rX2NvdW50GAMgASgEUgls'
    'aW5rQ291bnQSRwoNbG9naWNhbF9ieXRlcxgEIAEoCzIiLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi'
    '5PcHRpb25hbFU2NFIMbG9naWNhbEJ5dGVzEjsKCG1ldGFkYXRhGAUgASgLMh8uYWN5Y2xpYy5m'
    'aWxlc3lzdGVtLnYyLk1ldGFkYXRhUghtZXRhZGF0YQ==');

@$core.Deprecated('Use directoryEntryDescriptor instead')
const DirectoryEntry$json = {
  '1': 'DirectoryEntry',
  '2': [
    {
      '1': 'name',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.LogicalName',
      '10': 'name'
    },
    {
      '1': 'stat',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileStat',
      '10': 'stat'
    },
  ],
};

/// Descriptor for `DirectoryEntry`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List directoryEntryDescriptor = $convert.base64Decode(
    'Cg5EaXJlY3RvcnlFbnRyeRI2CgRuYW1lGAEgASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk'
    'xvZ2ljYWxOYW1lUgRuYW1lEjMKBHN0YXQYAiABKAsyHy5hY3ljbGljLmZpbGVzeXN0ZW0udjIu'
    'RmlsZVN0YXRSBHN0YXQ=');

@$core.Deprecated('Use directoryPageDescriptor instead')
const DirectoryPage$json = {
  '1': 'DirectoryPage',
  '2': [
    {
      '1': 'entries',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.DirectoryEntry',
      '10': 'entries'
    },
    {
      '1': 'next',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.LogicalName',
      '10': 'next'
    },
  ],
};

/// Descriptor for `DirectoryPage`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List directoryPageDescriptor = $convert.base64Decode(
    'Cg1EaXJlY3RvcnlQYWdlEj8KB2VudHJpZXMYASADKAsyJS5hY3ljbGljLmZpbGVzeXN0ZW0udj'
    'IuRGlyZWN0b3J5RW50cnlSB2VudHJpZXMSNgoEbmV4dBgCIAEoCzIiLmFjeWNsaWMuZmlsZXN5'
    'c3RlbS52Mi5Mb2dpY2FsTmFtZVIEbmV4dA==');

@$core.Deprecated('Use extentDescriptor instead')
const Extent$json = {
  '1': 'Extent',
  '2': [
    {
      '1': 'range',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ByteRange',
      '10': 'range'
    },
    {
      '1': 'kind',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.ExtentKind',
      '10': 'kind'
    },
  ],
};

/// Descriptor for `Extent`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extentDescriptor = $convert.base64Decode(
    'CgZFeHRlbnQSNgoFcmFuZ2UYASABKAsyIC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuQnl0ZVJhbm'
    'dlUgVyYW5nZRI1CgRraW5kGAIgASgOMiEuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkV4dGVudEtp'
    'bmRSBGtpbmQ=');

@$core.Deprecated('Use capabilitiesDescriptor instead')
const Capabilities$json = {
  '1': 'Capabilities',
  '2': [
    {'1': 'contract_version', '3': 1, '4': 1, '5': 9, '10': 'contractVersion'},
    {
      '1': 'profiles',
      '3': 2,
      '4': 3,
      '5': 14,
      '6': '.acyclic.filesystem.v2.FilesystemProfile',
      '10': 'profiles'
    },
    {
      '1': 'maximum_request_bytes',
      '3': 3,
      '4': 1,
      '5': 4,
      '10': 'maximumRequestBytes'
    },
    {
      '1': 'maximum_response_bytes',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'maximumResponseBytes'
    },
    {
      '1': 'maximum_transaction_mutations',
      '3': 5,
      '4': 1,
      '5': 13,
      '10': 'maximumTransactionMutations'
    },
    {
      '1': 'maximum_page_items',
      '3': 6,
      '4': 1,
      '5': 13,
      '10': 'maximumPageItems'
    },
    {
      '1': 'native_mount_credentials',
      '3': 7,
      '4': 1,
      '5': 8,
      '10': 'nativeMountCredentials'
    },
    {'1': 's3_credentials', '3': 8, '4': 1, '5': 8, '10': 's3Credentials'},
    {
      '1': 'source_reconciliation',
      '3': 9,
      '4': 1,
      '5': 8,
      '10': 'sourceReconciliation'
    },
  ],
};

/// Descriptor for `Capabilities`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List capabilitiesDescriptor = $convert.base64Decode(
    'CgxDYXBhYmlsaXRpZXMSKQoQY29udHJhY3RfdmVyc2lvbhgBIAEoCVIPY29udHJhY3RWZXJzaW'
    '9uEkQKCHByb2ZpbGVzGAIgAygOMiguYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkZpbGVzeXN0ZW1Q'
    'cm9maWxlUghwcm9maWxlcxIyChVtYXhpbXVtX3JlcXVlc3RfYnl0ZXMYAyABKARSE21heGltdW'
    '1SZXF1ZXN0Qnl0ZXMSNAoWbWF4aW11bV9yZXNwb25zZV9ieXRlcxgEIAEoBFIUbWF4aW11bVJl'
    'c3BvbnNlQnl0ZXMSQgodbWF4aW11bV90cmFuc2FjdGlvbl9tdXRhdGlvbnMYBSABKA1SG21heG'
    'ltdW1UcmFuc2FjdGlvbk11dGF0aW9ucxIsChJtYXhpbXVtX3BhZ2VfaXRlbXMYBiABKA1SEG1h'
    'eGltdW1QYWdlSXRlbXMSOAoYbmF0aXZlX21vdW50X2NyZWRlbnRpYWxzGAcgASgIUhZuYXRpdm'
    'VNb3VudENyZWRlbnRpYWxzEiUKDnMzX2NyZWRlbnRpYWxzGAggASgIUg1zM0NyZWRlbnRpYWxz'
    'EjMKFXNvdXJjZV9yZWNvbmNpbGlhdGlvbhgJIAEoCFIUc291cmNlUmVjb25jaWxpYXRpb24=');

@$core.Deprecated('Use sourceStateRequestDescriptor instead')
const SourceStateRequest$json = {
  '1': 'SourceStateRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
  ],
};

/// Descriptor for `SourceStateRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List sourceStateRequestDescriptor = $convert.base64Decode(
    'ChJTb3VyY2VTdGF0ZVJlcXVlc3QSQQoJd29ya3NwYWNlGAEgASgLMiMuYWN5Y2xpYy5maWxlc3'
    'lzdGVtLnYyLldvcmtzcGFjZVJlZlIJd29ya3NwYWNl');

@$core.Deprecated('Use sourceOperationRequestDescriptor instead')
const SourceOperationRequest$json = {
  '1': 'SourceOperationRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {
      '1': 'operation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `SourceOperationRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List sourceOperationRequestDescriptor = $convert.base64Decode(
    'ChZTb3VyY2VPcGVyYXRpb25SZXF1ZXN0EkEKCXdvcmtzcGFjZRgBIAEoCzIjLmFjeWNsaWMuZm'
    'lsZXN5c3RlbS52Mi5Xb3Jrc3BhY2VSZWZSCXdvcmtzcGFjZRJFCglvcGVyYXRpb24YAiABKAsy'
    'Jy5hY3ljbGljLmZpbGVzeXN0ZW0udjIuT3BlcmF0aW9uT3B0aW9uc1IJb3BlcmF0aW9u');

@$core.Deprecated('Use sourceResponseDescriptor instead')
const SourceResponse$json = {
  '1': 'SourceResponse',
  '2': [
    {
      '1': 'state',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.SourceState',
      '10': 'state'
    },
    {
      '1': 'reason',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.SourceInvalidationReason',
      '10': 'reason'
    },
    {
      '1': 'generation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
  ],
};

/// Descriptor for `SourceResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List sourceResponseDescriptor = $convert.base64Decode(
    'Cg5Tb3VyY2VSZXNwb25zZRI4CgVzdGF0ZRgBIAEoDjIiLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi'
    '5Tb3VyY2VTdGF0ZVIFc3RhdGUSRwoGcmVhc29uGAIgASgOMi8uYWN5Y2xpYy5maWxlc3lzdGVt'
    'LnYyLlNvdXJjZUludmFsaWRhdGlvblJlYXNvblIGcmVhc29uEkQKCmdlbmVyYXRpb24YAyABKA'
    'syJC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuR2VuZXJhdGlvblJlZlIKZ2VuZXJhdGlvbg==');

@$core.Deprecated('Use workspaceDescriptor instead')
const Workspace$json = {
  '1': 'Workspace',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {'1': 'name', '3': 2, '4': 1, '5': 9, '10': 'name'},
    {
      '1': 'profile',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.FilesystemProfile',
      '10': 'profile'
    },
    {
      '1': 'head',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'head'
    },
    {'1': 'deleted', '3': 5, '4': 1, '5': 8, '10': 'deleted'},
  ],
};

/// Descriptor for `Workspace`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workspaceDescriptor = $convert.base64Decode(
    'CglXb3Jrc3BhY2USQQoJd29ya3NwYWNlGAEgASgLMiMuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLl'
    'dvcmtzcGFjZVJlZlIJd29ya3NwYWNlEhIKBG5hbWUYAiABKAlSBG5hbWUSQgoHcHJvZmlsZRgD'
    'IAEoDjIoLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5GaWxlc3lzdGVtUHJvZmlsZVIHcHJvZmlsZR'
    'I4CgRoZWFkGAQgASgLMiQuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkdlbmVyYXRpb25SZWZSBGhl'
    'YWQSGAoHZGVsZXRlZBgFIAEoCFIHZGVsZXRlZA==');

@$core.Deprecated('Use handshakeRequestDescriptor instead')
const HandshakeRequest$json = {
  '1': 'HandshakeRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.HandshakeRequest',
      '10': 'protocol'
    },
  ],
};

/// Descriptor for `HandshakeRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List handshakeRequestDescriptor = $convert.base64Decode(
    'ChBIYW5kc2hha2VSZXF1ZXN0EkEKCHByb3RvY29sGAEgASgLMiUuYWN5Y2xpYy5wcm90b2NvbC'
    '52MS5IYW5kc2hha2VSZXF1ZXN0Ughwcm90b2NvbA==');

@$core.Deprecated('Use handshakeResponseDescriptor instead')
const HandshakeResponse$json = {
  '1': 'HandshakeResponse',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.HandshakeResponse',
      '10': 'protocol'
    },
    {
      '1': 'capabilities',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Capabilities',
      '10': 'capabilities'
    },
  ],
};

/// Descriptor for `HandshakeResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List handshakeResponseDescriptor = $convert.base64Decode(
    'ChFIYW5kc2hha2VSZXNwb25zZRJCCghwcm90b2NvbBgBIAEoCzImLmFjeWNsaWMucHJvdG9jb2'
    'wudjEuSGFuZHNoYWtlUmVzcG9uc2VSCHByb3RvY29sEkcKDGNhcGFiaWxpdGllcxgCIAEoCzIj'
    'LmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5DYXBhYmlsaXRpZXNSDGNhcGFiaWxpdGllcw==');

@$core.Deprecated('Use createWorkspaceRequestDescriptor instead')
const CreateWorkspaceRequest$json = {
  '1': 'CreateWorkspaceRequest',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {
      '1': 'profile',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.FilesystemProfile',
      '10': 'profile'
    },
    {
      '1': 'operation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `CreateWorkspaceRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createWorkspaceRequestDescriptor = $convert.base64Decode(
    'ChZDcmVhdGVXb3Jrc3BhY2VSZXF1ZXN0EhIKBG5hbWUYASABKAlSBG5hbWUSQgoHcHJvZmlsZR'
    'gCIAEoDjIoLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5GaWxlc3lzdGVtUHJvZmlsZVIHcHJvZmls'
    'ZRJFCglvcGVyYXRpb24YAyABKAsyJy5hY3ljbGljLmZpbGVzeXN0ZW0udjIuT3BlcmF0aW9uT3'
    'B0aW9uc1IJb3BlcmF0aW9u');

@$core.Deprecated('Use openWorkspaceRequestDescriptor instead')
const OpenWorkspaceRequest$json = {
  '1': 'OpenWorkspaceRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '9': 0,
      '10': 'workspace'
    },
    {'1': 'name', '3': 2, '4': 1, '5': 9, '9': 0, '10': 'name'},
  ],
  '8': [
    {'1': 'selector'},
  ],
};

/// Descriptor for `OpenWorkspaceRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List openWorkspaceRequestDescriptor = $convert.base64Decode(
    'ChRPcGVuV29ya3NwYWNlUmVxdWVzdBJDCgl3b3Jrc3BhY2UYASABKAsyIy5hY3ljbGljLmZpbG'
    'VzeXN0ZW0udjIuV29ya3NwYWNlUmVmSABSCXdvcmtzcGFjZRIUCgRuYW1lGAIgASgJSABSBG5h'
    'bWVCCgoIc2VsZWN0b3I=');

@$core.Deprecated('Use workspaceResponseDescriptor instead')
const WorkspaceResponse$json = {
  '1': 'WorkspaceResponse',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Workspace',
      '10': 'workspace'
    },
    {
      '1': 'status',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.MutationStatus',
      '10': 'status'
    },
  ],
};

/// Descriptor for `WorkspaceResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workspaceResponseDescriptor = $convert.base64Decode(
    'ChFXb3Jrc3BhY2VSZXNwb25zZRI+Cgl3b3Jrc3BhY2UYASABKAsyIC5hY3ljbGljLmZpbGVzeX'
    'N0ZW0udjIuV29ya3NwYWNlUgl3b3Jrc3BhY2USPQoGc3RhdHVzGAIgASgOMiUuYWN5Y2xpYy5m'
    'aWxlc3lzdGVtLnYyLk11dGF0aW9uU3RhdHVzUgZzdGF0dXM=');

@$core.Deprecated('Use deleteWorkspaceRequestDescriptor instead')
const DeleteWorkspaceRequest$json = {
  '1': 'DeleteWorkspaceRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {
      '1': 'operation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `DeleteWorkspaceRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deleteWorkspaceRequestDescriptor = $convert.base64Decode(
    'ChZEZWxldGVXb3Jrc3BhY2VSZXF1ZXN0EkEKCXdvcmtzcGFjZRgBIAEoCzIjLmFjeWNsaWMuZm'
    'lsZXN5c3RlbS52Mi5Xb3Jrc3BhY2VSZWZSCXdvcmtzcGFjZRJFCglvcGVyYXRpb24YAiABKAsy'
    'Jy5hY3ljbGljLmZpbGVzeXN0ZW0udjIuT3BlcmF0aW9uT3B0aW9uc1IJb3BlcmF0aW9u');

@$core.Deprecated('Use mutationResponseDescriptor instead')
const MutationResponse$json = {
  '1': 'MutationResponse',
  '2': [
    {
      '1': 'status',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.MutationStatus',
      '10': 'status'
    },
    {
      '1': 'generation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {
      '1': 'actual_head',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'actualHead'
    },
    {
      '1': 'conflicts',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Conflict',
      '10': 'conflicts'
    },
    {'1': 'truncated', '3': 5, '4': 1, '5': 8, '10': 'truncated'},
  ],
};

/// Descriptor for `MutationResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List mutationResponseDescriptor = $convert.base64Decode(
    'ChBNdXRhdGlvblJlc3BvbnNlEj0KBnN0YXR1cxgBIAEoDjIlLmFjeWNsaWMuZmlsZXN5c3RlbS'
    '52Mi5NdXRhdGlvblN0YXR1c1IGc3RhdHVzEkQKCmdlbmVyYXRpb24YAiABKAsyJC5hY3ljbGlj'
    'LmZpbGVzeXN0ZW0udjIuR2VuZXJhdGlvblJlZlIKZ2VuZXJhdGlvbhJFCgthY3R1YWxfaGVhZB'
    'gDIAEoCzIkLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5HZW5lcmF0aW9uUmVmUgphY3R1YWxIZWFk'
    'Ej0KCWNvbmZsaWN0cxgEIAMoCzIfLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5Db25mbGljdFIJY2'
    '9uZmxpY3RzEhwKCXRydW5jYXRlZBgFIAEoCFIJdHJ1bmNhdGVk');

@$core.Deprecated('Use getHeadRequestDescriptor instead')
const GetHeadRequest$json = {
  '1': 'GetHeadRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
  ],
};

/// Descriptor for `GetHeadRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getHeadRequestDescriptor = $convert.base64Decode(
    'Cg5HZXRIZWFkUmVxdWVzdBJBCgl3b3Jrc3BhY2UYASABKAsyIy5hY3ljbGljLmZpbGVzeXN0ZW'
    '0udjIuV29ya3NwYWNlUmVmUgl3b3Jrc3BhY2U=');

@$core.Deprecated('Use getGenerationRequestDescriptor instead')
const GetGenerationRequest$json = {
  '1': 'GetGenerationRequest',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
  ],
};

/// Descriptor for `GetGenerationRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getGenerationRequestDescriptor = $convert.base64Decode(
    'ChRHZXRHZW5lcmF0aW9uUmVxdWVzdBJECgpnZW5lcmF0aW9uGAEgASgLMiQuYWN5Y2xpYy5maW'
    'xlc3lzdGVtLnYyLkdlbmVyYXRpb25SZWZSCmdlbmVyYXRpb24=');

@$core.Deprecated('Use generationResponseDescriptor instead')
const GenerationResponse$json = {
  '1': 'GenerationResponse',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {
      '1': 'parents',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'parents'
    },
  ],
};

/// Descriptor for `GenerationResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List generationResponseDescriptor = $convert.base64Decode(
    'ChJHZW5lcmF0aW9uUmVzcG9uc2USRAoKZ2VuZXJhdGlvbhgBIAEoCzIkLmFjeWNsaWMuZmlsZX'
    'N5c3RlbS52Mi5HZW5lcmF0aW9uUmVmUgpnZW5lcmF0aW9uEj4KB3BhcmVudHMYAiADKAsyJC5h'
    'Y3ljbGljLmZpbGVzeXN0ZW0udjIuR2VuZXJhdGlvblJlZlIHcGFyZW50cw==');

@$core.Deprecated('Use readRequestDescriptor instead')
const ReadRequest$json = {
  '1': 'ReadRequest',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'path', '3': 2, '4': 1, '5': 9, '10': 'path'},
    {
      '1': 'range',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ByteRange',
      '10': 'range'
    },
    {'1': 'maximum_bytes', '3': 4, '4': 1, '5': 4, '10': 'maximumBytes'},
  ],
};

/// Descriptor for `ReadRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List readRequestDescriptor = $convert.base64Decode(
    'CgtSZWFkUmVxdWVzdBJECgpnZW5lcmF0aW9uGAEgASgLMiQuYWN5Y2xpYy5maWxlc3lzdGVtLn'
    'YyLkdlbmVyYXRpb25SZWZSCmdlbmVyYXRpb24SEgoEcGF0aBgCIAEoCVIEcGF0aBI2CgVyYW5n'
    'ZRgDIAEoCzIgLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5CeXRlUmFuZ2VSBXJhbmdlEiMKDW1heG'
    'ltdW1fYnl0ZXMYBCABKARSDG1heGltdW1CeXRlcw==');

@$core.Deprecated('Use readResponseDescriptor instead')
const ReadResponse$json = {
  '1': 'ReadResponse',
  '2': [
    {'1': 'contents', '3': 1, '4': 1, '5': 12, '10': 'contents'},
  ],
};

/// Descriptor for `ReadResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List readResponseDescriptor = $convert
    .base64Decode('CgxSZWFkUmVzcG9uc2USGgoIY29udGVudHMYASABKAxSCGNvbnRlbnRz');

@$core.Deprecated('Use statRequestDescriptor instead')
const StatRequest$json = {
  '1': 'StatRequest',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'path', '3': 2, '4': 1, '5': 9, '10': 'path'},
  ],
};

/// Descriptor for `StatRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List statRequestDescriptor = $convert.base64Decode(
    'CgtTdGF0UmVxdWVzdBJECgpnZW5lcmF0aW9uGAEgASgLMiQuYWN5Y2xpYy5maWxlc3lzdGVtLn'
    'YyLkdlbmVyYXRpb25SZWZSCmdlbmVyYXRpb24SEgoEcGF0aBgCIAEoCVIEcGF0aA==');

@$core.Deprecated('Use statResponseDescriptor instead')
const StatResponse$json = {
  '1': 'StatResponse',
  '2': [
    {
      '1': 'stat',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileStat',
      '10': 'stat'
    },
  ],
};

/// Descriptor for `StatResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List statResponseDescriptor = $convert.base64Decode(
    'CgxTdGF0UmVzcG9uc2USMwoEc3RhdBgBIAEoCzIfLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5GaW'
    'xlU3RhdFIEc3RhdA==');

@$core.Deprecated('Use listDirectoryRequestDescriptor instead')
const ListDirectoryRequest$json = {
  '1': 'ListDirectoryRequest',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'path', '3': 2, '4': 1, '5': 9, '10': 'path'},
    {
      '1': 'page',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.PageOptions',
      '10': 'page'
    },
  ],
};

/// Descriptor for `ListDirectoryRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listDirectoryRequestDescriptor = $convert.base64Decode(
    'ChRMaXN0RGlyZWN0b3J5UmVxdWVzdBJECgpnZW5lcmF0aW9uGAEgASgLMiQuYWN5Y2xpYy5maW'
    'xlc3lzdGVtLnYyLkdlbmVyYXRpb25SZWZSCmdlbmVyYXRpb24SEgoEcGF0aBgCIAEoCVIEcGF0'
    'aBI2CgRwYWdlGAMgASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLlBhZ2VPcHRpb25zUgRwYW'
    'dl');

@$core.Deprecated('Use listDirectoryResponseDescriptor instead')
const ListDirectoryResponse$json = {
  '1': 'ListDirectoryResponse',
  '2': [
    {
      '1': 'page',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.DirectoryPage',
      '10': 'page'
    },
  ],
};

/// Descriptor for `ListDirectoryResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listDirectoryResponseDescriptor = $convert.base64Decode(
    'ChVMaXN0RGlyZWN0b3J5UmVzcG9uc2USOAoEcGFnZRgBIAEoCzIkLmFjeWNsaWMuZmlsZXN5c3'
    'RlbS52Mi5EaXJlY3RvcnlQYWdlUgRwYWdl');

@$core.Deprecated('Use readLinkRequestDescriptor instead')
const ReadLinkRequest$json = {
  '1': 'ReadLinkRequest',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'path', '3': 2, '4': 1, '5': 9, '10': 'path'},
    {'1': 'maximum_bytes', '3': 3, '4': 1, '5': 4, '10': 'maximumBytes'},
  ],
};

/// Descriptor for `ReadLinkRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List readLinkRequestDescriptor = $convert.base64Decode(
    'Cg9SZWFkTGlua1JlcXVlc3QSRAoKZ2VuZXJhdGlvbhgBIAEoCzIkLmFjeWNsaWMuZmlsZXN5c3'
    'RlbS52Mi5HZW5lcmF0aW9uUmVmUgpnZW5lcmF0aW9uEhIKBHBhdGgYAiABKAlSBHBhdGgSIwoN'
    'bWF4aW11bV9ieXRlcxgDIAEoBFIMbWF4aW11bUJ5dGVz');

@$core.Deprecated('Use planExtentsRequestDescriptor instead')
const PlanExtentsRequest$json = {
  '1': 'PlanExtentsRequest',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'path', '3': 2, '4': 1, '5': 9, '10': 'path'},
    {
      '1': 'range',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ByteRange',
      '10': 'range'
    },
    {'1': 'maximum_extents', '3': 4, '4': 1, '5': 13, '10': 'maximumExtents'},
  ],
};

/// Descriptor for `PlanExtentsRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List planExtentsRequestDescriptor = $convert.base64Decode(
    'ChJQbGFuRXh0ZW50c1JlcXVlc3QSRAoKZ2VuZXJhdGlvbhgBIAEoCzIkLmFjeWNsaWMuZmlsZX'
    'N5c3RlbS52Mi5HZW5lcmF0aW9uUmVmUgpnZW5lcmF0aW9uEhIKBHBhdGgYAiABKAlSBHBhdGgS'
    'NgoFcmFuZ2UYAyABKAsyIC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuQnl0ZVJhbmdlUgVyYW5nZR'
    'InCg9tYXhpbXVtX2V4dGVudHMYBCABKA1SDm1heGltdW1FeHRlbnRz');

@$core.Deprecated('Use planExtentsResponseDescriptor instead')
const PlanExtentsResponse$json = {
  '1': 'PlanExtentsResponse',
  '2': [
    {
      '1': 'extents',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Extent',
      '10': 'extents'
    },
    {'1': 'truncated', '3': 2, '4': 1, '5': 8, '10': 'truncated'},
  ],
};

/// Descriptor for `PlanExtentsResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List planExtentsResponseDescriptor = $convert.base64Decode(
    'ChNQbGFuRXh0ZW50c1Jlc3BvbnNlEjcKB2V4dGVudHMYASADKAsyHS5hY3ljbGljLmZpbGVzeX'
    'N0ZW0udjIuRXh0ZW50UgdleHRlbnRzEhwKCXRydW5jYXRlZBgCIAEoCFIJdHJ1bmNhdGVk');

@$core.Deprecated('Use createFileDescriptor instead')
const CreateFile$json = {
  '1': 'CreateFile',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'contents', '3': 2, '4': 1, '5': 12, '10': 'contents'},
    {
      '1': 'metadata',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Metadata',
      '10': 'metadata'
    },
  ],
};

/// Descriptor for `CreateFile`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createFileDescriptor = $convert.base64Decode(
    'CgpDcmVhdGVGaWxlEhIKBHBhdGgYASABKAlSBHBhdGgSGgoIY29udGVudHMYAiABKAxSCGNvbn'
    'RlbnRzEjsKCG1ldGFkYXRhGAMgASgLMh8uYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk1ldGFkYXRh'
    'UghtZXRhZGF0YQ==');

@$core.Deprecated('Use createDirectoryDescriptor instead')
const CreateDirectory$json = {
  '1': 'CreateDirectory',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {
      '1': 'metadata',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Metadata',
      '10': 'metadata'
    },
  ],
};

/// Descriptor for `CreateDirectory`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createDirectoryDescriptor = $convert.base64Decode(
    'Cg9DcmVhdGVEaXJlY3RvcnkSEgoEcGF0aBgBIAEoCVIEcGF0aBI7CghtZXRhZGF0YRgCIAEoCz'
    'IfLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5NZXRhZGF0YVIIbWV0YWRhdGE=');

@$core.Deprecated('Use createSymbolicLinkDescriptor instead')
const CreateSymbolicLink$json = {
  '1': 'CreateSymbolicLink',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'target', '3': 2, '4': 1, '5': 12, '10': 'target'},
    {
      '1': 'metadata',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Metadata',
      '10': 'metadata'
    },
  ],
};

/// Descriptor for `CreateSymbolicLink`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createSymbolicLinkDescriptor = $convert.base64Decode(
    'ChJDcmVhdGVTeW1ib2xpY0xpbmsSEgoEcGF0aBgBIAEoCVIEcGF0aBIWCgZ0YXJnZXQYAiABKA'
    'xSBnRhcmdldBI7CghtZXRhZGF0YRgDIAEoCzIfLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5NZXRh'
    'ZGF0YVIIbWV0YWRhdGE=');

@$core.Deprecated('Use removeDescriptor instead')
const Remove$json = {
  '1': 'Remove',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
  ],
};

/// Descriptor for `Remove`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List removeDescriptor =
    $convert.base64Decode('CgZSZW1vdmUSEgoEcGF0aBgBIAEoCVIEcGF0aA==');

@$core.Deprecated('Use renameDescriptor instead')
const Rename$json = {
  '1': 'Rename',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 9, '10': 'source'},
    {'1': 'destination', '3': 2, '4': 1, '5': 9, '10': 'destination'},
    {'1': 'replace', '3': 3, '4': 1, '5': 8, '10': 'replace'},
  ],
};

/// Descriptor for `Rename`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List renameDescriptor = $convert.base64Decode(
    'CgZSZW5hbWUSFgoGc291cmNlGAEgASgJUgZzb3VyY2USIAoLZGVzdGluYXRpb24YAiABKAlSC2'
    'Rlc3RpbmF0aW9uEhgKB3JlcGxhY2UYAyABKAhSB3JlcGxhY2U=');

@$core.Deprecated('Use hardLinkDescriptor instead')
const HardLink$json = {
  '1': 'HardLink',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 9, '10': 'source'},
    {'1': 'destination', '3': 2, '4': 1, '5': 9, '10': 'destination'},
  ],
};

/// Descriptor for `HardLink`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List hardLinkDescriptor = $convert.base64Decode(
    'CghIYXJkTGluaxIWCgZzb3VyY2UYASABKAlSBnNvdXJjZRIgCgtkZXN0aW5hdGlvbhgCIAEoCV'
    'ILZGVzdGluYXRpb24=');

@$core.Deprecated('Use writeDescriptor instead')
const Write$json = {
  '1': 'Write',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'offset', '3': 2, '4': 1, '5': 4, '10': 'offset'},
    {'1': 'contents', '3': 3, '4': 1, '5': 12, '10': 'contents'},
  ],
};

/// Descriptor for `Write`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List writeDescriptor = $convert.base64Decode(
    'CgVXcml0ZRISCgRwYXRoGAEgASgJUgRwYXRoEhYKBm9mZnNldBgCIAEoBFIGb2Zmc2V0EhoKCG'
    'NvbnRlbnRzGAMgASgMUghjb250ZW50cw==');

@$core.Deprecated('Use resizeDescriptor instead')
const Resize$json = {
  '1': 'Resize',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'logical_bytes', '3': 2, '4': 1, '5': 4, '10': 'logicalBytes'},
  ],
};

/// Descriptor for `Resize`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List resizeDescriptor = $convert.base64Decode(
    'CgZSZXNpemUSEgoEcGF0aBgBIAEoCVIEcGF0aBIjCg1sb2dpY2FsX2J5dGVzGAIgASgEUgxsb2'
    'dpY2FsQnl0ZXM=');

@$core.Deprecated('Use zeroRangeDescriptor instead')
const ZeroRange$json = {
  '1': 'ZeroRange',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {
      '1': 'range',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ByteRange',
      '10': 'range'
    },
    {'1': 'allocated', '3': 3, '4': 1, '5': 8, '10': 'allocated'},
    {'1': 'extend', '3': 4, '4': 1, '5': 8, '10': 'extend'},
  ],
};

/// Descriptor for `ZeroRange`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List zeroRangeDescriptor = $convert.base64Decode(
    'CglaZXJvUmFuZ2USEgoEcGF0aBgBIAEoCVIEcGF0aBI2CgVyYW5nZRgCIAEoCzIgLmFjeWNsaW'
    'MuZmlsZXN5c3RlbS52Mi5CeXRlUmFuZ2VSBXJhbmdlEhwKCWFsbG9jYXRlZBgDIAEoCFIJYWxs'
    'b2NhdGVkEhYKBmV4dGVuZBgEIAEoCFIGZXh0ZW5k');

@$core.Deprecated('Use preallocateDescriptor instead')
const Preallocate$json = {
  '1': 'Preallocate',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {
      '1': 'range',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ByteRange',
      '10': 'range'
    },
    {'1': 'keep_size', '3': 3, '4': 1, '5': 8, '10': 'keepSize'},
  ],
};

/// Descriptor for `Preallocate`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List preallocateDescriptor = $convert.base64Decode(
    'CgtQcmVhbGxvY2F0ZRISCgRwYXRoGAEgASgJUgRwYXRoEjYKBXJhbmdlGAIgASgLMiAuYWN5Y2'
    'xpYy5maWxlc3lzdGVtLnYyLkJ5dGVSYW5nZVIFcmFuZ2USGwoJa2VlcF9zaXplGAMgASgIUghr'
    'ZWVwU2l6ZQ==');

@$core.Deprecated('Use cloneRangeDescriptor instead')
const CloneRange$json = {
  '1': 'CloneRange',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 9, '10': 'source'},
    {'1': 'source_offset', '3': 2, '4': 1, '5': 4, '10': 'sourceOffset'},
    {'1': 'destination', '3': 3, '4': 1, '5': 9, '10': 'destination'},
    {
      '1': 'destination_offset',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'destinationOffset'
    },
    {'1': 'length', '3': 5, '4': 1, '5': 4, '10': 'length'},
  ],
};

/// Descriptor for `CloneRange`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List cloneRangeDescriptor = $convert.base64Decode(
    'CgpDbG9uZVJhbmdlEhYKBnNvdXJjZRgBIAEoCVIGc291cmNlEiMKDXNvdXJjZV9vZmZzZXQYAi'
    'ABKARSDHNvdXJjZU9mZnNldBIgCgtkZXN0aW5hdGlvbhgDIAEoCVILZGVzdGluYXRpb24SLQoS'
    'ZGVzdGluYXRpb25fb2Zmc2V0GAQgASgEUhFkZXN0aW5hdGlvbk9mZnNldBIWCgZsZW5ndGgYBS'
    'ABKARSBmxlbmd0aA==');

@$core.Deprecated('Use setMetadataDescriptor instead')
const SetMetadata$json = {
  '1': 'SetMetadata',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {
      '1': 'metadata',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Metadata',
      '10': 'metadata'
    },
  ],
};

/// Descriptor for `SetMetadata`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List setMetadataDescriptor = $convert.base64Decode(
    'CgtTZXRNZXRhZGF0YRISCgRwYXRoGAEgASgJUgRwYXRoEjsKCG1ldGFkYXRhGAIgASgLMh8uYW'
    'N5Y2xpYy5maWxlc3lzdGVtLnYyLk1ldGFkYXRhUghtZXRhZGF0YQ==');

@$core.Deprecated('Use createDirectoriesDescriptor instead')
const CreateDirectories$json = {
  '1': 'CreateDirectories',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
  ],
};

/// Descriptor for `CreateDirectories`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createDirectoriesDescriptor = $convert
    .base64Decode('ChFDcmVhdGVEaXJlY3RvcmllcxISCgRwYXRoGAEgASgJUgRwYXRo');

@$core.Deprecated('Use putFileDescriptor instead')
const PutFile$json = {
  '1': 'PutFile',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'contents', '3': 2, '4': 1, '5': 12, '10': 'contents'},
  ],
};

/// Descriptor for `PutFile`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List putFileDescriptor = $convert.base64Decode(
    'CgdQdXRGaWxlEhIKBHBhdGgYASABKAlSBHBhdGgSGgoIY29udGVudHMYAiABKAxSCGNvbnRlbn'
    'Rz');

@$core.Deprecated('Use copyFileDescriptor instead')
const CopyFile$json = {
  '1': 'CopyFile',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 9, '10': 'source'},
    {'1': 'destination', '3': 2, '4': 1, '5': 9, '10': 'destination'},
  ],
};

/// Descriptor for `CopyFile`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List copyFileDescriptor = $convert.base64Decode(
    'CghDb3B5RmlsZRIWCgZzb3VyY2UYASABKAlSBnNvdXJjZRIgCgtkZXN0aW5hdGlvbhgCIAEoCV'
    'ILZGVzdGluYXRpb24=');

@$core.Deprecated('Use mutationDescriptor instead')
const Mutation$json = {
  '1': 'Mutation',
  '2': [
    {
      '1': 'create_file',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.CreateFile',
      '9': 0,
      '10': 'createFile'
    },
    {
      '1': 'create_directory',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.CreateDirectory',
      '9': 0,
      '10': 'createDirectory'
    },
    {
      '1': 'create_symbolic_link',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.CreateSymbolicLink',
      '9': 0,
      '10': 'createSymbolicLink'
    },
    {
      '1': 'remove',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Remove',
      '9': 0,
      '10': 'remove'
    },
    {
      '1': 'rename',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Rename',
      '9': 0,
      '10': 'rename'
    },
    {
      '1': 'hard_link',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.HardLink',
      '9': 0,
      '10': 'hardLink'
    },
    {
      '1': 'write',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Write',
      '9': 0,
      '10': 'write'
    },
    {
      '1': 'resize',
      '3': 8,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Resize',
      '9': 0,
      '10': 'resize'
    },
    {
      '1': 'zero_range',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ZeroRange',
      '9': 0,
      '10': 'zeroRange'
    },
    {
      '1': 'preallocate',
      '3': 10,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Preallocate',
      '9': 0,
      '10': 'preallocate'
    },
    {
      '1': 'clone_range',
      '3': 11,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.CloneRange',
      '9': 0,
      '10': 'cloneRange'
    },
    {
      '1': 'set_metadata',
      '3': 12,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.SetMetadata',
      '9': 0,
      '10': 'setMetadata'
    },
    {
      '1': 'create_directories',
      '3': 13,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.CreateDirectories',
      '9': 0,
      '10': 'createDirectories'
    },
    {
      '1': 'put_file',
      '3': 14,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.PutFile',
      '9': 0,
      '10': 'putFile'
    },
    {
      '1': 'copy_file',
      '3': 15,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.CopyFile',
      '9': 0,
      '10': 'copyFile'
    },
  ],
  '8': [
    {'1': 'mutation'},
  ],
};

/// Descriptor for `Mutation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List mutationDescriptor = $convert.base64Decode(
    'CghNdXRhdGlvbhJECgtjcmVhdGVfZmlsZRgBIAEoCzIhLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi'
    '5DcmVhdGVGaWxlSABSCmNyZWF0ZUZpbGUSUwoQY3JlYXRlX2RpcmVjdG9yeRgCIAEoCzImLmFj'
    'eWNsaWMuZmlsZXN5c3RlbS52Mi5DcmVhdGVEaXJlY3RvcnlIAFIPY3JlYXRlRGlyZWN0b3J5El'
    '0KFGNyZWF0ZV9zeW1ib2xpY19saW5rGAMgASgLMikuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkNy'
    'ZWF0ZVN5bWJvbGljTGlua0gAUhJjcmVhdGVTeW1ib2xpY0xpbmsSNwoGcmVtb3ZlGAQgASgLMh'
    '0uYWN5Y2xpYy5maWxlc3lzdGVtLnYyLlJlbW92ZUgAUgZyZW1vdmUSNwoGcmVuYW1lGAUgASgL'
    'Mh0uYWN5Y2xpYy5maWxlc3lzdGVtLnYyLlJlbmFtZUgAUgZyZW5hbWUSPgoJaGFyZF9saW5rGA'
    'YgASgLMh8uYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkhhcmRMaW5rSABSCGhhcmRMaW5rEjQKBXdy'
    'aXRlGAcgASgLMhwuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLldyaXRlSABSBXdyaXRlEjcKBnJlc2'
    'l6ZRgIIAEoCzIdLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5SZXNpemVIAFIGcmVzaXplEkEKCnpl'
    'cm9fcmFuZ2UYCSABKAsyIC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuWmVyb1JhbmdlSABSCXplcm'
    '9SYW5nZRJGCgtwcmVhbGxvY2F0ZRgKIAEoCzIiLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5QcmVh'
    'bGxvY2F0ZUgAUgtwcmVhbGxvY2F0ZRJECgtjbG9uZV9yYW5nZRgLIAEoCzIhLmFjeWNsaWMuZm'
    'lsZXN5c3RlbS52Mi5DbG9uZVJhbmdlSABSCmNsb25lUmFuZ2USRwoMc2V0X21ldGFkYXRhGAwg'
    'ASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLlNldE1ldGFkYXRhSABSC3NldE1ldGFkYXRhEl'
    'kKEmNyZWF0ZV9kaXJlY3RvcmllcxgNIAEoCzIoLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5DcmVh'
    'dGVEaXJlY3Rvcmllc0gAUhFjcmVhdGVEaXJlY3RvcmllcxI7CghwdXRfZmlsZRgOIAEoCzIeLm'
    'FjeWNsaWMuZmlsZXN5c3RlbS52Mi5QdXRGaWxlSABSB3B1dEZpbGUSPgoJY29weV9maWxlGA8g'
    'ASgLMh8uYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkNvcHlGaWxlSABSCGNvcHlGaWxlQgoKCG11dG'
    'F0aW9u');

@$core.Deprecated('Use applyTransactionRequestDescriptor instead')
const ApplyTransactionRequest$json = {
  '1': 'ApplyTransactionRequest',
  '2': [
    {
      '1': 'base',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'base'
    },
    {
      '1': 'mutations',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Mutation',
      '10': 'mutations'
    },
    {
      '1': 'operation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
    {
      '1': 'maximum_conflicts',
      '3': 4,
      '4': 1,
      '5': 13,
      '10': 'maximumConflicts'
    },
  ],
};

/// Descriptor for `ApplyTransactionRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List applyTransactionRequestDescriptor = $convert.base64Decode(
    'ChdBcHBseVRyYW5zYWN0aW9uUmVxdWVzdBI4CgRiYXNlGAEgASgLMiQuYWN5Y2xpYy5maWxlc3'
    'lzdGVtLnYyLkdlbmVyYXRpb25SZWZSBGJhc2USPQoJbXV0YXRpb25zGAIgAygLMh8uYWN5Y2xp'
    'Yy5maWxlc3lzdGVtLnYyLk11dGF0aW9uUgltdXRhdGlvbnMSRQoJb3BlcmF0aW9uGAMgASgLMi'
    'cuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk9wZXJhdGlvbk9wdGlvbnNSCW9wZXJhdGlvbhIrChFt'
    'YXhpbXVtX2NvbmZsaWN0cxgEIAEoDVIQbWF4aW11bUNvbmZsaWN0cw==');

@$core.Deprecated('Use rebaseTransactionRequestDescriptor instead')
const RebaseTransactionRequest$json = {
  '1': 'RebaseTransactionRequest',
  '2': [
    {
      '1': 'base',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'base'
    },
    {
      '1': 'mutations',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Mutation',
      '10': 'mutations'
    },
    {
      '1': 'maximum_conflicts',
      '3': 3,
      '4': 1,
      '5': 13,
      '10': 'maximumConflicts'
    },
    {
      '1': 'operation',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `RebaseTransactionRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List rebaseTransactionRequestDescriptor = $convert.base64Decode(
    'ChhSZWJhc2VUcmFuc2FjdGlvblJlcXVlc3QSOAoEYmFzZRgBIAEoCzIkLmFjeWNsaWMuZmlsZX'
    'N5c3RlbS52Mi5HZW5lcmF0aW9uUmVmUgRiYXNlEj0KCW11dGF0aW9ucxgCIAMoCzIfLmFjeWNs'
    'aWMuZmlsZXN5c3RlbS52Mi5NdXRhdGlvblIJbXV0YXRpb25zEisKEW1heGltdW1fY29uZmxpY3'
    'RzGAMgASgNUhBtYXhpbXVtQ29uZmxpY3RzEkUKCW9wZXJhdGlvbhgEIAEoCzInLmFjeWNsaWMu'
    'ZmlsZXN5c3RlbS52Mi5PcGVyYXRpb25PcHRpb25zUglvcGVyYXRpb24=');

@$core.Deprecated('Use rebaseTransactionResponseDescriptor instead')
const RebaseTransactionResponse$json = {
  '1': 'RebaseTransactionResponse',
  '2': [
    {
      '1': 'base',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'base'
    },
    {
      '1': 'conflicts',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Conflict',
      '10': 'conflicts'
    },
    {'1': 'truncated', '3': 3, '4': 1, '5': 8, '10': 'truncated'},
  ],
};

/// Descriptor for `RebaseTransactionResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List rebaseTransactionResponseDescriptor = $convert.base64Decode(
    'ChlSZWJhc2VUcmFuc2FjdGlvblJlc3BvbnNlEjgKBGJhc2UYASABKAsyJC5hY3ljbGljLmZpbG'
    'VzeXN0ZW0udjIuR2VuZXJhdGlvblJlZlIEYmFzZRI9Cgljb25mbGljdHMYAiADKAsyHy5hY3lj'
    'bGljLmZpbGVzeXN0ZW0udjIuQ29uZmxpY3RSCWNvbmZsaWN0cxIcCgl0cnVuY2F0ZWQYAyABKA'
    'hSCXRydW5jYXRlZA==');

@$core.Deprecated('Use forkWorkspaceRequestDescriptor instead')
const ForkWorkspaceRequest$json = {
  '1': 'ForkWorkspaceRequest',
  '2': [
    {
      '1': 'source',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'source'
    },
    {'1': 'destination_name', '3': 2, '4': 1, '5': 9, '10': 'destinationName'},
    {
      '1': 'operation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `ForkWorkspaceRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkWorkspaceRequestDescriptor = $convert.base64Decode(
    'ChRGb3JrV29ya3NwYWNlUmVxdWVzdBI8CgZzb3VyY2UYASABKAsyJC5hY3ljbGljLmZpbGVzeX'
    'N0ZW0udjIuR2VuZXJhdGlvblJlZlIGc291cmNlEikKEGRlc3RpbmF0aW9uX25hbWUYAiABKAlS'
    'D2Rlc3RpbmF0aW9uTmFtZRJFCglvcGVyYXRpb24YAyABKAsyJy5hY3ljbGljLmZpbGVzeXN0ZW'
    '0udjIuT3BlcmF0aW9uT3B0aW9uc1IJb3BlcmF0aW9u');

@$core.Deprecated('Use diffRequestDescriptor instead')
const DiffRequest$json = {
  '1': 'DiffRequest',
  '2': [
    {
      '1': 'from',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'from'
    },
    {
      '1': 'to',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'to'
    },
    {'1': 'maximum_changes', '3': 3, '4': 1, '5': 13, '10': 'maximumChanges'},
  ],
};

/// Descriptor for `DiffRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List diffRequestDescriptor = $convert.base64Decode(
    'CgtEaWZmUmVxdWVzdBI4CgRmcm9tGAEgASgLMiQuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkdlbm'
    'VyYXRpb25SZWZSBGZyb20SNAoCdG8YAiABKAsyJC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuR2Vu'
    'ZXJhdGlvblJlZlICdG8SJwoPbWF4aW11bV9jaGFuZ2VzGAMgASgNUg5tYXhpbXVtQ2hhbmdlcw'
    '==');

@$core.Deprecated('Use logicalNameDescriptor instead')
const LogicalName$json = {
  '1': 'LogicalName',
  '2': [
    {
      '1': 'encoding',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.NameEncoding',
      '10': 'encoding'
    },
    {'1': 'bytes', '3': 2, '4': 1, '5': 12, '10': 'bytes'},
  ],
};

/// Descriptor for `LogicalName`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List logicalNameDescriptor = $convert.base64Decode(
    'CgtMb2dpY2FsTmFtZRI/CghlbmNvZGluZxgBIAEoDjIjLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi'
    '5OYW1lRW5jb2RpbmdSCGVuY29kaW5nEhQKBWJ5dGVzGAIgASgMUgVieXRlcw==');

@$core.Deprecated('Use fileRecordSnapshotDescriptor instead')
const FileRecordSnapshot$json = {
  '1': 'FileRecordSnapshot',
  '2': [
    {'1': 'file_id', '3': 1, '4': 1, '5': 12, '10': 'fileId'},
    {
      '1': 'file_kind',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.FileKind',
      '10': 'fileKind'
    },
    {'1': 'link_count', '3': 3, '4': 1, '5': 4, '10': 'linkCount'},
    {'1': 'metadata_object', '3': 4, '4': 1, '5': 12, '10': 'metadataObject'},
    {'1': 'payload_kind', '3': 5, '4': 1, '5': 9, '10': 'payloadKind'},
    {
      '1': 'logical_bytes',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU64',
      '10': 'logicalBytes'
    },
    {'1': 'payload_object', '3': 7, '4': 1, '5': 12, '10': 'payloadObject'},
    {'1': 'inline_bytes', '3': 8, '4': 1, '5': 12, '10': 'inlineBytes'},
    {
      '1': 'device_major',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU32',
      '10': 'deviceMajor'
    },
    {
      '1': 'device_minor',
      '3': 10,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OptionalU32',
      '10': 'deviceMinor'
    },
  ],
};

/// Descriptor for `FileRecordSnapshot`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List fileRecordSnapshotDescriptor = $convert.base64Decode(
    'ChJGaWxlUmVjb3JkU25hcHNob3QSFwoHZmlsZV9pZBgBIAEoDFIGZmlsZUlkEjwKCWZpbGVfa2'
    'luZBgCIAEoDjIfLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5GaWxlS2luZFIIZmlsZUtpbmQSHQoK'
    'bGlua19jb3VudBgDIAEoBFIJbGlua0NvdW50EicKD21ldGFkYXRhX29iamVjdBgEIAEoDFIObW'
    'V0YWRhdGFPYmplY3QSIQoMcGF5bG9hZF9raW5kGAUgASgJUgtwYXlsb2FkS2luZBJHCg1sb2dp'
    'Y2FsX2J5dGVzGAYgASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk9wdGlvbmFsVTY0Ugxsb2'
    'dpY2FsQnl0ZXMSJQoOcGF5bG9hZF9vYmplY3QYByABKAxSDXBheWxvYWRPYmplY3QSIQoMaW5s'
    'aW5lX2J5dGVzGAggASgMUgtpbmxpbmVCeXRlcxJFCgxkZXZpY2VfbWFqb3IYCSABKAsyIi5hY3'
    'ljbGljLmZpbGVzeXN0ZW0udjIuT3B0aW9uYWxVMzJSC2RldmljZU1ham9yEkUKDGRldmljZV9t'
    'aW5vchgKIAEoCzIiLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5PcHRpb25hbFUzMlILZGV2aWNlTW'
    'lub3I=');

@$core.Deprecated('Use fileRecordChangeDescriptor instead')
const FileRecordChange$json = {
  '1': 'FileRecordChange',
  '2': [
    {'1': 'file_id', '3': 1, '4': 1, '5': 12, '10': 'fileId'},
    {
      '1': 'before',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileRecordSnapshot',
      '10': 'before'
    },
    {
      '1': 'after',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileRecordSnapshot',
      '10': 'after'
    },
  ],
};

/// Descriptor for `FileRecordChange`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List fileRecordChangeDescriptor = $convert.base64Decode(
    'ChBGaWxlUmVjb3JkQ2hhbmdlEhcKB2ZpbGVfaWQYASABKAxSBmZpbGVJZBJBCgZiZWZvcmUYAi'
    'ABKAsyKS5hY3ljbGljLmZpbGVzeXN0ZW0udjIuRmlsZVJlY29yZFNuYXBzaG90UgZiZWZvcmUS'
    'PwoFYWZ0ZXIYAyABKAsyKS5hY3ljbGljLmZpbGVzeXN0ZW0udjIuRmlsZVJlY29yZFNuYXBzaG'
    '90UgVhZnRlcg==');

@$core.Deprecated('Use treeEntrySnapshotDescriptor instead')
const TreeEntrySnapshot$json = {
  '1': 'TreeEntrySnapshot',
  '2': [
    {
      '1': 'name',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.LogicalName',
      '10': 'name'
    },
    {'1': 'file_id', '3': 2, '4': 1, '5': 12, '10': 'fileId'},
    {
      '1': 'file_kind',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.FileKind',
      '10': 'fileKind'
    },
  ],
};

/// Descriptor for `TreeEntrySnapshot`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List treeEntrySnapshotDescriptor = $convert.base64Decode(
    'ChFUcmVlRW50cnlTbmFwc2hvdBI2CgRuYW1lGAEgASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLn'
    'YyLkxvZ2ljYWxOYW1lUgRuYW1lEhcKB2ZpbGVfaWQYAiABKAxSBmZpbGVJZBI8CglmaWxlX2tp'
    'bmQYAyABKA4yHy5hY3ljbGljLmZpbGVzeXN0ZW0udjIuRmlsZUtpbmRSCGZpbGVLaW5k');

@$core.Deprecated('Use directoryBindingChangeDescriptor instead')
const DirectoryBindingChange$json = {
  '1': 'DirectoryBindingChange',
  '2': [
    {'1': 'directory_id', '3': 1, '4': 1, '5': 12, '10': 'directoryId'},
    {
      '1': 'name',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.LogicalName',
      '10': 'name'
    },
    {
      '1': 'before',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.TreeEntrySnapshot',
      '10': 'before'
    },
    {
      '1': 'after',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.TreeEntrySnapshot',
      '10': 'after'
    },
  ],
};

/// Descriptor for `DirectoryBindingChange`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List directoryBindingChangeDescriptor = $convert.base64Decode(
    'ChZEaXJlY3RvcnlCaW5kaW5nQ2hhbmdlEiEKDGRpcmVjdG9yeV9pZBgBIAEoDFILZGlyZWN0b3'
    'J5SWQSNgoEbmFtZRgCIAEoCzIiLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5Mb2dpY2FsTmFtZVIE'
    'bmFtZRJACgZiZWZvcmUYAyABKAsyKC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuVHJlZUVudHJ5U2'
    '5hcHNob3RSBmJlZm9yZRI+CgVhZnRlchgEIAEoCzIoLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5U'
    'cmVlRW50cnlTbmFwc2hvdFIFYWZ0ZXI=');

@$core.Deprecated('Use workCountersDescriptor instead')
const WorkCounters$json = {
  '1': 'WorkCounters',
  '2': [
    {
      '1': 'authority_records_read',
      '3': 1,
      '4': 1,
      '5': 4,
      '10': 'authorityRecordsRead'
    },
    {
      '1': 'authority_records_appended',
      '3': 2,
      '4': 1,
      '5': 4,
      '10': 'authorityRecordsAppended'
    },
    {
      '1': 'authority_bytes_read',
      '3': 3,
      '4': 1,
      '5': 4,
      '10': 'authorityBytesRead'
    },
    {
      '1': 'authority_bytes_written',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'authorityBytesWritten'
    },
    {'1': 'object_probes', '3': 5, '4': 1, '5': 4, '10': 'objectProbes'},
    {
      '1': 'backend_read_operations',
      '3': 6,
      '4': 1,
      '5': 4,
      '10': 'backendReadOperations'
    },
    {
      '1': 'backend_write_operations',
      '3': 7,
      '4': 1,
      '5': 4,
      '10': 'backendWriteOperations'
    },
    {
      '1': 'durability_operations',
      '3': 8,
      '4': 1,
      '5': 4,
      '10': 'durabilityOperations'
    },
    {'1': 'page_reads', '3': 9, '4': 1, '5': 4, '10': 'pageReads'},
    {'1': 'page_writes', '3': 10, '4': 1, '5': 4, '10': 'pageWrites'},
    {
      '1': 'object_bytes_read',
      '3': 11,
      '4': 1,
      '5': 4,
      '10': 'objectBytesRead'
    },
    {
      '1': 'object_bytes_written',
      '3': 12,
      '4': 1,
      '5': 4,
      '10': 'objectBytesWritten'
    },
    {'1': 'bytes_hashed', '3': 13, '4': 1, '5': 4, '10': 'bytesHashed'},
    {'1': 'bytes_copied', '3': 14, '4': 1, '5': 4, '10': 'bytesCopied'},
    {'1': 'bytes_encoded', '3': 15, '4': 1, '5': 4, '10': 'bytesEncoded'},
    {
      '1': 'source_bytes_read',
      '3': 16,
      '4': 1,
      '5': 4,
      '10': 'sourceBytesRead'
    },
    {'1': 'output_bytes', '3': 17, '4': 1, '5': 4, '10': 'outputBytes'},
    {'1': 'items_examined', '3': 18, '4': 1, '5': 4, '10': 'itemsExamined'},
    {'1': 'items_returned', '3': 19, '4': 1, '5': 4, '10': 'itemsReturned'},
    {
      '1': 'allocation_operations',
      '3': 20,
      '4': 1,
      '5': 4,
      '10': 'allocationOperations'
    },
    {
      '1': 'peak_allocation_bytes',
      '3': 21,
      '4': 1,
      '5': 4,
      '10': 'peakAllocationBytes'
    },
    {
      '1': 'materializations',
      '3': 22,
      '4': 1,
      '5': 4,
      '10': 'materializations'
    },
    {
      '1': 'source_path_components',
      '3': 23,
      '4': 1,
      '5': 4,
      '10': 'sourcePathComponents'
    },
    {
      '1': 'source_entries_visited',
      '3': 24,
      '4': 1,
      '5': 4,
      '10': 'sourceEntriesVisited'
    },
  ],
};

/// Descriptor for `WorkCounters`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workCountersDescriptor = $convert.base64Decode(
    'CgxXb3JrQ291bnRlcnMSNAoWYXV0aG9yaXR5X3JlY29yZHNfcmVhZBgBIAEoBFIUYXV0aG9yaX'
    'R5UmVjb3Jkc1JlYWQSPAoaYXV0aG9yaXR5X3JlY29yZHNfYXBwZW5kZWQYAiABKARSGGF1dGhv'
    'cml0eVJlY29yZHNBcHBlbmRlZBIwChRhdXRob3JpdHlfYnl0ZXNfcmVhZBgDIAEoBFISYXV0aG'
    '9yaXR5Qnl0ZXNSZWFkEjYKF2F1dGhvcml0eV9ieXRlc193cml0dGVuGAQgASgEUhVhdXRob3Jp'
    'dHlCeXRlc1dyaXR0ZW4SIwoNb2JqZWN0X3Byb2JlcxgFIAEoBFIMb2JqZWN0UHJvYmVzEjYKF2'
    'JhY2tlbmRfcmVhZF9vcGVyYXRpb25zGAYgASgEUhViYWNrZW5kUmVhZE9wZXJhdGlvbnMSOAoY'
    'YmFja2VuZF93cml0ZV9vcGVyYXRpb25zGAcgASgEUhZiYWNrZW5kV3JpdGVPcGVyYXRpb25zEj'
    'MKFWR1cmFiaWxpdHlfb3BlcmF0aW9ucxgIIAEoBFIUZHVyYWJpbGl0eU9wZXJhdGlvbnMSHQoK'
    'cGFnZV9yZWFkcxgJIAEoBFIJcGFnZVJlYWRzEh8KC3BhZ2Vfd3JpdGVzGAogASgEUgpwYWdlV3'
    'JpdGVzEioKEW9iamVjdF9ieXRlc19yZWFkGAsgASgEUg9vYmplY3RCeXRlc1JlYWQSMAoUb2Jq'
    'ZWN0X2J5dGVzX3dyaXR0ZW4YDCABKARSEm9iamVjdEJ5dGVzV3JpdHRlbhIhCgxieXRlc19oYX'
    'NoZWQYDSABKARSC2J5dGVzSGFzaGVkEiEKDGJ5dGVzX2NvcGllZBgOIAEoBFILYnl0ZXNDb3Bp'
    'ZWQSIwoNYnl0ZXNfZW5jb2RlZBgPIAEoBFIMYnl0ZXNFbmNvZGVkEioKEXNvdXJjZV9ieXRlc1'
    '9yZWFkGBAgASgEUg9zb3VyY2VCeXRlc1JlYWQSIQoMb3V0cHV0X2J5dGVzGBEgASgEUgtvdXRw'
    'dXRCeXRlcxIlCg5pdGVtc19leGFtaW5lZBgSIAEoBFINaXRlbXNFeGFtaW5lZBIlCg5pdGVtc1'
    '9yZXR1cm5lZBgTIAEoBFINaXRlbXNSZXR1cm5lZBIzChVhbGxvY2F0aW9uX29wZXJhdGlvbnMY'
    'FCABKARSFGFsbG9jYXRpb25PcGVyYXRpb25zEjIKFXBlYWtfYWxsb2NhdGlvbl9ieXRlcxgVIA'
    'EoBFITcGVha0FsbG9jYXRpb25CeXRlcxIqChBtYXRlcmlhbGl6YXRpb25zGBYgASgEUhBtYXRl'
    'cmlhbGl6YXRpb25zEjQKFnNvdXJjZV9wYXRoX2NvbXBvbmVudHMYFyABKARSFHNvdXJjZVBhdG'
    'hDb21wb25lbnRzEjQKFnNvdXJjZV9lbnRyaWVzX3Zpc2l0ZWQYGCABKARSFHNvdXJjZUVudHJp'
    'ZXNWaXNpdGVk');

@$core.Deprecated('Use diffResponseDescriptor instead')
const DiffResponse$json = {
  '1': 'DiffResponse',
  '2': [
    {
      '1': 'from',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'from'
    },
    {
      '1': 'to',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'to'
    },
    {
      '1': 'files',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileRecordChange',
      '10': 'files'
    },
    {
      '1': 'bindings',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.DirectoryBindingChange',
      '10': 'bindings'
    },
    {'1': 'truncated', '3': 5, '4': 1, '5': 8, '10': 'truncated'},
    {
      '1': 'work',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkCounters',
      '10': 'work'
    },
  ],
};

/// Descriptor for `DiffResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List diffResponseDescriptor = $convert.base64Decode(
    'CgxEaWZmUmVzcG9uc2USOAoEZnJvbRgBIAEoCzIkLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5HZW'
    '5lcmF0aW9uUmVmUgRmcm9tEjQKAnRvGAIgASgLMiQuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkdl'
    'bmVyYXRpb25SZWZSAnRvEj0KBWZpbGVzGAMgAygLMicuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk'
    'ZpbGVSZWNvcmRDaGFuZ2VSBWZpbGVzEkkKCGJpbmRpbmdzGAQgAygLMi0uYWN5Y2xpYy5maWxl'
    'c3lzdGVtLnYyLkRpcmVjdG9yeUJpbmRpbmdDaGFuZ2VSCGJpbmRpbmdzEhwKCXRydW5jYXRlZB'
    'gFIAEoCFIJdHJ1bmNhdGVkEjcKBHdvcmsYBiABKAsyIy5hY3ljbGljLmZpbGVzeXN0ZW0udjIu'
    'V29ya0NvdW50ZXJzUgR3b3Jr');

@$core.Deprecated('Use rebaseRequestDescriptor instead')
const RebaseRequest$json = {
  '1': 'RebaseRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {
      '1': 'maximum_conflicts',
      '3': 3,
      '4': 1,
      '5': 13,
      '10': 'maximumConflicts'
    },
    {
      '1': 'operation',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
    {
      '1': 'maximum_generations',
      '3': 5,
      '4': 1,
      '5': 13,
      '10': 'maximumGenerations'
    },
    {'1': 'maximum_changes', '3': 6, '4': 1, '5': 13, '10': 'maximumChanges'},
  ],
  '9': [
    {'1': 2, '2': 3},
  ],
};

/// Descriptor for `RebaseRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List rebaseRequestDescriptor = $convert.base64Decode(
    'Cg1SZWJhc2VSZXF1ZXN0EkEKCXdvcmtzcGFjZRgBIAEoCzIjLmFjeWNsaWMuZmlsZXN5c3RlbS'
    '52Mi5Xb3Jrc3BhY2VSZWZSCXdvcmtzcGFjZRIrChFtYXhpbXVtX2NvbmZsaWN0cxgDIAEoDVIQ'
    'bWF4aW11bUNvbmZsaWN0cxJFCglvcGVyYXRpb24YBCABKAsyJy5hY3ljbGljLmZpbGVzeXN0ZW'
    '0udjIuT3BlcmF0aW9uT3B0aW9uc1IJb3BlcmF0aW9uEi8KE21heGltdW1fZ2VuZXJhdGlvbnMY'
    'BSABKA1SEm1heGltdW1HZW5lcmF0aW9ucxInCg9tYXhpbXVtX2NoYW5nZXMYBiABKA1SDm1heG'
    'ltdW1DaGFuZ2VzSgQIAhAD');

@$core.Deprecated('Use fileConflictDescriptor instead')
const FileConflict$json = {
  '1': 'FileConflict',
  '2': [
    {'1': 'file_id', '3': 1, '4': 1, '5': 12, '10': 'fileId'},
  ],
};

/// Descriptor for `FileConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List fileConflictDescriptor = $convert
    .base64Decode('CgxGaWxlQ29uZmxpY3QSFwoHZmlsZV9pZBgBIAEoDFIGZmlsZUlk');

@$core.Deprecated('Use contentConflictDescriptor instead')
const ContentConflict$json = {
  '1': 'ContentConflict',
  '2': [
    {'1': 'file_id', '3': 1, '4': 1, '5': 12, '10': 'fileId'},
    {
      '1': 'range',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ByteRange',
      '10': 'range'
    },
  ],
};

/// Descriptor for `ContentConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List contentConflictDescriptor = $convert.base64Decode(
    'Cg9Db250ZW50Q29uZmxpY3QSFwoHZmlsZV9pZBgBIAEoDFIGZmlsZUlkEjYKBXJhbmdlGAIgAS'
    'gLMiAuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkJ5dGVSYW5nZVIFcmFuZ2U=');

@$core.Deprecated('Use sparseConflictDescriptor instead')
const SparseConflict$json = {
  '1': 'SparseConflict',
  '2': [
    {'1': 'file_id', '3': 1, '4': 1, '5': 12, '10': 'fileId'},
    {'1': 'offset', '3': 2, '4': 1, '5': 4, '10': 'offset'},
    {
      '1': 'target',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.SparseTarget',
      '10': 'target'
    },
  ],
};

/// Descriptor for `SparseConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List sparseConflictDescriptor = $convert.base64Decode(
    'Cg5TcGFyc2VDb25mbGljdBIXCgdmaWxlX2lkGAEgASgMUgZmaWxlSWQSFgoGb2Zmc2V0GAIgAS'
    'gEUgZvZmZzZXQSOwoGdGFyZ2V0GAMgASgOMiMuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLlNwYXJz'
    'ZVRhcmdldFIGdGFyZ2V0');

@$core.Deprecated('Use directoryNameConflictDescriptor instead')
const DirectoryNameConflict$json = {
  '1': 'DirectoryNameConflict',
  '2': [
    {'1': 'directory_id', '3': 1, '4': 1, '5': 12, '10': 'directoryId'},
    {
      '1': 'name',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.LogicalName',
      '10': 'name'
    },
  ],
};

/// Descriptor for `DirectoryNameConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List directoryNameConflictDescriptor = $convert.base64Decode(
    'ChVEaXJlY3RvcnlOYW1lQ29uZmxpY3QSIQoMZGlyZWN0b3J5X2lkGAEgASgMUgtkaXJlY3Rvcn'
    'lJZBI2CgRuYW1lGAIgASgLMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkxvZ2ljYWxOYW1lUgRu'
    'YW1l');

@$core.Deprecated('Use directoryRangeConflictDescriptor instead')
const DirectoryRangeConflict$json = {
  '1': 'DirectoryRangeConflict',
  '2': [
    {'1': 'directory_id', '3': 1, '4': 1, '5': 12, '10': 'directoryId'},
    {
      '1': 'after',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.LogicalName',
      '10': 'after'
    },
    {'1': 'maximum_entries', '3': 3, '4': 1, '5': 13, '10': 'maximumEntries'},
  ],
};

/// Descriptor for `DirectoryRangeConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List directoryRangeConflictDescriptor = $convert.base64Decode(
    'ChZEaXJlY3RvcnlSYW5nZUNvbmZsaWN0EiEKDGRpcmVjdG9yeV9pZBgBIAEoDFILZGlyZWN0b3'
    'J5SWQSOAoFYWZ0ZXIYAiABKAsyIi5hY3ljbGljLmZpbGVzeXN0ZW0udjIuTG9naWNhbE5hbWVS'
    'BWFmdGVyEicKD21heGltdW1fZW50cmllcxgDIAEoDVIObWF4aW11bUVudHJpZXM=');

@$core.Deprecated('Use conflictDescriptor instead')
const Conflict$json = {
  '1': 'Conflict',
  '2': [
    {
      '1': 'file_record',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileConflict',
      '9': 0,
      '10': 'fileRecord'
    },
    {
      '1': 'metadata',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileConflict',
      '9': 0,
      '10': 'metadata'
    },
    {
      '1': 'file_length',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileConflict',
      '9': 0,
      '10': 'fileLength'
    },
    {
      '1': 'content_range',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ContentConflict',
      '9': 0,
      '10': 'contentRange'
    },
    {
      '1': 'sparse_seek',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.SparseConflict',
      '9': 0,
      '10': 'sparseSeek'
    },
    {
      '1': 'directory_name',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.DirectoryNameConflict',
      '9': 0,
      '10': 'directoryName'
    },
    {
      '1': 'directory_range',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.DirectoryRangeConflict',
      '9': 0,
      '10': 'directoryRange'
    },
    {
      '1': 'use',
      '3': 8,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.ConflictUse',
      '10': 'use'
    },
    {'1': 'expected_digest', '3': 9, '4': 1, '5': 12, '10': 'expectedDigest'},
    {'1': 'actual_digest', '3': 10, '4': 1, '5': 12, '10': 'actualDigest'},
  ],
  '8': [
    {'1': 'region'},
  ],
};

/// Descriptor for `Conflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List conflictDescriptor = $convert.base64Decode(
    'CghDb25mbGljdBJGCgtmaWxlX3JlY29yZBgBIAEoCzIjLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi'
    '5GaWxlQ29uZmxpY3RIAFIKZmlsZVJlY29yZBJBCghtZXRhZGF0YRgCIAEoCzIjLmFjeWNsaWMu'
    'ZmlsZXN5c3RlbS52Mi5GaWxlQ29uZmxpY3RIAFIIbWV0YWRhdGESRgoLZmlsZV9sZW5ndGgYAy'
    'ABKAsyIy5hY3ljbGljLmZpbGVzeXN0ZW0udjIuRmlsZUNvbmZsaWN0SABSCmZpbGVMZW5ndGgS'
    'TQoNY29udGVudF9yYW5nZRgEIAEoCzImLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5Db250ZW50Q2'
    '9uZmxpY3RIAFIMY29udGVudFJhbmdlEkgKC3NwYXJzZV9zZWVrGAUgASgLMiUuYWN5Y2xpYy5m'
    'aWxlc3lzdGVtLnYyLlNwYXJzZUNvbmZsaWN0SABSCnNwYXJzZVNlZWsSVQoOZGlyZWN0b3J5X2'
    '5hbWUYBiABKAsyLC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuRGlyZWN0b3J5TmFtZUNvbmZsaWN0'
    'SABSDWRpcmVjdG9yeU5hbWUSWAoPZGlyZWN0b3J5X3JhbmdlGAcgASgLMi0uYWN5Y2xpYy5maW'
    'xlc3lzdGVtLnYyLkRpcmVjdG9yeVJhbmdlQ29uZmxpY3RIAFIOZGlyZWN0b3J5UmFuZ2USNAoD'
    'dXNlGAggASgOMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkNvbmZsaWN0VXNlUgN1c2USJwoPZX'
    'hwZWN0ZWRfZGlnZXN0GAkgASgMUg5leHBlY3RlZERpZ2VzdBIjCg1hY3R1YWxfZGlnZXN0GAog'
    'ASgMUgxhY3R1YWxEaWdlc3RCCAoGcmVnaW9u');

@$core.Deprecated('Use rebaseResponseDescriptor instead')
const RebaseResponse$json = {
  '1': 'RebaseResponse',
  '2': [
    {
      '1': 'status',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.RebaseStatus',
      '10': 'status'
    },
    {
      '1': 'generation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {
      '1': 'conflicts',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Conflict',
      '10': 'conflicts'
    },
    {'1': 'truncated', '3': 4, '4': 1, '5': 8, '10': 'truncated'},
  ],
};

/// Descriptor for `RebaseResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List rebaseResponseDescriptor = $convert.base64Decode(
    'Cg5SZWJhc2VSZXNwb25zZRI7CgZzdGF0dXMYASABKA4yIy5hY3ljbGljLmZpbGVzeXN0ZW0udj'
    'IuUmViYXNlU3RhdHVzUgZzdGF0dXMSRAoKZ2VuZXJhdGlvbhgCIAEoCzIkLmFjeWNsaWMuZmls'
    'ZXN5c3RlbS52Mi5HZW5lcmF0aW9uUmVmUgpnZW5lcmF0aW9uEj0KCWNvbmZsaWN0cxgDIAMoCz'
    'IfLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi5Db25mbGljdFIJY29uZmxpY3RzEhwKCXRydW5jYXRl'
    'ZBgEIAEoCFIJdHJ1bmNhdGVk');

@$core.Deprecated('Use planJoinRequestDescriptor instead')
const PlanJoinRequest$json = {
  '1': 'PlanJoinRequest',
  '2': [
    {
      '1': 'source',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'source'
    },
    {
      '1': 'target',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'target'
    },
    {'1': 'maximum_changes', '3': 3, '4': 1, '5': 13, '10': 'maximumChanges'},
    {
      '1': 'maximum_conflicts',
      '3': 4,
      '4': 1,
      '5': 13,
      '10': 'maximumConflicts'
    },
    {
      '1': 'maximum_generations',
      '3': 5,
      '4': 1,
      '5': 13,
      '10': 'maximumGenerations'
    },
    {
      '1': 'history',
      '3': 6,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.JoinHistory',
      '10': 'history'
    },
  ],
};

/// Descriptor for `PlanJoinRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List planJoinRequestDescriptor = $convert.base64Decode(
    'Cg9QbGFuSm9pblJlcXVlc3QSPAoGc291cmNlGAEgASgLMiQuYWN5Y2xpYy5maWxlc3lzdGVtLn'
    'YyLkdlbmVyYXRpb25SZWZSBnNvdXJjZRI8CgZ0YXJnZXQYAiABKAsyJC5hY3ljbGljLmZpbGVz'
    'eXN0ZW0udjIuR2VuZXJhdGlvblJlZlIGdGFyZ2V0EicKD21heGltdW1fY2hhbmdlcxgDIAEoDV'
    'IObWF4aW11bUNoYW5nZXMSKwoRbWF4aW11bV9jb25mbGljdHMYBCABKA1SEG1heGltdW1Db25m'
    'bGljdHMSLwoTbWF4aW11bV9nZW5lcmF0aW9ucxgFIAEoDVISbWF4aW11bUdlbmVyYXRpb25zEj'
    'wKB2hpc3RvcnkYBiABKA4yIi5hY3ljbGljLmZpbGVzeXN0ZW0udjIuSm9pbkhpc3RvcnlSB2hp'
    'c3Rvcnk=');

@$core.Deprecated('Use joinPlanDescriptor instead')
const JoinPlan$json = {
  '1': 'JoinPlan',
  '2': [
    {'1': 'plan_id', '3': 1, '4': 1, '5': 12, '10': 'planId'},
    {
      '1': 'source',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'source'
    },
    {
      '1': 'expected_target',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'expectedTarget'
    },
    {
      '1': 'file_changes',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.FileRecordChange',
      '10': 'fileChanges'
    },
    {
      '1': 'conflicts',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Conflict',
      '10': 'conflicts'
    },
    {'1': 'truncated', '3': 6, '4': 1, '5': 8, '10': 'truncated'},
    {
      '1': 'common_ancestor',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'commonAncestor'
    },
    {
      '1': 'maximum_generations',
      '3': 8,
      '4': 1,
      '5': 13,
      '10': 'maximumGenerations'
    },
    {'1': 'maximum_changes', '3': 9, '4': 1, '5': 13, '10': 'maximumChanges'},
    {
      '1': 'maximum_conflicts',
      '3': 10,
      '4': 1,
      '5': 13,
      '10': 'maximumConflicts'
    },
    {
      '1': 'history',
      '3': 11,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.JoinHistory',
      '10': 'history'
    },
    {
      '1': 'binding_changes',
      '3': 12,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.DirectoryBindingChange',
      '10': 'bindingChanges'
    },
  ],
};

/// Descriptor for `JoinPlan`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List joinPlanDescriptor = $convert.base64Decode(
    'CghKb2luUGxhbhIXCgdwbGFuX2lkGAEgASgMUgZwbGFuSWQSPAoGc291cmNlGAIgASgLMiQuYW'
    'N5Y2xpYy5maWxlc3lzdGVtLnYyLkdlbmVyYXRpb25SZWZSBnNvdXJjZRJNCg9leHBlY3RlZF90'
    'YXJnZXQYAyABKAsyJC5hY3ljbGljLmZpbGVzeXN0ZW0udjIuR2VuZXJhdGlvblJlZlIOZXhwZW'
    'N0ZWRUYXJnZXQSSgoMZmlsZV9jaGFuZ2VzGAQgAygLMicuYWN5Y2xpYy5maWxlc3lzdGVtLnYy'
    'LkZpbGVSZWNvcmRDaGFuZ2VSC2ZpbGVDaGFuZ2VzEj0KCWNvbmZsaWN0cxgFIAMoCzIfLmFjeW'
    'NsaWMuZmlsZXN5c3RlbS52Mi5Db25mbGljdFIJY29uZmxpY3RzEhwKCXRydW5jYXRlZBgGIAEo'
    'CFIJdHJ1bmNhdGVkEk0KD2NvbW1vbl9hbmNlc3RvchgHIAEoCzIkLmFjeWNsaWMuZmlsZXN5c3'
    'RlbS52Mi5HZW5lcmF0aW9uUmVmUg5jb21tb25BbmNlc3RvchIvChNtYXhpbXVtX2dlbmVyYXRp'
    'b25zGAggASgNUhJtYXhpbXVtR2VuZXJhdGlvbnMSJwoPbWF4aW11bV9jaGFuZ2VzGAkgASgNUg'
    '5tYXhpbXVtQ2hhbmdlcxIrChFtYXhpbXVtX2NvbmZsaWN0cxgKIAEoDVIQbWF4aW11bUNvbmZs'
    'aWN0cxI8CgdoaXN0b3J5GAsgASgOMiIuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLkpvaW5IaXN0b3'
    'J5UgdoaXN0b3J5ElYKD2JpbmRpbmdfY2hhbmdlcxgMIAMoCzItLmFjeWNsaWMuZmlsZXN5c3Rl'
    'bS52Mi5EaXJlY3RvcnlCaW5kaW5nQ2hhbmdlUg5iaW5kaW5nQ2hhbmdlcw==');

@$core.Deprecated('Use applyJoinRequestDescriptor instead')
const ApplyJoinRequest$json = {
  '1': 'ApplyJoinRequest',
  '2': [
    {
      '1': 'plan',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.JoinPlan',
      '10': 'plan'
    },
    {
      '1': 'operation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `ApplyJoinRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List applyJoinRequestDescriptor = $convert.base64Decode(
    'ChBBcHBseUpvaW5SZXF1ZXN0EjMKBHBsYW4YASABKAsyHy5hY3ljbGljLmZpbGVzeXN0ZW0udj'
    'IuSm9pblBsYW5SBHBsYW4SRQoJb3BlcmF0aW9uGAIgASgLMicuYWN5Y2xpYy5maWxlc3lzdGVt'
    'LnYyLk9wZXJhdGlvbk9wdGlvbnNSCW9wZXJhdGlvbg==');

@$core.Deprecated('Use joinResponseDescriptor instead')
const JoinResponse$json = {
  '1': 'JoinResponse',
  '2': [
    {
      '1': 'status',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.JoinStatus',
      '10': 'status'
    },
    {
      '1': 'generation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {
      '1': 'conflicts',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.filesystem.v2.Conflict',
      '10': 'conflicts'
    },
    {'1': 'truncated', '3': 4, '4': 1, '5': 8, '10': 'truncated'},
  ],
};

/// Descriptor for `JoinResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List joinResponseDescriptor = $convert.base64Decode(
    'CgxKb2luUmVzcG9uc2USOQoGc3RhdHVzGAEgASgOMiEuYWN5Y2xpYy5maWxlc3lzdGVtLnYyLk'
    'pvaW5TdGF0dXNSBnN0YXR1cxJECgpnZW5lcmF0aW9uGAIgASgLMiQuYWN5Y2xpYy5maWxlc3lz'
    'dGVtLnYyLkdlbmVyYXRpb25SZWZSCmdlbmVyYXRpb24SPQoJY29uZmxpY3RzGAMgAygLMh8uYW'
    'N5Y2xpYy5maWxlc3lzdGVtLnYyLkNvbmZsaWN0Ugljb25mbGljdHMSHAoJdHJ1bmNhdGVkGAQg'
    'ASgIUgl0cnVuY2F0ZWQ=');

@$core.Deprecated('Use retainGenerationRequestDescriptor instead')
const RetainGenerationRequest$json = {
  '1': 'RetainGenerationRequest',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'identity', '3': 2, '4': 1, '5': 9, '10': 'identity'},
    {
      '1': 'operation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `RetainGenerationRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List retainGenerationRequestDescriptor = $convert.base64Decode(
    'ChdSZXRhaW5HZW5lcmF0aW9uUmVxdWVzdBJECgpnZW5lcmF0aW9uGAEgASgLMiQuYWN5Y2xpYy'
    '5maWxlc3lzdGVtLnYyLkdlbmVyYXRpb25SZWZSCmdlbmVyYXRpb24SGgoIaWRlbnRpdHkYAiAB'
    'KAlSCGlkZW50aXR5EkUKCW9wZXJhdGlvbhgDIAEoCzInLmFjeWNsaWMuZmlsZXN5c3RlbS52Mi'
    '5PcGVyYXRpb25PcHRpb25zUglvcGVyYXRpb24=');

@$core.Deprecated('Use retainGenerationResponseDescriptor instead')
const RetainGenerationResponse$json = {
  '1': 'RetainGenerationResponse',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'identity', '3': 2, '4': 1, '5': 9, '10': 'identity'},
    {
      '1': 'status',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.acyclic.filesystem.v2.MutationStatus',
      '10': 'status'
    },
  ],
};

/// Descriptor for `RetainGenerationResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List retainGenerationResponseDescriptor = $convert.base64Decode(
    'ChhSZXRhaW5HZW5lcmF0aW9uUmVzcG9uc2USRAoKZ2VuZXJhdGlvbhgBIAEoCzIkLmFjeWNsaW'
    'MuZmlsZXN5c3RlbS52Mi5HZW5lcmF0aW9uUmVmUgpnZW5lcmF0aW9uEhoKCGlkZW50aXR5GAIg'
    'ASgJUghpZGVudGl0eRI9CgZzdGF0dXMYAyABKA4yJS5hY3ljbGljLmZpbGVzeXN0ZW0udjIuTX'
    'V0YXRpb25TdGF0dXNSBnN0YXR1cw==');

@$core.Deprecated('Use exportRequestDescriptor instead')
const ExportRequest$json = {
  '1': 'ExportRequest',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'after', '3': 2, '4': 1, '5': 12, '10': 'after'},
    {'1': 'maximum_objects', '3': 3, '4': 1, '5': 13, '10': 'maximumObjects'},
    {'1': 'maximum_bytes', '3': 4, '4': 1, '5': 4, '10': 'maximumBytes'},
  ],
};

/// Descriptor for `ExportRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List exportRequestDescriptor = $convert.base64Decode(
    'Cg1FeHBvcnRSZXF1ZXN0EkQKCmdlbmVyYXRpb24YASABKAsyJC5hY3ljbGljLmZpbGVzeXN0ZW'
    '0udjIuR2VuZXJhdGlvblJlZlIKZ2VuZXJhdGlvbhIUCgVhZnRlchgCIAEoDFIFYWZ0ZXISJwoP'
    'bWF4aW11bV9vYmplY3RzGAMgASgNUg5tYXhpbXVtT2JqZWN0cxIjCg1tYXhpbXVtX2J5dGVzGA'
    'QgASgEUgxtYXhpbXVtQnl0ZXM=');

@$core.Deprecated('Use exportChunkDescriptor instead')
const ExportChunk$json = {
  '1': 'ExportChunk',
  '2': [
    {'1': 'cursor', '3': 1, '4': 1, '5': 12, '10': 'cursor'},
    {'1': 'object_id', '3': 2, '4': 1, '5': 12, '10': 'objectId'},
    {'1': 'contents', '3': 3, '4': 1, '5': 12, '10': 'contents'},
    {'1': 'terminal', '3': 4, '4': 1, '5': 8, '10': 'terminal'},
  ],
};

/// Descriptor for `ExportChunk`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List exportChunkDescriptor = $convert.base64Decode(
    'CgtFeHBvcnRDaHVuaxIWCgZjdXJzb3IYASABKAxSBmN1cnNvchIbCglvYmplY3RfaWQYAiABKA'
    'xSCG9iamVjdElkEhoKCGNvbnRlbnRzGAMgASgMUghjb250ZW50cxIaCgh0ZXJtaW5hbBgEIAEo'
    'CFIIdGVybWluYWw=');

@$core.Deprecated('Use importChunkDescriptor instead')
const ImportChunk$json = {
  '1': 'ImportChunk',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {'1': 'operation_id', '3': 2, '4': 1, '5': 12, '10': 'operationId'},
    {'1': 'cursor', '3': 3, '4': 1, '5': 12, '10': 'cursor'},
    {'1': 'object_id', '3': 4, '4': 1, '5': 12, '10': 'objectId'},
    {'1': 'contents', '3': 5, '4': 1, '5': 12, '10': 'contents'},
    {'1': 'terminal', '3': 6, '4': 1, '5': 8, '10': 'terminal'},
  ],
};

/// Descriptor for `ImportChunk`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List importChunkDescriptor = $convert.base64Decode(
    'CgtJbXBvcnRDaHVuaxJBCgl3b3Jrc3BhY2UYASABKAsyIy5hY3ljbGljLmZpbGVzeXN0ZW0udj'
    'IuV29ya3NwYWNlUmVmUgl3b3Jrc3BhY2USIQoMb3BlcmF0aW9uX2lkGAIgASgMUgtvcGVyYXRp'
    'b25JZBIWCgZjdXJzb3IYAyABKAxSBmN1cnNvchIbCglvYmplY3RfaWQYBCABKAxSCG9iamVjdE'
    'lkEhoKCGNvbnRlbnRzGAUgASgMUghjb250ZW50cxIaCgh0ZXJtaW5hbBgGIAEoCFIIdGVybWlu'
    'YWw=');

@$core.Deprecated('Use importResponseDescriptor instead')
const ImportResponse$json = {
  '1': 'ImportResponse',
  '2': [
    {
      '1': 'outcome',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.MutationResponse',
      '10': 'outcome'
    },
  ],
};

/// Descriptor for `ImportResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List importResponseDescriptor = $convert.base64Decode(
    'Cg5JbXBvcnRSZXNwb25zZRJBCgdvdXRjb21lGAEgASgLMicuYWN5Y2xpYy5maWxlc3lzdGVtLn'
    'YyLk11dGF0aW9uUmVzcG9uc2VSB291dGNvbWU=');

@$core.Deprecated('Use credentialRequestDescriptor instead')
const CredentialRequest$json = {
  '1': 'CredentialRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {
      '1': 'generation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.GenerationRef',
      '10': 'generation'
    },
    {'1': 'writable', '3': 3, '4': 1, '5': 8, '10': 'writable'},
    {
      '1': 'expires_after_seconds',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'expiresAfterSeconds'
    },
    {
      '1': 'operation',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.OperationOptions',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `CredentialRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List credentialRequestDescriptor = $convert.base64Decode(
    'ChFDcmVkZW50aWFsUmVxdWVzdBJBCgl3b3Jrc3BhY2UYASABKAsyIy5hY3ljbGljLmZpbGVzeX'
    'N0ZW0udjIuV29ya3NwYWNlUmVmUgl3b3Jrc3BhY2USRAoKZ2VuZXJhdGlvbhgCIAEoCzIkLmFj'
    'eWNsaWMuZmlsZXN5c3RlbS52Mi5HZW5lcmF0aW9uUmVmUgpnZW5lcmF0aW9uEhoKCHdyaXRhYm'
    'xlGAMgASgIUgh3cml0YWJsZRIyChVleHBpcmVzX2FmdGVyX3NlY29uZHMYBCABKARSE2V4cGly'
    'ZXNBZnRlclNlY29uZHMSRQoJb3BlcmF0aW9uGAUgASgLMicuYWN5Y2xpYy5maWxlc3lzdGVtLn'
    'YyLk9wZXJhdGlvbk9wdGlvbnNSCW9wZXJhdGlvbg==');

@$core.Deprecated('Use credentialResponseDescriptor instead')
const CredentialResponse$json = {
  '1': 'CredentialResponse',
  '2': [
    {'1': 'endpoint', '3': 1, '4': 1, '5': 9, '10': 'endpoint'},
    {
      '1': 'expires_at_unix_seconds',
      '3': 2,
      '4': 1,
      '5': 3,
      '10': 'expiresAtUnixSeconds'
    },
    {'1': 'bearer_token', '3': 3, '4': 1, '5': 9, '9': 0, '10': 'bearerToken'},
    {
      '1': 's3',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.S3Credential',
      '9': 0,
      '10': 's3'
    },
  ],
  '8': [
    {'1': 'credential'},
  ],
};

/// Descriptor for `CredentialResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List credentialResponseDescriptor = $convert.base64Decode(
    'ChJDcmVkZW50aWFsUmVzcG9uc2USGgoIZW5kcG9pbnQYASABKAlSCGVuZHBvaW50EjUKF2V4cG'
    'lyZXNfYXRfdW5peF9zZWNvbmRzGAIgASgDUhRleHBpcmVzQXRVbml4U2Vjb25kcxIjCgxiZWFy'
    'ZXJfdG9rZW4YAyABKAlIAFILYmVhcmVyVG9rZW4SNQoCczMYBCABKAsyIy5hY3ljbGljLmZpbG'
    'VzeXN0ZW0udjIuUzNDcmVkZW50aWFsSABSAnMzQgwKCmNyZWRlbnRpYWw=');

@$core.Deprecated('Use s3CredentialDescriptor instead')
const S3Credential$json = {
  '1': 'S3Credential',
  '2': [
    {'1': 'bucket', '3': 1, '4': 1, '5': 9, '10': 'bucket'},
    {'1': 'region', '3': 2, '4': 1, '5': 9, '10': 'region'},
    {'1': 'access_key_id', '3': 3, '4': 1, '5': 9, '10': 'accessKeyId'},
    {'1': 'secret_access_key', '3': 4, '4': 1, '5': 9, '10': 'secretAccessKey'},
    {'1': 'session_token', '3': 5, '4': 1, '5': 9, '10': 'sessionToken'},
  ],
};

/// Descriptor for `S3Credential`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List s3CredentialDescriptor = $convert.base64Decode(
    'CgxTM0NyZWRlbnRpYWwSFgoGYnVja2V0GAEgASgJUgZidWNrZXQSFgoGcmVnaW9uGAIgASgJUg'
    'ZyZWdpb24SIgoNYWNjZXNzX2tleV9pZBgDIAEoCVILYWNjZXNzS2V5SWQSKgoRc2VjcmV0X2Fj'
    'Y2Vzc19rZXkYBCABKAlSD3NlY3JldEFjY2Vzc0tleRIjCg1zZXNzaW9uX3Rva2VuGAUgASgJUg'
    'xzZXNzaW9uVG9rZW4=');

@$core.Deprecated('Use observeRequestDescriptor instead')
const ObserveRequest$json = {
  '1': 'ObserveRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {'1': 'operation_id', '3': 2, '4': 1, '5': 12, '10': 'operationId'},
  ],
};

/// Descriptor for `ObserveRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List observeRequestDescriptor = $convert.base64Decode(
    'Cg5PYnNlcnZlUmVxdWVzdBJBCgl3b3Jrc3BhY2UYASABKAsyIy5hY3ljbGljLmZpbGVzeXN0ZW'
    '0udjIuV29ya3NwYWNlUmVmUgl3b3Jrc3BhY2USIQoMb3BlcmF0aW9uX2lkGAIgASgMUgtvcGVy'
    'YXRpb25JZA==');

@$core.Deprecated('Use observeResponseDescriptor instead')
const ObserveResponse$json = {
  '1': 'ObserveResponse',
  '2': [
    {'1': 'state', '3': 1, '4': 1, '5': 9, '10': 'state'},
    {
      '1': 'outcome',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.MutationResponse',
      '10': 'outcome'
    },
  ],
};

/// Descriptor for `ObserveResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List observeResponseDescriptor = $convert.base64Decode(
    'Cg9PYnNlcnZlUmVzcG9uc2USFAoFc3RhdGUYASABKAlSBXN0YXRlEkEKB291dGNvbWUYAiABKA'
    'syJy5hY3ljbGljLmZpbGVzeXN0ZW0udjIuTXV0YXRpb25SZXNwb25zZVIHb3V0Y29tZQ==');

@$core.Deprecated('Use cancelRequestDescriptor instead')
const CancelRequest$json = {
  '1': 'CancelRequest',
  '2': [
    {
      '1': 'workspace',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.WorkspaceRef',
      '10': 'workspace'
    },
    {'1': 'operation_id', '3': 2, '4': 1, '5': 12, '10': 'operationId'},
  ],
};

/// Descriptor for `CancelRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List cancelRequestDescriptor = $convert.base64Decode(
    'Cg1DYW5jZWxSZXF1ZXN0EkEKCXdvcmtzcGFjZRgBIAEoCzIjLmFjeWNsaWMuZmlsZXN5c3RlbS'
    '52Mi5Xb3Jrc3BhY2VSZWZSCXdvcmtzcGFjZRIhCgxvcGVyYXRpb25faWQYAiABKAxSC29wZXJh'
    'dGlvbklk');

@$core.Deprecated('Use cancelResponseDescriptor instead')
const CancelResponse$json = {
  '1': 'CancelResponse',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.filesystem.v2.ObserveResponse',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `CancelResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List cancelResponseDescriptor = $convert.base64Decode(
    'Cg5DYW5jZWxSZXNwb25zZRJECglvcGVyYXRpb24YASABKAsyJi5hY3ljbGljLmZpbGVzeXN0ZW'
    '0udjIuT2JzZXJ2ZVJlc3BvbnNlUglvcGVyYXRpb24=');
