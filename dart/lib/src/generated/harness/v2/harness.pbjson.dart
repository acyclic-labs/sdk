// This is a generated file - do not edit.
//
// Generated from harness/v2/harness.proto.

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

@$core.Deprecated('Use errorCodeDescriptor instead')
const ErrorCode$json = {
  '1': 'ErrorCode',
  '2': [
    {'1': 'ERROR_CODE_UNSPECIFIED', '2': 0},
    {'1': 'ERROR_CODE_NOT_FOUND', '2': 1},
    {'1': 'ERROR_CODE_CONFLICT', '2': 2},
    {'1': 'ERROR_CODE_UNSUPPORTED', '2': 3},
    {'1': 'ERROR_CODE_INVALID', '2': 4},
    {'1': 'ERROR_CODE_UNAUTHORIZED', '2': 5},
    {'1': 'ERROR_CODE_STORAGE', '2': 6},
    {'1': 'ERROR_CODE_INDETERMINATE', '2': 7},
    {'1': 'ERROR_CODE_INTERACTION_DECLINED', '2': 8},
    {'1': 'ERROR_CODE_INTERACTION_CANCELLED', '2': 9},
    {'1': 'ERROR_CODE_INTERACTION_EXPIRED', '2': 10},
    {'1': 'ERROR_CODE_INTERACTION_DENIED', '2': 11},
  ],
};

/// Descriptor for `ErrorCode`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List errorCodeDescriptor = $convert.base64Decode(
    'CglFcnJvckNvZGUSGgoWRVJST1JfQ09ERV9VTlNQRUNJRklFRBAAEhgKFEVSUk9SX0NPREVfTk'
    '9UX0ZPVU5EEAESFwoTRVJST1JfQ09ERV9DT05GTElDVBACEhoKFkVSUk9SX0NPREVfVU5TVVBQ'
    'T1JURUQQAxIWChJFUlJPUl9DT0RFX0lOVkFMSUQQBBIbChdFUlJPUl9DT0RFX1VOQVVUSE9SSV'
    'pFRBAFEhYKEkVSUk9SX0NPREVfU1RPUkFHRRAGEhwKGEVSUk9SX0NPREVfSU5ERVRFUk1JTkFU'
    'RRAHEiMKH0VSUk9SX0NPREVfSU5URVJBQ1RJT05fREVDTElORUQQCBIkCiBFUlJPUl9DT0RFX0'
    'lOVEVSQUNUSU9OX0NBTkNFTExFRBAJEiIKHkVSUk9SX0NPREVfSU5URVJBQ1RJT05fRVhQSVJF'
    'RBAKEiEKHUVSUk9SX0NPREVfSU5URVJBQ1RJT05fREVOSUVEEAs=');

@$core.Deprecated('Use admissionStateDescriptor instead')
const AdmissionState$json = {
  '1': 'AdmissionState',
  '2': [
    {'1': 'ADMISSION_STATE_UNSPECIFIED', '2': 0},
    {'1': 'ADMISSION_STATE_ACCEPTED', '2': 1},
    {'1': 'ADMISSION_STATE_REJECTED', '2': 2},
    {'1': 'ADMISSION_STATE_INDETERMINATE', '2': 3},
  ],
};

/// Descriptor for `AdmissionState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List admissionStateDescriptor = $convert.base64Decode(
    'Cg5BZG1pc3Npb25TdGF0ZRIfChtBRE1JU1NJT05fU1RBVEVfVU5TUEVDSUZJRUQQABIcChhBRE'
    '1JU1NJT05fU1RBVEVfQUNDRVBURUQQARIcChhBRE1JU1NJT05fU1RBVEVfUkVKRUNURUQQAhIh'
    'Ch1BRE1JU1NJT05fU1RBVEVfSU5ERVRFUk1JTkFURRAD');

@$core.Deprecated('Use completionStateDescriptor instead')
const CompletionState$json = {
  '1': 'CompletionState',
  '2': [
    {'1': 'COMPLETION_STATE_UNSPECIFIED', '2': 0},
    {'1': 'COMPLETION_STATE_RUNNING', '2': 1},
    {'1': 'COMPLETION_STATE_SUCCEEDED', '2': 2},
    {'1': 'COMPLETION_STATE_FAILED', '2': 3},
    {'1': 'COMPLETION_STATE_CANCELLED', '2': 4},
    {'1': 'COMPLETION_STATE_INDETERMINATE', '2': 5},
  ],
};

/// Descriptor for `CompletionState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List completionStateDescriptor = $convert.base64Decode(
    'Cg9Db21wbGV0aW9uU3RhdGUSIAocQ09NUExFVElPTl9TVEFURV9VTlNQRUNJRklFRBAAEhwKGE'
    'NPTVBMRVRJT05fU1RBVEVfUlVOTklORxABEh4KGkNPTVBMRVRJT05fU1RBVEVfU1VDQ0VFREVE'
    'EAISGwoXQ09NUExFVElPTl9TVEFURV9GQUlMRUQQAxIeChpDT01QTEVUSU9OX1NUQVRFX0NBTk'
    'NFTExFRBAEEiIKHkNPTVBMRVRJT05fU1RBVEVfSU5ERVRFUk1JTkFURRAF');

@$core.Deprecated('Use aggregateKindDescriptor instead')
const AggregateKind$json = {
  '1': 'AggregateKind',
  '2': [
    {'1': 'AGGREGATE_KIND_UNSPECIFIED', '2': 0},
    {'1': 'AGGREGATE_KIND_AGENT', '2': 1},
    {'1': 'AGGREGATE_KIND_CONVERSATION', '2': 2},
    {'1': 'AGGREGATE_KIND_SESSION', '2': 3},
    {'1': 'AGGREGATE_KIND_TURN', '2': 4},
    {'1': 'AGGREGATE_KIND_TASK', '2': 5},
  ],
};

/// Descriptor for `AggregateKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List aggregateKindDescriptor = $convert.base64Decode(
    'Cg1BZ2dyZWdhdGVLaW5kEh4KGkFHR1JFR0FURV9LSU5EX1VOU1BFQ0lGSUVEEAASGAoUQUdHUk'
    'VHQVRFX0tJTkRfQUdFTlQQARIfChtBR0dSRUdBVEVfS0lORF9DT05WRVJTQVRJT04QAhIaChZB'
    'R0dSRUdBVEVfS0lORF9TRVNTSU9OEAMSFwoTQUdHUkVHQVRFX0tJTkRfVFVSThAEEhcKE0FHR1'
    'JFR0FURV9LSU5EX1RBU0sQBQ==');

@$core.Deprecated('Use applyStateDescriptor instead')
const ApplyState$json = {
  '1': 'ApplyState',
  '2': [
    {'1': 'APPLY_STATE_UNSPECIFIED', '2': 0},
    {'1': 'APPLY_STATE_APPLIED', '2': 1},
    {'1': 'APPLY_STATE_REPLAYED', '2': 2},
  ],
};

/// Descriptor for `ApplyState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List applyStateDescriptor = $convert.base64Decode(
    'CgpBcHBseVN0YXRlEhsKF0FQUExZX1NUQVRFX1VOU1BFQ0lGSUVEEAASFwoTQVBQTFlfU1RBVE'
    'VfQVBQTElFRBABEhgKFEFQUExZX1NUQVRFX1JFUExBWUVEEAI=');

@$core.Deprecated('Use volumeClassDescriptor instead')
const VolumeClass$json = {
  '1': 'VolumeClass',
  '2': [
    {'1': 'VOLUME_CLASS_UNSPECIFIED', '2': 0},
    {'1': 'VOLUME_CLASS_PROJECT', '2': 1},
    {'1': 'VOLUME_CLASS_AGENT_PRIVATE', '2': 2},
    {'1': 'VOLUME_CLASS_SESSION_SHARED', '2': 3},
  ],
};

/// Descriptor for `VolumeClass`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List volumeClassDescriptor = $convert.base64Decode(
    'CgtWb2x1bWVDbGFzcxIcChhWT0xVTUVfQ0xBU1NfVU5TUEVDSUZJRUQQABIYChRWT0xVTUVfQ0'
    'xBU1NfUFJPSkVDVBABEh4KGlZPTFVNRV9DTEFTU19BR0VOVF9QUklWQVRFEAISHwobVk9MVU1F'
    'X0NMQVNTX1NFU1NJT05fU0hBUkVEEAM=');

@$core.Deprecated('Use extensionForkPolicyDescriptor instead')
const ExtensionForkPolicy$json = {
  '1': 'ExtensionForkPolicy',
  '2': [
    {'1': 'EXTENSION_FORK_POLICY_UNSPECIFIED', '2': 0},
    {'1': 'EXTENSION_FORK_POLICY_INHERIT', '2': 1},
    {'1': 'EXTENSION_FORK_POLICY_RESET', '2': 2},
    {'1': 'EXTENSION_FORK_POLICY_REJECT', '2': 3},
  ],
};

/// Descriptor for `ExtensionForkPolicy`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List extensionForkPolicyDescriptor = $convert.base64Decode(
    'ChNFeHRlbnNpb25Gb3JrUG9saWN5EiUKIUVYVEVOU0lPTl9GT1JLX1BPTElDWV9VTlNQRUNJRk'
    'lFRBAAEiEKHUVYVEVOU0lPTl9GT1JLX1BPTElDWV9JTkhFUklUEAESHwobRVhURU5TSU9OX0ZP'
    'UktfUE9MSUNZX1JFU0VUEAISIAocRVhURU5TSU9OX0ZPUktfUE9MSUNZX1JFSkVDVBAD');

@$core.Deprecated('Use interactionKindDescriptor instead')
const InteractionKind$json = {
  '1': 'InteractionKind',
  '2': [
    {'1': 'INTERACTION_KIND_UNSPECIFIED', '2': 0},
    {'1': 'INTERACTION_KIND_QUESTION', '2': 1},
    {'1': 'INTERACTION_KIND_CHOICE', '2': 2},
    {'1': 'INTERACTION_KIND_FORM', '2': 3},
    {'1': 'INTERACTION_KIND_APPROVAL', '2': 4},
  ],
};

/// Descriptor for `InteractionKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List interactionKindDescriptor = $convert.base64Decode(
    'Cg9JbnRlcmFjdGlvbktpbmQSIAocSU5URVJBQ1RJT05fS0lORF9VTlNQRUNJRklFRBAAEh0KGU'
    'lOVEVSQUNUSU9OX0tJTkRfUVVFU1RJT04QARIbChdJTlRFUkFDVElPTl9LSU5EX0NIT0lDRRAC'
    'EhkKFUlOVEVSQUNUSU9OX0tJTkRfRk9STRADEh0KGUlOVEVSQUNUSU9OX0tJTkRfQVBQUk9WQU'
    'wQBA==');

@$core.Deprecated('Use conversationKindDescriptor instead')
const ConversationKind$json = {
  '1': 'ConversationKind',
  '2': [
    {'1': 'CONVERSATION_KIND_UNSPECIFIED', '2': 0},
    {'1': 'CONVERSATION_KIND_USER', '2': 1},
    {'1': 'CONVERSATION_KIND_ASSISTANT', '2': 2},
    {'1': 'CONVERSATION_KIND_SYSTEM', '2': 3},
    {'1': 'CONVERSATION_KIND_TOOL_CALL', '2': 4},
    {'1': 'CONVERSATION_KIND_TOOL_RESULT', '2': 5},
    {'1': 'CONVERSATION_KIND_INTERACTION', '2': 6},
    {'1': 'CONVERSATION_KIND_PERMISSION', '2': 7},
    {'1': 'CONVERSATION_KIND_FORK', '2': 8},
    {'1': 'CONVERSATION_KIND_MERGE', '2': 9},
  ],
};

/// Descriptor for `ConversationKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List conversationKindDescriptor = $convert.base64Decode(
    'ChBDb252ZXJzYXRpb25LaW5kEiEKHUNPTlZFUlNBVElPTl9LSU5EX1VOU1BFQ0lGSUVEEAASGg'
    'oWQ09OVkVSU0FUSU9OX0tJTkRfVVNFUhABEh8KG0NPTlZFUlNBVElPTl9LSU5EX0FTU0lTVEFO'
    'VBACEhwKGENPTlZFUlNBVElPTl9LSU5EX1NZU1RFTRADEh8KG0NPTlZFUlNBVElPTl9LSU5EX1'
    'RPT0xfQ0FMTBAEEiEKHUNPTlZFUlNBVElPTl9LSU5EX1RPT0xfUkVTVUxUEAUSIQodQ09OVkVS'
    'U0FUSU9OX0tJTkRfSU5URVJBQ1RJT04QBhIgChxDT05WRVJTQVRJT05fS0lORF9QRVJNSVNTSU'
    '9OEAcSGgoWQ09OVkVSU0FUSU9OX0tJTkRfRk9SSxAIEhsKF0NPTlZFUlNBVElPTl9LSU5EX01F'
    'UkdFEAk=');

@$core.Deprecated('Use resourceKindDescriptor instead')
const ResourceKind$json = {
  '1': 'ResourceKind',
  '2': [
    {'1': 'RESOURCE_KIND_UNSPECIFIED', '2': 0},
    {'1': 'RESOURCE_KIND_WORKSPACE', '2': 1},
    {'1': 'RESOURCE_KIND_GENERATION', '2': 2},
    {'1': 'RESOURCE_KIND_ARTIFACT', '2': 3},
    {'1': 'RESOURCE_KIND_SANDBOX', '2': 4},
    {'1': 'RESOURCE_KIND_CHECKPOINT', '2': 5},
    {'1': 'RESOURCE_KIND_STREAM', '2': 6},
    {'1': 'RESOURCE_KIND_CONTEXT', '2': 7},
    {'1': 'RESOURCE_KIND_RUN', '2': 8},
  ],
};

/// Descriptor for `ResourceKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List resourceKindDescriptor = $convert.base64Decode(
    'CgxSZXNvdXJjZUtpbmQSHQoZUkVTT1VSQ0VfS0lORF9VTlNQRUNJRklFRBAAEhsKF1JFU09VUk'
    'NFX0tJTkRfV09SS1NQQUNFEAESHAoYUkVTT1VSQ0VfS0lORF9HRU5FUkFUSU9OEAISGgoWUkVT'
    'T1VSQ0VfS0lORF9BUlRJRkFDVBADEhkKFVJFU09VUkNFX0tJTkRfU0FOREJPWBAEEhwKGFJFU0'
    '9VUkNFX0tJTkRfQ0hFQ0tQT0lOVBAFEhgKFFJFU09VUkNFX0tJTkRfU1RSRUFNEAYSGQoVUkVT'
    'T1VSQ0VfS0lORF9DT05URVhUEAcSFQoRUkVTT1VSQ0VfS0lORF9SVU4QCA==');

@$core.Deprecated('Use batchGroupPolicyDescriptor instead')
const BatchGroupPolicy$json = {
  '1': 'BatchGroupPolicy',
  '2': [
    {'1': 'BATCH_GROUP_POLICY_UNSPECIFIED', '2': 0},
    {'1': 'BATCH_GROUP_POLICY_COLLECT_ALL', '2': 1},
    {'1': 'BATCH_GROUP_POLICY_CANCEL_ON_FAILURE', '2': 2},
  ],
};

/// Descriptor for `BatchGroupPolicy`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List batchGroupPolicyDescriptor = $convert.base64Decode(
    'ChBCYXRjaEdyb3VwUG9saWN5EiIKHkJBVENIX0dST1VQX1BPTElDWV9VTlNQRUNJRklFRBAAEi'
    'IKHkJBVENIX0dST1VQX1BPTElDWV9DT0xMRUNUX0FMTBABEigKJEJBVENIX0dST1VQX1BPTElD'
    'WV9DQU5DRUxfT05fRkFJTFVSRRAC');

@$core.Deprecated('Use sharedVolumeOperationDescriptor instead')
const SharedVolumeOperation$json = {
  '1': 'SharedVolumeOperation',
  '2': [
    {'1': 'SHARED_VOLUME_OPERATION_UNSPECIFIED', '2': 0},
    {'1': 'SHARED_VOLUME_OPERATION_READ', '2': 1},
    {'1': 'SHARED_VOLUME_OPERATION_WRITE', '2': 2},
  ],
};

/// Descriptor for `SharedVolumeOperation`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List sharedVolumeOperationDescriptor = $convert.base64Decode(
    'ChVTaGFyZWRWb2x1bWVPcGVyYXRpb24SJwojU0hBUkVEX1ZPTFVNRV9PUEVSQVRJT05fVU5TUE'
    'VDSUZJRUQQABIgChxTSEFSRURfVk9MVU1FX09QRVJBVElPTl9SRUFEEAESIQodU0hBUkVEX1ZP'
    'TFVNRV9PUEVSQVRJT05fV1JJVEUQAg==');

@$core.Deprecated('Use operationIdentityDescriptor instead')
const OperationIdentity$json = {
  '1': 'OperationIdentity',
  '2': [
    {'1': 'operation_id', '3': 1, '4': 1, '5': 9, '10': 'operationId'},
    {'1': 'idempotency_key', '3': 2, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `OperationIdentity`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List operationIdentityDescriptor = $convert.base64Decode(
    'ChFPcGVyYXRpb25JZGVudGl0eRIhCgxvcGVyYXRpb25faWQYASABKAlSC29wZXJhdGlvbklkEi'
    'cKD2lkZW1wb3RlbmN5X2tleRgCIAEoCVIOaWRlbXBvdGVuY3lLZXk=');

@$core.Deprecated('Use errorDescriptor instead')
const Error$json = {
  '1': 'Error',
  '2': [
    {
      '1': 'code',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.ErrorCode',
      '10': 'code'
    },
    {'1': 'message', '3': 2, '4': 1, '5': 9, '10': 'message'},
    {'1': 'operation_id', '3': 3, '4': 1, '5': 9, '10': 'operationId'},
  ],
};

/// Descriptor for `Error`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List errorDescriptor = $convert.base64Decode(
    'CgVFcnJvchIxCgRjb2RlGAEgASgOMh0uYWN5Y2xpYy5oYXJuZXNzLnYyLkVycm9yQ29kZVIEY2'
    '9kZRIYCgdtZXNzYWdlGAIgASgJUgdtZXNzYWdlEiEKDG9wZXJhdGlvbl9pZBgDIAEoCVILb3Bl'
    'cmF0aW9uSWQ=');

@$core.Deprecated('Use admissionDescriptor instead')
const Admission$json = {
  '1': 'Admission',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationIdentity',
      '10': 'operation'
    },
    {
      '1': 'state',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.AdmissionState',
      '10': 'state'
    },
    {
      '1': 'error',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Error',
      '10': 'error'
    },
  ],
};

/// Descriptor for `Admission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List admissionDescriptor = $convert.base64Decode(
    'CglBZG1pc3Npb24SQwoJb3BlcmF0aW9uGAEgASgLMiUuYWN5Y2xpYy5oYXJuZXNzLnYyLk9wZX'
    'JhdGlvbklkZW50aXR5UglvcGVyYXRpb24SOAoFc3RhdGUYAiABKA4yIi5hY3ljbGljLmhhcm5l'
    'c3MudjIuQWRtaXNzaW9uU3RhdGVSBXN0YXRlEi8KBWVycm9yGAMgASgLMhkuYWN5Y2xpYy5oYX'
    'JuZXNzLnYyLkVycm9yUgVlcnJvcg==');

@$core.Deprecated('Use operationStatusDescriptor instead')
const OperationStatus$json = {
  '1': 'OperationStatus',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationIdentity',
      '10': 'operation'
    },
    {
      '1': 'state',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.CompletionState',
      '10': 'state'
    },
    {
      '1': 'error',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Error',
      '10': 'error'
    },
    {
      '1': 'protocol',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'owner',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'owner'
    },
    {
      '1': 'cancellation_requested',
      '3': 6,
      '4': 1,
      '5': 8,
      '10': 'cancellationRequested'
    },
    {'1': 'revision', '3': 7, '4': 1, '5': 4, '10': 'revision'},
  ],
};

/// Descriptor for `OperationStatus`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List operationStatusDescriptor = $convert.base64Decode(
    'Cg9PcGVyYXRpb25TdGF0dXMSQwoJb3BlcmF0aW9uGAEgASgLMiUuYWN5Y2xpYy5oYXJuZXNzLn'
    'YyLk9wZXJhdGlvbklkZW50aXR5UglvcGVyYXRpb24SOQoFc3RhdGUYAiABKA4yIy5hY3ljbGlj'
    'Lmhhcm5lc3MudjIuQ29tcGxldGlvblN0YXRlUgVzdGF0ZRIvCgVlcnJvchgDIAEoCzIZLmFjeW'
    'NsaWMuaGFybmVzcy52Mi5FcnJvclIFZXJyb3ISQQoIcHJvdG9jb2wYBCABKAsyJS5hY3ljbGlj'
    'LnByb3RvY29sLnYxLlByb3RvY29sSWRlbnRpdHlSCHByb3RvY29sEjMKBW93bmVyGAUgASgLMh'
    '0uYWN5Y2xpYy5oYXJuZXNzLnYyLkF1dGhvcml0eVIFb3duZXISNQoWY2FuY2VsbGF0aW9uX3Jl'
    'cXVlc3RlZBgGIAEoCFIVY2FuY2VsbGF0aW9uUmVxdWVzdGVkEhoKCHJldmlzaW9uGAcgASgEUg'
    'hyZXZpc2lvbg==');

@$core.Deprecated('Use observeRequestDescriptor instead')
const ObserveRequest$json = {
  '1': 'ObserveRequest',
  '2': [
    {'1': 'operation_id', '3': 1, '4': 1, '5': 9, '10': 'operationId'},
    {
      '1': 'protocol',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'owner',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'owner'
    },
    {
      '1': 'scope',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Scope',
      '10': 'scope'
    },
  ],
};

/// Descriptor for `ObserveRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List observeRequestDescriptor = $convert.base64Decode(
    'Cg5PYnNlcnZlUmVxdWVzdBIhCgxvcGVyYXRpb25faWQYASABKAlSC29wZXJhdGlvbklkEkEKCH'
    'Byb3RvY29sGAIgASgLMiUuYWN5Y2xpYy5wcm90b2NvbC52MS5Qcm90b2NvbElkZW50aXR5Ughw'
    'cm90b2NvbBIzCgVvd25lchgDIAEoCzIdLmFjeWNsaWMuaGFybmVzcy52Mi5BdXRob3JpdHlSBW'
    '93bmVyEi8KBXNjb3BlGAQgASgLMhkuYWN5Y2xpYy5oYXJuZXNzLnYyLlNjb3BlUgVzY29wZQ==');

@$core.Deprecated('Use cancelRequestDescriptor instead')
const CancelRequest$json = {
  '1': 'CancelRequest',
  '2': [
    {'1': 'operation_id', '3': 1, '4': 1, '5': 9, '10': 'operationId'},
    {
      '1': 'protocol',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'owner',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'owner'
    },
    {
      '1': 'scope',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Scope',
      '10': 'scope'
    },
    {'1': 'recursive', '3': 5, '4': 1, '5': 8, '10': 'recursive'},
    {'1': 'idempotency_key', '3': 6, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `CancelRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List cancelRequestDescriptor = $convert.base64Decode(
    'Cg1DYW5jZWxSZXF1ZXN0EiEKDG9wZXJhdGlvbl9pZBgBIAEoCVILb3BlcmF0aW9uSWQSQQoIcH'
    'JvdG9jb2wYAiABKAsyJS5hY3ljbGljLnByb3RvY29sLnYxLlByb3RvY29sSWRlbnRpdHlSCHBy'
    'b3RvY29sEjMKBW93bmVyGAMgASgLMh0uYWN5Y2xpYy5oYXJuZXNzLnYyLkF1dGhvcml0eVIFb3'
    'duZXISLwoFc2NvcGUYBCABKAsyGS5hY3ljbGljLmhhcm5lc3MudjIuU2NvcGVSBXNjb3BlEhwK'
    'CXJlY3Vyc2l2ZRgFIAEoCFIJcmVjdXJzaXZlEicKD2lkZW1wb3RlbmN5X2tleRgGIAEoCVIOaW'
    'RlbXBvdGVuY3lLZXk=');

@$core.Deprecated('Use cancelResponseDescriptor instead')
const CancelResponse$json = {
  '1': 'CancelResponse',
  '2': [
    {
      '1': 'status',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationStatus',
      '10': 'status'
    },
    {
      '1': 'operation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationIdentity',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `CancelResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List cancelResponseDescriptor = $convert.base64Decode(
    'Cg5DYW5jZWxSZXNwb25zZRI7CgZzdGF0dXMYASABKAsyIy5hY3ljbGljLmhhcm5lc3MudjIuT3'
    'BlcmF0aW9uU3RhdHVzUgZzdGF0dXMSQwoJb3BlcmF0aW9uGAIgASgLMiUuYWN5Y2xpYy5oYXJu'
    'ZXNzLnYyLk9wZXJhdGlvbklkZW50aXR5UglvcGVyYXRpb24=');

@$core.Deprecated('Use cursorDescriptor instead')
const Cursor$json = {
  '1': 'Cursor',
  '2': [
    {'1': 'opaque', '3': 1, '4': 1, '5': 12, '10': 'opaque'},
  ],
};

/// Descriptor for `Cursor`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List cursorDescriptor =
    $convert.base64Decode('CgZDdXJzb3ISFgoGb3BhcXVlGAEgASgMUgZvcGFxdWU=');

@$core.Deprecated('Use authorityDescriptor instead')
const Authority$json = {
  '1': 'Authority',
  '2': [
    {
      '1': 'kind',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.AggregateKind',
      '10': 'kind'
    },
    {'1': 'id', '3': 2, '4': 1, '5': 9, '10': 'id'},
  ],
};

/// Descriptor for `Authority`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List authorityDescriptor = $convert.base64Decode(
    'CglBdXRob3JpdHkSNQoEa2luZBgBIAEoDjIhLmFjeWNsaWMuaGFybmVzcy52Mi5BZ2dyZWdhdG'
    'VLaW5kUgRraW5kEg4KAmlkGAIgASgJUgJpZA==');

@$core.Deprecated('Use eventReferenceDescriptor instead')
const EventReference$json = {
  '1': 'EventReference',
  '2': [
    {
      '1': 'authority',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'authority'
    },
    {'1': 'revision', '3': 2, '4': 1, '5': 4, '10': 'revision'},
  ],
};

/// Descriptor for `EventReference`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List eventReferenceDescriptor = $convert.base64Decode(
    'Cg5FdmVudFJlZmVyZW5jZRI7CglhdXRob3JpdHkYASABKAsyHS5hY3ljbGljLmhhcm5lc3Mudj'
    'IuQXV0aG9yaXR5UglhdXRob3JpdHkSGgoIcmV2aXNpb24YAiABKARSCHJldmlzaW9u');

@$core.Deprecated('Use scopeDescriptor instead')
const Scope$json = {
  '1': 'Scope',
  '2': [
    {'1': 'id', '3': 1, '4': 1, '5': 9, '10': 'id'},
    {'1': 'capabilities', '3': 2, '4': 3, '5': 9, '10': 'capabilities'},
    {'1': 'issuer', '3': 3, '4': 1, '5': 9, '10': 'issuer'},
    {'1': 'parent_proof', '3': 4, '4': 1, '5': 12, '10': 'parentProof'},
    {'1': 'proof', '3': 5, '4': 1, '5': 12, '10': 'proof'},
    {'1': 'agent_id', '3': 6, '4': 1, '5': 9, '10': 'agentId'},
  ],
};

/// Descriptor for `Scope`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List scopeDescriptor = $convert.base64Decode(
    'CgVTY29wZRIOCgJpZBgBIAEoCVICaWQSIgoMY2FwYWJpbGl0aWVzGAIgAygJUgxjYXBhYmlsaX'
    'RpZXMSFgoGaXNzdWVyGAMgASgJUgZpc3N1ZXISIQoMcGFyZW50X3Byb29mGAQgASgMUgtwYXJl'
    'bnRQcm9vZhIUCgVwcm9vZhgFIAEoDFIFcHJvb2YSGQoIYWdlbnRfaWQYBiABKAlSB2FnZW50SW'
    'Q=');

@$core.Deprecated('Use recordedScopeDescriptor instead')
const RecordedScope$json = {
  '1': 'RecordedScope',
  '2': [
    {'1': 'id', '3': 1, '4': 1, '5': 9, '10': 'id'},
    {'1': 'capabilities', '3': 2, '4': 3, '5': 9, '10': 'capabilities'},
    {'1': 'issuer', '3': 3, '4': 1, '5': 9, '10': 'issuer'},
    {'1': 'agent_id', '3': 4, '4': 1, '5': 9, '10': 'agentId'},
  ],
};

/// Descriptor for `RecordedScope`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List recordedScopeDescriptor = $convert.base64Decode(
    'Cg1SZWNvcmRlZFNjb3BlEg4KAmlkGAEgASgJUgJpZBIiCgxjYXBhYmlsaXRpZXMYAiADKAlSDG'
    'NhcGFiaWxpdGllcxIWCgZpc3N1ZXIYAyABKAlSBmlzc3VlchIZCghhZ2VudF9pZBgEIAEoCVIH'
    'YWdlbnRJZA==');

@$core.Deprecated('Use commandEnvelopeDescriptor instead')
const CommandEnvelope$json = {
  '1': 'CommandEnvelope',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'authority',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'authority'
    },
    {
      '1': 'operation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationIdentity',
      '10': 'operation'
    },
    {
      '1': 'expected_revision',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'expectedRevision'
    },
    {
      '1': 'scope',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Scope',
      '10': 'scope'
    },
    {
      '1': 'causal_parent',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.EventReference',
      '10': 'causalParent'
    },
    {'1': 'action_type', '3': 7, '4': 1, '5': 9, '10': 'actionType'},
    {
      '1': 'canonical_action_json',
      '3': 8,
      '4': 1,
      '5': 12,
      '10': 'canonicalActionJson'
    },
    {'1': 'intent_digest', '3': 9, '4': 1, '5': 12, '10': 'intentDigest'},
  ],
};

/// Descriptor for `CommandEnvelope`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List commandEnvelopeDescriptor = $convert.base64Decode(
    'Cg9Db21tYW5kRW52ZWxvcGUSQQoIcHJvdG9jb2wYASABKAsyJS5hY3ljbGljLnByb3RvY29sLn'
    'YxLlByb3RvY29sSWRlbnRpdHlSCHByb3RvY29sEjsKCWF1dGhvcml0eRgCIAEoCzIdLmFjeWNs'
    'aWMuaGFybmVzcy52Mi5BdXRob3JpdHlSCWF1dGhvcml0eRJDCglvcGVyYXRpb24YAyABKAsyJS'
    '5hY3ljbGljLmhhcm5lc3MudjIuT3BlcmF0aW9uSWRlbnRpdHlSCW9wZXJhdGlvbhIrChFleHBl'
    'Y3RlZF9yZXZpc2lvbhgEIAEoBFIQZXhwZWN0ZWRSZXZpc2lvbhIvCgVzY29wZRgFIAEoCzIZLm'
    'FjeWNsaWMuaGFybmVzcy52Mi5TY29wZVIFc2NvcGUSRwoNY2F1c2FsX3BhcmVudBgGIAEoCzIi'
    'LmFjeWNsaWMuaGFybmVzcy52Mi5FdmVudFJlZmVyZW5jZVIMY2F1c2FsUGFyZW50Eh8KC2FjdG'
    'lvbl90eXBlGAcgASgJUgphY3Rpb25UeXBlEjIKFWNhbm9uaWNhbF9hY3Rpb25fanNvbhgIIAEo'
    'DFITY2Fub25pY2FsQWN0aW9uSnNvbhIjCg1pbnRlbnRfZGlnZXN0GAkgASgMUgxpbnRlbnREaW'
    'dlc3Q=');

@$core.Deprecated('Use eventEnvelopeDescriptor instead')
const EventEnvelope$json = {
  '1': 'EventEnvelope',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'authority',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'authority'
    },
    {'1': 'revision', '3': 3, '4': 1, '5': 4, '10': 'revision'},
    {'1': 'operation_id', '3': 4, '4': 1, '5': 9, '10': 'operationId'},
    {'1': 'intent_digest', '3': 5, '4': 1, '5': 12, '10': 'intentDigest'},
    {
      '1': 'scope',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.RecordedScope',
      '10': 'scope'
    },
    {
      '1': 'causal_parent',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.EventReference',
      '10': 'causalParent'
    },
    {'1': 'event_type', '3': 8, '4': 1, '5': 9, '10': 'eventType'},
    {
      '1': 'canonical_payload_json',
      '3': 9,
      '4': 1,
      '5': 12,
      '10': 'canonicalPayloadJson'
    },
    {'1': 'attestation', '3': 10, '4': 1, '5': 12, '10': 'attestation'},
  ],
};

/// Descriptor for `EventEnvelope`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List eventEnvelopeDescriptor = $convert.base64Decode(
    'Cg1FdmVudEVudmVsb3BlEkEKCHByb3RvY29sGAEgASgLMiUuYWN5Y2xpYy5wcm90b2NvbC52MS'
    '5Qcm90b2NvbElkZW50aXR5Ughwcm90b2NvbBI7CglhdXRob3JpdHkYAiABKAsyHS5hY3ljbGlj'
    'Lmhhcm5lc3MudjIuQXV0aG9yaXR5UglhdXRob3JpdHkSGgoIcmV2aXNpb24YAyABKARSCHJldm'
    'lzaW9uEiEKDG9wZXJhdGlvbl9pZBgEIAEoCVILb3BlcmF0aW9uSWQSIwoNaW50ZW50X2RpZ2Vz'
    'dBgFIAEoDFIMaW50ZW50RGlnZXN0EjcKBXNjb3BlGAYgASgLMiEuYWN5Y2xpYy5oYXJuZXNzLn'
    'YyLlJlY29yZGVkU2NvcGVSBXNjb3BlEkcKDWNhdXNhbF9wYXJlbnQYByABKAsyIi5hY3ljbGlj'
    'Lmhhcm5lc3MudjIuRXZlbnRSZWZlcmVuY2VSDGNhdXNhbFBhcmVudBIdCgpldmVudF90eXBlGA'
    'ggASgJUglldmVudFR5cGUSNAoWY2Fub25pY2FsX3BheWxvYWRfanNvbhgJIAEoDFIUY2Fub25p'
    'Y2FsUGF5bG9hZEpzb24SIAoLYXR0ZXN0YXRpb24YCiABKAxSC2F0dGVzdGF0aW9u');

@$core.Deprecated('Use applyResponseDescriptor instead')
const ApplyResponse$json = {
  '1': 'ApplyResponse',
  '2': [
    {
      '1': 'state',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.ApplyState',
      '10': 'state'
    },
    {
      '1': 'event',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.EventEnvelope',
      '10': 'event'
    },
  ],
};

/// Descriptor for `ApplyResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List applyResponseDescriptor = $convert.base64Decode(
    'Cg1BcHBseVJlc3BvbnNlEjQKBXN0YXRlGAEgASgOMh4uYWN5Y2xpYy5oYXJuZXNzLnYyLkFwcG'
    'x5U3RhdGVSBXN0YXRlEjcKBWV2ZW50GAIgASgLMiEuYWN5Y2xpYy5oYXJuZXNzLnYyLkV2ZW50'
    'RW52ZWxvcGVSBWV2ZW50');

@$core.Deprecated('Use snapshotEnvelopeDescriptor instead')
const SnapshotEnvelope$json = {
  '1': 'SnapshotEnvelope',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'authority',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'authority'
    },
    {'1': 'revision', '3': 3, '4': 1, '5': 4, '10': 'revision'},
    {'1': 'format_version', '3': 4, '4': 1, '5': 13, '10': 'formatVersion'},
    {
      '1': 'canonical_state_json',
      '3': 5,
      '4': 1,
      '5': 12,
      '10': 'canonicalStateJson'
    },
    {'1': 'state_digest', '3': 6, '4': 1, '5': 12, '10': 'stateDigest'},
  ],
};

/// Descriptor for `SnapshotEnvelope`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List snapshotEnvelopeDescriptor = $convert.base64Decode(
    'ChBTbmFwc2hvdEVudmVsb3BlEkEKCHByb3RvY29sGAEgASgLMiUuYWN5Y2xpYy5wcm90b2NvbC'
    '52MS5Qcm90b2NvbElkZW50aXR5Ughwcm90b2NvbBI7CglhdXRob3JpdHkYAiABKAsyHS5hY3lj'
    'bGljLmhhcm5lc3MudjIuQXV0aG9yaXR5UglhdXRob3JpdHkSGgoIcmV2aXNpb24YAyABKARSCH'
    'JldmlzaW9uEiUKDmZvcm1hdF92ZXJzaW9uGAQgASgNUg1mb3JtYXRWZXJzaW9uEjAKFGNhbm9u'
    'aWNhbF9zdGF0ZV9qc29uGAUgASgMUhJjYW5vbmljYWxTdGF0ZUpzb24SIQoMc3RhdGVfZGlnZX'
    'N0GAYgASgMUgtzdGF0ZURpZ2VzdA==');

@$core.Deprecated('Use replayCursorDescriptor instead')
const ReplayCursor$json = {
  '1': 'ReplayCursor',
  '2': [
    {
      '1': 'authority',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'authority'
    },
    {'1': 'generation', '3': 2, '4': 1, '5': 9, '10': 'generation'},
    {'1': 'revision', '3': 3, '4': 1, '5': 4, '10': 'revision'},
  ],
};

/// Descriptor for `ReplayCursor`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List replayCursorDescriptor = $convert.base64Decode(
    'CgxSZXBsYXlDdXJzb3ISOwoJYXV0aG9yaXR5GAEgASgLMh0uYWN5Y2xpYy5oYXJuZXNzLnYyLk'
    'F1dGhvcml0eVIJYXV0aG9yaXR5Eh4KCmdlbmVyYXRpb24YAiABKAlSCmdlbmVyYXRpb24SGgoI'
    'cmV2aXNpb24YAyABKARSCHJldmlzaW9u');

@$core.Deprecated('Use resumeRequestDescriptor instead')
const ResumeRequest$json = {
  '1': 'ResumeRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {
      '1': 'cursors',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ReplayCursor',
      '10': 'cursors'
    },
  ],
};

/// Descriptor for `ResumeRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List resumeRequestDescriptor = $convert.base64Decode(
    'Cg1SZXN1bWVSZXF1ZXN0EkEKCHByb3RvY29sGAEgASgLMiUuYWN5Y2xpYy5wcm90b2NvbC52MS'
    '5Qcm90b2NvbElkZW50aXR5Ughwcm90b2NvbBI6CgdjdXJzb3JzGAIgAygLMiAuYWN5Y2xpYy5o'
    'YXJuZXNzLnYyLlJlcGxheUN1cnNvclIHY3Vyc29ycw==');

@$core.Deprecated('Use deliveryDescriptor instead')
const Delivery$json = {
  '1': 'Delivery',
  '2': [
    {
      '1': 'authority',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'authority'
    },
    {'1': 'generation', '3': 2, '4': 1, '5': 9, '10': 'generation'},
    {'1': 'from_revision', '3': 3, '4': 1, '5': 4, '10': 'fromRevision'},
    {'1': 'through_revision', '3': 4, '4': 1, '5': 4, '10': 'throughRevision'},
    {
      '1': 'events',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.EventEnvelope',
      '10': 'events'
    },
    {'1': 'live', '3': 6, '4': 1, '5': 8, '10': 'live'},
  ],
};

/// Descriptor for `Delivery`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deliveryDescriptor = $convert.base64Decode(
    'CghEZWxpdmVyeRI7CglhdXRob3JpdHkYASABKAsyHS5hY3ljbGljLmhhcm5lc3MudjIuQXV0aG'
    '9yaXR5UglhdXRob3JpdHkSHgoKZ2VuZXJhdGlvbhgCIAEoCVIKZ2VuZXJhdGlvbhIjCg1mcm9t'
    'X3JldmlzaW9uGAMgASgEUgxmcm9tUmV2aXNpb24SKQoQdGhyb3VnaF9yZXZpc2lvbhgEIAEoBF'
    'IPdGhyb3VnaFJldmlzaW9uEjkKBmV2ZW50cxgFIAMoCzIhLmFjeWNsaWMuaGFybmVzcy52Mi5F'
    'dmVudEVudmVsb3BlUgZldmVudHMSEgoEbGl2ZRgGIAEoCFIEbGl2ZQ==');

@$core.Deprecated('Use acknowledgeDescriptor instead')
const Acknowledge$json = {
  '1': 'Acknowledge',
  '2': [
    {
      '1': 'authority',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'authority'
    },
    {'1': 'generation', '3': 2, '4': 1, '5': 9, '10': 'generation'},
    {'1': 'through_revision', '3': 3, '4': 1, '5': 4, '10': 'throughRevision'},
  ],
};

/// Descriptor for `Acknowledge`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List acknowledgeDescriptor = $convert.base64Decode(
    'CgtBY2tub3dsZWRnZRI7CglhdXRob3JpdHkYASABKAsyHS5hY3ljbGljLmhhcm5lc3MudjIuQX'
    'V0aG9yaXR5UglhdXRob3JpdHkSHgoKZ2VuZXJhdGlvbhgCIAEoCVIKZ2VuZXJhdGlvbhIpChB0'
    'aHJvdWdoX3JldmlzaW9uGAMgASgEUg90aHJvdWdoUmV2aXNpb24=');

@$core.Deprecated('Use clientFrameDescriptor instead')
const ClientFrame$json = {
  '1': 'ClientFrame',
  '2': [
    {
      '1': 'resume',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResumeRequest',
      '9': 0,
      '10': 'resume'
    },
    {
      '1': 'command',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.CommandEnvelope',
      '9': 0,
      '10': 'command'
    },
    {
      '1': 'acknowledge',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Acknowledge',
      '9': 0,
      '10': 'acknowledge'
    },
    {
      '1': 'handshake',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.HandshakeRequest',
      '9': 0,
      '10': 'handshake'
    },
    {
      '1': 'observe',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ObserveRequest',
      '9': 0,
      '10': 'observe'
    },
    {
      '1': 'cancel',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.CancelRequest',
      '9': 0,
      '10': 'cancel'
    },
  ],
  '8': [
    {'1': 'frame'},
  ],
};

/// Descriptor for `ClientFrame`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List clientFrameDescriptor = $convert.base64Decode(
    'CgtDbGllbnRGcmFtZRI7CgZyZXN1bWUYASABKAsyIS5hY3ljbGljLmhhcm5lc3MudjIuUmVzdW'
    '1lUmVxdWVzdEgAUgZyZXN1bWUSPwoHY29tbWFuZBgCIAEoCzIjLmFjeWNsaWMuaGFybmVzcy52'
    'Mi5Db21tYW5kRW52ZWxvcGVIAFIHY29tbWFuZBJDCgthY2tub3dsZWRnZRgDIAEoCzIfLmFjeW'
    'NsaWMuaGFybmVzcy52Mi5BY2tub3dsZWRnZUgAUgthY2tub3dsZWRnZRJFCgloYW5kc2hha2UY'
    'BCABKAsyJS5hY3ljbGljLnByb3RvY29sLnYxLkhhbmRzaGFrZVJlcXVlc3RIAFIJaGFuZHNoYW'
    'tlEj4KB29ic2VydmUYBSABKAsyIi5hY3ljbGljLmhhcm5lc3MudjIuT2JzZXJ2ZVJlcXVlc3RI'
    'AFIHb2JzZXJ2ZRI7CgZjYW5jZWwYBiABKAsyIS5hY3ljbGljLmhhcm5lc3MudjIuQ2FuY2VsUm'
    'VxdWVzdEgAUgZjYW5jZWxCBwoFZnJhbWU=');

@$core.Deprecated('Use serverFrameDescriptor instead')
const ServerFrame$json = {
  '1': 'ServerFrame',
  '2': [
    {
      '1': 'delivery',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Delivery',
      '9': 0,
      '10': 'delivery'
    },
    {
      '1': 'admission',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Admission',
      '9': 0,
      '10': 'admission'
    },
    {
      '1': 'error',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Error',
      '9': 0,
      '10': 'error'
    },
    {
      '1': 'handshake',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.HandshakeResponse',
      '9': 0,
      '10': 'handshake'
    },
    {
      '1': 'status',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationStatus',
      '9': 0,
      '10': 'status'
    },
    {
      '1': 'cancellation',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.CancelResponse',
      '9': 0,
      '10': 'cancellation'
    },
  ],
  '8': [
    {'1': 'frame'},
  ],
};

/// Descriptor for `ServerFrame`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List serverFrameDescriptor = $convert.base64Decode(
    'CgtTZXJ2ZXJGcmFtZRI6CghkZWxpdmVyeRgBIAEoCzIcLmFjeWNsaWMuaGFybmVzcy52Mi5EZW'
    'xpdmVyeUgAUghkZWxpdmVyeRI9CglhZG1pc3Npb24YAiABKAsyHS5hY3ljbGljLmhhcm5lc3Mu'
    'djIuQWRtaXNzaW9uSABSCWFkbWlzc2lvbhIxCgVlcnJvchgDIAEoCzIZLmFjeWNsaWMuaGFybm'
    'Vzcy52Mi5FcnJvckgAUgVlcnJvchJGCgloYW5kc2hha2UYBCABKAsyJi5hY3ljbGljLnByb3Rv'
    'Y29sLnYxLkhhbmRzaGFrZVJlc3BvbnNlSABSCWhhbmRzaGFrZRI9CgZzdGF0dXMYBSABKAsyIy'
    '5hY3ljbGljLmhhcm5lc3MudjIuT3BlcmF0aW9uU3RhdHVzSABSBnN0YXR1cxJICgxjYW5jZWxs'
    'YXRpb24YBiABKAsyIi5hY3ljbGljLmhhcm5lc3MudjIuQ2FuY2VsUmVzcG9uc2VIAFIMY2FuY2'
    'VsbGF0aW9uQgcKBWZyYW1l');

@$core.Deprecated('Use schedulerEventEnvelopeDescriptor instead')
const SchedulerEventEnvelope$json = {
  '1': 'SchedulerEventEnvelope',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.protocol.v1.ProtocolIdentity',
      '10': 'protocol'
    },
    {'1': 'revision', '3': 2, '4': 1, '5': 4, '10': 'revision'},
    {'1': 'operation_id', '3': 3, '4': 1, '5': 9, '10': 'operationId'},
    {'1': 'idempotency_key', '3': 4, '4': 1, '5': 9, '10': 'idempotencyKey'},
    {
      '1': 'canonical_event_json',
      '3': 5,
      '4': 1,
      '5': 12,
      '10': 'canonicalEventJson'
    },
    {'1': 'event_digest', '3': 6, '4': 1, '5': 12, '10': 'eventDigest'},
    {
      '1': 'committed_at_ms',
      '3': 7,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'committedAtMs',
      '17': true
    },
  ],
  '8': [
    {'1': '_committed_at_ms'},
  ],
};

/// Descriptor for `SchedulerEventEnvelope`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List schedulerEventEnvelopeDescriptor = $convert.base64Decode(
    'ChZTY2hlZHVsZXJFdmVudEVudmVsb3BlEkEKCHByb3RvY29sGAEgASgLMiUuYWN5Y2xpYy5wcm'
    '90b2NvbC52MS5Qcm90b2NvbElkZW50aXR5Ughwcm90b2NvbBIaCghyZXZpc2lvbhgCIAEoBFII'
    'cmV2aXNpb24SIQoMb3BlcmF0aW9uX2lkGAMgASgJUgtvcGVyYXRpb25JZBInCg9pZGVtcG90ZW'
    '5jeV9rZXkYBCABKAlSDmlkZW1wb3RlbmN5S2V5EjAKFGNhbm9uaWNhbF9ldmVudF9qc29uGAUg'
    'ASgMUhJjYW5vbmljYWxFdmVudEpzb24SIQoMZXZlbnRfZGlnZXN0GAYgASgMUgtldmVudERpZ2'
    'VzdBIrCg9jb21taXR0ZWRfYXRfbXMYByABKARIAFINY29tbWl0dGVkQXRNc4gBAUISChBfY29t'
    'bWl0dGVkX2F0X21z');

@$core.Deprecated('Use providerRefDescriptor instead')
const ProviderRef$json = {
  '1': 'ProviderRef',
  '2': [
    {'1': 'namespace', '3': 1, '4': 1, '5': 9, '10': 'namespace'},
    {'1': 'family', '3': 2, '4': 1, '5': 9, '10': 'family'},
    {'1': 'version', '3': 3, '4': 1, '5': 9, '10': 'version'},
  ],
};

/// Descriptor for `ProviderRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List providerRefDescriptor = $convert.base64Decode(
    'CgtQcm92aWRlclJlZhIcCgluYW1lc3BhY2UYASABKAlSCW5hbWVzcGFjZRIWCgZmYW1pbHkYAi'
    'ABKAlSBmZhbWlseRIYCgd2ZXJzaW9uGAMgASgJUgd2ZXJzaW9u');

@$core.Deprecated('Use volumeOwnerDescriptor instead')
const VolumeOwner$json = {
  '1': 'VolumeOwner',
  '2': [
    {'1': 'project', '3': 1, '4': 1, '5': 9, '9': 0, '10': 'project'},
    {'1': 'agent_id', '3': 2, '4': 1, '5': 9, '9': 0, '10': 'agentId'},
    {'1': 'session', '3': 3, '4': 1, '5': 9, '9': 0, '10': 'session'},
  ],
  '8': [
    {'1': 'owner'},
  ],
};

/// Descriptor for `VolumeOwner`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List volumeOwnerDescriptor = $convert.base64Decode(
    'CgtWb2x1bWVPd25lchIaCgdwcm9qZWN0GAEgASgJSABSB3Byb2plY3QSGwoIYWdlbnRfaWQYAi'
    'ABKAlIAFIHYWdlbnRJZBIaCgdzZXNzaW9uGAMgASgJSABSB3Nlc3Npb25CBwoFb3duZXI=');

@$core.Deprecated('Use volumeRefDescriptor instead')
const VolumeRef$json = {
  '1': 'VolumeRef',
  '2': [
    {
      '1': 'provider',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ProviderRef',
      '10': 'provider'
    },
    {'1': 'id', '3': 2, '4': 1, '5': 9, '10': 'id'},
    {
      '1': 'volume_class',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.VolumeClass',
      '10': 'volumeClass'
    },
    {
      '1': 'owner',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeOwner',
      '10': 'owner'
    },
  ],
};

/// Descriptor for `VolumeRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List volumeRefDescriptor = $convert.base64Decode(
    'CglWb2x1bWVSZWYSOwoIcHJvdmlkZXIYASABKAsyHy5hY3ljbGljLmhhcm5lc3MudjIuUHJvdm'
    'lkZXJSZWZSCHByb3ZpZGVyEg4KAmlkGAIgASgJUgJpZBJCCgx2b2x1bWVfY2xhc3MYAyABKA4y'
    'Hy5hY3ljbGljLmhhcm5lc3MudjIuVm9sdW1lQ2xhc3NSC3ZvbHVtZUNsYXNzEjUKBW93bmVyGA'
    'QgASgLMh8uYWN5Y2xpYy5oYXJuZXNzLnYyLlZvbHVtZU93bmVyUgVvd25lcg==');

@$core.Deprecated('Use fileDescriptorDescriptor instead')
const FileDescriptor$json = {
  '1': 'FileDescriptor',
  '2': [
    {'1': 'sha256', '3': 1, '4': 1, '5': 12, '10': 'sha256'},
    {'1': 'byte_length', '3': 2, '4': 1, '5': 4, '10': 'byteLength'},
    {'1': 'media_type', '3': 3, '4': 1, '5': 9, '10': 'mediaType'},
  ],
};

/// Descriptor for `FileDescriptor`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List fileDescriptorDescriptor = $convert.base64Decode(
    'Cg5GaWxlRGVzY3JpcHRvchIWCgZzaGEyNTYYASABKAxSBnNoYTI1NhIfCgtieXRlX2xlbmd0aB'
    'gCIAEoBFIKYnl0ZUxlbmd0aBIdCgptZWRpYV90eXBlGAMgASgJUgltZWRpYVR5cGU=');

@$core.Deprecated('Use fileRefDescriptor instead')
const FileRef$json = {
  '1': 'FileRef',
  '2': [
    {
      '1': 'volume',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'volume'
    },
    {'1': 'normalized_path', '3': 2, '4': 1, '5': 9, '10': 'normalizedPath'},
    {
      '1': 'immutable_version',
      '3': 3,
      '4': 1,
      '5': 9,
      '10': 'immutableVersion'
    },
    {
      '1': 'descriptor',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileDescriptor',
      '10': 'descriptor'
    },
    {'1': 'display_name', '3': 5, '4': 1, '5': 9, '10': 'displayName'},
  ],
};

/// Descriptor for `FileRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List fileRefDescriptor = $convert.base64Decode(
    'CgdGaWxlUmVmEjUKBnZvbHVtZRgBIAEoCzIdLmFjeWNsaWMuaGFybmVzcy52Mi5Wb2x1bWVSZW'
    'ZSBnZvbHVtZRInCg9ub3JtYWxpemVkX3BhdGgYAiABKAlSDm5vcm1hbGl6ZWRQYXRoEisKEWlt'
    'bXV0YWJsZV92ZXJzaW9uGAMgASgJUhBpbW11dGFibGVWZXJzaW9uEkIKCmRlc2NyaXB0b3IYBC'
    'ABKAsyIi5hY3ljbGljLmhhcm5lc3MudjIuRmlsZURlc2NyaXB0b3JSCmRlc2NyaXB0b3ISIQoM'
    'ZGlzcGxheV9uYW1lGAUgASgJUgtkaXNwbGF5TmFtZQ==');

@$core.Deprecated('Use taskOutcomeDescriptor instead')
const TaskOutcome$json = {
  '1': 'TaskOutcome',
  '2': [
    {
      '1': 'succeeded',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '9': 0,
      '10': 'succeeded'
    },
    {
      '1': 'failed_message',
      '3': 2,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'failedMessage'
    },
    {
      '1': 'indeterminate_operation_id',
      '3': 3,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'indeterminateOperationId'
    },
    {'1': 'cancelled', '3': 4, '4': 1, '5': 8, '9': 0, '10': 'cancelled'},
  ],
  '8': [
    {'1': 'result'},
  ],
};

/// Descriptor for `TaskOutcome`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List taskOutcomeDescriptor = $convert.base64Decode(
    'CgtUYXNrT3V0Y29tZRI7CglzdWNjZWVkZWQYASABKAsyGy5hY3ljbGljLmhhcm5lc3MudjIuRm'
    'lsZVJlZkgAUglzdWNjZWVkZWQSJwoOZmFpbGVkX21lc3NhZ2UYAiABKAlIAFINZmFpbGVkTWVz'
    'c2FnZRI+ChppbmRldGVybWluYXRlX29wZXJhdGlvbl9pZBgDIAEoCUgAUhhpbmRldGVybWluYX'
    'RlT3BlcmF0aW9uSWQSHgoJY2FuY2VsbGVkGAQgASgISABSCWNhbmNlbGxlZEIICgZyZXN1bHQ=');

@$core.Deprecated('Use extensionRecordDescriptor instead')
const ExtensionRecord$json = {
  '1': 'ExtensionRecord',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'version', '3': 2, '4': 1, '5': 13, '10': 'version'},
    {'1': 'schema_digest', '3': 3, '4': 1, '5': 12, '10': 'schemaDigest'},
    {
      '1': 'implementation_digest',
      '3': 4,
      '4': 1,
      '5': 12,
      '10': 'implementationDigest'
    },
    {
      '1': 'fork_policy',
      '3': 5,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.ExtensionForkPolicy',
      '10': 'forkPolicy'
    },
    {
      '1': 'content',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'content'
    },
  ],
};

/// Descriptor for `ExtensionRecord`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extensionRecordDescriptor = $convert.base64Decode(
    'Cg9FeHRlbnNpb25SZWNvcmQSEgoEbmFtZRgBIAEoCVIEbmFtZRIYCgd2ZXJzaW9uGAIgASgNUg'
    'd2ZXJzaW9uEiMKDXNjaGVtYV9kaWdlc3QYAyABKAxSDHNjaGVtYURpZ2VzdBIzChVpbXBsZW1l'
    'bnRhdGlvbl9kaWdlc3QYBCABKAxSFGltcGxlbWVudGF0aW9uRGlnZXN0EkgKC2ZvcmtfcG9saW'
    'N5GAUgASgOMicuYWN5Y2xpYy5oYXJuZXNzLnYyLkV4dGVuc2lvbkZvcmtQb2xpY3lSCmZvcmtQ'
    'b2xpY3kSNQoHY29udGVudBgGIAEoCzIbLmFjeWNsaWMuaGFybmVzcy52Mi5GaWxlUmVmUgdjb2'
    '50ZW50');

@$core.Deprecated('Use extensionStateMigrationDescriptor instead')
const ExtensionStateMigration$json = {
  '1': 'ExtensionStateMigration',
  '2': [
    {
      '1': 'previous',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.EventReference',
      '10': 'previous'
    },
    {
      '1': 'record',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionRecord',
      '10': 'record'
    },
  ],
};

/// Descriptor for `ExtensionStateMigration`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extensionStateMigrationDescriptor = $convert.base64Decode(
    'ChdFeHRlbnNpb25TdGF0ZU1pZ3JhdGlvbhI+CghwcmV2aW91cxgBIAEoCzIiLmFjeWNsaWMuaG'
    'FybmVzcy52Mi5FdmVudFJlZmVyZW5jZVIIcHJldmlvdXMSOwoGcmVjb3JkGAIgASgLMiMuYWN5'
    'Y2xpYy5oYXJuZXNzLnYyLkV4dGVuc2lvblJlY29yZFIGcmVjb3Jk');

@$core.Deprecated('Use extensionDependencyDescriptor instead')
const ExtensionDependency$json = {
  '1': 'ExtensionDependency',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'version', '3': 2, '4': 1, '5': 13, '10': 'version'},
  ],
};

/// Descriptor for `ExtensionDependency`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extensionDependencyDescriptor = $convert.base64Decode(
    'ChNFeHRlbnNpb25EZXBlbmRlbmN5EhIKBG5hbWUYASABKAlSBG5hbWUSGAoHdmVyc2lvbhgCIA'
    'EoDVIHdmVyc2lvbg==');

@$core.Deprecated('Use extensionConfigurationDescriptor instead')
const ExtensionConfiguration$json = {
  '1': 'ExtensionConfiguration',
  '2': [
    {
      '1': 'extension',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionDependency',
      '10': 'extension'
    },
    {'1': 'schema_digest', '3': 2, '4': 1, '5': 12, '10': 'schemaDigest'},
    {
      '1': 'content',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'content'
    },
  ],
};

/// Descriptor for `ExtensionConfiguration`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extensionConfigurationDescriptor = $convert.base64Decode(
    'ChZFeHRlbnNpb25Db25maWd1cmF0aW9uEkUKCWV4dGVuc2lvbhgBIAEoCzInLmFjeWNsaWMuaG'
    'FybmVzcy52Mi5FeHRlbnNpb25EZXBlbmRlbmN5UglleHRlbnNpb24SIwoNc2NoZW1hX2RpZ2Vz'
    'dBgCIAEoDFIMc2NoZW1hRGlnZXN0EjUKB2NvbnRlbnQYAyABKAsyGy5hY3ljbGljLmhhcm5lc3'
    'MudjIuRmlsZVJlZlIHY29udGVudA==');

@$core.Deprecated('Use extensionSelectionDescriptor instead')
const ExtensionSelection$json = {
  '1': 'ExtensionSelection',
  '2': [
    {
      '1': 'previous',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionDependency',
      '10': 'previous'
    },
    {
      '1': 'selected',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionDependency',
      '10': 'selected'
    },
    {
      '1': 'configurations',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionConfiguration',
      '10': 'configurations'
    },
  ],
};

/// Descriptor for `ExtensionSelection`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extensionSelectionDescriptor = $convert.base64Decode(
    'ChJFeHRlbnNpb25TZWxlY3Rpb24SQwoIcHJldmlvdXMYASADKAsyJy5hY3ljbGljLmhhcm5lc3'
    'MudjIuRXh0ZW5zaW9uRGVwZW5kZW5jeVIIcHJldmlvdXMSQwoIc2VsZWN0ZWQYAiADKAsyJy5h'
    'Y3ljbGljLmhhcm5lc3MudjIuRXh0ZW5zaW9uRGVwZW5kZW5jeVIIc2VsZWN0ZWQSUgoOY29uZm'
    'lndXJhdGlvbnMYAyADKAsyKi5hY3ljbGljLmhhcm5lc3MudjIuRXh0ZW5zaW9uQ29uZmlndXJh'
    'dGlvblIOY29uZmlndXJhdGlvbnM=');

@$core.Deprecated('Use extensionConfiguredDescriptor instead')
const ExtensionConfigured$json = {
  '1': 'ExtensionConfigured',
  '2': [
    {
      '1': 'previous',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionConfiguration',
      '10': 'previous'
    },
    {
      '1': 'record',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionConfiguration',
      '10': 'record'
    },
  ],
};

/// Descriptor for `ExtensionConfigured`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extensionConfiguredDescriptor = $convert.base64Decode(
    'ChNFeHRlbnNpb25Db25maWd1cmVkEkYKCHByZXZpb3VzGAEgASgLMiouYWN5Y2xpYy5oYXJuZX'
    'NzLnYyLkV4dGVuc2lvbkNvbmZpZ3VyYXRpb25SCHByZXZpb3VzEkIKBnJlY29yZBgCIAEoCzIq'
    'LmFjeWNsaWMuaGFybmVzcy52Mi5FeHRlbnNpb25Db25maWd1cmF0aW9uUgZyZWNvcmQ=');

@$core.Deprecated('Use extensionAdmissionDescriptor instead')
const ExtensionAdmission$json = {
  '1': 'ExtensionAdmission',
  '2': [
    {
      '1': 'source',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.EventReference',
      '10': 'source'
    },
    {
      '1': 'selected',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionDependency',
      '10': 'selected'
    },
    {
      '1': 'configurations',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionConfiguration',
      '10': 'configurations'
    },
  ],
};

/// Descriptor for `ExtensionAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extensionAdmissionDescriptor = $convert.base64Decode(
    'ChJFeHRlbnNpb25BZG1pc3Npb24SOgoGc291cmNlGAEgASgLMiIuYWN5Y2xpYy5oYXJuZXNzLn'
    'YyLkV2ZW50UmVmZXJlbmNlUgZzb3VyY2USQwoIc2VsZWN0ZWQYAiADKAsyJy5hY3ljbGljLmhh'
    'cm5lc3MudjIuRXh0ZW5zaW9uRGVwZW5kZW5jeVIIc2VsZWN0ZWQSUgoOY29uZmlndXJhdGlvbn'
    'MYAyADKAsyKi5hY3ljbGljLmhhcm5lc3MudjIuRXh0ZW5zaW9uQ29uZmlndXJhdGlvblIOY29u'
    'ZmlndXJhdGlvbnM=');

@$core.Deprecated('Use modelContextSelectionDescriptor instead')
const ModelContextSelection$json = {
  '1': 'ModelContextSelection',
  '2': [
    {
      '1': 'conversation_revision',
      '3': 1,
      '4': 1,
      '5': 4,
      '10': 'conversationRevision'
    },
    {'1': 'message_ids', '3': 2, '4': 3, '5': 9, '10': 'messageIds'},
  ],
};

/// Descriptor for `ModelContextSelection`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List modelContextSelectionDescriptor = $convert.base64Decode(
    'ChVNb2RlbENvbnRleHRTZWxlY3Rpb24SMwoVY29udmVyc2F0aW9uX3JldmlzaW9uGAEgASgEUh'
    'Rjb252ZXJzYXRpb25SZXZpc2lvbhIfCgttZXNzYWdlX2lkcxgCIAMoCVIKbWVzc2FnZUlkcw==');

@$core.Deprecated('Use approvalBindingDescriptor instead')
const ApprovalBinding$json = {
  '1': 'ApprovalBinding',
  '2': [
    {'1': 'operation_id', '3': 1, '4': 1, '5': 9, '10': 'operationId'},
    {'1': 'action_digest', '3': 2, '4': 1, '5': 12, '10': 'actionDigest'},
  ],
};

/// Descriptor for `ApprovalBinding`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List approvalBindingDescriptor = $convert.base64Decode(
    'Cg9BcHByb3ZhbEJpbmRpbmcSIQoMb3BlcmF0aW9uX2lkGAEgASgJUgtvcGVyYXRpb25JZBIjCg'
    '1hY3Rpb25fZGlnZXN0GAIgASgMUgxhY3Rpb25EaWdlc3Q=');

@$core.Deprecated('Use interactionTicketDescriptor instead')
const InteractionTicket$json = {
  '1': 'InteractionTicket',
  '2': [
    {'1': 'id', '3': 1, '4': 1, '5': 9, '10': 'id'},
    {
      '1': 'kind',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.InteractionKind',
      '10': 'kind'
    },
    {
      '1': 'request',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'request'
    },
    {
      '1': 'deadline_unix_ms',
      '3': 4,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'deadlineUnixMs',
      '17': true
    },
    {
      '1': 'approval',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ApprovalBinding',
      '10': 'approval'
    },
  ],
  '8': [
    {'1': '_deadline_unix_ms'},
  ],
};

/// Descriptor for `InteractionTicket`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List interactionTicketDescriptor = $convert.base64Decode(
    'ChFJbnRlcmFjdGlvblRpY2tldBIOCgJpZBgBIAEoCVICaWQSNwoEa2luZBgCIAEoDjIjLmFjeW'
    'NsaWMuaGFybmVzcy52Mi5JbnRlcmFjdGlvbktpbmRSBGtpbmQSNQoHcmVxdWVzdBgDIAEoCzIb'
    'LmFjeWNsaWMuaGFybmVzcy52Mi5GaWxlUmVmUgdyZXF1ZXN0Ei0KEGRlYWRsaW5lX3VuaXhfbX'
    'MYBCABKARIAFIOZGVhZGxpbmVVbml4TXOIAQESPwoIYXBwcm92YWwYBSABKAsyIy5hY3ljbGlj'
    'Lmhhcm5lc3MudjIuQXBwcm92YWxCaW5kaW5nUghhcHByb3ZhbEITChFfZGVhZGxpbmVfdW5peF'
    '9tcw==');

@$core.Deprecated('Use interactionOutcomeMarkerDescriptor instead')
const InteractionOutcomeMarker$json = {
  '1': 'InteractionOutcomeMarker',
};

/// Descriptor for `InteractionOutcomeMarker`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List interactionOutcomeMarkerDescriptor =
    $convert.base64Decode('ChhJbnRlcmFjdGlvbk91dGNvbWVNYXJrZXI=');

@$core.Deprecated('Use interactionOutcomeDescriptor instead')
const InteractionOutcome$json = {
  '1': 'InteractionOutcome',
  '2': [
    {
      '1': 'answered',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '9': 0,
      '10': 'answered'
    },
    {
      '1': 'approved',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.InteractionOutcomeMarker',
      '9': 0,
      '10': 'approved'
    },
    {
      '1': 'declined',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.InteractionOutcomeMarker',
      '9': 0,
      '10': 'declined'
    },
    {
      '1': 'cancelled',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.InteractionOutcomeMarker',
      '9': 0,
      '10': 'cancelled'
    },
    {
      '1': 'expired',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.InteractionOutcomeMarker',
      '9': 0,
      '10': 'expired'
    },
    {
      '1': 'denied',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.InteractionOutcomeMarker',
      '9': 0,
      '10': 'denied'
    },
    {
      '1': 'indeterminate_operation_id',
      '3': 7,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'indeterminateOperationId'
    },
  ],
  '8': [
    {'1': 'kind'},
  ],
};

/// Descriptor for `InteractionOutcome`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List interactionOutcomeDescriptor = $convert.base64Decode(
    'ChJJbnRlcmFjdGlvbk91dGNvbWUSOQoIYW5zd2VyZWQYASABKAsyGy5hY3ljbGljLmhhcm5lc3'
    'MudjIuRmlsZVJlZkgAUghhbnN3ZXJlZBJKCghhcHByb3ZlZBgCIAEoCzIsLmFjeWNsaWMuaGFy'
    'bmVzcy52Mi5JbnRlcmFjdGlvbk91dGNvbWVNYXJrZXJIAFIIYXBwcm92ZWQSSgoIZGVjbGluZW'
    'QYAyABKAsyLC5hY3ljbGljLmhhcm5lc3MudjIuSW50ZXJhY3Rpb25PdXRjb21lTWFya2VySABS'
    'CGRlY2xpbmVkEkwKCWNhbmNlbGxlZBgEIAEoCzIsLmFjeWNsaWMuaGFybmVzcy52Mi5JbnRlcm'
    'FjdGlvbk91dGNvbWVNYXJrZXJIAFIJY2FuY2VsbGVkEkgKB2V4cGlyZWQYBSABKAsyLC5hY3lj'
    'bGljLmhhcm5lc3MudjIuSW50ZXJhY3Rpb25PdXRjb21lTWFya2VySABSB2V4cGlyZWQSRgoGZG'
    'VuaWVkGAYgASgLMiwuYWN5Y2xpYy5oYXJuZXNzLnYyLkludGVyYWN0aW9uT3V0Y29tZU1hcmtl'
    'ckgAUgZkZW5pZWQSPgoaaW5kZXRlcm1pbmF0ZV9vcGVyYXRpb25faWQYByABKAlIAFIYaW5kZX'
    'Rlcm1pbmF0ZU9wZXJhdGlvbklkQgYKBGtpbmQ=');

@$core.Deprecated('Use interactionResolutionDescriptor instead')
const InteractionResolution$json = {
  '1': 'InteractionResolution',
  '2': [
    {'1': 'id', '3': 1, '4': 1, '5': 9, '10': 'id'},
    {'1': 'expected_version', '3': 2, '4': 1, '5': 4, '10': 'expectedVersion'},
    {
      '1': 'outcome',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.InteractionOutcome',
      '10': 'outcome'
    },
    {
      '1': 'detail',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'detail'
    },
  ],
};

/// Descriptor for `InteractionResolution`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List interactionResolutionDescriptor = $convert.base64Decode(
    'ChVJbnRlcmFjdGlvblJlc29sdXRpb24SDgoCaWQYASABKAlSAmlkEikKEGV4cGVjdGVkX3Zlcn'
    'Npb24YAiABKARSD2V4cGVjdGVkVmVyc2lvbhJACgdvdXRjb21lGAMgASgLMiYuYWN5Y2xpYy5o'
    'YXJuZXNzLnYyLkludGVyYWN0aW9uT3V0Y29tZVIHb3V0Y29tZRIzCgZkZXRhaWwYBCABKAsyGy'
    '5hY3ljbGljLmhhcm5lc3MudjIuRmlsZVJlZlIGZGV0YWls');

@$core.Deprecated('Use resolutionReceiptDescriptor instead')
const ResolutionReceipt$json = {
  '1': 'ResolutionReceipt',
  '2': [
    {'1': 'id', '3': 1, '4': 1, '5': 9, '10': 'id'},
    {'1': 'version', '3': 2, '4': 1, '5': 4, '10': 'version'},
    {'1': 'operation_id', '3': 3, '4': 1, '5': 9, '10': 'operationId'},
    {
      '1': 'conversation_revision',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'conversationRevision'
    },
    {'1': 'replayed', '3': 5, '4': 1, '5': 8, '10': 'replayed'},
    {
      '1': 'outcome',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.InteractionOutcome',
      '10': 'outcome'
    },
  ],
};

/// Descriptor for `ResolutionReceipt`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List resolutionReceiptDescriptor = $convert.base64Decode(
    'ChFSZXNvbHV0aW9uUmVjZWlwdBIOCgJpZBgBIAEoCVICaWQSGAoHdmVyc2lvbhgCIAEoBFIHdm'
    'Vyc2lvbhIhCgxvcGVyYXRpb25faWQYAyABKAlSC29wZXJhdGlvbklkEjMKFWNvbnZlcnNhdGlv'
    'bl9yZXZpc2lvbhgEIAEoBFIUY29udmVyc2F0aW9uUmV2aXNpb24SGgoIcmVwbGF5ZWQYBSABKA'
    'hSCHJlcGxheWVkEkAKB291dGNvbWUYBiABKAsyJi5hY3ljbGljLmhhcm5lc3MudjIuSW50ZXJh'
    'Y3Rpb25PdXRjb21lUgdvdXRjb21l');

@$core.Deprecated('Use attachmentDescriptor instead')
const Attachment$json = {
  '1': 'Attachment',
  '2': [
    {
      '1': 'file',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'file'
    },
    {'1': 'label', '3': 2, '4': 1, '5': 9, '9': 0, '10': 'label', '17': true},
  ],
  '8': [
    {'1': '_label'},
  ],
};

/// Descriptor for `Attachment`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List attachmentDescriptor = $convert.base64Decode(
    'CgpBdHRhY2htZW50Ei8KBGZpbGUYASABKAsyGy5hY3ljbGljLmhhcm5lc3MudjIuRmlsZVJlZl'
    'IEZmlsZRIZCgVsYWJlbBgCIAEoCUgAUgVsYWJlbIgBAUIICgZfbGFiZWw=');

@$core.Deprecated('Use attachmentItemsDescriptor instead')
const AttachmentItems$json = {
  '1': 'AttachmentItems',
  '2': [
    {
      '1': 'items',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.Attachment',
      '10': 'items'
    },
  ],
};

/// Descriptor for `AttachmentItems`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List attachmentItemsDescriptor = $convert.base64Decode(
    'Cg9BdHRhY2htZW50SXRlbXMSNAoFaXRlbXMYASADKAsyHi5hY3ljbGljLmhhcm5lc3MudjIuQX'
    'R0YWNobWVudFIFaXRlbXM=');

@$core.Deprecated('Use attachmentManifestDescriptor instead')
const AttachmentManifest$json = {
  '1': 'AttachmentManifest',
  '2': [
    {
      '1': 'manifest',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'manifest'
    },
    {'1': 'item_count', '3': 2, '4': 1, '5': 13, '10': 'itemCount'},
  ],
};

/// Descriptor for `AttachmentManifest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List attachmentManifestDescriptor = $convert.base64Decode(
    'ChJBdHRhY2htZW50TWFuaWZlc3QSNwoIbWFuaWZlc3QYASABKAsyGy5hY3ljbGljLmhhcm5lc3'
    'MudjIuRmlsZVJlZlIIbWFuaWZlc3QSHQoKaXRlbV9jb3VudBgCIAEoDVIJaXRlbUNvdW50');

@$core.Deprecated('Use referencedAttachmentsDescriptor instead')
const ReferencedAttachments$json = {
  '1': 'ReferencedAttachments',
  '2': [
    {
      '1': 'inline_items',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.AttachmentItems',
      '9': 0,
      '10': 'inlineItems'
    },
    {
      '1': 'manifest',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.AttachmentManifest',
      '9': 0,
      '10': 'manifest'
    },
  ],
  '8': [
    {'1': 'source'},
  ],
};

/// Descriptor for `ReferencedAttachments`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List referencedAttachmentsDescriptor = $convert.base64Decode(
    'ChVSZWZlcmVuY2VkQXR0YWNobWVudHMSSAoMaW5saW5lX2l0ZW1zGAEgASgLMiMuYWN5Y2xpYy'
    '5oYXJuZXNzLnYyLkF0dGFjaG1lbnRJdGVtc0gAUgtpbmxpbmVJdGVtcxJECghtYW5pZmVzdBgC'
    'IAEoCzImLmFjeWNsaWMuaGFybmVzcy52Mi5BdHRhY2htZW50TWFuaWZlc3RIAFIIbWFuaWZlc3'
    'RCCAoGc291cmNl');

@$core.Deprecated('Use conversationMessageDescriptor instead')
const ConversationMessage$json = {
  '1': 'ConversationMessage',
  '2': [
    {'1': 'id', '3': 1, '4': 1, '5': 9, '10': 'id'},
    {'1': 'sequence', '3': 2, '4': 1, '5': 4, '10': 'sequence'},
    {
      '1': 'kind',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.ConversationKind',
      '10': 'kind'
    },
    {
      '1': 'content',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'content'
    },
    {
      '1': 'attachments',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ReferencedAttachments',
      '10': 'attachments'
    },
    {
      '1': 'reply_to',
      '3': 6,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'replyTo',
      '17': true
    },
    {
      '1': 'tool_call_id',
      '3': 7,
      '4': 1,
      '5': 9,
      '9': 1,
      '10': 'toolCallId',
      '17': true
    },
    {
      '1': 'extensions',
      '3': 8,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ConversationMessage.ExtensionsEntry',
      '10': 'extensions'
    },
  ],
  '3': [ConversationMessage_ExtensionsEntry$json],
  '8': [
    {'1': '_reply_to'},
    {'1': '_tool_call_id'},
  ],
};

@$core.Deprecated('Use conversationMessageDescriptor instead')
const ConversationMessage_ExtensionsEntry$json = {
  '1': 'ExtensionsEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {
      '1': 'value',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'value'
    },
  ],
  '7': {'7': true},
};

/// Descriptor for `ConversationMessage`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List conversationMessageDescriptor = $convert.base64Decode(
    'ChNDb252ZXJzYXRpb25NZXNzYWdlEg4KAmlkGAEgASgJUgJpZBIaCghzZXF1ZW5jZRgCIAEoBF'
    'IIc2VxdWVuY2USOAoEa2luZBgDIAEoDjIkLmFjeWNsaWMuaGFybmVzcy52Mi5Db252ZXJzYXRp'
    'b25LaW5kUgRraW5kEjUKB2NvbnRlbnQYBCABKAsyGy5hY3ljbGljLmhhcm5lc3MudjIuRmlsZV'
    'JlZlIHY29udGVudBJLCgthdHRhY2htZW50cxgFIAEoCzIpLmFjeWNsaWMuaGFybmVzcy52Mi5S'
    'ZWZlcmVuY2VkQXR0YWNobWVudHNSC2F0dGFjaG1lbnRzEh4KCHJlcGx5X3RvGAYgASgJSABSB3'
    'JlcGx5VG+IAQESJQoMdG9vbF9jYWxsX2lkGAcgASgJSAFSCnRvb2xDYWxsSWSIAQESVwoKZXh0'
    'ZW5zaW9ucxgIIAMoCzI3LmFjeWNsaWMuaGFybmVzcy52Mi5Db252ZXJzYXRpb25NZXNzYWdlLk'
    'V4dGVuc2lvbnNFbnRyeVIKZXh0ZW5zaW9ucxpaCg9FeHRlbnNpb25zRW50cnkSEAoDa2V5GAEg'
    'ASgJUgNrZXkSMQoFdmFsdWUYAiABKAsyGy5hY3ljbGljLmhhcm5lc3MudjIuRmlsZVJlZlIFdm'
    'FsdWU6AjgBQgsKCV9yZXBseV90b0IPCg1fdG9vbF9jYWxsX2lk');

@$core.Deprecated('Use resourceRefDescriptor instead')
const ResourceRef$json = {
  '1': 'ResourceRef',
  '2': [
    {
      '1': 'provider',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ProviderRef',
      '10': 'provider'
    },
    {'1': 'key', '3': 2, '4': 1, '5': 12, '10': 'key'},
    {
      '1': 'version',
      '3': 3,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'version',
      '17': true
    },
    {
      '1': 'kind',
      '3': 4,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.ResourceKind',
      '10': 'kind'
    },
  ],
  '8': [
    {'1': '_version'},
  ],
};

/// Descriptor for `ResourceRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List resourceRefDescriptor = $convert.base64Decode(
    'CgtSZXNvdXJjZVJlZhI7Cghwcm92aWRlchgBIAEoCzIfLmFjeWNsaWMuaGFybmVzcy52Mi5Qcm'
    '92aWRlclJlZlIIcHJvdmlkZXISEAoDa2V5GAIgASgMUgNrZXkSHQoHdmVyc2lvbhgDIAEoCUgA'
    'Ugd2ZXJzaW9uiAEBEjQKBGtpbmQYBCABKA4yIC5hY3ljbGljLmhhcm5lc3MudjIuUmVzb3VyY2'
    'VLaW5kUgRraW5kQgoKCF92ZXJzaW9u');

@$core.Deprecated('Use componentIdentityDescriptor instead')
const ComponentIdentity$json = {
  '1': 'ComponentIdentity',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'version', '3': 2, '4': 1, '5': 9, '10': 'version'},
    {'1': 'digest', '3': 3, '4': 1, '5': 12, '10': 'digest'},
  ],
};

/// Descriptor for `ComponentIdentity`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List componentIdentityDescriptor = $convert.base64Decode(
    'ChFDb21wb25lbnRJZGVudGl0eRISCgRuYW1lGAEgASgJUgRuYW1lEhgKB3ZlcnNpb24YAiABKA'
    'lSB3ZlcnNpb24SFgoGZGlnZXN0GAMgASgMUgZkaWdlc3Q=');

@$core.Deprecated('Use machineIdentityDescriptor instead')
const MachineIdentity$json = {
  '1': 'MachineIdentity',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'version', '3': 2, '4': 1, '5': 9, '10': 'version'},
    {'1': 'digest', '3': 3, '4': 1, '5': 12, '10': 'digest'},
  ],
};

/// Descriptor for `MachineIdentity`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machineIdentityDescriptor = $convert.base64Decode(
    'Cg9NYWNoaW5lSWRlbnRpdHkSEgoEbmFtZRgBIAEoCVIEbmFtZRIYCgd2ZXJzaW9uGAIgASgJUg'
    'd2ZXJzaW9uEhYKBmRpZ2VzdBgDIAEoDFIGZGlnZXN0');

@$core.Deprecated('Use machineCheckpointDescriptor instead')
const MachineCheckpoint$json = {
  '1': 'MachineCheckpoint',
  '2': [
    {
      '1': 'machine',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.MachineIdentity',
      '10': 'machine'
    },
    {'1': 'revision', '3': 2, '4': 1, '5': 4, '10': 'revision'},
    {
      '1': 'canonical_state_json',
      '3': 3,
      '4': 1,
      '5': 12,
      '10': 'canonicalStateJson'
    },
  ],
};

/// Descriptor for `MachineCheckpoint`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machineCheckpointDescriptor = $convert.base64Decode(
    'ChFNYWNoaW5lQ2hlY2twb2ludBI9CgdtYWNoaW5lGAEgASgLMiMuYWN5Y2xpYy5oYXJuZXNzLn'
    'YyLk1hY2hpbmVJZGVudGl0eVIHbWFjaGluZRIaCghyZXZpc2lvbhgCIAEoBFIIcmV2aXNpb24S'
    'MAoUY2Fub25pY2FsX3N0YXRlX2pzb24YAyABKAxSEmNhbm9uaWNhbFN0YXRlSnNvbg==');

@$core.Deprecated('Use workflowAdmissionDescriptor instead')
const WorkflowAdmission$json = {
  '1': 'WorkflowAdmission',
  '2': [
    {'1': 'operation_id', '3': 1, '4': 1, '5': 9, '10': 'operationId'},
    {'1': 'request_digest', '3': 2, '4': 1, '5': 12, '10': 'requestDigest'},
    {
      '1': 'initial',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.MachineCheckpoint',
      '10': 'initial'
    },
  ],
};

/// Descriptor for `WorkflowAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workflowAdmissionDescriptor = $convert.base64Decode(
    'ChFXb3JrZmxvd0FkbWlzc2lvbhIhCgxvcGVyYXRpb25faWQYASABKAlSC29wZXJhdGlvbklkEi'
    'UKDnJlcXVlc3RfZGlnZXN0GAIgASgMUg1yZXF1ZXN0RGlnZXN0Ej8KB2luaXRpYWwYAyABKAsy'
    'JS5hY3ljbGljLmhhcm5lc3MudjIuTWFjaGluZUNoZWNrcG9pbnRSB2luaXRpYWw=');

@$core.Deprecated('Use workflowCommandDescriptor instead')
const WorkflowCommand$json = {
  '1': 'WorkflowCommand',
  '2': [
    {'1': 'operation_id', '3': 1, '4': 1, '5': 9, '10': 'operationId'},
    {'1': 'kind', '3': 2, '4': 1, '5': 9, '10': 'kind'},
    {
      '1': 'payload',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'payload'
    },
  ],
};

/// Descriptor for `WorkflowCommand`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workflowCommandDescriptor = $convert.base64Decode(
    'Cg9Xb3JrZmxvd0NvbW1hbmQSIQoMb3BlcmF0aW9uX2lkGAEgASgJUgtvcGVyYXRpb25JZBISCg'
    'RraW5kGAIgASgJUgRraW5kEjUKB3BheWxvYWQYAyABKAsyGy5hY3ljbGljLmhhcm5lc3MudjIu'
    'RmlsZVJlZlIHcGF5bG9hZA==');

@$core.Deprecated('Use workflowTransitionDescriptor instead')
const WorkflowTransition$json = {
  '1': 'WorkflowTransition',
  '2': [
    {
      '1': 'canonical_state_json',
      '3': 1,
      '4': 1,
      '5': 12,
      '10': 'canonicalStateJson'
    },
    {
      '1': 'commands',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.WorkflowCommand',
      '10': 'commands'
    },
    {'1': 'suspended', '3': 3, '4': 1, '5': 8, '9': 0, '10': 'suspended'},
    {
      '1': 'canonical_completed_value_json',
      '3': 4,
      '4': 1,
      '5': 12,
      '9': 0,
      '10': 'canonicalCompletedValueJson'
    },
    {
      '1': 'failure_message',
      '3': 5,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'failureMessage'
    },
  ],
  '8': [
    {'1': 'status'},
  ],
};

/// Descriptor for `WorkflowTransition`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workflowTransitionDescriptor = $convert.base64Decode(
    'ChJXb3JrZmxvd1RyYW5zaXRpb24SMAoUY2Fub25pY2FsX3N0YXRlX2pzb24YASABKAxSEmNhbm'
    '9uaWNhbFN0YXRlSnNvbhI/Cghjb21tYW5kcxgCIAMoCzIjLmFjeWNsaWMuaGFybmVzcy52Mi5X'
    'b3JrZmxvd0NvbW1hbmRSCGNvbW1hbmRzEh4KCXN1c3BlbmRlZBgDIAEoCEgAUglzdXNwZW5kZW'
    'QSRQoeY2Fub25pY2FsX2NvbXBsZXRlZF92YWx1ZV9qc29uGAQgASgMSABSG2Nhbm9uaWNhbENv'
    'bXBsZXRlZFZhbHVlSnNvbhIpCg9mYWlsdXJlX21lc3NhZ2UYBSABKAlIAFIOZmFpbHVyZU1lc3'
    'NhZ2VCCAoGc3RhdHVz');

@$core.Deprecated('Use workflowRecordDescriptor instead')
const WorkflowRecord$json = {
  '1': 'WorkflowRecord',
  '2': [
    {'1': 'operation_id', '3': 1, '4': 1, '5': 9, '10': 'operationId'},
    {'1': 'idempotency_key', '3': 2, '4': 1, '5': 9, '10': 'idempotencyKey'},
    {'1': 'input_digest', '3': 3, '4': 1, '5': 12, '10': 'inputDigest'},
    {
      '1': 'prior',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.MachineCheckpoint',
      '10': 'prior'
    },
    {
      '1': 'canonical_input_json',
      '3': 5,
      '4': 1,
      '5': 12,
      '10': 'canonicalInputJson'
    },
    {
      '1': 'transition',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.WorkflowTransition',
      '10': 'transition'
    },
    {
      '1': 'next',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.MachineCheckpoint',
      '10': 'next'
    },
  ],
};

/// Descriptor for `WorkflowRecord`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List workflowRecordDescriptor = $convert.base64Decode(
    'Cg5Xb3JrZmxvd1JlY29yZBIhCgxvcGVyYXRpb25faWQYASABKAlSC29wZXJhdGlvbklkEicKD2'
    'lkZW1wb3RlbmN5X2tleRgCIAEoCVIOaWRlbXBvdGVuY3lLZXkSIQoMaW5wdXRfZGlnZXN0GAMg'
    'ASgMUgtpbnB1dERpZ2VzdBI7CgVwcmlvchgEIAEoCzIlLmFjeWNsaWMuaGFybmVzcy52Mi5NYW'
    'NoaW5lQ2hlY2twb2ludFIFcHJpb3ISMAoUY2Fub25pY2FsX2lucHV0X2pzb24YBSABKAxSEmNh'
    'bm9uaWNhbElucHV0SnNvbhJGCgp0cmFuc2l0aW9uGAYgASgLMiYuYWN5Y2xpYy5oYXJuZXNzLn'
    'YyLldvcmtmbG93VHJhbnNpdGlvblIKdHJhbnNpdGlvbhI5CgRuZXh0GAcgASgLMiUuYWN5Y2xp'
    'Yy5oYXJuZXNzLnYyLk1hY2hpbmVDaGVja3BvaW50UgRuZXh0');

@$core.Deprecated('Use runtimeLimitsDescriptor instead')
const RuntimeLimits$json = {
  '1': 'RuntimeLimits',
  '2': [
    {'1': 'file_bytes', '3': 1, '4': 1, '5': 4, '10': 'fileBytes'},
    {'1': 'path_bytes', '3': 2, '4': 1, '5': 4, '10': 'pathBytes'},
    {'1': 'attachments', '3': 3, '4': 1, '5': 4, '10': 'attachments'},
    {'1': 'render_bytes', '3': 4, '4': 1, '5': 4, '10': 'renderBytes'},
    {'1': 'model_steps', '3': 5, '4': 1, '5': 4, '10': 'modelSteps'},
    {
      '1': 'model_events_per_step',
      '3': 6,
      '4': 1,
      '5': 4,
      '10': 'modelEventsPerStep'
    },
    {
      '1': 'tool_calls_per_step',
      '3': 7,
      '4': 1,
      '5': 4,
      '10': 'toolCallsPerStep'
    },
    {'1': 'context_messages', '3': 8, '4': 1, '5': 4, '10': 'contextMessages'},
  ],
};

/// Descriptor for `RuntimeLimits`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List runtimeLimitsDescriptor = $convert.base64Decode(
    'Cg1SdW50aW1lTGltaXRzEh0KCmZpbGVfYnl0ZXMYASABKARSCWZpbGVCeXRlcxIdCgpwYXRoX2'
    'J5dGVzGAIgASgEUglwYXRoQnl0ZXMSIAoLYXR0YWNobWVudHMYAyABKARSC2F0dGFjaG1lbnRz'
    'EiEKDHJlbmRlcl9ieXRlcxgEIAEoBFILcmVuZGVyQnl0ZXMSHwoLbW9kZWxfc3RlcHMYBSABKA'
    'RSCm1vZGVsU3RlcHMSMQoVbW9kZWxfZXZlbnRzX3Blcl9zdGVwGAYgASgEUhJtb2RlbEV2ZW50'
    'c1BlclN0ZXASLQoTdG9vbF9jYWxsc19wZXJfc3RlcBgHIAEoBFIQdG9vbENhbGxzUGVyU3RlcB'
    'IpChBjb250ZXh0X21lc3NhZ2VzGAggASgEUg9jb250ZXh0TWVzc2FnZXM=');

@$core.Deprecated('Use taskRunLimitsDescriptor instead')
const TaskRunLimits$json = {
  '1': 'TaskRunLimits',
  '2': [
    {
      '1': 'concurrency',
      '3': 1,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'concurrency',
      '17': true
    },
    {
      '1': 'max_steps',
      '3': 2,
      '4': 1,
      '5': 4,
      '9': 1,
      '10': 'maxSteps',
      '17': true
    },
    {
      '1': 'deadline_epoch_ms',
      '3': 3,
      '4': 1,
      '5': 4,
      '9': 2,
      '10': 'deadlineEpochMs',
      '17': true
    },
  ],
  '8': [
    {'1': '_concurrency'},
    {'1': '_max_steps'},
    {'1': '_deadline_epoch_ms'},
  ],
};

/// Descriptor for `TaskRunLimits`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List taskRunLimitsDescriptor = $convert.base64Decode(
    'Cg1UYXNrUnVuTGltaXRzEiUKC2NvbmN1cnJlbmN5GAEgASgESABSC2NvbmN1cnJlbmN5iAEBEi'
    'AKCW1heF9zdGVwcxgCIAEoBEgBUghtYXhTdGVwc4gBARIvChFkZWFkbGluZV9lcG9jaF9tcxgD'
    'IAEoBEgCUg9kZWFkbGluZUVwb2NoTXOIAQFCDgoMX2NvbmN1cnJlbmN5QgwKCl9tYXhfc3RlcH'
    'NCFAoSX2RlYWRsaW5lX2Vwb2NoX21z');

@$core.Deprecated('Use executionPlacementDescriptor instead')
const ExecutionPlacement$json = {
  '1': 'ExecutionPlacement',
  '2': [
    {
      '1': 'provider',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ComponentIdentity',
      '10': 'provider'
    },
    {
      '1': 'build',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRef',
      '10': 'build'
    },
    {
      '1': 'environment',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRef',
      '10': 'environment'
    },
    {
      '1': 'readiness_revision',
      '3': 4,
      '4': 1,
      '5': 12,
      '10': 'readinessRevision'
    },
  ],
};

/// Descriptor for `ExecutionPlacement`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List executionPlacementDescriptor = $convert.base64Decode(
    'ChJFeGVjdXRpb25QbGFjZW1lbnQSQQoIcHJvdmlkZXIYASABKAsyJS5hY3ljbGljLmhhcm5lc3'
    'MudjIuQ29tcG9uZW50SWRlbnRpdHlSCHByb3ZpZGVyEjUKBWJ1aWxkGAIgASgLMh8uYWN5Y2xp'
    'Yy5oYXJuZXNzLnYyLlJlc291cmNlUmVmUgVidWlsZBJBCgtlbnZpcm9ubWVudBgDIAEoCzIfLm'
    'FjeWNsaWMuaGFybmVzcy52Mi5SZXNvdXJjZVJlZlILZW52aXJvbm1lbnQSLQoScmVhZGluZXNz'
    'X3JldmlzaW9uGAQgASgMUhFyZWFkaW5lc3NSZXZpc2lvbg==');

@$core.Deprecated('Use taskAdmissionRecordDescriptor instead')
const TaskAdmissionRecord$json = {
  '1': 'TaskAdmissionRecord',
  '2': [
    {'1': 'operation_id', '3': 1, '4': 1, '5': 9, '10': 'operationId'},
    {
      '1': 'task',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ComponentIdentity',
      '10': 'task'
    },
    {
      '1': 'machine',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.MachineIdentity',
      '10': 'machine'
    },
    {
      '1': 'canonical_input_json',
      '3': 4,
      '4': 1,
      '5': 12,
      '10': 'canonicalInputJson'
    },
    {
      '1': 'canonical_input_schema_json',
      '3': 5,
      '4': 1,
      '5': 12,
      '10': 'canonicalInputSchemaJson'
    },
    {
      '1': 'canonical_output_schema_json',
      '3': 6,
      '4': 1,
      '5': 12,
      '10': 'canonicalOutputSchemaJson'
    },
    {
      '1': 'parent_task_id',
      '3': 7,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'parentTaskId',
      '17': true
    },
    {'1': 'grants', '3': 8, '4': 3, '5': 9, '10': 'grants'},
    {
      '1': 'limits',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.RuntimeLimits',
      '10': 'limits'
    },
    {
      '1': 'policy',
      '3': 10,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ComponentIdentity',
      '10': 'policy'
    },
    {
      '1': 'extensions',
      '3': 11,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionAdmission',
      '10': 'extensions'
    },
    {
      '1': 'execution',
      '3': 12,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExecutionPlacement',
      '10': 'execution'
    },
    {
      '1': 'run_limits',
      '3': 13,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.TaskRunLimits',
      '10': 'runLimits'
    },
  ],
  '8': [
    {'1': '_parent_task_id'},
  ],
};

/// Descriptor for `TaskAdmissionRecord`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List taskAdmissionRecordDescriptor = $convert.base64Decode(
    'ChNUYXNrQWRtaXNzaW9uUmVjb3JkEiEKDG9wZXJhdGlvbl9pZBgBIAEoCVILb3BlcmF0aW9uSW'
    'QSOQoEdGFzaxgCIAEoCzIlLmFjeWNsaWMuaGFybmVzcy52Mi5Db21wb25lbnRJZGVudGl0eVIE'
    'dGFzaxI9CgdtYWNoaW5lGAMgASgLMiMuYWN5Y2xpYy5oYXJuZXNzLnYyLk1hY2hpbmVJZGVudG'
    'l0eVIHbWFjaGluZRIwChRjYW5vbmljYWxfaW5wdXRfanNvbhgEIAEoDFISY2Fub25pY2FsSW5w'
    'dXRKc29uEj0KG2Nhbm9uaWNhbF9pbnB1dF9zY2hlbWFfanNvbhgFIAEoDFIYY2Fub25pY2FsSW'
    '5wdXRTY2hlbWFKc29uEj8KHGNhbm9uaWNhbF9vdXRwdXRfc2NoZW1hX2pzb24YBiABKAxSGWNh'
    'bm9uaWNhbE91dHB1dFNjaGVtYUpzb24SKQoOcGFyZW50X3Rhc2tfaWQYByABKAlIAFIMcGFyZW'
    '50VGFza0lkiAEBEhYKBmdyYW50cxgIIAMoCVIGZ3JhbnRzEjkKBmxpbWl0cxgJIAEoCzIhLmFj'
    'eWNsaWMuaGFybmVzcy52Mi5SdW50aW1lTGltaXRzUgZsaW1pdHMSPQoGcG9saWN5GAogASgLMi'
    'UuYWN5Y2xpYy5oYXJuZXNzLnYyLkNvbXBvbmVudElkZW50aXR5UgZwb2xpY3kSRgoKZXh0ZW5z'
    'aW9ucxgLIAEoCzImLmFjeWNsaWMuaGFybmVzcy52Mi5FeHRlbnNpb25BZG1pc3Npb25SCmV4dG'
    'Vuc2lvbnMSRAoJZXhlY3V0aW9uGAwgASgLMiYuYWN5Y2xpYy5oYXJuZXNzLnYyLkV4ZWN1dGlv'
    'blBsYWNlbWVudFIJZXhlY3V0aW9uEkAKCnJ1bl9saW1pdHMYDSABKAsyIS5hY3ljbGljLmhhcm'
    '5lc3MudjIuVGFza1J1bkxpbWl0c1IJcnVuTGltaXRzQhEKD19wYXJlbnRfdGFza19pZA==');

@$core.Deprecated('Use durableBatchRequestDescriptor instead')
const DurableBatchRequest$json = {
  '1': 'DurableBatchRequest',
  '2': [
    {'1': 'group_id', '3': 1, '4': 1, '5': 9, '10': 'groupId'},
    {'1': 'batch_id', '3': 2, '4': 1, '5': 9, '10': 'batchId'},
    {
      '1': 'group_policy',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.BatchGroupPolicy',
      '10': 'groupPolicy'
    },
    {
      '1': 'task',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ComponentIdentity',
      '10': 'task'
    },
    {
      '1': 'machine',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.MachineIdentity',
      '10': 'machine'
    },
    {
      '1': 'canonical_input_json',
      '3': 6,
      '4': 3,
      '5': 12,
      '10': 'canonicalInputJson'
    },
    {
      '1': 'canonical_input_schema_json',
      '3': 7,
      '4': 1,
      '5': 12,
      '10': 'canonicalInputSchemaJson'
    },
    {
      '1': 'canonical_output_schema_json',
      '3': 8,
      '4': 1,
      '5': 12,
      '10': 'canonicalOutputSchemaJson'
    },
    {
      '1': 'parent_task_id',
      '3': 9,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'parentTaskId',
      '17': true
    },
    {'1': 'grants', '3': 10, '4': 3, '5': 9, '10': 'grants'},
    {
      '1': 'limits',
      '3': 11,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.RuntimeLimits',
      '10': 'limits'
    },
    {
      '1': 'extensions',
      '3': 12,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionAdmission',
      '10': 'extensions'
    },
    {
      '1': 'policy',
      '3': 13,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ComponentIdentity',
      '10': 'policy'
    },
    {
      '1': 'execution',
      '3': 14,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExecutionPlacement',
      '10': 'execution'
    },
    {
      '1': 'run_limits',
      '3': 15,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.TaskRunLimits',
      '10': 'runLimits'
    },
  ],
  '8': [
    {'1': '_parent_task_id'},
  ],
};

/// Descriptor for `DurableBatchRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List durableBatchRequestDescriptor = $convert.base64Decode(
    'ChNEdXJhYmxlQmF0Y2hSZXF1ZXN0EhkKCGdyb3VwX2lkGAEgASgJUgdncm91cElkEhkKCGJhdG'
    'NoX2lkGAIgASgJUgdiYXRjaElkEkcKDGdyb3VwX3BvbGljeRgDIAEoDjIkLmFjeWNsaWMuaGFy'
    'bmVzcy52Mi5CYXRjaEdyb3VwUG9saWN5Ugtncm91cFBvbGljeRI5CgR0YXNrGAQgASgLMiUuYW'
    'N5Y2xpYy5oYXJuZXNzLnYyLkNvbXBvbmVudElkZW50aXR5UgR0YXNrEj0KB21hY2hpbmUYBSAB'
    'KAsyIy5hY3ljbGljLmhhcm5lc3MudjIuTWFjaGluZUlkZW50aXR5UgdtYWNoaW5lEjAKFGNhbm'
    '9uaWNhbF9pbnB1dF9qc29uGAYgAygMUhJjYW5vbmljYWxJbnB1dEpzb24SPQobY2Fub25pY2Fs'
    'X2lucHV0X3NjaGVtYV9qc29uGAcgASgMUhhjYW5vbmljYWxJbnB1dFNjaGVtYUpzb24SPwocY2'
    'Fub25pY2FsX291dHB1dF9zY2hlbWFfanNvbhgIIAEoDFIZY2Fub25pY2FsT3V0cHV0U2NoZW1h'
    'SnNvbhIpCg5wYXJlbnRfdGFza19pZBgJIAEoCUgAUgxwYXJlbnRUYXNrSWSIAQESFgoGZ3Jhbn'
    'RzGAogAygJUgZncmFudHMSOQoGbGltaXRzGAsgASgLMiEuYWN5Y2xpYy5oYXJuZXNzLnYyLlJ1'
    'bnRpbWVMaW1pdHNSBmxpbWl0cxJGCgpleHRlbnNpb25zGAwgASgLMiYuYWN5Y2xpYy5oYXJuZX'
    'NzLnYyLkV4dGVuc2lvbkFkbWlzc2lvblIKZXh0ZW5zaW9ucxI9CgZwb2xpY3kYDSABKAsyJS5h'
    'Y3ljbGljLmhhcm5lc3MudjIuQ29tcG9uZW50SWRlbnRpdHlSBnBvbGljeRJECglleGVjdXRpb2'
    '4YDiABKAsyJi5hY3ljbGljLmhhcm5lc3MudjIuRXhlY3V0aW9uUGxhY2VtZW50UglleGVjdXRp'
    'b24SQAoKcnVuX2xpbWl0cxgPIAEoCzIhLmFjeWNsaWMuaGFybmVzcy52Mi5UYXNrUnVuTGltaX'
    'RzUglydW5MaW1pdHNCEQoPX3BhcmVudF90YXNrX2lk');

@$core.Deprecated('Use generationRefDescriptor instead')
const GenerationRef$json = {
  '1': 'GenerationRef',
  '2': [
    {
      '1': 'resource',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRef',
      '10': 'resource'
    },
  ],
};

/// Descriptor for `GenerationRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List generationRefDescriptor = $convert.base64Decode(
    'Cg1HZW5lcmF0aW9uUmVmEjsKCHJlc291cmNlGAEgASgLMh8uYWN5Y2xpYy5oYXJuZXNzLnYyLl'
    'Jlc291cmNlUmVmUghyZXNvdXJjZQ==');

@$core.Deprecated('Use privateDirectoryEntryDescriptor instead')
const PrivateDirectoryEntry$json = {
  '1': 'PrivateDirectoryEntry',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {
      '1': 'kind',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.harness.v2.PrivateDirectoryEntry.Kind',
      '10': 'kind'
    },
  ],
  '4': [PrivateDirectoryEntry_Kind$json],
};

@$core.Deprecated('Use privateDirectoryEntryDescriptor instead')
const PrivateDirectoryEntry_Kind$json = {
  '1': 'Kind',
  '2': [
    {'1': 'KIND_UNSPECIFIED', '2': 0},
    {'1': 'KIND_FILE', '2': 1},
    {'1': 'KIND_DIRECTORY', '2': 2},
  ],
};

/// Descriptor for `PrivateDirectoryEntry`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List privateDirectoryEntryDescriptor = $convert.base64Decode(
    'ChVQcml2YXRlRGlyZWN0b3J5RW50cnkSEgoEbmFtZRgBIAEoCVIEbmFtZRJCCgRraW5kGAIgAS'
    'gOMi4uYWN5Y2xpYy5oYXJuZXNzLnYyLlByaXZhdGVEaXJlY3RvcnlFbnRyeS5LaW5kUgRraW5k'
    'Ij8KBEtpbmQSFAoQS0lORF9VTlNQRUNJRklFRBAAEg0KCUtJTkRfRklMRRABEhIKDktJTkRfRE'
    'lSRUNUT1JZEAI=');

@$core.Deprecated('Use privateDirectoryPageDescriptor instead')
const PrivateDirectoryPage$json = {
  '1': 'PrivateDirectoryPage',
  '2': [
    {
      '1': 'generation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.GenerationRef',
      '10': 'generation'
    },
    {
      '1': 'entries',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.PrivateDirectoryEntry',
      '10': 'entries'
    },
    {'1': 'has_more', '3': 3, '4': 1, '5': 8, '10': 'hasMore'},
  ],
};

/// Descriptor for `PrivateDirectoryPage`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List privateDirectoryPageDescriptor = $convert.base64Decode(
    'ChRQcml2YXRlRGlyZWN0b3J5UGFnZRJBCgpnZW5lcmF0aW9uGAEgASgLMiEuYWN5Y2xpYy5oYX'
    'JuZXNzLnYyLkdlbmVyYXRpb25SZWZSCmdlbmVyYXRpb24SQwoHZW50cmllcxgCIAMoCzIpLmFj'
    'eWNsaWMuaGFybmVzcy52Mi5Qcml2YXRlRGlyZWN0b3J5RW50cnlSB2VudHJpZXMSGQoIaGFzX2'
    '1vcmUYAyABKAhSB2hhc01vcmU=');

@$core.Deprecated('Use projectRevisionDescriptor instead')
const ProjectRevision$json = {
  '1': 'ProjectRevision',
  '2': [
    {
      '1': 'volume',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'volume'
    },
    {
      '1': 'generation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.GenerationRef',
      '10': 'generation'
    },
  ],
};

/// Descriptor for `ProjectRevision`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List projectRevisionDescriptor = $convert.base64Decode(
    'Cg9Qcm9qZWN0UmV2aXNpb24SNQoGdm9sdW1lGAEgASgLMh0uYWN5Y2xpYy5oYXJuZXNzLnYyLl'
    'ZvbHVtZVJlZlIGdm9sdW1lEkEKCmdlbmVyYXRpb24YAiABKAsyIS5hY3ljbGljLmhhcm5lc3Mu'
    'djIuR2VuZXJhdGlvblJlZlIKZ2VuZXJhdGlvbg==');

@$core.Deprecated('Use extensionRevisionDescriptor instead')
const ExtensionRevision$json = {
  '1': 'ExtensionRevision',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'version', '3': 2, '4': 1, '5': 13, '10': 'version'},
    {
      '1': 'reference',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRef',
      '10': 'reference'
    },
    {
      '1': 'implementation_digest',
      '3': 4,
      '4': 1,
      '5': 12,
      '10': 'implementationDigest'
    },
  ],
};

/// Descriptor for `ExtensionRevision`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List extensionRevisionDescriptor = $convert.base64Decode(
    'ChFFeHRlbnNpb25SZXZpc2lvbhISCgRuYW1lGAEgASgJUgRuYW1lEhgKB3ZlcnNpb24YAiABKA'
    '1SB3ZlcnNpb24SPQoJcmVmZXJlbmNlGAMgASgLMh8uYWN5Y2xpYy5oYXJuZXNzLnYyLlJlc291'
    'cmNlUmVmUglyZWZlcmVuY2USMwoVaW1wbGVtZW50YXRpb25fZGlnZXN0GAQgASgMUhRpbXBsZW'
    '1lbnRhdGlvbkRpZ2VzdA==');

@$core.Deprecated('Use resourceRevisionDescriptor instead')
const ResourceRevision$json = {
  '1': 'ResourceRevision',
  '2': [
    {
      '1': 'history',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRef',
      '9': 0,
      '10': 'history'
    },
    {
      '1': 'project',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ProjectRevision',
      '9': 0,
      '10': 'project'
    },
    {
      '1': 'context',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRef',
      '9': 0,
      '10': 'context'
    },
    {
      '1': 'process',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRef',
      '9': 0,
      '10': 'process'
    },
    {
      '1': 'artifact',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRef',
      '9': 0,
      '10': 'artifact'
    },
    {
      '1': 'shared_volume',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '9': 0,
      '10': 'sharedVolume'
    },
    {
      '1': 'extension',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ExtensionRevision',
      '9': 0,
      '10': 'extension'
    },
  ],
  '8': [
    {'1': 'kind'},
  ],
};

/// Descriptor for `ResourceRevision`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List resourceRevisionDescriptor = $convert.base64Decode(
    'ChBSZXNvdXJjZVJldmlzaW9uEjsKB2hpc3RvcnkYASABKAsyHy5hY3ljbGljLmhhcm5lc3Mudj'
    'IuUmVzb3VyY2VSZWZIAFIHaGlzdG9yeRI/Cgdwcm9qZWN0GAIgASgLMiMuYWN5Y2xpYy5oYXJu'
    'ZXNzLnYyLlByb2plY3RSZXZpc2lvbkgAUgdwcm9qZWN0EjsKB2NvbnRleHQYAyABKAsyHy5hY3'
    'ljbGljLmhhcm5lc3MudjIuUmVzb3VyY2VSZWZIAFIHY29udGV4dBI7Cgdwcm9jZXNzGAQgASgL'
    'Mh8uYWN5Y2xpYy5oYXJuZXNzLnYyLlJlc291cmNlUmVmSABSB3Byb2Nlc3MSPQoIYXJ0aWZhY3'
    'QYBSABKAsyHy5hY3ljbGljLmhhcm5lc3MudjIuUmVzb3VyY2VSZWZIAFIIYXJ0aWZhY3QSRAoN'
    'c2hhcmVkX3ZvbHVtZRgGIAEoCzIdLmFjeWNsaWMuaGFybmVzcy52Mi5Wb2x1bWVSZWZIAFIMc2'
    'hhcmVkVm9sdW1lEkUKCWV4dGVuc2lvbhgHIAEoCzIlLmFjeWNsaWMuaGFybmVzcy52Mi5FeHRl'
    'bnNpb25SZXZpc2lvbkgAUglleHRlbnNpb25CBgoEa2luZA==');

@$core.Deprecated('Use capturedResourceDescriptor instead')
const CapturedResource$json = {
  '1': 'CapturedResource',
  '2': [
    {
      '1': 'source',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRevision',
      '10': 'source'
    },
    {
      '1': 'revision',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRevision',
      '10': 'revision'
    },
  ],
};

/// Descriptor for `CapturedResource`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List capturedResourceDescriptor = $convert.base64Decode(
    'ChBDYXB0dXJlZFJlc291cmNlEjwKBnNvdXJjZRgBIAEoCzIkLmFjeWNsaWMuaGFybmVzcy52Mi'
    '5SZXNvdXJjZVJldmlzaW9uUgZzb3VyY2USQAoIcmV2aXNpb24YAiABKAsyJC5hY3ljbGljLmhh'
    'cm5lc3MudjIuUmVzb3VyY2VSZXZpc2lvblIIcmV2aXNpb24=');

@$core.Deprecated('Use forkOmissionDescriptor instead')
const ForkOmission$json = {
  '1': 'ForkOmission',
  '2': [
    {
      '1': 'selection',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRevision',
      '10': 'selection'
    },
    {
      '1': 'unsupported_reason',
      '3': 2,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'unsupportedReason'
    },
    {
      '1': 'in_flight_operation_id',
      '3': 3,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'inFlightOperationId'
    },
    {
      '1': 'indeterminate_operation_id',
      '3': 4,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'indeterminateOperationId'
    },
  ],
  '8': [
    {'1': 'outcome'},
  ],
};

/// Descriptor for `ForkOmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkOmissionDescriptor = $convert.base64Decode(
    'CgxGb3JrT21pc3Npb24SQgoJc2VsZWN0aW9uGAEgASgLMiQuYWN5Y2xpYy5oYXJuZXNzLnYyLl'
    'Jlc291cmNlUmV2aXNpb25SCXNlbGVjdGlvbhIvChJ1bnN1cHBvcnRlZF9yZWFzb24YAiABKAlI'
    'AFIRdW5zdXBwb3J0ZWRSZWFzb24SNQoWaW5fZmxpZ2h0X29wZXJhdGlvbl9pZBgDIAEoCUgAUh'
    'NpbkZsaWdodE9wZXJhdGlvbklkEj4KGmluZGV0ZXJtaW5hdGVfb3BlcmF0aW9uX2lkGAQgASgJ'
    'SABSGGluZGV0ZXJtaW5hdGVPcGVyYXRpb25JZEIJCgdvdXRjb21l');

@$core.Deprecated('Use attestedBoundaryDescriptor instead')
const AttestedBoundary$json = {
  '1': 'AttestedBoundary',
  '2': [
    {
      '1': 'provider',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ProviderRef',
      '10': 'provider'
    },
    {'1': 'evidence', '3': 2, '4': 1, '5': 12, '10': 'evidence'},
  ],
};

/// Descriptor for `AttestedBoundary`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List attestedBoundaryDescriptor = $convert.base64Decode(
    'ChBBdHRlc3RlZEJvdW5kYXJ5EjsKCHByb3ZpZGVyGAEgASgLMh8uYWN5Y2xpYy5oYXJuZXNzLn'
    'YyLlByb3ZpZGVyUmVmUghwcm92aWRlchIaCghldmlkZW5jZRgCIAEoDFIIZXZpZGVuY2U=');

@$core.Deprecated('Use sharedGrantDescriptor instead')
const SharedGrant$json = {
  '1': 'SharedGrant',
  '2': [
    {
      '1': 'volume',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'volume'
    },
    {'1': 'child_agent_id', '3': 2, '4': 1, '5': 9, '10': 'childAgentId'},
    {
      '1': 'operations',
      '3': 3,
      '4': 3,
      '5': 14,
      '6': '.acyclic.harness.v2.SharedVolumeOperation',
      '10': 'operations'
    },
  ],
};

/// Descriptor for `SharedGrant`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List sharedGrantDescriptor = $convert.base64Decode(
    'CgtTaGFyZWRHcmFudBI1CgZ2b2x1bWUYASABKAsyHS5hY3ljbGljLmhhcm5lc3MudjIuVm9sdW'
    '1lUmVmUgZ2b2x1bWUSJAoOY2hpbGRfYWdlbnRfaWQYAiABKAlSDGNoaWxkQWdlbnRJZBJJCgpv'
    'cGVyYXRpb25zGAMgAygOMikuYWN5Y2xpYy5oYXJuZXNzLnYyLlNoYXJlZFZvbHVtZU9wZXJhdG'
    'lvblIKb3BlcmF0aW9ucw==');

@$core.Deprecated('Use referenceGrantDescriptor instead')
const ReferenceGrant$json = {
  '1': 'ReferenceGrant',
  '2': [
    {
      '1': 'file',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'file'
    },
    {'1': 'reader_agent_id', '3': 2, '4': 1, '5': 9, '10': 'readerAgentId'},
    {
      '1': 'attachment_manifest',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'attachmentManifest'
    },
  ],
};

/// Descriptor for `ReferenceGrant`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List referenceGrantDescriptor = $convert.base64Decode(
    'Cg5SZWZlcmVuY2VHcmFudBIvCgRmaWxlGAEgASgLMhsuYWN5Y2xpYy5oYXJuZXNzLnYyLkZpbG'
    'VSZWZSBGZpbGUSJgoPcmVhZGVyX2FnZW50X2lkGAIgASgJUg1yZWFkZXJBZ2VudElkEkwKE2F0'
    'dGFjaG1lbnRfbWFuaWZlc3QYAyABKAsyGy5hY3ljbGljLmhhcm5lc3MudjIuRmlsZVJlZlISYX'
    'R0YWNobWVudE1hbmlmZXN0');

@$core.Deprecated('Use forkSelectionDescriptor instead')
const ForkSelection$json = {
  '1': 'ForkSelection',
  '2': [
    {'1': 'required', '3': 1, '4': 1, '5': 8, '10': 'required'},
    {
      '1': 'revision',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ResourceRevision',
      '10': 'revision'
    },
  ],
};

/// Descriptor for `ForkSelection`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkSelectionDescriptor = $convert.base64Decode(
    'Cg1Gb3JrU2VsZWN0aW9uEhoKCHJlcXVpcmVkGAEgASgIUghyZXF1aXJlZBJACghyZXZpc2lvbh'
    'gCIAEoCzIkLmFjeWNsaWMuaGFybmVzcy52Mi5SZXNvdXJjZVJldmlzaW9uUghyZXZpc2lvbg==');

@$core.Deprecated('Use forkPreparationDescriptor instead')
const ForkPreparation$json = {
  '1': 'ForkPreparation',
  '2': [
    {
      '1': 'child_project_volume',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'childProjectVolume'
    },
    {
      '1': 'child_private_volume',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'childPrivateVolume'
    },
    {
      '1': 'inherited_through_sequence',
      '3': 3,
      '4': 1,
      '5': 4,
      '10': 'inheritedThroughSequence'
    },
    {
      '1': 'maximum_inherited_messages',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'maximumInheritedMessages'
    },
    {
      '1': 'maximum_inherited_bytes',
      '3': 5,
      '4': 1,
      '5': 4,
      '10': 'maximumInheritedBytes'
    },
    {
      '1': 'maximum_inherited_references',
      '3': 6,
      '4': 1,
      '5': 13,
      '10': 'maximumInheritedReferences'
    },
  ],
};

/// Descriptor for `ForkPreparation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkPreparationDescriptor = $convert.base64Decode(
    'Cg9Gb3JrUHJlcGFyYXRpb24STwoUY2hpbGRfcHJvamVjdF92b2x1bWUYASABKAsyHS5hY3ljbG'
    'ljLmhhcm5lc3MudjIuVm9sdW1lUmVmUhJjaGlsZFByb2plY3RWb2x1bWUSTwoUY2hpbGRfcHJp'
    'dmF0ZV92b2x1bWUYAiABKAsyHS5hY3ljbGljLmhhcm5lc3MudjIuVm9sdW1lUmVmUhJjaGlsZF'
    'ByaXZhdGVWb2x1bWUSPAoaaW5oZXJpdGVkX3Rocm91Z2hfc2VxdWVuY2UYAyABKARSGGluaGVy'
    'aXRlZFRocm91Z2hTZXF1ZW5jZRI8ChptYXhpbXVtX2luaGVyaXRlZF9tZXNzYWdlcxgEIAEoBF'
    'IYbWF4aW11bUluaGVyaXRlZE1lc3NhZ2VzEjYKF21heGltdW1faW5oZXJpdGVkX2J5dGVzGAUg'
    'ASgEUhVtYXhpbXVtSW5oZXJpdGVkQnl0ZXMSQAocbWF4aW11bV9pbmhlcml0ZWRfcmVmZXJlbm'
    'NlcxgGIAEoDVIabWF4aW11bUluaGVyaXRlZFJlZmVyZW5jZXM=');

@$core.Deprecated('Use forkRequestDescriptor instead')
const ForkRequest$json = {
  '1': 'ForkRequest',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationIdentity',
      '10': 'operation'
    },
    {
      '1': 'parent',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'parent'
    },
    {'1': 'parent_revision', '3': 3, '4': 1, '5': 4, '10': 'parentRevision'},
    {
      '1': 'child',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'child'
    },
    {'1': 'child_agent_id', '3': 5, '4': 1, '5': 9, '10': 'childAgentId'},
    {
      '1': 'selections',
      '3': 6,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ForkSelection',
      '10': 'selections'
    },
    {
      '1': 'boundary',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.AttestedBoundary',
      '10': 'boundary'
    },
    {
      '1': 'attached_agent_ids',
      '3': 8,
      '4': 3,
      '5': 9,
      '10': 'attachedAgentIds'
    },
    {
      '1': 'preparation',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ForkPreparation',
      '10': 'preparation'
    },
  ],
};

/// Descriptor for `ForkRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkRequestDescriptor = $convert.base64Decode(
    'CgtGb3JrUmVxdWVzdBJDCglvcGVyYXRpb24YASABKAsyJS5hY3ljbGljLmhhcm5lc3MudjIuT3'
    'BlcmF0aW9uSWRlbnRpdHlSCW9wZXJhdGlvbhI1CgZwYXJlbnQYAiABKAsyHS5hY3ljbGljLmhh'
    'cm5lc3MudjIuQXV0aG9yaXR5UgZwYXJlbnQSJwoPcGFyZW50X3JldmlzaW9uGAMgASgEUg5wYX'
    'JlbnRSZXZpc2lvbhIzCgVjaGlsZBgEIAEoCzIdLmFjeWNsaWMuaGFybmVzcy52Mi5BdXRob3Jp'
    'dHlSBWNoaWxkEiQKDmNoaWxkX2FnZW50X2lkGAUgASgJUgxjaGlsZEFnZW50SWQSQQoKc2VsZW'
    'N0aW9ucxgGIAMoCzIhLmFjeWNsaWMuaGFybmVzcy52Mi5Gb3JrU2VsZWN0aW9uUgpzZWxlY3Rp'
    'b25zEkAKCGJvdW5kYXJ5GAcgASgLMiQuYWN5Y2xpYy5oYXJuZXNzLnYyLkF0dGVzdGVkQm91bm'
    'RhcnlSCGJvdW5kYXJ5EiwKEmF0dGFjaGVkX2FnZW50X2lkcxgIIAMoCVIQYXR0YWNoZWRBZ2Vu'
    'dElkcxJFCgtwcmVwYXJhdGlvbhgJIAEoCzIjLmFjeWNsaWMuaGFybmVzcy52Mi5Gb3JrUHJlcG'
    'FyYXRpb25SC3ByZXBhcmF0aW9u');

@$core.Deprecated('Use captureDescriptor instead')
const Capture$json = {
  '1': 'Capture',
  '2': [
    {
      '1': 'captured',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.CapturedResource',
      '9': 0,
      '10': 'captured'
    },
    {
      '1': 'unsupported_reason',
      '3': 2,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'unsupportedReason'
    },
    {
      '1': 'in_flight_operation_id',
      '3': 3,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'inFlightOperationId'
    },
    {
      '1': 'indeterminate_operation_id',
      '3': 4,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'indeterminateOperationId'
    },
  ],
  '8': [
    {'1': 'outcome'},
  ],
};

/// Descriptor for `Capture`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List captureDescriptor = $convert.base64Decode(
    'CgdDYXB0dXJlEkIKCGNhcHR1cmVkGAEgASgLMiQuYWN5Y2xpYy5oYXJuZXNzLnYyLkNhcHR1cm'
    'VkUmVzb3VyY2VIAFIIY2FwdHVyZWQSLwoSdW5zdXBwb3J0ZWRfcmVhc29uGAIgASgJSABSEXVu'
    'c3VwcG9ydGVkUmVhc29uEjUKFmluX2ZsaWdodF9vcGVyYXRpb25faWQYAyABKAlIAFITaW5GbG'
    'lnaHRPcGVyYXRpb25JZBI+ChppbmRldGVybWluYXRlX29wZXJhdGlvbl9pZBgEIAEoCUgAUhhp'
    'bmRldGVybWluYXRlT3BlcmF0aW9uSWRCCQoHb3V0Y29tZQ==');

@$core.Deprecated('Use forkReportDescriptor instead')
const ForkReport$json = {
  '1': 'ForkReport',
  '2': [
    {
      '1': 'request',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ForkRequest',
      '10': 'request'
    },
    {
      '1': 'captures',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.Capture',
      '10': 'captures'
    },
    {
      '1': 'child_private_volume',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'childPrivateVolume'
    },
    {
      '1': 'inherited_context',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'inheritedContext'
    },
    {
      '1': 'shared_grants',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.SharedGrant',
      '10': 'sharedGrants'
    },
    {
      '1': 'reference_grants',
      '3': 6,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ReferenceGrant',
      '10': 'referenceGrants'
    },
    {
      '1': 'attachment_manifests',
      '3': 7,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'attachmentManifests'
    },
    {
      '1': 'inherited_through_sequence',
      '3': 8,
      '4': 1,
      '5': 4,
      '10': 'inheritedThroughSequence'
    },
    {
      '1': 'child_private_generation',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.GenerationRef',
      '10': 'childPrivateGeneration'
    },
  ],
};

/// Descriptor for `ForkReport`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkReportDescriptor = $convert.base64Decode(
    'CgpGb3JrUmVwb3J0EjkKB3JlcXVlc3QYASABKAsyHy5hY3ljbGljLmhhcm5lc3MudjIuRm9ya1'
    'JlcXVlc3RSB3JlcXVlc3QSNwoIY2FwdHVyZXMYAiADKAsyGy5hY3ljbGljLmhhcm5lc3MudjIu'
    'Q2FwdHVyZVIIY2FwdHVyZXMSTwoUY2hpbGRfcHJpdmF0ZV92b2x1bWUYAyABKAsyHS5hY3ljbG'
    'ljLmhhcm5lc3MudjIuVm9sdW1lUmVmUhJjaGlsZFByaXZhdGVWb2x1bWUSSAoRaW5oZXJpdGVk'
    'X2NvbnRleHQYBCADKAsyGy5hY3ljbGljLmhhcm5lc3MudjIuRmlsZVJlZlIQaW5oZXJpdGVkQ2'
    '9udGV4dBJECg1zaGFyZWRfZ3JhbnRzGAUgAygLMh8uYWN5Y2xpYy5oYXJuZXNzLnYyLlNoYXJl'
    'ZEdyYW50UgxzaGFyZWRHcmFudHMSTQoQcmVmZXJlbmNlX2dyYW50cxgGIAMoCzIiLmFjeWNsaW'
    'MuaGFybmVzcy52Mi5SZWZlcmVuY2VHcmFudFIPcmVmZXJlbmNlR3JhbnRzEk4KFGF0dGFjaG1l'
    'bnRfbWFuaWZlc3RzGAcgAygLMhsuYWN5Y2xpYy5oYXJuZXNzLnYyLkZpbGVSZWZSE2F0dGFjaG'
    '1lbnRNYW5pZmVzdHMSPAoaaW5oZXJpdGVkX3Rocm91Z2hfc2VxdWVuY2UYCCABKARSGGluaGVy'
    'aXRlZFRocm91Z2hTZXF1ZW5jZRJbChhjaGlsZF9wcml2YXRlX2dlbmVyYXRpb24YCSABKAsyIS'
    '5hY3ljbGljLmhhcm5lc3MudjIuR2VuZXJhdGlvblJlZlIWY2hpbGRQcml2YXRlR2VuZXJhdGlv'
    'bg==');

@$core.Deprecated('Use forkSeedDescriptor instead')
const ForkSeed$json = {
  '1': 'ForkSeed',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationIdentity',
      '10': 'operation'
    },
    {
      '1': 'parent',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'parent'
    },
    {'1': 'parent_revision', '3': 3, '4': 1, '5': 4, '10': 'parentRevision'},
    {
      '1': 'child',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'child'
    },
    {'1': 'child_agent_id', '3': 5, '4': 1, '5': 9, '10': 'childAgentId'},
    {
      '1': 'resources',
      '3': 6,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.CapturedResource',
      '10': 'resources'
    },
    {
      '1': 'omissions',
      '3': 7,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ForkOmission',
      '10': 'omissions'
    },
    {
      '1': 'child_private_volume',
      '3': 8,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'childPrivateVolume'
    },
    {
      '1': 'inherited_context',
      '3': 9,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'inheritedContext'
    },
    {
      '1': 'boundary',
      '3': 10,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.AttestedBoundary',
      '10': 'boundary'
    },
    {
      '1': 'shared_grants',
      '3': 11,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.SharedGrant',
      '10': 'sharedGrants'
    },
    {
      '1': 'attached_agent_ids',
      '3': 12,
      '4': 3,
      '5': 9,
      '10': 'attachedAgentIds'
    },
    {
      '1': 'reference_grants',
      '3': 13,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.ReferenceGrant',
      '10': 'referenceGrants'
    },
    {
      '1': 'attachment_manifests',
      '3': 14,
      '4': 3,
      '5': 11,
      '6': '.acyclic.harness.v2.FileRef',
      '10': 'attachmentManifests'
    },
    {
      '1': 'inherited_through_sequence',
      '3': 15,
      '4': 1,
      '5': 4,
      '10': 'inheritedThroughSequence'
    },
    {
      '1': 'child_private_generation',
      '3': 16,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.GenerationRef',
      '10': 'childPrivateGeneration'
    },
  ],
};

/// Descriptor for `ForkSeed`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkSeedDescriptor = $convert.base64Decode(
    'CghGb3JrU2VlZBJDCglvcGVyYXRpb24YASABKAsyJS5hY3ljbGljLmhhcm5lc3MudjIuT3Blcm'
    'F0aW9uSWRlbnRpdHlSCW9wZXJhdGlvbhI1CgZwYXJlbnQYAiABKAsyHS5hY3ljbGljLmhhcm5l'
    'c3MudjIuQXV0aG9yaXR5UgZwYXJlbnQSJwoPcGFyZW50X3JldmlzaW9uGAMgASgEUg5wYXJlbn'
    'RSZXZpc2lvbhIzCgVjaGlsZBgEIAEoCzIdLmFjeWNsaWMuaGFybmVzcy52Mi5BdXRob3JpdHlS'
    'BWNoaWxkEiQKDmNoaWxkX2FnZW50X2lkGAUgASgJUgxjaGlsZEFnZW50SWQSQgoJcmVzb3VyY2'
    'VzGAYgAygLMiQuYWN5Y2xpYy5oYXJuZXNzLnYyLkNhcHR1cmVkUmVzb3VyY2VSCXJlc291cmNl'
    'cxI+CglvbWlzc2lvbnMYByADKAsyIC5hY3ljbGljLmhhcm5lc3MudjIuRm9ya09taXNzaW9uUg'
    'lvbWlzc2lvbnMSTwoUY2hpbGRfcHJpdmF0ZV92b2x1bWUYCCABKAsyHS5hY3ljbGljLmhhcm5l'
    'c3MudjIuVm9sdW1lUmVmUhJjaGlsZFByaXZhdGVWb2x1bWUSSAoRaW5oZXJpdGVkX2NvbnRleH'
    'QYCSADKAsyGy5hY3ljbGljLmhhcm5lc3MudjIuRmlsZVJlZlIQaW5oZXJpdGVkQ29udGV4dBJA'
    'Cghib3VuZGFyeRgKIAEoCzIkLmFjeWNsaWMuaGFybmVzcy52Mi5BdHRlc3RlZEJvdW5kYXJ5Ug'
    'hib3VuZGFyeRJECg1zaGFyZWRfZ3JhbnRzGAsgAygLMh8uYWN5Y2xpYy5oYXJuZXNzLnYyLlNo'
    'YXJlZEdyYW50UgxzaGFyZWRHcmFudHMSLAoSYXR0YWNoZWRfYWdlbnRfaWRzGAwgAygJUhBhdH'
    'RhY2hlZEFnZW50SWRzEk0KEHJlZmVyZW5jZV9ncmFudHMYDSADKAsyIi5hY3ljbGljLmhhcm5l'
    'c3MudjIuUmVmZXJlbmNlR3JhbnRSD3JlZmVyZW5jZUdyYW50cxJOChRhdHRhY2htZW50X21hbm'
    'lmZXN0cxgOIAMoCzIbLmFjeWNsaWMuaGFybmVzcy52Mi5GaWxlUmVmUhNhdHRhY2htZW50TWFu'
    'aWZlc3RzEjwKGmluaGVyaXRlZF90aHJvdWdoX3NlcXVlbmNlGA8gASgEUhhpbmhlcml0ZWRUaH'
    'JvdWdoU2VxdWVuY2USWwoYY2hpbGRfcHJpdmF0ZV9nZW5lcmF0aW9uGBAgASgLMiEuYWN5Y2xp'
    'Yy5oYXJuZXNzLnYyLkdlbmVyYXRpb25SZWZSFmNoaWxkUHJpdmF0ZUdlbmVyYXRpb24=');

@$core.Deprecated('Use providerJoinProofDescriptor instead')
const ProviderJoinProof$json = {
  '1': 'ProviderJoinProof',
  '2': [
    {
      '1': 'provider',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ProviderRef',
      '10': 'provider'
    },
    {'1': 'format', '3': 2, '4': 1, '5': 9, '10': 'format'},
    {
      '1': 'canonical_json_statement',
      '3': 3,
      '4': 1,
      '5': 12,
      '10': 'canonicalJsonStatement'
    },
  ],
};

/// Descriptor for `ProviderJoinProof`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List providerJoinProofDescriptor = $convert.base64Decode(
    'ChFQcm92aWRlckpvaW5Qcm9vZhI7Cghwcm92aWRlchgBIAEoCzIfLmFjeWNsaWMuaGFybmVzcy'
    '52Mi5Qcm92aWRlclJlZlIIcHJvdmlkZXISFgoGZm9ybWF0GAIgASgJUgZmb3JtYXQSOAoYY2Fu'
    'b25pY2FsX2pzb25fc3RhdGVtZW50GAMgASgMUhZjYW5vbmljYWxKc29uU3RhdGVtZW50');

@$core.Deprecated('Use projectMergeReceiptDescriptor instead')
const ProjectMergeReceipt$json = {
  '1': 'ProjectMergeReceipt',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.OperationIdentity',
      '10': 'operation'
    },
    {
      '1': 'child',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.Authority',
      '10': 'child'
    },
    {
      '1': 'source_project',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'sourceProject'
    },
    {
      '1': 'source_generation',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.GenerationRef',
      '10': 'sourceGeneration'
    },
    {
      '1': 'target_project',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.VolumeRef',
      '10': 'targetProject'
    },
    {
      '1': 'expected_target_generation',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.GenerationRef',
      '10': 'expectedTargetGeneration'
    },
    {
      '1': 'result_generation',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.GenerationRef',
      '10': 'resultGeneration'
    },
    {
      '1': 'provider_operation_id',
      '3': 8,
      '4': 1,
      '5': 12,
      '10': 'providerOperationId'
    },
    {
      '1': 'provider_proof',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ProviderJoinProof',
      '10': 'providerProof'
    },
    {
      '1': 'notice',
      '3': 10,
      '4': 1,
      '5': 11,
      '6': '.acyclic.harness.v2.ConversationMessage',
      '10': 'notice'
    },
  ],
};

/// Descriptor for `ProjectMergeReceipt`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List projectMergeReceiptDescriptor = $convert.base64Decode(
    'ChNQcm9qZWN0TWVyZ2VSZWNlaXB0EkMKCW9wZXJhdGlvbhgBIAEoCzIlLmFjeWNsaWMuaGFybm'
    'Vzcy52Mi5PcGVyYXRpb25JZGVudGl0eVIJb3BlcmF0aW9uEjMKBWNoaWxkGAIgASgLMh0uYWN5'
    'Y2xpYy5oYXJuZXNzLnYyLkF1dGhvcml0eVIFY2hpbGQSRAoOc291cmNlX3Byb2plY3QYAyABKA'
    'syHS5hY3ljbGljLmhhcm5lc3MudjIuVm9sdW1lUmVmUg1zb3VyY2VQcm9qZWN0Ek4KEXNvdXJj'
    'ZV9nZW5lcmF0aW9uGAQgASgLMiEuYWN5Y2xpYy5oYXJuZXNzLnYyLkdlbmVyYXRpb25SZWZSEH'
    'NvdXJjZUdlbmVyYXRpb24SRAoOdGFyZ2V0X3Byb2plY3QYBSABKAsyHS5hY3ljbGljLmhhcm5l'
    'c3MudjIuVm9sdW1lUmVmUg10YXJnZXRQcm9qZWN0El8KGmV4cGVjdGVkX3RhcmdldF9nZW5lcm'
    'F0aW9uGAYgASgLMiEuYWN5Y2xpYy5oYXJuZXNzLnYyLkdlbmVyYXRpb25SZWZSGGV4cGVjdGVk'
    'VGFyZ2V0R2VuZXJhdGlvbhJOChFyZXN1bHRfZ2VuZXJhdGlvbhgHIAEoCzIhLmFjeWNsaWMuaG'
    'FybmVzcy52Mi5HZW5lcmF0aW9uUmVmUhByZXN1bHRHZW5lcmF0aW9uEjIKFXByb3ZpZGVyX29w'
    'ZXJhdGlvbl9pZBgIIAEoDFITcHJvdmlkZXJPcGVyYXRpb25JZBJMCg5wcm92aWRlcl9wcm9vZh'
    'gJIAEoCzIlLmFjeWNsaWMuaGFybmVzcy52Mi5Qcm92aWRlckpvaW5Qcm9vZlINcHJvdmlkZXJQ'
    'cm9vZhI/CgZub3RpY2UYCiABKAsyJy5hY3ljbGljLmhhcm5lc3MudjIuQ29udmVyc2F0aW9uTW'
    'Vzc2FnZVIGbm90aWNl');
