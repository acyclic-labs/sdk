// This is a generated file - do not edit.
//
// Generated from inference/v1/inference.proto.

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

@$core.Deprecated('Use warmStateDescriptor instead')
const WarmState$json = {
  '1': 'WarmState',
  '2': [
    {'1': 'WARM_STATE_UNSPECIFIED', '2': 0},
    {'1': 'WARM_STATE_ACTIVE', '2': 1},
    {'1': 'WARM_STATE_EXPIRED', '2': 2},
    {'1': 'WARM_STATE_BREACHED', '2': 3},
    {'1': 'WARM_STATE_RELEASED', '2': 4},
  ],
};

/// Descriptor for `WarmState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List warmStateDescriptor = $convert.base64Decode(
    'CglXYXJtU3RhdGUSGgoWV0FSTV9TVEFURV9VTlNQRUNJRklFRBAAEhUKEVdBUk1fU1RBVEVfQU'
    'NUSVZFEAESFgoSV0FSTV9TVEFURV9FWFBJUkVEEAISFwoTV0FSTV9TVEFURV9CUkVBQ0hFRBAD'
    'EhcKE1dBUk1fU1RBVEVfUkVMRUFTRUQQBA==');

@$core.Deprecated('Use evaluationAggregationDescriptor instead')
const EvaluationAggregation$json = {
  '1': 'EvaluationAggregation',
  '2': [
    {'1': 'EVALUATION_AGGREGATION_UNSPECIFIED', '2': 0},
    {'1': 'EVALUATION_AGGREGATION_MEAN', '2': 1},
    {'1': 'EVALUATION_AGGREGATION_SUM', '2': 2},
    {'1': 'EVALUATION_AGGREGATION_MINIMUM', '2': 3},
    {'1': 'EVALUATION_AGGREGATION_MAXIMUM', '2': 4},
  ],
};

/// Descriptor for `EvaluationAggregation`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List evaluationAggregationDescriptor = $convert.base64Decode(
    'ChVFdmFsdWF0aW9uQWdncmVnYXRpb24SJgoiRVZBTFVBVElPTl9BR0dSRUdBVElPTl9VTlNQRU'
    'NJRklFRBAAEh8KG0VWQUxVQVRJT05fQUdHUkVHQVRJT05fTUVBThABEh4KGkVWQUxVQVRJT05f'
    'QUdHUkVHQVRJT05fU1VNEAISIgoeRVZBTFVBVElPTl9BR0dSRUdBVElPTl9NSU5JTVVNEAMSIg'
    'oeRVZBTFVBVElPTl9BR0dSRUdBVElPTl9NQVhJTVVNEAQ=');

@$core.Deprecated('Use evaluationCaseOutcomeDescriptor instead')
const EvaluationCaseOutcome$json = {
  '1': 'EvaluationCaseOutcome',
  '2': [
    {'1': 'EVALUATION_CASE_OUTCOME_UNSPECIFIED', '2': 0},
    {'1': 'EVALUATION_CASE_OUTCOME_SCORED', '2': 1},
    {'1': 'EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED', '2': 2},
    {'1': 'EVALUATION_CASE_OUTCOME_GRADER_FAILED', '2': 3},
  ],
};

/// Descriptor for `EvaluationCaseOutcome`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List evaluationCaseOutcomeDescriptor = $convert.base64Decode(
    'ChVFdmFsdWF0aW9uQ2FzZU91dGNvbWUSJwojRVZBTFVBVElPTl9DQVNFX09VVENPTUVfVU5TUE'
    'VDSUZJRUQQABIiCh5FVkFMVUFUSU9OX0NBU0VfT1VUQ09NRV9TQ09SRUQQARIsCihFVkFMVUFU'
    'SU9OX0NBU0VfT1VUQ09NRV9DQU5ESURBVEVfRkFJTEVEEAISKQolRVZBTFVBVElPTl9DQVNFX0'
    '9VVENPTUVfR1JBREVSX0ZBSUxFRBAD');

@$core.Deprecated('Use evaluationStateDescriptor instead')
const EvaluationState$json = {
  '1': 'EvaluationState',
  '2': [
    {'1': 'EVALUATION_STATE_UNSPECIFIED', '2': 0},
    {'1': 'EVALUATION_STATE_ADMITTED', '2': 1},
    {'1': 'EVALUATION_STATE_RUNNING', '2': 2},
    {'1': 'EVALUATION_STATE_COMPLETED', '2': 3},
    {'1': 'EVALUATION_STATE_FAILED', '2': 4},
    {'1': 'EVALUATION_STATE_CANCELLED', '2': 5},
  ],
};

/// Descriptor for `EvaluationState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List evaluationStateDescriptor = $convert.base64Decode(
    'Cg9FdmFsdWF0aW9uU3RhdGUSIAocRVZBTFVBVElPTl9TVEFURV9VTlNQRUNJRklFRBAAEh0KGU'
    'VWQUxVQVRJT05fU1RBVEVfQURNSVRURUQQARIcChhFVkFMVUFUSU9OX1NUQVRFX1JVTk5JTkcQ'
    'AhIeChpFVkFMVUFUSU9OX1NUQVRFX0NPTVBMRVRFRBADEhsKF0VWQUxVQVRJT05fU1RBVEVfRk'
    'FJTEVEEAQSHgoaRVZBTFVBVElPTl9TVEFURV9DQU5DRUxMRUQQBQ==');

@$core.Deprecated('Use itemKindDescriptor instead')
const ItemKind$json = {
  '1': 'ItemKind',
  '2': [
    {'1': 'ITEM_KIND_UNSPECIFIED', '2': 0},
    {'1': 'ITEM_KIND_INSTRUCTION', '2': 1},
    {'1': 'ITEM_KIND_SYSTEM', '2': 2},
    {'1': 'ITEM_KIND_DEVELOPER', '2': 3},
    {'1': 'ITEM_KIND_USER', '2': 4},
    {'1': 'ITEM_KIND_ASSISTANT', '2': 5},
    {'1': 'ITEM_KIND_TOOL_DEFINITION', '2': 6},
    {'1': 'ITEM_KIND_TOOL_CALL', '2': 7},
    {'1': 'ITEM_KIND_TOOL_RESULT', '2': 8},
    {'1': 'ITEM_KIND_IMAGE', '2': 9},
    {'1': 'ITEM_KIND_AUDIO', '2': 10},
    {'1': 'ITEM_KIND_FILE', '2': 11},
    {'1': 'ITEM_KIND_CONTINUATION', '2': 12},
  ],
};

/// Descriptor for `ItemKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List itemKindDescriptor = $convert.base64Decode(
    'CghJdGVtS2luZBIZChVJVEVNX0tJTkRfVU5TUEVDSUZJRUQQABIZChVJVEVNX0tJTkRfSU5TVF'
    'JVQ1RJT04QARIUChBJVEVNX0tJTkRfU1lTVEVNEAISFwoTSVRFTV9LSU5EX0RFVkVMT1BFUhAD'
    'EhIKDklURU1fS0lORF9VU0VSEAQSFwoTSVRFTV9LSU5EX0FTU0lTVEFOVBAFEh0KGUlURU1fS0'
    'lORF9UT09MX0RFRklOSVRJT04QBhIXChNJVEVNX0tJTkRfVE9PTF9DQUxMEAcSGQoVSVRFTV9L'
    'SU5EX1RPT0xfUkVTVUxUEAgSEwoPSVRFTV9LSU5EX0lNQUdFEAkSEwoPSVRFTV9LSU5EX0FVRE'
    'lPEAoSEgoOSVRFTV9LSU5EX0ZJTEUQCxIaChZJVEVNX0tJTkRfQ09OVElOVUFUSU9OEAw=');

@$core.Deprecated('Use runTerminalDescriptor instead')
const RunTerminal$json = {
  '1': 'RunTerminal',
  '2': [
    {'1': 'RUN_TERMINAL_UNSPECIFIED', '2': 0},
    {'1': 'RUN_TERMINAL_COMPLETED', '2': 1},
    {'1': 'RUN_TERMINAL_OUTPUT_LIMITED', '2': 2},
    {'1': 'RUN_TERMINAL_TOOL_CALL', '2': 3},
    {'1': 'RUN_TERMINAL_REFUSAL', '2': 4},
    {'1': 'RUN_TERMINAL_CANCELLED', '2': 5, '3': {}},
    {'1': 'RUN_TERMINAL_FAILED', '2': 6, '3': {}},
    {'1': 'RUN_TERMINAL_INDETERMINATE', '2': 7, '3': {}},
  ],
};

/// Descriptor for `RunTerminal`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List runTerminalDescriptor = $convert.base64Decode(
    'CgtSdW5UZXJtaW5hbBIcChhSVU5fVEVSTUlOQUxfVU5TUEVDSUZJRUQQABIaChZSVU5fVEVSTU'
    'lOQUxfQ09NUExFVEVEEAESHwobUlVOX1RFUk1JTkFMX09VVFBVVF9MSU1JVEVEEAISGgoWUlVO'
    'X1RFUk1JTkFMX1RPT0xfQ0FMTBADEhgKFFJVTl9URVJNSU5BTF9SRUZVU0FMEAQSIAoWUlVOX1'
    'RFUk1JTkFMX0NBTkNFTExFRBAFGgSY9BgBEh0KE1JVTl9URVJNSU5BTF9GQUlMRUQQBhoEmPQY'
    'ARIkChpSVU5fVEVSTUlOQUxfSU5ERVRFUk1JTkFURRAHGgSY9BgB');

@$core.Deprecated('Use listModelsRequestDescriptor instead')
const ListModelsRequest$json = {
  '1': 'ListModelsRequest',
};

/// Descriptor for `ListModelsRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listModelsRequestDescriptor =
    $convert.base64Decode('ChFMaXN0TW9kZWxzUmVxdWVzdA==');

@$core.Deprecated('Use listModelsResponseDescriptor instead')
const ListModelsResponse$json = {
  '1': 'ListModelsResponse',
  '2': [
    {
      '1': 'models',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.ModelCapability',
      '8': {},
      '10': 'models'
    },
  ],
};

/// Descriptor for `ListModelsResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listModelsResponseDescriptor = $convert.base64Decode(
    'ChJMaXN0TW9kZWxzUmVzcG9uc2USSQoGbW9kZWxzGAEgAygLMiYuaW5mZXJlbmNlLmN1c3RvbW'
    'VyLnYxLk1vZGVsQ2FwYWJpbGl0eUIJ6PMYAfDzGIAgUgZtb2RlbHM=');

@$core.Deprecated('Use modelCapabilityDescriptor instead')
const ModelCapability$json = {
  '1': 'ModelCapability',
  '2': [
    {'1': 'model', '3': 1, '4': 1, '5': 9, '8': {}, '10': 'model'},
    {
      '1': 'execution_profile',
      '3': 2,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'executionProfile'
    },
    {
      '1': 'maximum_context',
      '3': 3,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'maximumContext'
    },
    {
      '1': 'maximum_output',
      '3': 4,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'maximumOutput'
    },
    {'1': 'features', '3': 5, '4': 3, '5': 9, '8': {}, '10': 'features'},
    {
      '1': 'retention_profiles',
      '3': 6,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.RetentionProfile',
      '8': {},
      '10': 'retentionProfiles'
    },
    {
      '1': 'idle_kv_profiles',
      '3': 7,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.RetentionProfile',
      '8': {},
      '10': 'idleKvProfiles'
    },
  ],
};

/// Descriptor for `ModelCapability`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List modelCapabilityDescriptor = $convert.base64Decode(
    'Cg9Nb2RlbENhcGFiaWxpdHkSGwoFbW9kZWwYASABKAlCBfjzGIACUgVtb2RlbBIxChFleGVjdX'
    'Rpb25fcHJvZmlsZRgCIAEoDEIEyPMYIFIQZXhlY3V0aW9uUHJvZmlsZRItCg9tYXhpbXVtX2Nv'
    'bnRleHQYAyABKARCBNjzGAFSDm1heGltdW1Db250ZXh0EisKDm1heGltdW1fb3V0cHV0GAQgAS'
    'gEQgTY8xgBUg1tYXhpbXVtT3V0cHV0EigKCGZlYXR1cmVzGAUgAygJQgzo8xgB8PMYQJD0GEBS'
    'CGZlYXR1cmVzElwKEnJldGVudGlvbl9wcm9maWxlcxgGIAMoCzInLmluZmVyZW5jZS5jdXN0b2'
    '1lci52MS5SZXRlbnRpb25Qcm9maWxlQgTw8xhAUhFyZXRlbnRpb25Qcm9maWxlcxJXChBpZGxl'
    'X2t2X3Byb2ZpbGVzGAcgAygLMicuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLlJldGVudGlvblByb2'
    'ZpbGVCBPDzGEBSDmlkbGVLdlByb2ZpbGVz');

@$core.Deprecated('Use retentionProfileDescriptor instead')
const RetentionProfile$json = {
  '1': 'RetentionProfile',
  '2': [
    {'1': 'profile', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'profile'},
    {
      '1': 'minimum_duration_ms',
      '3': 2,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'minimumDurationMs'
    },
    {
      '1': 'maximum_duration_ms',
      '3': 3,
      '4': 1,
      '5': 4,
      '10': 'maximumDurationMs'
    },
  ],
};

/// Descriptor for `RetentionProfile`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List retentionProfileDescriptor = $convert.base64Decode(
    'ChBSZXRlbnRpb25Qcm9maWxlEh4KB3Byb2ZpbGUYASABKAxCBMjzGCBSB3Byb2ZpbGUSNAoTbW'
    'luaW11bV9kdXJhdGlvbl9tcxgCIAEoBEIE2PMYAVIRbWluaW11bUR1cmF0aW9uTXMSLgoTbWF4'
    'aW11bV9kdXJhdGlvbl9tcxgDIAEoBFIRbWF4aW11bUR1cmF0aW9uTXM=');

@$core.Deprecated('Use retainWarmRequestDescriptor instead')
const RetainWarmRequest$json = {
  '1': 'RetainWarmRequest',
  '2': [
    {
      '1': 'identity',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RequestIdentity',
      '8': {},
      '10': 'identity'
    },
    {'1': 'context', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'context'},
    {'1': 'latency_profile', '3': 3, '4': 1, '5': 12, '10': 'latencyProfile'},
    {'1': 'expires_at_ms', '3': 4, '4': 1, '5': 4, '10': 'expiresAtMs'},
    {
      '1': 'idle_kv',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.IdleKvPolicy',
      '10': 'idleKv'
    },
  ],
};

/// Descriptor for `RetainWarmRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List retainWarmRequestDescriptor = $convert.base64Decode(
    'ChFSZXRhaW5XYXJtUmVxdWVzdBJICghpZGVudGl0eRgBIAEoCzImLmluZmVyZW5jZS5jdXN0b2'
    '1lci52MS5SZXF1ZXN0SWRlbnRpdHlCBNDzGAFSCGlkZW50aXR5Eh4KB2NvbnRleHQYAiABKAxC'
    'BMjzGCBSB2NvbnRleHQSJwoPbGF0ZW5jeV9wcm9maWxlGAMgASgMUg5sYXRlbmN5UHJvZmlsZR'
    'IiCg1leHBpcmVzX2F0X21zGAQgASgEUgtleHBpcmVzQXRNcxI8CgdpZGxlX2t2GAUgASgLMiMu'
    'aW5mZXJlbmNlLmN1c3RvbWVyLnYxLklkbGVLdlBvbGljeVIGaWRsZUt2');

@$core.Deprecated('Use idleKvPolicyDescriptor instead')
const IdleKvPolicy$json = {
  '1': 'IdleKvPolicy',
  '2': [
    {'1': 'profile', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'profile'},
    {
      '1': 'idle_timeout_ms',
      '3': 2,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'idleTimeoutMs'
    },
  ],
};

/// Descriptor for `IdleKvPolicy`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List idleKvPolicyDescriptor = $convert.base64Decode(
    'CgxJZGxlS3ZQb2xpY3kSHgoHcHJvZmlsZRgBIAEoDEIEyPMYIFIHcHJvZmlsZRIsCg9pZGxlX3'
    'RpbWVvdXRfbXMYAiABKARCBNjzGAFSDWlkbGVUaW1lb3V0TXM=');

@$core.Deprecated('Use idleKvRetentionDescriptor instead')
const IdleKvRetention$json = {
  '1': 'IdleKvRetention',
  '2': [
    {
      '1': 'policy',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.IdleKvPolicy',
      '8': {},
      '10': 'policy'
    },
    {
      '1': 'retained_at_ms',
      '3': 2,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'retainedAtMs'
    },
    {
      '1': 'last_used_at_ms',
      '3': 3,
      '4': 1,
      '5': 4,
      '8': {},
      '9': 0,
      '10': 'lastUsedAtMs',
      '17': true
    },
    {
      '1': 'last_run_id',
      '3': 4,
      '4': 1,
      '5': 12,
      '8': {},
      '9': 1,
      '10': 'lastRunId',
      '17': true
    },
  ],
  '8': [
    {'1': '_last_used_at_ms'},
    {'1': '_last_run_id'},
  ],
};

/// Descriptor for `IdleKvRetention`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List idleKvRetentionDescriptor = $convert.base64Decode(
    'Cg9JZGxlS3ZSZXRlbnRpb24SQQoGcG9saWN5GAEgASgLMiMuaW5mZXJlbmNlLmN1c3RvbWVyLn'
    'YxLklkbGVLdlBvbGljeUIE0PMYAVIGcG9saWN5EioKDnJldGFpbmVkX2F0X21zGAIgASgEQgTY'
    '8xgBUgxyZXRhaW5lZEF0TXMSMAoPbGFzdF91c2VkX2F0X21zGAMgASgEQgTY8xgBSABSDGxhc3'
    'RVc2VkQXRNc4gBARIpCgtsYXN0X3J1bl9pZBgEIAEoDEIEyPMYEEgBUglsYXN0UnVuSWSIAQFC'
    'EgoQX2xhc3RfdXNlZF9hdF9tc0IOCgxfbGFzdF9ydW5faWQ=');

@$core.Deprecated('Use inspectWarmRequestDescriptor instead')
const InspectWarmRequest$json = {
  '1': 'InspectWarmRequest',
  '2': [
    {'1': 'commitment', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'commitment'},
  ],
};

/// Descriptor for `InspectWarmRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectWarmRequestDescriptor = $convert.base64Decode(
    'ChJJbnNwZWN0V2FybVJlcXVlc3QSJAoKY29tbWl0bWVudBgBIAEoDEIEyPMYIFIKY29tbWl0bW'
    'VudA==');

@$core.Deprecated('Use renewWarmRequestDescriptor instead')
const RenewWarmRequest$json = {
  '1': 'RenewWarmRequest',
  '2': [
    {
      '1': 'identity',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RequestIdentity',
      '8': {},
      '10': 'identity'
    },
    {'1': 'commitment', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'commitment'},
    {'1': 'expires_at_ms', '3': 3, '4': 1, '5': 4, '10': 'expiresAtMs'},
    {
      '1': 'idle_timeout_ms',
      '3': 4,
      '4': 1,
      '5': 4,
      '8': {},
      '9': 0,
      '10': 'idleTimeoutMs',
      '17': true
    },
  ],
  '8': [
    {'1': '_idle_timeout_ms'},
  ],
};

/// Descriptor for `RenewWarmRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List renewWarmRequestDescriptor = $convert.base64Decode(
    'ChBSZW5ld1dhcm1SZXF1ZXN0EkgKCGlkZW50aXR5GAEgASgLMiYuaW5mZXJlbmNlLmN1c3RvbW'
    'VyLnYxLlJlcXVlc3RJZGVudGl0eUIE0PMYAVIIaWRlbnRpdHkSJAoKY29tbWl0bWVudBgCIAEo'
    'DEIEyPMYIFIKY29tbWl0bWVudBIiCg1leHBpcmVzX2F0X21zGAMgASgEUgtleHBpcmVzQXRNcx'
    'IxCg9pZGxlX3RpbWVvdXRfbXMYBCABKARCBNjzGAFIAFINaWRsZVRpbWVvdXRNc4gBAUISChBf'
    'aWRsZV90aW1lb3V0X21z');

@$core.Deprecated('Use releaseWarmRequestDescriptor instead')
const ReleaseWarmRequest$json = {
  '1': 'ReleaseWarmRequest',
  '2': [
    {
      '1': 'identity',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RequestIdentity',
      '8': {},
      '10': 'identity'
    },
    {'1': 'commitment', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'commitment'},
  ],
};

/// Descriptor for `ReleaseWarmRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List releaseWarmRequestDescriptor = $convert.base64Decode(
    'ChJSZWxlYXNlV2FybVJlcXVlc3QSSAoIaWRlbnRpdHkYASABKAsyJi5pbmZlcmVuY2UuY3VzdG'
    '9tZXIudjEuUmVxdWVzdElkZW50aXR5QgTQ8xgBUghpZGVudGl0eRIkCgpjb21taXRtZW50GAIg'
    'ASgMQgTI8xggUgpjb21taXRtZW50');

@$core.Deprecated('Use warmViewDescriptor instead')
const WarmView$json = {
  '1': 'WarmView',
  '2': [
    {'1': 'commitment', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'commitment'},
    {'1': 'context', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'context'},
    {
      '1': 'model_profile',
      '3': 3,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'modelProfile'
    },
    {'1': 'latency_profile', '3': 4, '4': 1, '5': 12, '10': 'latencyProfile'},
    {
      '1': 'expires_at_ms',
      '3': 5,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'expiresAtMs'
    },
    {
      '1': 'state',
      '3': 6,
      '4': 1,
      '5': 14,
      '6': '.inference.customer.v1.WarmState',
      '8': {},
      '10': 'state'
    },
    {
      '1': 'evidence_digest',
      '3': 7,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'evidenceDigest'
    },
    {
      '1': 'admission_receipt_id',
      '3': 8,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'admissionReceiptId'
    },
    {'1': 'sequence', '3': 9, '4': 1, '5': 4, '8': {}, '10': 'sequence'},
    {
      '1': 'idle_kv',
      '3': 10,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.IdleKvRetention',
      '10': 'idleKv'
    },
  ],
};

/// Descriptor for `WarmView`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List warmViewDescriptor = $convert.base64Decode(
    'CghXYXJtVmlldxIkCgpjb21taXRtZW50GAEgASgMQgTI8xggUgpjb21taXRtZW50Eh4KB2Nvbn'
    'RleHQYAiABKAxCBMjzGCBSB2NvbnRleHQSKQoNbW9kZWxfcHJvZmlsZRgDIAEoDEIEyPMYIFIM'
    'bW9kZWxQcm9maWxlEicKD2xhdGVuY3lfcHJvZmlsZRgEIAEoDFIObGF0ZW5jeVByb2ZpbGUSKA'
    'oNZXhwaXJlc19hdF9tcxgFIAEoBEIE2PMYAVILZXhwaXJlc0F0TXMSPAoFc3RhdGUYBiABKA4y'
    'IC5pbmZlcmVuY2UuY3VzdG9tZXIudjEuV2FybVN0YXRlQgSI9BgBUgVzdGF0ZRItCg9ldmlkZW'
    '5jZV9kaWdlc3QYByABKAxCBMjzGCBSDmV2aWRlbmNlRGlnZXN0EjYKFGFkbWlzc2lvbl9yZWNl'
    'aXB0X2lkGAggASgMQgTI8xggUhJhZG1pc3Npb25SZWNlaXB0SWQSIAoIc2VxdWVuY2UYCSABKA'
    'RCBNjzGAFSCHNlcXVlbmNlEj8KB2lkbGVfa3YYCiABKAsyJi5pbmZlcmVuY2UuY3VzdG9tZXIu'
    'djEuSWRsZUt2UmV0ZW50aW9uUgZpZGxlS3Y=');

@$core.Deprecated('Use evaluationArtifactDescriptor instead')
const EvaluationArtifact$json = {
  '1': 'EvaluationArtifact',
  '2': [
    {'1': 'digest', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'digest'},
    {'1': 'media_type', '3': 2, '4': 1, '5': 9, '8': {}, '10': 'mediaType'},
    {'1': 'logical_size', '3': 3, '4': 1, '5': 4, '8': {}, '10': 'logicalSize'},
  ],
};

/// Descriptor for `EvaluationArtifact`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationArtifactDescriptor = $convert.base64Decode(
    'ChJFdmFsdWF0aW9uQXJ0aWZhY3QSHAoGZGlnZXN0GAEgASgMQgTI8xggUgZkaWdlc3QSJAoKbW'
    'VkaWFfdHlwZRgCIAEoCUIF+PMYgAJSCW1lZGlhVHlwZRInCgxsb2dpY2FsX3NpemUYAyABKARC'
    'BNjzGAFSC2xvZ2ljYWxTaXpl');

@$core.Deprecated('Use evaluationCaseDescriptor instead')
const EvaluationCase$json = {
  '1': 'EvaluationCase',
  '2': [
    {'1': 'case_id', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'caseId'},
    {'1': 'input', '3': 2, '4': 1, '5': 12, '10': 'input'},
    {
      '1': 'input_artifact_digest',
      '3': 3,
      '4': 1,
      '5': 12,
      '8': {},
      '9': 0,
      '10': 'inputArtifactDigest',
      '17': true
    },
  ],
  '8': [
    {'1': '_input_artifact_digest'},
  ],
};

/// Descriptor for `EvaluationCase`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationCaseDescriptor = $convert.base64Decode(
    'Cg5FdmFsdWF0aW9uQ2FzZRIdCgdjYXNlX2lkGAEgASgMQgTI8xgQUgZjYXNlSWQSFAoFaW5wdX'
    'QYAiABKAxSBWlucHV0Ej0KFWlucHV0X2FydGlmYWN0X2RpZ2VzdBgDIAEoDEIEyPMYIEgAUhNp'
    'bnB1dEFydGlmYWN0RGlnZXN0iAEBQhgKFl9pbnB1dF9hcnRpZmFjdF9kaWdlc3Q=');

@$core.Deprecated('Use evaluationSuiteDescriptor instead')
const EvaluationSuite$json = {
  '1': 'EvaluationSuite',
  '2': [
    {'1': 'identity', '3': 1, '4': 1, '5': 9, '8': {}, '10': 'identity'},
    {'1': 'digest', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'digest'},
    {
      '1': 'cases',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationCase',
      '8': {},
      '10': 'cases'
    },
  ],
};

/// Descriptor for `EvaluationSuite`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationSuiteDescriptor = $convert.base64Decode(
    'Cg9FdmFsdWF0aW9uU3VpdGUSIQoIaWRlbnRpdHkYASABKAlCBfjzGIACUghpZGVudGl0eRIcCg'
    'ZkaWdlc3QYAiABKAxCBMjzGCBSBmRpZ2VzdBJGCgVjYXNlcxgDIAMoCzIlLmluZmVyZW5jZS5j'
    'dXN0b21lci52MS5FdmFsdWF0aW9uQ2FzZUIJ6PMYAfDzGIAgUgVjYXNlcw==');

@$core.Deprecated('Use evaluationGraderDescriptor instead')
const EvaluationGrader$json = {
  '1': 'EvaluationGrader',
  '2': [
    {'1': 'handle', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'handle'},
    {
      '1': 'artifact_digest',
      '3': 2,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'artifactDigest'
    },
  ],
};

/// Descriptor for `EvaluationGrader`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationGraderDescriptor = $convert.base64Decode(
    'ChBFdmFsdWF0aW9uR3JhZGVyEh0KBmhhbmRsZRgBIAEoDEIF+PMYgCBSBmhhbmRsZRItCg9hcn'
    'RpZmFjdF9kaWdlc3QYAiABKAxCBMjzGCBSDmFydGlmYWN0RGlnZXN0');

@$core.Deprecated('Use evaluationMetricDescriptor instead')
const EvaluationMetric$json = {
  '1': 'EvaluationMetric',
  '2': [
    {'1': 'identity', '3': 1, '4': 1, '5': 9, '8': {}, '10': 'identity'},
    {
      '1': 'aggregation',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.inference.customer.v1.EvaluationAggregation',
      '8': {},
      '10': 'aggregation'
    },
  ],
};

/// Descriptor for `EvaluationMetric`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationMetricDescriptor = $convert.base64Decode(
    'ChBFdmFsdWF0aW9uTWV0cmljEiEKCGlkZW50aXR5GAEgASgJQgX48xiAAlIIaWRlbnRpdHkSVA'
    'oLYWdncmVnYXRpb24YAiABKA4yLC5pbmZlcmVuY2UuY3VzdG9tZXIudjEuRXZhbHVhdGlvbkFn'
    'Z3JlZ2F0aW9uQgSI9BgBUgthZ2dyZWdhdGlvbg==');

@$core.Deprecated('Use evaluationSpecDescriptor instead')
const EvaluationSpec$json = {
  '1': 'EvaluationSpec',
  '2': [
    {
      '1': 'candidates',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationArtifact',
      '8': {},
      '10': 'candidates'
    },
    {
      '1': 'suite',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationSuite',
      '8': {},
      '10': 'suite'
    },
    {
      '1': 'grader',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationGrader',
      '8': {},
      '10': 'grader'
    },
    {
      '1': 'metrics',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationMetric',
      '8': {},
      '10': 'metrics'
    },
    {
      '1': 'maximum_case_results',
      '3': 5,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'maximumCaseResults'
    },
    {'1': 'spec_digest', '3': 6, '4': 1, '5': 12, '8': {}, '10': 'specDigest'},
  ],
};

/// Descriptor for `EvaluationSpec`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationSpecDescriptor = $convert.base64Decode(
    'Cg5FdmFsdWF0aW9uU3BlYxJUCgpjYW5kaWRhdGVzGAEgAygLMikuaW5mZXJlbmNlLmN1c3RvbW'
    'VyLnYxLkV2YWx1YXRpb25BcnRpZmFjdEIJ6PMYAfDzGIACUgpjYW5kaWRhdGVzEkIKBXN1aXRl'
    'GAIgASgLMiYuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLkV2YWx1YXRpb25TdWl0ZUIE0PMYAVIFc3'
    'VpdGUSRQoGZ3JhZGVyGAMgASgLMicuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLkV2YWx1YXRpb25H'
    'cmFkZXJCBNDzGAFSBmdyYWRlchJLCgdtZXRyaWNzGAQgAygLMicuaW5mZXJlbmNlLmN1c3RvbW'
    'VyLnYxLkV2YWx1YXRpb25NZXRyaWNCCOjzGAHw8xhAUgdtZXRyaWNzEjwKFG1heGltdW1fY2Fz'
    'ZV9yZXN1bHRzGAUgASgEQgrY8xgBgPQYgIAEUhJtYXhpbXVtQ2FzZVJlc3VsdHMSJQoLc3BlY1'
    '9kaWdlc3QYBiABKAxCBMjzGCBSCnNwZWNEaWdlc3Q=');

@$core.Deprecated('Use createEvaluationRequestDescriptor instead')
const CreateEvaluationRequest$json = {
  '1': 'CreateEvaluationRequest',
  '2': [
    {
      '1': 'identity',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RequestIdentity',
      '8': {},
      '10': 'identity'
    },
    {
      '1': 'spec',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationSpec',
      '8': {},
      '10': 'spec'
    },
  ],
};

/// Descriptor for `CreateEvaluationRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createEvaluationRequestDescriptor = $convert.base64Decode(
    'ChdDcmVhdGVFdmFsdWF0aW9uUmVxdWVzdBJICghpZGVudGl0eRgBIAEoCzImLmluZmVyZW5jZS'
    '5jdXN0b21lci52MS5SZXF1ZXN0SWRlbnRpdHlCBNDzGAFSCGlkZW50aXR5Ej8KBHNwZWMYAiAB'
    'KAsyJS5pbmZlcmVuY2UuY3VzdG9tZXIudjEuRXZhbHVhdGlvblNwZWNCBNDzGAFSBHNwZWM=');

@$core.Deprecated('Use inspectEvaluationRequestDescriptor instead')
const InspectEvaluationRequest$json = {
  '1': 'InspectEvaluationRequest',
  '2': [
    {
      '1': 'evaluation_id',
      '3': 1,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'evaluationId'
    },
  ],
};

/// Descriptor for `InspectEvaluationRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectEvaluationRequestDescriptor =
    $convert.base64Decode(
        'ChhJbnNwZWN0RXZhbHVhdGlvblJlcXVlc3QSKQoNZXZhbHVhdGlvbl9pZBgBIAEoDEIEyPMYEF'
        'IMZXZhbHVhdGlvbklk');

@$core.Deprecated('Use exactRationalDescriptor instead')
const ExactRational$json = {
  '1': 'ExactRational',
  '2': [
    {'1': 'numerator', '3': 1, '4': 1, '5': 18, '10': 'numerator'},
    {'1': 'denominator', '3': 2, '4': 1, '5': 4, '8': {}, '10': 'denominator'},
  ],
};

/// Descriptor for `ExactRational`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List exactRationalDescriptor = $convert.base64Decode(
    'Cg1FeGFjdFJhdGlvbmFsEhwKCW51bWVyYXRvchgBIAEoElIJbnVtZXJhdG9yEiYKC2Rlbm9taW'
    '5hdG9yGAIgASgEQgTY8xgBUgtkZW5vbWluYXRvcg==');

@$core.Deprecated('Use evaluationMetricValueDescriptor instead')
const EvaluationMetricValue$json = {
  '1': 'EvaluationMetricValue',
  '2': [
    {'1': 'metric_identity', '3': 1, '4': 1, '5': 9, '10': 'metricIdentity'},
    {
      '1': 'value',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.ExactRational',
      '8': {},
      '10': 'value'
    },
  ],
};

/// Descriptor for `EvaluationMetricValue`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationMetricValueDescriptor = $convert.base64Decode(
    'ChVFdmFsdWF0aW9uTWV0cmljVmFsdWUSJwoPbWV0cmljX2lkZW50aXR5GAEgASgJUg5tZXRyaW'
    'NJZGVudGl0eRJACgV2YWx1ZRgCIAEoCzIkLmluZmVyZW5jZS5jdXN0b21lci52MS5FeGFjdFJh'
    'dGlvbmFsQgTQ8xgBUgV2YWx1ZQ==');

@$core.Deprecated('Use evaluationCaseResultDescriptor instead')
const EvaluationCaseResult$json = {
  '1': 'EvaluationCaseResult',
  '2': [
    {
      '1': 'candidate_digest',
      '3': 1,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'candidateDigest'
    },
    {'1': 'case_id', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'caseId'},
    {
      '1': 'observation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationGraderObservation',
      '8': {},
      '10': 'observation'
    },
    {
      '1': 'metrics',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationMetricValue',
      '10': 'metrics'
    },
    {
      '1': 'outcome',
      '3': 5,
      '4': 1,
      '5': 14,
      '6': '.inference.customer.v1.EvaluationCaseOutcome',
      '8': {},
      '10': 'outcome'
    },
  ],
};

/// Descriptor for `EvaluationCaseResult`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationCaseResultDescriptor = $convert.base64Decode(
    'ChRFdmFsdWF0aW9uQ2FzZVJlc3VsdBIvChBjYW5kaWRhdGVfZGlnZXN0GAEgASgMQgTI8xggUg'
    '9jYW5kaWRhdGVEaWdlc3QSHQoHY2FzZV9pZBgCIAEoDEIEyPMYEFIGY2FzZUlkEloKC29ic2Vy'
    'dmF0aW9uGAMgASgLMjIuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLkV2YWx1YXRpb25HcmFkZXJPYn'
    'NlcnZhdGlvbkIE0PMYAVILb2JzZXJ2YXRpb24SRgoHbWV0cmljcxgEIAMoCzIsLmluZmVyZW5j'
    'ZS5jdXN0b21lci52MS5FdmFsdWF0aW9uTWV0cmljVmFsdWVSB21ldHJpY3MSTAoHb3V0Y29tZR'
    'gFIAEoDjIsLmluZmVyZW5jZS5jdXN0b21lci52MS5FdmFsdWF0aW9uQ2FzZU91dGNvbWVCBIj0'
    'GAFSB291dGNvbWU=');

@$core.Deprecated('Use evaluationGraderObservationDescriptor instead')
const EvaluationGraderObservation$json = {
  '1': 'EvaluationGraderObservation',
  '2': [
    {
      '1': 'native_output_digest',
      '3': 1,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'nativeOutputDigest'
    },
    {
      '1': 'observation_digest',
      '3': 2,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'observationDigest'
    },
    {
      '1': 'binding_digest',
      '3': 3,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'bindingDigest'
    },
  ],
};

/// Descriptor for `EvaluationGraderObservation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationGraderObservationDescriptor = $convert.base64Decode(
    'ChtFdmFsdWF0aW9uR3JhZGVyT2JzZXJ2YXRpb24SNgoUbmF0aXZlX291dHB1dF9kaWdlc3QYAS'
    'ABKAxCBMjzGCBSEm5hdGl2ZU91dHB1dERpZ2VzdBIzChJvYnNlcnZhdGlvbl9kaWdlc3QYAiAB'
    'KAxCBMjzGCBSEW9ic2VydmF0aW9uRGlnZXN0EisKDmJpbmRpbmdfZGlnZXN0GAMgASgMQgTI8x'
    'ggUg1iaW5kaW5nRGlnZXN0');

@$core.Deprecated('Use evaluationAggregateDescriptor instead')
const EvaluationAggregate$json = {
  '1': 'EvaluationAggregate',
  '2': [
    {
      '1': 'candidate_digest',
      '3': 1,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'candidateDigest'
    },
    {'1': 'metric_identity', '3': 2, '4': 1, '5': 9, '10': 'metricIdentity'},
    {
      '1': 'aggregation',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.inference.customer.v1.EvaluationAggregation',
      '8': {},
      '10': 'aggregation'
    },
    {
      '1': 'value',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.ExactRational',
      '8': {},
      '10': 'value'
    },
  ],
};

/// Descriptor for `EvaluationAggregate`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationAggregateDescriptor = $convert.base64Decode(
    'ChNFdmFsdWF0aW9uQWdncmVnYXRlEi8KEGNhbmRpZGF0ZV9kaWdlc3QYASABKAxCBMjzGCBSD2'
    'NhbmRpZGF0ZURpZ2VzdBInCg9tZXRyaWNfaWRlbnRpdHkYAiABKAlSDm1ldHJpY0lkZW50aXR5'
    'ElQKC2FnZ3JlZ2F0aW9uGAMgASgOMiwuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLkV2YWx1YXRpb2'
    '5BZ2dyZWdhdGlvbkIEiPQYAVILYWdncmVnYXRpb24SQAoFdmFsdWUYBCABKAsyJC5pbmZlcmVu'
    'Y2UuY3VzdG9tZXIudjEuRXhhY3RSYXRpb25hbEIE0PMYAVIFdmFsdWU=');

@$core.Deprecated('Use evaluationResultDescriptor instead')
const EvaluationResult$json = {
  '1': 'EvaluationResult',
  '2': [
    {'1': 'spec_digest', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'specDigest'},
    {
      '1': 'case_results',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationCaseResult',
      '10': 'caseResults'
    },
    {
      '1': 'aggregates',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationAggregate',
      '10': 'aggregates'
    },
    {
      '1': 'result_digest',
      '3': 4,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'resultDigest'
    },
  ],
};

/// Descriptor for `EvaluationResult`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationResultDescriptor = $convert.base64Decode(
    'ChBFdmFsdWF0aW9uUmVzdWx0EiUKC3NwZWNfZGlnZXN0GAEgASgMQgTI8xggUgpzcGVjRGlnZX'
    'N0Ek4KDGNhc2VfcmVzdWx0cxgCIAMoCzIrLmluZmVyZW5jZS5jdXN0b21lci52MS5FdmFsdWF0'
    'aW9uQ2FzZVJlc3VsdFILY2FzZVJlc3VsdHMSSgoKYWdncmVnYXRlcxgDIAMoCzIqLmluZmVyZW'
    '5jZS5jdXN0b21lci52MS5FdmFsdWF0aW9uQWdncmVnYXRlUgphZ2dyZWdhdGVzEikKDXJlc3Vs'
    'dF9kaWdlc3QYBCABKAxCBMjzGCBSDHJlc3VsdERpZ2VzdA==');

@$core.Deprecated('Use evaluationViewDescriptor instead')
const EvaluationView$json = {
  '1': 'EvaluationView',
  '2': [
    {
      '1': 'evaluation_id',
      '3': 1,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'evaluationId'
    },
    {
      '1': 'spec',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationSpec',
      '8': {},
      '10': 'spec'
    },
    {
      '1': 'state',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.inference.customer.v1.EvaluationState',
      '8': {},
      '10': 'state'
    },
    {
      '1': 'result',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.EvaluationResult',
      '9': 0,
      '10': 'result',
      '17': true
    },
    {'1': 'sequence', '3': 5, '4': 1, '5': 4, '8': {}, '10': 'sequence'},
  ],
  '8': [
    {'1': '_result'},
  ],
};

/// Descriptor for `EvaluationView`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List evaluationViewDescriptor = $convert.base64Decode(
    'Cg5FdmFsdWF0aW9uVmlldxIpCg1ldmFsdWF0aW9uX2lkGAEgASgMQgTI8xgQUgxldmFsdWF0aW'
    '9uSWQSPwoEc3BlYxgCIAEoCzIlLmluZmVyZW5jZS5jdXN0b21lci52MS5FdmFsdWF0aW9uU3Bl'
    'Y0IE0PMYAVIEc3BlYxJCCgVzdGF0ZRgDIAEoDjImLmluZmVyZW5jZS5jdXN0b21lci52MS5Fdm'
    'FsdWF0aW9uU3RhdGVCBIj0GAFSBXN0YXRlEkQKBnJlc3VsdBgEIAEoCzInLmluZmVyZW5jZS5j'
    'dXN0b21lci52MS5FdmFsdWF0aW9uUmVzdWx0SABSBnJlc3VsdIgBARIgCghzZXF1ZW5jZRgFIA'
    'EoBEIE2PMYAVIIc2VxdWVuY2VCCQoHX3Jlc3VsdA==');

@$core.Deprecated('Use requestIdentityDescriptor instead')
const RequestIdentity$json = {
  '1': 'RequestIdentity',
  '2': [
    {
      '1': 'client_instance',
      '3': 1,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'clientInstance'
    },
    {'1': 'request_id', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'requestId'},
  ],
};

/// Descriptor for `RequestIdentity`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List requestIdentityDescriptor = $convert.base64Decode(
    'Cg9SZXF1ZXN0SWRlbnRpdHkSLQoPY2xpZW50X2luc3RhbmNlGAEgASgMQgTI8xgQUg5jbGllbn'
    'RJbnN0YW5jZRIjCgpyZXF1ZXN0X2lkGAIgASgMQgTI8xgQUglyZXF1ZXN0SWQ=');

@$core.Deprecated('Use itemDescriptor instead')
const Item$json = {
  '1': 'Item',
  '2': [
    {'1': 'id', '3': 1, '4': 1, '5': 12, '10': 'id'},
    {
      '1': 'kind',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.inference.customer.v1.ItemKind',
      '10': 'kind'
    },
    {'1': 'payload', '3': 3, '4': 1, '5': 12, '10': 'payload'},
    {'1': 'link', '3': 4, '4': 1, '5': 12, '10': 'link'},
    {
      '1': 'continuation_profile',
      '3': 5,
      '4': 1,
      '5': 12,
      '10': 'continuationProfile'
    },
  ],
};

/// Descriptor for `Item`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List itemDescriptor = $convert.base64Decode(
    'CgRJdGVtEg4KAmlkGAEgASgMUgJpZBIzCgRraW5kGAIgASgOMh8uaW5mZXJlbmNlLmN1c3RvbW'
    'VyLnYxLkl0ZW1LaW5kUgRraW5kEhgKB3BheWxvYWQYAyABKAxSB3BheWxvYWQSEgoEbGluaxgE'
    'IAEoDFIEbGluaxIxChRjb250aW51YXRpb25fcHJvZmlsZRgFIAEoDFITY29udGludWF0aW9uUH'
    'JvZmlsZQ==');

@$core.Deprecated('Use createContextRequestDescriptor instead')
const CreateContextRequest$json = {
  '1': 'CreateContextRequest',
  '2': [
    {
      '1': 'identity',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RequestIdentity',
      '8': {},
      '10': 'identity'
    },
    {'1': 'model', '3': 2, '4': 1, '5': 9, '10': 'model'},
    {
      '1': 'items',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.Item',
      '10': 'items'
    },
  ],
};

/// Descriptor for `CreateContextRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createContextRequestDescriptor = $convert.base64Decode(
    'ChRDcmVhdGVDb250ZXh0UmVxdWVzdBJICghpZGVudGl0eRgBIAEoCzImLmluZmVyZW5jZS5jdX'
    'N0b21lci52MS5SZXF1ZXN0SWRlbnRpdHlCBNDzGAFSCGlkZW50aXR5EhQKBW1vZGVsGAIgASgJ'
    'UgVtb2RlbBIxCgVpdGVtcxgDIAMoCzIbLmluZmVyZW5jZS5jdXN0b21lci52MS5JdGVtUgVpdG'
    'Vtcw==');

@$core.Deprecated('Use inspectContextRequestDescriptor instead')
const InspectContextRequest$json = {
  '1': 'InspectContextRequest',
  '2': [
    {'1': 'revision', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'revision'},
  ],
};

/// Descriptor for `InspectContextRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectContextRequestDescriptor = $convert.base64Decode(
    'ChVJbnNwZWN0Q29udGV4dFJlcXVlc3QSIAoIcmV2aXNpb24YASABKAxCBMjzGCBSCHJldmlzaW'
    '9u');

@$core.Deprecated('Use emptyDescriptor instead')
const Empty$json = {
  '1': 'Empty',
};

/// Descriptor for `Empty`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List emptyDescriptor =
    $convert.base64Decode('CgVFbXB0eQ==');

@$core.Deprecated('Use insertDescriptor instead')
const Insert$json = {
  '1': 'Insert',
  '2': [
    {'1': 'target', '3': 1, '4': 1, '5': 12, '10': 'target'},
    {
      '1': 'item',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Item',
      '10': 'item'
    },
  ],
};

/// Descriptor for `Insert`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List insertDescriptor = $convert.base64Decode(
    'CgZJbnNlcnQSFgoGdGFyZ2V0GAEgASgMUgZ0YXJnZXQSLwoEaXRlbRgCIAEoCzIbLmluZmVyZW'
    '5jZS5jdXN0b21lci52MS5JdGVtUgRpdGVt');

@$core.Deprecated('Use replaceDescriptor instead')
const Replace$json = {
  '1': 'Replace',
  '2': [
    {'1': 'target', '3': 1, '4': 1, '5': 12, '10': 'target'},
    {'1': 'payload', '3': 2, '4': 1, '5': 12, '10': 'payload'},
  ],
};

/// Descriptor for `Replace`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List replaceDescriptor = $convert.base64Decode(
    'CgdSZXBsYWNlEhYKBnRhcmdldBgBIAEoDFIGdGFyZ2V0EhgKB3BheWxvYWQYAiABKAxSB3BheW'
    'xvYWQ=');

@$core.Deprecated('Use editDescriptor instead')
const Edit$json = {
  '1': 'Edit',
  '2': [
    {
      '1': 'append',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Item',
      '9': 0,
      '10': 'append'
    },
    {
      '1': 'insert_before',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Insert',
      '9': 0,
      '10': 'insertBefore'
    },
    {
      '1': 'insert_after',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Insert',
      '9': 0,
      '10': 'insertAfter'
    },
    {
      '1': 'replace',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Replace',
      '9': 0,
      '10': 'replace'
    },
    {'1': 'delete', '3': 5, '4': 1, '5': 12, '9': 0, '10': 'delete'},
  ],
  '8': [
    {'1': 'action'},
  ],
};

/// Descriptor for `Edit`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List editDescriptor = $convert.base64Decode(
    'CgRFZGl0EjUKBmFwcGVuZBgBIAEoCzIbLmluZmVyZW5jZS5jdXN0b21lci52MS5JdGVtSABSBm'
    'FwcGVuZBJECg1pbnNlcnRfYmVmb3JlGAIgASgLMh0uaW5mZXJlbmNlLmN1c3RvbWVyLnYxLklu'
    'c2VydEgAUgxpbnNlcnRCZWZvcmUSQgoMaW5zZXJ0X2FmdGVyGAMgASgLMh0uaW5mZXJlbmNlLm'
    'N1c3RvbWVyLnYxLkluc2VydEgAUgtpbnNlcnRBZnRlchI6CgdyZXBsYWNlGAQgASgLMh4uaW5m'
    'ZXJlbmNlLmN1c3RvbWVyLnYxLlJlcGxhY2VIAFIHcmVwbGFjZRIYCgZkZWxldGUYBSABKAxIAF'
    'IGZGVsZXRlQggKBmFjdGlvbg==');

@$core.Deprecated('Use editsDescriptor instead')
const Edits$json = {
  '1': 'Edits',
  '2': [
    {
      '1': 'edits',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.Edit',
      '10': 'edits'
    },
  ],
};

/// Descriptor for `Edits`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List editsDescriptor = $convert.base64Decode(
    'CgVFZGl0cxIxCgVlZGl0cxgBIAMoCzIbLmluZmVyZW5jZS5jdXN0b21lci52MS5FZGl0UgVlZG'
    'l0cw==');

@$core.Deprecated('Use truncateDescriptor instead')
const Truncate$json = {
  '1': 'Truncate',
  '2': [
    {
      '1': 'through',
      '3': 1,
      '4': 1,
      '5': 12,
      '9': 0,
      '10': 'through',
      '17': true
    },
  ],
  '8': [
    {'1': '_through'},
  ],
};

/// Descriptor for `Truncate`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List truncateDescriptor = $convert.base64Decode(
    'CghUcnVuY2F0ZRIdCgd0aHJvdWdoGAEgASgMSABSB3Rocm91Z2iIAQFCCgoIX3Rocm91Z2g=');

@$core.Deprecated('Use compactDescriptor instead')
const Compact$json = {
  '1': 'Compact',
  '2': [
    {'1': 'selected', '3': 1, '4': 3, '5': 12, '10': 'selected'},
    {
      '1': 'replacement',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.Item',
      '10': 'replacement'
    },
  ],
};

/// Descriptor for `Compact`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List compactDescriptor = $convert.base64Decode(
    'CgdDb21wYWN0EhoKCHNlbGVjdGVkGAEgAygMUghzZWxlY3RlZBI9CgtyZXBsYWNlbWVudBgCIA'
    'MoCzIbLmluZmVyZW5jZS5jdXN0b21lci52MS5JdGVtUgtyZXBsYWNlbWVudA==');

@$core.Deprecated('Use transferDescriptor instead')
const Transfer$json = {
  '1': 'Transfer',
  '2': [
    {'1': 'model', '3': 1, '4': 1, '5': 9, '10': 'model'},
  ],
};

/// Descriptor for `Transfer`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List transferDescriptor =
    $convert.base64Decode('CghUcmFuc2ZlchIUCgVtb2RlbBgBIAEoCVIFbW9kZWw=');

@$core.Deprecated('Use mutateContextRequestDescriptor instead')
const MutateContextRequest$json = {
  '1': 'MutateContextRequest',
  '2': [
    {
      '1': 'identity',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RequestIdentity',
      '8': {},
      '10': 'identity'
    },
    {'1': 'source', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'source'},
    {
      '1': 'edit',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Edits',
      '9': 0,
      '10': 'edit'
    },
    {
      '1': 'fork',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Empty',
      '9': 0,
      '10': 'fork'
    },
    {
      '1': 'truncate',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Truncate',
      '9': 0,
      '10': 'truncate'
    },
    {
      '1': 'compact',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Compact',
      '9': 0,
      '10': 'compact'
    },
    {
      '1': 'release',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Empty',
      '9': 0,
      '10': 'release'
    },
    {
      '1': 'transfer',
      '3': 8,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Transfer',
      '9': 0,
      '10': 'transfer'
    },
  ],
  '8': [
    {'1': 'action', '2': {}},
  ],
};

/// Descriptor for `MutateContextRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List mutateContextRequestDescriptor = $convert.base64Decode(
    'ChRNdXRhdGVDb250ZXh0UmVxdWVzdBJICghpZGVudGl0eRgBIAEoCzImLmluZmVyZW5jZS5jdX'
    'N0b21lci52MS5SZXF1ZXN0SWRlbnRpdHlCBNDzGAFSCGlkZW50aXR5EhwKBnNvdXJjZRgCIAEo'
    'DEIEyPMYIFIGc291cmNlEjIKBGVkaXQYAyABKAsyHC5pbmZlcmVuY2UuY3VzdG9tZXIudjEuRW'
    'RpdHNIAFIEZWRpdBIyCgRmb3JrGAQgASgLMhwuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLkVtcHR5'
    'SABSBGZvcmsSPQoIdHJ1bmNhdGUYBSABKAsyHy5pbmZlcmVuY2UuY3VzdG9tZXIudjEuVHJ1bm'
    'NhdGVIAFIIdHJ1bmNhdGUSOgoHY29tcGFjdBgGIAEoCzIeLmluZmVyZW5jZS5jdXN0b21lci52'
    'MS5Db21wYWN0SABSB2NvbXBhY3QSOAoHcmVsZWFzZRgHIAEoCzIcLmluZmVyZW5jZS5jdXN0b2'
    '1lci52MS5FbXB0eUgAUgdyZWxlYXNlEj0KCHRyYW5zZmVyGAggASgLMh8uaW5mZXJlbmNlLmN1'
    'c3RvbWVyLnYxLlRyYW5zZmVySABSCHRyYW5zZmVyQg4KBmFjdGlvbhIE4PMYAQ==');

@$core.Deprecated('Use mutationReceiptDescriptor instead')
const MutationReceipt$json = {
  '1': 'MutationReceipt',
  '2': [
    {'1': 'revision', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'revision'},
    {
      '1': 'command_digest',
      '3': 2,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'commandDigest'
    },
    {'1': 'sequence', '3': 3, '4': 1, '5': 4, '8': {}, '10': 'sequence'},
    {'1': 'retained', '3': 4, '4': 1, '5': 8, '10': 'retained'},
  ],
};

/// Descriptor for `MutationReceipt`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List mutationReceiptDescriptor = $convert.base64Decode(
    'Cg9NdXRhdGlvblJlY2VpcHQSIAoIcmV2aXNpb24YASABKAxCBMjzGCBSCHJldmlzaW9uEisKDm'
    'NvbW1hbmRfZGlnZXN0GAIgASgMQgTI8xggUg1jb21tYW5kRGlnZXN0EiAKCHNlcXVlbmNlGAMg'
    'ASgEQgTY8xgBUghzZXF1ZW5jZRIaCghyZXRhaW5lZBgEIAEoCFIIcmV0YWluZWQ=');

@$core.Deprecated('Use contextViewDescriptor instead')
const ContextView$json = {
  '1': 'ContextView',
  '2': [
    {'1': 'revision', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'revision'},
    {
      '1': 'parent',
      '3': 2,
      '4': 1,
      '5': 12,
      '8': {},
      '9': 0,
      '10': 'parent',
      '17': true
    },
    {'1': 'lineage', '3': 3, '4': 1, '5': 12, '8': {}, '10': 'lineage'},
    {
      '1': 'execution_profile',
      '3': 4,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'executionProfile'
    },
    {
      '1': 'content_digest',
      '3': 5,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'contentDigest'
    },
    {
      '1': 'items',
      '3': 6,
      '4': 3,
      '5': 11,
      '6': '.inference.customer.v1.Item',
      '10': 'items'
    },
    {'1': 'model', '3': 7, '4': 1, '5': 9, '8': {}, '10': 'model'},
    {
      '1': 'provenance',
      '3': 8,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.ContextProvenance',
      '8': {},
      '10': 'provenance'
    },
  ],
  '8': [
    {'1': '_parent'},
  ],
};

/// Descriptor for `ContextView`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List contextViewDescriptor = $convert.base64Decode(
    'CgtDb250ZXh0VmlldxIgCghyZXZpc2lvbhgBIAEoDEIEyPMYIFIIcmV2aXNpb24SIQoGcGFyZW'
    '50GAIgASgMQgTI8xggSABSBnBhcmVudIgBARIeCgdsaW5lYWdlGAMgASgMQgTI8xggUgdsaW5l'
    'YWdlEjEKEWV4ZWN1dGlvbl9wcm9maWxlGAQgASgMQgTI8xggUhBleGVjdXRpb25Qcm9maWxlEi'
    'sKDmNvbnRlbnRfZGlnZXN0GAUgASgMQgTI8xggUg1jb250ZW50RGlnZXN0EjEKBWl0ZW1zGAYg'
    'AygLMhsuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLkl0ZW1SBWl0ZW1zEhsKBW1vZGVsGAcgASgJQg'
    'X48xiAAlIFbW9kZWwSTgoKcHJvdmVuYW5jZRgIIAEoCzIoLmluZmVyZW5jZS5jdXN0b21lci52'
    'MS5Db250ZXh0UHJvdmVuYW5jZUIE0PMYAVIKcHJvdmVuYW5jZUIJCgdfcGFyZW50');

@$core.Deprecated('Use contextProvenanceDescriptor instead')
const ContextProvenance$json = {
  '1': 'ContextProvenance',
  '2': [
    {
      '1': 'created',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Empty',
      '9': 0,
      '10': 'created'
    },
    {
      '1': 'derived',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.ProvenanceSource',
      '9': 0,
      '10': 'derived'
    },
    {
      '1': 'forked',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.ProvenanceSource',
      '9': 0,
      '10': 'forked'
    },
    {
      '1': 'transferred',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.TransferProvenance',
      '9': 0,
      '10': 'transferred'
    },
    {
      '1': 'generated',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.GenerationProvenance',
      '9': 0,
      '10': 'generated'
    },
    {
      '1': 'run_input',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RunInputProvenance',
      '9': 0,
      '10': 'runInput'
    },
  ],
  '8': [
    {'1': 'origin', '2': {}},
  ],
};

/// Descriptor for `ContextProvenance`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List contextProvenanceDescriptor = $convert.base64Decode(
    'ChFDb250ZXh0UHJvdmVuYW5jZRI4CgdjcmVhdGVkGAEgASgLMhwuaW5mZXJlbmNlLmN1c3RvbW'
    'VyLnYxLkVtcHR5SABSB2NyZWF0ZWQSQwoHZGVyaXZlZBgCIAEoCzInLmluZmVyZW5jZS5jdXN0'
    'b21lci52MS5Qcm92ZW5hbmNlU291cmNlSABSB2Rlcml2ZWQSQQoGZm9ya2VkGAMgASgLMicuaW'
    '5mZXJlbmNlLmN1c3RvbWVyLnYxLlByb3ZlbmFuY2VTb3VyY2VIAFIGZm9ya2VkEk0KC3RyYW5z'
    'ZmVycmVkGAQgASgLMikuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLlRyYW5zZmVyUHJvdmVuYW5jZU'
    'gAUgt0cmFuc2ZlcnJlZBJLCglnZW5lcmF0ZWQYBSABKAsyKy5pbmZlcmVuY2UuY3VzdG9tZXIu'
    'djEuR2VuZXJhdGlvblByb3ZlbmFuY2VIAFIJZ2VuZXJhdGVkEkgKCXJ1bl9pbnB1dBgGIAEoCz'
    'IpLmluZmVyZW5jZS5jdXN0b21lci52MS5SdW5JbnB1dFByb3ZlbmFuY2VIAFIIcnVuSW5wdXRC'
    'DgoGb3JpZ2luEgTg8xgB');

@$core.Deprecated('Use provenanceSourceDescriptor instead')
const ProvenanceSource$json = {
  '1': 'ProvenanceSource',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'source'},
  ],
};

/// Descriptor for `ProvenanceSource`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List provenanceSourceDescriptor = $convert.base64Decode(
    'ChBQcm92ZW5hbmNlU291cmNlEhwKBnNvdXJjZRgBIAEoDEIEyPMYIFIGc291cmNl');

@$core.Deprecated('Use transferProvenanceDescriptor instead')
const TransferProvenance$json = {
  '1': 'TransferProvenance',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'source'},
    {
      '1': 'reused_compatible_state',
      '3': 2,
      '4': 1,
      '5': 8,
      '10': 'reusedCompatibleState'
    },
  ],
};

/// Descriptor for `TransferProvenance`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List transferProvenanceDescriptor = $convert.base64Decode(
    'ChJUcmFuc2ZlclByb3ZlbmFuY2USHAoGc291cmNlGAEgASgMQgTI8xggUgZzb3VyY2USNgoXcm'
    'V1c2VkX2NvbXBhdGlibGVfc3RhdGUYAiABKAhSFXJldXNlZENvbXBhdGlibGVTdGF0ZQ==');

@$core.Deprecated('Use generationProvenanceDescriptor instead')
const GenerationProvenance$json = {
  '1': 'GenerationProvenance',
  '2': [
    {'1': 'run_id', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'runId'},
    {
      '1': 'terminal_receipt_digest',
      '3': 2,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'terminalReceiptDigest'
    },
  ],
};

/// Descriptor for `GenerationProvenance`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List generationProvenanceDescriptor = $convert.base64Decode(
    'ChRHZW5lcmF0aW9uUHJvdmVuYW5jZRIbCgZydW5faWQYASABKAxCBMjzGBBSBXJ1bklkEjwKF3'
    'Rlcm1pbmFsX3JlY2VpcHRfZGlnZXN0GAIgASgMQgTI8xggUhV0ZXJtaW5hbFJlY2VpcHREaWdl'
    'c3Q=');

@$core.Deprecated('Use runInputProvenanceDescriptor instead')
const RunInputProvenance$json = {
  '1': 'RunInputProvenance',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'source'},
    {'1': 'run_id', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'runId'},
    {
      '1': 'maximum_output',
      '3': 3,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'maximumOutput'
    },
    {'1': 'seed', '3': 4, '4': 1, '5': 4, '9': 0, '10': 'seed', '17': true},
  ],
  '8': [
    {'1': '_seed'},
  ],
};

/// Descriptor for `RunInputProvenance`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List runInputProvenanceDescriptor = $convert.base64Decode(
    'ChJSdW5JbnB1dFByb3ZlbmFuY2USHAoGc291cmNlGAEgASgMQgTI8xggUgZzb3VyY2USGwoGcn'
    'VuX2lkGAIgASgMQgTI8xgQUgVydW5JZBIrCg5tYXhpbXVtX291dHB1dBgDIAEoBEIE2PMYAVIN'
    'bWF4aW11bU91dHB1dBIXCgRzZWVkGAQgASgESABSBHNlZWSIAQFCBwoFX3NlZWQ=');

@$core.Deprecated('Use generateRunRequestDescriptor instead')
const GenerateRunRequest$json = {
  '1': 'GenerateRunRequest',
  '2': [
    {
      '1': 'identity',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RequestIdentity',
      '8': {},
      '10': 'identity'
    },
    {'1': 'context', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'context'},
    {
      '1': 'input',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.Item',
      '8': {},
      '10': 'input'
    },
    {
      '1': 'maximum_output',
      '3': 4,
      '4': 1,
      '5': 4,
      '8': {},
      '10': 'maximumOutput'
    },
    {'1': 'seed', '3': 5, '4': 1, '5': 4, '9': 0, '10': 'seed', '17': true},
  ],
  '8': [
    {'1': '_seed'},
  ],
};

/// Descriptor for `GenerateRunRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List generateRunRequestDescriptor = $convert.base64Decode(
    'ChJHZW5lcmF0ZVJ1blJlcXVlc3QSSAoIaWRlbnRpdHkYASABKAsyJi5pbmZlcmVuY2UuY3VzdG'
    '9tZXIudjEuUmVxdWVzdElkZW50aXR5QgTQ8xgBUghpZGVudGl0eRIeCgdjb250ZXh0GAIgASgM'
    'QgTI8xggUgdjb250ZXh0EjcKBWlucHV0GAMgASgLMhsuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLk'
    'l0ZW1CBNDzGAFSBWlucHV0EisKDm1heGltdW1fb3V0cHV0GAQgASgEQgTY8xgBUg1tYXhpbXVt'
    'T3V0cHV0EhcKBHNlZWQYBSABKARIAFIEc2VlZIgBAUIHCgVfc2VlZA==');

@$core.Deprecated('Use generateRunResponseDescriptor instead')
const GenerateRunResponse$json = {
  '1': 'GenerateRunResponse',
  '2': [
    {
      '1': 'run',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RunView',
      '8': {},
      '10': 'run'
    },
  ],
};

/// Descriptor for `GenerateRunResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List generateRunResponseDescriptor = $convert.base64Decode(
    'ChNHZW5lcmF0ZVJ1blJlc3BvbnNlEjYKA3J1bhgBIAEoCzIeLmluZmVyZW5jZS5jdXN0b21lci'
    '52MS5SdW5WaWV3QgTQ8xgBUgNydW4=');

@$core.Deprecated('Use inspectRunRequestDescriptor instead')
const InspectRunRequest$json = {
  '1': 'InspectRunRequest',
  '2': [
    {'1': 'run_id', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'runId'},
  ],
};

/// Descriptor for `InspectRunRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectRunRequestDescriptor = $convert.base64Decode(
    'ChFJbnNwZWN0UnVuUmVxdWVzdBIbCgZydW5faWQYASABKAxCBMjzGBBSBXJ1bklk');

@$core.Deprecated('Use watchRunRequestDescriptor instead')
const WatchRunRequest$json = {
  '1': 'WatchRunRequest',
  '2': [
    {'1': 'run_id', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'runId'},
    {'1': 'from_sequence', '3': 2, '4': 1, '5': 4, '10': 'fromSequence'},
  ],
};

/// Descriptor for `WatchRunRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List watchRunRequestDescriptor = $convert.base64Decode(
    'Cg9XYXRjaFJ1blJlcXVlc3QSGwoGcnVuX2lkGAEgASgMQgTI8xgQUgVydW5JZBIjCg1mcm9tX3'
    'NlcXVlbmNlGAIgASgEUgxmcm9tU2VxdWVuY2U=');

@$core.Deprecated('Use logicalUsageDescriptor instead')
const LogicalUsage$json = {
  '1': 'LogicalUsage',
  '2': [
    {'1': 'new_prefill', '3': 1, '4': 1, '5': 4, '10': 'newPrefill'},
    {'1': 'generated_output', '3': 2, '4': 1, '5': 4, '10': 'generatedOutput'},
    {
      '1': 'effective_context_reads',
      '3': 3,
      '4': 1,
      '5': 4,
      '10': 'effectiveContextReads'
    },
    {
      '1': 'retained_byte_millis',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'retainedByteMillis'
    },
  ],
};

/// Descriptor for `LogicalUsage`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List logicalUsageDescriptor = $convert.base64Decode(
    'CgxMb2dpY2FsVXNhZ2USHwoLbmV3X3ByZWZpbGwYASABKARSCm5ld1ByZWZpbGwSKQoQZ2VuZX'
    'JhdGVkX291dHB1dBgCIAEoBFIPZ2VuZXJhdGVkT3V0cHV0EjYKF2VmZmVjdGl2ZV9jb250ZXh0'
    'X3JlYWRzGAMgASgEUhVlZmZlY3RpdmVDb250ZXh0UmVhZHMSMAoUcmV0YWluZWRfYnl0ZV9taW'
    'xsaXMYBCABKARSEnJldGFpbmVkQnl0ZU1pbGxpcw==');

@$core.Deprecated('Use usageReceiptDescriptor instead')
const UsageReceipt$json = {
  '1': 'UsageReceipt',
  '2': [
    {'1': 'receipt_id', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'receiptId'},
    {
      '1': 'model_profile',
      '3': 2,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'modelProfile'
    },
    {
      '1': 'meter_revision',
      '3': 3,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'meterRevision'
    },
    {
      '1': 'usage',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.LogicalUsage',
      '8': {},
      '10': 'usage'
    },
    {
      '1': 'rate_card_revision',
      '3': 5,
      '4': 1,
      '5': 12,
      '8': {},
      '10': 'rateCardRevision'
    },
  ],
};

/// Descriptor for `UsageReceipt`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List usageReceiptDescriptor = $convert.base64Decode(
    'CgxVc2FnZVJlY2VpcHQSIwoKcmVjZWlwdF9pZBgBIAEoDEIEyPMYIFIJcmVjZWlwdElkEikKDW'
    '1vZGVsX3Byb2ZpbGUYAiABKAxCBMjzGCBSDG1vZGVsUHJvZmlsZRIrCg5tZXRlcl9yZXZpc2lv'
    'bhgDIAEoDEIEyPMYIFINbWV0ZXJSZXZpc2lvbhI/CgV1c2FnZRgEIAEoCzIjLmluZmVyZW5jZS'
    '5jdXN0b21lci52MS5Mb2dpY2FsVXNhZ2VCBNDzGAFSBXVzYWdlEjIKEnJhdGVfY2FyZF9yZXZp'
    'c2lvbhgFIAEoDEIEyPMYIFIQcmF0ZUNhcmRSZXZpc2lvbg==');

@$core.Deprecated('Use runResultDescriptor instead')
const RunResult$json = {
  '1': 'RunResult',
  '2': [
    {'1': 'output', '3': 1, '4': 1, '5': 12, '10': 'output'},
    {
      '1': 'context',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.ContextView',
      '9': 0,
      '10': 'context',
      '17': true
    },
    {
      '1': 'terminal',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.inference.customer.v1.RunTerminal',
      '8': {},
      '10': 'terminal'
    },
    {
      '1': 'receipt',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.UsageReceipt',
      '9': 1,
      '10': 'receipt',
      '17': true
    },
  ],
  '8': [
    {'1': '_context'},
    {'1': '_receipt'},
  ],
};

/// Descriptor for `RunResult`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List runResultDescriptor = $convert.base64Decode(
    'CglSdW5SZXN1bHQSFgoGb3V0cHV0GAEgASgMUgZvdXRwdXQSQQoHY29udGV4dBgCIAEoCzIiLm'
    'luZmVyZW5jZS5jdXN0b21lci52MS5Db250ZXh0Vmlld0gAUgdjb250ZXh0iAEBEkQKCHRlcm1p'
    'bmFsGAMgASgOMiIuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLlJ1blRlcm1pbmFsQgSI9BgBUgh0ZX'
    'JtaW5hbBJCCgdyZWNlaXB0GAQgASgLMiMuaW5mZXJlbmNlLmN1c3RvbWVyLnYxLlVzYWdlUmVj'
    'ZWlwdEgBUgdyZWNlaXB0iAEBQgoKCF9jb250ZXh0QgoKCF9yZWNlaXB0');

@$core.Deprecated('Use runViewDescriptor instead')
const RunView$json = {
  '1': 'RunView',
  '2': [
    {'1': 'run_id', '3': 1, '4': 1, '5': 12, '8': {}, '10': 'runId'},
    {'1': 'input', '3': 2, '4': 1, '5': 12, '8': {}, '10': 'input'},
    {'1': 'model', '3': 3, '4': 1, '5': 9, '8': {}, '10': 'model'},
    {'1': 'last_sequence', '3': 4, '4': 1, '5': 4, '10': 'lastSequence'},
    {
      '1': 'cancellation_requested',
      '3': 5,
      '4': 1,
      '5': 8,
      '10': 'cancellationRequested'
    },
    {
      '1': 'result',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RunResult',
      '9': 0,
      '10': 'result',
      '17': true
    },
  ],
  '8': [
    {'1': '_result'},
  ],
};

/// Descriptor for `RunView`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List runViewDescriptor = $convert.base64Decode(
    'CgdSdW5WaWV3EhsKBnJ1bl9pZBgBIAEoDEIEyPMYEFIFcnVuSWQSGgoFaW5wdXQYAiABKAxCBM'
    'jzGCBSBWlucHV0EhsKBW1vZGVsGAMgASgJQgX48xiAAlIFbW9kZWwSIwoNbGFzdF9zZXF1ZW5j'
    'ZRgEIAEoBFIMbGFzdFNlcXVlbmNlEjUKFmNhbmNlbGxhdGlvbl9yZXF1ZXN0ZWQYBSABKAhSFW'
    'NhbmNlbGxhdGlvblJlcXVlc3RlZBI9CgZyZXN1bHQYBiABKAsyIC5pbmZlcmVuY2UuY3VzdG9t'
    'ZXIudjEuUnVuUmVzdWx0SABSBnJlc3VsdIgBAUIJCgdfcmVzdWx0');

@$core.Deprecated('Use runEventDescriptor instead')
const RunEvent$json = {
  '1': 'RunEvent',
  '2': [
    {'1': 'sequence', '3': 1, '4': 1, '5': 4, '10': 'sequence'},
    {'1': 'output', '3': 2, '4': 1, '5': 12, '9': 0, '10': 'output'},
    {
      '1': 'usage',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.LogicalUsage',
      '9': 0,
      '10': 'usage'
    },
    {
      '1': 'terminal',
      '3': 4,
      '4': 1,
      '5': 14,
      '6': '.inference.customer.v1.RunTerminal',
      '8': {},
      '9': 0,
      '10': 'terminal'
    },
    {
      '1': 'progress',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.inference.customer.v1.RunProgress',
      '9': 0,
      '10': 'progress'
    },
  ],
  '8': [
    {'1': 'event', '2': {}},
  ],
};

/// Descriptor for `RunEvent`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List runEventDescriptor = $convert.base64Decode(
    'CghSdW5FdmVudBIaCghzZXF1ZW5jZRgBIAEoBFIIc2VxdWVuY2USGAoGb3V0cHV0GAIgASgMSA'
    'BSBm91dHB1dBI7CgV1c2FnZRgDIAEoCzIjLmluZmVyZW5jZS5jdXN0b21lci52MS5Mb2dpY2Fs'
    'VXNhZ2VIAFIFdXNhZ2USRgoIdGVybWluYWwYBCABKA4yIi5pbmZlcmVuY2UuY3VzdG9tZXIudj'
    'EuUnVuVGVybWluYWxCBIj0GAFIAFIIdGVybWluYWwSQAoIcHJvZ3Jlc3MYBSABKAsyIi5pbmZl'
    'cmVuY2UuY3VzdG9tZXIudjEuUnVuUHJvZ3Jlc3NIAFIIcHJvZ3Jlc3NCDQoFZXZlbnQSBODzGA'
    'E=');

@$core.Deprecated('Use runProgressDescriptor instead')
const RunProgress$json = {
  '1': 'RunProgress',
  '2': [
    {'1': 'kind', '3': 1, '4': 1, '5': 9, '10': 'kind'},
  ],
};

/// Descriptor for `RunProgress`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List runProgressDescriptor =
    $convert.base64Decode('CgtSdW5Qcm9ncmVzcxISCgRraW5kGAEgASgJUgRraW5k');
