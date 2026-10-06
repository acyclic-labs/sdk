// This is a generated file - do not edit.
//
// Generated from actors/v1/actors.proto.

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

@$core.Deprecated('Use subscriptionStateDescriptor instead')
const SubscriptionState$json = {
  '1': 'SubscriptionState',
  '2': [
    {'1': 'SUBSCRIPTION_STATE_UNSPECIFIED', '2': 0},
    {'1': 'SUBSCRIPTION_STATE_ACTIVE', '2': 1},
    {'1': 'SUBSCRIPTION_STATE_PAUSED', '2': 2},
  ],
};

/// Descriptor for `SubscriptionState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List subscriptionStateDescriptor = $convert.base64Decode(
    'ChFTdWJzY3JpcHRpb25TdGF0ZRIiCh5TVUJTQ1JJUFRJT05fU1RBVEVfVU5TUEVDSUZJRUQQAB'
    'IdChlTVUJTQ1JJUFRJT05fU1RBVEVfQUNUSVZFEAESHQoZU1VCU0NSSVBUSU9OX1NUQVRFX1BB'
    'VVNFRBAC');

@$core.Deprecated('Use actorStateDescriptor instead')
const ActorState$json = {
  '1': 'ActorState',
  '2': [
    {'1': 'ACTOR_STATE_UNSPECIFIED', '2': 0},
    {'1': 'ACTOR_STATE_ACTIVE', '2': 1},
    {'1': 'ACTOR_STATE_HIBERNATED', '2': 2},
    {'1': 'ACTOR_STATE_PAUSED', '2': 3},
  ],
};

/// Descriptor for `ActorState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List actorStateDescriptor = $convert.base64Decode(
    'CgpBY3RvclN0YXRlEhsKF0FDVE9SX1NUQVRFX1VOU1BFQ0lGSUVEEAASFgoSQUNUT1JfU1RBVE'
    'VfQUNUSVZFEAESGgoWQUNUT1JfU1RBVEVfSElCRVJOQVRFRBACEhYKEkFDVE9SX1NUQVRFX1BB'
    'VVNFRBAD');

@$core.Deprecated('Use errorCodeDescriptor instead')
const ErrorCode$json = {
  '1': 'ErrorCode',
  '2': [
    {'1': 'ERROR_CODE_UNSPECIFIED', '2': 0},
    {'1': 'ERROR_CODE_INVALID_ARGUMENT', '2': 1},
    {'1': 'ERROR_CODE_CAPABILITY_DENIED', '2': 2},
    {'1': 'ERROR_CODE_CAPABILITY_EXPIRED', '2': 3},
    {'1': 'ERROR_CODE_ACTOR_NOT_FOUND', '2': 4},
    {'1': 'ERROR_CODE_SUBSCRIPTION_NOT_FOUND', '2': 5},
    {'1': 'ERROR_CODE_IDEMPOTENCY_MISMATCH', '2': 6},
    {'1': 'ERROR_CODE_CONFLICT', '2': 7},
    {'1': 'ERROR_CODE_ADMISSION_DENIED', '2': 8},
    {'1': 'ERROR_CODE_CHECKPOINT_FAILED', '2': 9},
    {'1': 'ERROR_CODE_DEPENDENCY_UNAVAILABLE', '2': 10},
  ],
};

/// Descriptor for `ErrorCode`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List errorCodeDescriptor = $convert.base64Decode(
    'CglFcnJvckNvZGUSGgoWRVJST1JfQ09ERV9VTlNQRUNJRklFRBAAEh8KG0VSUk9SX0NPREVfSU'
    '5WQUxJRF9BUkdVTUVOVBABEiAKHEVSUk9SX0NPREVfQ0FQQUJJTElUWV9ERU5JRUQQAhIhCh1F'
    'UlJPUl9DT0RFX0NBUEFCSUxJVFlfRVhQSVJFRBADEh4KGkVSUk9SX0NPREVfQUNUT1JfTk9UX0'
    'ZPVU5EEAQSJQohRVJST1JfQ09ERV9TVUJTQ1JJUFRJT05fTk9UX0ZPVU5EEAUSIwofRVJST1Jf'
    'Q09ERV9JREVNUE9URU5DWV9NSVNNQVRDSBAGEhcKE0VSUk9SX0NPREVfQ09ORkxJQ1QQBxIfCh'
    'tFUlJPUl9DT0RFX0FETUlTU0lPTl9ERU5JRUQQCBIgChxFUlJPUl9DT0RFX0NIRUNLUE9JTlRf'
    'RkFJTEVEEAkSJQohRVJST1JfQ09ERV9ERVBFTkRFTkNZX1VOQVZBSUxBQkxFEAo=');

@$core.Deprecated('Use bindingDescriptor instead')
const Binding$json = {
  '1': 'Binding',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'capability', '3': 2, '4': 1, '5': 9, '10': 'capability'},
    {'1': 'resource', '3': 3, '4': 1, '5': 9, '10': 'resource'},
  ],
};

/// Descriptor for `Binding`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List bindingDescriptor = $convert.base64Decode(
    'CgdCaW5kaW5nEhIKBG5hbWUYASABKAlSBG5hbWUSHgoKY2FwYWJpbGl0eRgCIAEoCVIKY2FwYW'
    'JpbGl0eRIaCghyZXNvdXJjZRgDIAEoCVIIcmVzb3VyY2U=');

@$core.Deprecated('Use actorLimitsDescriptor instead')
const ActorLimits$json = {
  '1': 'ActorLimits',
  '2': [
    {
      '1': 'handler_timeout_millis',
      '3': 1,
      '4': 1,
      '5': 4,
      '10': 'handlerTimeoutMillis'
    },
    {'1': 'memory_bytes', '3': 2, '4': 1, '5': 4, '10': 'memoryBytes'},
    {'1': 'checkpoint_bytes', '3': 3, '4': 1, '5': 4, '10': 'checkpointBytes'},
  ],
};

/// Descriptor for `ActorLimits`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List actorLimitsDescriptor = $convert.base64Decode(
    'CgtBY3RvckxpbWl0cxI0ChZoYW5kbGVyX3RpbWVvdXRfbWlsbGlzGAEgASgEUhRoYW5kbGVyVG'
    'ltZW91dE1pbGxpcxIhCgxtZW1vcnlfYnl0ZXMYAiABKARSC21lbW9yeUJ5dGVzEikKEGNoZWNr'
    'cG9pbnRfYnl0ZXMYAyABKARSD2NoZWNrcG9pbnRCeXRlcw==');

@$core.Deprecated('Use subscriptionStartDescriptor instead')
const SubscriptionStart$json = {
  '1': 'SubscriptionStart',
  '2': [
    {'1': 'cursor', '3': 1, '4': 1, '5': 4, '9': 0, '10': 'cursor'},
    {'1': 'current_head', '3': 2, '4': 1, '5': 8, '9': 0, '10': 'currentHead'},
  ],
  '8': [
    {'1': 'start'},
  ],
};

/// Descriptor for `SubscriptionStart`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List subscriptionStartDescriptor = $convert.base64Decode(
    'ChFTdWJzY3JpcHRpb25TdGFydBIYCgZjdXJzb3IYASABKARIAFIGY3Vyc29yEiMKDGN1cnJlbn'
    'RfaGVhZBgCIAEoCEgAUgtjdXJyZW50SGVhZEIHCgVzdGFydA==');

@$core.Deprecated('Use subscriptionSpecDescriptor instead')
const SubscriptionSpec$json = {
  '1': 'SubscriptionSpec',
  '2': [
    {'1': 'subscription_id', '3': 1, '4': 1, '5': 9, '10': 'subscriptionId'},
    {'1': 'stream_path', '3': 2, '4': 1, '5': 9, '10': 'streamPath'},
    {
      '1': 'start',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.SubscriptionStart',
      '10': 'start'
    },
    {'1': 'placement_anchor', '3': 4, '4': 1, '5': 8, '10': 'placementAnchor'},
  ],
};

/// Descriptor for `SubscriptionSpec`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List subscriptionSpecDescriptor = $convert.base64Decode(
    'ChBTdWJzY3JpcHRpb25TcGVjEicKD3N1YnNjcmlwdGlvbl9pZBgBIAEoCVIOc3Vic2NyaXB0aW'
    '9uSWQSHwoLc3RyZWFtX3BhdGgYAiABKAlSCnN0cmVhbVBhdGgSOgoFc3RhcnQYAyABKAsyJC5h'
    'Y3ljbGljLmFjdG9ycy52MS5TdWJzY3JpcHRpb25TdGFydFIFc3RhcnQSKQoQcGxhY2VtZW50X2'
    'FuY2hvchgEIAEoCFIPcGxhY2VtZW50QW5jaG9y');

@$core.Deprecated('Use subscriptionObservationDescriptor instead')
const SubscriptionObservation$json = {
  '1': 'SubscriptionObservation',
  '2': [
    {'1': 'subscription_id', '3': 1, '4': 1, '5': 9, '10': 'subscriptionId'},
    {'1': 'stream_path', '3': 2, '4': 1, '5': 9, '10': 'streamPath'},
    {
      '1': 'state',
      '3': 3,
      '4': 1,
      '5': 14,
      '6': '.acyclic.actors.v1.SubscriptionState',
      '10': 'state'
    },
    {'1': 'delivered_cursor', '3': 4, '4': 1, '5': 4, '10': 'deliveredCursor'},
    {'1': 'completed_cursor', '3': 5, '4': 1, '5': 4, '10': 'completedCursor'},
    {
      '1': 'recoverable_cursor',
      '3': 6,
      '4': 1,
      '5': 4,
      '10': 'recoverableCursor'
    },
    {'1': 'placement_anchor', '3': 7, '4': 1, '5': 8, '10': 'placementAnchor'},
    {'1': 'retry_count', '3': 8, '4': 1, '5': 13, '10': 'retryCount'},
    {'1': 'failure_code', '3': 9, '4': 1, '5': 9, '10': 'failureCode'},
    {
      '1': 'failed_cursor',
      '3': 10,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'failedCursor',
      '17': true
    },
  ],
  '8': [
    {'1': '_failed_cursor'},
  ],
};

/// Descriptor for `SubscriptionObservation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List subscriptionObservationDescriptor = $convert.base64Decode(
    'ChdTdWJzY3JpcHRpb25PYnNlcnZhdGlvbhInCg9zdWJzY3JpcHRpb25faWQYASABKAlSDnN1Yn'
    'NjcmlwdGlvbklkEh8KC3N0cmVhbV9wYXRoGAIgASgJUgpzdHJlYW1QYXRoEjoKBXN0YXRlGAMg'
    'ASgOMiQuYWN5Y2xpYy5hY3RvcnMudjEuU3Vic2NyaXB0aW9uU3RhdGVSBXN0YXRlEikKEGRlbG'
    'l2ZXJlZF9jdXJzb3IYBCABKARSD2RlbGl2ZXJlZEN1cnNvchIpChBjb21wbGV0ZWRfY3Vyc29y'
    'GAUgASgEUg9jb21wbGV0ZWRDdXJzb3ISLQoScmVjb3ZlcmFibGVfY3Vyc29yGAYgASgEUhFyZW'
    'NvdmVyYWJsZUN1cnNvchIpChBwbGFjZW1lbnRfYW5jaG9yGAcgASgIUg9wbGFjZW1lbnRBbmNo'
    'b3ISHwoLcmV0cnlfY291bnQYCCABKA1SCnJldHJ5Q291bnQSIQoMZmFpbHVyZV9jb2RlGAkgAS'
    'gJUgtmYWlsdXJlQ29kZRIoCg1mYWlsZWRfY3Vyc29yGAogASgESABSDGZhaWxlZEN1cnNvcogB'
    'AUIQCg5fZmFpbGVkX2N1cnNvcg==');

@$core.Deprecated('Use actorObservationDescriptor instead')
const ActorObservation$json = {
  '1': 'ActorObservation',
  '2': [
    {'1': 'actor_id', '3': 1, '4': 1, '5': 9, '10': 'actorId'},
    {'1': 'code_sha256', '3': 2, '4': 1, '5': 12, '10': 'codeSha256'},
    {'1': 'home_region', '3': 3, '4': 1, '5': 9, '10': 'homeRegion'},
    {
      '1': 'state',
      '3': 4,
      '4': 1,
      '5': 14,
      '6': '.acyclic.actors.v1.ActorState',
      '10': 'state'
    },
    {
      '1': 'subscriptions',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.actors.v1.SubscriptionObservation',
      '10': 'subscriptions'
    },
    {
      '1': 'checkpoint_unix_millis',
      '3': 6,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'checkpointUnixMillis',
      '17': true
    },
    {'1': 'checkpoint_epoch', '3': 7, '4': 1, '5': 4, '10': 'checkpointEpoch'},
    {
      '1': 'configuration_revision',
      '3': 8,
      '4': 1,
      '5': 4,
      '10': 'configurationRevision'
    },
  ],
  '8': [
    {'1': '_checkpoint_unix_millis'},
  ],
};

/// Descriptor for `ActorObservation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List actorObservationDescriptor = $convert.base64Decode(
    'ChBBY3Rvck9ic2VydmF0aW9uEhkKCGFjdG9yX2lkGAEgASgJUgdhY3RvcklkEh8KC2NvZGVfc2'
    'hhMjU2GAIgASgMUgpjb2RlU2hhMjU2Eh8KC2hvbWVfcmVnaW9uGAMgASgJUgpob21lUmVnaW9u'
    'EjMKBXN0YXRlGAQgASgOMh0uYWN5Y2xpYy5hY3RvcnMudjEuQWN0b3JTdGF0ZVIFc3RhdGUSUA'
    'oNc3Vic2NyaXB0aW9ucxgFIAMoCzIqLmFjeWNsaWMuYWN0b3JzLnYxLlN1YnNjcmlwdGlvbk9i'
    'c2VydmF0aW9uUg1zdWJzY3JpcHRpb25zEjkKFmNoZWNrcG9pbnRfdW5peF9taWxsaXMYBiABKA'
    'RIAFIUY2hlY2twb2ludFVuaXhNaWxsaXOIAQESKQoQY2hlY2twb2ludF9lcG9jaBgHIAEoBFIP'
    'Y2hlY2twb2ludEVwb2NoEjUKFmNvbmZpZ3VyYXRpb25fcmV2aXNpb24YCCABKARSFWNvbmZpZ3'
    'VyYXRpb25SZXZpc2lvbkIZChdfY2hlY2twb2ludF91bml4X21pbGxpcw==');

@$core.Deprecated('Use createActorRequestDescriptor instead')
const CreateActorRequest$json = {
  '1': 'CreateActorRequest',
  '2': [
    {'1': 'code_sha256', '3': 1, '4': 1, '5': 12, '10': 'codeSha256'},
    {'1': 'home_region', '3': 2, '4': 1, '5': 9, '10': 'homeRegion'},
    {
      '1': 'bindings',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.actors.v1.Binding',
      '10': 'bindings'
    },
    {
      '1': 'limits',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorLimits',
      '10': 'limits'
    },
    {
      '1': 'subscriptions',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.actors.v1.SubscriptionSpec',
      '10': 'subscriptions'
    },
    {'1': 'idempotency_key', '3': 6, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `CreateActorRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createActorRequestDescriptor = $convert.base64Decode(
    'ChJDcmVhdGVBY3RvclJlcXVlc3QSHwoLY29kZV9zaGEyNTYYASABKAxSCmNvZGVTaGEyNTYSHw'
    'oLaG9tZV9yZWdpb24YAiABKAlSCmhvbWVSZWdpb24SNgoIYmluZGluZ3MYAyADKAsyGi5hY3lj'
    'bGljLmFjdG9ycy52MS5CaW5kaW5nUghiaW5kaW5ncxI2CgZsaW1pdHMYBCABKAsyHi5hY3ljbG'
    'ljLmFjdG9ycy52MS5BY3RvckxpbWl0c1IGbGltaXRzEkkKDXN1YnNjcmlwdGlvbnMYBSADKAsy'
    'Iy5hY3ljbGljLmFjdG9ycy52MS5TdWJzY3JpcHRpb25TcGVjUg1zdWJzY3JpcHRpb25zEicKD2'
    'lkZW1wb3RlbmN5X2tleRgGIAEoCVIOaWRlbXBvdGVuY3lLZXk=');

@$core.Deprecated('Use createActorResponseDescriptor instead')
const CreateActorResponse$json = {
  '1': 'CreateActorResponse',
  '2': [
    {
      '1': 'actor',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorObservation',
      '10': 'actor'
    },
  ],
};

/// Descriptor for `CreateActorResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createActorResponseDescriptor = $convert.base64Decode(
    'ChNDcmVhdGVBY3RvclJlc3BvbnNlEjkKBWFjdG9yGAEgASgLMiMuYWN5Y2xpYy5hY3RvcnMudj'
    'EuQWN0b3JPYnNlcnZhdGlvblIFYWN0b3I=');

@$core.Deprecated('Use updateActorRequestDescriptor instead')
const UpdateActorRequest$json = {
  '1': 'UpdateActorRequest',
  '2': [
    {'1': 'actor_id', '3': 1, '4': 1, '5': 9, '10': 'actorId'},
    {'1': 'code_sha256', '3': 2, '4': 1, '5': 12, '10': 'codeSha256'},
    {
      '1': 'bindings',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.actors.v1.Binding',
      '10': 'bindings'
    },
    {
      '1': 'limits',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorLimits',
      '10': 'limits'
    },
    {
      '1': 'expected_configuration_revision',
      '3': 5,
      '4': 1,
      '5': 4,
      '10': 'expectedConfigurationRevision'
    },
    {'1': 'idempotency_key', '3': 6, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `UpdateActorRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List updateActorRequestDescriptor = $convert.base64Decode(
    'ChJVcGRhdGVBY3RvclJlcXVlc3QSGQoIYWN0b3JfaWQYASABKAlSB2FjdG9ySWQSHwoLY29kZV'
    '9zaGEyNTYYAiABKAxSCmNvZGVTaGEyNTYSNgoIYmluZGluZ3MYAyADKAsyGi5hY3ljbGljLmFj'
    'dG9ycy52MS5CaW5kaW5nUghiaW5kaW5ncxI2CgZsaW1pdHMYBCABKAsyHi5hY3ljbGljLmFjdG'
    '9ycy52MS5BY3RvckxpbWl0c1IGbGltaXRzEkYKH2V4cGVjdGVkX2NvbmZpZ3VyYXRpb25fcmV2'
    'aXNpb24YBSABKARSHWV4cGVjdGVkQ29uZmlndXJhdGlvblJldmlzaW9uEicKD2lkZW1wb3Rlbm'
    'N5X2tleRgGIAEoCVIOaWRlbXBvdGVuY3lLZXk=');

@$core.Deprecated('Use updateActorResponseDescriptor instead')
const UpdateActorResponse$json = {
  '1': 'UpdateActorResponse',
  '2': [
    {
      '1': 'actor',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorObservation',
      '10': 'actor'
    },
  ],
};

/// Descriptor for `UpdateActorResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List updateActorResponseDescriptor = $convert.base64Decode(
    'ChNVcGRhdGVBY3RvclJlc3BvbnNlEjkKBWFjdG9yGAEgASgLMiMuYWN5Y2xpYy5hY3RvcnMudj'
    'EuQWN0b3JPYnNlcnZhdGlvblIFYWN0b3I=');

@$core.Deprecated('Use inspectActorRequestDescriptor instead')
const InspectActorRequest$json = {
  '1': 'InspectActorRequest',
  '2': [
    {'1': 'actor_id', '3': 1, '4': 1, '5': 9, '10': 'actorId'},
  ],
};

/// Descriptor for `InspectActorRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectActorRequestDescriptor =
    $convert.base64Decode(
        'ChNJbnNwZWN0QWN0b3JSZXF1ZXN0EhkKCGFjdG9yX2lkGAEgASgJUgdhY3Rvcklk');

@$core.Deprecated('Use inspectActorResponseDescriptor instead')
const InspectActorResponse$json = {
  '1': 'InspectActorResponse',
  '2': [
    {
      '1': 'actor',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorObservation',
      '10': 'actor'
    },
  ],
};

/// Descriptor for `InspectActorResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectActorResponseDescriptor = $convert.base64Decode(
    'ChRJbnNwZWN0QWN0b3JSZXNwb25zZRI5CgVhY3RvchgBIAEoCzIjLmFjeWNsaWMuYWN0b3JzLn'
    'YxLkFjdG9yT2JzZXJ2YXRpb25SBWFjdG9y');

@$core.Deprecated('Use addSubscriptionRequestDescriptor instead')
const AddSubscriptionRequest$json = {
  '1': 'AddSubscriptionRequest',
  '2': [
    {'1': 'actor_id', '3': 1, '4': 1, '5': 9, '10': 'actorId'},
    {
      '1': 'subscription',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.SubscriptionSpec',
      '10': 'subscription'
    },
    {'1': 'idempotency_key', '3': 3, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `AddSubscriptionRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List addSubscriptionRequestDescriptor = $convert.base64Decode(
    'ChZBZGRTdWJzY3JpcHRpb25SZXF1ZXN0EhkKCGFjdG9yX2lkGAEgASgJUgdhY3RvcklkEkcKDH'
    'N1YnNjcmlwdGlvbhgCIAEoCzIjLmFjeWNsaWMuYWN0b3JzLnYxLlN1YnNjcmlwdGlvblNwZWNS'
    'DHN1YnNjcmlwdGlvbhInCg9pZGVtcG90ZW5jeV9rZXkYAyABKAlSDmlkZW1wb3RlbmN5S2V5');

@$core.Deprecated('Use addSubscriptionResponseDescriptor instead')
const AddSubscriptionResponse$json = {
  '1': 'AddSubscriptionResponse',
  '2': [
    {
      '1': 'actor',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorObservation',
      '10': 'actor'
    },
  ],
};

/// Descriptor for `AddSubscriptionResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List addSubscriptionResponseDescriptor =
    $convert.base64Decode(
        'ChdBZGRTdWJzY3JpcHRpb25SZXNwb25zZRI5CgVhY3RvchgBIAEoCzIjLmFjeWNsaWMuYWN0b3'
        'JzLnYxLkFjdG9yT2JzZXJ2YXRpb25SBWFjdG9y');

@$core.Deprecated('Use removeSubscriptionRequestDescriptor instead')
const RemoveSubscriptionRequest$json = {
  '1': 'RemoveSubscriptionRequest',
  '2': [
    {'1': 'actor_id', '3': 1, '4': 1, '5': 9, '10': 'actorId'},
    {'1': 'subscription_id', '3': 2, '4': 1, '5': 9, '10': 'subscriptionId'},
    {'1': 'idempotency_key', '3': 3, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `RemoveSubscriptionRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List removeSubscriptionRequestDescriptor = $convert.base64Decode(
    'ChlSZW1vdmVTdWJzY3JpcHRpb25SZXF1ZXN0EhkKCGFjdG9yX2lkGAEgASgJUgdhY3RvcklkEi'
    'cKD3N1YnNjcmlwdGlvbl9pZBgCIAEoCVIOc3Vic2NyaXB0aW9uSWQSJwoPaWRlbXBvdGVuY3lf'
    'a2V5GAMgASgJUg5pZGVtcG90ZW5jeUtleQ==');

@$core.Deprecated('Use removeSubscriptionResponseDescriptor instead')
const RemoveSubscriptionResponse$json = {
  '1': 'RemoveSubscriptionResponse',
  '2': [
    {
      '1': 'actor',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorObservation',
      '10': 'actor'
    },
  ],
};

/// Descriptor for `RemoveSubscriptionResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List removeSubscriptionResponseDescriptor =
    $convert.base64Decode(
        'ChpSZW1vdmVTdWJzY3JpcHRpb25SZXNwb25zZRI5CgVhY3RvchgBIAEoCzIjLmFjeWNsaWMuYW'
        'N0b3JzLnYxLkFjdG9yT2JzZXJ2YXRpb25SBWFjdG9y');

@$core.Deprecated('Use resumeSubscriptionRequestDescriptor instead')
const ResumeSubscriptionRequest$json = {
  '1': 'ResumeSubscriptionRequest',
  '2': [
    {'1': 'actor_id', '3': 1, '4': 1, '5': 9, '10': 'actorId'},
    {'1': 'subscription_id', '3': 2, '4': 1, '5': 9, '10': 'subscriptionId'},
    {'1': 'idempotency_key', '3': 3, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `ResumeSubscriptionRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List resumeSubscriptionRequestDescriptor = $convert.base64Decode(
    'ChlSZXN1bWVTdWJzY3JpcHRpb25SZXF1ZXN0EhkKCGFjdG9yX2lkGAEgASgJUgdhY3RvcklkEi'
    'cKD3N1YnNjcmlwdGlvbl9pZBgCIAEoCVIOc3Vic2NyaXB0aW9uSWQSJwoPaWRlbXBvdGVuY3lf'
    'a2V5GAMgASgJUg5pZGVtcG90ZW5jeUtleQ==');

@$core.Deprecated('Use resumeSubscriptionResponseDescriptor instead')
const ResumeSubscriptionResponse$json = {
  '1': 'ResumeSubscriptionResponse',
  '2': [
    {
      '1': 'actor',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorObservation',
      '10': 'actor'
    },
  ],
};

/// Descriptor for `ResumeSubscriptionResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List resumeSubscriptionResponseDescriptor =
    $convert.base64Decode(
        'ChpSZXN1bWVTdWJzY3JpcHRpb25SZXNwb25zZRI5CgVhY3RvchgBIAEoCzIjLmFjeWNsaWMuYW'
        'N0b3JzLnYxLkFjdG9yT2JzZXJ2YXRpb25SBWFjdG9y');

@$core.Deprecated('Use checkpointActorRequestDescriptor instead')
const CheckpointActorRequest$json = {
  '1': 'CheckpointActorRequest',
  '2': [
    {'1': 'actor_id', '3': 1, '4': 1, '5': 9, '10': 'actorId'},
    {'1': 'idempotency_key', '3': 2, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `CheckpointActorRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkpointActorRequestDescriptor =
    $convert.base64Decode(
        'ChZDaGVja3BvaW50QWN0b3JSZXF1ZXN0EhkKCGFjdG9yX2lkGAEgASgJUgdhY3RvcklkEicKD2'
        'lkZW1wb3RlbmN5X2tleRgCIAEoCVIOaWRlbXBvdGVuY3lLZXk=');

@$core.Deprecated('Use checkpointActorResponseDescriptor instead')
const CheckpointActorResponse$json = {
  '1': 'CheckpointActorResponse',
  '2': [
    {
      '1': 'actor',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.actors.v1.ActorObservation',
      '10': 'actor'
    },
  ],
};

/// Descriptor for `CheckpointActorResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkpointActorResponseDescriptor =
    $convert.base64Decode(
        'ChdDaGVja3BvaW50QWN0b3JSZXNwb25zZRI5CgVhY3RvchgBIAEoCzIjLmFjeWNsaWMuYWN0b3'
        'JzLnYxLkFjdG9yT2JzZXJ2YXRpb25SBWFjdG9y');

@$core.Deprecated('Use headerDescriptor instead')
const Header$json = {
  '1': 'Header',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'value', '3': 2, '4': 1, '5': 9, '10': 'value'},
  ],
};

/// Descriptor for `Header`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List headerDescriptor = $convert.base64Decode(
    'CgZIZWFkZXISEgoEbmFtZRgBIAEoCVIEbmFtZRIUCgV2YWx1ZRgCIAEoCVIFdmFsdWU=');

@$core.Deprecated('Use invokeActorRequestDescriptor instead')
const InvokeActorRequest$json = {
  '1': 'InvokeActorRequest',
  '2': [
    {'1': 'actor_id', '3': 1, '4': 1, '5': 9, '10': 'actorId'},
    {'1': 'method', '3': 2, '4': 1, '5': 9, '10': 'method'},
    {'1': 'url', '3': 3, '4': 1, '5': 9, '10': 'url'},
    {'1': 'body', '3': 4, '4': 1, '5': 12, '10': 'body'},
    {
      '1': 'headers',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.actors.v1.Header',
      '10': 'headers'
    },
  ],
};

/// Descriptor for `InvokeActorRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List invokeActorRequestDescriptor = $convert.base64Decode(
    'ChJJbnZva2VBY3RvclJlcXVlc3QSGQoIYWN0b3JfaWQYASABKAlSB2FjdG9ySWQSFgoGbWV0aG'
    '9kGAIgASgJUgZtZXRob2QSEAoDdXJsGAMgASgJUgN1cmwSEgoEYm9keRgEIAEoDFIEYm9keRIz'
    'CgdoZWFkZXJzGAUgAygLMhkuYWN5Y2xpYy5hY3RvcnMudjEuSGVhZGVyUgdoZWFkZXJz');

@$core.Deprecated('Use invokeActorResponseDescriptor instead')
const InvokeActorResponse$json = {
  '1': 'InvokeActorResponse',
  '2': [
    {'1': 'status', '3': 1, '4': 1, '5': 13, '10': 'status'},
    {'1': 'body', '3': 2, '4': 1, '5': 12, '10': 'body'},
    {
      '1': 'headers',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.actors.v1.Header',
      '10': 'headers'
    },
  ],
};

/// Descriptor for `InvokeActorResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List invokeActorResponseDescriptor = $convert.base64Decode(
    'ChNJbnZva2VBY3RvclJlc3BvbnNlEhYKBnN0YXR1cxgBIAEoDVIGc3RhdHVzEhIKBGJvZHkYAi'
    'ABKAxSBGJvZHkSMwoHaGVhZGVycxgDIAMoCzIZLmFjeWNsaWMuYWN0b3JzLnYxLkhlYWRlclIH'
    'aGVhZGVycw==');

@$core.Deprecated('Use errorDescriptor instead')
const Error$json = {
  '1': 'Error',
  '2': [
    {
      '1': 'code',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.actors.v1.ErrorCode',
      '10': 'code'
    },
    {'1': 'message', '3': 2, '4': 1, '5': 9, '10': 'message'},
  ],
};

/// Descriptor for `Error`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List errorDescriptor = $convert.base64Decode(
    'CgVFcnJvchIwCgRjb2RlGAEgASgOMhwuYWN5Y2xpYy5hY3RvcnMudjEuRXJyb3JDb2RlUgRjb2'
    'RlEhgKB21lc3NhZ2UYAiABKAlSB21lc3NhZ2U=');
