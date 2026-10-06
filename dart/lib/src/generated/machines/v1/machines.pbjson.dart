// This is a generated file - do not edit.
//
// Generated from machines/v1/machines.proto.

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

@$core.Deprecated('Use imageKindDescriptor instead')
const ImageKind$json = {
  '1': 'ImageKind',
  '2': [
    {'1': 'IMAGE_KIND_UNSPECIFIED', '2': 0},
    {'1': 'IMAGE_KIND_MANAGED_OCI', '2': 1},
    {'1': 'IMAGE_KIND_CUSTOM', '2': 2},
    {'1': 'IMAGE_KIND_CHECKPOINT', '2': 3},
  ],
};

/// Descriptor for `ImageKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List imageKindDescriptor = $convert.base64Decode(
    'CglJbWFnZUtpbmQSGgoWSU1BR0VfS0lORF9VTlNQRUNJRklFRBAAEhoKFklNQUdFX0tJTkRfTU'
    'FOQUdFRF9PQ0kQARIVChFJTUFHRV9LSU5EX0NVU1RPTRACEhkKFUlNQUdFX0tJTkRfQ0hFQ0tQ'
    'T0lOVBAD');

@$core.Deprecated('Use capabilityDescriptor instead')
const Capability$json = {
  '1': 'Capability',
  '2': [
    {'1': 'CAPABILITY_UNSPECIFIED', '2': 0},
    {'1': 'CAPABILITY_ELASTIC_CPU', '2': 1},
    {'1': 'CAPABILITY_ELASTIC_MEMORY', '2': 2},
    {'1': 'CAPABILITY_LIVE_CHECKPOINT', '2': 3},
    {'1': 'CAPABILITY_LIVE_FORK', '2': 4},
    {'1': 'CAPABILITY_SUSPEND_RESUME', '2': 5},
    {'1': 'CAPABILITY_LIVE_MOVEMENT', '2': 6},
    {'1': 'CAPABILITY_DISK_FORK', '2': 7},
  ],
};

/// Descriptor for `Capability`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List capabilityDescriptor = $convert.base64Decode(
    'CgpDYXBhYmlsaXR5EhoKFkNBUEFCSUxJVFlfVU5TUEVDSUZJRUQQABIaChZDQVBBQklMSVRZX0'
    'VMQVNUSUNfQ1BVEAESHQoZQ0FQQUJJTElUWV9FTEFTVElDX01FTU9SWRACEh4KGkNBUEFCSUxJ'
    'VFlfTElWRV9DSEVDS1BPSU5UEAMSGAoUQ0FQQUJJTElUWV9MSVZFX0ZPUksQBBIdChlDQVBBQk'
    'lMSVRZX1NVU1BFTkRfUkVTVU1FEAUSHAoYQ0FQQUJJTElUWV9MSVZFX01PVkVNRU5UEAYSGAoU'
    'Q0FQQUJJTElUWV9ESVNLX0ZPUksQBw==');

@$core.Deprecated('Use compatibilityModeDescriptor instead')
const CompatibilityMode$json = {
  '1': 'CompatibilityMode',
  '2': [
    {'1': 'COMPATIBILITY_MODE_UNSPECIFIED', '2': 0},
    {'1': 'COMPATIBILITY_MODE_BEST_EFFORT', '2': 1},
    {'1': 'COMPATIBILITY_MODE_REQUIRE', '2': 2},
  ],
};

/// Descriptor for `CompatibilityMode`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List compatibilityModeDescriptor = $convert.base64Decode(
    'ChFDb21wYXRpYmlsaXR5TW9kZRIiCh5DT01QQVRJQklMSVRZX01PREVfVU5TUEVDSUZJRUQQAB'
    'IiCh5DT01QQVRJQklMSVRZX01PREVfQkVTVF9FRkZPUlQQARIeChpDT01QQVRJQklMSVRZX01P'
    'REVfUkVRVUlSRRAC');

@$core.Deprecated('Use expirationKindDescriptor instead')
const ExpirationKind$json = {
  '1': 'ExpirationKind',
  '2': [
    {'1': 'EXPIRATION_KIND_UNSPECIFIED', '2': 0},
    {'1': 'EXPIRATION_KIND_NEVER', '2': 1},
    {'1': 'EXPIRATION_KIND_MAX_AGE', '2': 2},
    {'1': 'EXPIRATION_KIND_AT', '2': 3},
    {'1': 'EXPIRATION_KIND_IDLE', '2': 4},
  ],
};

/// Descriptor for `ExpirationKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List expirationKindDescriptor = $convert.base64Decode(
    'Cg5FeHBpcmF0aW9uS2luZBIfChtFWFBJUkFUSU9OX0tJTkRfVU5TUEVDSUZJRUQQABIZChVFWF'
    'BJUkFUSU9OX0tJTkRfTkVWRVIQARIbChdFWFBJUkFUSU9OX0tJTkRfTUFYX0FHRRACEhYKEkVY'
    'UElSQVRJT05fS0lORF9BVBADEhgKFEVYUElSQVRJT05fS0lORF9JRExFEAQ=');

@$core.Deprecated('Use operationStatusDescriptor instead')
const OperationStatus$json = {
  '1': 'OperationStatus',
  '2': [
    {'1': 'OPERATION_STATUS_UNSPECIFIED', '2': 0},
    {'1': 'OPERATION_STATUS_PENDING', '2': 1},
    {'1': 'OPERATION_STATUS_SUCCEEDED', '2': 2},
    {'1': 'OPERATION_STATUS_CANCELLED', '2': 3},
    {'1': 'OPERATION_STATUS_INDETERMINATE', '2': 4},
    {'1': 'OPERATION_STATUS_FAILED', '2': 5},
  ],
};

/// Descriptor for `OperationStatus`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List operationStatusDescriptor = $convert.base64Decode(
    'Cg9PcGVyYXRpb25TdGF0dXMSIAocT1BFUkFUSU9OX1NUQVRVU19VTlNQRUNJRklFRBAAEhwKGE'
    '9QRVJBVElPTl9TVEFUVVNfUEVORElORxABEh4KGk9QRVJBVElPTl9TVEFUVVNfU1VDQ0VFREVE'
    'EAISHgoaT1BFUkFUSU9OX1NUQVRVU19DQU5DRUxMRUQQAxIiCh5PUEVSQVRJT05fU1RBVFVTX0'
    'lOREVURVJNSU5BVEUQBBIbChdPUEVSQVRJT05fU1RBVFVTX0ZBSUxFRBAF');

@$core.Deprecated('Use machineStatusDescriptor instead')
const MachineStatus$json = {
  '1': 'MachineStatus',
  '2': [
    {'1': 'MACHINE_STATUS_UNSPECIFIED', '2': 0},
    {'1': 'MACHINE_STATUS_STARTING', '2': 1},
    {'1': 'MACHINE_STATUS_RUNNING', '2': 2},
    {'1': 'MACHINE_STATUS_SUSPENDING', '2': 3},
    {'1': 'MACHINE_STATUS_SUSPENDED', '2': 4},
    {'1': 'MACHINE_STATUS_WAKING', '2': 5},
    {'1': 'MACHINE_STATUS_DESTROYING', '2': 6},
    {'1': 'MACHINE_STATUS_DESTROYED', '2': 7},
    {'1': 'MACHINE_STATUS_FAILED', '2': 8},
    {'1': 'MACHINE_STATUS_INDETERMINATE', '2': 9},
  ],
};

/// Descriptor for `MachineStatus`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List machineStatusDescriptor = $convert.base64Decode(
    'Cg1NYWNoaW5lU3RhdHVzEh4KGk1BQ0hJTkVfU1RBVFVTX1VOU1BFQ0lGSUVEEAASGwoXTUFDSE'
    'lORV9TVEFUVVNfU1RBUlRJTkcQARIaChZNQUNISU5FX1NUQVRVU19SVU5OSU5HEAISHQoZTUFD'
    'SElORV9TVEFUVVNfU1VTUEVORElORxADEhwKGE1BQ0hJTkVfU1RBVFVTX1NVU1BFTkRFRBAEEh'
    'kKFU1BQ0hJTkVfU1RBVFVTX1dBS0lORxAFEh0KGU1BQ0hJTkVfU1RBVFVTX0RFU1RST1lJTkcQ'
    'BhIcChhNQUNISU5FX1NUQVRVU19ERVNUUk9ZRUQQBxIZChVNQUNISU5FX1NUQVRVU19GQUlMRU'
    'QQCBIgChxNQUNISU5FX1NUQVRVU19JTkRFVEVSTUlOQVRFEAk=');

@$core.Deprecated('Use forkFidelityDescriptor instead')
const ForkFidelity$json = {
  '1': 'ForkFidelity',
  '2': [
    {'1': 'FORK_FIDELITY_UNSPECIFIED', '2': 0},
    {'1': 'FORK_FIDELITY_MEMORY_AND_DISK', '2': 1},
    {'1': 'FORK_FIDELITY_DISK_ONLY', '2': 2},
  ],
};

/// Descriptor for `ForkFidelity`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List forkFidelityDescriptor = $convert.base64Decode(
    'CgxGb3JrRmlkZWxpdHkSHQoZRk9SS19GSURFTElUWV9VTlNQRUNJRklFRBAAEiEKHUZPUktfRk'
    'lERUxJVFlfTUVNT1JZX0FORF9ESVNLEAESGwoXRk9SS19GSURFTElUWV9ESVNLX09OTFkQAg==');

@$core.Deprecated('Use pressureKindDescriptor instead')
const PressureKind$json = {
  '1': 'PressureKind',
  '2': [
    {'1': 'PRESSURE_KIND_UNSPECIFIED', '2': 0},
    {'1': 'PRESSURE_KIND_CUSTOMER_BUDGET', '2': 1},
    {'1': 'PRESSURE_KIND_MACHINE_LIMIT', '2': 2},
    {'1': 'PRESSURE_KIND_SERVICE_SATURATION', '2': 3},
  ],
};

/// Descriptor for `PressureKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List pressureKindDescriptor = $convert.base64Decode(
    'CgxQcmVzc3VyZUtpbmQSHQoZUFJFU1NVUkVfS0lORF9VTlNQRUNJRklFRBAAEiEKHVBSRVNTVV'
    'JFX0tJTkRfQ1VTVE9NRVJfQlVER0VUEAESHwobUFJFU1NVUkVfS0lORF9NQUNISU5FX0xJTUlU'
    'EAISJAogUFJFU1NVUkVfS0lORF9TRVJWSUNFX1NBVFVSQVRJT04QAw==');

@$core.Deprecated('Use eventKindDescriptor instead')
const EventKind$json = {
  '1': 'EventKind',
  '2': [
    {'1': 'EVENT_KIND_UNSPECIFIED', '2': 0},
    {'1': 'EVENT_KIND_STATE', '2': 1},
    {'1': 'EVENT_KIND_PRESSURE', '2': 2},
    {'1': 'EVENT_KIND_CAPACITY', '2': 3},
  ],
};

/// Descriptor for `EventKind`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List eventKindDescriptor = $convert.base64Decode(
    'CglFdmVudEtpbmQSGgoWRVZFTlRfS0lORF9VTlNQRUNJRklFRBAAEhQKEEVWRU5UX0tJTkRfU1'
    'RBVEUQARIXChNFVkVOVF9LSU5EX1BSRVNTVVJFEAISFwoTRVZFTlRfS0lORF9DQVBBQ0lUWRAD');

@$core.Deprecated('Use protocolVersionDescriptor instead')
const ProtocolVersion$json = {
  '1': 'ProtocolVersion',
  '2': [
    {'1': 'major', '3': 1, '4': 1, '5': 13, '10': 'major'},
    {'1': 'minor', '3': 2, '4': 1, '5': 13, '10': 'minor'},
  ],
};

/// Descriptor for `ProtocolVersion`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List protocolVersionDescriptor = $convert.base64Decode(
    'Cg9Qcm90b2NvbFZlcnNpb24SFAoFbWFqb3IYASABKA1SBW1ham9yEhQKBW1pbm9yGAIgASgNUg'
    'VtaW5vcg==');

@$core.Deprecated('Use operationIdDescriptor instead')
const OperationId$json = {
  '1': 'OperationId',
  '2': [
    {'1': 'value', '3': 1, '4': 1, '5': 12, '10': 'value'},
  ],
};

/// Descriptor for `OperationId`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List operationIdDescriptor =
    $convert.base64Decode('CgtPcGVyYXRpb25JZBIUCgV2YWx1ZRgBIAEoDFIFdmFsdWU=');

@$core.Deprecated('Use idempotencyKeyDescriptor instead')
const IdempotencyKey$json = {
  '1': 'IdempotencyKey',
  '2': [
    {'1': 'value', '3': 1, '4': 1, '5': 12, '10': 'value'},
  ],
};

/// Descriptor for `IdempotencyKey`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List idempotencyKeyDescriptor = $convert
    .base64Decode('Cg5JZGVtcG90ZW5jeUtleRIUCgV2YWx1ZRgBIAEoDFIFdmFsdWU=');

@$core.Deprecated('Use machineIdDescriptor instead')
const MachineId$json = {
  '1': 'MachineId',
  '2': [
    {'1': 'value', '3': 1, '4': 1, '5': 12, '10': 'value'},
  ],
};

/// Descriptor for `MachineId`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machineIdDescriptor =
    $convert.base64Decode('CglNYWNoaW5lSWQSFAoFdmFsdWUYASABKAxSBXZhbHVl');

@$core.Deprecated('Use checkpointIdDescriptor instead')
const CheckpointId$json = {
  '1': 'CheckpointId',
  '2': [
    {'1': 'value', '3': 1, '4': 1, '5': 12, '10': 'value'},
  ],
};

/// Descriptor for `CheckpointId`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkpointIdDescriptor =
    $convert.base64Decode('CgxDaGVja3BvaW50SWQSFAoFdmFsdWUYASABKAxSBXZhbHVl');

@$core.Deprecated('Use imageDescriptor instead')
const Image$json = {
  '1': 'Image',
  '2': [
    {
      '1': 'kind',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.ImageKind',
      '10': 'kind'
    },
    {
      '1': 'managed_digest',
      '3': 2,
      '4': 1,
      '5': 12,
      '9': 0,
      '10': 'managedDigest'
    },
    {
      '1': 'custom_digest',
      '3': 3,
      '4': 1,
      '5': 12,
      '9': 0,
      '10': 'customDigest'
    },
    {
      '1': 'checkpoint',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '9': 0,
      '10': 'checkpoint'
    },
  ],
  '8': [
    {'1': 'immutable_reference'},
  ],
};

/// Descriptor for `Image`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List imageDescriptor = $convert.base64Decode(
    'CgVJbWFnZRIyCgRraW5kGAEgASgOMh4uYWN5Y2xpYy5tYWNoaW5lcy52MS5JbWFnZUtpbmRSBG'
    'tpbmQSJwoObWFuYWdlZF9kaWdlc3QYAiABKAxIAFINbWFuYWdlZERpZ2VzdBIlCg1jdXN0b21f'
    'ZGlnZXN0GAMgASgMSABSDGN1c3RvbURpZ2VzdBJDCgpjaGVja3BvaW50GAQgASgLMiEuYWN5Y2'
    'xpYy5tYWNoaW5lcy52MS5DaGVja3BvaW50SWRIAFIKY2hlY2twb2ludEIVChNpbW11dGFibGVf'
    'cmVmZXJlbmNl');

@$core.Deprecated('Use compatibilityPolicyDescriptor instead')
const CompatibilityPolicy$json = {
  '1': 'CompatibilityPolicy',
  '2': [
    {
      '1': 'mode',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.CompatibilityMode',
      '10': 'mode'
    },
    {
      '1': 'required',
      '3': 2,
      '4': 3,
      '5': 14,
      '6': '.acyclic.machines.v1.Capability',
      '10': 'required'
    },
  ],
};

/// Descriptor for `CompatibilityPolicy`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List compatibilityPolicyDescriptor = $convert.base64Decode(
    'ChNDb21wYXRpYmlsaXR5UG9saWN5EjoKBG1vZGUYASABKA4yJi5hY3ljbGljLm1hY2hpbmVzLn'
    'YxLkNvbXBhdGliaWxpdHlNb2RlUgRtb2RlEjsKCHJlcXVpcmVkGAIgAygOMh8uYWN5Y2xpYy5t'
    'YWNoaW5lcy52MS5DYXBhYmlsaXR5UghyZXF1aXJlZA==');

@$core.Deprecated('Use imageQualificationDescriptor instead')
const ImageQualification$json = {
  '1': 'ImageQualification',
  '2': [
    {
      '1': 'image',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.Image',
      '10': 'image'
    },
    {
      '1': 'capabilities',
      '3': 2,
      '4': 3,
      '5': 14,
      '6': '.acyclic.machines.v1.Capability',
      '10': 'capabilities'
    },
    {
      '1': 'compatibility_revision',
      '3': 3,
      '4': 1,
      '5': 12,
      '10': 'compatibilityRevision'
    },
  ],
};

/// Descriptor for `ImageQualification`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List imageQualificationDescriptor = $convert.base64Decode(
    'ChJJbWFnZVF1YWxpZmljYXRpb24SMAoFaW1hZ2UYASABKAsyGi5hY3ljbGljLm1hY2hpbmVzLn'
    'YxLkltYWdlUgVpbWFnZRJDCgxjYXBhYmlsaXRpZXMYAiADKA4yHy5hY3ljbGljLm1hY2hpbmVz'
    'LnYxLkNhcGFiaWxpdHlSDGNhcGFiaWxpdGllcxI1ChZjb21wYXRpYmlsaXR5X3JldmlzaW9uGA'
    'MgASgMUhVjb21wYXRpYmlsaXR5UmV2aXNpb24=');

@$core.Deprecated('Use suspensionPolicyDescriptor instead')
const SuspensionPolicy$json = {
  '1': 'SuspensionPolicy',
  '2': [
    {'1': 'manual', '3': 1, '4': 1, '5': 8, '9': 0, '10': 'manual'},
    {'1': 'after_idle_ms', '3': 2, '4': 1, '5': 4, '9': 0, '10': 'afterIdleMs'},
  ],
  '8': [
    {'1': 'policy'},
  ],
};

/// Descriptor for `SuspensionPolicy`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List suspensionPolicyDescriptor = $convert.base64Decode(
    'ChBTdXNwZW5zaW9uUG9saWN5EhgKBm1hbnVhbBgBIAEoCEgAUgZtYW51YWwSJAoNYWZ0ZXJfaW'
    'RsZV9tcxgCIAEoBEgAUgthZnRlcklkbGVNc0IICgZwb2xpY3k=');

@$core.Deprecated('Use expirationPolicyDescriptor instead')
const ExpirationPolicy$json = {
  '1': 'ExpirationPolicy',
  '2': [
    {
      '1': 'kind',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.ExpirationKind',
      '10': 'kind'
    },
    {'1': 'value_ms', '3': 2, '4': 1, '5': 4, '10': 'valueMs'},
  ],
};

/// Descriptor for `ExpirationPolicy`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List expirationPolicyDescriptor = $convert.base64Decode(
    'ChBFeHBpcmF0aW9uUG9saWN5EjcKBGtpbmQYASABKA4yIy5hY3ljbGljLm1hY2hpbmVzLnYxLk'
    'V4cGlyYXRpb25LaW5kUgRraW5kEhkKCHZhbHVlX21zGAIgASgEUgd2YWx1ZU1z');

@$core.Deprecated('Use budgetsDescriptor instead')
const Budgets$json = {
  '1': 'Budgets',
  '2': [
    {'1': 'spend_micros', '3': 1, '4': 1, '5': 4, '10': 'spendMicros'},
    {'1': 'concurrency', '3': 2, '4': 1, '5': 13, '10': 'concurrency'},
  ],
};

/// Descriptor for `Budgets`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List budgetsDescriptor = $convert.base64Decode(
    'CgdCdWRnZXRzEiEKDHNwZW5kX21pY3JvcxgBIAEoBFILc3BlbmRNaWNyb3MSIAoLY29uY3Vycm'
    'VuY3kYAiABKA1SC2NvbmN1cnJlbmN5');

@$core.Deprecated('Use machineContractDescriptor instead')
const MachineContract$json = {
  '1': 'MachineContract',
  '2': [
    {
      '1': 'image',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.Image',
      '10': 'image'
    },
    {
      '1': 'capabilities',
      '3': 2,
      '4': 3,
      '5': 14,
      '6': '.acyclic.machines.v1.Capability',
      '10': 'capabilities'
    },
    {
      '1': 'compatibility',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CompatibilityPolicy',
      '10': 'compatibility'
    },
    {
      '1': 'compatibility_revision',
      '3': 4,
      '4': 1,
      '5': 12,
      '10': 'compatibilityRevision'
    },
    {
      '1': 'suspension',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.SuspensionPolicy',
      '10': 'suspension'
    },
    {
      '1': 'expiration',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ExpirationPolicy',
      '10': 'expiration'
    },
    {
      '1': 'network_policy_digest',
      '3': 8,
      '4': 1,
      '5': 12,
      '10': 'networkPolicyDigest'
    },
    {
      '1': 'budgets',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.Budgets',
      '10': 'budgets'
    },
  ],
  '9': [
    {'1': 5, '2': 6},
  ],
  '10': ['performance'],
};

/// Descriptor for `MachineContract`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machineContractDescriptor = $convert.base64Decode(
    'Cg9NYWNoaW5lQ29udHJhY3QSMAoFaW1hZ2UYASABKAsyGi5hY3ljbGljLm1hY2hpbmVzLnYxLk'
    'ltYWdlUgVpbWFnZRJDCgxjYXBhYmlsaXRpZXMYAiADKA4yHy5hY3ljbGljLm1hY2hpbmVzLnYx'
    'LkNhcGFiaWxpdHlSDGNhcGFiaWxpdGllcxJOCg1jb21wYXRpYmlsaXR5GAMgASgLMiguYWN5Y2'
    'xpYy5tYWNoaW5lcy52MS5Db21wYXRpYmlsaXR5UG9saWN5Ug1jb21wYXRpYmlsaXR5EjUKFmNv'
    'bXBhdGliaWxpdHlfcmV2aXNpb24YBCABKAxSFWNvbXBhdGliaWxpdHlSZXZpc2lvbhJFCgpzdX'
    'NwZW5zaW9uGAYgASgLMiUuYWN5Y2xpYy5tYWNoaW5lcy52MS5TdXNwZW5zaW9uUG9saWN5Ugpz'
    'dXNwZW5zaW9uEkUKCmV4cGlyYXRpb24YByABKAsyJS5hY3ljbGljLm1hY2hpbmVzLnYxLkV4cG'
    'lyYXRpb25Qb2xpY3lSCmV4cGlyYXRpb24SMgoVbmV0d29ya19wb2xpY3lfZGlnZXN0GAggASgM'
    'UhNuZXR3b3JrUG9saWN5RGlnZXN0EjYKB2J1ZGdldHMYCSABKAsyHC5hY3ljbGljLm1hY2hpbm'
    'VzLnYxLkJ1ZGdldHNSB2J1ZGdldHNKBAgFEAZSC3BlcmZvcm1hbmNl');

@$core.Deprecated('Use qualifyImageRequestDescriptor instead')
const QualifyImageRequest$json = {
  '1': 'QualifyImageRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'image',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.Image',
      '10': 'image'
    },
  ],
};

/// Descriptor for `QualifyImageRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List qualifyImageRequestDescriptor = $convert.base64Decode(
    'ChNRdWFsaWZ5SW1hZ2VSZXF1ZXN0EkAKCHByb3RvY29sGAEgASgLMiQuYWN5Y2xpYy5tYWNoaW'
    '5lcy52MS5Qcm90b2NvbFZlcnNpb25SCHByb3RvY29sEjAKBWltYWdlGAIgASgLMhouYWN5Y2xp'
    'Yy5tYWNoaW5lcy52MS5JbWFnZVIFaW1hZ2U=');

@$core.Deprecated('Use createMachineRequestDescriptor instead')
const CreateMachineRequest$json = {
  '1': 'CreateMachineRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'idempotency_key',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.IdempotencyKey',
      '10': 'idempotencyKey'
    },
    {
      '1': 'image',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.Image',
      '10': 'image'
    },
    {
      '1': 'compatibility',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CompatibilityPolicy',
      '10': 'compatibility'
    },
    {
      '1': 'suspension',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.SuspensionPolicy',
      '10': 'suspension'
    },
    {
      '1': 'expiration',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ExpirationPolicy',
      '10': 'expiration'
    },
    {
      '1': 'network_policy_digest',
      '3': 8,
      '4': 1,
      '5': 12,
      '10': 'networkPolicyDigest'
    },
    {
      '1': 'budgets',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.Budgets',
      '10': 'budgets'
    },
  ],
  '9': [
    {'1': 5, '2': 6},
  ],
  '10': ['performance'],
};

/// Descriptor for `CreateMachineRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createMachineRequestDescriptor = $convert.base64Decode(
    'ChRDcmVhdGVNYWNoaW5lUmVxdWVzdBJACghwcm90b2NvbBgBIAEoCzIkLmFjeWNsaWMubWFjaG'
    'luZXMudjEuUHJvdG9jb2xWZXJzaW9uUghwcm90b2NvbBJMCg9pZGVtcG90ZW5jeV9rZXkYAiAB'
    'KAsyIy5hY3ljbGljLm1hY2hpbmVzLnYxLklkZW1wb3RlbmN5S2V5Ug5pZGVtcG90ZW5jeUtleR'
    'IwCgVpbWFnZRgDIAEoCzIaLmFjeWNsaWMubWFjaGluZXMudjEuSW1hZ2VSBWltYWdlEk4KDWNv'
    'bXBhdGliaWxpdHkYBCABKAsyKC5hY3ljbGljLm1hY2hpbmVzLnYxLkNvbXBhdGliaWxpdHlQb2'
    'xpY3lSDWNvbXBhdGliaWxpdHkSRQoKc3VzcGVuc2lvbhgGIAEoCzIlLmFjeWNsaWMubWFjaGlu'
    'ZXMudjEuU3VzcGVuc2lvblBvbGljeVIKc3VzcGVuc2lvbhJFCgpleHBpcmF0aW9uGAcgASgLMi'
    'UuYWN5Y2xpYy5tYWNoaW5lcy52MS5FeHBpcmF0aW9uUG9saWN5UgpleHBpcmF0aW9uEjIKFW5l'
    'dHdvcmtfcG9saWN5X2RpZ2VzdBgIIAEoDFITbmV0d29ya1BvbGljeURpZ2VzdBI2CgdidWRnZX'
    'RzGAkgASgLMhwuYWN5Y2xpYy5tYWNoaW5lcy52MS5CdWRnZXRzUgdidWRnZXRzSgQIBRAGUgtw'
    'ZXJmb3JtYW5jZQ==');

@$core.Deprecated('Use machineMutationRequestDescriptor instead')
const MachineMutationRequest$json = {
  '1': 'MachineMutationRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'idempotency_key',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.IdempotencyKey',
      '10': 'idempotencyKey'
    },
    {
      '1': 'machine',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
  ],
};

/// Descriptor for `MachineMutationRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machineMutationRequestDescriptor = $convert.base64Decode(
    'ChZNYWNoaW5lTXV0YXRpb25SZXF1ZXN0EkAKCHByb3RvY29sGAEgASgLMiQuYWN5Y2xpYy5tYW'
    'NoaW5lcy52MS5Qcm90b2NvbFZlcnNpb25SCHByb3RvY29sEkwKD2lkZW1wb3RlbmN5X2tleRgC'
    'IAEoCzIjLmFjeWNsaWMubWFjaGluZXMudjEuSWRlbXBvdGVuY3lLZXlSDmlkZW1wb3RlbmN5S2'
    'V5EjgKB21hY2hpbmUYAyABKAsyHi5hY3ljbGljLm1hY2hpbmVzLnYxLk1hY2hpbmVJZFIHbWFj'
    'aGluZQ==');

@$core.Deprecated('Use checkpointMachineRequestDescriptor instead')
const CheckpointMachineRequest$json = {
  '1': 'CheckpointMachineRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'idempotency_key',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.IdempotencyKey',
      '10': 'idempotencyKey'
    },
    {
      '1': 'machine',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
  ],
};

/// Descriptor for `CheckpointMachineRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkpointMachineRequestDescriptor = $convert.base64Decode(
    'ChhDaGVja3BvaW50TWFjaGluZVJlcXVlc3QSQAoIcHJvdG9jb2wYASABKAsyJC5hY3ljbGljLm'
    '1hY2hpbmVzLnYxLlByb3RvY29sVmVyc2lvblIIcHJvdG9jb2wSTAoPaWRlbXBvdGVuY3lfa2V5'
    'GAIgASgLMiMuYWN5Y2xpYy5tYWNoaW5lcy52MS5JZGVtcG90ZW5jeUtleVIOaWRlbXBvdGVuY3'
    'lLZXkSOAoHbWFjaGluZRgDIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudjEuTWFjaGluZUlkUgdt'
    'YWNoaW5l');

@$core.Deprecated('Use forkCheckpointRequestDescriptor instead')
const ForkCheckpointRequest$json = {
  '1': 'ForkCheckpointRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'idempotency_key',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.IdempotencyKey',
      '10': 'idempotencyKey'
    },
    {
      '1': 'checkpoint',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '10': 'checkpoint'
    },
    {'1': 'count', '3': 4, '4': 1, '5': 13, '10': 'count'},
  ],
  '9': [
    {'1': 5, '2': 6},
  ],
  '10': ['performance'],
};

/// Descriptor for `ForkCheckpointRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkCheckpointRequestDescriptor = $convert.base64Decode(
    'ChVGb3JrQ2hlY2twb2ludFJlcXVlc3QSQAoIcHJvdG9jb2wYASABKAsyJC5hY3ljbGljLm1hY2'
    'hpbmVzLnYxLlByb3RvY29sVmVyc2lvblIIcHJvdG9jb2wSTAoPaWRlbXBvdGVuY3lfa2V5GAIg'
    'ASgLMiMuYWN5Y2xpYy5tYWNoaW5lcy52MS5JZGVtcG90ZW5jeUtleVIOaWRlbXBvdGVuY3lLZX'
    'kSQQoKY2hlY2twb2ludBgDIAEoCzIhLmFjeWNsaWMubWFjaGluZXMudjEuQ2hlY2twb2ludElk'
    'UgpjaGVja3BvaW50EhQKBWNvdW50GAQgASgNUgVjb3VudEoECAUQBlILcGVyZm9ybWFuY2U=');

@$core.Deprecated('Use forkMachineRequestDescriptor instead')
const ForkMachineRequest$json = {
  '1': 'ForkMachineRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'idempotency_key',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.IdempotencyKey',
      '10': 'idempotencyKey'
    },
    {
      '1': 'machine',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {'1': 'count', '3': 4, '4': 1, '5': 13, '10': 'count'},
  ],
};

/// Descriptor for `ForkMachineRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkMachineRequestDescriptor = $convert.base64Decode(
    'ChJGb3JrTWFjaGluZVJlcXVlc3QSQAoIcHJvdG9jb2wYASABKAsyJC5hY3ljbGljLm1hY2hpbm'
    'VzLnYxLlByb3RvY29sVmVyc2lvblIIcHJvdG9jb2wSTAoPaWRlbXBvdGVuY3lfa2V5GAIgASgL'
    'MiMuYWN5Y2xpYy5tYWNoaW5lcy52MS5JZGVtcG90ZW5jeUtleVIOaWRlbXBvdGVuY3lLZXkSOA'
    'oHbWFjaGluZRgDIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudjEuTWFjaGluZUlkUgdtYWNoaW5l'
    'EhQKBWNvdW50GAQgASgNUgVjb3VudA==');

@$core.Deprecated('Use setSuspensionPolicyRequestDescriptor instead')
const SetSuspensionPolicyRequest$json = {
  '1': 'SetSuspensionPolicyRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'idempotency_key',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.IdempotencyKey',
      '10': 'idempotencyKey'
    },
    {
      '1': 'machine',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {
      '1': 'policy',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.SuspensionPolicy',
      '10': 'policy'
    },
  ],
};

/// Descriptor for `SetSuspensionPolicyRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List setSuspensionPolicyRequestDescriptor = $convert.base64Decode(
    'ChpTZXRTdXNwZW5zaW9uUG9saWN5UmVxdWVzdBJACghwcm90b2NvbBgBIAEoCzIkLmFjeWNsaW'
    'MubWFjaGluZXMudjEuUHJvdG9jb2xWZXJzaW9uUghwcm90b2NvbBJMCg9pZGVtcG90ZW5jeV9r'
    'ZXkYAiABKAsyIy5hY3ljbGljLm1hY2hpbmVzLnYxLklkZW1wb3RlbmN5S2V5Ug5pZGVtcG90ZW'
    '5jeUtleRI4CgdtYWNoaW5lGAMgASgLMh4uYWN5Y2xpYy5tYWNoaW5lcy52MS5NYWNoaW5lSWRS'
    'B21hY2hpbmUSPQoGcG9saWN5GAQgASgLMiUuYWN5Y2xpYy5tYWNoaW5lcy52MS5TdXNwZW5zaW'
    '9uUG9saWN5UgZwb2xpY3k=');

@$core.Deprecated('Use checkpointMutationRequestDescriptor instead')
const CheckpointMutationRequest$json = {
  '1': 'CheckpointMutationRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'idempotency_key',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.IdempotencyKey',
      '10': 'idempotencyKey'
    },
    {
      '1': 'checkpoint',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '10': 'checkpoint'
    },
  ],
};

/// Descriptor for `CheckpointMutationRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkpointMutationRequestDescriptor = $convert.base64Decode(
    'ChlDaGVja3BvaW50TXV0YXRpb25SZXF1ZXN0EkAKCHByb3RvY29sGAEgASgLMiQuYWN5Y2xpYy'
    '5tYWNoaW5lcy52MS5Qcm90b2NvbFZlcnNpb25SCHByb3RvY29sEkwKD2lkZW1wb3RlbmN5X2tl'
    'eRgCIAEoCzIjLmFjeWNsaWMubWFjaGluZXMudjEuSWRlbXBvdGVuY3lLZXlSDmlkZW1wb3Rlbm'
    'N5S2V5EkEKCmNoZWNrcG9pbnQYAyABKAsyIS5hY3ljbGljLm1hY2hpbmVzLnYxLkNoZWNrcG9p'
    'bnRJZFIKY2hlY2twb2ludA==');

@$core.Deprecated('Use recoverRequestDescriptor instead')
const RecoverRequest$json = {
  '1': 'RecoverRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'idempotency_key',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.IdempotencyKey',
      '10': 'idempotencyKey'
    },
  ],
};

/// Descriptor for `RecoverRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List recoverRequestDescriptor = $convert.base64Decode(
    'Cg5SZWNvdmVyUmVxdWVzdBJACghwcm90b2NvbBgBIAEoCzIkLmFjeWNsaWMubWFjaGluZXMudj'
    'EuUHJvdG9jb2xWZXJzaW9uUghwcm90b2NvbBJMCg9pZGVtcG90ZW5jeV9rZXkYAiABKAsyIy5h'
    'Y3ljbGljLm1hY2hpbmVzLnYxLklkZW1wb3RlbmN5S2V5Ug5pZGVtcG90ZW5jeUtleQ==');

@$core.Deprecated('Use inspectMachineRequestDescriptor instead')
const InspectMachineRequest$json = {
  '1': 'InspectMachineRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'machine',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
  ],
};

/// Descriptor for `InspectMachineRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectMachineRequestDescriptor = $convert.base64Decode(
    'ChVJbnNwZWN0TWFjaGluZVJlcXVlc3QSQAoIcHJvdG9jb2wYASABKAsyJC5hY3ljbGljLm1hY2'
    'hpbmVzLnYxLlByb3RvY29sVmVyc2lvblIIcHJvdG9jb2wSOAoHbWFjaGluZRgCIAEoCzIeLmFj'
    'eWNsaWMubWFjaGluZXMudjEuTWFjaGluZUlkUgdtYWNoaW5l');

@$core.Deprecated('Use inspectCheckpointRequestDescriptor instead')
const InspectCheckpointRequest$json = {
  '1': 'InspectCheckpointRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'checkpoint',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '10': 'checkpoint'
    },
  ],
};

/// Descriptor for `InspectCheckpointRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectCheckpointRequestDescriptor = $convert.base64Decode(
    'ChhJbnNwZWN0Q2hlY2twb2ludFJlcXVlc3QSQAoIcHJvdG9jb2wYASABKAsyJC5hY3ljbGljLm'
    '1hY2hpbmVzLnYxLlByb3RvY29sVmVyc2lvblIIcHJvdG9jb2wSQQoKY2hlY2twb2ludBgCIAEo'
    'CzIhLmFjeWNsaWMubWFjaGluZXMudjEuQ2hlY2twb2ludElkUgpjaGVja3BvaW50');

@$core.Deprecated('Use listMachinesRequestDescriptor instead')
const ListMachinesRequest$json = {
  '1': 'ListMachinesRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'after',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'after'
    },
    {'1': 'limit', '3': 3, '4': 1, '5': 13, '10': 'limit'},
  ],
};

/// Descriptor for `ListMachinesRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listMachinesRequestDescriptor = $convert.base64Decode(
    'ChNMaXN0TWFjaGluZXNSZXF1ZXN0EkAKCHByb3RvY29sGAEgASgLMiQuYWN5Y2xpYy5tYWNoaW'
    '5lcy52MS5Qcm90b2NvbFZlcnNpb25SCHByb3RvY29sEjQKBWFmdGVyGAIgASgLMh4uYWN5Y2xp'
    'Yy5tYWNoaW5lcy52MS5NYWNoaW5lSWRSBWFmdGVyEhQKBWxpbWl0GAMgASgNUgVsaW1pdA==');

@$core.Deprecated('Use operationRequestDescriptor instead')
const OperationRequest$json = {
  '1': 'OperationRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'operation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
  ],
};

/// Descriptor for `OperationRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List operationRequestDescriptor = $convert.base64Decode(
    'ChBPcGVyYXRpb25SZXF1ZXN0EkAKCHByb3RvY29sGAEgASgLMiQuYWN5Y2xpYy5tYWNoaW5lcy'
    '52MS5Qcm90b2NvbFZlcnNpb25SCHByb3RvY29sEj4KCW9wZXJhdGlvbhgCIAEoCzIgLmFjeWNs'
    'aWMubWFjaGluZXMudjEuT3BlcmF0aW9uSWRSCW9wZXJhdGlvbg==');

@$core.Deprecated('Use operationStateDescriptor instead')
const OperationState$json = {
  '1': 'OperationState',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
    {
      '1': 'status',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.OperationStatus',
      '10': 'status'
    },
  ],
};

/// Descriptor for `OperationState`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List operationStateDescriptor = $convert.base64Decode(
    'Cg5PcGVyYXRpb25TdGF0ZRI+CglvcGVyYXRpb24YASABKAsyIC5hY3ljbGljLm1hY2hpbmVzLn'
    'YxLk9wZXJhdGlvbklkUglvcGVyYXRpb24SPAoGc3RhdHVzGAIgASgOMiQuYWN5Y2xpYy5tYWNo'
    'aW5lcy52MS5PcGVyYXRpb25TdGF0dXNSBnN0YXR1cw==');

@$core.Deprecated('Use endpointDescriptor instead')
const Endpoint$json = {
  '1': 'Endpoint',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {'1': 'uri', '3': 2, '4': 1, '5': 9, '10': 'uri'},
  ],
};

/// Descriptor for `Endpoint`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List endpointDescriptor = $convert.base64Decode(
    'CghFbmRwb2ludBISCgRuYW1lGAEgASgJUgRuYW1lEhAKA3VyaRgCIAEoCVIDdXJp');

@$core.Deprecated('Use machineStateDescriptor instead')
const MachineState$json = {
  '1': 'MachineState',
  '2': [
    {
      '1': 'machine',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {
      '1': 'status',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.MachineStatus',
      '10': 'status'
    },
    {
      '1': 'contract',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineContract',
      '10': 'contract'
    },
    {
      '1': 'endpoints',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.machines.v1.Endpoint',
      '10': 'endpoints'
    },
    {
      '1': 'last_checkpoint',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '10': 'lastCheckpoint'
    },
    {
      '1': 'created_at_unix_ms',
      '3': 6,
      '4': 1,
      '5': 4,
      '10': 'createdAtUnixMs'
    },
    {
      '1': 'changed_at_unix_ms',
      '3': 7,
      '4': 1,
      '5': 4,
      '10': 'changedAtUnixMs'
    },
  ],
};

/// Descriptor for `MachineState`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machineStateDescriptor = $convert.base64Decode(
    'CgxNYWNoaW5lU3RhdGUSOAoHbWFjaGluZRgBIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudjEuTW'
    'FjaGluZUlkUgdtYWNoaW5lEjoKBnN0YXR1cxgCIAEoDjIiLmFjeWNsaWMubWFjaGluZXMudjEu'
    'TWFjaGluZVN0YXR1c1IGc3RhdHVzEkAKCGNvbnRyYWN0GAMgASgLMiQuYWN5Y2xpYy5tYWNoaW'
    '5lcy52MS5NYWNoaW5lQ29udHJhY3RSCGNvbnRyYWN0EjsKCWVuZHBvaW50cxgEIAMoCzIdLmFj'
    'eWNsaWMubWFjaGluZXMudjEuRW5kcG9pbnRSCWVuZHBvaW50cxJKCg9sYXN0X2NoZWNrcG9pbn'
    'QYBSABKAsyIS5hY3ljbGljLm1hY2hpbmVzLnYxLkNoZWNrcG9pbnRJZFIObGFzdENoZWNrcG9p'
    'bnQSKwoSY3JlYXRlZF9hdF91bml4X21zGAYgASgEUg9jcmVhdGVkQXRVbml4TXMSKwoSY2hhbm'
    'dlZF9hdF91bml4X21zGAcgASgEUg9jaGFuZ2VkQXRVbml4TXM=');

@$core.Deprecated('Use machinePageDescriptor instead')
const MachinePage$json = {
  '1': 'MachinePage',
  '2': [
    {
      '1': 'machines',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineState',
      '10': 'machines'
    },
    {
      '1': 'next',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'next'
    },
  ],
};

/// Descriptor for `MachinePage`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machinePageDescriptor = $convert.base64Decode(
    'CgtNYWNoaW5lUGFnZRI9CghtYWNoaW5lcxgBIAMoCzIhLmFjeWNsaWMubWFjaGluZXMudjEuTW'
    'FjaGluZVN0YXRlUghtYWNoaW5lcxIyCgRuZXh0GAIgASgLMh4uYWN5Y2xpYy5tYWNoaW5lcy52'
    'MS5NYWNoaW5lSWRSBG5leHQ=');

@$core.Deprecated('Use checkpointStateDescriptor instead')
const CheckpointState$json = {
  '1': 'CheckpointState',
  '2': [
    {
      '1': 'checkpoint',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '10': 'checkpoint'
    },
    {
      '1': 'source',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'source'
    },
    {
      '1': 'contract',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineContract',
      '10': 'contract'
    },
    {'1': 'forkable', '3': 4, '4': 1, '5': 8, '10': 'forkable'},
    {
      '1': 'created_at_unix_ms',
      '3': 5,
      '4': 1,
      '5': 4,
      '10': 'createdAtUnixMs'
    },
  ],
};

/// Descriptor for `CheckpointState`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkpointStateDescriptor = $convert.base64Decode(
    'Cg9DaGVja3BvaW50U3RhdGUSQQoKY2hlY2twb2ludBgBIAEoCzIhLmFjeWNsaWMubWFjaGluZX'
    'MudjEuQ2hlY2twb2ludElkUgpjaGVja3BvaW50EjYKBnNvdXJjZRgCIAEoCzIeLmFjeWNsaWMu'
    'bWFjaGluZXMudjEuTWFjaGluZUlkUgZzb3VyY2USQAoIY29udHJhY3QYAyABKAsyJC5hY3ljbG'
    'ljLm1hY2hpbmVzLnYxLk1hY2hpbmVDb250cmFjdFIIY29udHJhY3QSGgoIZm9ya2FibGUYBCAB'
    'KAhSCGZvcmthYmxlEisKEmNyZWF0ZWRfYXRfdW5peF9tcxgFIAEoBFIPY3JlYXRlZEF0VW5peE'
    '1z');

@$core.Deprecated('Use machineAdmissionDescriptor instead')
const MachineAdmission$json = {
  '1': 'MachineAdmission',
  '2': [
    {
      '1': 'machine',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {
      '1': 'operation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
    {
      '1': 'contract',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineContract',
      '10': 'contract'
    },
  ],
};

/// Descriptor for `MachineAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machineAdmissionDescriptor = $convert.base64Decode(
    'ChBNYWNoaW5lQWRtaXNzaW9uEjgKB21hY2hpbmUYASABKAsyHi5hY3ljbGljLm1hY2hpbmVzLn'
    'YxLk1hY2hpbmVJZFIHbWFjaGluZRI+CglvcGVyYXRpb24YAiABKAsyIC5hY3ljbGljLm1hY2hp'
    'bmVzLnYxLk9wZXJhdGlvbklkUglvcGVyYXRpb24SQAoIY29udHJhY3QYAyABKAsyJC5hY3ljbG'
    'ljLm1hY2hpbmVzLnYxLk1hY2hpbmVDb250cmFjdFIIY29udHJhY3Q=');

@$core.Deprecated('Use checkpointAdmissionDescriptor instead')
const CheckpointAdmission$json = {
  '1': 'CheckpointAdmission',
  '2': [
    {
      '1': 'checkpoint',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '10': 'checkpoint'
    },
    {
      '1': 'source',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'source'
    },
    {
      '1': 'operation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
    {
      '1': 'contract',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineContract',
      '10': 'contract'
    },
  ],
};

/// Descriptor for `CheckpointAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List checkpointAdmissionDescriptor = $convert.base64Decode(
    'ChNDaGVja3BvaW50QWRtaXNzaW9uEkEKCmNoZWNrcG9pbnQYASABKAsyIS5hY3ljbGljLm1hY2'
    'hpbmVzLnYxLkNoZWNrcG9pbnRJZFIKY2hlY2twb2ludBI2CgZzb3VyY2UYAiABKAsyHi5hY3lj'
    'bGljLm1hY2hpbmVzLnYxLk1hY2hpbmVJZFIGc291cmNlEj4KCW9wZXJhdGlvbhgDIAEoCzIgLm'
    'FjeWNsaWMubWFjaGluZXMudjEuT3BlcmF0aW9uSWRSCW9wZXJhdGlvbhJACghjb250cmFjdBgE'
    'IAEoCzIkLmFjeWNsaWMubWFjaGluZXMudjEuTWFjaGluZUNvbnRyYWN0Ughjb250cmFjdA==');

@$core.Deprecated('Use forkAdmissionDescriptor instead')
const ForkAdmission$json = {
  '1': 'ForkAdmission',
  '2': [
    {
      '1': 'checkpoint',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '10': 'checkpoint'
    },
    {
      '1': 'children',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'children'
    },
    {
      '1': 'operation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
    {
      '1': 'contract',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineContract',
      '10': 'contract'
    },
  ],
};

/// Descriptor for `ForkAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkAdmissionDescriptor = $convert.base64Decode(
    'Cg1Gb3JrQWRtaXNzaW9uEkEKCmNoZWNrcG9pbnQYASABKAsyIS5hY3ljbGljLm1hY2hpbmVzLn'
    'YxLkNoZWNrcG9pbnRJZFIKY2hlY2twb2ludBI6CghjaGlsZHJlbhgCIAMoCzIeLmFjeWNsaWMu'
    'bWFjaGluZXMudjEuTWFjaGluZUlkUghjaGlsZHJlbhI+CglvcGVyYXRpb24YAyABKAsyIC5hY3'
    'ljbGljLm1hY2hpbmVzLnYxLk9wZXJhdGlvbklkUglvcGVyYXRpb24SQAoIY29udHJhY3QYBCAB'
    'KAsyJC5hY3ljbGljLm1hY2hpbmVzLnYxLk1hY2hpbmVDb250cmFjdFIIY29udHJhY3Q=');

@$core.Deprecated('Use forkMachineAdmissionDescriptor instead')
const ForkMachineAdmission$json = {
  '1': 'ForkMachineAdmission',
  '2': [
    {
      '1': 'source',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'source'
    },
    {
      '1': 'children',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'children'
    },
    {
      '1': 'operation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
    {
      '1': 'contract',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineContract',
      '10': 'contract'
    },
    {
      '1': 'fidelity',
      '3': 5,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.ForkFidelity',
      '10': 'fidelity'
    },
  ],
};

/// Descriptor for `ForkMachineAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkMachineAdmissionDescriptor = $convert.base64Decode(
    'ChRGb3JrTWFjaGluZUFkbWlzc2lvbhI2CgZzb3VyY2UYASABKAsyHi5hY3ljbGljLm1hY2hpbm'
    'VzLnYxLk1hY2hpbmVJZFIGc291cmNlEjoKCGNoaWxkcmVuGAIgAygLMh4uYWN5Y2xpYy5tYWNo'
    'aW5lcy52MS5NYWNoaW5lSWRSCGNoaWxkcmVuEj4KCW9wZXJhdGlvbhgDIAEoCzIgLmFjeWNsaW'
    'MubWFjaGluZXMudjEuT3BlcmF0aW9uSWRSCW9wZXJhdGlvbhJACghjb250cmFjdBgEIAEoCzIk'
    'LmFjeWNsaWMubWFjaGluZXMudjEuTWFjaGluZUNvbnRyYWN0Ughjb250cmFjdBI9CghmaWRlbG'
    'l0eRgFIAEoDjIhLmFjeWNsaWMubWFjaGluZXMudjEuRm9ya0ZpZGVsaXR5UghmaWRlbGl0eQ==');

@$core.Deprecated('Use policyAdmissionDescriptor instead')
const PolicyAdmission$json = {
  '1': 'PolicyAdmission',
  '2': [
    {
      '1': 'machine',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {
      '1': 'operation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
    {
      '1': 'policy',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.SuspensionPolicy',
      '10': 'policy'
    },
  ],
};

/// Descriptor for `PolicyAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List policyAdmissionDescriptor = $convert.base64Decode(
    'Cg9Qb2xpY3lBZG1pc3Npb24SOAoHbWFjaGluZRgBIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudj'
    'EuTWFjaGluZUlkUgdtYWNoaW5lEj4KCW9wZXJhdGlvbhgCIAEoCzIgLmFjeWNsaWMubWFjaGlu'
    'ZXMudjEuT3BlcmF0aW9uSWRSCW9wZXJhdGlvbhI9CgZwb2xpY3kYAyABKAsyJS5hY3ljbGljLm'
    '1hY2hpbmVzLnYxLlN1c3BlbnNpb25Qb2xpY3lSBnBvbGljeQ==');

@$core.Deprecated('Use mutationAdmissionDescriptor instead')
const MutationAdmission$json = {
  '1': 'MutationAdmission',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
    {
      '1': 'machine',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {
      '1': 'checkpoint',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '10': 'checkpoint'
    },
  ],
};

/// Descriptor for `MutationAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List mutationAdmissionDescriptor = $convert.base64Decode(
    'ChFNdXRhdGlvbkFkbWlzc2lvbhI+CglvcGVyYXRpb24YASABKAsyIC5hY3ljbGljLm1hY2hpbm'
    'VzLnYxLk9wZXJhdGlvbklkUglvcGVyYXRpb24SOAoHbWFjaGluZRgCIAEoCzIeLmFjeWNsaWMu'
    'bWFjaGluZXMudjEuTWFjaGluZUlkUgdtYWNoaW5lEkEKCmNoZWNrcG9pbnQYAyABKAsyIS5hY3'
    'ljbGljLm1hY2hpbmVzLnYxLkNoZWNrcG9pbnRJZFIKY2hlY2twb2ludA==');

@$core.Deprecated('Use recoveredAdmissionDescriptor instead')
const RecoveredAdmission$json = {
  '1': 'RecoveredAdmission',
  '2': [
    {
      '1': 'operation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationId',
      '10': 'operation'
    },
    {
      '1': 'create',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineAdmission',
      '9': 0,
      '10': 'create'
    },
    {
      '1': 'checkpoint',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointAdmission',
      '9': 0,
      '10': 'checkpoint'
    },
    {
      '1': 'fork',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ForkAdmission',
      '9': 0,
      '10': 'fork'
    },
    {
      '1': 'suspend',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MutationAdmission',
      '9': 0,
      '10': 'suspend'
    },
    {
      '1': 'wake',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MutationAdmission',
      '9': 0,
      '10': 'wake'
    },
    {
      '1': 'destroy_machine',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MutationAdmission',
      '9': 0,
      '10': 'destroyMachine'
    },
    {
      '1': 'set_suspension_policy',
      '3': 8,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.PolicyAdmission',
      '9': 0,
      '10': 'setSuspensionPolicy'
    },
    {
      '1': 'destroy_checkpoint',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MutationAdmission',
      '9': 0,
      '10': 'destroyCheckpoint'
    },
    {
      '1': 'fork_machine',
      '3': 10,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ForkMachineAdmission',
      '9': 0,
      '10': 'forkMachine'
    },
  ],
  '8': [
    {'1': 'result'},
  ],
};

/// Descriptor for `RecoveredAdmission`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List recoveredAdmissionDescriptor = $convert.base64Decode(
    'ChJSZWNvdmVyZWRBZG1pc3Npb24SPgoJb3BlcmF0aW9uGAEgASgLMiAuYWN5Y2xpYy5tYWNoaW'
    '5lcy52MS5PcGVyYXRpb25JZFIJb3BlcmF0aW9uEj8KBmNyZWF0ZRgCIAEoCzIlLmFjeWNsaWMu'
    'bWFjaGluZXMudjEuTWFjaGluZUFkbWlzc2lvbkgAUgZjcmVhdGUSSgoKY2hlY2twb2ludBgDIA'
    'EoCzIoLmFjeWNsaWMubWFjaGluZXMudjEuQ2hlY2twb2ludEFkbWlzc2lvbkgAUgpjaGVja3Bv'
    'aW50EjgKBGZvcmsYBCABKAsyIi5hY3ljbGljLm1hY2hpbmVzLnYxLkZvcmtBZG1pc3Npb25IAF'
    'IEZm9yaxJCCgdzdXNwZW5kGAUgASgLMiYuYWN5Y2xpYy5tYWNoaW5lcy52MS5NdXRhdGlvbkFk'
    'bWlzc2lvbkgAUgdzdXNwZW5kEjwKBHdha2UYBiABKAsyJi5hY3ljbGljLm1hY2hpbmVzLnYxLk'
    '11dGF0aW9uQWRtaXNzaW9uSABSBHdha2USUQoPZGVzdHJveV9tYWNoaW5lGAcgASgLMiYuYWN5'
    'Y2xpYy5tYWNoaW5lcy52MS5NdXRhdGlvbkFkbWlzc2lvbkgAUg5kZXN0cm95TWFjaGluZRJaCh'
    'VzZXRfc3VzcGVuc2lvbl9wb2xpY3kYCCABKAsyJC5hY3ljbGljLm1hY2hpbmVzLnYxLlBvbGlj'
    'eUFkbWlzc2lvbkgAUhNzZXRTdXNwZW5zaW9uUG9saWN5ElcKEmRlc3Ryb3lfY2hlY2twb2ludB'
    'gJIAEoCzImLmFjeWNsaWMubWFjaGluZXMudjEuTXV0YXRpb25BZG1pc3Npb25IAFIRZGVzdHJv'
    'eUNoZWNrcG9pbnQSTgoMZm9ya19tYWNoaW5lGAogASgLMikuYWN5Y2xpYy5tYWNoaW5lcy52MS'
    '5Gb3JrTWFjaGluZUFkbWlzc2lvbkgAUgtmb3JrTWFjaGluZUIICgZyZXN1bHQ=');

@$core.Deprecated('Use forkedMachinesDescriptor instead')
const ForkedMachines$json = {
  '1': 'ForkedMachines',
  '2': [
    {
      '1': 'machines',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineState',
      '10': 'machines'
    },
  ],
};

/// Descriptor for `ForkedMachines`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkedMachinesDescriptor = $convert.base64Decode(
    'Cg5Gb3JrZWRNYWNoaW5lcxI9CghtYWNoaW5lcxgBIAMoCzIhLmFjeWNsaWMubWFjaGluZXMudj'
    'EuTWFjaGluZVN0YXRlUghtYWNoaW5lcw==');

@$core.Deprecated('Use forkedLiveMachinesDescriptor instead')
const ForkedLiveMachines$json = {
  '1': 'ForkedLiveMachines',
  '2': [
    {
      '1': 'source',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'source'
    },
    {
      '1': 'fidelity',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.ForkFidelity',
      '10': 'fidelity'
    },
    {
      '1': 'children',
      '3': 3,
      '4': 3,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineState',
      '10': 'children'
    },
  ],
};

/// Descriptor for `ForkedLiveMachines`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkedLiveMachinesDescriptor = $convert.base64Decode(
    'ChJGb3JrZWRMaXZlTWFjaGluZXMSNgoGc291cmNlGAEgASgLMh4uYWN5Y2xpYy5tYWNoaW5lcy'
    '52MS5NYWNoaW5lSWRSBnNvdXJjZRI9CghmaWRlbGl0eRgCIAEoDjIhLmFjeWNsaWMubWFjaGlu'
    'ZXMudjEuRm9ya0ZpZGVsaXR5UghmaWRlbGl0eRI9CghjaGlsZHJlbhgDIAMoCzIhLmFjeWNsaW'
    'MubWFjaGluZXMudjEuTWFjaGluZVN0YXRlUghjaGlsZHJlbg==');

@$core.Deprecated('Use policySetDescriptor instead')
const PolicySet$json = {
  '1': 'PolicySet',
  '2': [
    {
      '1': 'machine',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {
      '1': 'policy',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.SuspensionPolicy',
      '10': 'policy'
    },
  ],
};

/// Descriptor for `PolicySet`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List policySetDescriptor = $convert.base64Decode(
    'CglQb2xpY3lTZXQSOAoHbWFjaGluZRgBIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudjEuTWFjaG'
    'luZUlkUgdtYWNoaW5lEj0KBnBvbGljeRgCIAEoCzIlLmFjeWNsaWMubWFjaGluZXMudjEuU3Vz'
    'cGVuc2lvblBvbGljeVIGcG9saWN5');

@$core.Deprecated('Use mutationOutcomeDescriptor instead')
const MutationOutcome$json = {
  '1': 'MutationOutcome',
  '2': [
    {
      '1': 'created',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineState',
      '9': 0,
      '10': 'created'
    },
    {
      '1': 'checkpointed',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointState',
      '9': 0,
      '10': 'checkpointed'
    },
    {
      '1': 'forked',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ForkedMachines',
      '9': 0,
      '10': 'forked'
    },
    {
      '1': 'suspended',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '9': 0,
      '10': 'suspended'
    },
    {
      '1': 'woken',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '9': 0,
      '10': 'woken'
    },
    {
      '1': 'suspension_policy_set',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.PolicySet',
      '9': 0,
      '10': 'suspensionPolicySet'
    },
    {
      '1': 'machine_destroyed',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '9': 0,
      '10': 'machineDestroyed'
    },
    {
      '1': 'checkpoint_destroyed',
      '3': 8,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.CheckpointId',
      '9': 0,
      '10': 'checkpointDestroyed'
    },
    {
      '1': 'machine_forked',
      '3': 9,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ForkedLiveMachines',
      '9': 0,
      '10': 'machineForked'
    },
  ],
  '8': [
    {'1': 'result'},
  ],
};

/// Descriptor for `MutationOutcome`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List mutationOutcomeDescriptor = $convert.base64Decode(
    'Cg9NdXRhdGlvbk91dGNvbWUSPQoHY3JlYXRlZBgBIAEoCzIhLmFjeWNsaWMubWFjaGluZXMudj'
    'EuTWFjaGluZVN0YXRlSABSB2NyZWF0ZWQSSgoMY2hlY2twb2ludGVkGAIgASgLMiQuYWN5Y2xp'
    'Yy5tYWNoaW5lcy52MS5DaGVja3BvaW50U3RhdGVIAFIMY2hlY2twb2ludGVkEj0KBmZvcmtlZB'
    'gDIAEoCzIjLmFjeWNsaWMubWFjaGluZXMudjEuRm9ya2VkTWFjaGluZXNIAFIGZm9ya2VkEj4K'
    'CXN1c3BlbmRlZBgEIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudjEuTWFjaGluZUlkSABSCXN1c3'
    'BlbmRlZBI2CgV3b2tlbhgFIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudjEuTWFjaGluZUlkSABS'
    'BXdva2VuElQKFXN1c3BlbnNpb25fcG9saWN5X3NldBgGIAEoCzIeLmFjeWNsaWMubWFjaGluZX'
    'MudjEuUG9saWN5U2V0SABSE3N1c3BlbnNpb25Qb2xpY3lTZXQSTQoRbWFjaGluZV9kZXN0cm95'
    'ZWQYByABKAsyHi5hY3ljbGljLm1hY2hpbmVzLnYxLk1hY2hpbmVJZEgAUhBtYWNoaW5lRGVzdH'
    'JveWVkElYKFGNoZWNrcG9pbnRfZGVzdHJveWVkGAggASgLMiEuYWN5Y2xpYy5tYWNoaW5lcy52'
    'MS5DaGVja3BvaW50SWRIAFITY2hlY2twb2ludERlc3Ryb3llZBJQCg5tYWNoaW5lX2ZvcmtlZB'
    'gJIAEoCzInLmFjeWNsaWMubWFjaGluZXMudjEuRm9ya2VkTGl2ZU1hY2hpbmVzSABSDW1hY2hp'
    'bmVGb3JrZWRCCAoGcmVzdWx0');

@$core.Deprecated('Use operationPageDescriptor instead')
const OperationPage$json = {
  '1': 'OperationPage',
  '2': [
    {
      '1': 'operations',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.machines.v1.OperationState',
      '10': 'operations'
    },
  ],
};

/// Descriptor for `OperationPage`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List operationPageDescriptor = $convert.base64Decode(
    'Cg1PcGVyYXRpb25QYWdlEkMKCm9wZXJhdGlvbnMYASADKAsyIy5hY3ljbGljLm1hY2hpbmVzLn'
    'YxLk9wZXJhdGlvblN0YXRlUgpvcGVyYXRpb25z');

@$core.Deprecated('Use machineEventDescriptor instead')
const MachineEvent$json = {
  '1': 'MachineEvent',
  '2': [
    {
      '1': 'machine',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {'1': 'sequence', '3': 2, '4': 1, '5': 4, '10': 'sequence'},
    {
      '1': 'observed_at_unix_ms',
      '3': 3,
      '4': 1,
      '5': 4,
      '10': 'observedAtUnixMs'
    },
    {
      '1': 'kind',
      '3': 4,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.EventKind',
      '10': 'kind'
    },
    {
      '1': 'state',
      '3': 5,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.MachineStatus',
      '10': 'state'
    },
    {
      '1': 'pressure',
      '3': 6,
      '4': 1,
      '5': 14,
      '6': '.acyclic.machines.v1.PressureKind',
      '10': 'pressure'
    },
  ],
};

/// Descriptor for `MachineEvent`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List machineEventDescriptor = $convert.base64Decode(
    'CgxNYWNoaW5lRXZlbnQSOAoHbWFjaGluZRgBIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudjEuTW'
    'FjaGluZUlkUgdtYWNoaW5lEhoKCHNlcXVlbmNlGAIgASgEUghzZXF1ZW5jZRItChNvYnNlcnZl'
    'ZF9hdF91bml4X21zGAMgASgEUhBvYnNlcnZlZEF0VW5peE1zEjIKBGtpbmQYBCABKA4yHi5hY3'
    'ljbGljLm1hY2hpbmVzLnYxLkV2ZW50S2luZFIEa2luZBI4CgVzdGF0ZRgFIAEoDjIiLmFjeWNs'
    'aWMubWFjaGluZXMudjEuTWFjaGluZVN0YXR1c1IFc3RhdGUSPQoIcHJlc3N1cmUYBiABKA4yIS'
    '5hY3ljbGljLm1hY2hpbmVzLnYxLlByZXNzdXJlS2luZFIIcHJlc3N1cmU=');

@$core.Deprecated('Use eventsRequestDescriptor instead')
const EventsRequest$json = {
  '1': 'EventsRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'machine',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {'1': 'after_sequence', '3': 3, '4': 1, '5': 4, '10': 'afterSequence'},
    {'1': 'limit', '3': 4, '4': 1, '5': 13, '10': 'limit'},
  ],
};

/// Descriptor for `EventsRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List eventsRequestDescriptor = $convert.base64Decode(
    'Cg1FdmVudHNSZXF1ZXN0EkAKCHByb3RvY29sGAEgASgLMiQuYWN5Y2xpYy5tYWNoaW5lcy52MS'
    '5Qcm90b2NvbFZlcnNpb25SCHByb3RvY29sEjgKB21hY2hpbmUYAiABKAsyHi5hY3ljbGljLm1h'
    'Y2hpbmVzLnYxLk1hY2hpbmVJZFIHbWFjaGluZRIlCg5hZnRlcl9zZXF1ZW5jZRgDIAEoBFINYW'
    'Z0ZXJTZXF1ZW5jZRIUCgVsaW1pdBgEIAEoDVIFbGltaXQ=');

@$core.Deprecated('Use eventPageDescriptor instead')
const EventPage$json = {
  '1': 'EventPage',
  '2': [
    {
      '1': 'events',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineEvent',
      '10': 'events'
    },
    {'1': 'next_sequence', '3': 2, '4': 1, '5': 4, '10': 'nextSequence'},
  ],
};

/// Descriptor for `EventPage`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List eventPageDescriptor = $convert.base64Decode(
    'CglFdmVudFBhZ2USOQoGZXZlbnRzGAEgAygLMiEuYWN5Y2xpYy5tYWNoaW5lcy52MS5NYWNoaW'
    '5lRXZlbnRSBmV2ZW50cxIjCg1uZXh0X3NlcXVlbmNlGAIgASgEUgxuZXh0U2VxdWVuY2U=');

@$core.Deprecated('Use usageRequestDescriptor instead')
const UsageRequest$json = {
  '1': 'UsageRequest',
  '2': [
    {
      '1': 'protocol',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.ProtocolVersion',
      '10': 'protocol'
    },
    {
      '1': 'machine',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {'1': 'start_unix_ms', '3': 3, '4': 1, '5': 4, '10': 'startUnixMs'},
    {'1': 'end_unix_ms', '3': 4, '4': 1, '5': 4, '10': 'endUnixMs'},
  ],
};

/// Descriptor for `UsageRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List usageRequestDescriptor = $convert.base64Decode(
    'CgxVc2FnZVJlcXVlc3QSQAoIcHJvdG9jb2wYASABKAsyJC5hY3ljbGljLm1hY2hpbmVzLnYxLl'
    'Byb3RvY29sVmVyc2lvblIIcHJvdG9jb2wSOAoHbWFjaGluZRgCIAEoCzIeLmFjeWNsaWMubWFj'
    'aGluZXMudjEuTWFjaGluZUlkUgdtYWNoaW5lEiIKDXN0YXJ0X3VuaXhfbXMYAyABKARSC3N0YX'
    'J0VW5peE1zEh4KC2VuZF91bml4X21zGAQgASgEUgllbmRVbml4TXM=');

@$core.Deprecated('Use usageReceiptDescriptor instead')
const UsageReceipt$json = {
  '1': 'UsageReceipt',
  '2': [
    {
      '1': 'machine',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.machines.v1.MachineId',
      '10': 'machine'
    },
    {'1': 'start_unix_ms', '3': 2, '4': 1, '5': 4, '10': 'startUnixMs'},
    {'1': 'end_unix_ms', '3': 3, '4': 1, '5': 4, '10': 'endUnixMs'},
    {'1': 'elastic_cpu_ns', '3': 4, '4': 1, '5': 4, '10': 'elasticCpuNs'},
    {'1': 'dedicated_cpu_ns', '3': 5, '4': 1, '5': 4, '10': 'dedicatedCpuNs'},
    {
      '1': 'private_resident_byte_seconds',
      '3': 6,
      '4': 1,
      '5': 4,
      '10': 'privateResidentByteSeconds'
    },
    {
      '1': 'durable_private_bytes',
      '3': 7,
      '4': 1,
      '5': 4,
      '10': 'durablePrivateBytes'
    },
    {
      '1': 'lineage_receipt_sha256',
      '3': 11,
      '4': 1,
      '5': 12,
      '10': 'lineageReceiptSha256'
    },
    {'1': 'egress_bytes', '3': 9, '4': 1, '5': 4, '10': 'egressBytes'},
    {'1': 'receipt', '3': 10, '4': 1, '5': 12, '10': 'receipt'},
  ],
  '9': [
    {'1': 8, '2': 9},
  ],
  '10': ['lineage_shared_bytes'],
};

/// Descriptor for `UsageReceipt`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List usageReceiptDescriptor = $convert.base64Decode(
    'CgxVc2FnZVJlY2VpcHQSOAoHbWFjaGluZRgBIAEoCzIeLmFjeWNsaWMubWFjaGluZXMudjEuTW'
    'FjaGluZUlkUgdtYWNoaW5lEiIKDXN0YXJ0X3VuaXhfbXMYAiABKARSC3N0YXJ0VW5peE1zEh4K'
    'C2VuZF91bml4X21zGAMgASgEUgllbmRVbml4TXMSJAoOZWxhc3RpY19jcHVfbnMYBCABKARSDG'
    'VsYXN0aWNDcHVOcxIoChBkZWRpY2F0ZWRfY3B1X25zGAUgASgEUg5kZWRpY2F0ZWRDcHVOcxJB'
    'Ch1wcml2YXRlX3Jlc2lkZW50X2J5dGVfc2Vjb25kcxgGIAEoBFIacHJpdmF0ZVJlc2lkZW50Qn'
    'l0ZVNlY29uZHMSMgoVZHVyYWJsZV9wcml2YXRlX2J5dGVzGAcgASgEUhNkdXJhYmxlUHJpdmF0'
    'ZUJ5dGVzEjQKFmxpbmVhZ2VfcmVjZWlwdF9zaGEyNTYYCyABKAxSFGxpbmVhZ2VSZWNlaXB0U2'
    'hhMjU2EiEKDGVncmVzc19ieXRlcxgJIAEoBFILZWdyZXNzQnl0ZXMSGAoHcmVjZWlwdBgKIAEo'
    'DFIHcmVjZWlwdEoECAgQCVIUbGluZWFnZV9zaGFyZWRfYnl0ZXM=');
