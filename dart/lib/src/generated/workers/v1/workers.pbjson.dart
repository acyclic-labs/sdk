// This is a generated file - do not edit.
//
// Generated from workers/v1/workers.proto.

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

@$core.Deprecated('Use jobStateDescriptor instead')
const JobState$json = {
  '1': 'JobState',
  '2': [
    {'1': 'JOB_STATE_UNSPECIFIED', '2': 0},
    {'1': 'JOB_STATE_ACCEPTED', '2': 1},
    {'1': 'JOB_STATE_RUNNING', '2': 2},
    {'1': 'JOB_STATE_SUCCEEDED', '2': 3},
    {'1': 'JOB_STATE_FAILED', '2': 4},
    {'1': 'JOB_STATE_CANCELLED', '2': 5},
  ],
};

/// Descriptor for `JobState`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List jobStateDescriptor = $convert.base64Decode(
    'CghKb2JTdGF0ZRIZChVKT0JfU1RBVEVfVU5TUEVDSUZJRUQQABIWChJKT0JfU1RBVEVfQUNDRV'
    'BURUQQARIVChFKT0JfU1RBVEVfUlVOTklORxACEhcKE0pPQl9TVEFURV9TVUNDRUVERUQQAxIU'
    'ChBKT0JfU1RBVEVfRkFJTEVEEAQSFwoTSk9CX1NUQVRFX0NBTkNFTExFRBAF');

@$core.Deprecated('Use errorCodeDescriptor instead')
const ErrorCode$json = {
  '1': 'ErrorCode',
  '2': [
    {'1': 'ERROR_CODE_UNSPECIFIED', '2': 0},
    {'1': 'ERROR_CODE_INVALID_ARGUMENT', '2': 1},
    {'1': 'ERROR_CODE_CAPABILITY_DENIED', '2': 2},
    {'1': 'ERROR_CODE_CAPABILITY_EXPIRED', '2': 3},
    {'1': 'ERROR_CODE_VERSION_NOT_FOUND', '2': 4},
    {'1': 'ERROR_CODE_DEPLOYMENT_NOT_FOUND', '2': 5},
    {'1': 'ERROR_CODE_JOB_NOT_FOUND', '2': 6},
    {'1': 'ERROR_CODE_IDEMPOTENCY_MISMATCH', '2': 7},
    {'1': 'ERROR_CODE_REVISION_CONFLICT', '2': 8},
    {'1': 'ERROR_CODE_OVERLOADED', '2': 9},
    {'1': 'ERROR_CODE_TERMINAL_JOB_FAILURE', '2': 10},
  ],
};

/// Descriptor for `ErrorCode`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List errorCodeDescriptor = $convert.base64Decode(
    'CglFcnJvckNvZGUSGgoWRVJST1JfQ09ERV9VTlNQRUNJRklFRBAAEh8KG0VSUk9SX0NPREVfSU'
    '5WQUxJRF9BUkdVTUVOVBABEiAKHEVSUk9SX0NPREVfQ0FQQUJJTElUWV9ERU5JRUQQAhIhCh1F'
    'UlJPUl9DT0RFX0NBUEFCSUxJVFlfRVhQSVJFRBADEiAKHEVSUk9SX0NPREVfVkVSU0lPTl9OT1'
    'RfRk9VTkQQBBIjCh9FUlJPUl9DT0RFX0RFUExPWU1FTlRfTk9UX0ZPVU5EEAUSHAoYRVJST1Jf'
    'Q09ERV9KT0JfTk9UX0ZPVU5EEAYSIwofRVJST1JfQ09ERV9JREVNUE9URU5DWV9NSVNNQVRDSB'
    'AHEiAKHEVSUk9SX0NPREVfUkVWSVNJT05fQ09ORkxJQ1QQCBIZChVFUlJPUl9DT0RFX09WRVJM'
    'T0FERUQQCRIjCh9FUlJPUl9DT0RFX1RFUk1JTkFMX0pPQl9GQUlMVVJFEAo=');

@$core.Deprecated('Use codeVersionDescriptor instead')
const CodeVersion$json = {
  '1': 'CodeVersion',
  '2': [
    {'1': 'sha256', '3': 1, '4': 1, '5': 12, '10': 'sha256'},
    {'1': 'size_bytes', '3': 2, '4': 1, '5': 4, '10': 'sizeBytes'},
  ],
};

/// Descriptor for `CodeVersion`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List codeVersionDescriptor = $convert.base64Decode(
    'CgtDb2RlVmVyc2lvbhIWCgZzaGEyNTYYASABKAxSBnNoYTI1NhIdCgpzaXplX2J5dGVzGAIgAS'
    'gEUglzaXplQnl0ZXM=');

@$core.Deprecated('Use deploymentDescriptor instead')
const Deployment$json = {
  '1': 'Deployment',
  '2': [
    {'1': 'alias', '3': 1, '4': 1, '5': 9, '10': 'alias'},
    {
      '1': 'version',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.CodeVersion',
      '10': 'version'
    },
    {'1': 'revision', '3': 3, '4': 1, '5': 4, '10': 'revision'},
  ],
};

/// Descriptor for `Deployment`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deploymentDescriptor = $convert.base64Decode(
    'CgpEZXBsb3ltZW50EhQKBWFsaWFzGAEgASgJUgVhbGlhcxI5Cgd2ZXJzaW9uGAIgASgLMh8uYW'
    'N5Y2xpYy53b3JrZXJzLnYxLkNvZGVWZXJzaW9uUgd2ZXJzaW9uEhoKCHJldmlzaW9uGAMgASgE'
    'UghyZXZpc2lvbg==');

@$core.Deprecated('Use publishVersionRequestDescriptor instead')
const PublishVersionRequest$json = {
  '1': 'PublishVersionRequest',
  '2': [
    {
      '1': 'javascript_module',
      '3': 1,
      '4': 1,
      '5': 12,
      '10': 'javascriptModule'
    },
    {'1': 'expected_sha256', '3': 2, '4': 1, '5': 12, '10': 'expectedSha256'},
    {'1': 'idempotency_key', '3': 3, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `PublishVersionRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List publishVersionRequestDescriptor = $convert.base64Decode(
    'ChVQdWJsaXNoVmVyc2lvblJlcXVlc3QSKwoRamF2YXNjcmlwdF9tb2R1bGUYASABKAxSEGphdm'
    'FzY3JpcHRNb2R1bGUSJwoPZXhwZWN0ZWRfc2hhMjU2GAIgASgMUg5leHBlY3RlZFNoYTI1NhIn'
    'Cg9pZGVtcG90ZW5jeV9rZXkYAyABKAlSDmlkZW1wb3RlbmN5S2V5');

@$core.Deprecated('Use publishVersionResponseDescriptor instead')
const PublishVersionResponse$json = {
  '1': 'PublishVersionResponse',
  '2': [
    {
      '1': 'version',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.CodeVersion',
      '10': 'version'
    },
  ],
};

/// Descriptor for `PublishVersionResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List publishVersionResponseDescriptor =
    $convert.base64Decode(
        'ChZQdWJsaXNoVmVyc2lvblJlc3BvbnNlEjkKB3ZlcnNpb24YASABKAsyHy5hY3ljbGljLndvcm'
        'tlcnMudjEuQ29kZVZlcnNpb25SB3ZlcnNpb24=');

@$core.Deprecated('Use selectDeploymentRequestDescriptor instead')
const SelectDeploymentRequest$json = {
  '1': 'SelectDeploymentRequest',
  '2': [
    {'1': 'alias', '3': 1, '4': 1, '5': 9, '10': 'alias'},
    {'1': 'version_sha256', '3': 2, '4': 1, '5': 12, '10': 'versionSha256'},
    {
      '1': 'expected_revision',
      '3': 3,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'expectedRevision',
      '17': true
    },
    {'1': 'idempotency_key', '3': 4, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
  '8': [
    {'1': '_expected_revision'},
  ],
};

/// Descriptor for `SelectDeploymentRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List selectDeploymentRequestDescriptor = $convert.base64Decode(
    'ChdTZWxlY3REZXBsb3ltZW50UmVxdWVzdBIUCgVhbGlhcxgBIAEoCVIFYWxpYXMSJQoOdmVyc2'
    'lvbl9zaGEyNTYYAiABKAxSDXZlcnNpb25TaGEyNTYSMAoRZXhwZWN0ZWRfcmV2aXNpb24YAyAB'
    'KARIAFIQZXhwZWN0ZWRSZXZpc2lvbogBARInCg9pZGVtcG90ZW5jeV9rZXkYBCABKAlSDmlkZW'
    '1wb3RlbmN5S2V5QhQKEl9leHBlY3RlZF9yZXZpc2lvbg==');

@$core.Deprecated('Use selectDeploymentResponseDescriptor instead')
const SelectDeploymentResponse$json = {
  '1': 'SelectDeploymentResponse',
  '2': [
    {
      '1': 'deployment',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.Deployment',
      '10': 'deployment'
    },
  ],
};

/// Descriptor for `SelectDeploymentResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List selectDeploymentResponseDescriptor =
    $convert.base64Decode(
        'ChhTZWxlY3REZXBsb3ltZW50UmVzcG9uc2USPgoKZGVwbG95bWVudBgBIAEoCzIeLmFjeWNsaW'
        'Mud29ya2Vycy52MS5EZXBsb3ltZW50UgpkZXBsb3ltZW50');

@$core.Deprecated('Use objectRefDescriptor instead')
const ObjectRef$json = {
  '1': 'ObjectRef',
  '2': [
    {'1': 'bucket', '3': 1, '4': 1, '5': 9, '10': 'bucket'},
    {'1': 'key', '3': 2, '4': 1, '5': 9, '10': 'key'},
  ],
  '9': [
    {'1': 3, '2': 4},
  ],
  '10': ['version_id'],
};

/// Descriptor for `ObjectRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List objectRefDescriptor = $convert.base64Decode(
    'CglPYmplY3RSZWYSFgoGYnVja2V0GAEgASgJUgZidWNrZXQSEAoDa2V5GAIgASgJUgNrZXlKBA'
    'gDEARSCnZlcnNpb25faWQ=');

@$core.Deprecated('Use payloadDescriptor instead')
const Payload$json = {
  '1': 'Payload',
  '2': [
    {'1': 'inline_bytes', '3': 1, '4': 1, '5': 12, '9': 0, '10': 'inlineBytes'},
    {
      '1': 'object',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.ObjectRef',
      '9': 0,
      '10': 'object'
    },
  ],
  '8': [
    {'1': 'source'},
  ],
};

/// Descriptor for `Payload`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List payloadDescriptor = $convert.base64Decode(
    'CgdQYXlsb2FkEiMKDGlubGluZV9ieXRlcxgBIAEoDEgAUgtpbmxpbmVCeXRlcxI3CgZvYmplY3'
    'QYAiABKAsyHS5hY3ljbGljLndvcmtlcnMudjEuT2JqZWN0UmVmSABSBm9iamVjdEIICgZzb3Vy'
    'Y2U=');

@$core.Deprecated('Use jobResultDescriptor instead')
const JobResult$json = {
  '1': 'JobResult',
  '2': [
    {'1': 'body', '3': 1, '4': 1, '5': 12, '10': 'body'},
  ],
  '9': [
    {'1': 2, '2': 3},
  ],
  '10': ['object_version'],
};

/// Descriptor for `JobResult`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List jobResultDescriptor = $convert.base64Decode(
    'CglKb2JSZXN1bHQSEgoEYm9keRgBIAEoDFIEYm9keUoECAIQA1IOb2JqZWN0X3ZlcnNpb24=');

@$core.Deprecated('Use jobLimitsDescriptor instead')
const JobLimits$json = {
  '1': 'JobLimits',
  '2': [
    {'1': 'timeout_millis', '3': 1, '4': 1, '5': 4, '10': 'timeoutMillis'},
    {'1': 'memory_bytes', '3': 2, '4': 1, '5': 4, '10': 'memoryBytes'},
    {'1': 'output_bytes', '3': 3, '4': 1, '5': 4, '10': 'outputBytes'},
  ],
};

/// Descriptor for `JobLimits`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List jobLimitsDescriptor = $convert.base64Decode(
    'CglKb2JMaW1pdHMSJQoOdGltZW91dF9taWxsaXMYASABKARSDXRpbWVvdXRNaWxsaXMSIQoMbW'
    'Vtb3J5X2J5dGVzGAIgASgEUgttZW1vcnlCeXRlcxIhCgxvdXRwdXRfYnl0ZXMYAyABKARSC291'
    'dHB1dEJ5dGVz');

@$core.Deprecated('Use retryPolicyDescriptor instead')
const RetryPolicy$json = {
  '1': 'RetryPolicy',
  '2': [
    {'1': 'max_attempts', '3': 1, '4': 1, '5': 13, '10': 'maxAttempts'},
    {'1': 'backoff_millis', '3': 2, '4': 1, '5': 4, '10': 'backoffMillis'},
  ],
};

/// Descriptor for `RetryPolicy`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List retryPolicyDescriptor = $convert.base64Decode(
    'CgtSZXRyeVBvbGljeRIhCgxtYXhfYXR0ZW1wdHMYASABKA1SC21heEF0dGVtcHRzEiUKDmJhY2'
    'tvZmZfbWlsbGlzGAIgASgEUg1iYWNrb2ZmTWlsbGlz');

@$core.Deprecated('Use jobTargetDescriptor instead')
const JobTarget$json = {
  '1': 'JobTarget',
  '2': [
    {
      '1': 'deployment_alias',
      '3': 1,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'deploymentAlias'
    },
    {
      '1': 'version_sha256',
      '3': 2,
      '4': 1,
      '5': 12,
      '9': 0,
      '10': 'versionSha256'
    },
  ],
  '8': [
    {'1': 'target'},
  ],
};

/// Descriptor for `JobTarget`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List jobTargetDescriptor = $convert.base64Decode(
    'CglKb2JUYXJnZXQSKwoQZGVwbG95bWVudF9hbGlhcxgBIAEoCUgAUg9kZXBsb3ltZW50QWxpYX'
    'MSJwoOdmVyc2lvbl9zaGEyNTYYAiABKAxIAFINdmVyc2lvblNoYTI1NkIICgZ0YXJnZXQ=');

@$core.Deprecated('Use submitJobRequestDescriptor instead')
const SubmitJobRequest$json = {
  '1': 'SubmitJobRequest',
  '2': [
    {
      '1': 'target',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.JobTarget',
      '10': 'target'
    },
    {
      '1': 'input',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.Payload',
      '10': 'input'
    },
    {
      '1': 'limits',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.JobLimits',
      '10': 'limits'
    },
    {
      '1': 'retry',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.RetryPolicy',
      '10': 'retry'
    },
    {'1': 'idempotency_key', '3': 5, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `SubmitJobRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List submitJobRequestDescriptor = $convert.base64Decode(
    'ChBTdWJtaXRKb2JSZXF1ZXN0EjUKBnRhcmdldBgBIAEoCzIdLmFjeWNsaWMud29ya2Vycy52MS'
    '5Kb2JUYXJnZXRSBnRhcmdldBIxCgVpbnB1dBgCIAEoCzIbLmFjeWNsaWMud29ya2Vycy52MS5Q'
    'YXlsb2FkUgVpbnB1dBI1CgZsaW1pdHMYAyABKAsyHS5hY3ljbGljLndvcmtlcnMudjEuSm9iTG'
    'ltaXRzUgZsaW1pdHMSNQoFcmV0cnkYBCABKAsyHy5hY3ljbGljLndvcmtlcnMudjEuUmV0cnlQ'
    'b2xpY3lSBXJldHJ5EicKD2lkZW1wb3RlbmN5X2tleRgFIAEoCVIOaWRlbXBvdGVuY3lLZXk=');

@$core.Deprecated('Use submitJobResponseDescriptor instead')
const SubmitJobResponse$json = {
  '1': 'SubmitJobResponse',
  '2': [
    {
      '1': 'job',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.JobObservation',
      '10': 'job'
    },
  ],
};

/// Descriptor for `SubmitJobResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List submitJobResponseDescriptor = $convert.base64Decode(
    'ChFTdWJtaXRKb2JSZXNwb25zZRI0CgNqb2IYASABKAsyIi5hY3ljbGljLndvcmtlcnMudjEuSm'
    '9iT2JzZXJ2YXRpb25SA2pvYg==');

@$core.Deprecated('Use jobObservationDescriptor instead')
const JobObservation$json = {
  '1': 'JobObservation',
  '2': [
    {'1': 'job_id', '3': 1, '4': 1, '5': 9, '10': 'jobId'},
    {
      '1': 'state',
      '3': 2,
      '4': 1,
      '5': 14,
      '6': '.acyclic.workers.v1.JobState',
      '10': 'state'
    },
    {'1': 'resolved_sha256', '3': 3, '4': 1, '5': 12, '10': 'resolvedSha256'},
    {'1': 'attempt', '3': 4, '4': 1, '5': 13, '10': 'attempt'},
    {
      '1': 'result',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.JobResult',
      '10': 'result'
    },
    {'1': 'failure_code', '3': 6, '4': 1, '5': 9, '10': 'failureCode'},
    {
      '1': 'cancellation_requested',
      '3': 7,
      '4': 1,
      '5': 8,
      '10': 'cancellationRequested'
    },
  ],
};

/// Descriptor for `JobObservation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List jobObservationDescriptor = $convert.base64Decode(
    'Cg5Kb2JPYnNlcnZhdGlvbhIVCgZqb2JfaWQYASABKAlSBWpvYklkEjIKBXN0YXRlGAIgASgOMh'
    'wuYWN5Y2xpYy53b3JrZXJzLnYxLkpvYlN0YXRlUgVzdGF0ZRInCg9yZXNvbHZlZF9zaGEyNTYY'
    'AyABKAxSDnJlc29sdmVkU2hhMjU2EhgKB2F0dGVtcHQYBCABKA1SB2F0dGVtcHQSNQoGcmVzdW'
    'x0GAUgASgLMh0uYWN5Y2xpYy53b3JrZXJzLnYxLkpvYlJlc3VsdFIGcmVzdWx0EiEKDGZhaWx1'
    'cmVfY29kZRgGIAEoCVILZmFpbHVyZUNvZGUSNQoWY2FuY2VsbGF0aW9uX3JlcXVlc3RlZBgHIA'
    'EoCFIVY2FuY2VsbGF0aW9uUmVxdWVzdGVk');

@$core.Deprecated('Use inspectJobRequestDescriptor instead')
const InspectJobRequest$json = {
  '1': 'InspectJobRequest',
  '2': [
    {'1': 'job_id', '3': 1, '4': 1, '5': 9, '10': 'jobId'},
  ],
};

/// Descriptor for `InspectJobRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectJobRequestDescriptor = $convert
    .base64Decode('ChFJbnNwZWN0Sm9iUmVxdWVzdBIVCgZqb2JfaWQYASABKAlSBWpvYklk');

@$core.Deprecated('Use inspectJobResponseDescriptor instead')
const InspectJobResponse$json = {
  '1': 'InspectJobResponse',
  '2': [
    {
      '1': 'job',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.JobObservation',
      '10': 'job'
    },
  ],
};

/// Descriptor for `InspectJobResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectJobResponseDescriptor = $convert.base64Decode(
    'ChJJbnNwZWN0Sm9iUmVzcG9uc2USNAoDam9iGAEgASgLMiIuYWN5Y2xpYy53b3JrZXJzLnYxLk'
    'pvYk9ic2VydmF0aW9uUgNqb2I=');

@$core.Deprecated('Use cancelJobRequestDescriptor instead')
const CancelJobRequest$json = {
  '1': 'CancelJobRequest',
  '2': [
    {'1': 'job_id', '3': 1, '4': 1, '5': 9, '10': 'jobId'},
    {'1': 'idempotency_key', '3': 2, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `CancelJobRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List cancelJobRequestDescriptor = $convert.base64Decode(
    'ChBDYW5jZWxKb2JSZXF1ZXN0EhUKBmpvYl9pZBgBIAEoCVIFam9iSWQSJwoPaWRlbXBvdGVuY3'
    'lfa2V5GAIgASgJUg5pZGVtcG90ZW5jeUtleQ==');

@$core.Deprecated('Use cancelJobResponseDescriptor instead')
const CancelJobResponse$json = {
  '1': 'CancelJobResponse',
  '2': [
    {
      '1': 'job',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.workers.v1.JobObservation',
      '10': 'job'
    },
  ],
};

/// Descriptor for `CancelJobResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List cancelJobResponseDescriptor = $convert.base64Decode(
    'ChFDYW5jZWxKb2JSZXNwb25zZRI0CgNqb2IYASABKAsyIi5hY3ljbGljLndvcmtlcnMudjEuSm'
    '9iT2JzZXJ2YXRpb25SA2pvYg==');

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

@$core.Deprecated('Use invokeVersionRequestDescriptor instead')
const InvokeVersionRequest$json = {
  '1': 'InvokeVersionRequest',
  '2': [
    {'1': 'version_sha256', '3': 1, '4': 1, '5': 12, '10': 'versionSha256'},
    {'1': 'method', '3': 2, '4': 1, '5': 9, '10': 'method'},
    {'1': 'url', '3': 3, '4': 1, '5': 9, '10': 'url'},
    {
      '1': 'headers',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.workers.v1.Header',
      '10': 'headers'
    },
    {'1': 'body', '3': 5, '4': 1, '5': 12, '10': 'body'},
  ],
};

/// Descriptor for `InvokeVersionRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List invokeVersionRequestDescriptor = $convert.base64Decode(
    'ChRJbnZva2VWZXJzaW9uUmVxdWVzdBIlCg52ZXJzaW9uX3NoYTI1NhgBIAEoDFINdmVyc2lvbl'
    'NoYTI1NhIWCgZtZXRob2QYAiABKAlSBm1ldGhvZBIQCgN1cmwYAyABKAlSA3VybBI0CgdoZWFk'
    'ZXJzGAQgAygLMhouYWN5Y2xpYy53b3JrZXJzLnYxLkhlYWRlclIHaGVhZGVycxISCgRib2R5GA'
    'UgASgMUgRib2R5');

@$core.Deprecated('Use invokeDeploymentRequestDescriptor instead')
const InvokeDeploymentRequest$json = {
  '1': 'InvokeDeploymentRequest',
  '2': [
    {'1': 'alias', '3': 1, '4': 1, '5': 9, '10': 'alias'},
    {'1': 'method', '3': 2, '4': 1, '5': 9, '10': 'method'},
    {'1': 'url', '3': 3, '4': 1, '5': 9, '10': 'url'},
    {
      '1': 'headers',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.workers.v1.Header',
      '10': 'headers'
    },
    {'1': 'body', '3': 5, '4': 1, '5': 12, '10': 'body'},
  ],
};

/// Descriptor for `InvokeDeploymentRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List invokeDeploymentRequestDescriptor = $convert.base64Decode(
    'ChdJbnZva2VEZXBsb3ltZW50UmVxdWVzdBIUCgVhbGlhcxgBIAEoCVIFYWxpYXMSFgoGbWV0aG'
    '9kGAIgASgJUgZtZXRob2QSEAoDdXJsGAMgASgJUgN1cmwSNAoHaGVhZGVycxgEIAMoCzIaLmFj'
    'eWNsaWMud29ya2Vycy52MS5IZWFkZXJSB2hlYWRlcnMSEgoEYm9keRgFIAEoDFIEYm9keQ==');

@$core.Deprecated('Use invokeResponseDescriptor instead')
const InvokeResponse$json = {
  '1': 'InvokeResponse',
  '2': [
    {'1': 'status', '3': 1, '4': 1, '5': 13, '10': 'status'},
    {
      '1': 'headers',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.workers.v1.Header',
      '10': 'headers'
    },
    {'1': 'body', '3': 3, '4': 1, '5': 12, '10': 'body'},
    {'1': 'resolved_sha256', '3': 4, '4': 1, '5': 12, '10': 'resolvedSha256'},
    {
      '1': 'resolved_revision',
      '3': 5,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'resolvedRevision',
      '17': true
    },
  ],
  '8': [
    {'1': '_resolved_revision'},
  ],
};

/// Descriptor for `InvokeResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List invokeResponseDescriptor = $convert.base64Decode(
    'Cg5JbnZva2VSZXNwb25zZRIWCgZzdGF0dXMYASABKA1SBnN0YXR1cxI0CgdoZWFkZXJzGAIgAy'
    'gLMhouYWN5Y2xpYy53b3JrZXJzLnYxLkhlYWRlclIHaGVhZGVycxISCgRib2R5GAMgASgMUgRi'
    'b2R5EicKD3Jlc29sdmVkX3NoYTI1NhgEIAEoDFIOcmVzb2x2ZWRTaGEyNTYSMAoRcmVzb2x2ZW'
    'RfcmV2aXNpb24YBSABKARIAFIQcmVzb2x2ZWRSZXZpc2lvbogBAUIUChJfcmVzb2x2ZWRfcmV2'
    'aXNpb24=');

@$core.Deprecated('Use errorDescriptor instead')
const Error$json = {
  '1': 'Error',
  '2': [
    {
      '1': 'code',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.workers.v1.ErrorCode',
      '10': 'code'
    },
    {'1': 'message', '3': 2, '4': 1, '5': 9, '10': 'message'},
  ],
};

/// Descriptor for `Error`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List errorDescriptor = $convert.base64Decode(
    'CgVFcnJvchIxCgRjb2RlGAEgASgOMh0uYWN5Y2xpYy53b3JrZXJzLnYxLkVycm9yQ29kZVIEY2'
    '9kZRIYCgdtZXNzYWdlGAIgASgJUgdtZXNzYWdl');
