// This is a generated file - do not edit.
//
// Generated from stream/v2/stream.proto.

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

@$core.Deprecated('Use streamLimitDescriptor instead')
const StreamLimit$json = {
  '1': 'StreamLimit',
  '2': [
    {'1': 'STREAM_LIMIT_UNSPECIFIED', '2': 0},
    {'1': 'STREAM_LIMIT_MAX_RECORD_BYTES', '2': 65536},
    {'1': 'STREAM_LIMIT_MAX_ITEMS', '2': 1024},
    {'1': 'STREAM_LIMIT_MAX_COMMAND_BYTES', '2': 1056768},
    {'1': 'STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES', '2': 256},
    {'1': 'STREAM_LIMIT_MAX_PATH_BYTES', '2': 65535},
  ],
};

/// Descriptor for `StreamLimit`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List streamLimitDescriptor = $convert.base64Decode(
    'CgtTdHJlYW1MaW1pdBIcChhTVFJFQU1fTElNSVRfVU5TUEVDSUZJRUQQABIjCh1TVFJFQU1fTE'
    'lNSVRfTUFYX1JFQ09SRF9CWVRFUxCAgAQSGwoWU1RSRUFNX0xJTUlUX01BWF9JVEVNUxCACBIk'
    'Ch5TVFJFQU1fTElNSVRfTUFYX0NPTU1BTkRfQllURVMQgMBAEisKJlNUUkVBTV9MSU1JVF9NQV'
    'hfSURFTVBPVEVOQ1lfS0VZX0JZVEVTEIACEiEKG1NUUkVBTV9MSU1JVF9NQVhfUEFUSF9CWVRF'
    'UxD//wM=');

@$core.Deprecated('Use recordDescriptor instead')
const Record$json = {
  '1': 'Record',
  '2': [
    {'1': 'sequence', '3': 1, '4': 1, '5': 4, '10': 'sequence'},
    {'1': 'value', '3': 2, '4': 1, '5': 12, '10': 'value'},
    {'1': 'commit_id', '3': 3, '4': 1, '5': 12, '10': 'commitId'},
    {
      '1': 'committed_at_micros',
      '3': 4,
      '4': 1,
      '5': 4,
      '10': 'committedAtMicros'
    },
  ],
};

/// Descriptor for `Record`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List recordDescriptor = $convert.base64Decode(
    'CgZSZWNvcmQSGgoIc2VxdWVuY2UYASABKARSCHNlcXVlbmNlEhQKBXZhbHVlGAIgASgMUgV2YW'
    'x1ZRIbCgljb21taXRfaWQYAyABKAxSCGNvbW1pdElkEi4KE2NvbW1pdHRlZF9hdF9taWNyb3MY'
    'BCABKARSEWNvbW1pdHRlZEF0TWljcm9z');

@$core.Deprecated('Use appendRequestDescriptor instead')
const AppendRequest$json = {
  '1': 'AppendRequest',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'records', '3': 2, '4': 3, '5': 12, '10': 'records'},
    {
      '1': 'if_tail',
      '3': 3,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'ifTail',
      '17': true
    },
    {
      '1': 'idempotency_key',
      '3': 4,
      '4': 1,
      '5': 12,
      '9': 1,
      '10': 'idempotencyKey',
      '17': true
    },
  ],
  '8': [
    {'1': '_if_tail'},
    {'1': '_idempotency_key'},
  ],
};

/// Descriptor for `AppendRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List appendRequestDescriptor = $convert.base64Decode(
    'Cg1BcHBlbmRSZXF1ZXN0EhIKBHBhdGgYASABKAlSBHBhdGgSGAoHcmVjb3JkcxgCIAMoDFIHcm'
    'Vjb3JkcxIcCgdpZl90YWlsGAMgASgESABSBmlmVGFpbIgBARIsCg9pZGVtcG90ZW5jeV9rZXkY'
    'BCABKAxIAVIOaWRlbXBvdGVuY3lLZXmIAQFCCgoIX2lmX3RhaWxCEgoQX2lkZW1wb3RlbmN5X2'
    'tleQ==');

@$core.Deprecated('Use appendReceiptDescriptor instead')
const AppendReceipt$json = {
  '1': 'AppendReceipt',
  '2': [
    {'1': 'start', '3': 1, '4': 1, '5': 4, '10': 'start'},
    {'1': 'end', '3': 2, '4': 1, '5': 4, '10': 'end'},
    {'1': 'tail', '3': 3, '4': 1, '5': 4, '10': 'tail'},
    {'1': 'commit_id', '3': 4, '4': 1, '5': 12, '10': 'commitId'},
  ],
};

/// Descriptor for `AppendReceipt`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List appendReceiptDescriptor = $convert.base64Decode(
    'Cg1BcHBlbmRSZWNlaXB0EhQKBXN0YXJ0GAEgASgEUgVzdGFydBIQCgNlbmQYAiABKARSA2VuZB'
    'ISCgR0YWlsGAMgASgEUgR0YWlsEhsKCWNvbW1pdF9pZBgEIAEoDFIIY29tbWl0SWQ=');

@$core.Deprecated('Use tailConflictDescriptor instead')
const TailConflict$json = {
  '1': 'TailConflict',
  '2': [
    {'1': 'actual_tail', '3': 1, '4': 1, '5': 4, '10': 'actualTail'},
  ],
};

/// Descriptor for `TailConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List tailConflictDescriptor = $convert.base64Decode(
    'CgxUYWlsQ29uZmxpY3QSHwoLYWN0dWFsX3RhaWwYASABKARSCmFjdHVhbFRhaWw=');

@$core.Deprecated('Use appendResponseDescriptor instead')
const AppendResponse$json = {
  '1': 'AppendResponse',
  '2': [
    {
      '1': 'committed',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.AppendReceipt',
      '9': 0,
      '10': 'committed'
    },
    {
      '1': 'conflict',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.TailConflict',
      '9': 0,
      '10': 'conflict'
    },
  ],
  '8': [
    {'1': 'outcome'},
  ],
};

/// Descriptor for `AppendResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List appendResponseDescriptor = $convert.base64Decode(
    'Cg5BcHBlbmRSZXNwb25zZRJACgljb21taXR0ZWQYASABKAsyIC5hY3ljbGljLnN0cmVhbS52Mi'
    '5BcHBlbmRSZWNlaXB0SABSCWNvbW1pdHRlZBI9Cghjb25mbGljdBgCIAEoCzIfLmFjeWNsaWMu'
    'c3RyZWFtLnYyLlRhaWxDb25mbGljdEgAUghjb25mbGljdEIJCgdvdXRjb21l');

@$core.Deprecated('Use tailRequestDescriptor instead')
const TailRequest$json = {
  '1': 'TailRequest',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
  ],
};

/// Descriptor for `TailRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List tailRequestDescriptor =
    $convert.base64Decode('CgtUYWlsUmVxdWVzdBISCgRwYXRoGAEgASgJUgRwYXRo');

@$core.Deprecated('Use tailResponseDescriptor instead')
const TailResponse$json = {
  '1': 'TailResponse',
  '2': [
    {'1': 'tail', '3': 1, '4': 1, '5': 4, '10': 'tail'},
  ],
  '9': [
    {'1': 2, '2': 3},
  ],
  '10': ['trim_point'],
};

/// Descriptor for `TailResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List tailResponseDescriptor = $convert.base64Decode(
    'CgxUYWlsUmVzcG9uc2USEgoEdGFpbBgBIAEoBFIEdGFpbEoECAIQA1IKdHJpbV9wb2ludA==');

@$core.Deprecated('Use forkRequestDescriptor instead')
const ForkRequest$json = {
  '1': 'ForkRequest',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 9, '10': 'source'},
    {'1': 'destination', '3': 2, '4': 1, '5': 9, '10': 'destination'},
    {
      '1': 'at_tail',
      '3': 3,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'atTail',
      '17': true
    },
    {
      '1': 'idempotency_key',
      '3': 4,
      '4': 1,
      '5': 12,
      '9': 1,
      '10': 'idempotencyKey',
      '17': true
    },
  ],
  '8': [
    {'1': '_at_tail'},
    {'1': '_idempotency_key'},
  ],
};

/// Descriptor for `ForkRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkRequestDescriptor = $convert.base64Decode(
    'CgtGb3JrUmVxdWVzdBIWCgZzb3VyY2UYASABKAlSBnNvdXJjZRIgCgtkZXN0aW5hdGlvbhgCIA'
    'EoCVILZGVzdGluYXRpb24SHAoHYXRfdGFpbBgDIAEoBEgAUgZhdFRhaWyIAQESLAoPaWRlbXBv'
    'dGVuY3lfa2V5GAQgASgMSAFSDmlkZW1wb3RlbmN5S2V5iAEBQgoKCF9hdF90YWlsQhIKEF9pZG'
    'VtcG90ZW5jeV9rZXk=');

@$core.Deprecated('Use forkReceiptDescriptor instead')
const ForkReceipt$json = {
  '1': 'ForkReceipt',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 9, '10': 'source'},
    {'1': 'destination', '3': 2, '4': 1, '5': 9, '10': 'destination'},
    {'1': 'forked_at', '3': 3, '4': 1, '5': 4, '10': 'forkedAt'},
    {'1': 'tail', '3': 4, '4': 1, '5': 4, '10': 'tail'},
    {'1': 'commit_id', '3': 5, '4': 1, '5': 12, '10': 'commitId'},
  ],
};

/// Descriptor for `ForkReceipt`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkReceiptDescriptor = $convert.base64Decode(
    'CgtGb3JrUmVjZWlwdBIWCgZzb3VyY2UYASABKAlSBnNvdXJjZRIgCgtkZXN0aW5hdGlvbhgCIA'
    'EoCVILZGVzdGluYXRpb24SGwoJZm9ya2VkX2F0GAMgASgEUghmb3JrZWRBdBISCgR0YWlsGAQg'
    'ASgEUgR0YWlsEhsKCWNvbW1pdF9pZBgFIAEoDFIIY29tbWl0SWQ=');

@$core.Deprecated('Use readRequestDescriptor instead')
const ReadRequest$json = {
  '1': 'ReadRequest',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'from', '3': 2, '4': 1, '5': 4, '10': 'from'},
    {'1': 'limit', '3': 3, '4': 1, '5': 13, '10': 'limit'},
  ],
};

/// Descriptor for `ReadRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List readRequestDescriptor = $convert.base64Decode(
    'CgtSZWFkUmVxdWVzdBISCgRwYXRoGAEgASgJUgRwYXRoEhIKBGZyb20YAiABKARSBGZyb20SFA'
    'oFbGltaXQYAyABKA1SBWxpbWl0');

@$core.Deprecated('Use followRequestDescriptor instead')
const FollowRequest$json = {
  '1': 'FollowRequest',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'from', '3': 2, '4': 1, '5': 4, '10': 'from'},
  ],
};

/// Descriptor for `FollowRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List followRequestDescriptor = $convert.base64Decode(
    'Cg1Gb2xsb3dSZXF1ZXN0EhIKBHBhdGgYASABKAlSBHBhdGgSEgoEZnJvbRgCIAEoBFIEZnJvbQ'
    '==');

@$core.Deprecated('Use readResponseDescriptor instead')
const ReadResponse$json = {
  '1': 'ReadResponse',
  '2': [
    {
      '1': 'record',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.Record',
      '10': 'record'
    },
  ],
};

/// Descriptor for `ReadResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List readResponseDescriptor = $convert.base64Decode(
    'CgxSZWFkUmVzcG9uc2USMQoGcmVjb3JkGAEgASgLMhkuYWN5Y2xpYy5zdHJlYW0udjIuUmVjb3'
    'JkUgZyZWNvcmQ=');

@$core.Deprecated('Use childrenRequestDescriptor instead')
const ChildrenRequest$json = {
  '1': 'ChildrenRequest',
  '2': [
    {'1': 'parent', '3': 1, '4': 1, '5': 9, '9': 0, '10': 'parent', '17': true},
    {'1': 'limit', '3': 2, '4': 1, '5': 13, '10': 'limit'},
  ],
  '8': [
    {'1': '_parent'},
  ],
};

/// Descriptor for `ChildrenRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List childrenRequestDescriptor = $convert.base64Decode(
    'Cg9DaGlsZHJlblJlcXVlc3QSGwoGcGFyZW50GAEgASgJSABSBnBhcmVudIgBARIUCgVsaW1pdB'
    'gCIAEoDVIFbGltaXRCCQoHX3BhcmVudA==');

@$core.Deprecated('Use childDescriptor instead')
const Child$json = {
  '1': 'Child',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
  ],
};

/// Descriptor for `Child`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List childDescriptor =
    $convert.base64Decode('CgVDaGlsZBISCgRwYXRoGAEgASgJUgRwYXRo');

@$core.Deprecated('Use childrenResponseDescriptor instead')
const ChildrenResponse$json = {
  '1': 'ChildrenResponse',
  '2': [
    {
      '1': 'child',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.Child',
      '10': 'child'
    },
  ],
};

/// Descriptor for `ChildrenResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List childrenResponseDescriptor = $convert.base64Decode(
    'ChBDaGlsZHJlblJlc3BvbnNlEi4KBWNoaWxkGAEgASgLMhguYWN5Y2xpYy5zdHJlYW0udjIuQ2'
    'hpbGRSBWNoaWxk');

@$core.Deprecated('Use childrenPageRequestDescriptor instead')
const ChildrenPageRequest$json = {
  '1': 'ChildrenPageRequest',
  '2': [
    {'1': 'parent', '3': 1, '4': 1, '5': 9, '9': 0, '10': 'parent', '17': true},
    {'1': 'after', '3': 2, '4': 1, '5': 9, '9': 1, '10': 'after', '17': true},
    {
      '1': 'hierarchy_version',
      '3': 3,
      '4': 1,
      '5': 12,
      '9': 2,
      '10': 'hierarchyVersion',
      '17': true
    },
    {'1': 'limit', '3': 4, '4': 1, '5': 13, '10': 'limit'},
  ],
  '8': [
    {'1': '_parent'},
    {'1': '_after'},
    {'1': '_hierarchy_version'},
  ],
};

/// Descriptor for `ChildrenPageRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List childrenPageRequestDescriptor = $convert.base64Decode(
    'ChNDaGlsZHJlblBhZ2VSZXF1ZXN0EhsKBnBhcmVudBgBIAEoCUgAUgZwYXJlbnSIAQESGQoFYW'
    'Z0ZXIYAiABKAlIAVIFYWZ0ZXKIAQESMAoRaGllcmFyY2h5X3ZlcnNpb24YAyABKAxIAlIQaGll'
    'cmFyY2h5VmVyc2lvbogBARIUCgVsaW1pdBgEIAEoDVIFbGltaXRCCQoHX3BhcmVudEIICgZfYW'
    'Z0ZXJCFAoSX2hpZXJhcmNoeV92ZXJzaW9u');

@$core.Deprecated('Use childrenPageResponseDescriptor instead')
const ChildrenPageResponse$json = {
  '1': 'ChildrenPageResponse',
  '2': [
    {
      '1': 'hierarchy_version',
      '3': 1,
      '4': 1,
      '5': 12,
      '10': 'hierarchyVersion'
    },
    {
      '1': 'children',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.stream.v2.Child',
      '10': 'children'
    },
    {
      '1': 'next_after',
      '3': 3,
      '4': 1,
      '5': 9,
      '9': 0,
      '10': 'nextAfter',
      '17': true
    },
  ],
  '8': [
    {'1': '_next_after'},
  ],
};

/// Descriptor for `ChildrenPageResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List childrenPageResponseDescriptor = $convert.base64Decode(
    'ChRDaGlsZHJlblBhZ2VSZXNwb25zZRIrChFoaWVyYXJjaHlfdmVyc2lvbhgBIAEoDFIQaGllcm'
    'FyY2h5VmVyc2lvbhI0CghjaGlsZHJlbhgCIAMoCzIYLmFjeWNsaWMuc3RyZWFtLnYyLkNoaWxk'
    'UghjaGlsZHJlbhIiCgpuZXh0X2FmdGVyGAMgASgJSABSCW5leHRBZnRlcogBAUINCgtfbmV4dF'
    '9hZnRlcg==');

@$core.Deprecated('Use tailConditionDescriptor instead')
const TailCondition$json = {
  '1': 'TailCondition',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'expected', '3': 2, '4': 1, '5': 4, '10': 'expected'},
  ],
};

/// Descriptor for `TailCondition`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List tailConditionDescriptor = $convert.base64Decode(
    'Cg1UYWlsQ29uZGl0aW9uEhIKBHBhdGgYASABKAlSBHBhdGgSGgoIZXhwZWN0ZWQYAiABKARSCG'
    'V4cGVjdGVk');

@$core.Deprecated('Use absentConditionDescriptor instead')
const AbsentCondition$json = {
  '1': 'AbsentCondition',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
  ],
};

/// Descriptor for `AbsentCondition`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List absentConditionDescriptor = $convert
    .base64Decode('Cg9BYnNlbnRDb25kaXRpb24SEgoEcGF0aBgBIAEoCVIEcGF0aA==');

@$core.Deprecated('Use commitConditionDescriptor instead')
const CommitCondition$json = {
  '1': 'CommitCondition',
  '2': [
    {
      '1': 'tail',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.TailCondition',
      '9': 0,
      '10': 'tail'
    },
    {
      '1': 'absent',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.AbsentCondition',
      '9': 0,
      '10': 'absent'
    },
  ],
  '8': [
    {'1': 'condition'},
  ],
};

/// Descriptor for `CommitCondition`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List commitConditionDescriptor = $convert.base64Decode(
    'Cg9Db21taXRDb25kaXRpb24SNgoEdGFpbBgBIAEoCzIgLmFjeWNsaWMuc3RyZWFtLnYyLlRhaW'
    'xDb25kaXRpb25IAFIEdGFpbBI8CgZhYnNlbnQYAiABKAsyIi5hY3ljbGljLnN0cmVhbS52Mi5B'
    'YnNlbnRDb25kaXRpb25IAFIGYWJzZW50QgsKCWNvbmRpdGlvbg==');

@$core.Deprecated('Use appendMutationDescriptor instead')
const AppendMutation$json = {
  '1': 'AppendMutation',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'records', '3': 2, '4': 3, '5': 12, '10': 'records'},
  ],
};

/// Descriptor for `AppendMutation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List appendMutationDescriptor = $convert.base64Decode(
    'Cg5BcHBlbmRNdXRhdGlvbhISCgRwYXRoGAEgASgJUgRwYXRoEhgKB3JlY29yZHMYAiADKAxSB3'
    'JlY29yZHM=');

@$core.Deprecated('Use forkMutationDescriptor instead')
const ForkMutation$json = {
  '1': 'ForkMutation',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 9, '10': 'source'},
    {'1': 'destination', '3': 2, '4': 1, '5': 9, '10': 'destination'},
    {'1': 'at_tail', '3': 3, '4': 1, '5': 4, '10': 'atTail'},
    {'1': 'records', '3': 4, '4': 3, '5': 12, '10': 'records'},
  ],
};

/// Descriptor for `ForkMutation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkMutationDescriptor = $convert.base64Decode(
    'CgxGb3JrTXV0YXRpb24SFgoGc291cmNlGAEgASgJUgZzb3VyY2USIAoLZGVzdGluYXRpb24YAi'
    'ABKAlSC2Rlc3RpbmF0aW9uEhcKB2F0X3RhaWwYAyABKARSBmF0VGFpbBIYCgdyZWNvcmRzGAQg'
    'AygMUgdyZWNvcmRz');

@$core.Deprecated('Use commitMutationDescriptor instead')
const CommitMutation$json = {
  '1': 'CommitMutation',
  '2': [
    {
      '1': 'append',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.AppendMutation',
      '9': 0,
      '10': 'append'
    },
    {
      '1': 'fork',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.ForkMutation',
      '9': 0,
      '10': 'fork'
    },
  ],
  '8': [
    {'1': 'mutation'},
  ],
  '9': [
    {'1': 3, '2': 4},
    {'1': 4, '2': 5},
  ],
  '10': ['trim', 'delete'],
};

/// Descriptor for `CommitMutation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List commitMutationDescriptor = $convert.base64Decode(
    'Cg5Db21taXRNdXRhdGlvbhI7CgZhcHBlbmQYASABKAsyIS5hY3ljbGljLnN0cmVhbS52Mi5BcH'
    'BlbmRNdXRhdGlvbkgAUgZhcHBlbmQSNQoEZm9yaxgCIAEoCzIfLmFjeWNsaWMuc3RyZWFtLnYy'
    'LkZvcmtNdXRhdGlvbkgAUgRmb3JrQgoKCG11dGF0aW9uSgQIAxAESgQIBBAFUgR0cmltUgZkZW'
    'xldGU=');

@$core.Deprecated('Use commitRequestDescriptor instead')
const CommitRequest$json = {
  '1': 'CommitRequest',
  '2': [
    {
      '1': 'conditions',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.stream.v2.CommitCondition',
      '10': 'conditions'
    },
    {
      '1': 'mutations',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.stream.v2.CommitMutation',
      '10': 'mutations'
    },
    {'1': 'idempotency_key', '3': 3, '4': 1, '5': 12, '10': 'idempotencyKey'},
    {
      '1': 'deadline_unix_millis',
      '3': 4,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'deadlineUnixMillis',
      '17': true
    },
  ],
  '8': [
    {'1': '_deadline_unix_millis'},
  ],
};

/// Descriptor for `CommitRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List commitRequestDescriptor = $convert.base64Decode(
    'Cg1Db21taXRSZXF1ZXN0EkIKCmNvbmRpdGlvbnMYASADKAsyIi5hY3ljbGljLnN0cmVhbS52Mi'
    '5Db21taXRDb25kaXRpb25SCmNvbmRpdGlvbnMSPwoJbXV0YXRpb25zGAIgAygLMiEuYWN5Y2xp'
    'Yy5zdHJlYW0udjIuQ29tbWl0TXV0YXRpb25SCW11dGF0aW9ucxInCg9pZGVtcG90ZW5jeV9rZX'
    'kYAyABKAxSDmlkZW1wb3RlbmN5S2V5EjUKFGRlYWRsaW5lX3VuaXhfbWlsbGlzGAQgASgESABS'
    'EmRlYWRsaW5lVW5peE1pbGxpc4gBAUIXChVfZGVhZGxpbmVfdW5peF9taWxsaXM=');

@$core.Deprecated('Use committedAppendDescriptor instead')
const CommittedAppend$json = {
  '1': 'CommittedAppend',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'start', '3': 2, '4': 1, '5': 4, '10': 'start'},
    {'1': 'end', '3': 3, '4': 1, '5': 4, '10': 'end'},
    {'1': 'tail', '3': 4, '4': 1, '5': 4, '10': 'tail'},
    {
      '1': 'records',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.stream.v2.Record',
      '10': 'records'
    },
  ],
};

/// Descriptor for `CommittedAppend`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List committedAppendDescriptor = $convert.base64Decode(
    'Cg9Db21taXR0ZWRBcHBlbmQSEgoEcGF0aBgBIAEoCVIEcGF0aBIUCgVzdGFydBgCIAEoBFIFc3'
    'RhcnQSEAoDZW5kGAMgASgEUgNlbmQSEgoEdGFpbBgEIAEoBFIEdGFpbBIzCgdyZWNvcmRzGAUg'
    'AygLMhkuYWN5Y2xpYy5zdHJlYW0udjIuUmVjb3JkUgdyZWNvcmRz');

@$core.Deprecated('Use committedForkDescriptor instead')
const CommittedFork$json = {
  '1': 'CommittedFork',
  '2': [
    {'1': 'source', '3': 1, '4': 1, '5': 9, '10': 'source'},
    {'1': 'destination', '3': 2, '4': 1, '5': 9, '10': 'destination'},
    {'1': 'forked_at', '3': 3, '4': 1, '5': 4, '10': 'forkedAt'},
    {'1': 'tail', '3': 4, '4': 1, '5': 4, '10': 'tail'},
    {
      '1': 'records',
      '3': 5,
      '4': 3,
      '5': 11,
      '6': '.acyclic.stream.v2.Record',
      '10': 'records'
    },
  ],
};

/// Descriptor for `CommittedFork`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List committedForkDescriptor = $convert.base64Decode(
    'Cg1Db21taXR0ZWRGb3JrEhYKBnNvdXJjZRgBIAEoCVIGc291cmNlEiAKC2Rlc3RpbmF0aW9uGA'
    'IgASgJUgtkZXN0aW5hdGlvbhIbCglmb3JrZWRfYXQYAyABKARSCGZvcmtlZEF0EhIKBHRhaWwY'
    'BCABKARSBHRhaWwSMwoHcmVjb3JkcxgFIAMoCzIZLmFjeWNsaWMuc3RyZWFtLnYyLlJlY29yZF'
    'IHcmVjb3Jkcw==');

@$core.Deprecated('Use committedMutationDescriptor instead')
const CommittedMutation$json = {
  '1': 'CommittedMutation',
  '2': [
    {
      '1': 'append',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.CommittedAppend',
      '9': 0,
      '10': 'append'
    },
    {
      '1': 'fork',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.CommittedFork',
      '9': 0,
      '10': 'fork'
    },
  ],
  '8': [
    {'1': 'mutation'},
  ],
  '9': [
    {'1': 3, '2': 4},
    {'1': 4, '2': 5},
  ],
  '10': ['trim', 'delete'],
};

/// Descriptor for `CommittedMutation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List committedMutationDescriptor = $convert.base64Decode(
    'ChFDb21taXR0ZWRNdXRhdGlvbhI8CgZhcHBlbmQYASABKAsyIi5hY3ljbGljLnN0cmVhbS52Mi'
    '5Db21taXR0ZWRBcHBlbmRIAFIGYXBwZW5kEjYKBGZvcmsYAiABKAsyIC5hY3ljbGljLnN0cmVh'
    'bS52Mi5Db21taXR0ZWRGb3JrSABSBGZvcmtCCgoIbXV0YXRpb25KBAgDEARKBAgEEAVSBHRyaW'
    '1SBmRlbGV0ZQ==');

@$core.Deprecated('Use committedEnvelopeDescriptor instead')
const CommittedEnvelope$json = {
  '1': 'CommittedEnvelope',
  '2': [
    {'1': 'commit_id', '3': 1, '4': 1, '5': 12, '10': 'commitId'},
    {
      '1': 'mutations',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.stream.v2.CommittedMutation',
      '10': 'mutations'
    },
  ],
};

/// Descriptor for `CommittedEnvelope`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List committedEnvelopeDescriptor = $convert.base64Decode(
    'ChFDb21taXR0ZWRFbnZlbG9wZRIbCgljb21taXRfaWQYASABKAxSCGNvbW1pdElkEkIKCW11dG'
    'F0aW9ucxgCIAMoCzIkLmFjeWNsaWMuc3RyZWFtLnYyLkNvbW1pdHRlZE11dGF0aW9uUgltdXRh'
    'dGlvbnM=');

@$core.Deprecated('Use tailCommitConflictDescriptor instead')
const TailCommitConflict$json = {
  '1': 'TailCommitConflict',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {'1': 'expected', '3': 2, '4': 1, '5': 4, '10': 'expected'},
    {'1': 'actual', '3': 3, '4': 1, '5': 4, '9': 0, '10': 'actual', '17': true},
  ],
  '8': [
    {'1': '_actual'},
  ],
};

/// Descriptor for `TailCommitConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List tailCommitConflictDescriptor = $convert.base64Decode(
    'ChJUYWlsQ29tbWl0Q29uZmxpY3QSEgoEcGF0aBgBIAEoCVIEcGF0aBIaCghleHBlY3RlZBgCIA'
    'EoBFIIZXhwZWN0ZWQSGwoGYWN0dWFsGAMgASgESABSBmFjdHVhbIgBAUIJCgdfYWN0dWFs');

@$core.Deprecated('Use existsCommitConflictDescriptor instead')
const ExistsCommitConflict$json = {
  '1': 'ExistsCommitConflict',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
  ],
};

/// Descriptor for `ExistsCommitConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List existsCommitConflictDescriptor = $convert
    .base64Decode('ChRFeGlzdHNDb21taXRDb25mbGljdBISCgRwYXRoGAEgASgJUgRwYXRo');

@$core.Deprecated('Use commitConflictDescriptor instead')
const CommitConflict$json = {
  '1': 'CommitConflict',
  '2': [
    {
      '1': 'tail',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.TailCommitConflict',
      '9': 0,
      '10': 'tail'
    },
    {
      '1': 'exists',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.ExistsCommitConflict',
      '9': 0,
      '10': 'exists'
    },
  ],
  '8': [
    {'1': 'conflict'},
  ],
  '9': [
    {'1': 3, '2': 4},
  ],
  '10': ['retired'],
};

/// Descriptor for `CommitConflict`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List commitConflictDescriptor = $convert.base64Decode(
    'Cg5Db21taXRDb25mbGljdBI7CgR0YWlsGAEgASgLMiUuYWN5Y2xpYy5zdHJlYW0udjIuVGFpbE'
    'NvbW1pdENvbmZsaWN0SABSBHRhaWwSQQoGZXhpc3RzGAIgASgLMicuYWN5Y2xpYy5zdHJlYW0u'
    'djIuRXhpc3RzQ29tbWl0Q29uZmxpY3RIAFIGZXhpc3RzQgoKCGNvbmZsaWN0SgQIAxAEUgdyZX'
    'RpcmVk');

@$core.Deprecated('Use commitConflictsDescriptor instead')
const CommitConflicts$json = {
  '1': 'CommitConflicts',
  '2': [
    {
      '1': 'conflicts',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.stream.v2.CommitConflict',
      '10': 'conflicts'
    },
  ],
};

/// Descriptor for `CommitConflicts`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List commitConflictsDescriptor = $convert.base64Decode(
    'Cg9Db21taXRDb25mbGljdHMSPwoJY29uZmxpY3RzGAEgAygLMiEuYWN5Y2xpYy5zdHJlYW0udj'
    'IuQ29tbWl0Q29uZmxpY3RSCWNvbmZsaWN0cw==');

@$core.Deprecated('Use commitResponseDescriptor instead')
const CommitResponse$json = {
  '1': 'CommitResponse',
  '2': [
    {
      '1': 'committed',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.CommittedEnvelope',
      '9': 0,
      '10': 'committed'
    },
    {
      '1': 'conflict',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.CommitConflicts',
      '9': 0,
      '10': 'conflict'
    },
  ],
  '8': [
    {'1': 'outcome'},
  ],
};

/// Descriptor for `CommitResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List commitResponseDescriptor = $convert.base64Decode(
    'Cg5Db21taXRSZXNwb25zZRJECgljb21taXR0ZWQYASABKAsyJC5hY3ljbGljLnN0cmVhbS52Mi'
    '5Db21taXR0ZWRFbnZlbG9wZUgAUgljb21taXR0ZWQSQAoIY29uZmxpY3QYAiABKAsyIi5hY3lj'
    'bGljLnN0cmVhbS52Mi5Db21taXRDb25mbGljdHNIAFIIY29uZmxpY3RCCQoHb3V0Y29tZQ==');

@$core.Deprecated('Use readCommitRequestDescriptor instead')
const ReadCommitRequest$json = {
  '1': 'ReadCommitRequest',
  '2': [
    {'1': 'commit_id', '3': 1, '4': 1, '5': 12, '10': 'commitId'},
  ],
};

/// Descriptor for `ReadCommitRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List readCommitRequestDescriptor = $convert.base64Decode(
    'ChFSZWFkQ29tbWl0UmVxdWVzdBIbCgljb21taXRfaWQYASABKAxSCGNvbW1pdElk');

@$core.Deprecated('Use inspectIdempotencyRequestDescriptor instead')
const InspectIdempotencyRequest$json = {
  '1': 'InspectIdempotencyRequest',
  '2': [
    {'1': 'idempotency_key', '3': 1, '4': 1, '5': 12, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `InspectIdempotencyRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectIdempotencyRequestDescriptor =
    $convert.base64Decode(
        'ChlJbnNwZWN0SWRlbXBvdGVuY3lSZXF1ZXN0EicKD2lkZW1wb3RlbmN5X2tleRgBIAEoDFIOaW'
        'RlbXBvdGVuY3lLZXk=');

@$core.Deprecated('Use idempotencyObservationDescriptor instead')
const IdempotencyObservation$json = {
  '1': 'IdempotencyObservation',
  '2': [
    {'1': 'idempotency_key', '3': 1, '4': 1, '5': 12, '10': 'idempotencyKey'},
    {'1': 'request_digest', '3': 2, '4': 1, '5': 12, '10': 'requestDigest'},
    {
      '1': 'append',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.AppendResponse',
      '9': 0,
      '10': 'append'
    },
    {
      '1': 'fork',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.ForkReceipt',
      '9': 0,
      '10': 'fork'
    },
    {
      '1': 'commit',
      '3': 7,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.CommitResponse',
      '9': 0,
      '10': 'commit'
    },
  ],
  '8': [
    {'1': 'outcome'},
  ],
  '9': [
    {'1': 5, '2': 6},
    {'1': 6, '2': 7},
  ],
  '10': ['trim', 'delete'],
};

/// Descriptor for `IdempotencyObservation`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List idempotencyObservationDescriptor = $convert.base64Decode(
    'ChZJZGVtcG90ZW5jeU9ic2VydmF0aW9uEicKD2lkZW1wb3RlbmN5X2tleRgBIAEoDFIOaWRlbX'
    'BvdGVuY3lLZXkSJQoOcmVxdWVzdF9kaWdlc3QYAiABKAxSDXJlcXVlc3REaWdlc3QSOwoGYXBw'
    'ZW5kGAMgASgLMiEuYWN5Y2xpYy5zdHJlYW0udjIuQXBwZW5kUmVzcG9uc2VIAFIGYXBwZW5kEj'
    'QKBGZvcmsYBCABKAsyHi5hY3ljbGljLnN0cmVhbS52Mi5Gb3JrUmVjZWlwdEgAUgRmb3JrEjsK'
    'BmNvbW1pdBgHIAEoCzIhLmFjeWNsaWMuc3RyZWFtLnYyLkNvbW1pdFJlc3BvbnNlSABSBmNvbW'
    '1pdEIJCgdvdXRjb21lSgQIBRAGSgQIBhAHUgR0cmltUgZkZWxldGU=');

@$core.Deprecated('Use inspectIdempotencyResponseDescriptor instead')
const InspectIdempotencyResponse$json = {
  '1': 'InspectIdempotencyResponse',
  '2': [
    {
      '1': 'observation',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.stream.v2.IdempotencyObservation',
      '9': 0,
      '10': 'observation',
      '17': true
    },
  ],
  '8': [
    {'1': '_observation'},
  ],
};

/// Descriptor for `InspectIdempotencyResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inspectIdempotencyResponseDescriptor =
    $convert.base64Decode(
        'ChpJbnNwZWN0SWRlbXBvdGVuY3lSZXNwb25zZRJQCgtvYnNlcnZhdGlvbhgBIAEoCzIpLmFjeW'
        'NsaWMuc3RyZWFtLnYyLklkZW1wb3RlbmN5T2JzZXJ2YXRpb25IAFILb2JzZXJ2YXRpb26IAQFC'
        'DgoMX29ic2VydmF0aW9u');

@$core.Deprecated('Use tokenGrantDescriptor instead')
const TokenGrant$json = {
  '1': 'TokenGrant',
  '2': [
    {'1': 'path', '3': 1, '4': 1, '5': 9, '10': 'path'},
    {
      '1': 'subtree',
      '3': 2,
      '4': 1,
      '5': 8,
      '9': 0,
      '10': 'subtree',
      '17': true
    },
    {'1': 'operations', '3': 3, '4': 3, '5': 9, '10': 'operations'},
  ],
  '8': [
    {'1': '_subtree'},
  ],
};

/// Descriptor for `TokenGrant`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List tokenGrantDescriptor = $convert.base64Decode(
    'CgpUb2tlbkdyYW50EhIKBHBhdGgYASABKAlSBHBhdGgSHQoHc3VidHJlZRgCIAEoCEgAUgdzdW'
    'J0cmVliAEBEh4KCm9wZXJhdGlvbnMYAyADKAlSCm9wZXJhdGlvbnNCCgoIX3N1YnRyZWU=');

@$core.Deprecated('Use createTokenRequestDescriptor instead')
const CreateTokenRequest$json = {
  '1': 'CreateTokenRequest',
  '2': [
    {'1': 'expires_in', '3': 1, '4': 1, '5': 9, '10': 'expiresIn'},
    {
      '1': 'allow',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.stream.v2.TokenGrant',
      '10': 'allow'
    },
  ],
};

/// Descriptor for `CreateTokenRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createTokenRequestDescriptor = $convert.base64Decode(
    'ChJDcmVhdGVUb2tlblJlcXVlc3QSHQoKZXhwaXJlc19pbhgBIAEoCVIJZXhwaXJlc0luEjMKBW'
    'FsbG93GAIgAygLMh0uYWN5Y2xpYy5zdHJlYW0udjIuVG9rZW5HcmFudFIFYWxsb3c=');
