// This is a generated file - do not edit.
//
// Generated from objects/v1/objects.proto.

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

@$core.Deprecated('Use objectsLimitDescriptor instead')
const ObjectsLimit$json = {
  '1': 'ObjectsLimit',
  '2': [
    {'1': 'OBJECTS_LIMIT_UNSPECIFIED', '2': 0},
    {'1': 'OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES', '2': 256},
  ],
};

/// Descriptor for `ObjectsLimit`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List objectsLimitDescriptor = $convert.base64Decode(
    'CgxPYmplY3RzTGltaXQSHQoZT0JKRUNUU19MSU1JVF9VTlNQRUNJRklFRBAAEiwKJ09CSkVDVF'
    'NfTElNSVRfTUFYX0lERU1QT1RFTkNZX0tFWV9CWVRFUxCAAg==');

@$core.Deprecated('Use listingModeDescriptor instead')
const ListingMode$json = {
  '1': 'ListingMode',
  '2': [
    {'1': 'LISTING_MODE_UNSPECIFIED', '2': 0},
    {'1': 'LISTING_MODE_CURRENT', '2': 1},
    {'1': 'LISTING_MODE_VERSIONS', '2': 2},
  ],
};

/// Descriptor for `ListingMode`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List listingModeDescriptor = $convert.base64Decode(
    'CgtMaXN0aW5nTW9kZRIcChhMSVNUSU5HX01PREVfVU5TUEVDSUZJRUQQABIYChRMSVNUSU5HX0'
    '1PREVfQ1VSUkVOVBABEhkKFUxJU1RJTkdfTU9ERV9WRVJTSU9OUxAC');

@$core.Deprecated('Use errorCodeDescriptor instead')
const ErrorCode$json = {
  '1': 'ErrorCode',
  '2': [
    {'1': 'ERROR_CODE_UNSPECIFIED', '2': 0},
    {'1': 'ERROR_CODE_INVALID_ARGUMENT', '2': 1},
    {'1': 'ERROR_CODE_NOT_FOUND', '2': 2},
    {'1': 'ERROR_CODE_ALREADY_EXISTS', '2': 3},
    {'1': 'ERROR_CODE_PRECONDITION_FAILED', '2': 4},
    {'1': 'ERROR_CODE_IDEMPOTENCY_MISMATCH', '2': 5},
    {'1': 'ERROR_CODE_TOKEN_EXPIRED', '2': 6},
    {'1': 'ERROR_CODE_QUOTA_EXCEEDED', '2': 7},
    {'1': 'ERROR_CODE_UNSUPPORTED', '2': 8},
    {'1': 'ERROR_CODE_UNAVAILABLE', '2': 9},
  ],
};

/// Descriptor for `ErrorCode`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List errorCodeDescriptor = $convert.base64Decode(
    'CglFcnJvckNvZGUSGgoWRVJST1JfQ09ERV9VTlNQRUNJRklFRBAAEh8KG0VSUk9SX0NPREVfSU'
    '5WQUxJRF9BUkdVTUVOVBABEhgKFEVSUk9SX0NPREVfTk9UX0ZPVU5EEAISHQoZRVJST1JfQ09E'
    'RV9BTFJFQURZX0VYSVNUUxADEiIKHkVSUk9SX0NPREVfUFJFQ09ORElUSU9OX0ZBSUxFRBAEEi'
    'MKH0VSUk9SX0NPREVfSURFTVBPVEVOQ1lfTUlTTUFUQ0gQBRIcChhFUlJPUl9DT0RFX1RPS0VO'
    'X0VYUElSRUQQBhIdChlFUlJPUl9DT0RFX1FVT1RBX0VYQ0VFREVEEAcSGgoWRVJST1JfQ09ERV'
    '9VTlNVUFBPUlRFRBAIEhoKFkVSUk9SX0NPREVfVU5BVkFJTEFCTEUQCQ==');

@$core.Deprecated('Use bucketRefDescriptor instead')
const BucketRef$json = {
  '1': 'BucketRef',
  '2': [
    {'1': 'bucket_id', '3': 1, '4': 1, '5': 9, '10': 'bucketId'},
    {'1': 'name', '3': 2, '4': 1, '5': 9, '10': 'name'},
  ],
};

/// Descriptor for `BucketRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List bucketRefDescriptor = $convert.base64Decode(
    'CglCdWNrZXRSZWYSGwoJYnVja2V0X2lkGAEgASgJUghidWNrZXRJZBISCgRuYW1lGAIgASgJUg'
    'RuYW1l');

@$core.Deprecated('Use snapshotRefDescriptor instead')
const SnapshotRef$json = {
  '1': 'SnapshotRef',
  '2': [
    {'1': 'snapshot_id', '3': 1, '4': 1, '5': 9, '10': 'snapshotId'},
    {'1': 'source_bucket_id', '3': 2, '4': 1, '5': 9, '10': 'sourceBucketId'},
  ],
};

/// Descriptor for `SnapshotRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List snapshotRefDescriptor = $convert.base64Decode(
    'CgtTbmFwc2hvdFJlZhIfCgtzbmFwc2hvdF9pZBgBIAEoCVIKc25hcHNob3RJZBIoChBzb3VyY2'
    'VfYnVja2V0X2lkGAIgASgJUg5zb3VyY2VCdWNrZXRJZA==');

@$core.Deprecated('Use readTargetDescriptor instead')
const ReadTarget$json = {
  '1': 'ReadTarget',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '9': 0,
      '10': 'bucket'
    },
    {
      '1': 'snapshot',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.SnapshotRef',
      '9': 0,
      '10': 'snapshot'
    },
  ],
  '8': [
    {'1': 'target'},
  ],
};

/// Descriptor for `ReadTarget`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List readTargetDescriptor = $convert.base64Decode(
    'CgpSZWFkVGFyZ2V0EjcKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy52MS5CdWNrZX'
    'RSZWZIAFIGYnVja2V0Ej0KCHNuYXBzaG90GAIgASgLMh8uYWN5Y2xpYy5vYmplY3RzLnYxLlNu'
    'YXBzaG90UmVmSABSCHNuYXBzaG90QggKBnRhcmdldA==');

@$core.Deprecated('Use objectMetadataDescriptor instead')
const ObjectMetadata$json = {
  '1': 'ObjectMetadata',
  '2': [
    {'1': 'content_type', '3': 1, '4': 1, '5': 9, '10': 'contentType'},
    {
      '1': 'user',
      '3': 2,
      '4': 3,
      '5': 11,
      '6': '.acyclic.objects.v1.ObjectMetadata.UserEntry',
      '10': 'user'
    },
    {'1': 'content_encoding', '3': 3, '4': 1, '5': 9, '10': 'contentEncoding'},
    {'1': 'cache_control', '3': 4, '4': 1, '5': 9, '10': 'cacheControl'},
    {
      '1': 'content_disposition',
      '3': 5,
      '4': 1,
      '5': 9,
      '10': 'contentDisposition'
    },
    {'1': 'content_language', '3': 6, '4': 1, '5': 9, '10': 'contentLanguage'},
    {
      '1': 'expires_unix_seconds',
      '3': 7,
      '4': 1,
      '5': 3,
      '9': 0,
      '10': 'expiresUnixSeconds',
      '17': true
    },
  ],
  '3': [ObjectMetadata_UserEntry$json],
  '8': [
    {'1': '_expires_unix_seconds'},
  ],
};

@$core.Deprecated('Use objectMetadataDescriptor instead')
const ObjectMetadata_UserEntry$json = {
  '1': 'UserEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {'1': 'value', '3': 2, '4': 1, '5': 9, '10': 'value'},
  ],
  '7': {'7': true},
};

/// Descriptor for `ObjectMetadata`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List objectMetadataDescriptor = $convert.base64Decode(
    'Cg5PYmplY3RNZXRhZGF0YRIhCgxjb250ZW50X3R5cGUYASABKAlSC2NvbnRlbnRUeXBlEkAKBH'
    'VzZXIYAiADKAsyLC5hY3ljbGljLm9iamVjdHMudjEuT2JqZWN0TWV0YWRhdGEuVXNlckVudHJ5'
    'UgR1c2VyEikKEGNvbnRlbnRfZW5jb2RpbmcYAyABKAlSD2NvbnRlbnRFbmNvZGluZxIjCg1jYW'
    'NoZV9jb250cm9sGAQgASgJUgxjYWNoZUNvbnRyb2wSLwoTY29udGVudF9kaXNwb3NpdGlvbhgF'
    'IAEoCVISY29udGVudERpc3Bvc2l0aW9uEikKEGNvbnRlbnRfbGFuZ3VhZ2UYBiABKAlSD2Nvbn'
    'RlbnRMYW5ndWFnZRI1ChRleHBpcmVzX3VuaXhfc2Vjb25kcxgHIAEoA0gAUhJleHBpcmVzVW5p'
    'eFNlY29uZHOIAQEaNwoJVXNlckVudHJ5EhAKA2tleRgBIAEoCVIDa2V5EhQKBXZhbHVlGAIgAS'
    'gJUgV2YWx1ZToCOAFCFwoVX2V4cGlyZXNfdW5peF9zZWNvbmRz');

@$core.Deprecated('Use preconditionsDescriptor instead')
const Preconditions$json = {
  '1': 'Preconditions',
  '2': [
    {'1': 'if_absent', '3': 1, '4': 1, '5': 8, '9': 0, '10': 'ifAbsent'},
    {'1': 'if_match', '3': 2, '4': 1, '5': 9, '9': 0, '10': 'ifMatch'},
    {'1': 'if_version', '3': 3, '4': 1, '5': 9, '9': 0, '10': 'ifVersion'},
  ],
  '8': [
    {'1': 'condition'},
  ],
};

/// Descriptor for `Preconditions`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List preconditionsDescriptor = $convert.base64Decode(
    'Cg1QcmVjb25kaXRpb25zEh0KCWlmX2Fic2VudBgBIAEoCEgAUghpZkFic2VudBIbCghpZl9tYX'
    'RjaBgCIAEoCUgAUgdpZk1hdGNoEh8KCmlmX3ZlcnNpb24YAyABKAlIAFIJaWZWZXJzaW9uQgsK'
    'CWNvbmRpdGlvbg==');

@$core.Deprecated('Use mutationIdentityDescriptor instead')
const MutationIdentity$json = {
  '1': 'MutationIdentity',
  '2': [
    {'1': 'idempotency_key', '3': 1, '4': 1, '5': 9, '10': 'idempotencyKey'},
  ],
};

/// Descriptor for `MutationIdentity`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List mutationIdentityDescriptor = $convert.base64Decode(
    'ChBNdXRhdGlvbklkZW50aXR5EicKD2lkZW1wb3RlbmN5X2tleRgBIAEoCVIOaWRlbXBvdGVuY3'
    'lLZXk=');

@$core.Deprecated('Use bucketDescriptor instead')
const Bucket$json = {
  '1': 'Bucket',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {
      '1': 'created_at',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.google.protobuf.Timestamp',
      '10': 'createdAt'
    },
  ],
};

/// Descriptor for `Bucket`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List bucketDescriptor = $convert.base64Decode(
    'CgZCdWNrZXQSNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYmplY3RzLnYxLkJ1Y2tldFJlZl'
    'IGYnVja2V0EjkKCmNyZWF0ZWRfYXQYAiABKAsyGi5nb29nbGUucHJvdG9idWYuVGltZXN0YW1w'
    'UgljcmVhdGVkQXQ=');

@$core.Deprecated('Use createBucketRequestDescriptor instead')
const CreateBucketRequest$json = {
  '1': 'CreateBucketRequest',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
    {
      '1': 'mutation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `CreateBucketRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createBucketRequestDescriptor = $convert.base64Decode(
    'ChNDcmVhdGVCdWNrZXRSZXF1ZXN0EhIKBG5hbWUYASABKAlSBG5hbWUSQAoIbXV0YXRpb24YAi'
    'ABKAsyJC5hY3ljbGljLm9iamVjdHMudjEuTXV0YXRpb25JZGVudGl0eVIIbXV0YXRpb24=');

@$core.Deprecated('Use headBucketRequestDescriptor instead')
const HeadBucketRequest$json = {
  '1': 'HeadBucketRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
  ],
};

/// Descriptor for `HeadBucketRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List headBucketRequestDescriptor = $convert.base64Decode(
    'ChFIZWFkQnVja2V0UmVxdWVzdBI1CgZidWNrZXQYASABKAsyHS5hY3ljbGljLm9iamVjdHMudj'
    'EuQnVja2V0UmVmUgZidWNrZXQ=');

@$core.Deprecated('Use deleteBucketRequestDescriptor instead')
const DeleteBucketRequest$json = {
  '1': 'DeleteBucketRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {
      '1': 'mutation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `DeleteBucketRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deleteBucketRequestDescriptor = $convert.base64Decode(
    'ChNEZWxldGVCdWNrZXRSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy'
    '52MS5CdWNrZXRSZWZSBmJ1Y2tldBJACghtdXRhdGlvbhgCIAEoCzIkLmFjeWNsaWMub2JqZWN0'
    'cy52MS5NdXRhdGlvbklkZW50aXR5UghtdXRhdGlvbg==');

@$core.Deprecated('Use deleteBucketResponseDescriptor instead')
const DeleteBucketResponse$json = {
  '1': 'DeleteBucketResponse',
  '2': [
    {'1': 'existed', '3': 1, '4': 1, '5': 8, '10': 'existed'},
  ],
};

/// Descriptor for `DeleteBucketResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deleteBucketResponseDescriptor =
    $convert.base64Decode(
        'ChREZWxldGVCdWNrZXRSZXNwb25zZRIYCgdleGlzdGVkGAEgASgIUgdleGlzdGVk');

@$core.Deprecated('Use putObjectHeaderDescriptor instead')
const PutObjectHeader$json = {
  '1': 'PutObjectHeader',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {
      '1': 'metadata',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ObjectMetadata',
      '10': 'metadata'
    },
    {
      '1': 'preconditions',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.Preconditions',
      '10': 'preconditions'
    },
    {
      '1': 'mutation',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `PutObjectHeader`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List putObjectHeaderDescriptor = $convert.base64Decode(
    'Cg9QdXRPYmplY3RIZWFkZXISNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYmplY3RzLnYxLk'
    'J1Y2tldFJlZlIGYnVja2V0Eh0KCm9iamVjdF9rZXkYAiABKAlSCW9iamVjdEtleRI+CghtZXRh'
    'ZGF0YRgDIAEoCzIiLmFjeWNsaWMub2JqZWN0cy52MS5PYmplY3RNZXRhZGF0YVIIbWV0YWRhdG'
    'ESRwoNcHJlY29uZGl0aW9ucxgEIAEoCzIhLmFjeWNsaWMub2JqZWN0cy52MS5QcmVjb25kaXRp'
    'b25zUg1wcmVjb25kaXRpb25zEkAKCG11dGF0aW9uGAUgASgLMiQuYWN5Y2xpYy5vYmplY3RzLn'
    'YxLk11dGF0aW9uSWRlbnRpdHlSCG11dGF0aW9u');

@$core.Deprecated('Use putObjectRequestDescriptor instead')
const PutObjectRequest$json = {
  '1': 'PutObjectRequest',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.PutObjectHeader',
      '9': 0,
      '10': 'header'
    },
    {'1': 'body', '3': 2, '4': 1, '5': 12, '9': 0, '10': 'body'},
  ],
  '8': [
    {'1': 'frame'},
  ],
};

/// Descriptor for `PutObjectRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List putObjectRequestDescriptor = $convert.base64Decode(
    'ChBQdXRPYmplY3RSZXF1ZXN0Ej0KBmhlYWRlchgBIAEoCzIjLmFjeWNsaWMub2JqZWN0cy52MS'
    '5QdXRPYmplY3RIZWFkZXJIAFIGaGVhZGVyEhQKBGJvZHkYAiABKAxIAFIEYm9keUIHCgVmcmFt'
    'ZQ==');

@$core.Deprecated('Use objectVersionDescriptor instead')
const ObjectVersion$json = {
  '1': 'ObjectVersion',
  '2': [
    {'1': 'version_id', '3': 1, '4': 1, '5': 9, '10': 'versionId'},
    {'1': 'etag', '3': 2, '4': 1, '5': 9, '10': 'etag'},
    {'1': 'size', '3': 3, '4': 1, '5': 4, '10': 'size'},
    {'1': 'delete_marker', '3': 4, '4': 1, '5': 8, '10': 'deleteMarker'},
    {
      '1': 'metadata',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ObjectMetadata',
      '10': 'metadata'
    },
    {
      '1': 'created_at',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.google.protobuf.Timestamp',
      '10': 'createdAt'
    },
  ],
};

/// Descriptor for `ObjectVersion`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List objectVersionDescriptor = $convert.base64Decode(
    'Cg1PYmplY3RWZXJzaW9uEh0KCnZlcnNpb25faWQYASABKAlSCXZlcnNpb25JZBISCgRldGFnGA'
    'IgASgJUgRldGFnEhIKBHNpemUYAyABKARSBHNpemUSIwoNZGVsZXRlX21hcmtlchgEIAEoCFIM'
    'ZGVsZXRlTWFya2VyEj4KCG1ldGFkYXRhGAUgASgLMiIuYWN5Y2xpYy5vYmplY3RzLnYxLk9iam'
    'VjdE1ldGFkYXRhUghtZXRhZGF0YRI5CgpjcmVhdGVkX2F0GAYgASgLMhouZ29vZ2xlLnByb3Rv'
    'YnVmLlRpbWVzdGFtcFIJY3JlYXRlZEF0');

@$core.Deprecated('Use getObjectRequestDescriptor instead')
const GetObjectRequest$json = {
  '1': 'GetObjectRequest',
  '2': [
    {
      '1': 'target',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ReadTarget',
      '10': 'target'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'version_id', '3': 3, '4': 1, '5': 9, '10': 'versionId'},
    {'1': 'range_start', '3': 4, '4': 1, '5': 4, '10': 'rangeStart'},
    {
      '1': 'range_end_inclusive',
      '3': 5,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'rangeEndInclusive',
      '17': true
    },
    {'1': 'if_match', '3': 6, '4': 1, '5': 9, '10': 'ifMatch'},
    {'1': 'if_none_match', '3': 7, '4': 1, '5': 9, '10': 'ifNoneMatch'},
    {'1': 'range_requested', '3': 8, '4': 1, '5': 8, '10': 'rangeRequested'},
  ],
  '8': [
    {'1': '_range_end_inclusive'},
  ],
};

/// Descriptor for `GetObjectRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getObjectRequestDescriptor = $convert.base64Decode(
    'ChBHZXRPYmplY3RSZXF1ZXN0EjYKBnRhcmdldBgBIAEoCzIeLmFjeWNsaWMub2JqZWN0cy52MS'
    '5SZWFkVGFyZ2V0UgZ0YXJnZXQSHQoKb2JqZWN0X2tleRgCIAEoCVIJb2JqZWN0S2V5Eh0KCnZl'
    'cnNpb25faWQYAyABKAlSCXZlcnNpb25JZBIfCgtyYW5nZV9zdGFydBgEIAEoBFIKcmFuZ2VTdG'
    'FydBIzChNyYW5nZV9lbmRfaW5jbHVzaXZlGAUgASgESABSEXJhbmdlRW5kSW5jbHVzaXZliAEB'
    'EhkKCGlmX21hdGNoGAYgASgJUgdpZk1hdGNoEiIKDWlmX25vbmVfbWF0Y2gYByABKAlSC2lmTm'
    '9uZU1hdGNoEicKD3JhbmdlX3JlcXVlc3RlZBgIIAEoCFIOcmFuZ2VSZXF1ZXN0ZWRCFgoUX3Jh'
    'bmdlX2VuZF9pbmNsdXNpdmU=');

@$core.Deprecated('Use getObjectResponseDescriptor instead')
const GetObjectResponse$json = {
  '1': 'GetObjectResponse',
  '2': [
    {
      '1': 'version',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ObjectVersion',
      '9': 0,
      '10': 'version'
    },
    {'1': 'body', '3': 2, '4': 1, '5': 12, '9': 0, '10': 'body'},
  ],
  '8': [
    {'1': 'frame'},
  ],
};

/// Descriptor for `GetObjectResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getObjectResponseDescriptor = $convert.base64Decode(
    'ChFHZXRPYmplY3RSZXNwb25zZRI9Cgd2ZXJzaW9uGAEgASgLMiEuYWN5Y2xpYy5vYmplY3RzLn'
    'YxLk9iamVjdFZlcnNpb25IAFIHdmVyc2lvbhIUCgRib2R5GAIgASgMSABSBGJvZHlCBwoFZnJh'
    'bWU=');

@$core.Deprecated('Use headObjectRequestDescriptor instead')
const HeadObjectRequest$json = {
  '1': 'HeadObjectRequest',
  '2': [
    {
      '1': 'target',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ReadTarget',
      '10': 'target'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'version_id', '3': 3, '4': 1, '5': 9, '10': 'versionId'},
    {'1': 'if_match', '3': 4, '4': 1, '5': 9, '10': 'ifMatch'},
    {'1': 'if_none_match', '3': 5, '4': 1, '5': 9, '10': 'ifNoneMatch'},
  ],
};

/// Descriptor for `HeadObjectRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List headObjectRequestDescriptor = $convert.base64Decode(
    'ChFIZWFkT2JqZWN0UmVxdWVzdBI2CgZ0YXJnZXQYASABKAsyHi5hY3ljbGljLm9iamVjdHMudj'
    'EuUmVhZFRhcmdldFIGdGFyZ2V0Eh0KCm9iamVjdF9rZXkYAiABKAlSCW9iamVjdEtleRIdCgp2'
    'ZXJzaW9uX2lkGAMgASgJUgl2ZXJzaW9uSWQSGQoIaWZfbWF0Y2gYBCABKAlSB2lmTWF0Y2gSIg'
    'oNaWZfbm9uZV9tYXRjaBgFIAEoCVILaWZOb25lTWF0Y2g=');

@$core.Deprecated('Use headObjectResponseDescriptor instead')
const HeadObjectResponse$json = {
  '1': 'HeadObjectResponse',
  '2': [
    {
      '1': 'version',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ObjectVersion',
      '10': 'version'
    },
  ],
};

/// Descriptor for `HeadObjectResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List headObjectResponseDescriptor = $convert.base64Decode(
    'ChJIZWFkT2JqZWN0UmVzcG9uc2USOwoHdmVyc2lvbhgBIAEoCzIhLmFjeWNsaWMub2JqZWN0cy'
    '52MS5PYmplY3RWZXJzaW9uUgd2ZXJzaW9u');

@$core.Deprecated('Use deleteObjectRequestDescriptor instead')
const DeleteObjectRequest$json = {
  '1': 'DeleteObjectRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'version_id', '3': 3, '4': 1, '5': 9, '10': 'versionId'},
    {
      '1': 'preconditions',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.Preconditions',
      '10': 'preconditions'
    },
    {
      '1': 'mutation',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `DeleteObjectRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deleteObjectRequestDescriptor = $convert.base64Decode(
    'ChNEZWxldGVPYmplY3RSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy'
    '52MS5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkSHQoK'
    'dmVyc2lvbl9pZBgDIAEoCVIJdmVyc2lvbklkEkcKDXByZWNvbmRpdGlvbnMYBCABKAsyIS5hY3'
    'ljbGljLm9iamVjdHMudjEuUHJlY29uZGl0aW9uc1INcHJlY29uZGl0aW9ucxJACghtdXRhdGlv'
    'bhgFIAEoCzIkLmFjeWNsaWMub2JqZWN0cy52MS5NdXRhdGlvbklkZW50aXR5UghtdXRhdGlvbg'
    '==');

@$core.Deprecated('Use deleteObjectResponseDescriptor instead')
const DeleteObjectResponse$json = {
  '1': 'DeleteObjectResponse',
  '2': [
    {'1': 'existed', '3': 1, '4': 1, '5': 8, '10': 'existed'},
    {
      '1': 'version',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ObjectVersion',
      '10': 'version'
    },
  ],
};

/// Descriptor for `DeleteObjectResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deleteObjectResponseDescriptor = $convert.base64Decode(
    'ChREZWxldGVPYmplY3RSZXNwb25zZRIYCgdleGlzdGVkGAEgASgIUgdleGlzdGVkEjsKB3Zlcn'
    'Npb24YAiABKAsyIS5hY3ljbGljLm9iamVjdHMudjEuT2JqZWN0VmVyc2lvblIHdmVyc2lvbg==');

@$core.Deprecated('Use listObjectsRequestDescriptor instead')
const ListObjectsRequest$json = {
  '1': 'ListObjectsRequest',
  '2': [
    {
      '1': 'target',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ReadTarget',
      '10': 'target'
    },
    {'1': 'prefix', '3': 2, '4': 1, '5': 9, '10': 'prefix'},
    {'1': 'delimiter', '3': 3, '4': 1, '5': 9, '10': 'delimiter'},
    {
      '1': 'mode',
      '3': 4,
      '4': 1,
      '5': 14,
      '6': '.acyclic.objects.v1.ListingMode',
      '10': 'mode'
    },
    {'1': 'page_size', '3': 5, '4': 1, '5': 13, '10': 'pageSize'},
    {
      '1': 'continuation_token',
      '3': 6,
      '4': 1,
      '5': 9,
      '10': 'continuationToken'
    },
  ],
};

/// Descriptor for `ListObjectsRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listObjectsRequestDescriptor = $convert.base64Decode(
    'ChJMaXN0T2JqZWN0c1JlcXVlc3QSNgoGdGFyZ2V0GAEgASgLMh4uYWN5Y2xpYy5vYmplY3RzLn'
    'YxLlJlYWRUYXJnZXRSBnRhcmdldBIWCgZwcmVmaXgYAiABKAlSBnByZWZpeBIcCglkZWxpbWl0'
    'ZXIYAyABKAlSCWRlbGltaXRlchIzCgRtb2RlGAQgASgOMh8uYWN5Y2xpYy5vYmplY3RzLnYxLk'
    'xpc3RpbmdNb2RlUgRtb2RlEhsKCXBhZ2Vfc2l6ZRgFIAEoDVIIcGFnZVNpemUSLQoSY29udGlu'
    'dWF0aW9uX3Rva2VuGAYgASgJUhFjb250aW51YXRpb25Ub2tlbg==');

@$core.Deprecated('Use listEntryDescriptor instead')
const ListEntry$json = {
  '1': 'ListEntry',
  '2': [
    {'1': 'object_key', '3': 1, '4': 1, '5': 9, '10': 'objectKey'},
    {
      '1': 'version',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ObjectVersion',
      '10': 'version'
    },
  ],
};

/// Descriptor for `ListEntry`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listEntryDescriptor = $convert.base64Decode(
    'CglMaXN0RW50cnkSHQoKb2JqZWN0X2tleRgBIAEoCVIJb2JqZWN0S2V5EjsKB3ZlcnNpb24YAi'
    'ABKAsyIS5hY3ljbGljLm9iamVjdHMudjEuT2JqZWN0VmVyc2lvblIHdmVyc2lvbg==');

@$core.Deprecated('Use listObjectsResponseDescriptor instead')
const ListObjectsResponse$json = {
  '1': 'ListObjectsResponse',
  '2': [
    {
      '1': 'entries',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.objects.v1.ListEntry',
      '10': 'entries'
    },
    {'1': 'common_prefixes', '3': 2, '4': 3, '5': 9, '10': 'commonPrefixes'},
    {
      '1': 'continuation_token',
      '3': 3,
      '4': 1,
      '5': 9,
      '10': 'continuationToken'
    },
  ],
};

/// Descriptor for `ListObjectsResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listObjectsResponseDescriptor = $convert.base64Decode(
    'ChNMaXN0T2JqZWN0c1Jlc3BvbnNlEjcKB2VudHJpZXMYASADKAsyHS5hY3ljbGljLm9iamVjdH'
    'MudjEuTGlzdEVudHJ5UgdlbnRyaWVzEicKD2NvbW1vbl9wcmVmaXhlcxgCIAMoCVIOY29tbW9u'
    'UHJlZml4ZXMSLQoSY29udGludWF0aW9uX3Rva2VuGAMgASgJUhFjb250aW51YXRpb25Ub2tlbg'
    '==');

@$core.Deprecated('Use createMultipartRequestDescriptor instead')
const CreateMultipartRequest$json = {
  '1': 'CreateMultipartRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {
      '1': 'metadata',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.ObjectMetadata',
      '10': 'metadata'
    },
    {
      '1': 'preconditions',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.Preconditions',
      '10': 'preconditions'
    },
    {
      '1': 'mutation',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `CreateMultipartRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createMultipartRequestDescriptor = $convert.base64Decode(
    'ChZDcmVhdGVNdWx0aXBhcnRSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZW'
    'N0cy52MS5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkS'
    'PgoIbWV0YWRhdGEYAyABKAsyIi5hY3ljbGljLm9iamVjdHMudjEuT2JqZWN0TWV0YWRhdGFSCG'
    '1ldGFkYXRhEkcKDXByZWNvbmRpdGlvbnMYBCABKAsyIS5hY3ljbGljLm9iamVjdHMudjEuUHJl'
    'Y29uZGl0aW9uc1INcHJlY29uZGl0aW9ucxJACghtdXRhdGlvbhgFIAEoCzIkLmFjeWNsaWMub2'
    'JqZWN0cy52MS5NdXRhdGlvbklkZW50aXR5UghtdXRhdGlvbg==');

@$core.Deprecated('Use multipartUploadDescriptor instead')
const MultipartUpload$json = {
  '1': 'MultipartUpload',
  '2': [
    {'1': 'upload_id', '3': 1, '4': 1, '5': 9, '10': 'uploadId'},
  ],
};

/// Descriptor for `MultipartUpload`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List multipartUploadDescriptor = $convert.base64Decode(
    'Cg9NdWx0aXBhcnRVcGxvYWQSGwoJdXBsb2FkX2lkGAEgASgJUgh1cGxvYWRJZA==');

@$core.Deprecated('Use uploadPartHeaderDescriptor instead')
const UploadPartHeader$json = {
  '1': 'UploadPartHeader',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'upload_id', '3': 3, '4': 1, '5': 9, '10': 'uploadId'},
    {'1': 'part_number', '3': 4, '4': 1, '5': 13, '10': 'partNumber'},
    {
      '1': 'mutation',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `UploadPartHeader`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List uploadPartHeaderDescriptor = $convert.base64Decode(
    'ChBVcGxvYWRQYXJ0SGVhZGVyEjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy52MS'
    '5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkSGwoJdXBs'
    'b2FkX2lkGAMgASgJUgh1cGxvYWRJZBIfCgtwYXJ0X251bWJlchgEIAEoDVIKcGFydE51bWJlch'
    'JACghtdXRhdGlvbhgFIAEoCzIkLmFjeWNsaWMub2JqZWN0cy52MS5NdXRhdGlvbklkZW50aXR5'
    'UghtdXRhdGlvbg==');

@$core.Deprecated('Use uploadPartRequestDescriptor instead')
const UploadPartRequest$json = {
  '1': 'UploadPartRequest',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.UploadPartHeader',
      '9': 0,
      '10': 'header'
    },
    {'1': 'body', '3': 2, '4': 1, '5': 12, '9': 0, '10': 'body'},
  ],
  '8': [
    {'1': 'frame'},
  ],
};

/// Descriptor for `UploadPartRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List uploadPartRequestDescriptor = $convert.base64Decode(
    'ChFVcGxvYWRQYXJ0UmVxdWVzdBI+CgZoZWFkZXIYASABKAsyJC5hY3ljbGljLm9iamVjdHMudj'
    'EuVXBsb2FkUGFydEhlYWRlckgAUgZoZWFkZXISFAoEYm9keRgCIAEoDEgAUgRib2R5QgcKBWZy'
    'YW1l');

@$core.Deprecated('Use uploadedPartDescriptor instead')
const UploadedPart$json = {
  '1': 'UploadedPart',
  '2': [
    {'1': 'part_number', '3': 1, '4': 1, '5': 13, '10': 'partNumber'},
    {'1': 'etag', '3': 2, '4': 1, '5': 9, '10': 'etag'},
    {'1': 'size', '3': 3, '4': 1, '5': 4, '10': 'size'},
  ],
};

/// Descriptor for `UploadedPart`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List uploadedPartDescriptor = $convert.base64Decode(
    'CgxVcGxvYWRlZFBhcnQSHwoLcGFydF9udW1iZXIYASABKA1SCnBhcnROdW1iZXISEgoEZXRhZx'
    'gCIAEoCVIEZXRhZxISCgRzaXplGAMgASgEUgRzaXpl');

@$core.Deprecated('Use listPartsRequestDescriptor instead')
const ListPartsRequest$json = {
  '1': 'ListPartsRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'upload_id', '3': 3, '4': 1, '5': 9, '10': 'uploadId'},
  ],
};

/// Descriptor for `ListPartsRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listPartsRequestDescriptor = $convert.base64Decode(
    'ChBMaXN0UGFydHNSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy52MS'
    '5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkSGwoJdXBs'
    'b2FkX2lkGAMgASgJUgh1cGxvYWRJZA==');

@$core.Deprecated('Use listPartsResponseDescriptor instead')
const ListPartsResponse$json = {
  '1': 'ListPartsResponse',
  '2': [
    {
      '1': 'parts',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.objects.v1.UploadedPart',
      '10': 'parts'
    },
  ],
};

/// Descriptor for `ListPartsResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listPartsResponseDescriptor = $convert.base64Decode(
    'ChFMaXN0UGFydHNSZXNwb25zZRI2CgVwYXJ0cxgBIAMoCzIgLmFjeWNsaWMub2JqZWN0cy52MS'
    '5VcGxvYWRlZFBhcnRSBXBhcnRz');

@$core.Deprecated('Use completeMultipartRequestDescriptor instead')
const CompleteMultipartRequest$json = {
  '1': 'CompleteMultipartRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'upload_id', '3': 3, '4': 1, '5': 9, '10': 'uploadId'},
    {
      '1': 'parts',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.objects.v1.UploadedPart',
      '10': 'parts'
    },
    {
      '1': 'mutation',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `CompleteMultipartRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List completeMultipartRequestDescriptor = $convert.base64Decode(
    'ChhDb21wbGV0ZU11bHRpcGFydFJlcXVlc3QSNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYm'
    'plY3RzLnYxLkJ1Y2tldFJlZlIGYnVja2V0Eh0KCm9iamVjdF9rZXkYAiABKAlSCW9iamVjdEtl'
    'eRIbCgl1cGxvYWRfaWQYAyABKAlSCHVwbG9hZElkEjYKBXBhcnRzGAQgAygLMiAuYWN5Y2xpYy'
    '5vYmplY3RzLnYxLlVwbG9hZGVkUGFydFIFcGFydHMSQAoIbXV0YXRpb24YBSABKAsyJC5hY3lj'
    'bGljLm9iamVjdHMudjEuTXV0YXRpb25JZGVudGl0eVIIbXV0YXRpb24=');

@$core.Deprecated('Use abortMultipartRequestDescriptor instead')
const AbortMultipartRequest$json = {
  '1': 'AbortMultipartRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'upload_id', '3': 3, '4': 1, '5': 9, '10': 'uploadId'},
    {
      '1': 'mutation',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `AbortMultipartRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List abortMultipartRequestDescriptor = $convert.base64Decode(
    'ChVBYm9ydE11bHRpcGFydFJlcXVlc3QSNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYmplY3'
    'RzLnYxLkJ1Y2tldFJlZlIGYnVja2V0Eh0KCm9iamVjdF9rZXkYAiABKAlSCW9iamVjdEtleRIb'
    'Cgl1cGxvYWRfaWQYAyABKAlSCHVwbG9hZElkEkAKCG11dGF0aW9uGAQgASgLMiQuYWN5Y2xpYy'
    '5vYmplY3RzLnYxLk11dGF0aW9uSWRlbnRpdHlSCG11dGF0aW9u');

@$core.Deprecated('Use abortMultipartResponseDescriptor instead')
const AbortMultipartResponse$json = {
  '1': 'AbortMultipartResponse',
  '2': [
    {'1': 'existed', '3': 1, '4': 1, '5': 8, '10': 'existed'},
  ],
};

/// Descriptor for `AbortMultipartResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List abortMultipartResponseDescriptor =
    $convert.base64Decode(
        'ChZBYm9ydE11bHRpcGFydFJlc3BvbnNlEhgKB2V4aXN0ZWQYASABKAhSB2V4aXN0ZWQ=');

@$core.Deprecated('Use snapshotDescriptor instead')
const Snapshot$json = {
  '1': 'Snapshot',
  '2': [
    {
      '1': 'snapshot',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.SnapshotRef',
      '10': 'snapshot'
    },
    {
      '1': 'created_at',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.google.protobuf.Timestamp',
      '10': 'createdAt'
    },
  ],
};

/// Descriptor for `Snapshot`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List snapshotDescriptor = $convert.base64Decode(
    'CghTbmFwc2hvdBI7CghzbmFwc2hvdBgBIAEoCzIfLmFjeWNsaWMub2JqZWN0cy52MS5TbmFwc2'
    'hvdFJlZlIIc25hcHNob3QSOQoKY3JlYXRlZF9hdBgCIAEoCzIaLmdvb2dsZS5wcm90b2J1Zi5U'
    'aW1lc3RhbXBSCWNyZWF0ZWRBdA==');

@$core.Deprecated('Use createSnapshotRequestDescriptor instead')
const CreateSnapshotRequest$json = {
  '1': 'CreateSnapshotRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'bucket'
    },
    {
      '1': 'mutation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `CreateSnapshotRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createSnapshotRequestDescriptor = $convert.base64Decode(
    'ChVDcmVhdGVTbmFwc2hvdFJlcXVlc3QSNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYmplY3'
    'RzLnYxLkJ1Y2tldFJlZlIGYnVja2V0EkAKCG11dGF0aW9uGAIgASgLMiQuYWN5Y2xpYy5vYmpl'
    'Y3RzLnYxLk11dGF0aW9uSWRlbnRpdHlSCG11dGF0aW9u');

@$core.Deprecated('Use destroySnapshotRequestDescriptor instead')
const DestroySnapshotRequest$json = {
  '1': 'DestroySnapshotRequest',
  '2': [
    {
      '1': 'snapshot',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.SnapshotRef',
      '10': 'snapshot'
    },
    {
      '1': 'mutation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `DestroySnapshotRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List destroySnapshotRequestDescriptor = $convert.base64Decode(
    'ChZEZXN0cm95U25hcHNob3RSZXF1ZXN0EjsKCHNuYXBzaG90GAEgASgLMh8uYWN5Y2xpYy5vYm'
    'plY3RzLnYxLlNuYXBzaG90UmVmUghzbmFwc2hvdBJACghtdXRhdGlvbhgCIAEoCzIkLmFjeWNs'
    'aWMub2JqZWN0cy52MS5NdXRhdGlvbklkZW50aXR5UghtdXRhdGlvbg==');

@$core.Deprecated('Use destroySnapshotResponseDescriptor instead')
const DestroySnapshotResponse$json = {
  '1': 'DestroySnapshotResponse',
  '2': [
    {'1': 'existed', '3': 1, '4': 1, '5': 8, '10': 'existed'},
  ],
};

/// Descriptor for `DestroySnapshotResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List destroySnapshotResponseDescriptor =
    $convert.base64Decode(
        'ChdEZXN0cm95U25hcHNob3RSZXNwb25zZRIYCgdleGlzdGVkGAEgASgIUgdleGlzdGVk');

@$core.Deprecated('Use forkSnapshotRequestDescriptor instead')
const ForkSnapshotRequest$json = {
  '1': 'ForkSnapshotRequest',
  '2': [
    {
      '1': 'snapshot',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.SnapshotRef',
      '10': 'snapshot'
    },
    {'1': 'destination_name', '3': 2, '4': 1, '5': 9, '10': 'destinationName'},
    {
      '1': 'mutation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `ForkSnapshotRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkSnapshotRequestDescriptor = $convert.base64Decode(
    'ChNGb3JrU25hcHNob3RSZXF1ZXN0EjsKCHNuYXBzaG90GAEgASgLMh8uYWN5Y2xpYy5vYmplY3'
    'RzLnYxLlNuYXBzaG90UmVmUghzbmFwc2hvdBIpChBkZXN0aW5hdGlvbl9uYW1lGAIgASgJUg9k'
    'ZXN0aW5hdGlvbk5hbWUSQAoIbXV0YXRpb24YAyABKAsyJC5hY3ljbGljLm9iamVjdHMudjEuTX'
    'V0YXRpb25JZGVudGl0eVIIbXV0YXRpb24=');

@$core.Deprecated('Use forkBucketRequestDescriptor instead')
const ForkBucketRequest$json = {
  '1': 'ForkBucketRequest',
  '2': [
    {
      '1': 'source',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.BucketRef',
      '10': 'source'
    },
    {'1': 'destination_name', '3': 2, '4': 1, '5': 9, '10': 'destinationName'},
    {
      '1': 'mutation',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v1.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `ForkBucketRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List forkBucketRequestDescriptor = $convert.base64Decode(
    'ChFGb3JrQnVja2V0UmVxdWVzdBI1CgZzb3VyY2UYASABKAsyHS5hY3ljbGljLm9iamVjdHMudj'
    'EuQnVja2V0UmVmUgZzb3VyY2USKQoQZGVzdGluYXRpb25fbmFtZRgCIAEoCVIPZGVzdGluYXRp'
    'b25OYW1lEkAKCG11dGF0aW9uGAMgASgLMiQuYWN5Y2xpYy5vYmplY3RzLnYxLk11dGF0aW9uSW'
    'RlbnRpdHlSCG11dGF0aW9u');

@$core.Deprecated('Use errorDetailDescriptor instead')
const ErrorDetail$json = {
  '1': 'ErrorDetail',
  '2': [
    {
      '1': 'code',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.objects.v1.ErrorCode',
      '10': 'code'
    },
    {'1': 'request_id', '3': 2, '4': 1, '5': 9, '10': 'requestId'},
  ],
};

/// Descriptor for `ErrorDetail`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List errorDetailDescriptor = $convert.base64Decode(
    'CgtFcnJvckRldGFpbBIxCgRjb2RlGAEgASgOMh0uYWN5Y2xpYy5vYmplY3RzLnYxLkVycm9yQ2'
    '9kZVIEY29kZRIdCgpyZXF1ZXN0X2lkGAIgASgJUglyZXF1ZXN0SWQ=');
