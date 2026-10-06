// This is a generated file - do not edit.
//
// Generated from objects/v2/objects.proto.

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
    {'1': 'OBJECTS_LIMIT_MAX_KEY_BYTES', '2': 1024},
    {'1': 'OBJECTS_LIMIT_MAX_USER_METADATA_BYTES', '2': 2048},
    {'1': 'OBJECTS_LIMIT_MAX_PAGE_ENTRIES', '2': 1000},
    {'1': 'OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES', '2': 65536},
    {'1': 'OBJECTS_LIMIT_MAX_MULTIPART_PARTS', '2': 10000},
  ],
};

/// Descriptor for `ObjectsLimit`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List objectsLimitDescriptor = $convert.base64Decode(
    'CgxPYmplY3RzTGltaXQSHQoZT0JKRUNUU19MSU1JVF9VTlNQRUNJRklFRBAAEiwKJ09CSkVDVF'
    'NfTElNSVRfTUFYX0lERU1QT1RFTkNZX0tFWV9CWVRFUxCAAhIgChtPQkpFQ1RTX0xJTUlUX01B'
    'WF9LRVlfQllURVMQgAgSKgolT0JKRUNUU19MSU1JVF9NQVhfVVNFUl9NRVRBREFUQV9CWVRFUx'
    'CAEBIjCh5PQkpFQ1RTX0xJTUlUX01BWF9QQUdFX0VOVFJJRVMQ6AcSKAoiT0JKRUNUU19MSU1J'
    'VF9NQVhfQk9EWV9GUkFNRV9CWVRFUxCAgAQSJgohT0JKRUNUU19MSU1JVF9NQVhfTVVMVElQQV'
    'JUX1BBUlRTEJBO');

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
    {'1': 'ERROR_CODE_QUOTA_EXCEEDED', '2': 6},
    {'1': 'ERROR_CODE_UNSUPPORTED', '2': 7},
    {'1': 'ERROR_CODE_UNAVAILABLE', '2': 8},
    {'1': 'ERROR_CODE_ACCESS_DENIED', '2': 9},
    {'1': 'ERROR_CODE_RANGE_NOT_SATISFIABLE', '2': 10},
    {'1': 'ERROR_CODE_NOT_MODIFIED', '2': 11},
  ],
};

/// Descriptor for `ErrorCode`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List errorCodeDescriptor = $convert.base64Decode(
    'CglFcnJvckNvZGUSGgoWRVJST1JfQ09ERV9VTlNQRUNJRklFRBAAEh8KG0VSUk9SX0NPREVfSU'
    '5WQUxJRF9BUkdVTUVOVBABEhgKFEVSUk9SX0NPREVfTk9UX0ZPVU5EEAISHQoZRVJST1JfQ09E'
    'RV9BTFJFQURZX0VYSVNUUxADEiIKHkVSUk9SX0NPREVfUFJFQ09ORElUSU9OX0ZBSUxFRBAEEi'
    'MKH0VSUk9SX0NPREVfSURFTVBPVEVOQ1lfTUlTTUFUQ0gQBRIdChlFUlJPUl9DT0RFX1FVT1RB'
    'X0VYQ0VFREVEEAYSGgoWRVJST1JfQ09ERV9VTlNVUFBPUlRFRBAHEhoKFkVSUk9SX0NPREVfVU'
    '5BVkFJTEFCTEUQCBIcChhFUlJPUl9DT0RFX0FDQ0VTU19ERU5JRUQQCRIkCiBFUlJPUl9DT0RF'
    'X1JBTkdFX05PVF9TQVRJU0ZJQUJMRRAKEhsKF0VSUk9SX0NPREVfTk9UX01PRElGSUVEEAs=');

@$core.Deprecated('Use bucketRefDescriptor instead')
const BucketRef$json = {
  '1': 'BucketRef',
  '2': [
    {'1': 'name', '3': 1, '4': 1, '5': 9, '10': 'name'},
  ],
};

/// Descriptor for `BucketRef`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List bucketRefDescriptor =
    $convert.base64Decode('CglCdWNrZXRSZWYSEgoEbmFtZRgBIAEoCVIEbmFtZQ==');

@$core.Deprecated('Use bucketDescriptor instead')
const Bucket$json = {
  '1': 'Bucket',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
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
    'CgZCdWNrZXQSNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYmplY3RzLnYyLkJ1Y2tldFJlZl'
    'IGYnVja2V0EjkKCmNyZWF0ZWRfYXQYAiABKAsyGi5nb29nbGUucHJvdG9idWYuVGltZXN0YW1w'
    'UgljcmVhdGVkQXQ=');

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
      '6': '.acyclic.objects.v2.ObjectMetadata.UserEntry',
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
    'VzZXIYAiADKAsyLC5hY3ljbGljLm9iamVjdHMudjIuT2JqZWN0TWV0YWRhdGEuVXNlckVudHJ5'
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
  ],
  '8': [
    {'1': 'condition'},
  ],
};

/// Descriptor for `Preconditions`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List preconditionsDescriptor = $convert.base64Decode(
    'Cg1QcmVjb25kaXRpb25zEh0KCWlmX2Fic2VudBgBIAEoCEgAUghpZkFic2VudBIbCghpZl9tYX'
    'RjaBgCIAEoCUgAUgdpZk1hdGNoQgsKCWNvbmRpdGlvbg==');

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

@$core.Deprecated('Use objectInfoDescriptor instead')
const ObjectInfo$json = {
  '1': 'ObjectInfo',
  '2': [
    {'1': 'etag', '3': 1, '4': 1, '5': 9, '10': 'etag'},
    {'1': 'size', '3': 2, '4': 1, '5': 4, '10': 'size'},
    {
      '1': 'metadata',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ObjectMetadata',
      '10': 'metadata'
    },
    {
      '1': 'last_modified',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.google.protobuf.Timestamp',
      '10': 'lastModified'
    },
  ],
};

/// Descriptor for `ObjectInfo`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List objectInfoDescriptor = $convert.base64Decode(
    'CgpPYmplY3RJbmZvEhIKBGV0YWcYASABKAlSBGV0YWcSEgoEc2l6ZRgCIAEoBFIEc2l6ZRI+Cg'
    'htZXRhZGF0YRgDIAEoCzIiLmFjeWNsaWMub2JqZWN0cy52Mi5PYmplY3RNZXRhZGF0YVIIbWV0'
    'YWRhdGESPwoNbGFzdF9tb2RpZmllZBgEIAEoCzIaLmdvb2dsZS5wcm90b2J1Zi5UaW1lc3RhbX'
    'BSDGxhc3RNb2RpZmllZA==');

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
      '6': '.acyclic.objects.v2.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `CreateBucketRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createBucketRequestDescriptor = $convert.base64Decode(
    'ChNDcmVhdGVCdWNrZXRSZXF1ZXN0EhIKBG5hbWUYASABKAlSBG5hbWUSQAoIbXV0YXRpb24YAi'
    'ABKAsyJC5hY3ljbGljLm9iamVjdHMudjIuTXV0YXRpb25JZGVudGl0eVIIbXV0YXRpb24=');

@$core.Deprecated('Use headBucketRequestDescriptor instead')
const HeadBucketRequest$json = {
  '1': 'HeadBucketRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
  ],
};

/// Descriptor for `HeadBucketRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List headBucketRequestDescriptor = $convert.base64Decode(
    'ChFIZWFkQnVja2V0UmVxdWVzdBI1CgZidWNrZXQYASABKAsyHS5hY3ljbGljLm9iamVjdHMudj'
    'IuQnVja2V0UmVmUgZidWNrZXQ=');

@$core.Deprecated('Use deleteBucketRequestDescriptor instead')
const DeleteBucketRequest$json = {
  '1': 'DeleteBucketRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {
      '1': 'mutation',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `DeleteBucketRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deleteBucketRequestDescriptor = $convert.base64Decode(
    'ChNEZWxldGVCdWNrZXRSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy'
    '52Mi5CdWNrZXRSZWZSBmJ1Y2tldBJACghtdXRhdGlvbhgCIAEoCzIkLmFjeWNsaWMub2JqZWN0'
    'cy52Mi5NdXRhdGlvbklkZW50aXR5UghtdXRhdGlvbg==');

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
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {
      '1': 'metadata',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ObjectMetadata',
      '10': 'metadata'
    },
    {
      '1': 'preconditions',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.Preconditions',
      '10': 'preconditions'
    },
    {
      '1': 'mutation',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `PutObjectHeader`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List putObjectHeaderDescriptor = $convert.base64Decode(
    'Cg9QdXRPYmplY3RIZWFkZXISNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYmplY3RzLnYyLk'
    'J1Y2tldFJlZlIGYnVja2V0Eh0KCm9iamVjdF9rZXkYAiABKAlSCW9iamVjdEtleRI+CghtZXRh'
    'ZGF0YRgDIAEoCzIiLmFjeWNsaWMub2JqZWN0cy52Mi5PYmplY3RNZXRhZGF0YVIIbWV0YWRhdG'
    'ESRwoNcHJlY29uZGl0aW9ucxgEIAEoCzIhLmFjeWNsaWMub2JqZWN0cy52Mi5QcmVjb25kaXRp'
    'b25zUg1wcmVjb25kaXRpb25zEkAKCG11dGF0aW9uGAUgASgLMiQuYWN5Y2xpYy5vYmplY3RzLn'
    'YyLk11dGF0aW9uSWRlbnRpdHlSCG11dGF0aW9u');

@$core.Deprecated('Use putObjectRequestDescriptor instead')
const PutObjectRequest$json = {
  '1': 'PutObjectRequest',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.PutObjectHeader',
      '9': 0,
      '10': 'header'
    },
    {'1': 'body', '3': 2, '4': 1, '5': 12, '9': 0, '10': 'body'},
    {'1': 'complete', '3': 3, '4': 1, '5': 8, '9': 0, '10': 'complete'},
  ],
  '8': [
    {'1': 'frame'},
  ],
};

/// Descriptor for `PutObjectRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List putObjectRequestDescriptor = $convert.base64Decode(
    'ChBQdXRPYmplY3RSZXF1ZXN0Ej0KBmhlYWRlchgBIAEoCzIjLmFjeWNsaWMub2JqZWN0cy52Mi'
    '5QdXRPYmplY3RIZWFkZXJIAFIGaGVhZGVyEhQKBGJvZHkYAiABKAxIAFIEYm9keRIcCghjb21w'
    'bGV0ZRgDIAEoCEgAUghjb21wbGV0ZUIHCgVmcmFtZQ==');

@$core.Deprecated('Use inclusiveRangeDescriptor instead')
const InclusiveRange$json = {
  '1': 'InclusiveRange',
  '2': [
    {'1': 'start', '3': 1, '4': 1, '5': 4, '10': 'start'},
    {'1': 'end', '3': 2, '4': 1, '5': 4, '9': 0, '10': 'end', '17': true},
  ],
  '8': [
    {'1': '_end'},
  ],
};

/// Descriptor for `InclusiveRange`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List inclusiveRangeDescriptor = $convert.base64Decode(
    'Cg5JbmNsdXNpdmVSYW5nZRIUCgVzdGFydBgBIAEoBFIFc3RhcnQSFQoDZW5kGAIgASgESABSA2'
    'VuZIgBAUIGCgRfZW5k');

@$core.Deprecated('Use byteRangeDescriptor instead')
const ByteRange$json = {
  '1': 'ByteRange',
  '2': [
    {
      '1': 'bytes',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.InclusiveRange',
      '9': 0,
      '10': 'bytes'
    },
    {
      '1': 'suffix_length',
      '3': 2,
      '4': 1,
      '5': 4,
      '9': 0,
      '10': 'suffixLength'
    },
  ],
  '8': [
    {'1': 'selection'},
  ],
};

/// Descriptor for `ByteRange`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List byteRangeDescriptor = $convert.base64Decode(
    'CglCeXRlUmFuZ2USOgoFYnl0ZXMYASABKAsyIi5hY3ljbGljLm9iamVjdHMudjIuSW5jbHVzaX'
    'ZlUmFuZ2VIAFIFYnl0ZXMSJQoNc3VmZml4X2xlbmd0aBgCIAEoBEgAUgxzdWZmaXhMZW5ndGhC'
    'CwoJc2VsZWN0aW9u');

@$core.Deprecated('Use getObjectRequestDescriptor instead')
const GetObjectRequest$json = {
  '1': 'GetObjectRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {
      '1': 'range',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ByteRange',
      '10': 'range'
    },
    {'1': 'if_match', '3': 4, '4': 1, '5': 9, '10': 'ifMatch'},
    {'1': 'if_none_match', '3': 5, '4': 1, '5': 9, '10': 'ifNoneMatch'},
  ],
};

/// Descriptor for `GetObjectRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getObjectRequestDescriptor = $convert.base64Decode(
    'ChBHZXRPYmplY3RSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy52Mi'
    '5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkSMwoFcmFu'
    'Z2UYAyABKAsyHS5hY3ljbGljLm9iamVjdHMudjIuQnl0ZVJhbmdlUgVyYW5nZRIZCghpZl9tYX'
    'RjaBgEIAEoCVIHaWZNYXRjaBIiCg1pZl9ub25lX21hdGNoGAUgASgJUgtpZk5vbmVNYXRjaA==');

@$core.Deprecated('Use contentRangeDescriptor instead')
const ContentRange$json = {
  '1': 'ContentRange',
  '2': [
    {'1': 'start', '3': 1, '4': 1, '5': 4, '10': 'start'},
    {'1': 'end', '3': 2, '4': 1, '5': 4, '10': 'end'},
    {'1': 'total', '3': 3, '4': 1, '5': 4, '10': 'total'},
  ],
};

/// Descriptor for `ContentRange`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List contentRangeDescriptor = $convert.base64Decode(
    'CgxDb250ZW50UmFuZ2USFAoFc3RhcnQYASABKARSBXN0YXJ0EhAKA2VuZBgCIAEoBFIDZW5kEh'
    'QKBXRvdGFsGAMgASgEUgV0b3RhbA==');

@$core.Deprecated('Use getObjectHeaderDescriptor instead')
const GetObjectHeader$json = {
  '1': 'GetObjectHeader',
  '2': [
    {
      '1': 'object',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ObjectInfo',
      '10': 'object'
    },
    {
      '1': 'content_range',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ContentRange',
      '10': 'contentRange'
    },
  ],
};

/// Descriptor for `GetObjectHeader`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getObjectHeaderDescriptor = $convert.base64Decode(
    'Cg9HZXRPYmplY3RIZWFkZXISNgoGb2JqZWN0GAEgASgLMh4uYWN5Y2xpYy5vYmplY3RzLnYyLk'
    '9iamVjdEluZm9SBm9iamVjdBJFCg1jb250ZW50X3JhbmdlGAIgASgLMiAuYWN5Y2xpYy5vYmpl'
    'Y3RzLnYyLkNvbnRlbnRSYW5nZVIMY29udGVudFJhbmdl');

@$core.Deprecated('Use getObjectResponseDescriptor instead')
const GetObjectResponse$json = {
  '1': 'GetObjectResponse',
  '2': [
    {
      '1': 'header',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.GetObjectHeader',
      '9': 0,
      '10': 'header'
    },
    {'1': 'body', '3': 2, '4': 1, '5': 12, '9': 0, '10': 'body'},
    {
      '1': 'error',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ErrorDetail',
      '9': 0,
      '10': 'error'
    },
  ],
  '8': [
    {'1': 'frame'},
  ],
};

/// Descriptor for `GetObjectResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List getObjectResponseDescriptor = $convert.base64Decode(
    'ChFHZXRPYmplY3RSZXNwb25zZRI9CgZoZWFkZXIYASABKAsyIy5hY3ljbGljLm9iamVjdHMudj'
    'IuR2V0T2JqZWN0SGVhZGVySABSBmhlYWRlchIUCgRib2R5GAIgASgMSABSBGJvZHkSNwoFZXJy'
    'b3IYAyABKAsyHy5hY3ljbGljLm9iamVjdHMudjIuRXJyb3JEZXRhaWxIAFIFZXJyb3JCBwoFZn'
    'JhbWU=');

@$core.Deprecated('Use headObjectRequestDescriptor instead')
const HeadObjectRequest$json = {
  '1': 'HeadObjectRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'if_match', '3': 3, '4': 1, '5': 9, '10': 'ifMatch'},
    {'1': 'if_none_match', '3': 4, '4': 1, '5': 9, '10': 'ifNoneMatch'},
  ],
};

/// Descriptor for `HeadObjectRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List headObjectRequestDescriptor = $convert.base64Decode(
    'ChFIZWFkT2JqZWN0UmVxdWVzdBI1CgZidWNrZXQYASABKAsyHS5hY3ljbGljLm9iamVjdHMudj'
    'IuQnVja2V0UmVmUgZidWNrZXQSHQoKb2JqZWN0X2tleRgCIAEoCVIJb2JqZWN0S2V5EhkKCGlm'
    'X21hdGNoGAMgASgJUgdpZk1hdGNoEiIKDWlmX25vbmVfbWF0Y2gYBCABKAlSC2lmTm9uZU1hdG'
    'No');

@$core.Deprecated('Use headObjectResponseDescriptor instead')
const HeadObjectResponse$json = {
  '1': 'HeadObjectResponse',
  '2': [
    {
      '1': 'object',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ObjectInfo',
      '10': 'object'
    },
  ],
};

/// Descriptor for `HeadObjectResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List headObjectResponseDescriptor = $convert.base64Decode(
    'ChJIZWFkT2JqZWN0UmVzcG9uc2USNgoGb2JqZWN0GAEgASgLMh4uYWN5Y2xpYy5vYmplY3RzLn'
    'YyLk9iamVjdEluZm9SBm9iamVjdA==');

@$core.Deprecated('Use deleteObjectRequestDescriptor instead')
const DeleteObjectRequest$json = {
  '1': 'DeleteObjectRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {
      '1': 'preconditions',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.Preconditions',
      '10': 'preconditions'
    },
    {
      '1': 'mutation',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `DeleteObjectRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deleteObjectRequestDescriptor = $convert.base64Decode(
    'ChNEZWxldGVPYmplY3RSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy'
    '52Mi5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkSRwoN'
    'cHJlY29uZGl0aW9ucxgDIAEoCzIhLmFjeWNsaWMub2JqZWN0cy52Mi5QcmVjb25kaXRpb25zUg'
    '1wcmVjb25kaXRpb25zEkAKCG11dGF0aW9uGAQgASgLMiQuYWN5Y2xpYy5vYmplY3RzLnYyLk11'
    'dGF0aW9uSWRlbnRpdHlSCG11dGF0aW9u');

@$core.Deprecated('Use deleteObjectResponseDescriptor instead')
const DeleteObjectResponse$json = {
  '1': 'DeleteObjectResponse',
  '2': [
    {'1': 'existed', '3': 1, '4': 1, '5': 8, '10': 'existed'},
  ],
};

/// Descriptor for `DeleteObjectResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List deleteObjectResponseDescriptor =
    $convert.base64Decode(
        'ChREZWxldGVPYmplY3RSZXNwb25zZRIYCgdleGlzdGVkGAEgASgIUgdleGlzdGVk');

@$core.Deprecated('Use listObjectsRequestDescriptor instead')
const ListObjectsRequest$json = {
  '1': 'ListObjectsRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'prefix', '3': 2, '4': 1, '5': 9, '10': 'prefix'},
    {'1': 'delimiter', '3': 3, '4': 1, '5': 9, '10': 'delimiter'},
    {'1': 'page_size', '3': 4, '4': 1, '5': 13, '10': 'pageSize'},
    {
      '1': 'continuation_token',
      '3': 5,
      '4': 1,
      '5': 9,
      '10': 'continuationToken'
    },
  ],
};

/// Descriptor for `ListObjectsRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listObjectsRequestDescriptor = $convert.base64Decode(
    'ChJMaXN0T2JqZWN0c1JlcXVlc3QSNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYmplY3RzLn'
    'YyLkJ1Y2tldFJlZlIGYnVja2V0EhYKBnByZWZpeBgCIAEoCVIGcHJlZml4EhwKCWRlbGltaXRl'
    'chgDIAEoCVIJZGVsaW1pdGVyEhsKCXBhZ2Vfc2l6ZRgEIAEoDVIIcGFnZVNpemUSLQoSY29udG'
    'ludWF0aW9uX3Rva2VuGAUgASgJUhFjb250aW51YXRpb25Ub2tlbg==');

@$core.Deprecated('Use listEntryDescriptor instead')
const ListEntry$json = {
  '1': 'ListEntry',
  '2': [
    {'1': 'object_key', '3': 1, '4': 1, '5': 9, '10': 'objectKey'},
    {
      '1': 'object',
      '3': 2,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ObjectInfo',
      '10': 'object'
    },
  ],
};

/// Descriptor for `ListEntry`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listEntryDescriptor = $convert.base64Decode(
    'CglMaXN0RW50cnkSHQoKb2JqZWN0X2tleRgBIAEoCVIJb2JqZWN0S2V5EjYKBm9iamVjdBgCIA'
    'EoCzIeLmFjeWNsaWMub2JqZWN0cy52Mi5PYmplY3RJbmZvUgZvYmplY3Q=');

@$core.Deprecated('Use listObjectsResponseDescriptor instead')
const ListObjectsResponse$json = {
  '1': 'ListObjectsResponse',
  '2': [
    {
      '1': 'entries',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.objects.v2.ListEntry',
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
    {'1': 'is_truncated', '3': 4, '4': 1, '5': 8, '10': 'isTruncated'},
  ],
};

/// Descriptor for `ListObjectsResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listObjectsResponseDescriptor = $convert.base64Decode(
    'ChNMaXN0T2JqZWN0c1Jlc3BvbnNlEjcKB2VudHJpZXMYASADKAsyHS5hY3ljbGljLm9iamVjdH'
    'MudjIuTGlzdEVudHJ5UgdlbnRyaWVzEicKD2NvbW1vbl9wcmVmaXhlcxgCIAMoCVIOY29tbW9u'
    'UHJlZml4ZXMSLQoSY29udGludWF0aW9uX3Rva2VuGAMgASgJUhFjb250aW51YXRpb25Ub2tlbh'
    'IhCgxpc190cnVuY2F0ZWQYBCABKAhSC2lzVHJ1bmNhdGVk');

@$core.Deprecated('Use createMultipartRequestDescriptor instead')
const CreateMultipartRequest$json = {
  '1': 'CreateMultipartRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {
      '1': 'metadata',
      '3': 3,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.ObjectMetadata',
      '10': 'metadata'
    },
    {
      '1': 'mutation',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `CreateMultipartRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List createMultipartRequestDescriptor = $convert.base64Decode(
    'ChZDcmVhdGVNdWx0aXBhcnRSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZW'
    'N0cy52Mi5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkS'
    'PgoIbWV0YWRhdGEYAyABKAsyIi5hY3ljbGljLm9iamVjdHMudjIuT2JqZWN0TWV0YWRhdGFSCG'
    '1ldGFkYXRhEkAKCG11dGF0aW9uGAQgASgLMiQuYWN5Y2xpYy5vYmplY3RzLnYyLk11dGF0aW9u'
    'SWRlbnRpdHlSCG11dGF0aW9u');

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
      '6': '.acyclic.objects.v2.BucketRef',
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
      '6': '.acyclic.objects.v2.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `UploadPartHeader`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List uploadPartHeaderDescriptor = $convert.base64Decode(
    'ChBVcGxvYWRQYXJ0SGVhZGVyEjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy52Mi'
    '5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkSGwoJdXBs'
    'b2FkX2lkGAMgASgJUgh1cGxvYWRJZBIfCgtwYXJ0X251bWJlchgEIAEoDVIKcGFydE51bWJlch'
    'JACghtdXRhdGlvbhgFIAEoCzIkLmFjeWNsaWMub2JqZWN0cy52Mi5NdXRhdGlvbklkZW50aXR5'
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
      '6': '.acyclic.objects.v2.UploadPartHeader',
      '9': 0,
      '10': 'header'
    },
    {'1': 'body', '3': 2, '4': 1, '5': 12, '9': 0, '10': 'body'},
    {'1': 'complete', '3': 3, '4': 1, '5': 8, '9': 0, '10': 'complete'},
  ],
  '8': [
    {'1': 'frame'},
  ],
};

/// Descriptor for `UploadPartRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List uploadPartRequestDescriptor = $convert.base64Decode(
    'ChFVcGxvYWRQYXJ0UmVxdWVzdBI+CgZoZWFkZXIYASABKAsyJC5hY3ljbGljLm9iamVjdHMudj'
    'IuVXBsb2FkUGFydEhlYWRlckgAUgZoZWFkZXISFAoEYm9keRgCIAEoDEgAUgRib2R5EhwKCGNv'
    'bXBsZXRlGAMgASgISABSCGNvbXBsZXRlQgcKBWZyYW1l');

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
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'upload_id', '3': 3, '4': 1, '5': 9, '10': 'uploadId'},
    {
      '1': 'after_part_number',
      '3': 4,
      '4': 1,
      '5': 13,
      '10': 'afterPartNumber'
    },
    {'1': 'page_size', '3': 5, '4': 1, '5': 13, '10': 'pageSize'},
  ],
};

/// Descriptor for `ListPartsRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listPartsRequestDescriptor = $convert.base64Decode(
    'ChBMaXN0UGFydHNSZXF1ZXN0EjUKBmJ1Y2tldBgBIAEoCzIdLmFjeWNsaWMub2JqZWN0cy52Mi'
    '5CdWNrZXRSZWZSBmJ1Y2tldBIdCgpvYmplY3Rfa2V5GAIgASgJUglvYmplY3RLZXkSGwoJdXBs'
    'b2FkX2lkGAMgASgJUgh1cGxvYWRJZBIqChFhZnRlcl9wYXJ0X251bWJlchgEIAEoDVIPYWZ0ZX'
    'JQYXJ0TnVtYmVyEhsKCXBhZ2Vfc2l6ZRgFIAEoDVIIcGFnZVNpemU=');

@$core.Deprecated('Use listPartsResponseDescriptor instead')
const ListPartsResponse$json = {
  '1': 'ListPartsResponse',
  '2': [
    {
      '1': 'parts',
      '3': 1,
      '4': 3,
      '5': 11,
      '6': '.acyclic.objects.v2.UploadedPart',
      '10': 'parts'
    },
    {'1': 'next_part_number', '3': 2, '4': 1, '5': 13, '10': 'nextPartNumber'},
    {'1': 'is_truncated', '3': 3, '4': 1, '5': 8, '10': 'isTruncated'},
  ],
};

/// Descriptor for `ListPartsResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List listPartsResponseDescriptor = $convert.base64Decode(
    'ChFMaXN0UGFydHNSZXNwb25zZRI2CgVwYXJ0cxgBIAMoCzIgLmFjeWNsaWMub2JqZWN0cy52Mi'
    '5VcGxvYWRlZFBhcnRSBXBhcnRzEigKEG5leHRfcGFydF9udW1iZXIYAiABKA1SDm5leHRQYXJ0'
    'TnVtYmVyEiEKDGlzX3RydW5jYXRlZBgDIAEoCFILaXNUcnVuY2F0ZWQ=');

@$core.Deprecated('Use completeMultipartRequestDescriptor instead')
const CompleteMultipartRequest$json = {
  '1': 'CompleteMultipartRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'upload_id', '3': 3, '4': 1, '5': 9, '10': 'uploadId'},
    {
      '1': 'parts',
      '3': 4,
      '4': 3,
      '5': 11,
      '6': '.acyclic.objects.v2.UploadedPart',
      '10': 'parts'
    },
    {
      '1': 'preconditions',
      '3': 5,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.Preconditions',
      '10': 'preconditions'
    },
    {
      '1': 'mutation',
      '3': 6,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `CompleteMultipartRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List completeMultipartRequestDescriptor = $convert.base64Decode(
    'ChhDb21wbGV0ZU11bHRpcGFydFJlcXVlc3QSNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYm'
    'plY3RzLnYyLkJ1Y2tldFJlZlIGYnVja2V0Eh0KCm9iamVjdF9rZXkYAiABKAlSCW9iamVjdEtl'
    'eRIbCgl1cGxvYWRfaWQYAyABKAlSCHVwbG9hZElkEjYKBXBhcnRzGAQgAygLMiAuYWN5Y2xpYy'
    '5vYmplY3RzLnYyLlVwbG9hZGVkUGFydFIFcGFydHMSRwoNcHJlY29uZGl0aW9ucxgFIAEoCzIh'
    'LmFjeWNsaWMub2JqZWN0cy52Mi5QcmVjb25kaXRpb25zUg1wcmVjb25kaXRpb25zEkAKCG11dG'
    'F0aW9uGAYgASgLMiQuYWN5Y2xpYy5vYmplY3RzLnYyLk11dGF0aW9uSWRlbnRpdHlSCG11dGF0'
    'aW9u');

@$core.Deprecated('Use abortMultipartRequestDescriptor instead')
const AbortMultipartRequest$json = {
  '1': 'AbortMultipartRequest',
  '2': [
    {
      '1': 'bucket',
      '3': 1,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.BucketRef',
      '10': 'bucket'
    },
    {'1': 'object_key', '3': 2, '4': 1, '5': 9, '10': 'objectKey'},
    {'1': 'upload_id', '3': 3, '4': 1, '5': 9, '10': 'uploadId'},
    {
      '1': 'mutation',
      '3': 4,
      '4': 1,
      '5': 11,
      '6': '.acyclic.objects.v2.MutationIdentity',
      '10': 'mutation'
    },
  ],
};

/// Descriptor for `AbortMultipartRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List abortMultipartRequestDescriptor = $convert.base64Decode(
    'ChVBYm9ydE11bHRpcGFydFJlcXVlc3QSNQoGYnVja2V0GAEgASgLMh0uYWN5Y2xpYy5vYmplY3'
    'RzLnYyLkJ1Y2tldFJlZlIGYnVja2V0Eh0KCm9iamVjdF9rZXkYAiABKAlSCW9iamVjdEtleRIb'
    'Cgl1cGxvYWRfaWQYAyABKAlSCHVwbG9hZElkEkAKCG11dGF0aW9uGAQgASgLMiQuYWN5Y2xpYy'
    '5vYmplY3RzLnYyLk11dGF0aW9uSWRlbnRpdHlSCG11dGF0aW9u');

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

@$core.Deprecated('Use errorDetailDescriptor instead')
const ErrorDetail$json = {
  '1': 'ErrorDetail',
  '2': [
    {
      '1': 'code',
      '3': 1,
      '4': 1,
      '5': 14,
      '6': '.acyclic.objects.v2.ErrorCode',
      '10': 'code'
    },
    {'1': 'request_id', '3': 2, '4': 1, '5': 9, '10': 'requestId'},
  ],
};

/// Descriptor for `ErrorDetail`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List errorDetailDescriptor = $convert.base64Decode(
    'CgtFcnJvckRldGFpbBIxCgRjb2RlGAEgASgOMh0uYWN5Y2xpYy5vYmplY3RzLnYyLkVycm9yQ2'
    '9kZVIEY29kZRIdCgpyZXF1ZXN0X2lkGAIgASgJUglyZXF1ZXN0SWQ=');
