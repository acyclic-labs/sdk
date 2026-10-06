// This is a generated file - do not edit.
//
// Generated from objects/v2/objects.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class ObjectsLimit extends $pb.ProtobufEnum {
  static const ObjectsLimit OBJECTS_LIMIT_UNSPECIFIED =
      ObjectsLimit._(0, _omitEnumNames ? '' : 'OBJECTS_LIMIT_UNSPECIFIED');
  static const ObjectsLimit OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES =
      ObjectsLimit._(
          256, _omitEnumNames ? '' : 'OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES');
  static const ObjectsLimit OBJECTS_LIMIT_MAX_KEY_BYTES =
      ObjectsLimit._(1024, _omitEnumNames ? '' : 'OBJECTS_LIMIT_MAX_KEY_BYTES');
  static const ObjectsLimit OBJECTS_LIMIT_MAX_USER_METADATA_BYTES =
      ObjectsLimit._(
          2048, _omitEnumNames ? '' : 'OBJECTS_LIMIT_MAX_USER_METADATA_BYTES');
  static const ObjectsLimit OBJECTS_LIMIT_MAX_PAGE_ENTRIES = ObjectsLimit._(
      1000, _omitEnumNames ? '' : 'OBJECTS_LIMIT_MAX_PAGE_ENTRIES');
  static const ObjectsLimit OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES = ObjectsLimit._(
      65536, _omitEnumNames ? '' : 'OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES');
  static const ObjectsLimit OBJECTS_LIMIT_MAX_MULTIPART_PARTS = ObjectsLimit._(
      10000, _omitEnumNames ? '' : 'OBJECTS_LIMIT_MAX_MULTIPART_PARTS');

  static const $core.List<ObjectsLimit> values = <ObjectsLimit>[
    OBJECTS_LIMIT_UNSPECIFIED,
    OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES,
    OBJECTS_LIMIT_MAX_KEY_BYTES,
    OBJECTS_LIMIT_MAX_USER_METADATA_BYTES,
    OBJECTS_LIMIT_MAX_PAGE_ENTRIES,
    OBJECTS_LIMIT_MAX_BODY_FRAME_BYTES,
    OBJECTS_LIMIT_MAX_MULTIPART_PARTS,
  ];

  static final $core.Map<$core.int, ObjectsLimit> _byValue =
      $pb.ProtobufEnum.initByValue(values);
  static ObjectsLimit? valueOf($core.int value) => _byValue[value];

  const ObjectsLimit._(super.value, super.name);
}

class ErrorCode extends $pb.ProtobufEnum {
  static const ErrorCode ERROR_CODE_UNSPECIFIED =
      ErrorCode._(0, _omitEnumNames ? '' : 'ERROR_CODE_UNSPECIFIED');
  static const ErrorCode ERROR_CODE_INVALID_ARGUMENT =
      ErrorCode._(1, _omitEnumNames ? '' : 'ERROR_CODE_INVALID_ARGUMENT');
  static const ErrorCode ERROR_CODE_NOT_FOUND =
      ErrorCode._(2, _omitEnumNames ? '' : 'ERROR_CODE_NOT_FOUND');
  static const ErrorCode ERROR_CODE_ALREADY_EXISTS =
      ErrorCode._(3, _omitEnumNames ? '' : 'ERROR_CODE_ALREADY_EXISTS');
  static const ErrorCode ERROR_CODE_PRECONDITION_FAILED =
      ErrorCode._(4, _omitEnumNames ? '' : 'ERROR_CODE_PRECONDITION_FAILED');
  static const ErrorCode ERROR_CODE_IDEMPOTENCY_MISMATCH =
      ErrorCode._(5, _omitEnumNames ? '' : 'ERROR_CODE_IDEMPOTENCY_MISMATCH');
  static const ErrorCode ERROR_CODE_QUOTA_EXCEEDED =
      ErrorCode._(6, _omitEnumNames ? '' : 'ERROR_CODE_QUOTA_EXCEEDED');
  static const ErrorCode ERROR_CODE_UNSUPPORTED =
      ErrorCode._(7, _omitEnumNames ? '' : 'ERROR_CODE_UNSUPPORTED');
  static const ErrorCode ERROR_CODE_UNAVAILABLE =
      ErrorCode._(8, _omitEnumNames ? '' : 'ERROR_CODE_UNAVAILABLE');
  static const ErrorCode ERROR_CODE_ACCESS_DENIED =
      ErrorCode._(9, _omitEnumNames ? '' : 'ERROR_CODE_ACCESS_DENIED');
  static const ErrorCode ERROR_CODE_RANGE_NOT_SATISFIABLE =
      ErrorCode._(10, _omitEnumNames ? '' : 'ERROR_CODE_RANGE_NOT_SATISFIABLE');
  static const ErrorCode ERROR_CODE_NOT_MODIFIED =
      ErrorCode._(11, _omitEnumNames ? '' : 'ERROR_CODE_NOT_MODIFIED');

  static const $core.List<ErrorCode> values = <ErrorCode>[
    ERROR_CODE_UNSPECIFIED,
    ERROR_CODE_INVALID_ARGUMENT,
    ERROR_CODE_NOT_FOUND,
    ERROR_CODE_ALREADY_EXISTS,
    ERROR_CODE_PRECONDITION_FAILED,
    ERROR_CODE_IDEMPOTENCY_MISMATCH,
    ERROR_CODE_QUOTA_EXCEEDED,
    ERROR_CODE_UNSUPPORTED,
    ERROR_CODE_UNAVAILABLE,
    ERROR_CODE_ACCESS_DENIED,
    ERROR_CODE_RANGE_NOT_SATISFIABLE,
    ERROR_CODE_NOT_MODIFIED,
  ];

  static final $core.List<ErrorCode?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 11);
  static ErrorCode? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ErrorCode._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
