// This is a generated file - do not edit.
//
// Generated from objects/v1/objects.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

/// Fixed limits shared by every Objects transport.
class ObjectsLimit extends $pb.ProtobufEnum {
  static const ObjectsLimit OBJECTS_LIMIT_UNSPECIFIED =
      ObjectsLimit._(0, _omitEnumNames ? '' : 'OBJECTS_LIMIT_UNSPECIFIED');
  static const ObjectsLimit OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES =
      ObjectsLimit._(
          256, _omitEnumNames ? '' : 'OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES');

  static const $core.List<ObjectsLimit> values = <ObjectsLimit>[
    OBJECTS_LIMIT_UNSPECIFIED,
    OBJECTS_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES,
  ];

  static final $core.Map<$core.int, ObjectsLimit> _byValue =
      $pb.ProtobufEnum.initByValue(values);
  static ObjectsLimit? valueOf($core.int value) => _byValue[value];

  const ObjectsLimit._(super.value, super.name);
}

class ListingMode extends $pb.ProtobufEnum {
  static const ListingMode LISTING_MODE_UNSPECIFIED =
      ListingMode._(0, _omitEnumNames ? '' : 'LISTING_MODE_UNSPECIFIED');
  static const ListingMode LISTING_MODE_CURRENT =
      ListingMode._(1, _omitEnumNames ? '' : 'LISTING_MODE_CURRENT');
  static const ListingMode LISTING_MODE_VERSIONS =
      ListingMode._(2, _omitEnumNames ? '' : 'LISTING_MODE_VERSIONS');

  static const $core.List<ListingMode> values = <ListingMode>[
    LISTING_MODE_UNSPECIFIED,
    LISTING_MODE_CURRENT,
    LISTING_MODE_VERSIONS,
  ];

  static final $core.List<ListingMode?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static ListingMode? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ListingMode._(super.value, super.name);
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
  static const ErrorCode ERROR_CODE_TOKEN_EXPIRED =
      ErrorCode._(6, _omitEnumNames ? '' : 'ERROR_CODE_TOKEN_EXPIRED');
  static const ErrorCode ERROR_CODE_QUOTA_EXCEEDED =
      ErrorCode._(7, _omitEnumNames ? '' : 'ERROR_CODE_QUOTA_EXCEEDED');
  static const ErrorCode ERROR_CODE_UNSUPPORTED =
      ErrorCode._(8, _omitEnumNames ? '' : 'ERROR_CODE_UNSUPPORTED');
  static const ErrorCode ERROR_CODE_UNAVAILABLE =
      ErrorCode._(9, _omitEnumNames ? '' : 'ERROR_CODE_UNAVAILABLE');

  static const $core.List<ErrorCode> values = <ErrorCode>[
    ERROR_CODE_UNSPECIFIED,
    ERROR_CODE_INVALID_ARGUMENT,
    ERROR_CODE_NOT_FOUND,
    ERROR_CODE_ALREADY_EXISTS,
    ERROR_CODE_PRECONDITION_FAILED,
    ERROR_CODE_IDEMPOTENCY_MISMATCH,
    ERROR_CODE_TOKEN_EXPIRED,
    ERROR_CODE_QUOTA_EXCEEDED,
    ERROR_CODE_UNSUPPORTED,
    ERROR_CODE_UNAVAILABLE,
  ];

  static final $core.List<ErrorCode?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 9);
  static ErrorCode? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ErrorCode._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
