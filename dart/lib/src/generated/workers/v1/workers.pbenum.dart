// This is a generated file - do not edit.
//
// Generated from workers/v1/workers.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class JobState extends $pb.ProtobufEnum {
  static const JobState JOB_STATE_UNSPECIFIED =
      JobState._(0, _omitEnumNames ? '' : 'JOB_STATE_UNSPECIFIED');
  static const JobState JOB_STATE_ACCEPTED =
      JobState._(1, _omitEnumNames ? '' : 'JOB_STATE_ACCEPTED');
  static const JobState JOB_STATE_RUNNING =
      JobState._(2, _omitEnumNames ? '' : 'JOB_STATE_RUNNING');
  static const JobState JOB_STATE_SUCCEEDED =
      JobState._(3, _omitEnumNames ? '' : 'JOB_STATE_SUCCEEDED');
  static const JobState JOB_STATE_FAILED =
      JobState._(4, _omitEnumNames ? '' : 'JOB_STATE_FAILED');
  static const JobState JOB_STATE_CANCELLED =
      JobState._(5, _omitEnumNames ? '' : 'JOB_STATE_CANCELLED');

  static const $core.List<JobState> values = <JobState>[
    JOB_STATE_UNSPECIFIED,
    JOB_STATE_ACCEPTED,
    JOB_STATE_RUNNING,
    JOB_STATE_SUCCEEDED,
    JOB_STATE_FAILED,
    JOB_STATE_CANCELLED,
  ];

  static final $core.List<JobState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static JobState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const JobState._(super.value, super.name);
}

class ErrorCode extends $pb.ProtobufEnum {
  static const ErrorCode ERROR_CODE_UNSPECIFIED =
      ErrorCode._(0, _omitEnumNames ? '' : 'ERROR_CODE_UNSPECIFIED');
  static const ErrorCode ERROR_CODE_INVALID_ARGUMENT =
      ErrorCode._(1, _omitEnumNames ? '' : 'ERROR_CODE_INVALID_ARGUMENT');
  static const ErrorCode ERROR_CODE_CAPABILITY_DENIED =
      ErrorCode._(2, _omitEnumNames ? '' : 'ERROR_CODE_CAPABILITY_DENIED');
  static const ErrorCode ERROR_CODE_CAPABILITY_EXPIRED =
      ErrorCode._(3, _omitEnumNames ? '' : 'ERROR_CODE_CAPABILITY_EXPIRED');
  static const ErrorCode ERROR_CODE_VERSION_NOT_FOUND =
      ErrorCode._(4, _omitEnumNames ? '' : 'ERROR_CODE_VERSION_NOT_FOUND');
  static const ErrorCode ERROR_CODE_DEPLOYMENT_NOT_FOUND =
      ErrorCode._(5, _omitEnumNames ? '' : 'ERROR_CODE_DEPLOYMENT_NOT_FOUND');
  static const ErrorCode ERROR_CODE_JOB_NOT_FOUND =
      ErrorCode._(6, _omitEnumNames ? '' : 'ERROR_CODE_JOB_NOT_FOUND');
  static const ErrorCode ERROR_CODE_IDEMPOTENCY_MISMATCH =
      ErrorCode._(7, _omitEnumNames ? '' : 'ERROR_CODE_IDEMPOTENCY_MISMATCH');
  static const ErrorCode ERROR_CODE_REVISION_CONFLICT =
      ErrorCode._(8, _omitEnumNames ? '' : 'ERROR_CODE_REVISION_CONFLICT');
  static const ErrorCode ERROR_CODE_OVERLOADED =
      ErrorCode._(9, _omitEnumNames ? '' : 'ERROR_CODE_OVERLOADED');
  static const ErrorCode ERROR_CODE_TERMINAL_JOB_FAILURE =
      ErrorCode._(10, _omitEnumNames ? '' : 'ERROR_CODE_TERMINAL_JOB_FAILURE');

  static const $core.List<ErrorCode> values = <ErrorCode>[
    ERROR_CODE_UNSPECIFIED,
    ERROR_CODE_INVALID_ARGUMENT,
    ERROR_CODE_CAPABILITY_DENIED,
    ERROR_CODE_CAPABILITY_EXPIRED,
    ERROR_CODE_VERSION_NOT_FOUND,
    ERROR_CODE_DEPLOYMENT_NOT_FOUND,
    ERROR_CODE_JOB_NOT_FOUND,
    ERROR_CODE_IDEMPOTENCY_MISMATCH,
    ERROR_CODE_REVISION_CONFLICT,
    ERROR_CODE_OVERLOADED,
    ERROR_CODE_TERMINAL_JOB_FAILURE,
  ];

  static final $core.List<ErrorCode?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 10);
  static ErrorCode? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ErrorCode._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
