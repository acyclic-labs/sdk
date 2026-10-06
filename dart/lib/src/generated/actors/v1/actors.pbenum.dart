// This is a generated file - do not edit.
//
// Generated from actors/v1/actors.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class SubscriptionState extends $pb.ProtobufEnum {
  static const SubscriptionState SUBSCRIPTION_STATE_UNSPECIFIED =
      SubscriptionState._(
          0, _omitEnumNames ? '' : 'SUBSCRIPTION_STATE_UNSPECIFIED');
  static const SubscriptionState SUBSCRIPTION_STATE_ACTIVE =
      SubscriptionState._(1, _omitEnumNames ? '' : 'SUBSCRIPTION_STATE_ACTIVE');
  static const SubscriptionState SUBSCRIPTION_STATE_PAUSED =
      SubscriptionState._(2, _omitEnumNames ? '' : 'SUBSCRIPTION_STATE_PAUSED');

  static const $core.List<SubscriptionState> values = <SubscriptionState>[
    SUBSCRIPTION_STATE_UNSPECIFIED,
    SUBSCRIPTION_STATE_ACTIVE,
    SUBSCRIPTION_STATE_PAUSED,
  ];

  static final $core.List<SubscriptionState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static SubscriptionState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const SubscriptionState._(super.value, super.name);
}

class ActorState extends $pb.ProtobufEnum {
  static const ActorState ACTOR_STATE_UNSPECIFIED =
      ActorState._(0, _omitEnumNames ? '' : 'ACTOR_STATE_UNSPECIFIED');
  static const ActorState ACTOR_STATE_ACTIVE =
      ActorState._(1, _omitEnumNames ? '' : 'ACTOR_STATE_ACTIVE');
  static const ActorState ACTOR_STATE_HIBERNATED =
      ActorState._(2, _omitEnumNames ? '' : 'ACTOR_STATE_HIBERNATED');
  static const ActorState ACTOR_STATE_PAUSED =
      ActorState._(3, _omitEnumNames ? '' : 'ACTOR_STATE_PAUSED');

  static const $core.List<ActorState> values = <ActorState>[
    ACTOR_STATE_UNSPECIFIED,
    ACTOR_STATE_ACTIVE,
    ACTOR_STATE_HIBERNATED,
    ACTOR_STATE_PAUSED,
  ];

  static final $core.List<ActorState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static ActorState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ActorState._(super.value, super.name);
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
  static const ErrorCode ERROR_CODE_ACTOR_NOT_FOUND =
      ErrorCode._(4, _omitEnumNames ? '' : 'ERROR_CODE_ACTOR_NOT_FOUND');
  static const ErrorCode ERROR_CODE_SUBSCRIPTION_NOT_FOUND =
      ErrorCode._(5, _omitEnumNames ? '' : 'ERROR_CODE_SUBSCRIPTION_NOT_FOUND');
  static const ErrorCode ERROR_CODE_IDEMPOTENCY_MISMATCH =
      ErrorCode._(6, _omitEnumNames ? '' : 'ERROR_CODE_IDEMPOTENCY_MISMATCH');
  static const ErrorCode ERROR_CODE_CONFLICT =
      ErrorCode._(7, _omitEnumNames ? '' : 'ERROR_CODE_CONFLICT');
  static const ErrorCode ERROR_CODE_ADMISSION_DENIED =
      ErrorCode._(8, _omitEnumNames ? '' : 'ERROR_CODE_ADMISSION_DENIED');
  static const ErrorCode ERROR_CODE_CHECKPOINT_FAILED =
      ErrorCode._(9, _omitEnumNames ? '' : 'ERROR_CODE_CHECKPOINT_FAILED');
  static const ErrorCode ERROR_CODE_DEPENDENCY_UNAVAILABLE = ErrorCode._(
      10, _omitEnumNames ? '' : 'ERROR_CODE_DEPENDENCY_UNAVAILABLE');

  static const $core.List<ErrorCode> values = <ErrorCode>[
    ERROR_CODE_UNSPECIFIED,
    ERROR_CODE_INVALID_ARGUMENT,
    ERROR_CODE_CAPABILITY_DENIED,
    ERROR_CODE_CAPABILITY_EXPIRED,
    ERROR_CODE_ACTOR_NOT_FOUND,
    ERROR_CODE_SUBSCRIPTION_NOT_FOUND,
    ERROR_CODE_IDEMPOTENCY_MISMATCH,
    ERROR_CODE_CONFLICT,
    ERROR_CODE_ADMISSION_DENIED,
    ERROR_CODE_CHECKPOINT_FAILED,
    ERROR_CODE_DEPENDENCY_UNAVAILABLE,
  ];

  static final $core.List<ErrorCode?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 10);
  static ErrorCode? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ErrorCode._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
