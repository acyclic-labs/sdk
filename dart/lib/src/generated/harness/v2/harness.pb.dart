// This is a generated file - do not edit.
//
// Generated from harness/v2/harness.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import '../../protocol/v1/protocol.pb.dart' as $0;
import 'harness.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'harness.pbenum.dart';

/// Release-candidate v2 contract types. Compatibility is bound to ProtocolIdentity.
class OperationIdentity extends $pb.GeneratedMessage {
  factory OperationIdentity({
    $core.String? operationId,
    $core.String? idempotencyKey,
  }) {
    final result = OperationIdentity._();
    if (operationId != null) result.operationId = operationId;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  OperationIdentity._();

  factory OperationIdentity.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationIdentity()..mergeFromBuffer(data, registry);
  factory OperationIdentity.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationIdentity()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OperationIdentity',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: OperationIdentity.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'operationId')
    ..aOS(2, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationIdentity clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationIdentity copyWith(void Function(OperationIdentity) updates) =>
      super.copyWith((message) => updates(message as OperationIdentity))
          as OperationIdentity;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OperationIdentity() / OperationIdentity.new instead')
  static OperationIdentity create() => OperationIdentity._();
  static $pb.GeneratedMessage $_createMessage() => OperationIdentity._();
  @$core.override
  OperationIdentity createEmptyInstance() => OperationIdentity._();
  @$core.pragma('dart2js:noInline')
  static OperationIdentity getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OperationIdentity>(
          OperationIdentity.$_createMessage);
  static OperationIdentity? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get operationId => $_getSZ(0);
  @$pb.TagNumber(1)
  set operationId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOperationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperationId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get idempotencyKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set idempotencyKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
}

class Error extends $pb.GeneratedMessage {
  factory Error({
    ErrorCode? code,
    $core.String? message,
    $core.String? operationId,
  }) {
    final result = Error._();
    if (code != null) result.code = code;
    if (message != null) result.message = message;
    if (operationId != null) result.operationId = operationId;
    return result;
  }

  Error._();

  factory Error.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Error()..mergeFromBuffer(data, registry);
  factory Error.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Error()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Error',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Error.$_createMessage)
    ..aE<ErrorCode>(1, _omitFieldNames ? '' : 'code',
        enumValues: ErrorCode.values)
    ..aOS(2, _omitFieldNames ? '' : 'message')
    ..aOS(3, _omitFieldNames ? '' : 'operationId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Error clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Error copyWith(void Function(Error) updates) =>
      super.copyWith((message) => updates(message as Error)) as Error;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Error() / Error.new instead')
  static Error create() => Error._();
  static $pb.GeneratedMessage $_createMessage() => Error._();
  @$core.override
  Error createEmptyInstance() => Error._();
  @$core.pragma('dart2js:noInline')
  static Error getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Error>(Error.$_createMessage);
  static Error? _defaultInstance;

  @$pb.TagNumber(1)
  ErrorCode get code => $_getN(0);
  @$pb.TagNumber(1)
  set code(ErrorCode value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCode() => $_has(0);
  @$pb.TagNumber(1)
  void clearCode() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get message => $_getSZ(1);
  @$pb.TagNumber(2)
  set message($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMessage() => $_has(1);
  @$pb.TagNumber(2)
  void clearMessage() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get operationId => $_getSZ(2);
  @$pb.TagNumber(3)
  set operationId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasOperationId() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperationId() => $_clearField(3);
}

class Admission extends $pb.GeneratedMessage {
  factory Admission({
    OperationIdentity? operation,
    AdmissionState? state,
    Error? error,
  }) {
    final result = Admission._();
    if (operation != null) result.operation = operation;
    if (state != null) result.state = state;
    if (error != null) result.error = error;
    return result;
  }

  Admission._();

  factory Admission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Admission()..mergeFromBuffer(data, registry);
  factory Admission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Admission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Admission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Admission.$_createMessage)
    ..aOM<OperationIdentity>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationIdentity.$_createMessage)
    ..aE<AdmissionState>(2, _omitFieldNames ? '' : 'state',
        enumValues: AdmissionState.values)
    ..aOM<Error>(3, _omitFieldNames ? '' : 'error',
        subBuilder: Error.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Admission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Admission copyWith(void Function(Admission) updates) =>
      super.copyWith((message) => updates(message as Admission)) as Admission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Admission() / Admission.new instead')
  static Admission create() => Admission._();
  static $pb.GeneratedMessage $_createMessage() => Admission._();
  @$core.override
  Admission createEmptyInstance() => Admission._();
  @$core.pragma('dart2js:noInline')
  static Admission getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Admission>(Admission.$_createMessage);
  static Admission? _defaultInstance;

  @$pb.TagNumber(1)
  OperationIdentity get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(OperationIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationIdentity ensureOperation() => $_ensure(0);

  @$pb.TagNumber(2)
  AdmissionState get state => $_getN(1);
  @$pb.TagNumber(2)
  set state(AdmissionState value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasState() => $_has(1);
  @$pb.TagNumber(2)
  void clearState() => $_clearField(2);

  @$pb.TagNumber(3)
  Error get error => $_getN(2);
  @$pb.TagNumber(3)
  set error(Error value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasError() => $_has(2);
  @$pb.TagNumber(3)
  void clearError() => $_clearField(3);
  @$pb.TagNumber(3)
  Error ensureError() => $_ensure(2);
}

class OperationStatus extends $pb.GeneratedMessage {
  factory OperationStatus({
    OperationIdentity? operation,
    CompletionState? state,
    Error? error,
    $0.ProtocolIdentity? protocol,
    Authority? owner,
    $core.bool? cancellationRequested,
    $fixnum.Int64? revision,
  }) {
    final result = OperationStatus._();
    if (operation != null) result.operation = operation;
    if (state != null) result.state = state;
    if (error != null) result.error = error;
    if (protocol != null) result.protocol = protocol;
    if (owner != null) result.owner = owner;
    if (cancellationRequested != null)
      result.cancellationRequested = cancellationRequested;
    if (revision != null) result.revision = revision;
    return result;
  }

  OperationStatus._();

  factory OperationStatus.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationStatus()..mergeFromBuffer(data, registry);
  factory OperationStatus.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationStatus()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OperationStatus',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: OperationStatus.$_createMessage)
    ..aOM<OperationIdentity>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationIdentity.$_createMessage)
    ..aE<CompletionState>(2, _omitFieldNames ? '' : 'state',
        enumValues: CompletionState.values)
    ..aOM<Error>(3, _omitFieldNames ? '' : 'error',
        subBuilder: Error.$_createMessage)
    ..aOM<$0.ProtocolIdentity>(4, _omitFieldNames ? '' : 'protocol',
        subBuilder: $0.ProtocolIdentity.$_createMessage)
    ..aOM<Authority>(5, _omitFieldNames ? '' : 'owner',
        subBuilder: Authority.$_createMessage)
    ..aOB(6, _omitFieldNames ? '' : 'cancellationRequested')
    ..a<$fixnum.Int64>(
        7, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationStatus clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationStatus copyWith(void Function(OperationStatus) updates) =>
      super.copyWith((message) => updates(message as OperationStatus))
          as OperationStatus;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OperationStatus() / OperationStatus.new instead')
  static OperationStatus create() => OperationStatus._();
  static $pb.GeneratedMessage $_createMessage() => OperationStatus._();
  @$core.override
  OperationStatus createEmptyInstance() => OperationStatus._();
  @$core.pragma('dart2js:noInline')
  static OperationStatus getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OperationStatus>(
          OperationStatus.$_createMessage);
  static OperationStatus? _defaultInstance;

  @$pb.TagNumber(1)
  OperationIdentity get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(OperationIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationIdentity ensureOperation() => $_ensure(0);

  @$pb.TagNumber(2)
  CompletionState get state => $_getN(1);
  @$pb.TagNumber(2)
  set state(CompletionState value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasState() => $_has(1);
  @$pb.TagNumber(2)
  void clearState() => $_clearField(2);

  @$pb.TagNumber(3)
  Error get error => $_getN(2);
  @$pb.TagNumber(3)
  set error(Error value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasError() => $_has(2);
  @$pb.TagNumber(3)
  void clearError() => $_clearField(3);
  @$pb.TagNumber(3)
  Error ensureError() => $_ensure(2);

  @$pb.TagNumber(4)
  $0.ProtocolIdentity get protocol => $_getN(3);
  @$pb.TagNumber(4)
  set protocol($0.ProtocolIdentity value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasProtocol() => $_has(3);
  @$pb.TagNumber(4)
  void clearProtocol() => $_clearField(4);
  @$pb.TagNumber(4)
  $0.ProtocolIdentity ensureProtocol() => $_ensure(3);

  @$pb.TagNumber(5)
  Authority get owner => $_getN(4);
  @$pb.TagNumber(5)
  set owner(Authority value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasOwner() => $_has(4);
  @$pb.TagNumber(5)
  void clearOwner() => $_clearField(5);
  @$pb.TagNumber(5)
  Authority ensureOwner() => $_ensure(4);

  @$pb.TagNumber(6)
  $core.bool get cancellationRequested => $_getBF(5);
  @$pb.TagNumber(6)
  set cancellationRequested($core.bool value) => $_setBool(5, value);
  @$pb.TagNumber(6)
  $core.bool hasCancellationRequested() => $_has(5);
  @$pb.TagNumber(6)
  void clearCancellationRequested() => $_clearField(6);

  @$pb.TagNumber(7)
  $fixnum.Int64 get revision => $_getI64(6);
  @$pb.TagNumber(7)
  set revision($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasRevision() => $_has(6);
  @$pb.TagNumber(7)
  void clearRevision() => $_clearField(7);
}

class ObserveRequest extends $pb.GeneratedMessage {
  factory ObserveRequest({
    $core.String? operationId,
    $0.ProtocolIdentity? protocol,
    Authority? owner,
    Scope? scope,
  }) {
    final result = ObserveRequest._();
    if (operationId != null) result.operationId = operationId;
    if (protocol != null) result.protocol = protocol;
    if (owner != null) result.owner = owner;
    if (scope != null) result.scope = scope;
    return result;
  }

  ObserveRequest._();

  factory ObserveRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObserveRequest()..mergeFromBuffer(data, registry);
  factory ObserveRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObserveRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ObserveRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ObserveRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'operationId')
    ..aOM<$0.ProtocolIdentity>(2, _omitFieldNames ? '' : 'protocol',
        subBuilder: $0.ProtocolIdentity.$_createMessage)
    ..aOM<Authority>(3, _omitFieldNames ? '' : 'owner',
        subBuilder: Authority.$_createMessage)
    ..aOM<Scope>(4, _omitFieldNames ? '' : 'scope',
        subBuilder: Scope.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObserveRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObserveRequest copyWith(void Function(ObserveRequest) updates) =>
      super.copyWith((message) => updates(message as ObserveRequest))
          as ObserveRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ObserveRequest() / ObserveRequest.new instead')
  static ObserveRequest create() => ObserveRequest._();
  static $pb.GeneratedMessage $_createMessage() => ObserveRequest._();
  @$core.override
  ObserveRequest createEmptyInstance() => ObserveRequest._();
  @$core.pragma('dart2js:noInline')
  static ObserveRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ObserveRequest>(
          ObserveRequest.$_createMessage);
  static ObserveRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get operationId => $_getSZ(0);
  @$pb.TagNumber(1)
  set operationId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOperationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperationId() => $_clearField(1);

  @$pb.TagNumber(2)
  $0.ProtocolIdentity get protocol => $_getN(1);
  @$pb.TagNumber(2)
  set protocol($0.ProtocolIdentity value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasProtocol() => $_has(1);
  @$pb.TagNumber(2)
  void clearProtocol() => $_clearField(2);
  @$pb.TagNumber(2)
  $0.ProtocolIdentity ensureProtocol() => $_ensure(1);

  @$pb.TagNumber(3)
  Authority get owner => $_getN(2);
  @$pb.TagNumber(3)
  set owner(Authority value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOwner() => $_has(2);
  @$pb.TagNumber(3)
  void clearOwner() => $_clearField(3);
  @$pb.TagNumber(3)
  Authority ensureOwner() => $_ensure(2);

  @$pb.TagNumber(4)
  Scope get scope => $_getN(3);
  @$pb.TagNumber(4)
  set scope(Scope value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasScope() => $_has(3);
  @$pb.TagNumber(4)
  void clearScope() => $_clearField(4);
  @$pb.TagNumber(4)
  Scope ensureScope() => $_ensure(3);
}

class CancelRequest extends $pb.GeneratedMessage {
  factory CancelRequest({
    $core.String? operationId,
    $0.ProtocolIdentity? protocol,
    Authority? owner,
    Scope? scope,
    $core.bool? recursive,
    $core.String? idempotencyKey,
  }) {
    final result = CancelRequest._();
    if (operationId != null) result.operationId = operationId;
    if (protocol != null) result.protocol = protocol;
    if (owner != null) result.owner = owner;
    if (scope != null) result.scope = scope;
    if (recursive != null) result.recursive = recursive;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  CancelRequest._();

  factory CancelRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CancelRequest()..mergeFromBuffer(data, registry);
  factory CancelRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CancelRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CancelRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: CancelRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'operationId')
    ..aOM<$0.ProtocolIdentity>(2, _omitFieldNames ? '' : 'protocol',
        subBuilder: $0.ProtocolIdentity.$_createMessage)
    ..aOM<Authority>(3, _omitFieldNames ? '' : 'owner',
        subBuilder: Authority.$_createMessage)
    ..aOM<Scope>(4, _omitFieldNames ? '' : 'scope',
        subBuilder: Scope.$_createMessage)
    ..aOB(5, _omitFieldNames ? '' : 'recursive')
    ..aOS(6, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CancelRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CancelRequest copyWith(void Function(CancelRequest) updates) =>
      super.copyWith((message) => updates(message as CancelRequest))
          as CancelRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CancelRequest() / CancelRequest.new instead')
  static CancelRequest create() => CancelRequest._();
  static $pb.GeneratedMessage $_createMessage() => CancelRequest._();
  @$core.override
  CancelRequest createEmptyInstance() => CancelRequest._();
  @$core.pragma('dart2js:noInline')
  static CancelRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CancelRequest>(
          CancelRequest.$_createMessage);
  static CancelRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get operationId => $_getSZ(0);
  @$pb.TagNumber(1)
  set operationId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOperationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperationId() => $_clearField(1);

  @$pb.TagNumber(2)
  $0.ProtocolIdentity get protocol => $_getN(1);
  @$pb.TagNumber(2)
  set protocol($0.ProtocolIdentity value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasProtocol() => $_has(1);
  @$pb.TagNumber(2)
  void clearProtocol() => $_clearField(2);
  @$pb.TagNumber(2)
  $0.ProtocolIdentity ensureProtocol() => $_ensure(1);

  @$pb.TagNumber(3)
  Authority get owner => $_getN(2);
  @$pb.TagNumber(3)
  set owner(Authority value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOwner() => $_has(2);
  @$pb.TagNumber(3)
  void clearOwner() => $_clearField(3);
  @$pb.TagNumber(3)
  Authority ensureOwner() => $_ensure(2);

  @$pb.TagNumber(4)
  Scope get scope => $_getN(3);
  @$pb.TagNumber(4)
  set scope(Scope value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasScope() => $_has(3);
  @$pb.TagNumber(4)
  void clearScope() => $_clearField(4);
  @$pb.TagNumber(4)
  Scope ensureScope() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.bool get recursive => $_getBF(4);
  @$pb.TagNumber(5)
  set recursive($core.bool value) => $_setBool(4, value);
  @$pb.TagNumber(5)
  $core.bool hasRecursive() => $_has(4);
  @$pb.TagNumber(5)
  void clearRecursive() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get idempotencyKey => $_getSZ(5);
  @$pb.TagNumber(6)
  set idempotencyKey($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasIdempotencyKey() => $_has(5);
  @$pb.TagNumber(6)
  void clearIdempotencyKey() => $_clearField(6);
}

class CancelResponse extends $pb.GeneratedMessage {
  factory CancelResponse({
    OperationStatus? status,
    OperationIdentity? operation,
  }) {
    final result = CancelResponse._();
    if (status != null) result.status = status;
    if (operation != null) result.operation = operation;
    return result;
  }

  CancelResponse._();

  factory CancelResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CancelResponse()..mergeFromBuffer(data, registry);
  factory CancelResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CancelResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CancelResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: CancelResponse.$_createMessage)
    ..aOM<OperationStatus>(1, _omitFieldNames ? '' : 'status',
        subBuilder: OperationStatus.$_createMessage)
    ..aOM<OperationIdentity>(2, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CancelResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CancelResponse copyWith(void Function(CancelResponse) updates) =>
      super.copyWith((message) => updates(message as CancelResponse))
          as CancelResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CancelResponse() / CancelResponse.new instead')
  static CancelResponse create() => CancelResponse._();
  static $pb.GeneratedMessage $_createMessage() => CancelResponse._();
  @$core.override
  CancelResponse createEmptyInstance() => CancelResponse._();
  @$core.pragma('dart2js:noInline')
  static CancelResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CancelResponse>(
          CancelResponse.$_createMessage);
  static CancelResponse? _defaultInstance;

  @$pb.TagNumber(1)
  OperationStatus get status => $_getN(0);
  @$pb.TagNumber(1)
  set status(OperationStatus value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStatus() => $_has(0);
  @$pb.TagNumber(1)
  void clearStatus() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationStatus ensureStatus() => $_ensure(0);

  @$pb.TagNumber(2)
  OperationIdentity get operation => $_getN(1);
  @$pb.TagNumber(2)
  set operation(OperationIdentity value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOperation() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperation() => $_clearField(2);
  @$pb.TagNumber(2)
  OperationIdentity ensureOperation() => $_ensure(1);
}

class Cursor extends $pb.GeneratedMessage {
  factory Cursor({
    $core.List<$core.int>? opaque,
  }) {
    final result = Cursor._();
    if (opaque != null) result.opaque = opaque;
    return result;
  }

  Cursor._();

  factory Cursor.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Cursor()..mergeFromBuffer(data, registry);
  factory Cursor.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Cursor()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Cursor',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Cursor.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'opaque', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Cursor clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Cursor copyWith(void Function(Cursor) updates) =>
      super.copyWith((message) => updates(message as Cursor)) as Cursor;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Cursor() / Cursor.new instead')
  static Cursor create() => Cursor._();
  static $pb.GeneratedMessage $_createMessage() => Cursor._();
  @$core.override
  Cursor createEmptyInstance() => Cursor._();
  @$core.pragma('dart2js:noInline')
  static Cursor getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Cursor>(Cursor.$_createMessage);
  static Cursor? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get opaque => $_getN(0);
  @$pb.TagNumber(1)
  set opaque($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOpaque() => $_has(0);
  @$pb.TagNumber(1)
  void clearOpaque() => $_clearField(1);
}

/// Stable identity of one aggregate history.
class Authority extends $pb.GeneratedMessage {
  factory Authority({
    AggregateKind? kind,
    $core.String? id,
  }) {
    final result = Authority._();
    if (kind != null) result.kind = kind;
    if (id != null) result.id = id;
    return result;
  }

  Authority._();

  factory Authority.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Authority()..mergeFromBuffer(data, registry);
  factory Authority.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Authority()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Authority',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Authority.$_createMessage)
    ..aE<AggregateKind>(1, _omitFieldNames ? '' : 'kind',
        enumValues: AggregateKind.values)
    ..aOS(2, _omitFieldNames ? '' : 'id')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Authority clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Authority copyWith(void Function(Authority) updates) =>
      super.copyWith((message) => updates(message as Authority)) as Authority;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Authority() / Authority.new instead')
  static Authority create() => Authority._();
  static $pb.GeneratedMessage $_createMessage() => Authority._();
  @$core.override
  Authority createEmptyInstance() => Authority._();
  @$core.pragma('dart2js:noInline')
  static Authority getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Authority>(Authority.$_createMessage);
  static Authority? _defaultInstance;

  @$pb.TagNumber(1)
  AggregateKind get kind => $_getN(0);
  @$pb.TagNumber(1)
  set kind(AggregateKind value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasKind() => $_has(0);
  @$pb.TagNumber(1)
  void clearKind() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get id => $_getSZ(1);
  @$pb.TagNumber(2)
  set id($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasId() => $_has(1);
  @$pb.TagNumber(2)
  void clearId() => $_clearField(2);
}

/// Exact causal predecessor in another aggregate.
class EventReference extends $pb.GeneratedMessage {
  factory EventReference({
    Authority? authority,
    $fixnum.Int64? revision,
  }) {
    final result = EventReference._();
    if (authority != null) result.authority = authority;
    if (revision != null) result.revision = revision;
    return result;
  }

  EventReference._();

  factory EventReference.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EventReference()..mergeFromBuffer(data, registry);
  factory EventReference.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EventReference()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EventReference',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: EventReference.$_createMessage)
    ..aOM<Authority>(1, _omitFieldNames ? '' : 'authority',
        subBuilder: Authority.$_createMessage)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EventReference clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EventReference copyWith(void Function(EventReference) updates) =>
      super.copyWith((message) => updates(message as EventReference))
          as EventReference;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EventReference() / EventReference.new instead')
  static EventReference create() => EventReference._();
  static $pb.GeneratedMessage $_createMessage() => EventReference._();
  @$core.override
  EventReference createEmptyInstance() => EventReference._();
  @$core.pragma('dart2js:noInline')
  static EventReference getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EventReference>(
          EventReference.$_createMessage);
  static EventReference? _defaultInstance;

  @$pb.TagNumber(1)
  Authority get authority => $_getN(0);
  @$pb.TagNumber(1)
  set authority(Authority value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasAuthority() => $_has(0);
  @$pb.TagNumber(1)
  void clearAuthority() => $_clearField(1);
  @$pb.TagNumber(1)
  Authority ensureAuthority() => $_ensure(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get revision => $_getI64(1);
  @$pb.TagNumber(2)
  set revision($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRevision() => $_has(1);
  @$pb.TagNumber(2)
  void clearRevision() => $_clearField(2);
}

/// Bearer grant present only in command admission, never in durable events.
class Scope extends $pb.GeneratedMessage {
  factory Scope({
    $core.String? id,
    $core.Iterable<$core.String>? capabilities,
    $core.String? issuer,
    $core.List<$core.int>? parentProof,
    $core.List<$core.int>? proof,
    $core.String? agentId,
  }) {
    final result = Scope._();
    if (id != null) result.id = id;
    if (capabilities != null) result.capabilities.addAll(capabilities);
    if (issuer != null) result.issuer = issuer;
    if (parentProof != null) result.parentProof = parentProof;
    if (proof != null) result.proof = proof;
    if (agentId != null) result.agentId = agentId;
    return result;
  }

  Scope._();

  factory Scope.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Scope()..mergeFromBuffer(data, registry);
  factory Scope.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Scope()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Scope',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Scope.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..pPS(2, _omitFieldNames ? '' : 'capabilities')
    ..aOS(3, _omitFieldNames ? '' : 'issuer')
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'parentProof', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'proof', $pb.PbFieldType.OY)
    ..aOS(6, _omitFieldNames ? '' : 'agentId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Scope clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Scope copyWith(void Function(Scope) updates) =>
      super.copyWith((message) => updates(message as Scope)) as Scope;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Scope() / Scope.new instead')
  static Scope create() => Scope._();
  static $pb.GeneratedMessage $_createMessage() => Scope._();
  @$core.override
  Scope createEmptyInstance() => Scope._();
  @$core.pragma('dart2js:noInline')
  static Scope getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Scope>(Scope.$_createMessage);
  static Scope? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<$core.String> get capabilities => $_getList(1);

  @$pb.TagNumber(3)
  $core.String get issuer => $_getSZ(2);
  @$pb.TagNumber(3)
  set issuer($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIssuer() => $_has(2);
  @$pb.TagNumber(3)
  void clearIssuer() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get parentProof => $_getN(3);
  @$pb.TagNumber(4)
  set parentProof($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasParentProof() => $_has(3);
  @$pb.TagNumber(4)
  void clearParentProof() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get proof => $_getN(4);
  @$pb.TagNumber(5)
  set proof($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasProof() => $_has(4);
  @$pb.TagNumber(5)
  void clearProof() => $_clearField(5);

  /// Host-attested acting agent; absent scopes cannot write agent-private files.
  @$pb.TagNumber(6)
  $core.String get agentId => $_getSZ(5);
  @$pb.TagNumber(6)
  set agentId($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasAgentId() => $_has(5);
  @$pb.TagNumber(6)
  void clearAgentId() => $_clearField(6);
}

/// Non-bearer metadata for a committed event. The event attestation is bound
/// to the complete event and cannot be reused as a command authorization.
class RecordedScope extends $pb.GeneratedMessage {
  factory RecordedScope({
    $core.String? id,
    $core.Iterable<$core.String>? capabilities,
    $core.String? issuer,
    $core.String? agentId,
  }) {
    final result = RecordedScope._();
    if (id != null) result.id = id;
    if (capabilities != null) result.capabilities.addAll(capabilities);
    if (issuer != null) result.issuer = issuer;
    if (agentId != null) result.agentId = agentId;
    return result;
  }

  RecordedScope._();

  factory RecordedScope.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RecordedScope()..mergeFromBuffer(data, registry);
  factory RecordedScope.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RecordedScope()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RecordedScope',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: RecordedScope.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..pPS(2, _omitFieldNames ? '' : 'capabilities')
    ..aOS(3, _omitFieldNames ? '' : 'issuer')
    ..aOS(4, _omitFieldNames ? '' : 'agentId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RecordedScope clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RecordedScope copyWith(void Function(RecordedScope) updates) =>
      super.copyWith((message) => updates(message as RecordedScope))
          as RecordedScope;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RecordedScope() / RecordedScope.new instead')
  static RecordedScope create() => RecordedScope._();
  static $pb.GeneratedMessage $_createMessage() => RecordedScope._();
  @$core.override
  RecordedScope createEmptyInstance() => RecordedScope._();
  @$core.pragma('dart2js:noInline')
  static RecordedScope getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RecordedScope>(
          RecordedScope.$_createMessage);
  static RecordedScope? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<$core.String> get capabilities => $_getList(1);

  @$pb.TagNumber(3)
  $core.String get issuer => $_getSZ(2);
  @$pb.TagNumber(3)
  set issuer($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIssuer() => $_has(2);
  @$pb.TagNumber(3)
  void clearIssuer() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get agentId => $_getSZ(3);
  @$pb.TagNumber(4)
  set agentId($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasAgentId() => $_has(3);
  @$pb.TagNumber(4)
  void clearAgentId() => $_clearField(4);
}

/// Transport-neutral command whose canonical payload is interpreted by Rust.
class CommandEnvelope extends $pb.GeneratedMessage {
  factory CommandEnvelope({
    $0.ProtocolIdentity? protocol,
    Authority? authority,
    OperationIdentity? operation,
    $fixnum.Int64? expectedRevision,
    Scope? scope,
    EventReference? causalParent,
    $core.String? actionType,
    $core.List<$core.int>? canonicalActionJson,
    $core.List<$core.int>? intentDigest,
  }) {
    final result = CommandEnvelope._();
    if (protocol != null) result.protocol = protocol;
    if (authority != null) result.authority = authority;
    if (operation != null) result.operation = operation;
    if (expectedRevision != null) result.expectedRevision = expectedRevision;
    if (scope != null) result.scope = scope;
    if (causalParent != null) result.causalParent = causalParent;
    if (actionType != null) result.actionType = actionType;
    if (canonicalActionJson != null)
      result.canonicalActionJson = canonicalActionJson;
    if (intentDigest != null) result.intentDigest = intentDigest;
    return result;
  }

  CommandEnvelope._();

  factory CommandEnvelope.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommandEnvelope()..mergeFromBuffer(data, registry);
  factory CommandEnvelope.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommandEnvelope()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommandEnvelope',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: CommandEnvelope.$_createMessage)
    ..aOM<$0.ProtocolIdentity>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: $0.ProtocolIdentity.$_createMessage)
    ..aOM<Authority>(2, _omitFieldNames ? '' : 'authority',
        subBuilder: Authority.$_createMessage)
    ..aOM<OperationIdentity>(3, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationIdentity.$_createMessage)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'expectedRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<Scope>(5, _omitFieldNames ? '' : 'scope',
        subBuilder: Scope.$_createMessage)
    ..aOM<EventReference>(6, _omitFieldNames ? '' : 'causalParent',
        subBuilder: EventReference.$_createMessage)
    ..aOS(7, _omitFieldNames ? '' : 'actionType')
    ..a<$core.List<$core.int>>(
        8, _omitFieldNames ? '' : 'canonicalActionJson', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        9, _omitFieldNames ? '' : 'intentDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommandEnvelope clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommandEnvelope copyWith(void Function(CommandEnvelope) updates) =>
      super.copyWith((message) => updates(message as CommandEnvelope))
          as CommandEnvelope;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommandEnvelope() / CommandEnvelope.new instead')
  static CommandEnvelope create() => CommandEnvelope._();
  static $pb.GeneratedMessage $_createMessage() => CommandEnvelope._();
  @$core.override
  CommandEnvelope createEmptyInstance() => CommandEnvelope._();
  @$core.pragma('dart2js:noInline')
  static CommandEnvelope getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommandEnvelope>(
          CommandEnvelope.$_createMessage);
  static CommandEnvelope? _defaultInstance;

  @$pb.TagNumber(1)
  $0.ProtocolIdentity get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol($0.ProtocolIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.ProtocolIdentity ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  Authority get authority => $_getN(1);
  @$pb.TagNumber(2)
  set authority(Authority value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAuthority() => $_has(1);
  @$pb.TagNumber(2)
  void clearAuthority() => $_clearField(2);
  @$pb.TagNumber(2)
  Authority ensureAuthority() => $_ensure(1);

  @$pb.TagNumber(3)
  OperationIdentity get operation => $_getN(2);
  @$pb.TagNumber(3)
  set operation(OperationIdentity value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperation() => $_clearField(3);
  @$pb.TagNumber(3)
  OperationIdentity ensureOperation() => $_ensure(2);

  @$pb.TagNumber(4)
  $fixnum.Int64 get expectedRevision => $_getI64(3);
  @$pb.TagNumber(4)
  set expectedRevision($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasExpectedRevision() => $_has(3);
  @$pb.TagNumber(4)
  void clearExpectedRevision() => $_clearField(4);

  @$pb.TagNumber(5)
  Scope get scope => $_getN(4);
  @$pb.TagNumber(5)
  set scope(Scope value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasScope() => $_has(4);
  @$pb.TagNumber(5)
  void clearScope() => $_clearField(5);
  @$pb.TagNumber(5)
  Scope ensureScope() => $_ensure(4);

  @$pb.TagNumber(6)
  EventReference get causalParent => $_getN(5);
  @$pb.TagNumber(6)
  set causalParent(EventReference value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasCausalParent() => $_has(5);
  @$pb.TagNumber(6)
  void clearCausalParent() => $_clearField(6);
  @$pb.TagNumber(6)
  EventReference ensureCausalParent() => $_ensure(5);

  @$pb.TagNumber(7)
  $core.String get actionType => $_getSZ(6);
  @$pb.TagNumber(7)
  set actionType($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasActionType() => $_has(6);
  @$pb.TagNumber(7)
  void clearActionType() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.List<$core.int> get canonicalActionJson => $_getN(7);
  @$pb.TagNumber(8)
  set canonicalActionJson($core.List<$core.int> value) => $_setBytes(7, value);
  @$pb.TagNumber(8)
  $core.bool hasCanonicalActionJson() => $_has(7);
  @$pb.TagNumber(8)
  void clearCanonicalActionJson() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.List<$core.int> get intentDigest => $_getN(8);
  @$pb.TagNumber(9)
  set intentDigest($core.List<$core.int> value) => $_setBytes(8, value);
  @$pb.TagNumber(9)
  $core.bool hasIntentDigest() => $_has(8);
  @$pb.TagNumber(9)
  void clearIntentDigest() => $_clearField(9);
}

/// Canonical durable event carried identically by every transport.
class EventEnvelope extends $pb.GeneratedMessage {
  factory EventEnvelope({
    $0.ProtocolIdentity? protocol,
    Authority? authority,
    $fixnum.Int64? revision,
    $core.String? operationId,
    $core.List<$core.int>? intentDigest,
    RecordedScope? scope,
    EventReference? causalParent,
    $core.String? eventType,
    $core.List<$core.int>? canonicalPayloadJson,
    $core.List<$core.int>? attestation,
  }) {
    final result = EventEnvelope._();
    if (protocol != null) result.protocol = protocol;
    if (authority != null) result.authority = authority;
    if (revision != null) result.revision = revision;
    if (operationId != null) result.operationId = operationId;
    if (intentDigest != null) result.intentDigest = intentDigest;
    if (scope != null) result.scope = scope;
    if (causalParent != null) result.causalParent = causalParent;
    if (eventType != null) result.eventType = eventType;
    if (canonicalPayloadJson != null)
      result.canonicalPayloadJson = canonicalPayloadJson;
    if (attestation != null) result.attestation = attestation;
    return result;
  }

  EventEnvelope._();

  factory EventEnvelope.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EventEnvelope()..mergeFromBuffer(data, registry);
  factory EventEnvelope.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EventEnvelope()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EventEnvelope',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: EventEnvelope.$_createMessage)
    ..aOM<$0.ProtocolIdentity>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: $0.ProtocolIdentity.$_createMessage)
    ..aOM<Authority>(2, _omitFieldNames ? '' : 'authority',
        subBuilder: Authority.$_createMessage)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(4, _omitFieldNames ? '' : 'operationId')
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'intentDigest', $pb.PbFieldType.OY)
    ..aOM<RecordedScope>(6, _omitFieldNames ? '' : 'scope',
        subBuilder: RecordedScope.$_createMessage)
    ..aOM<EventReference>(7, _omitFieldNames ? '' : 'causalParent',
        subBuilder: EventReference.$_createMessage)
    ..aOS(8, _omitFieldNames ? '' : 'eventType')
    ..a<$core.List<$core.int>>(
        9, _omitFieldNames ? '' : 'canonicalPayloadJson', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        10, _omitFieldNames ? '' : 'attestation', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EventEnvelope clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EventEnvelope copyWith(void Function(EventEnvelope) updates) =>
      super.copyWith((message) => updates(message as EventEnvelope))
          as EventEnvelope;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EventEnvelope() / EventEnvelope.new instead')
  static EventEnvelope create() => EventEnvelope._();
  static $pb.GeneratedMessage $_createMessage() => EventEnvelope._();
  @$core.override
  EventEnvelope createEmptyInstance() => EventEnvelope._();
  @$core.pragma('dart2js:noInline')
  static EventEnvelope getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EventEnvelope>(
          EventEnvelope.$_createMessage);
  static EventEnvelope? _defaultInstance;

  @$pb.TagNumber(1)
  $0.ProtocolIdentity get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol($0.ProtocolIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.ProtocolIdentity ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  Authority get authority => $_getN(1);
  @$pb.TagNumber(2)
  set authority(Authority value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAuthority() => $_has(1);
  @$pb.TagNumber(2)
  void clearAuthority() => $_clearField(2);
  @$pb.TagNumber(2)
  Authority ensureAuthority() => $_ensure(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get revision => $_getI64(2);
  @$pb.TagNumber(3)
  set revision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearRevision() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get operationId => $_getSZ(3);
  @$pb.TagNumber(4)
  set operationId($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasOperationId() => $_has(3);
  @$pb.TagNumber(4)
  void clearOperationId() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get intentDigest => $_getN(4);
  @$pb.TagNumber(5)
  set intentDigest($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasIntentDigest() => $_has(4);
  @$pb.TagNumber(5)
  void clearIntentDigest() => $_clearField(5);

  @$pb.TagNumber(6)
  RecordedScope get scope => $_getN(5);
  @$pb.TagNumber(6)
  set scope(RecordedScope value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasScope() => $_has(5);
  @$pb.TagNumber(6)
  void clearScope() => $_clearField(6);
  @$pb.TagNumber(6)
  RecordedScope ensureScope() => $_ensure(5);

  @$pb.TagNumber(7)
  EventReference get causalParent => $_getN(6);
  @$pb.TagNumber(7)
  set causalParent(EventReference value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasCausalParent() => $_has(6);
  @$pb.TagNumber(7)
  void clearCausalParent() => $_clearField(7);
  @$pb.TagNumber(7)
  EventReference ensureCausalParent() => $_ensure(6);

  @$pb.TagNumber(8)
  $core.String get eventType => $_getSZ(7);
  @$pb.TagNumber(8)
  set eventType($core.String value) => $_setString(7, value);
  @$pb.TagNumber(8)
  $core.bool hasEventType() => $_has(7);
  @$pb.TagNumber(8)
  void clearEventType() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.List<$core.int> get canonicalPayloadJson => $_getN(8);
  @$pb.TagNumber(9)
  set canonicalPayloadJson($core.List<$core.int> value) => $_setBytes(8, value);
  @$pb.TagNumber(9)
  $core.bool hasCanonicalPayloadJson() => $_has(8);
  @$pb.TagNumber(9)
  void clearCanonicalPayloadJson() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.List<$core.int> get attestation => $_getN(9);
  @$pb.TagNumber(10)
  set attestation($core.List<$core.int> value) => $_setBytes(9, value);
  @$pb.TagNumber(10)
  $core.bool hasAttestation() => $_has(9);
  @$pb.TagNumber(10)
  void clearAttestation() => $_clearField(10);
}

class ApplyResponse extends $pb.GeneratedMessage {
  factory ApplyResponse({
    ApplyState? state,
    EventEnvelope? event,
  }) {
    final result = ApplyResponse._();
    if (state != null) result.state = state;
    if (event != null) result.event = event;
    return result;
  }

  ApplyResponse._();

  factory ApplyResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ApplyResponse()..mergeFromBuffer(data, registry);
  factory ApplyResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ApplyResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ApplyResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ApplyResponse.$_createMessage)
    ..aE<ApplyState>(1, _omitFieldNames ? '' : 'state',
        enumValues: ApplyState.values)
    ..aOM<EventEnvelope>(2, _omitFieldNames ? '' : 'event',
        subBuilder: EventEnvelope.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ApplyResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ApplyResponse copyWith(void Function(ApplyResponse) updates) =>
      super.copyWith((message) => updates(message as ApplyResponse))
          as ApplyResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ApplyResponse() / ApplyResponse.new instead')
  static ApplyResponse create() => ApplyResponse._();
  static $pb.GeneratedMessage $_createMessage() => ApplyResponse._();
  @$core.override
  ApplyResponse createEmptyInstance() => ApplyResponse._();
  @$core.pragma('dart2js:noInline')
  static ApplyResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ApplyResponse>(
          ApplyResponse.$_createMessage);
  static ApplyResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ApplyState get state => $_getN(0);
  @$pb.TagNumber(1)
  set state(ApplyState value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasState() => $_has(0);
  @$pb.TagNumber(1)
  void clearState() => $_clearField(1);

  @$pb.TagNumber(2)
  EventEnvelope get event => $_getN(1);
  @$pb.TagNumber(2)
  set event(EventEnvelope value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasEvent() => $_has(1);
  @$pb.TagNumber(2)
  void clearEvent() => $_clearField(2);
  @$pb.TagNumber(2)
  EventEnvelope ensureEvent() => $_ensure(1);
}

/// Versioned restoration accelerator. Events remain authoritative.
class SnapshotEnvelope extends $pb.GeneratedMessage {
  factory SnapshotEnvelope({
    $0.ProtocolIdentity? protocol,
    Authority? authority,
    $fixnum.Int64? revision,
    $core.int? formatVersion,
    $core.List<$core.int>? canonicalStateJson,
    $core.List<$core.int>? stateDigest,
  }) {
    final result = SnapshotEnvelope._();
    if (protocol != null) result.protocol = protocol;
    if (authority != null) result.authority = authority;
    if (revision != null) result.revision = revision;
    if (formatVersion != null) result.formatVersion = formatVersion;
    if (canonicalStateJson != null)
      result.canonicalStateJson = canonicalStateJson;
    if (stateDigest != null) result.stateDigest = stateDigest;
    return result;
  }

  SnapshotEnvelope._();

  factory SnapshotEnvelope.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SnapshotEnvelope()..mergeFromBuffer(data, registry);
  factory SnapshotEnvelope.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SnapshotEnvelope()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotEnvelope',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: SnapshotEnvelope.$_createMessage)
    ..aOM<$0.ProtocolIdentity>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: $0.ProtocolIdentity.$_createMessage)
    ..aOM<Authority>(2, _omitFieldNames ? '' : 'authority',
        subBuilder: Authority.$_createMessage)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aI(4, _omitFieldNames ? '' : 'formatVersion',
        fieldType: $pb.PbFieldType.OU3)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'canonicalStateJson', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        6, _omitFieldNames ? '' : 'stateDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotEnvelope clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotEnvelope copyWith(void Function(SnapshotEnvelope) updates) =>
      super.copyWith((message) => updates(message as SnapshotEnvelope))
          as SnapshotEnvelope;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SnapshotEnvelope() / SnapshotEnvelope.new instead')
  static SnapshotEnvelope create() => SnapshotEnvelope._();
  static $pb.GeneratedMessage $_createMessage() => SnapshotEnvelope._();
  @$core.override
  SnapshotEnvelope createEmptyInstance() => SnapshotEnvelope._();
  @$core.pragma('dart2js:noInline')
  static SnapshotEnvelope getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SnapshotEnvelope>(
          SnapshotEnvelope.$_createMessage);
  static SnapshotEnvelope? _defaultInstance;

  @$pb.TagNumber(1)
  $0.ProtocolIdentity get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol($0.ProtocolIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.ProtocolIdentity ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  Authority get authority => $_getN(1);
  @$pb.TagNumber(2)
  set authority(Authority value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAuthority() => $_has(1);
  @$pb.TagNumber(2)
  void clearAuthority() => $_clearField(2);
  @$pb.TagNumber(2)
  Authority ensureAuthority() => $_ensure(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get revision => $_getI64(2);
  @$pb.TagNumber(3)
  set revision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearRevision() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.int get formatVersion => $_getIZ(3);
  @$pb.TagNumber(4)
  set formatVersion($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasFormatVersion() => $_has(3);
  @$pb.TagNumber(4)
  void clearFormatVersion() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get canonicalStateJson => $_getN(4);
  @$pb.TagNumber(5)
  set canonicalStateJson($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasCanonicalStateJson() => $_has(4);
  @$pb.TagNumber(5)
  void clearCanonicalStateJson() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.List<$core.int> get stateDigest => $_getN(5);
  @$pb.TagNumber(6)
  set stateDigest($core.List<$core.int> value) => $_setBytes(5, value);
  @$pb.TagNumber(6)
  $core.bool hasStateDigest() => $_has(5);
  @$pb.TagNumber(6)
  void clearStateDigest() => $_clearField(6);
}

/// Transport-independent resumable subscription cursor.
class ReplayCursor extends $pb.GeneratedMessage {
  factory ReplayCursor({
    Authority? authority,
    $core.String? generation,
    $fixnum.Int64? revision,
  }) {
    final result = ReplayCursor._();
    if (authority != null) result.authority = authority;
    if (generation != null) result.generation = generation;
    if (revision != null) result.revision = revision;
    return result;
  }

  ReplayCursor._();

  factory ReplayCursor.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReplayCursor()..mergeFromBuffer(data, registry);
  factory ReplayCursor.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReplayCursor()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReplayCursor',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ReplayCursor.$_createMessage)
    ..aOM<Authority>(1, _omitFieldNames ? '' : 'authority',
        subBuilder: Authority.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'generation')
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReplayCursor clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReplayCursor copyWith(void Function(ReplayCursor) updates) =>
      super.copyWith((message) => updates(message as ReplayCursor))
          as ReplayCursor;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ReplayCursor() / ReplayCursor.new instead')
  static ReplayCursor create() => ReplayCursor._();
  static $pb.GeneratedMessage $_createMessage() => ReplayCursor._();
  @$core.override
  ReplayCursor createEmptyInstance() => ReplayCursor._();
  @$core.pragma('dart2js:noInline')
  static ReplayCursor getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ReplayCursor>(
          ReplayCursor.$_createMessage);
  static ReplayCursor? _defaultInstance;

  @$pb.TagNumber(1)
  Authority get authority => $_getN(0);
  @$pb.TagNumber(1)
  set authority(Authority value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasAuthority() => $_has(0);
  @$pb.TagNumber(1)
  void clearAuthority() => $_clearField(1);
  @$pb.TagNumber(1)
  Authority ensureAuthority() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get generation => $_getSZ(1);
  @$pb.TagNumber(2)
  set generation($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasGeneration() => $_has(1);
  @$pb.TagNumber(2)
  void clearGeneration() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get revision => $_getI64(2);
  @$pb.TagNumber(3)
  set revision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearRevision() => $_clearField(3);
}

class ResumeRequest extends $pb.GeneratedMessage {
  factory ResumeRequest({
    $0.ProtocolIdentity? protocol,
    $core.Iterable<ReplayCursor>? cursors,
  }) {
    final result = ResumeRequest._();
    if (protocol != null) result.protocol = protocol;
    if (cursors != null) result.cursors.addAll(cursors);
    return result;
  }

  ResumeRequest._();

  factory ResumeRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResumeRequest()..mergeFromBuffer(data, registry);
  factory ResumeRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResumeRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ResumeRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ResumeRequest.$_createMessage)
    ..aOM<$0.ProtocolIdentity>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: $0.ProtocolIdentity.$_createMessage)
    ..pPM<ReplayCursor>(2, _omitFieldNames ? '' : 'cursors',
        subBuilder: ReplayCursor.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResumeRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResumeRequest copyWith(void Function(ResumeRequest) updates) =>
      super.copyWith((message) => updates(message as ResumeRequest))
          as ResumeRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ResumeRequest() / ResumeRequest.new instead')
  static ResumeRequest create() => ResumeRequest._();
  static $pb.GeneratedMessage $_createMessage() => ResumeRequest._();
  @$core.override
  ResumeRequest createEmptyInstance() => ResumeRequest._();
  @$core.pragma('dart2js:noInline')
  static ResumeRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ResumeRequest>(
          ResumeRequest.$_createMessage);
  static ResumeRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $0.ProtocolIdentity get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol($0.ProtocolIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.ProtocolIdentity ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<ReplayCursor> get cursors => $_getList(1);
}

/// One fully validated contiguous authoritative delivery.
class Delivery extends $pb.GeneratedMessage {
  factory Delivery({
    Authority? authority,
    $core.String? generation,
    $fixnum.Int64? fromRevision,
    $fixnum.Int64? throughRevision,
    $core.Iterable<EventEnvelope>? events,
    $core.bool? live,
  }) {
    final result = Delivery._();
    if (authority != null) result.authority = authority;
    if (generation != null) result.generation = generation;
    if (fromRevision != null) result.fromRevision = fromRevision;
    if (throughRevision != null) result.throughRevision = throughRevision;
    if (events != null) result.events.addAll(events);
    if (live != null) result.live = live;
    return result;
  }

  Delivery._();

  factory Delivery.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Delivery()..mergeFromBuffer(data, registry);
  factory Delivery.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Delivery()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Delivery',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Delivery.$_createMessage)
    ..aOM<Authority>(1, _omitFieldNames ? '' : 'authority',
        subBuilder: Authority.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'generation')
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'fromRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'throughRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..pPM<EventEnvelope>(5, _omitFieldNames ? '' : 'events',
        subBuilder: EventEnvelope.$_createMessage)
    ..aOB(6, _omitFieldNames ? '' : 'live')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Delivery clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Delivery copyWith(void Function(Delivery) updates) =>
      super.copyWith((message) => updates(message as Delivery)) as Delivery;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Delivery() / Delivery.new instead')
  static Delivery create() => Delivery._();
  static $pb.GeneratedMessage $_createMessage() => Delivery._();
  @$core.override
  Delivery createEmptyInstance() => Delivery._();
  @$core.pragma('dart2js:noInline')
  static Delivery getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Delivery>(Delivery.$_createMessage);
  static Delivery? _defaultInstance;

  @$pb.TagNumber(1)
  Authority get authority => $_getN(0);
  @$pb.TagNumber(1)
  set authority(Authority value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasAuthority() => $_has(0);
  @$pb.TagNumber(1)
  void clearAuthority() => $_clearField(1);
  @$pb.TagNumber(1)
  Authority ensureAuthority() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get generation => $_getSZ(1);
  @$pb.TagNumber(2)
  set generation($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasGeneration() => $_has(1);
  @$pb.TagNumber(2)
  void clearGeneration() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get fromRevision => $_getI64(2);
  @$pb.TagNumber(3)
  set fromRevision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasFromRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearFromRevision() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get throughRevision => $_getI64(3);
  @$pb.TagNumber(4)
  set throughRevision($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasThroughRevision() => $_has(3);
  @$pb.TagNumber(4)
  void clearThroughRevision() => $_clearField(4);

  @$pb.TagNumber(5)
  $pb.PbList<EventEnvelope> get events => $_getList(4);

  @$pb.TagNumber(6)
  $core.bool get live => $_getBF(5);
  @$pb.TagNumber(6)
  set live($core.bool value) => $_setBool(5, value);
  @$pb.TagNumber(6)
  $core.bool hasLive() => $_has(5);
  @$pb.TagNumber(6)
  void clearLive() => $_clearField(6);
}

class Acknowledge extends $pb.GeneratedMessage {
  factory Acknowledge({
    Authority? authority,
    $core.String? generation,
    $fixnum.Int64? throughRevision,
  }) {
    final result = Acknowledge._();
    if (authority != null) result.authority = authority;
    if (generation != null) result.generation = generation;
    if (throughRevision != null) result.throughRevision = throughRevision;
    return result;
  }

  Acknowledge._();

  factory Acknowledge.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Acknowledge()..mergeFromBuffer(data, registry);
  factory Acknowledge.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Acknowledge()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Acknowledge',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Acknowledge.$_createMessage)
    ..aOM<Authority>(1, _omitFieldNames ? '' : 'authority',
        subBuilder: Authority.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'generation')
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'throughRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Acknowledge clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Acknowledge copyWith(void Function(Acknowledge) updates) =>
      super.copyWith((message) => updates(message as Acknowledge))
          as Acknowledge;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Acknowledge() / Acknowledge.new instead')
  static Acknowledge create() => Acknowledge._();
  static $pb.GeneratedMessage $_createMessage() => Acknowledge._();
  @$core.override
  Acknowledge createEmptyInstance() => Acknowledge._();
  @$core.pragma('dart2js:noInline')
  static Acknowledge getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Acknowledge>(
          Acknowledge.$_createMessage);
  static Acknowledge? _defaultInstance;

  @$pb.TagNumber(1)
  Authority get authority => $_getN(0);
  @$pb.TagNumber(1)
  set authority(Authority value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasAuthority() => $_has(0);
  @$pb.TagNumber(1)
  void clearAuthority() => $_clearField(1);
  @$pb.TagNumber(1)
  Authority ensureAuthority() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get generation => $_getSZ(1);
  @$pb.TagNumber(2)
  set generation($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasGeneration() => $_has(1);
  @$pb.TagNumber(2)
  void clearGeneration() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get throughRevision => $_getI64(2);
  @$pb.TagNumber(3)
  set throughRevision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasThroughRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearThroughRevision() => $_clearField(3);
}

enum ClientFrame_Frame {
  resume,
  command,
  acknowledge,
  handshake,
  observe,
  cancel,
  notSet
}

/// Framing used unchanged by embedded, HTTP/SSE, WebSocket, JSONL/stdio and gRPC adapters.
class ClientFrame extends $pb.GeneratedMessage {
  factory ClientFrame({
    ResumeRequest? resume,
    CommandEnvelope? command,
    Acknowledge? acknowledge,
    $0.HandshakeRequest? handshake,
    ObserveRequest? observe,
    CancelRequest? cancel,
  }) {
    final result = ClientFrame._();
    if (resume != null) result.resume = resume;
    if (command != null) result.command = command;
    if (acknowledge != null) result.acknowledge = acknowledge;
    if (handshake != null) result.handshake = handshake;
    if (observe != null) result.observe = observe;
    if (cancel != null) result.cancel = cancel;
    return result;
  }

  ClientFrame._();

  factory ClientFrame.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ClientFrame()..mergeFromBuffer(data, registry);
  factory ClientFrame.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ClientFrame()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ClientFrame_Frame> _ClientFrame_FrameByTag =
      {
    1: ClientFrame_Frame.resume,
    2: ClientFrame_Frame.command,
    3: ClientFrame_Frame.acknowledge,
    4: ClientFrame_Frame.handshake,
    5: ClientFrame_Frame.observe,
    6: ClientFrame_Frame.cancel,
    0: ClientFrame_Frame.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ClientFrame',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ClientFrame.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5, 6])
    ..aOM<ResumeRequest>(1, _omitFieldNames ? '' : 'resume',
        subBuilder: ResumeRequest.$_createMessage)
    ..aOM<CommandEnvelope>(2, _omitFieldNames ? '' : 'command',
        subBuilder: CommandEnvelope.$_createMessage)
    ..aOM<Acknowledge>(3, _omitFieldNames ? '' : 'acknowledge',
        subBuilder: Acknowledge.$_createMessage)
    ..aOM<$0.HandshakeRequest>(4, _omitFieldNames ? '' : 'handshake',
        subBuilder: $0.HandshakeRequest.$_createMessage)
    ..aOM<ObserveRequest>(5, _omitFieldNames ? '' : 'observe',
        subBuilder: ObserveRequest.$_createMessage)
    ..aOM<CancelRequest>(6, _omitFieldNames ? '' : 'cancel',
        subBuilder: CancelRequest.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ClientFrame clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ClientFrame copyWith(void Function(ClientFrame) updates) =>
      super.copyWith((message) => updates(message as ClientFrame))
          as ClientFrame;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ClientFrame() / ClientFrame.new instead')
  static ClientFrame create() => ClientFrame._();
  static $pb.GeneratedMessage $_createMessage() => ClientFrame._();
  @$core.override
  ClientFrame createEmptyInstance() => ClientFrame._();
  @$core.pragma('dart2js:noInline')
  static ClientFrame getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ClientFrame>(
          ClientFrame.$_createMessage);
  static ClientFrame? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  ClientFrame_Frame whichFrame() => _ClientFrame_FrameByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  void clearFrame() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  ResumeRequest get resume => $_getN(0);
  @$pb.TagNumber(1)
  set resume(ResumeRequest value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasResume() => $_has(0);
  @$pb.TagNumber(1)
  void clearResume() => $_clearField(1);
  @$pb.TagNumber(1)
  ResumeRequest ensureResume() => $_ensure(0);

  @$pb.TagNumber(2)
  CommandEnvelope get command => $_getN(1);
  @$pb.TagNumber(2)
  set command(CommandEnvelope value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasCommand() => $_has(1);
  @$pb.TagNumber(2)
  void clearCommand() => $_clearField(2);
  @$pb.TagNumber(2)
  CommandEnvelope ensureCommand() => $_ensure(1);

  @$pb.TagNumber(3)
  Acknowledge get acknowledge => $_getN(2);
  @$pb.TagNumber(3)
  set acknowledge(Acknowledge value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasAcknowledge() => $_has(2);
  @$pb.TagNumber(3)
  void clearAcknowledge() => $_clearField(3);
  @$pb.TagNumber(3)
  Acknowledge ensureAcknowledge() => $_ensure(2);

  @$pb.TagNumber(4)
  $0.HandshakeRequest get handshake => $_getN(3);
  @$pb.TagNumber(4)
  set handshake($0.HandshakeRequest value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasHandshake() => $_has(3);
  @$pb.TagNumber(4)
  void clearHandshake() => $_clearField(4);
  @$pb.TagNumber(4)
  $0.HandshakeRequest ensureHandshake() => $_ensure(3);

  @$pb.TagNumber(5)
  ObserveRequest get observe => $_getN(4);
  @$pb.TagNumber(5)
  set observe(ObserveRequest value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasObserve() => $_has(4);
  @$pb.TagNumber(5)
  void clearObserve() => $_clearField(5);
  @$pb.TagNumber(5)
  ObserveRequest ensureObserve() => $_ensure(4);

  @$pb.TagNumber(6)
  CancelRequest get cancel => $_getN(5);
  @$pb.TagNumber(6)
  set cancel(CancelRequest value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasCancel() => $_has(5);
  @$pb.TagNumber(6)
  void clearCancel() => $_clearField(6);
  @$pb.TagNumber(6)
  CancelRequest ensureCancel() => $_ensure(5);
}

enum ServerFrame_Frame {
  delivery,
  admission,
  error,
  handshake,
  status,
  cancellation,
  notSet
}

class ServerFrame extends $pb.GeneratedMessage {
  factory ServerFrame({
    Delivery? delivery,
    Admission? admission,
    Error? error,
    $0.HandshakeResponse? handshake,
    OperationStatus? status,
    CancelResponse? cancellation,
  }) {
    final result = ServerFrame._();
    if (delivery != null) result.delivery = delivery;
    if (admission != null) result.admission = admission;
    if (error != null) result.error = error;
    if (handshake != null) result.handshake = handshake;
    if (status != null) result.status = status;
    if (cancellation != null) result.cancellation = cancellation;
    return result;
  }

  ServerFrame._();

  factory ServerFrame.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ServerFrame()..mergeFromBuffer(data, registry);
  factory ServerFrame.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ServerFrame()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ServerFrame_Frame> _ServerFrame_FrameByTag =
      {
    1: ServerFrame_Frame.delivery,
    2: ServerFrame_Frame.admission,
    3: ServerFrame_Frame.error,
    4: ServerFrame_Frame.handshake,
    5: ServerFrame_Frame.status,
    6: ServerFrame_Frame.cancellation,
    0: ServerFrame_Frame.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ServerFrame',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ServerFrame.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5, 6])
    ..aOM<Delivery>(1, _omitFieldNames ? '' : 'delivery',
        subBuilder: Delivery.$_createMessage)
    ..aOM<Admission>(2, _omitFieldNames ? '' : 'admission',
        subBuilder: Admission.$_createMessage)
    ..aOM<Error>(3, _omitFieldNames ? '' : 'error',
        subBuilder: Error.$_createMessage)
    ..aOM<$0.HandshakeResponse>(4, _omitFieldNames ? '' : 'handshake',
        subBuilder: $0.HandshakeResponse.$_createMessage)
    ..aOM<OperationStatus>(5, _omitFieldNames ? '' : 'status',
        subBuilder: OperationStatus.$_createMessage)
    ..aOM<CancelResponse>(6, _omitFieldNames ? '' : 'cancellation',
        subBuilder: CancelResponse.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ServerFrame clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ServerFrame copyWith(void Function(ServerFrame) updates) =>
      super.copyWith((message) => updates(message as ServerFrame))
          as ServerFrame;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ServerFrame() / ServerFrame.new instead')
  static ServerFrame create() => ServerFrame._();
  static $pb.GeneratedMessage $_createMessage() => ServerFrame._();
  @$core.override
  ServerFrame createEmptyInstance() => ServerFrame._();
  @$core.pragma('dart2js:noInline')
  static ServerFrame getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ServerFrame>(
          ServerFrame.$_createMessage);
  static ServerFrame? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  ServerFrame_Frame whichFrame() => _ServerFrame_FrameByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  void clearFrame() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  Delivery get delivery => $_getN(0);
  @$pb.TagNumber(1)
  set delivery(Delivery value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasDelivery() => $_has(0);
  @$pb.TagNumber(1)
  void clearDelivery() => $_clearField(1);
  @$pb.TagNumber(1)
  Delivery ensureDelivery() => $_ensure(0);

  @$pb.TagNumber(2)
  Admission get admission => $_getN(1);
  @$pb.TagNumber(2)
  set admission(Admission value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAdmission() => $_has(1);
  @$pb.TagNumber(2)
  void clearAdmission() => $_clearField(2);
  @$pb.TagNumber(2)
  Admission ensureAdmission() => $_ensure(1);

  @$pb.TagNumber(3)
  Error get error => $_getN(2);
  @$pb.TagNumber(3)
  set error(Error value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasError() => $_has(2);
  @$pb.TagNumber(3)
  void clearError() => $_clearField(3);
  @$pb.TagNumber(3)
  Error ensureError() => $_ensure(2);

  @$pb.TagNumber(4)
  $0.HandshakeResponse get handshake => $_getN(3);
  @$pb.TagNumber(4)
  set handshake($0.HandshakeResponse value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasHandshake() => $_has(3);
  @$pb.TagNumber(4)
  void clearHandshake() => $_clearField(4);
  @$pb.TagNumber(4)
  $0.HandshakeResponse ensureHandshake() => $_ensure(3);

  @$pb.TagNumber(5)
  OperationStatus get status => $_getN(4);
  @$pb.TagNumber(5)
  set status(OperationStatus value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasStatus() => $_has(4);
  @$pb.TagNumber(5)
  void clearStatus() => $_clearField(5);
  @$pb.TagNumber(5)
  OperationStatus ensureStatus() => $_ensure(4);

  @$pb.TagNumber(6)
  CancelResponse get cancellation => $_getN(5);
  @$pb.TagNumber(6)
  set cancellation(CancelResponse value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasCancellation() => $_has(5);
  @$pb.TagNumber(6)
  void clearCancellation() => $_clearField(6);
  @$pb.TagNumber(6)
  CancelResponse ensureCancellation() => $_ensure(5);
}

/// Canonical record stored in the single logical distributed coordinator Stream.
class SchedulerEventEnvelope extends $pb.GeneratedMessage {
  factory SchedulerEventEnvelope({
    $0.ProtocolIdentity? protocol,
    $fixnum.Int64? revision,
    $core.String? operationId,
    $core.String? idempotencyKey,
    $core.List<$core.int>? canonicalEventJson,
    $core.List<$core.int>? eventDigest,
    $fixnum.Int64? committedAtMs,
  }) {
    final result = SchedulerEventEnvelope._();
    if (protocol != null) result.protocol = protocol;
    if (revision != null) result.revision = revision;
    if (operationId != null) result.operationId = operationId;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (canonicalEventJson != null)
      result.canonicalEventJson = canonicalEventJson;
    if (eventDigest != null) result.eventDigest = eventDigest;
    if (committedAtMs != null) result.committedAtMs = committedAtMs;
    return result;
  }

  SchedulerEventEnvelope._();

  factory SchedulerEventEnvelope.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SchedulerEventEnvelope()..mergeFromBuffer(data, registry);
  factory SchedulerEventEnvelope.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SchedulerEventEnvelope()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SchedulerEventEnvelope',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: SchedulerEventEnvelope.$_createMessage)
    ..aOM<$0.ProtocolIdentity>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: $0.ProtocolIdentity.$_createMessage)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(3, _omitFieldNames ? '' : 'operationId')
    ..aOS(4, _omitFieldNames ? '' : 'idempotencyKey')
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'canonicalEventJson', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        6, _omitFieldNames ? '' : 'eventDigest', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        7, _omitFieldNames ? '' : 'committedAtMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SchedulerEventEnvelope clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SchedulerEventEnvelope copyWith(
          void Function(SchedulerEventEnvelope) updates) =>
      super.copyWith((message) => updates(message as SchedulerEventEnvelope))
          as SchedulerEventEnvelope;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use SchedulerEventEnvelope() / SchedulerEventEnvelope.new instead')
  static SchedulerEventEnvelope create() => SchedulerEventEnvelope._();
  static $pb.GeneratedMessage $_createMessage() => SchedulerEventEnvelope._();
  @$core.override
  SchedulerEventEnvelope createEmptyInstance() => SchedulerEventEnvelope._();
  @$core.pragma('dart2js:noInline')
  static SchedulerEventEnvelope getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SchedulerEventEnvelope>(
          SchedulerEventEnvelope.$_createMessage);
  static SchedulerEventEnvelope? _defaultInstance;

  @$pb.TagNumber(1)
  $0.ProtocolIdentity get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol($0.ProtocolIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  $0.ProtocolIdentity ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get revision => $_getI64(1);
  @$pb.TagNumber(2)
  set revision($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRevision() => $_has(1);
  @$pb.TagNumber(2)
  void clearRevision() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get operationId => $_getSZ(2);
  @$pb.TagNumber(3)
  set operationId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasOperationId() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperationId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get idempotencyKey => $_getSZ(3);
  @$pb.TagNumber(4)
  set idempotencyKey($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIdempotencyKey() => $_has(3);
  @$pb.TagNumber(4)
  void clearIdempotencyKey() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get canonicalEventJson => $_getN(4);
  @$pb.TagNumber(5)
  set canonicalEventJson($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasCanonicalEventJson() => $_has(4);
  @$pb.TagNumber(5)
  void clearCanonicalEventJson() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.List<$core.int> get eventDigest => $_getN(5);
  @$pb.TagNumber(6)
  set eventDigest($core.List<$core.int> value) => $_setBytes(5, value);
  @$pb.TagNumber(6)
  $core.bool hasEventDigest() => $_has(5);
  @$pb.TagNumber(6)
  void clearEventDigest() => $_clearField(6);

  @$pb.TagNumber(7)
  $fixnum.Int64 get committedAtMs => $_getI64(6);
  @$pb.TagNumber(7)
  set committedAtMs($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasCommittedAtMs() => $_has(6);
  @$pb.TagNumber(7)
  void clearCommittedAtMs() => $_clearField(7);
}

/// Canonical ref-only conversation values. File bodies are never protocol events.
class ProviderRef extends $pb.GeneratedMessage {
  factory ProviderRef({
    $core.String? namespace,
    $core.String? family,
    $core.String? version,
  }) {
    final result = ProviderRef._();
    if (namespace != null) result.namespace = namespace;
    if (family != null) result.family = family;
    if (version != null) result.version = version;
    return result;
  }

  ProviderRef._();

  factory ProviderRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProviderRef()..mergeFromBuffer(data, registry);
  factory ProviderRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProviderRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProviderRef',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ProviderRef.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'namespace')
    ..aOS(2, _omitFieldNames ? '' : 'family')
    ..aOS(3, _omitFieldNames ? '' : 'version')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProviderRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProviderRef copyWith(void Function(ProviderRef) updates) =>
      super.copyWith((message) => updates(message as ProviderRef))
          as ProviderRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ProviderRef() / ProviderRef.new instead')
  static ProviderRef create() => ProviderRef._();
  static $pb.GeneratedMessage $_createMessage() => ProviderRef._();
  @$core.override
  ProviderRef createEmptyInstance() => ProviderRef._();
  @$core.pragma('dart2js:noInline')
  static ProviderRef getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ProviderRef>(
          ProviderRef.$_createMessage);
  static ProviderRef? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get namespace => $_getSZ(0);
  @$pb.TagNumber(1)
  set namespace($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasNamespace() => $_has(0);
  @$pb.TagNumber(1)
  void clearNamespace() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get family => $_getSZ(1);
  @$pb.TagNumber(2)
  set family($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasFamily() => $_has(1);
  @$pb.TagNumber(2)
  void clearFamily() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get version => $_getSZ(2);
  @$pb.TagNumber(3)
  set version($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasVersion() => $_has(2);
  @$pb.TagNumber(3)
  void clearVersion() => $_clearField(3);
}

enum VolumeOwner_Owner { project, agentId, session, notSet }

class VolumeOwner extends $pb.GeneratedMessage {
  factory VolumeOwner({
    $core.String? project,
    $core.String? agentId,
    $core.String? session,
  }) {
    final result = VolumeOwner._();
    if (project != null) result.project = project;
    if (agentId != null) result.agentId = agentId;
    if (session != null) result.session = session;
    return result;
  }

  VolumeOwner._();

  factory VolumeOwner.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      VolumeOwner()..mergeFromBuffer(data, registry);
  factory VolumeOwner.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      VolumeOwner()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, VolumeOwner_Owner> _VolumeOwner_OwnerByTag =
      {
    1: VolumeOwner_Owner.project,
    2: VolumeOwner_Owner.agentId,
    3: VolumeOwner_Owner.session,
    0: VolumeOwner_Owner.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'VolumeOwner',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: VolumeOwner.$_createMessage)
    ..oo(0, [1, 2, 3])
    ..aOS(1, _omitFieldNames ? '' : 'project')
    ..aOS(2, _omitFieldNames ? '' : 'agentId')
    ..aOS(3, _omitFieldNames ? '' : 'session')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  VolumeOwner clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  VolumeOwner copyWith(void Function(VolumeOwner) updates) =>
      super.copyWith((message) => updates(message as VolumeOwner))
          as VolumeOwner;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use VolumeOwner() / VolumeOwner.new instead')
  static VolumeOwner create() => VolumeOwner._();
  static $pb.GeneratedMessage $_createMessage() => VolumeOwner._();
  @$core.override
  VolumeOwner createEmptyInstance() => VolumeOwner._();
  @$core.pragma('dart2js:noInline')
  static VolumeOwner getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<VolumeOwner>(
          VolumeOwner.$_createMessage);
  static VolumeOwner? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  VolumeOwner_Owner whichOwner() => _VolumeOwner_OwnerByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  void clearOwner() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.String get project => $_getSZ(0);
  @$pb.TagNumber(1)
  set project($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasProject() => $_has(0);
  @$pb.TagNumber(1)
  void clearProject() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get agentId => $_getSZ(1);
  @$pb.TagNumber(2)
  set agentId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAgentId() => $_has(1);
  @$pb.TagNumber(2)
  void clearAgentId() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get session => $_getSZ(2);
  @$pb.TagNumber(3)
  set session($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasSession() => $_has(2);
  @$pb.TagNumber(3)
  void clearSession() => $_clearField(3);
}

class VolumeRef extends $pb.GeneratedMessage {
  factory VolumeRef({
    ProviderRef? provider,
    $core.String? id,
    VolumeClass? volumeClass,
    VolumeOwner? owner,
  }) {
    final result = VolumeRef._();
    if (provider != null) result.provider = provider;
    if (id != null) result.id = id;
    if (volumeClass != null) result.volumeClass = volumeClass;
    if (owner != null) result.owner = owner;
    return result;
  }

  VolumeRef._();

  factory VolumeRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      VolumeRef()..mergeFromBuffer(data, registry);
  factory VolumeRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      VolumeRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'VolumeRef',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: VolumeRef.$_createMessage)
    ..aOM<ProviderRef>(1, _omitFieldNames ? '' : 'provider',
        subBuilder: ProviderRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'id')
    ..aE<VolumeClass>(3, _omitFieldNames ? '' : 'volumeClass',
        enumValues: VolumeClass.values)
    ..aOM<VolumeOwner>(4, _omitFieldNames ? '' : 'owner',
        subBuilder: VolumeOwner.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  VolumeRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  VolumeRef copyWith(void Function(VolumeRef) updates) =>
      super.copyWith((message) => updates(message as VolumeRef)) as VolumeRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use VolumeRef() / VolumeRef.new instead')
  static VolumeRef create() => VolumeRef._();
  static $pb.GeneratedMessage $_createMessage() => VolumeRef._();
  @$core.override
  VolumeRef createEmptyInstance() => VolumeRef._();
  @$core.pragma('dart2js:noInline')
  static VolumeRef getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<VolumeRef>(VolumeRef.$_createMessage);
  static VolumeRef? _defaultInstance;

  @$pb.TagNumber(1)
  ProviderRef get provider => $_getN(0);
  @$pb.TagNumber(1)
  set provider(ProviderRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProvider() => $_has(0);
  @$pb.TagNumber(1)
  void clearProvider() => $_clearField(1);
  @$pb.TagNumber(1)
  ProviderRef ensureProvider() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get id => $_getSZ(1);
  @$pb.TagNumber(2)
  set id($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasId() => $_has(1);
  @$pb.TagNumber(2)
  void clearId() => $_clearField(2);

  @$pb.TagNumber(3)
  VolumeClass get volumeClass => $_getN(2);
  @$pb.TagNumber(3)
  set volumeClass(VolumeClass value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasVolumeClass() => $_has(2);
  @$pb.TagNumber(3)
  void clearVolumeClass() => $_clearField(3);

  @$pb.TagNumber(4)
  VolumeOwner get owner => $_getN(3);
  @$pb.TagNumber(4)
  set owner(VolumeOwner value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasOwner() => $_has(3);
  @$pb.TagNumber(4)
  void clearOwner() => $_clearField(4);
  @$pb.TagNumber(4)
  VolumeOwner ensureOwner() => $_ensure(3);
}

class FileDescriptor extends $pb.GeneratedMessage {
  factory FileDescriptor({
    $core.List<$core.int>? sha256,
    $fixnum.Int64? byteLength,
    $core.String? mediaType,
  }) {
    final result = FileDescriptor._();
    if (sha256 != null) result.sha256 = sha256;
    if (byteLength != null) result.byteLength = byteLength;
    if (mediaType != null) result.mediaType = mediaType;
    return result;
  }

  FileDescriptor._();

  factory FileDescriptor.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileDescriptor()..mergeFromBuffer(data, registry);
  factory FileDescriptor.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileDescriptor()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'FileDescriptor',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: FileDescriptor.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'sha256', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'byteLength', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(3, _omitFieldNames ? '' : 'mediaType')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileDescriptor clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileDescriptor copyWith(void Function(FileDescriptor) updates) =>
      super.copyWith((message) => updates(message as FileDescriptor))
          as FileDescriptor;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use FileDescriptor() / FileDescriptor.new instead')
  static FileDescriptor create() => FileDescriptor._();
  static $pb.GeneratedMessage $_createMessage() => FileDescriptor._();
  @$core.override
  FileDescriptor createEmptyInstance() => FileDescriptor._();
  @$core.pragma('dart2js:noInline')
  static FileDescriptor getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<FileDescriptor>(
          FileDescriptor.$_createMessage);
  static FileDescriptor? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get sha256 => $_getN(0);
  @$pb.TagNumber(1)
  set sha256($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSha256() => $_has(0);
  @$pb.TagNumber(1)
  void clearSha256() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get byteLength => $_getI64(1);
  @$pb.TagNumber(2)
  set byteLength($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasByteLength() => $_has(1);
  @$pb.TagNumber(2)
  void clearByteLength() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get mediaType => $_getSZ(2);
  @$pb.TagNumber(3)
  set mediaType($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMediaType() => $_has(2);
  @$pb.TagNumber(3)
  void clearMediaType() => $_clearField(3);
}

class FileRef extends $pb.GeneratedMessage {
  factory FileRef({
    VolumeRef? volume,
    $core.String? normalizedPath,
    $core.String? immutableVersion,
    FileDescriptor? descriptor,
    $core.String? displayName,
  }) {
    final result = FileRef._();
    if (volume != null) result.volume = volume;
    if (normalizedPath != null) result.normalizedPath = normalizedPath;
    if (immutableVersion != null) result.immutableVersion = immutableVersion;
    if (descriptor != null) result.descriptor = descriptor;
    if (displayName != null) result.displayName = displayName;
    return result;
  }

  FileRef._();

  factory FileRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileRef()..mergeFromBuffer(data, registry);
  factory FileRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'FileRef',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: FileRef.$_createMessage)
    ..aOM<VolumeRef>(1, _omitFieldNames ? '' : 'volume',
        subBuilder: VolumeRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'normalizedPath')
    ..aOS(3, _omitFieldNames ? '' : 'immutableVersion')
    ..aOM<FileDescriptor>(4, _omitFieldNames ? '' : 'descriptor',
        subBuilder: FileDescriptor.$_createMessage)
    ..aOS(5, _omitFieldNames ? '' : 'displayName')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileRef copyWith(void Function(FileRef) updates) =>
      super.copyWith((message) => updates(message as FileRef)) as FileRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use FileRef() / FileRef.new instead')
  static FileRef create() => FileRef._();
  static $pb.GeneratedMessage $_createMessage() => FileRef._();
  @$core.override
  FileRef createEmptyInstance() => FileRef._();
  @$core.pragma('dart2js:noInline')
  static FileRef getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<FileRef>(FileRef.$_createMessage);
  static FileRef? _defaultInstance;

  @$pb.TagNumber(1)
  VolumeRef get volume => $_getN(0);
  @$pb.TagNumber(1)
  set volume(VolumeRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasVolume() => $_has(0);
  @$pb.TagNumber(1)
  void clearVolume() => $_clearField(1);
  @$pb.TagNumber(1)
  VolumeRef ensureVolume() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get normalizedPath => $_getSZ(1);
  @$pb.TagNumber(2)
  set normalizedPath($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasNormalizedPath() => $_has(1);
  @$pb.TagNumber(2)
  void clearNormalizedPath() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get immutableVersion => $_getSZ(2);
  @$pb.TagNumber(3)
  set immutableVersion($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasImmutableVersion() => $_has(2);
  @$pb.TagNumber(3)
  void clearImmutableVersion() => $_clearField(3);

  @$pb.TagNumber(4)
  FileDescriptor get descriptor => $_getN(3);
  @$pb.TagNumber(4)
  set descriptor(FileDescriptor value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasDescriptor() => $_has(3);
  @$pb.TagNumber(4)
  void clearDescriptor() => $_clearField(4);
  @$pb.TagNumber(4)
  FileDescriptor ensureDescriptor() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.String get displayName => $_getSZ(4);
  @$pb.TagNumber(5)
  set displayName($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasDisplayName() => $_has(4);
  @$pb.TagNumber(5)
  void clearDisplayName() => $_clearField(5);
}

enum TaskOutcome_Result {
  succeeded,
  failedMessage,
  indeterminateOperationId,
  cancelled,
  notSet
}

class TaskOutcome extends $pb.GeneratedMessage {
  factory TaskOutcome({
    FileRef? succeeded,
    $core.String? failedMessage,
    $core.String? indeterminateOperationId,
    $core.bool? cancelled,
  }) {
    final result = TaskOutcome._();
    if (succeeded != null) result.succeeded = succeeded;
    if (failedMessage != null) result.failedMessage = failedMessage;
    if (indeterminateOperationId != null)
      result.indeterminateOperationId = indeterminateOperationId;
    if (cancelled != null) result.cancelled = cancelled;
    return result;
  }

  TaskOutcome._();

  factory TaskOutcome.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TaskOutcome()..mergeFromBuffer(data, registry);
  factory TaskOutcome.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TaskOutcome()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, TaskOutcome_Result>
      _TaskOutcome_ResultByTag = {
    1: TaskOutcome_Result.succeeded,
    2: TaskOutcome_Result.failedMessage,
    3: TaskOutcome_Result.indeterminateOperationId,
    4: TaskOutcome_Result.cancelled,
    0: TaskOutcome_Result.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TaskOutcome',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: TaskOutcome.$_createMessage)
    ..oo(0, [1, 2, 3, 4])
    ..aOM<FileRef>(1, _omitFieldNames ? '' : 'succeeded',
        subBuilder: FileRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'failedMessage')
    ..aOS(3, _omitFieldNames ? '' : 'indeterminateOperationId')
    ..aOB(4, _omitFieldNames ? '' : 'cancelled')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TaskOutcome clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TaskOutcome copyWith(void Function(TaskOutcome) updates) =>
      super.copyWith((message) => updates(message as TaskOutcome))
          as TaskOutcome;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TaskOutcome() / TaskOutcome.new instead')
  static TaskOutcome create() => TaskOutcome._();
  static $pb.GeneratedMessage $_createMessage() => TaskOutcome._();
  @$core.override
  TaskOutcome createEmptyInstance() => TaskOutcome._();
  @$core.pragma('dart2js:noInline')
  static TaskOutcome getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TaskOutcome>(
          TaskOutcome.$_createMessage);
  static TaskOutcome? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  TaskOutcome_Result whichResult() =>
      _TaskOutcome_ResultByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  void clearResult() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  FileRef get succeeded => $_getN(0);
  @$pb.TagNumber(1)
  set succeeded(FileRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSucceeded() => $_has(0);
  @$pb.TagNumber(1)
  void clearSucceeded() => $_clearField(1);
  @$pb.TagNumber(1)
  FileRef ensureSucceeded() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get failedMessage => $_getSZ(1);
  @$pb.TagNumber(2)
  set failedMessage($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasFailedMessage() => $_has(1);
  @$pb.TagNumber(2)
  void clearFailedMessage() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get indeterminateOperationId => $_getSZ(2);
  @$pb.TagNumber(3)
  set indeterminateOperationId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIndeterminateOperationId() => $_has(2);
  @$pb.TagNumber(3)
  void clearIndeterminateOperationId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.bool get cancelled => $_getBF(3);
  @$pb.TagNumber(4)
  set cancelled($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCancelled() => $_has(3);
  @$pb.TagNumber(4)
  void clearCancelled() => $_clearField(4);
}

class ExtensionRecord extends $pb.GeneratedMessage {
  factory ExtensionRecord({
    $core.String? name,
    $core.int? version,
    $core.List<$core.int>? schemaDigest,
    $core.List<$core.int>? implementationDigest,
    ExtensionForkPolicy? forkPolicy,
    FileRef? content,
  }) {
    final result = ExtensionRecord._();
    if (name != null) result.name = name;
    if (version != null) result.version = version;
    if (schemaDigest != null) result.schemaDigest = schemaDigest;
    if (implementationDigest != null)
      result.implementationDigest = implementationDigest;
    if (forkPolicy != null) result.forkPolicy = forkPolicy;
    if (content != null) result.content = content;
    return result;
  }

  ExtensionRecord._();

  factory ExtensionRecord.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionRecord()..mergeFromBuffer(data, registry);
  factory ExtensionRecord.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionRecord()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExtensionRecord',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExtensionRecord.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aI(2, _omitFieldNames ? '' : 'version', fieldType: $pb.PbFieldType.OU3)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'schemaDigest', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'implementationDigest', $pb.PbFieldType.OY)
    ..aE<ExtensionForkPolicy>(5, _omitFieldNames ? '' : 'forkPolicy',
        enumValues: ExtensionForkPolicy.values)
    ..aOM<FileRef>(6, _omitFieldNames ? '' : 'content',
        subBuilder: FileRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionRecord clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionRecord copyWith(void Function(ExtensionRecord) updates) =>
      super.copyWith((message) => updates(message as ExtensionRecord))
          as ExtensionRecord;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExtensionRecord() / ExtensionRecord.new instead')
  static ExtensionRecord create() => ExtensionRecord._();
  static $pb.GeneratedMessage $_createMessage() => ExtensionRecord._();
  @$core.override
  ExtensionRecord createEmptyInstance() => ExtensionRecord._();
  @$core.pragma('dart2js:noInline')
  static ExtensionRecord getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ExtensionRecord>(
          ExtensionRecord.$_createMessage);
  static ExtensionRecord? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.int get version => $_getIZ(1);
  @$pb.TagNumber(2)
  set version($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get schemaDigest => $_getN(2);
  @$pb.TagNumber(3)
  set schemaDigest($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasSchemaDigest() => $_has(2);
  @$pb.TagNumber(3)
  void clearSchemaDigest() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get implementationDigest => $_getN(3);
  @$pb.TagNumber(4)
  set implementationDigest($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasImplementationDigest() => $_has(3);
  @$pb.TagNumber(4)
  void clearImplementationDigest() => $_clearField(4);

  @$pb.TagNumber(5)
  ExtensionForkPolicy get forkPolicy => $_getN(4);
  @$pb.TagNumber(5)
  set forkPolicy(ExtensionForkPolicy value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasForkPolicy() => $_has(4);
  @$pb.TagNumber(5)
  void clearForkPolicy() => $_clearField(5);

  @$pb.TagNumber(6)
  FileRef get content => $_getN(5);
  @$pb.TagNumber(6)
  set content(FileRef value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasContent() => $_has(5);
  @$pb.TagNumber(6)
  void clearContent() => $_clearField(6);
  @$pb.TagNumber(6)
  FileRef ensureContent() => $_ensure(5);
}

class ExtensionStateMigration extends $pb.GeneratedMessage {
  factory ExtensionStateMigration({
    EventReference? previous,
    ExtensionRecord? record,
  }) {
    final result = ExtensionStateMigration._();
    if (previous != null) result.previous = previous;
    if (record != null) result.record = record;
    return result;
  }

  ExtensionStateMigration._();

  factory ExtensionStateMigration.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionStateMigration()..mergeFromBuffer(data, registry);
  factory ExtensionStateMigration.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionStateMigration()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExtensionStateMigration',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExtensionStateMigration.$_createMessage)
    ..aOM<EventReference>(1, _omitFieldNames ? '' : 'previous',
        subBuilder: EventReference.$_createMessage)
    ..aOM<ExtensionRecord>(2, _omitFieldNames ? '' : 'record',
        subBuilder: ExtensionRecord.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionStateMigration clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionStateMigration copyWith(
          void Function(ExtensionStateMigration) updates) =>
      super.copyWith((message) => updates(message as ExtensionStateMigration))
          as ExtensionStateMigration;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ExtensionStateMigration() / ExtensionStateMigration.new instead')
  static ExtensionStateMigration create() => ExtensionStateMigration._();
  static $pb.GeneratedMessage $_createMessage() => ExtensionStateMigration._();
  @$core.override
  ExtensionStateMigration createEmptyInstance() => ExtensionStateMigration._();
  @$core.pragma('dart2js:noInline')
  static ExtensionStateMigration getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ExtensionStateMigration>(
          ExtensionStateMigration.$_createMessage);
  static ExtensionStateMigration? _defaultInstance;

  @$pb.TagNumber(1)
  EventReference get previous => $_getN(0);
  @$pb.TagNumber(1)
  set previous(EventReference value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasPrevious() => $_has(0);
  @$pb.TagNumber(1)
  void clearPrevious() => $_clearField(1);
  @$pb.TagNumber(1)
  EventReference ensurePrevious() => $_ensure(0);

  @$pb.TagNumber(2)
  ExtensionRecord get record => $_getN(1);
  @$pb.TagNumber(2)
  set record(ExtensionRecord value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasRecord() => $_has(1);
  @$pb.TagNumber(2)
  void clearRecord() => $_clearField(2);
  @$pb.TagNumber(2)
  ExtensionRecord ensureRecord() => $_ensure(1);
}

class ExtensionDependency extends $pb.GeneratedMessage {
  factory ExtensionDependency({
    $core.String? name,
    $core.int? version,
  }) {
    final result = ExtensionDependency._();
    if (name != null) result.name = name;
    if (version != null) result.version = version;
    return result;
  }

  ExtensionDependency._();

  factory ExtensionDependency.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionDependency()..mergeFromBuffer(data, registry);
  factory ExtensionDependency.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionDependency()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExtensionDependency',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExtensionDependency.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aI(2, _omitFieldNames ? '' : 'version', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionDependency clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionDependency copyWith(void Function(ExtensionDependency) updates) =>
      super.copyWith((message) => updates(message as ExtensionDependency))
          as ExtensionDependency;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use ExtensionDependency() / ExtensionDependency.new instead')
  static ExtensionDependency create() => ExtensionDependency._();
  static $pb.GeneratedMessage $_createMessage() => ExtensionDependency._();
  @$core.override
  ExtensionDependency createEmptyInstance() => ExtensionDependency._();
  @$core.pragma('dart2js:noInline')
  static ExtensionDependency getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ExtensionDependency>(
          ExtensionDependency.$_createMessage);
  static ExtensionDependency? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.int get version => $_getIZ(1);
  @$pb.TagNumber(2)
  set version($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);
}

class ExtensionConfiguration extends $pb.GeneratedMessage {
  factory ExtensionConfiguration({
    ExtensionDependency? extension_1,
    $core.List<$core.int>? schemaDigest,
    FileRef? content,
  }) {
    final result = ExtensionConfiguration._();
    if (extension_1 != null) result.extension_1 = extension_1;
    if (schemaDigest != null) result.schemaDigest = schemaDigest;
    if (content != null) result.content = content;
    return result;
  }

  ExtensionConfiguration._();

  factory ExtensionConfiguration.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionConfiguration()..mergeFromBuffer(data, registry);
  factory ExtensionConfiguration.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionConfiguration()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExtensionConfiguration',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExtensionConfiguration.$_createMessage)
    ..aOM<ExtensionDependency>(1, _omitFieldNames ? '' : 'extension',
        subBuilder: ExtensionDependency.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'schemaDigest', $pb.PbFieldType.OY)
    ..aOM<FileRef>(3, _omitFieldNames ? '' : 'content',
        subBuilder: FileRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionConfiguration clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionConfiguration copyWith(
          void Function(ExtensionConfiguration) updates) =>
      super.copyWith((message) => updates(message as ExtensionConfiguration))
          as ExtensionConfiguration;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ExtensionConfiguration() / ExtensionConfiguration.new instead')
  static ExtensionConfiguration create() => ExtensionConfiguration._();
  static $pb.GeneratedMessage $_createMessage() => ExtensionConfiguration._();
  @$core.override
  ExtensionConfiguration createEmptyInstance() => ExtensionConfiguration._();
  @$core.pragma('dart2js:noInline')
  static ExtensionConfiguration getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ExtensionConfiguration>(
          ExtensionConfiguration.$_createMessage);
  static ExtensionConfiguration? _defaultInstance;

  @$pb.TagNumber(1)
  ExtensionDependency get extension_1 => $_getN(0);
  @$pb.TagNumber(1)
  set extension_1(ExtensionDependency value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasExtension_1() => $_has(0);
  @$pb.TagNumber(1)
  void clearExtension_1() => $_clearField(1);
  @$pb.TagNumber(1)
  ExtensionDependency ensureExtension_1() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get schemaDigest => $_getN(1);
  @$pb.TagNumber(2)
  set schemaDigest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSchemaDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearSchemaDigest() => $_clearField(2);

  @$pb.TagNumber(3)
  FileRef get content => $_getN(2);
  @$pb.TagNumber(3)
  set content(FileRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasContent() => $_has(2);
  @$pb.TagNumber(3)
  void clearContent() => $_clearField(3);
  @$pb.TagNumber(3)
  FileRef ensureContent() => $_ensure(2);
}

class ExtensionSelection extends $pb.GeneratedMessage {
  factory ExtensionSelection({
    $core.Iterable<ExtensionDependency>? previous,
    $core.Iterable<ExtensionDependency>? selected,
    $core.Iterable<ExtensionConfiguration>? configurations,
  }) {
    final result = ExtensionSelection._();
    if (previous != null) result.previous.addAll(previous);
    if (selected != null) result.selected.addAll(selected);
    if (configurations != null) result.configurations.addAll(configurations);
    return result;
  }

  ExtensionSelection._();

  factory ExtensionSelection.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionSelection()..mergeFromBuffer(data, registry);
  factory ExtensionSelection.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionSelection()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExtensionSelection',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExtensionSelection.$_createMessage)
    ..pPM<ExtensionDependency>(1, _omitFieldNames ? '' : 'previous',
        subBuilder: ExtensionDependency.$_createMessage)
    ..pPM<ExtensionDependency>(2, _omitFieldNames ? '' : 'selected',
        subBuilder: ExtensionDependency.$_createMessage)
    ..pPM<ExtensionConfiguration>(3, _omitFieldNames ? '' : 'configurations',
        subBuilder: ExtensionConfiguration.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionSelection clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionSelection copyWith(void Function(ExtensionSelection) updates) =>
      super.copyWith((message) => updates(message as ExtensionSelection))
          as ExtensionSelection;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExtensionSelection() / ExtensionSelection.new instead')
  static ExtensionSelection create() => ExtensionSelection._();
  static $pb.GeneratedMessage $_createMessage() => ExtensionSelection._();
  @$core.override
  ExtensionSelection createEmptyInstance() => ExtensionSelection._();
  @$core.pragma('dart2js:noInline')
  static ExtensionSelection getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ExtensionSelection>(
          ExtensionSelection.$_createMessage);
  static ExtensionSelection? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<ExtensionDependency> get previous => $_getList(0);

  @$pb.TagNumber(2)
  $pb.PbList<ExtensionDependency> get selected => $_getList(1);

  @$pb.TagNumber(3)
  $pb.PbList<ExtensionConfiguration> get configurations => $_getList(2);
}

class ExtensionConfigured extends $pb.GeneratedMessage {
  factory ExtensionConfigured({
    ExtensionConfiguration? previous,
    ExtensionConfiguration? record,
  }) {
    final result = ExtensionConfigured._();
    if (previous != null) result.previous = previous;
    if (record != null) result.record = record;
    return result;
  }

  ExtensionConfigured._();

  factory ExtensionConfigured.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionConfigured()..mergeFromBuffer(data, registry);
  factory ExtensionConfigured.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionConfigured()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExtensionConfigured',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExtensionConfigured.$_createMessage)
    ..aOM<ExtensionConfiguration>(1, _omitFieldNames ? '' : 'previous',
        subBuilder: ExtensionConfiguration.$_createMessage)
    ..aOM<ExtensionConfiguration>(2, _omitFieldNames ? '' : 'record',
        subBuilder: ExtensionConfiguration.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionConfigured clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionConfigured copyWith(void Function(ExtensionConfigured) updates) =>
      super.copyWith((message) => updates(message as ExtensionConfigured))
          as ExtensionConfigured;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use ExtensionConfigured() / ExtensionConfigured.new instead')
  static ExtensionConfigured create() => ExtensionConfigured._();
  static $pb.GeneratedMessage $_createMessage() => ExtensionConfigured._();
  @$core.override
  ExtensionConfigured createEmptyInstance() => ExtensionConfigured._();
  @$core.pragma('dart2js:noInline')
  static ExtensionConfigured getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ExtensionConfigured>(
          ExtensionConfigured.$_createMessage);
  static ExtensionConfigured? _defaultInstance;

  @$pb.TagNumber(1)
  ExtensionConfiguration get previous => $_getN(0);
  @$pb.TagNumber(1)
  set previous(ExtensionConfiguration value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasPrevious() => $_has(0);
  @$pb.TagNumber(1)
  void clearPrevious() => $_clearField(1);
  @$pb.TagNumber(1)
  ExtensionConfiguration ensurePrevious() => $_ensure(0);

  @$pb.TagNumber(2)
  ExtensionConfiguration get record => $_getN(1);
  @$pb.TagNumber(2)
  set record(ExtensionConfiguration value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasRecord() => $_has(1);
  @$pb.TagNumber(2)
  void clearRecord() => $_clearField(2);
  @$pb.TagNumber(2)
  ExtensionConfiguration ensureRecord() => $_ensure(1);
}

class ExtensionAdmission extends $pb.GeneratedMessage {
  factory ExtensionAdmission({
    EventReference? source,
    $core.Iterable<ExtensionDependency>? selected,
    $core.Iterable<ExtensionConfiguration>? configurations,
  }) {
    final result = ExtensionAdmission._();
    if (source != null) result.source = source;
    if (selected != null) result.selected.addAll(selected);
    if (configurations != null) result.configurations.addAll(configurations);
    return result;
  }

  ExtensionAdmission._();

  factory ExtensionAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionAdmission()..mergeFromBuffer(data, registry);
  factory ExtensionAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionAdmission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExtensionAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExtensionAdmission.$_createMessage)
    ..aOM<EventReference>(1, _omitFieldNames ? '' : 'source',
        subBuilder: EventReference.$_createMessage)
    ..pPM<ExtensionDependency>(2, _omitFieldNames ? '' : 'selected',
        subBuilder: ExtensionDependency.$_createMessage)
    ..pPM<ExtensionConfiguration>(3, _omitFieldNames ? '' : 'configurations',
        subBuilder: ExtensionConfiguration.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionAdmission copyWith(void Function(ExtensionAdmission) updates) =>
      super.copyWith((message) => updates(message as ExtensionAdmission))
          as ExtensionAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExtensionAdmission() / ExtensionAdmission.new instead')
  static ExtensionAdmission create() => ExtensionAdmission._();
  static $pb.GeneratedMessage $_createMessage() => ExtensionAdmission._();
  @$core.override
  ExtensionAdmission createEmptyInstance() => ExtensionAdmission._();
  @$core.pragma('dart2js:noInline')
  static ExtensionAdmission getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ExtensionAdmission>(
          ExtensionAdmission.$_createMessage);
  static ExtensionAdmission? _defaultInstance;

  @$pb.TagNumber(1)
  EventReference get source => $_getN(0);
  @$pb.TagNumber(1)
  set source(EventReference value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);
  @$pb.TagNumber(1)
  EventReference ensureSource() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<ExtensionDependency> get selected => $_getList(1);

  @$pb.TagNumber(3)
  $pb.PbList<ExtensionConfiguration> get configurations => $_getList(2);
}

class ModelContextSelection extends $pb.GeneratedMessage {
  factory ModelContextSelection({
    $fixnum.Int64? conversationRevision,
    $core.Iterable<$core.String>? messageIds,
  }) {
    final result = ModelContextSelection._();
    if (conversationRevision != null)
      result.conversationRevision = conversationRevision;
    if (messageIds != null) result.messageIds.addAll(messageIds);
    return result;
  }

  ModelContextSelection._();

  factory ModelContextSelection.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ModelContextSelection()..mergeFromBuffer(data, registry);
  factory ModelContextSelection.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ModelContextSelection()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ModelContextSelection',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ModelContextSelection.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'conversationRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..pPS(2, _omitFieldNames ? '' : 'messageIds')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ModelContextSelection clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ModelContextSelection copyWith(
          void Function(ModelContextSelection) updates) =>
      super.copyWith((message) => updates(message as ModelContextSelection))
          as ModelContextSelection;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ModelContextSelection() / ModelContextSelection.new instead')
  static ModelContextSelection create() => ModelContextSelection._();
  static $pb.GeneratedMessage $_createMessage() => ModelContextSelection._();
  @$core.override
  ModelContextSelection createEmptyInstance() => ModelContextSelection._();
  @$core.pragma('dart2js:noInline')
  static ModelContextSelection getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ModelContextSelection>(
          ModelContextSelection.$_createMessage);
  static ModelContextSelection? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get conversationRevision => $_getI64(0);
  @$pb.TagNumber(1)
  set conversationRevision($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasConversationRevision() => $_has(0);
  @$pb.TagNumber(1)
  void clearConversationRevision() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<$core.String> get messageIds => $_getList(1);
}

class ApprovalBinding extends $pb.GeneratedMessage {
  factory ApprovalBinding({
    $core.String? operationId,
    $core.List<$core.int>? actionDigest,
  }) {
    final result = ApprovalBinding._();
    if (operationId != null) result.operationId = operationId;
    if (actionDigest != null) result.actionDigest = actionDigest;
    return result;
  }

  ApprovalBinding._();

  factory ApprovalBinding.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ApprovalBinding()..mergeFromBuffer(data, registry);
  factory ApprovalBinding.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ApprovalBinding()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ApprovalBinding',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ApprovalBinding.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'operationId')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'actionDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ApprovalBinding clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ApprovalBinding copyWith(void Function(ApprovalBinding) updates) =>
      super.copyWith((message) => updates(message as ApprovalBinding))
          as ApprovalBinding;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ApprovalBinding() / ApprovalBinding.new instead')
  static ApprovalBinding create() => ApprovalBinding._();
  static $pb.GeneratedMessage $_createMessage() => ApprovalBinding._();
  @$core.override
  ApprovalBinding createEmptyInstance() => ApprovalBinding._();
  @$core.pragma('dart2js:noInline')
  static ApprovalBinding getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ApprovalBinding>(
          ApprovalBinding.$_createMessage);
  static ApprovalBinding? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get operationId => $_getSZ(0);
  @$pb.TagNumber(1)
  set operationId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOperationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperationId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get actionDigest => $_getN(1);
  @$pb.TagNumber(2)
  set actionDigest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasActionDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearActionDigest() => $_clearField(2);
}

class InteractionTicket extends $pb.GeneratedMessage {
  factory InteractionTicket({
    $core.String? id,
    InteractionKind? kind,
    FileRef? request,
    $fixnum.Int64? deadlineUnixMs,
    ApprovalBinding? approval,
  }) {
    final result = InteractionTicket._();
    if (id != null) result.id = id;
    if (kind != null) result.kind = kind;
    if (request != null) result.request = request;
    if (deadlineUnixMs != null) result.deadlineUnixMs = deadlineUnixMs;
    if (approval != null) result.approval = approval;
    return result;
  }

  InteractionTicket._();

  factory InteractionTicket.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InteractionTicket()..mergeFromBuffer(data, registry);
  factory InteractionTicket.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InteractionTicket()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InteractionTicket',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: InteractionTicket.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..aE<InteractionKind>(2, _omitFieldNames ? '' : 'kind',
        enumValues: InteractionKind.values)
    ..aOM<FileRef>(3, _omitFieldNames ? '' : 'request',
        subBuilder: FileRef.$_createMessage)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'deadlineUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<ApprovalBinding>(5, _omitFieldNames ? '' : 'approval',
        subBuilder: ApprovalBinding.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InteractionTicket clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InteractionTicket copyWith(void Function(InteractionTicket) updates) =>
      super.copyWith((message) => updates(message as InteractionTicket))
          as InteractionTicket;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InteractionTicket() / InteractionTicket.new instead')
  static InteractionTicket create() => InteractionTicket._();
  static $pb.GeneratedMessage $_createMessage() => InteractionTicket._();
  @$core.override
  InteractionTicket createEmptyInstance() => InteractionTicket._();
  @$core.pragma('dart2js:noInline')
  static InteractionTicket getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<InteractionTicket>(
          InteractionTicket.$_createMessage);
  static InteractionTicket? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  InteractionKind get kind => $_getN(1);
  @$pb.TagNumber(2)
  set kind(InteractionKind value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasKind() => $_has(1);
  @$pb.TagNumber(2)
  void clearKind() => $_clearField(2);

  @$pb.TagNumber(3)
  FileRef get request => $_getN(2);
  @$pb.TagNumber(3)
  set request(FileRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasRequest() => $_has(2);
  @$pb.TagNumber(3)
  void clearRequest() => $_clearField(3);
  @$pb.TagNumber(3)
  FileRef ensureRequest() => $_ensure(2);

  @$pb.TagNumber(4)
  $fixnum.Int64 get deadlineUnixMs => $_getI64(3);
  @$pb.TagNumber(4)
  set deadlineUnixMs($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasDeadlineUnixMs() => $_has(3);
  @$pb.TagNumber(4)
  void clearDeadlineUnixMs() => $_clearField(4);

  @$pb.TagNumber(5)
  ApprovalBinding get approval => $_getN(4);
  @$pb.TagNumber(5)
  set approval(ApprovalBinding value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasApproval() => $_has(4);
  @$pb.TagNumber(5)
  void clearApproval() => $_clearField(5);
  @$pb.TagNumber(5)
  ApprovalBinding ensureApproval() => $_ensure(4);
}

class InteractionOutcomeMarker extends $pb.GeneratedMessage {
  factory InteractionOutcomeMarker() => InteractionOutcomeMarker._();

  InteractionOutcomeMarker._();

  factory InteractionOutcomeMarker.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InteractionOutcomeMarker()..mergeFromBuffer(data, registry);
  factory InteractionOutcomeMarker.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InteractionOutcomeMarker()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InteractionOutcomeMarker',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: InteractionOutcomeMarker.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InteractionOutcomeMarker clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InteractionOutcomeMarker copyWith(
          void Function(InteractionOutcomeMarker) updates) =>
      super.copyWith((message) => updates(message as InteractionOutcomeMarker))
          as InteractionOutcomeMarker;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InteractionOutcomeMarker() / InteractionOutcomeMarker.new instead')
  static InteractionOutcomeMarker create() => InteractionOutcomeMarker._();
  static $pb.GeneratedMessage $_createMessage() => InteractionOutcomeMarker._();
  @$core.override
  InteractionOutcomeMarker createEmptyInstance() =>
      InteractionOutcomeMarker._();
  @$core.pragma('dart2js:noInline')
  static InteractionOutcomeMarker getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InteractionOutcomeMarker>(
          InteractionOutcomeMarker.$_createMessage);
  static InteractionOutcomeMarker? _defaultInstance;
}

enum InteractionOutcome_Kind {
  answered,
  approved,
  declined,
  cancelled,
  expired,
  denied,
  indeterminateOperationId,
  notSet
}

class InteractionOutcome extends $pb.GeneratedMessage {
  factory InteractionOutcome({
    FileRef? answered,
    InteractionOutcomeMarker? approved,
    InteractionOutcomeMarker? declined,
    InteractionOutcomeMarker? cancelled,
    InteractionOutcomeMarker? expired,
    InteractionOutcomeMarker? denied,
    $core.String? indeterminateOperationId,
  }) {
    final result = InteractionOutcome._();
    if (answered != null) result.answered = answered;
    if (approved != null) result.approved = approved;
    if (declined != null) result.declined = declined;
    if (cancelled != null) result.cancelled = cancelled;
    if (expired != null) result.expired = expired;
    if (denied != null) result.denied = denied;
    if (indeterminateOperationId != null)
      result.indeterminateOperationId = indeterminateOperationId;
    return result;
  }

  InteractionOutcome._();

  factory InteractionOutcome.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InteractionOutcome()..mergeFromBuffer(data, registry);
  factory InteractionOutcome.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InteractionOutcome()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, InteractionOutcome_Kind>
      _InteractionOutcome_KindByTag = {
    1: InteractionOutcome_Kind.answered,
    2: InteractionOutcome_Kind.approved,
    3: InteractionOutcome_Kind.declined,
    4: InteractionOutcome_Kind.cancelled,
    5: InteractionOutcome_Kind.expired,
    6: InteractionOutcome_Kind.denied,
    7: InteractionOutcome_Kind.indeterminateOperationId,
    0: InteractionOutcome_Kind.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InteractionOutcome',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: InteractionOutcome.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5, 6, 7])
    ..aOM<FileRef>(1, _omitFieldNames ? '' : 'answered',
        subBuilder: FileRef.$_createMessage)
    ..aOM<InteractionOutcomeMarker>(2, _omitFieldNames ? '' : 'approved',
        subBuilder: InteractionOutcomeMarker.$_createMessage)
    ..aOM<InteractionOutcomeMarker>(3, _omitFieldNames ? '' : 'declined',
        subBuilder: InteractionOutcomeMarker.$_createMessage)
    ..aOM<InteractionOutcomeMarker>(4, _omitFieldNames ? '' : 'cancelled',
        subBuilder: InteractionOutcomeMarker.$_createMessage)
    ..aOM<InteractionOutcomeMarker>(5, _omitFieldNames ? '' : 'expired',
        subBuilder: InteractionOutcomeMarker.$_createMessage)
    ..aOM<InteractionOutcomeMarker>(6, _omitFieldNames ? '' : 'denied',
        subBuilder: InteractionOutcomeMarker.$_createMessage)
    ..aOS(7, _omitFieldNames ? '' : 'indeterminateOperationId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InteractionOutcome clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InteractionOutcome copyWith(void Function(InteractionOutcome) updates) =>
      super.copyWith((message) => updates(message as InteractionOutcome))
          as InteractionOutcome;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InteractionOutcome() / InteractionOutcome.new instead')
  static InteractionOutcome create() => InteractionOutcome._();
  static $pb.GeneratedMessage $_createMessage() => InteractionOutcome._();
  @$core.override
  InteractionOutcome createEmptyInstance() => InteractionOutcome._();
  @$core.pragma('dart2js:noInline')
  static InteractionOutcome getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InteractionOutcome>(
          InteractionOutcome.$_createMessage);
  static InteractionOutcome? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  InteractionOutcome_Kind whichKind() =>
      _InteractionOutcome_KindByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  void clearKind() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  FileRef get answered => $_getN(0);
  @$pb.TagNumber(1)
  set answered(FileRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasAnswered() => $_has(0);
  @$pb.TagNumber(1)
  void clearAnswered() => $_clearField(1);
  @$pb.TagNumber(1)
  FileRef ensureAnswered() => $_ensure(0);

  @$pb.TagNumber(2)
  InteractionOutcomeMarker get approved => $_getN(1);
  @$pb.TagNumber(2)
  set approved(InteractionOutcomeMarker value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasApproved() => $_has(1);
  @$pb.TagNumber(2)
  void clearApproved() => $_clearField(2);
  @$pb.TagNumber(2)
  InteractionOutcomeMarker ensureApproved() => $_ensure(1);

  @$pb.TagNumber(3)
  InteractionOutcomeMarker get declined => $_getN(2);
  @$pb.TagNumber(3)
  set declined(InteractionOutcomeMarker value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasDeclined() => $_has(2);
  @$pb.TagNumber(3)
  void clearDeclined() => $_clearField(3);
  @$pb.TagNumber(3)
  InteractionOutcomeMarker ensureDeclined() => $_ensure(2);

  @$pb.TagNumber(4)
  InteractionOutcomeMarker get cancelled => $_getN(3);
  @$pb.TagNumber(4)
  set cancelled(InteractionOutcomeMarker value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasCancelled() => $_has(3);
  @$pb.TagNumber(4)
  void clearCancelled() => $_clearField(4);
  @$pb.TagNumber(4)
  InteractionOutcomeMarker ensureCancelled() => $_ensure(3);

  @$pb.TagNumber(5)
  InteractionOutcomeMarker get expired => $_getN(4);
  @$pb.TagNumber(5)
  set expired(InteractionOutcomeMarker value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasExpired() => $_has(4);
  @$pb.TagNumber(5)
  void clearExpired() => $_clearField(5);
  @$pb.TagNumber(5)
  InteractionOutcomeMarker ensureExpired() => $_ensure(4);

  @$pb.TagNumber(6)
  InteractionOutcomeMarker get denied => $_getN(5);
  @$pb.TagNumber(6)
  set denied(InteractionOutcomeMarker value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasDenied() => $_has(5);
  @$pb.TagNumber(6)
  void clearDenied() => $_clearField(6);
  @$pb.TagNumber(6)
  InteractionOutcomeMarker ensureDenied() => $_ensure(5);

  @$pb.TagNumber(7)
  $core.String get indeterminateOperationId => $_getSZ(6);
  @$pb.TagNumber(7)
  set indeterminateOperationId($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasIndeterminateOperationId() => $_has(6);
  @$pb.TagNumber(7)
  void clearIndeterminateOperationId() => $_clearField(7);
}

class InteractionResolution extends $pb.GeneratedMessage {
  factory InteractionResolution({
    $core.String? id,
    $fixnum.Int64? expectedVersion,
    InteractionOutcome? outcome,
    FileRef? detail,
  }) {
    final result = InteractionResolution._();
    if (id != null) result.id = id;
    if (expectedVersion != null) result.expectedVersion = expectedVersion;
    if (outcome != null) result.outcome = outcome;
    if (detail != null) result.detail = detail;
    return result;
  }

  InteractionResolution._();

  factory InteractionResolution.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InteractionResolution()..mergeFromBuffer(data, registry);
  factory InteractionResolution.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InteractionResolution()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InteractionResolution',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: InteractionResolution.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'expectedVersion', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<InteractionOutcome>(3, _omitFieldNames ? '' : 'outcome',
        subBuilder: InteractionOutcome.$_createMessage)
    ..aOM<FileRef>(4, _omitFieldNames ? '' : 'detail',
        subBuilder: FileRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InteractionResolution clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InteractionResolution copyWith(
          void Function(InteractionResolution) updates) =>
      super.copyWith((message) => updates(message as InteractionResolution))
          as InteractionResolution;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InteractionResolution() / InteractionResolution.new instead')
  static InteractionResolution create() => InteractionResolution._();
  static $pb.GeneratedMessage $_createMessage() => InteractionResolution._();
  @$core.override
  InteractionResolution createEmptyInstance() => InteractionResolution._();
  @$core.pragma('dart2js:noInline')
  static InteractionResolution getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InteractionResolution>(
          InteractionResolution.$_createMessage);
  static InteractionResolution? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get expectedVersion => $_getI64(1);
  @$pb.TagNumber(2)
  set expectedVersion($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasExpectedVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearExpectedVersion() => $_clearField(2);

  @$pb.TagNumber(3)
  InteractionOutcome get outcome => $_getN(2);
  @$pb.TagNumber(3)
  set outcome(InteractionOutcome value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOutcome() => $_has(2);
  @$pb.TagNumber(3)
  void clearOutcome() => $_clearField(3);
  @$pb.TagNumber(3)
  InteractionOutcome ensureOutcome() => $_ensure(2);

  @$pb.TagNumber(4)
  FileRef get detail => $_getN(3);
  @$pb.TagNumber(4)
  set detail(FileRef value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasDetail() => $_has(3);
  @$pb.TagNumber(4)
  void clearDetail() => $_clearField(4);
  @$pb.TagNumber(4)
  FileRef ensureDetail() => $_ensure(3);
}

class ResolutionReceipt extends $pb.GeneratedMessage {
  factory ResolutionReceipt({
    $core.String? id,
    $fixnum.Int64? version,
    $core.String? operationId,
    $fixnum.Int64? conversationRevision,
    $core.bool? replayed,
    InteractionOutcome? outcome,
  }) {
    final result = ResolutionReceipt._();
    if (id != null) result.id = id;
    if (version != null) result.version = version;
    if (operationId != null) result.operationId = operationId;
    if (conversationRevision != null)
      result.conversationRevision = conversationRevision;
    if (replayed != null) result.replayed = replayed;
    if (outcome != null) result.outcome = outcome;
    return result;
  }

  ResolutionReceipt._();

  factory ResolutionReceipt.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResolutionReceipt()..mergeFromBuffer(data, registry);
  factory ResolutionReceipt.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResolutionReceipt()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ResolutionReceipt',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ResolutionReceipt.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'version', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(3, _omitFieldNames ? '' : 'operationId')
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'conversationRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOB(5, _omitFieldNames ? '' : 'replayed')
    ..aOM<InteractionOutcome>(6, _omitFieldNames ? '' : 'outcome',
        subBuilder: InteractionOutcome.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResolutionReceipt clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResolutionReceipt copyWith(void Function(ResolutionReceipt) updates) =>
      super.copyWith((message) => updates(message as ResolutionReceipt))
          as ResolutionReceipt;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ResolutionReceipt() / ResolutionReceipt.new instead')
  static ResolutionReceipt create() => ResolutionReceipt._();
  static $pb.GeneratedMessage $_createMessage() => ResolutionReceipt._();
  @$core.override
  ResolutionReceipt createEmptyInstance() => ResolutionReceipt._();
  @$core.pragma('dart2js:noInline')
  static ResolutionReceipt getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ResolutionReceipt>(
          ResolutionReceipt.$_createMessage);
  static ResolutionReceipt? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get version => $_getI64(1);
  @$pb.TagNumber(2)
  set version($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get operationId => $_getSZ(2);
  @$pb.TagNumber(3)
  set operationId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasOperationId() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperationId() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get conversationRevision => $_getI64(3);
  @$pb.TagNumber(4)
  set conversationRevision($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasConversationRevision() => $_has(3);
  @$pb.TagNumber(4)
  void clearConversationRevision() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.bool get replayed => $_getBF(4);
  @$pb.TagNumber(5)
  set replayed($core.bool value) => $_setBool(4, value);
  @$pb.TagNumber(5)
  $core.bool hasReplayed() => $_has(4);
  @$pb.TagNumber(5)
  void clearReplayed() => $_clearField(5);

  @$pb.TagNumber(6)
  InteractionOutcome get outcome => $_getN(5);
  @$pb.TagNumber(6)
  set outcome(InteractionOutcome value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasOutcome() => $_has(5);
  @$pb.TagNumber(6)
  void clearOutcome() => $_clearField(6);
  @$pb.TagNumber(6)
  InteractionOutcome ensureOutcome() => $_ensure(5);
}

class Attachment extends $pb.GeneratedMessage {
  factory Attachment({
    FileRef? file,
    $core.String? label,
  }) {
    final result = Attachment._();
    if (file != null) result.file = file;
    if (label != null) result.label = label;
    return result;
  }

  Attachment._();

  factory Attachment.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Attachment()..mergeFromBuffer(data, registry);
  factory Attachment.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Attachment()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Attachment',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Attachment.$_createMessage)
    ..aOM<FileRef>(1, _omitFieldNames ? '' : 'file',
        subBuilder: FileRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'label')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Attachment clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Attachment copyWith(void Function(Attachment) updates) =>
      super.copyWith((message) => updates(message as Attachment)) as Attachment;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Attachment() / Attachment.new instead')
  static Attachment create() => Attachment._();
  static $pb.GeneratedMessage $_createMessage() => Attachment._();
  @$core.override
  Attachment createEmptyInstance() => Attachment._();
  @$core.pragma('dart2js:noInline')
  static Attachment getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Attachment>(Attachment.$_createMessage);
  static Attachment? _defaultInstance;

  @$pb.TagNumber(1)
  FileRef get file => $_getN(0);
  @$pb.TagNumber(1)
  set file(FileRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasFile() => $_has(0);
  @$pb.TagNumber(1)
  void clearFile() => $_clearField(1);
  @$pb.TagNumber(1)
  FileRef ensureFile() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get label => $_getSZ(1);
  @$pb.TagNumber(2)
  set label($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasLabel() => $_has(1);
  @$pb.TagNumber(2)
  void clearLabel() => $_clearField(2);
}

class AttachmentItems extends $pb.GeneratedMessage {
  factory AttachmentItems({
    $core.Iterable<Attachment>? items,
  }) {
    final result = AttachmentItems._();
    if (items != null) result.items.addAll(items);
    return result;
  }

  AttachmentItems._();

  factory AttachmentItems.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AttachmentItems()..mergeFromBuffer(data, registry);
  factory AttachmentItems.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AttachmentItems()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AttachmentItems',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: AttachmentItems.$_createMessage)
    ..pPM<Attachment>(1, _omitFieldNames ? '' : 'items',
        subBuilder: Attachment.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AttachmentItems clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AttachmentItems copyWith(void Function(AttachmentItems) updates) =>
      super.copyWith((message) => updates(message as AttachmentItems))
          as AttachmentItems;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use AttachmentItems() / AttachmentItems.new instead')
  static AttachmentItems create() => AttachmentItems._();
  static $pb.GeneratedMessage $_createMessage() => AttachmentItems._();
  @$core.override
  AttachmentItems createEmptyInstance() => AttachmentItems._();
  @$core.pragma('dart2js:noInline')
  static AttachmentItems getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<AttachmentItems>(
          AttachmentItems.$_createMessage);
  static AttachmentItems? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<Attachment> get items => $_getList(0);
}

class AttachmentManifest extends $pb.GeneratedMessage {
  factory AttachmentManifest({
    FileRef? manifest,
    $core.int? itemCount,
  }) {
    final result = AttachmentManifest._();
    if (manifest != null) result.manifest = manifest;
    if (itemCount != null) result.itemCount = itemCount;
    return result;
  }

  AttachmentManifest._();

  factory AttachmentManifest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AttachmentManifest()..mergeFromBuffer(data, registry);
  factory AttachmentManifest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AttachmentManifest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AttachmentManifest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: AttachmentManifest.$_createMessage)
    ..aOM<FileRef>(1, _omitFieldNames ? '' : 'manifest',
        subBuilder: FileRef.$_createMessage)
    ..aI(2, _omitFieldNames ? '' : 'itemCount', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AttachmentManifest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AttachmentManifest copyWith(void Function(AttachmentManifest) updates) =>
      super.copyWith((message) => updates(message as AttachmentManifest))
          as AttachmentManifest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use AttachmentManifest() / AttachmentManifest.new instead')
  static AttachmentManifest create() => AttachmentManifest._();
  static $pb.GeneratedMessage $_createMessage() => AttachmentManifest._();
  @$core.override
  AttachmentManifest createEmptyInstance() => AttachmentManifest._();
  @$core.pragma('dart2js:noInline')
  static AttachmentManifest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<AttachmentManifest>(
          AttachmentManifest.$_createMessage);
  static AttachmentManifest? _defaultInstance;

  @$pb.TagNumber(1)
  FileRef get manifest => $_getN(0);
  @$pb.TagNumber(1)
  set manifest(FileRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasManifest() => $_has(0);
  @$pb.TagNumber(1)
  void clearManifest() => $_clearField(1);
  @$pb.TagNumber(1)
  FileRef ensureManifest() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.int get itemCount => $_getIZ(1);
  @$pb.TagNumber(2)
  set itemCount($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(2)
  $core.bool hasItemCount() => $_has(1);
  @$pb.TagNumber(2)
  void clearItemCount() => $_clearField(2);
}

enum ReferencedAttachments_Source { inlineItems, manifest, notSet }

class ReferencedAttachments extends $pb.GeneratedMessage {
  factory ReferencedAttachments({
    AttachmentItems? inlineItems,
    AttachmentManifest? manifest,
  }) {
    final result = ReferencedAttachments._();
    if (inlineItems != null) result.inlineItems = inlineItems;
    if (manifest != null) result.manifest = manifest;
    return result;
  }

  ReferencedAttachments._();

  factory ReferencedAttachments.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReferencedAttachments()..mergeFromBuffer(data, registry);
  factory ReferencedAttachments.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReferencedAttachments()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ReferencedAttachments_Source>
      _ReferencedAttachments_SourceByTag = {
    1: ReferencedAttachments_Source.inlineItems,
    2: ReferencedAttachments_Source.manifest,
    0: ReferencedAttachments_Source.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReferencedAttachments',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ReferencedAttachments.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<AttachmentItems>(1, _omitFieldNames ? '' : 'inlineItems',
        subBuilder: AttachmentItems.$_createMessage)
    ..aOM<AttachmentManifest>(2, _omitFieldNames ? '' : 'manifest',
        subBuilder: AttachmentManifest.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReferencedAttachments clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReferencedAttachments copyWith(
          void Function(ReferencedAttachments) updates) =>
      super.copyWith((message) => updates(message as ReferencedAttachments))
          as ReferencedAttachments;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ReferencedAttachments() / ReferencedAttachments.new instead')
  static ReferencedAttachments create() => ReferencedAttachments._();
  static $pb.GeneratedMessage $_createMessage() => ReferencedAttachments._();
  @$core.override
  ReferencedAttachments createEmptyInstance() => ReferencedAttachments._();
  @$core.pragma('dart2js:noInline')
  static ReferencedAttachments getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ReferencedAttachments>(
          ReferencedAttachments.$_createMessage);
  static ReferencedAttachments? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  ReferencedAttachments_Source whichSource() =>
      _ReferencedAttachments_SourceByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearSource() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  AttachmentItems get inlineItems => $_getN(0);
  @$pb.TagNumber(1)
  set inlineItems(AttachmentItems value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasInlineItems() => $_has(0);
  @$pb.TagNumber(1)
  void clearInlineItems() => $_clearField(1);
  @$pb.TagNumber(1)
  AttachmentItems ensureInlineItems() => $_ensure(0);

  @$pb.TagNumber(2)
  AttachmentManifest get manifest => $_getN(1);
  @$pb.TagNumber(2)
  set manifest(AttachmentManifest value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasManifest() => $_has(1);
  @$pb.TagNumber(2)
  void clearManifest() => $_clearField(2);
  @$pb.TagNumber(2)
  AttachmentManifest ensureManifest() => $_ensure(1);
}

class ConversationMessage extends $pb.GeneratedMessage {
  factory ConversationMessage({
    $core.String? id,
    $fixnum.Int64? sequence,
    ConversationKind? kind,
    FileRef? content,
    ReferencedAttachments? attachments,
    $core.String? replyTo,
    $core.String? toolCallId,
    $core.Iterable<$core.MapEntry<$core.String, FileRef>>? extensions,
  }) {
    final result = ConversationMessage._();
    if (id != null) result.id = id;
    if (sequence != null) result.sequence = sequence;
    if (kind != null) result.kind = kind;
    if (content != null) result.content = content;
    if (attachments != null) result.attachments = attachments;
    if (replyTo != null) result.replyTo = replyTo;
    if (toolCallId != null) result.toolCallId = toolCallId;
    if (extensions != null) result.extensions.addEntries(extensions);
    return result;
  }

  ConversationMessage._();

  factory ConversationMessage.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ConversationMessage()..mergeFromBuffer(data, registry);
  factory ConversationMessage.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ConversationMessage()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ConversationMessage',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ConversationMessage.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'id')
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'sequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aE<ConversationKind>(3, _omitFieldNames ? '' : 'kind',
        enumValues: ConversationKind.values)
    ..aOM<FileRef>(4, _omitFieldNames ? '' : 'content',
        subBuilder: FileRef.$_createMessage)
    ..aOM<ReferencedAttachments>(5, _omitFieldNames ? '' : 'attachments',
        subBuilder: ReferencedAttachments.$_createMessage)
    ..aOS(6, _omitFieldNames ? '' : 'replyTo')
    ..aOS(7, _omitFieldNames ? '' : 'toolCallId')
    ..m<$core.String, FileRef>(8, _omitFieldNames ? '' : 'extensions',
        entryClassName: 'ConversationMessage.ExtensionsEntry',
        keyFieldType: $pb.PbFieldType.OS,
        valueFieldType: $pb.PbFieldType.OM,
        valueCreator: FileRef.$_createMessage,
        valueDefaultOrMaker: FileRef.getDefault,
        packageName: const $pb.PackageName('acyclic.harness.v2'))
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ConversationMessage clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ConversationMessage copyWith(void Function(ConversationMessage) updates) =>
      super.copyWith((message) => updates(message as ConversationMessage))
          as ConversationMessage;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use ConversationMessage() / ConversationMessage.new instead')
  static ConversationMessage create() => ConversationMessage._();
  static $pb.GeneratedMessage $_createMessage() => ConversationMessage._();
  @$core.override
  ConversationMessage createEmptyInstance() => ConversationMessage._();
  @$core.pragma('dart2js:noInline')
  static ConversationMessage getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ConversationMessage>(
          ConversationMessage.$_createMessage);
  static ConversationMessage? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get id => $_getSZ(0);
  @$pb.TagNumber(1)
  set id($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get sequence => $_getI64(1);
  @$pb.TagNumber(2)
  set sequence($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSequence() => $_has(1);
  @$pb.TagNumber(2)
  void clearSequence() => $_clearField(2);

  @$pb.TagNumber(3)
  ConversationKind get kind => $_getN(2);
  @$pb.TagNumber(3)
  set kind(ConversationKind value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasKind() => $_has(2);
  @$pb.TagNumber(3)
  void clearKind() => $_clearField(3);

  @$pb.TagNumber(4)
  FileRef get content => $_getN(3);
  @$pb.TagNumber(4)
  set content(FileRef value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasContent() => $_has(3);
  @$pb.TagNumber(4)
  void clearContent() => $_clearField(4);
  @$pb.TagNumber(4)
  FileRef ensureContent() => $_ensure(3);

  @$pb.TagNumber(5)
  ReferencedAttachments get attachments => $_getN(4);
  @$pb.TagNumber(5)
  set attachments(ReferencedAttachments value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasAttachments() => $_has(4);
  @$pb.TagNumber(5)
  void clearAttachments() => $_clearField(5);
  @$pb.TagNumber(5)
  ReferencedAttachments ensureAttachments() => $_ensure(4);

  @$pb.TagNumber(6)
  $core.String get replyTo => $_getSZ(5);
  @$pb.TagNumber(6)
  set replyTo($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasReplyTo() => $_has(5);
  @$pb.TagNumber(6)
  void clearReplyTo() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get toolCallId => $_getSZ(6);
  @$pb.TagNumber(7)
  set toolCallId($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasToolCallId() => $_has(6);
  @$pb.TagNumber(7)
  void clearToolCallId() => $_clearField(7);

  @$pb.TagNumber(8)
  $pb.PbMap<$core.String, FileRef> get extensions => $_getMap(7);
}

class ResourceRef extends $pb.GeneratedMessage {
  factory ResourceRef({
    ProviderRef? provider,
    $core.List<$core.int>? key,
    $core.String? version,
    ResourceKind? kind,
  }) {
    final result = ResourceRef._();
    if (provider != null) result.provider = provider;
    if (key != null) result.key = key;
    if (version != null) result.version = version;
    if (kind != null) result.kind = kind;
    return result;
  }

  ResourceRef._();

  factory ResourceRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResourceRef()..mergeFromBuffer(data, registry);
  factory ResourceRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResourceRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ResourceRef',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ResourceRef.$_createMessage)
    ..aOM<ProviderRef>(1, _omitFieldNames ? '' : 'provider',
        subBuilder: ProviderRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'key', $pb.PbFieldType.OY)
    ..aOS(3, _omitFieldNames ? '' : 'version')
    ..aE<ResourceKind>(4, _omitFieldNames ? '' : 'kind',
        enumValues: ResourceKind.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResourceRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResourceRef copyWith(void Function(ResourceRef) updates) =>
      super.copyWith((message) => updates(message as ResourceRef))
          as ResourceRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ResourceRef() / ResourceRef.new instead')
  static ResourceRef create() => ResourceRef._();
  static $pb.GeneratedMessage $_createMessage() => ResourceRef._();
  @$core.override
  ResourceRef createEmptyInstance() => ResourceRef._();
  @$core.pragma('dart2js:noInline')
  static ResourceRef getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ResourceRef>(
          ResourceRef.$_createMessage);
  static ResourceRef? _defaultInstance;

  @$pb.TagNumber(1)
  ProviderRef get provider => $_getN(0);
  @$pb.TagNumber(1)
  set provider(ProviderRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProvider() => $_has(0);
  @$pb.TagNumber(1)
  void clearProvider() => $_clearField(1);
  @$pb.TagNumber(1)
  ProviderRef ensureProvider() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get key => $_getN(1);
  @$pb.TagNumber(2)
  set key($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get version => $_getSZ(2);
  @$pb.TagNumber(3)
  set version($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasVersion() => $_has(2);
  @$pb.TagNumber(3)
  void clearVersion() => $_clearField(3);

  @$pb.TagNumber(4)
  ResourceKind get kind => $_getN(3);
  @$pb.TagNumber(4)
  set kind(ResourceKind value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasKind() => $_has(3);
  @$pb.TagNumber(4)
  void clearKind() => $_clearField(4);
}

/// Durable task and execution contracts. JSON fields are canonical bounded
/// schema/value documents, never file bodies, credentials, or event payloads.
class ComponentIdentity extends $pb.GeneratedMessage {
  factory ComponentIdentity({
    $core.String? name,
    $core.String? version,
    $core.List<$core.int>? digest,
  }) {
    final result = ComponentIdentity._();
    if (name != null) result.name = name;
    if (version != null) result.version = version;
    if (digest != null) result.digest = digest;
    return result;
  }

  ComponentIdentity._();

  factory ComponentIdentity.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ComponentIdentity()..mergeFromBuffer(data, registry);
  factory ComponentIdentity.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ComponentIdentity()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ComponentIdentity',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ComponentIdentity.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aOS(2, _omitFieldNames ? '' : 'version')
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'digest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ComponentIdentity clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ComponentIdentity copyWith(void Function(ComponentIdentity) updates) =>
      super.copyWith((message) => updates(message as ComponentIdentity))
          as ComponentIdentity;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ComponentIdentity() / ComponentIdentity.new instead')
  static ComponentIdentity create() => ComponentIdentity._();
  static $pb.GeneratedMessage $_createMessage() => ComponentIdentity._();
  @$core.override
  ComponentIdentity createEmptyInstance() => ComponentIdentity._();
  @$core.pragma('dart2js:noInline')
  static ComponentIdentity getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ComponentIdentity>(
          ComponentIdentity.$_createMessage);
  static ComponentIdentity? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get version => $_getSZ(1);
  @$pb.TagNumber(2)
  set version($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get digest => $_getN(2);
  @$pb.TagNumber(3)
  set digest($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasDigest() => $_has(2);
  @$pb.TagNumber(3)
  void clearDigest() => $_clearField(3);
}

class MachineIdentity extends $pb.GeneratedMessage {
  factory MachineIdentity({
    $core.String? name,
    $core.String? version,
    $core.List<$core.int>? digest,
  }) {
    final result = MachineIdentity._();
    if (name != null) result.name = name;
    if (version != null) result.version = version;
    if (digest != null) result.digest = digest;
    return result;
  }

  MachineIdentity._();

  factory MachineIdentity.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineIdentity()..mergeFromBuffer(data, registry);
  factory MachineIdentity.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineIdentity()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachineIdentity',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: MachineIdentity.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aOS(2, _omitFieldNames ? '' : 'version')
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'digest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineIdentity clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineIdentity copyWith(void Function(MachineIdentity) updates) =>
      super.copyWith((message) => updates(message as MachineIdentity))
          as MachineIdentity;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MachineIdentity() / MachineIdentity.new instead')
  static MachineIdentity create() => MachineIdentity._();
  static $pb.GeneratedMessage $_createMessage() => MachineIdentity._();
  @$core.override
  MachineIdentity createEmptyInstance() => MachineIdentity._();
  @$core.pragma('dart2js:noInline')
  static MachineIdentity getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MachineIdentity>(
          MachineIdentity.$_createMessage);
  static MachineIdentity? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get version => $_getSZ(1);
  @$pb.TagNumber(2)
  set version($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get digest => $_getN(2);
  @$pb.TagNumber(3)
  set digest($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasDigest() => $_has(2);
  @$pb.TagNumber(3)
  void clearDigest() => $_clearField(3);
}

/// A resumable tool is admitted before any transition or effect. Its exact
/// initial state and implementation pin survive a lost admission reply.
class MachineCheckpoint extends $pb.GeneratedMessage {
  factory MachineCheckpoint({
    MachineIdentity? machine,
    $fixnum.Int64? revision,
    $core.List<$core.int>? canonicalStateJson,
  }) {
    final result = MachineCheckpoint._();
    if (machine != null) result.machine = machine;
    if (revision != null) result.revision = revision;
    if (canonicalStateJson != null)
      result.canonicalStateJson = canonicalStateJson;
    return result;
  }

  MachineCheckpoint._();

  factory MachineCheckpoint.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineCheckpoint()..mergeFromBuffer(data, registry);
  factory MachineCheckpoint.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineCheckpoint()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachineCheckpoint',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: MachineCheckpoint.$_createMessage)
    ..aOM<MachineIdentity>(1, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineIdentity.$_createMessage)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'canonicalStateJson', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineCheckpoint clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineCheckpoint copyWith(void Function(MachineCheckpoint) updates) =>
      super.copyWith((message) => updates(message as MachineCheckpoint))
          as MachineCheckpoint;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MachineCheckpoint() / MachineCheckpoint.new instead')
  static MachineCheckpoint create() => MachineCheckpoint._();
  static $pb.GeneratedMessage $_createMessage() => MachineCheckpoint._();
  @$core.override
  MachineCheckpoint createEmptyInstance() => MachineCheckpoint._();
  @$core.pragma('dart2js:noInline')
  static MachineCheckpoint getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MachineCheckpoint>(
          MachineCheckpoint.$_createMessage);
  static MachineCheckpoint? _defaultInstance;

  @$pb.TagNumber(1)
  MachineIdentity get machine => $_getN(0);
  @$pb.TagNumber(1)
  set machine(MachineIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasMachine() => $_has(0);
  @$pb.TagNumber(1)
  void clearMachine() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineIdentity ensureMachine() => $_ensure(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get revision => $_getI64(1);
  @$pb.TagNumber(2)
  set revision($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRevision() => $_has(1);
  @$pb.TagNumber(2)
  void clearRevision() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get canonicalStateJson => $_getN(2);
  @$pb.TagNumber(3)
  set canonicalStateJson($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasCanonicalStateJson() => $_has(2);
  @$pb.TagNumber(3)
  void clearCanonicalStateJson() => $_clearField(3);
}

class WorkflowAdmission extends $pb.GeneratedMessage {
  factory WorkflowAdmission({
    $core.String? operationId,
    $core.List<$core.int>? requestDigest,
    MachineCheckpoint? initial,
  }) {
    final result = WorkflowAdmission._();
    if (operationId != null) result.operationId = operationId;
    if (requestDigest != null) result.requestDigest = requestDigest;
    if (initial != null) result.initial = initial;
    return result;
  }

  WorkflowAdmission._();

  factory WorkflowAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkflowAdmission()..mergeFromBuffer(data, registry);
  factory WorkflowAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkflowAdmission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkflowAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: WorkflowAdmission.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'operationId')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'requestDigest', $pb.PbFieldType.OY)
    ..aOM<MachineCheckpoint>(3, _omitFieldNames ? '' : 'initial',
        subBuilder: MachineCheckpoint.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkflowAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkflowAdmission copyWith(void Function(WorkflowAdmission) updates) =>
      super.copyWith((message) => updates(message as WorkflowAdmission))
          as WorkflowAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WorkflowAdmission() / WorkflowAdmission.new instead')
  static WorkflowAdmission create() => WorkflowAdmission._();
  static $pb.GeneratedMessage $_createMessage() => WorkflowAdmission._();
  @$core.override
  WorkflowAdmission createEmptyInstance() => WorkflowAdmission._();
  @$core.pragma('dart2js:noInline')
  static WorkflowAdmission getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<WorkflowAdmission>(
          WorkflowAdmission.$_createMessage);
  static WorkflowAdmission? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get operationId => $_getSZ(0);
  @$pb.TagNumber(1)
  set operationId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOperationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperationId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get requestDigest => $_getN(1);
  @$pb.TagNumber(2)
  set requestDigest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRequestDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearRequestDigest() => $_clearField(2);

  @$pb.TagNumber(3)
  MachineCheckpoint get initial => $_getN(2);
  @$pb.TagNumber(3)
  set initial(MachineCheckpoint value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasInitial() => $_has(2);
  @$pb.TagNumber(3)
  void clearInitial() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineCheckpoint ensureInitial() => $_ensure(2);
}

class WorkflowCommand extends $pb.GeneratedMessage {
  factory WorkflowCommand({
    $core.String? operationId,
    $core.String? kind,
    FileRef? payload,
  }) {
    final result = WorkflowCommand._();
    if (operationId != null) result.operationId = operationId;
    if (kind != null) result.kind = kind;
    if (payload != null) result.payload = payload;
    return result;
  }

  WorkflowCommand._();

  factory WorkflowCommand.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkflowCommand()..mergeFromBuffer(data, registry);
  factory WorkflowCommand.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkflowCommand()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkflowCommand',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: WorkflowCommand.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'operationId')
    ..aOS(2, _omitFieldNames ? '' : 'kind')
    ..aOM<FileRef>(3, _omitFieldNames ? '' : 'payload',
        subBuilder: FileRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkflowCommand clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkflowCommand copyWith(void Function(WorkflowCommand) updates) =>
      super.copyWith((message) => updates(message as WorkflowCommand))
          as WorkflowCommand;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WorkflowCommand() / WorkflowCommand.new instead')
  static WorkflowCommand create() => WorkflowCommand._();
  static $pb.GeneratedMessage $_createMessage() => WorkflowCommand._();
  @$core.override
  WorkflowCommand createEmptyInstance() => WorkflowCommand._();
  @$core.pragma('dart2js:noInline')
  static WorkflowCommand getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<WorkflowCommand>(
          WorkflowCommand.$_createMessage);
  static WorkflowCommand? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get operationId => $_getSZ(0);
  @$pb.TagNumber(1)
  set operationId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOperationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperationId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get kind => $_getSZ(1);
  @$pb.TagNumber(2)
  set kind($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasKind() => $_has(1);
  @$pb.TagNumber(2)
  void clearKind() => $_clearField(2);

  @$pb.TagNumber(3)
  FileRef get payload => $_getN(2);
  @$pb.TagNumber(3)
  set payload(FileRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasPayload() => $_has(2);
  @$pb.TagNumber(3)
  void clearPayload() => $_clearField(3);
  @$pb.TagNumber(3)
  FileRef ensurePayload() => $_ensure(2);
}

enum WorkflowTransition_Status {
  suspended,
  canonicalCompletedValueJson,
  failureMessage,
  notSet
}

class WorkflowTransition extends $pb.GeneratedMessage {
  factory WorkflowTransition({
    $core.List<$core.int>? canonicalStateJson,
    $core.Iterable<WorkflowCommand>? commands,
    $core.bool? suspended,
    $core.List<$core.int>? canonicalCompletedValueJson,
    $core.String? failureMessage,
  }) {
    final result = WorkflowTransition._();
    if (canonicalStateJson != null)
      result.canonicalStateJson = canonicalStateJson;
    if (commands != null) result.commands.addAll(commands);
    if (suspended != null) result.suspended = suspended;
    if (canonicalCompletedValueJson != null)
      result.canonicalCompletedValueJson = canonicalCompletedValueJson;
    if (failureMessage != null) result.failureMessage = failureMessage;
    return result;
  }

  WorkflowTransition._();

  factory WorkflowTransition.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkflowTransition()..mergeFromBuffer(data, registry);
  factory WorkflowTransition.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkflowTransition()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, WorkflowTransition_Status>
      _WorkflowTransition_StatusByTag = {
    3: WorkflowTransition_Status.suspended,
    4: WorkflowTransition_Status.canonicalCompletedValueJson,
    5: WorkflowTransition_Status.failureMessage,
    0: WorkflowTransition_Status.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkflowTransition',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: WorkflowTransition.$_createMessage)
    ..oo(0, [3, 4, 5])
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'canonicalStateJson', $pb.PbFieldType.OY)
    ..pPM<WorkflowCommand>(2, _omitFieldNames ? '' : 'commands',
        subBuilder: WorkflowCommand.$_createMessage)
    ..aOB(3, _omitFieldNames ? '' : 'suspended')
    ..a<$core.List<$core.int>>(
        4,
        _omitFieldNames ? '' : 'canonicalCompletedValueJson',
        $pb.PbFieldType.OY)
    ..aOS(5, _omitFieldNames ? '' : 'failureMessage')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkflowTransition clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkflowTransition copyWith(void Function(WorkflowTransition) updates) =>
      super.copyWith((message) => updates(message as WorkflowTransition))
          as WorkflowTransition;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WorkflowTransition() / WorkflowTransition.new instead')
  static WorkflowTransition create() => WorkflowTransition._();
  static $pb.GeneratedMessage $_createMessage() => WorkflowTransition._();
  @$core.override
  WorkflowTransition createEmptyInstance() => WorkflowTransition._();
  @$core.pragma('dart2js:noInline')
  static WorkflowTransition getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<WorkflowTransition>(
          WorkflowTransition.$_createMessage);
  static WorkflowTransition? _defaultInstance;

  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  WorkflowTransition_Status whichStatus() =>
      _WorkflowTransition_StatusByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  void clearStatus() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.List<$core.int> get canonicalStateJson => $_getN(0);
  @$pb.TagNumber(1)
  set canonicalStateJson($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCanonicalStateJson() => $_has(0);
  @$pb.TagNumber(1)
  void clearCanonicalStateJson() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<WorkflowCommand> get commands => $_getList(1);

  @$pb.TagNumber(3)
  $core.bool get suspended => $_getBF(2);
  @$pb.TagNumber(3)
  set suspended($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasSuspended() => $_has(2);
  @$pb.TagNumber(3)
  void clearSuspended() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get canonicalCompletedValueJson => $_getN(3);
  @$pb.TagNumber(4)
  set canonicalCompletedValueJson($core.List<$core.int> value) =>
      $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCanonicalCompletedValueJson() => $_has(3);
  @$pb.TagNumber(4)
  void clearCanonicalCompletedValueJson() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get failureMessage => $_getSZ(4);
  @$pb.TagNumber(5)
  set failureMessage($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasFailureMessage() => $_has(4);
  @$pb.TagNumber(5)
  void clearFailureMessage() => $_clearField(5);
}

class WorkflowRecord extends $pb.GeneratedMessage {
  factory WorkflowRecord({
    $core.String? operationId,
    $core.String? idempotencyKey,
    $core.List<$core.int>? inputDigest,
    MachineCheckpoint? prior,
    $core.List<$core.int>? canonicalInputJson,
    WorkflowTransition? transition,
    MachineCheckpoint? next,
  }) {
    final result = WorkflowRecord._();
    if (operationId != null) result.operationId = operationId;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (inputDigest != null) result.inputDigest = inputDigest;
    if (prior != null) result.prior = prior;
    if (canonicalInputJson != null)
      result.canonicalInputJson = canonicalInputJson;
    if (transition != null) result.transition = transition;
    if (next != null) result.next = next;
    return result;
  }

  WorkflowRecord._();

  factory WorkflowRecord.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkflowRecord()..mergeFromBuffer(data, registry);
  factory WorkflowRecord.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkflowRecord()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkflowRecord',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: WorkflowRecord.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'operationId')
    ..aOS(2, _omitFieldNames ? '' : 'idempotencyKey')
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'inputDigest', $pb.PbFieldType.OY)
    ..aOM<MachineCheckpoint>(4, _omitFieldNames ? '' : 'prior',
        subBuilder: MachineCheckpoint.$_createMessage)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'canonicalInputJson', $pb.PbFieldType.OY)
    ..aOM<WorkflowTransition>(6, _omitFieldNames ? '' : 'transition',
        subBuilder: WorkflowTransition.$_createMessage)
    ..aOM<MachineCheckpoint>(7, _omitFieldNames ? '' : 'next',
        subBuilder: MachineCheckpoint.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkflowRecord clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkflowRecord copyWith(void Function(WorkflowRecord) updates) =>
      super.copyWith((message) => updates(message as WorkflowRecord))
          as WorkflowRecord;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WorkflowRecord() / WorkflowRecord.new instead')
  static WorkflowRecord create() => WorkflowRecord._();
  static $pb.GeneratedMessage $_createMessage() => WorkflowRecord._();
  @$core.override
  WorkflowRecord createEmptyInstance() => WorkflowRecord._();
  @$core.pragma('dart2js:noInline')
  static WorkflowRecord getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<WorkflowRecord>(
          WorkflowRecord.$_createMessage);
  static WorkflowRecord? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get operationId => $_getSZ(0);
  @$pb.TagNumber(1)
  set operationId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOperationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperationId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get idempotencyKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set idempotencyKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get inputDigest => $_getN(2);
  @$pb.TagNumber(3)
  set inputDigest($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasInputDigest() => $_has(2);
  @$pb.TagNumber(3)
  void clearInputDigest() => $_clearField(3);

  @$pb.TagNumber(4)
  MachineCheckpoint get prior => $_getN(3);
  @$pb.TagNumber(4)
  set prior(MachineCheckpoint value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasPrior() => $_has(3);
  @$pb.TagNumber(4)
  void clearPrior() => $_clearField(4);
  @$pb.TagNumber(4)
  MachineCheckpoint ensurePrior() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.List<$core.int> get canonicalInputJson => $_getN(4);
  @$pb.TagNumber(5)
  set canonicalInputJson($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasCanonicalInputJson() => $_has(4);
  @$pb.TagNumber(5)
  void clearCanonicalInputJson() => $_clearField(5);

  @$pb.TagNumber(6)
  WorkflowTransition get transition => $_getN(5);
  @$pb.TagNumber(6)
  set transition(WorkflowTransition value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasTransition() => $_has(5);
  @$pb.TagNumber(6)
  void clearTransition() => $_clearField(6);
  @$pb.TagNumber(6)
  WorkflowTransition ensureTransition() => $_ensure(5);

  @$pb.TagNumber(7)
  MachineCheckpoint get next => $_getN(6);
  @$pb.TagNumber(7)
  set next(MachineCheckpoint value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasNext() => $_has(6);
  @$pb.TagNumber(7)
  void clearNext() => $_clearField(7);
  @$pb.TagNumber(7)
  MachineCheckpoint ensureNext() => $_ensure(6);
}

class RuntimeLimits extends $pb.GeneratedMessage {
  factory RuntimeLimits({
    $fixnum.Int64? fileBytes,
    $fixnum.Int64? pathBytes,
    $fixnum.Int64? attachments,
    $fixnum.Int64? renderBytes,
    $fixnum.Int64? modelSteps,
    $fixnum.Int64? modelEventsPerStep,
    $fixnum.Int64? toolCallsPerStep,
    $fixnum.Int64? contextMessages,
  }) {
    final result = RuntimeLimits._();
    if (fileBytes != null) result.fileBytes = fileBytes;
    if (pathBytes != null) result.pathBytes = pathBytes;
    if (attachments != null) result.attachments = attachments;
    if (renderBytes != null) result.renderBytes = renderBytes;
    if (modelSteps != null) result.modelSteps = modelSteps;
    if (modelEventsPerStep != null)
      result.modelEventsPerStep = modelEventsPerStep;
    if (toolCallsPerStep != null) result.toolCallsPerStep = toolCallsPerStep;
    if (contextMessages != null) result.contextMessages = contextMessages;
    return result;
  }

  RuntimeLimits._();

  factory RuntimeLimits.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RuntimeLimits()..mergeFromBuffer(data, registry);
  factory RuntimeLimits.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RuntimeLimits()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RuntimeLimits',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: RuntimeLimits.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'fileBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'pathBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'attachments', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'renderBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'modelSteps', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        6, _omitFieldNames ? '' : 'modelEventsPerStep', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        7, _omitFieldNames ? '' : 'toolCallsPerStep', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        8, _omitFieldNames ? '' : 'contextMessages', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RuntimeLimits clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RuntimeLimits copyWith(void Function(RuntimeLimits) updates) =>
      super.copyWith((message) => updates(message as RuntimeLimits))
          as RuntimeLimits;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RuntimeLimits() / RuntimeLimits.new instead')
  static RuntimeLimits create() => RuntimeLimits._();
  static $pb.GeneratedMessage $_createMessage() => RuntimeLimits._();
  @$core.override
  RuntimeLimits createEmptyInstance() => RuntimeLimits._();
  @$core.pragma('dart2js:noInline')
  static RuntimeLimits getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RuntimeLimits>(
          RuntimeLimits.$_createMessage);
  static RuntimeLimits? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get fileBytes => $_getI64(0);
  @$pb.TagNumber(1)
  set fileBytes($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasFileBytes() => $_has(0);
  @$pb.TagNumber(1)
  void clearFileBytes() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get pathBytes => $_getI64(1);
  @$pb.TagNumber(2)
  set pathBytes($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPathBytes() => $_has(1);
  @$pb.TagNumber(2)
  void clearPathBytes() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get attachments => $_getI64(2);
  @$pb.TagNumber(3)
  set attachments($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasAttachments() => $_has(2);
  @$pb.TagNumber(3)
  void clearAttachments() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get renderBytes => $_getI64(3);
  @$pb.TagNumber(4)
  set renderBytes($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasRenderBytes() => $_has(3);
  @$pb.TagNumber(4)
  void clearRenderBytes() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get modelSteps => $_getI64(4);
  @$pb.TagNumber(5)
  set modelSteps($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasModelSteps() => $_has(4);
  @$pb.TagNumber(5)
  void clearModelSteps() => $_clearField(5);

  @$pb.TagNumber(6)
  $fixnum.Int64 get modelEventsPerStep => $_getI64(5);
  @$pb.TagNumber(6)
  set modelEventsPerStep($fixnum.Int64 value) => $_setInt64(5, value);
  @$pb.TagNumber(6)
  $core.bool hasModelEventsPerStep() => $_has(5);
  @$pb.TagNumber(6)
  void clearModelEventsPerStep() => $_clearField(6);

  @$pb.TagNumber(7)
  $fixnum.Int64 get toolCallsPerStep => $_getI64(6);
  @$pb.TagNumber(7)
  set toolCallsPerStep($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasToolCallsPerStep() => $_has(6);
  @$pb.TagNumber(7)
  void clearToolCallsPerStep() => $_clearField(7);

  @$pb.TagNumber(8)
  $fixnum.Int64 get contextMessages => $_getI64(7);
  @$pb.TagNumber(8)
  set contextMessages($fixnum.Int64 value) => $_setInt64(7, value);
  @$pb.TagNumber(8)
  $core.bool hasContextMessages() => $_has(7);
  @$pb.TagNumber(8)
  void clearContextMessages() => $_clearField(8);
}

class TaskRunLimits extends $pb.GeneratedMessage {
  factory TaskRunLimits({
    $fixnum.Int64? concurrency,
    $fixnum.Int64? maxSteps,
    $fixnum.Int64? deadlineEpochMs,
  }) {
    final result = TaskRunLimits._();
    if (concurrency != null) result.concurrency = concurrency;
    if (maxSteps != null) result.maxSteps = maxSteps;
    if (deadlineEpochMs != null) result.deadlineEpochMs = deadlineEpochMs;
    return result;
  }

  TaskRunLimits._();

  factory TaskRunLimits.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TaskRunLimits()..mergeFromBuffer(data, registry);
  factory TaskRunLimits.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TaskRunLimits()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TaskRunLimits',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: TaskRunLimits.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'concurrency', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'maxSteps', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'deadlineEpochMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TaskRunLimits clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TaskRunLimits copyWith(void Function(TaskRunLimits) updates) =>
      super.copyWith((message) => updates(message as TaskRunLimits))
          as TaskRunLimits;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TaskRunLimits() / TaskRunLimits.new instead')
  static TaskRunLimits create() => TaskRunLimits._();
  static $pb.GeneratedMessage $_createMessage() => TaskRunLimits._();
  @$core.override
  TaskRunLimits createEmptyInstance() => TaskRunLimits._();
  @$core.pragma('dart2js:noInline')
  static TaskRunLimits getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TaskRunLimits>(
          TaskRunLimits.$_createMessage);
  static TaskRunLimits? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get concurrency => $_getI64(0);
  @$pb.TagNumber(1)
  set concurrency($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasConcurrency() => $_has(0);
  @$pb.TagNumber(1)
  void clearConcurrency() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get maxSteps => $_getI64(1);
  @$pb.TagNumber(2)
  set maxSteps($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMaxSteps() => $_has(1);
  @$pb.TagNumber(2)
  void clearMaxSteps() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get deadlineEpochMs => $_getI64(2);
  @$pb.TagNumber(3)
  set deadlineEpochMs($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasDeadlineEpochMs() => $_has(2);
  @$pb.TagNumber(3)
  void clearDeadlineEpochMs() => $_clearField(3);
}

class ExecutionPlacement extends $pb.GeneratedMessage {
  factory ExecutionPlacement({
    ComponentIdentity? provider,
    ResourceRef? build,
    ResourceRef? environment,
    $core.List<$core.int>? readinessRevision,
  }) {
    final result = ExecutionPlacement._();
    if (provider != null) result.provider = provider;
    if (build != null) result.build = build;
    if (environment != null) result.environment = environment;
    if (readinessRevision != null) result.readinessRevision = readinessRevision;
    return result;
  }

  ExecutionPlacement._();

  factory ExecutionPlacement.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExecutionPlacement()..mergeFromBuffer(data, registry);
  factory ExecutionPlacement.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExecutionPlacement()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExecutionPlacement',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExecutionPlacement.$_createMessage)
    ..aOM<ComponentIdentity>(1, _omitFieldNames ? '' : 'provider',
        subBuilder: ComponentIdentity.$_createMessage)
    ..aOM<ResourceRef>(2, _omitFieldNames ? '' : 'build',
        subBuilder: ResourceRef.$_createMessage)
    ..aOM<ResourceRef>(3, _omitFieldNames ? '' : 'environment',
        subBuilder: ResourceRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'readinessRevision', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExecutionPlacement clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExecutionPlacement copyWith(void Function(ExecutionPlacement) updates) =>
      super.copyWith((message) => updates(message as ExecutionPlacement))
          as ExecutionPlacement;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExecutionPlacement() / ExecutionPlacement.new instead')
  static ExecutionPlacement create() => ExecutionPlacement._();
  static $pb.GeneratedMessage $_createMessage() => ExecutionPlacement._();
  @$core.override
  ExecutionPlacement createEmptyInstance() => ExecutionPlacement._();
  @$core.pragma('dart2js:noInline')
  static ExecutionPlacement getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ExecutionPlacement>(
          ExecutionPlacement.$_createMessage);
  static ExecutionPlacement? _defaultInstance;

  @$pb.TagNumber(1)
  ComponentIdentity get provider => $_getN(0);
  @$pb.TagNumber(1)
  set provider(ComponentIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProvider() => $_has(0);
  @$pb.TagNumber(1)
  void clearProvider() => $_clearField(1);
  @$pb.TagNumber(1)
  ComponentIdentity ensureProvider() => $_ensure(0);

  @$pb.TagNumber(2)
  ResourceRef get build => $_getN(1);
  @$pb.TagNumber(2)
  set build(ResourceRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasBuild() => $_has(1);
  @$pb.TagNumber(2)
  void clearBuild() => $_clearField(2);
  @$pb.TagNumber(2)
  ResourceRef ensureBuild() => $_ensure(1);

  @$pb.TagNumber(3)
  ResourceRef get environment => $_getN(2);
  @$pb.TagNumber(3)
  set environment(ResourceRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasEnvironment() => $_has(2);
  @$pb.TagNumber(3)
  void clearEnvironment() => $_clearField(3);
  @$pb.TagNumber(3)
  ResourceRef ensureEnvironment() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.List<$core.int> get readinessRevision => $_getN(3);
  @$pb.TagNumber(4)
  set readinessRevision($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasReadinessRevision() => $_has(3);
  @$pb.TagNumber(4)
  void clearReadinessRevision() => $_clearField(4);
}

class TaskAdmissionRecord extends $pb.GeneratedMessage {
  factory TaskAdmissionRecord({
    $core.String? operationId,
    ComponentIdentity? task,
    MachineIdentity? machine,
    $core.List<$core.int>? canonicalInputJson,
    $core.List<$core.int>? canonicalInputSchemaJson,
    $core.List<$core.int>? canonicalOutputSchemaJson,
    $core.String? parentTaskId,
    $core.Iterable<$core.String>? grants,
    RuntimeLimits? limits,
    ComponentIdentity? policy,
    ExtensionAdmission? extensions,
    ExecutionPlacement? execution,
    TaskRunLimits? runLimits,
  }) {
    final result = TaskAdmissionRecord._();
    if (operationId != null) result.operationId = operationId;
    if (task != null) result.task = task;
    if (machine != null) result.machine = machine;
    if (canonicalInputJson != null)
      result.canonicalInputJson = canonicalInputJson;
    if (canonicalInputSchemaJson != null)
      result.canonicalInputSchemaJson = canonicalInputSchemaJson;
    if (canonicalOutputSchemaJson != null)
      result.canonicalOutputSchemaJson = canonicalOutputSchemaJson;
    if (parentTaskId != null) result.parentTaskId = parentTaskId;
    if (grants != null) result.grants.addAll(grants);
    if (limits != null) result.limits = limits;
    if (policy != null) result.policy = policy;
    if (extensions != null) result.extensions = extensions;
    if (execution != null) result.execution = execution;
    if (runLimits != null) result.runLimits = runLimits;
    return result;
  }

  TaskAdmissionRecord._();

  factory TaskAdmissionRecord.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TaskAdmissionRecord()..mergeFromBuffer(data, registry);
  factory TaskAdmissionRecord.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TaskAdmissionRecord()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TaskAdmissionRecord',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: TaskAdmissionRecord.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'operationId')
    ..aOM<ComponentIdentity>(2, _omitFieldNames ? '' : 'task',
        subBuilder: ComponentIdentity.$_createMessage)
    ..aOM<MachineIdentity>(3, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineIdentity.$_createMessage)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'canonicalInputJson', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(5,
        _omitFieldNames ? '' : 'canonicalInputSchemaJson', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(6,
        _omitFieldNames ? '' : 'canonicalOutputSchemaJson', $pb.PbFieldType.OY)
    ..aOS(7, _omitFieldNames ? '' : 'parentTaskId')
    ..pPS(8, _omitFieldNames ? '' : 'grants')
    ..aOM<RuntimeLimits>(9, _omitFieldNames ? '' : 'limits',
        subBuilder: RuntimeLimits.$_createMessage)
    ..aOM<ComponentIdentity>(10, _omitFieldNames ? '' : 'policy',
        subBuilder: ComponentIdentity.$_createMessage)
    ..aOM<ExtensionAdmission>(11, _omitFieldNames ? '' : 'extensions',
        subBuilder: ExtensionAdmission.$_createMessage)
    ..aOM<ExecutionPlacement>(12, _omitFieldNames ? '' : 'execution',
        subBuilder: ExecutionPlacement.$_createMessage)
    ..aOM<TaskRunLimits>(13, _omitFieldNames ? '' : 'runLimits',
        subBuilder: TaskRunLimits.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TaskAdmissionRecord clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TaskAdmissionRecord copyWith(void Function(TaskAdmissionRecord) updates) =>
      super.copyWith((message) => updates(message as TaskAdmissionRecord))
          as TaskAdmissionRecord;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use TaskAdmissionRecord() / TaskAdmissionRecord.new instead')
  static TaskAdmissionRecord create() => TaskAdmissionRecord._();
  static $pb.GeneratedMessage $_createMessage() => TaskAdmissionRecord._();
  @$core.override
  TaskAdmissionRecord createEmptyInstance() => TaskAdmissionRecord._();
  @$core.pragma('dart2js:noInline')
  static TaskAdmissionRecord getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<TaskAdmissionRecord>(
          TaskAdmissionRecord.$_createMessage);
  static TaskAdmissionRecord? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get operationId => $_getSZ(0);
  @$pb.TagNumber(1)
  set operationId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOperationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperationId() => $_clearField(1);

  @$pb.TagNumber(2)
  ComponentIdentity get task => $_getN(1);
  @$pb.TagNumber(2)
  set task(ComponentIdentity value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasTask() => $_has(1);
  @$pb.TagNumber(2)
  void clearTask() => $_clearField(2);
  @$pb.TagNumber(2)
  ComponentIdentity ensureTask() => $_ensure(1);

  @$pb.TagNumber(3)
  MachineIdentity get machine => $_getN(2);
  @$pb.TagNumber(3)
  set machine(MachineIdentity value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMachine() => $_has(2);
  @$pb.TagNumber(3)
  void clearMachine() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineIdentity ensureMachine() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.List<$core.int> get canonicalInputJson => $_getN(3);
  @$pb.TagNumber(4)
  set canonicalInputJson($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCanonicalInputJson() => $_has(3);
  @$pb.TagNumber(4)
  void clearCanonicalInputJson() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get canonicalInputSchemaJson => $_getN(4);
  @$pb.TagNumber(5)
  set canonicalInputSchemaJson($core.List<$core.int> value) =>
      $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasCanonicalInputSchemaJson() => $_has(4);
  @$pb.TagNumber(5)
  void clearCanonicalInputSchemaJson() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.List<$core.int> get canonicalOutputSchemaJson => $_getN(5);
  @$pb.TagNumber(6)
  set canonicalOutputSchemaJson($core.List<$core.int> value) =>
      $_setBytes(5, value);
  @$pb.TagNumber(6)
  $core.bool hasCanonicalOutputSchemaJson() => $_has(5);
  @$pb.TagNumber(6)
  void clearCanonicalOutputSchemaJson() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get parentTaskId => $_getSZ(6);
  @$pb.TagNumber(7)
  set parentTaskId($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasParentTaskId() => $_has(6);
  @$pb.TagNumber(7)
  void clearParentTaskId() => $_clearField(7);

  @$pb.TagNumber(8)
  $pb.PbList<$core.String> get grants => $_getList(7);

  @$pb.TagNumber(9)
  RuntimeLimits get limits => $_getN(8);
  @$pb.TagNumber(9)
  set limits(RuntimeLimits value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasLimits() => $_has(8);
  @$pb.TagNumber(9)
  void clearLimits() => $_clearField(9);
  @$pb.TagNumber(9)
  RuntimeLimits ensureLimits() => $_ensure(8);

  @$pb.TagNumber(10)
  ComponentIdentity get policy => $_getN(9);
  @$pb.TagNumber(10)
  set policy(ComponentIdentity value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasPolicy() => $_has(9);
  @$pb.TagNumber(10)
  void clearPolicy() => $_clearField(10);
  @$pb.TagNumber(10)
  ComponentIdentity ensurePolicy() => $_ensure(9);

  @$pb.TagNumber(11)
  ExtensionAdmission get extensions => $_getN(10);
  @$pb.TagNumber(11)
  set extensions(ExtensionAdmission value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasExtensions() => $_has(10);
  @$pb.TagNumber(11)
  void clearExtensions() => $_clearField(11);
  @$pb.TagNumber(11)
  ExtensionAdmission ensureExtensions() => $_ensure(10);

  @$pb.TagNumber(12)
  ExecutionPlacement get execution => $_getN(11);
  @$pb.TagNumber(12)
  set execution(ExecutionPlacement value) => $_setField(12, value);
  @$pb.TagNumber(12)
  $core.bool hasExecution() => $_has(11);
  @$pb.TagNumber(12)
  void clearExecution() => $_clearField(12);
  @$pb.TagNumber(12)
  ExecutionPlacement ensureExecution() => $_ensure(11);

  @$pb.TagNumber(13)
  TaskRunLimits get runLimits => $_getN(12);
  @$pb.TagNumber(13)
  set runLimits(TaskRunLimits value) => $_setField(13, value);
  @$pb.TagNumber(13)
  $core.bool hasRunLimits() => $_has(12);
  @$pb.TagNumber(13)
  void clearRunLimits() => $_clearField(13);
  @$pb.TagNumber(13)
  TaskRunLimits ensureRunLimits() => $_ensure(12);
}

class DurableBatchRequest extends $pb.GeneratedMessage {
  factory DurableBatchRequest({
    $core.String? groupId,
    $core.String? batchId,
    BatchGroupPolicy? groupPolicy,
    ComponentIdentity? task,
    MachineIdentity? machine,
    $core.Iterable<$core.List<$core.int>>? canonicalInputJson,
    $core.List<$core.int>? canonicalInputSchemaJson,
    $core.List<$core.int>? canonicalOutputSchemaJson,
    $core.String? parentTaskId,
    $core.Iterable<$core.String>? grants,
    RuntimeLimits? limits,
    ExtensionAdmission? extensions,
    ComponentIdentity? policy,
    ExecutionPlacement? execution,
    TaskRunLimits? runLimits,
  }) {
    final result = DurableBatchRequest._();
    if (groupId != null) result.groupId = groupId;
    if (batchId != null) result.batchId = batchId;
    if (groupPolicy != null) result.groupPolicy = groupPolicy;
    if (task != null) result.task = task;
    if (machine != null) result.machine = machine;
    if (canonicalInputJson != null)
      result.canonicalInputJson.addAll(canonicalInputJson);
    if (canonicalInputSchemaJson != null)
      result.canonicalInputSchemaJson = canonicalInputSchemaJson;
    if (canonicalOutputSchemaJson != null)
      result.canonicalOutputSchemaJson = canonicalOutputSchemaJson;
    if (parentTaskId != null) result.parentTaskId = parentTaskId;
    if (grants != null) result.grants.addAll(grants);
    if (limits != null) result.limits = limits;
    if (extensions != null) result.extensions = extensions;
    if (policy != null) result.policy = policy;
    if (execution != null) result.execution = execution;
    if (runLimits != null) result.runLimits = runLimits;
    return result;
  }

  DurableBatchRequest._();

  factory DurableBatchRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DurableBatchRequest()..mergeFromBuffer(data, registry);
  factory DurableBatchRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DurableBatchRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DurableBatchRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: DurableBatchRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'groupId')
    ..aOS(2, _omitFieldNames ? '' : 'batchId')
    ..aE<BatchGroupPolicy>(3, _omitFieldNames ? '' : 'groupPolicy',
        enumValues: BatchGroupPolicy.values)
    ..aOM<ComponentIdentity>(4, _omitFieldNames ? '' : 'task',
        subBuilder: ComponentIdentity.$_createMessage)
    ..aOM<MachineIdentity>(5, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineIdentity.$_createMessage)
    ..p<$core.List<$core.int>>(
        6, _omitFieldNames ? '' : 'canonicalInputJson', $pb.PbFieldType.PY)
    ..a<$core.List<$core.int>>(7,
        _omitFieldNames ? '' : 'canonicalInputSchemaJson', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(8,
        _omitFieldNames ? '' : 'canonicalOutputSchemaJson', $pb.PbFieldType.OY)
    ..aOS(9, _omitFieldNames ? '' : 'parentTaskId')
    ..pPS(10, _omitFieldNames ? '' : 'grants')
    ..aOM<RuntimeLimits>(11, _omitFieldNames ? '' : 'limits',
        subBuilder: RuntimeLimits.$_createMessage)
    ..aOM<ExtensionAdmission>(12, _omitFieldNames ? '' : 'extensions',
        subBuilder: ExtensionAdmission.$_createMessage)
    ..aOM<ComponentIdentity>(13, _omitFieldNames ? '' : 'policy',
        subBuilder: ComponentIdentity.$_createMessage)
    ..aOM<ExecutionPlacement>(14, _omitFieldNames ? '' : 'execution',
        subBuilder: ExecutionPlacement.$_createMessage)
    ..aOM<TaskRunLimits>(15, _omitFieldNames ? '' : 'runLimits',
        subBuilder: TaskRunLimits.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DurableBatchRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DurableBatchRequest copyWith(void Function(DurableBatchRequest) updates) =>
      super.copyWith((message) => updates(message as DurableBatchRequest))
          as DurableBatchRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use DurableBatchRequest() / DurableBatchRequest.new instead')
  static DurableBatchRequest create() => DurableBatchRequest._();
  static $pb.GeneratedMessage $_createMessage() => DurableBatchRequest._();
  @$core.override
  DurableBatchRequest createEmptyInstance() => DurableBatchRequest._();
  @$core.pragma('dart2js:noInline')
  static DurableBatchRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DurableBatchRequest>(
          DurableBatchRequest.$_createMessage);
  static DurableBatchRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get groupId => $_getSZ(0);
  @$pb.TagNumber(1)
  set groupId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasGroupId() => $_has(0);
  @$pb.TagNumber(1)
  void clearGroupId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get batchId => $_getSZ(1);
  @$pb.TagNumber(2)
  set batchId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBatchId() => $_has(1);
  @$pb.TagNumber(2)
  void clearBatchId() => $_clearField(2);

  @$pb.TagNumber(3)
  BatchGroupPolicy get groupPolicy => $_getN(2);
  @$pb.TagNumber(3)
  set groupPolicy(BatchGroupPolicy value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasGroupPolicy() => $_has(2);
  @$pb.TagNumber(3)
  void clearGroupPolicy() => $_clearField(3);

  @$pb.TagNumber(4)
  ComponentIdentity get task => $_getN(3);
  @$pb.TagNumber(4)
  set task(ComponentIdentity value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasTask() => $_has(3);
  @$pb.TagNumber(4)
  void clearTask() => $_clearField(4);
  @$pb.TagNumber(4)
  ComponentIdentity ensureTask() => $_ensure(3);

  @$pb.TagNumber(5)
  MachineIdentity get machine => $_getN(4);
  @$pb.TagNumber(5)
  set machine(MachineIdentity value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasMachine() => $_has(4);
  @$pb.TagNumber(5)
  void clearMachine() => $_clearField(5);
  @$pb.TagNumber(5)
  MachineIdentity ensureMachine() => $_ensure(4);

  @$pb.TagNumber(6)
  $pb.PbList<$core.List<$core.int>> get canonicalInputJson => $_getList(5);

  @$pb.TagNumber(7)
  $core.List<$core.int> get canonicalInputSchemaJson => $_getN(6);
  @$pb.TagNumber(7)
  set canonicalInputSchemaJson($core.List<$core.int> value) =>
      $_setBytes(6, value);
  @$pb.TagNumber(7)
  $core.bool hasCanonicalInputSchemaJson() => $_has(6);
  @$pb.TagNumber(7)
  void clearCanonicalInputSchemaJson() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.List<$core.int> get canonicalOutputSchemaJson => $_getN(7);
  @$pb.TagNumber(8)
  set canonicalOutputSchemaJson($core.List<$core.int> value) =>
      $_setBytes(7, value);
  @$pb.TagNumber(8)
  $core.bool hasCanonicalOutputSchemaJson() => $_has(7);
  @$pb.TagNumber(8)
  void clearCanonicalOutputSchemaJson() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.String get parentTaskId => $_getSZ(8);
  @$pb.TagNumber(9)
  set parentTaskId($core.String value) => $_setString(8, value);
  @$pb.TagNumber(9)
  $core.bool hasParentTaskId() => $_has(8);
  @$pb.TagNumber(9)
  void clearParentTaskId() => $_clearField(9);

  @$pb.TagNumber(10)
  $pb.PbList<$core.String> get grants => $_getList(9);

  @$pb.TagNumber(11)
  RuntimeLimits get limits => $_getN(10);
  @$pb.TagNumber(11)
  set limits(RuntimeLimits value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasLimits() => $_has(10);
  @$pb.TagNumber(11)
  void clearLimits() => $_clearField(11);
  @$pb.TagNumber(11)
  RuntimeLimits ensureLimits() => $_ensure(10);

  @$pb.TagNumber(12)
  ExtensionAdmission get extensions => $_getN(11);
  @$pb.TagNumber(12)
  set extensions(ExtensionAdmission value) => $_setField(12, value);
  @$pb.TagNumber(12)
  $core.bool hasExtensions() => $_has(11);
  @$pb.TagNumber(12)
  void clearExtensions() => $_clearField(12);
  @$pb.TagNumber(12)
  ExtensionAdmission ensureExtensions() => $_ensure(11);

  @$pb.TagNumber(13)
  ComponentIdentity get policy => $_getN(12);
  @$pb.TagNumber(13)
  set policy(ComponentIdentity value) => $_setField(13, value);
  @$pb.TagNumber(13)
  $core.bool hasPolicy() => $_has(12);
  @$pb.TagNumber(13)
  void clearPolicy() => $_clearField(13);
  @$pb.TagNumber(13)
  ComponentIdentity ensurePolicy() => $_ensure(12);

  @$pb.TagNumber(14)
  ExecutionPlacement get execution => $_getN(13);
  @$pb.TagNumber(14)
  set execution(ExecutionPlacement value) => $_setField(14, value);
  @$pb.TagNumber(14)
  $core.bool hasExecution() => $_has(13);
  @$pb.TagNumber(14)
  void clearExecution() => $_clearField(14);
  @$pb.TagNumber(14)
  ExecutionPlacement ensureExecution() => $_ensure(13);

  @$pb.TagNumber(15)
  TaskRunLimits get runLimits => $_getN(14);
  @$pb.TagNumber(15)
  set runLimits(TaskRunLimits value) => $_setField(15, value);
  @$pb.TagNumber(15)
  $core.bool hasRunLimits() => $_has(14);
  @$pb.TagNumber(15)
  void clearRunLimits() => $_clearField(15);
  @$pb.TagNumber(15)
  TaskRunLimits ensureRunLimits() => $_ensure(14);
}

/// Narrows a provider-owned resource to an immutable Filesystem generation.
class GenerationRef extends $pb.GeneratedMessage {
  factory GenerationRef({
    ResourceRef? resource,
  }) {
    final result = GenerationRef._();
    if (resource != null) result.resource = resource;
    return result;
  }

  GenerationRef._();

  factory GenerationRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerationRef()..mergeFromBuffer(data, registry);
  factory GenerationRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerationRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GenerationRef',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: GenerationRef.$_createMessage)
    ..aOM<ResourceRef>(1, _omitFieldNames ? '' : 'resource',
        subBuilder: ResourceRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerationRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerationRef copyWith(void Function(GenerationRef) updates) =>
      super.copyWith((message) => updates(message as GenerationRef))
          as GenerationRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use GenerationRef() / GenerationRef.new instead')
  static GenerationRef create() => GenerationRef._();
  static $pb.GeneratedMessage $_createMessage() => GenerationRef._();
  @$core.override
  GenerationRef createEmptyInstance() => GenerationRef._();
  @$core.pragma('dart2js:noInline')
  static GenerationRef getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GenerationRef>(
          GenerationRef.$_createMessage);
  static GenerationRef? _defaultInstance;

  @$pb.TagNumber(1)
  ResourceRef get resource => $_getN(0);
  @$pb.TagNumber(1)
  set resource(ResourceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasResource() => $_has(0);
  @$pb.TagNumber(1)
  void clearResource() => $_clearField(1);
  @$pb.TagNumber(1)
  ResourceRef ensureResource() => $_ensure(0);
}

/// One owner-authenticated lazy listing of an agent-private directory.
class PrivateDirectoryEntry extends $pb.GeneratedMessage {
  factory PrivateDirectoryEntry({
    $core.String? name,
    PrivateDirectoryEntry_Kind? kind,
  }) {
    final result = PrivateDirectoryEntry._();
    if (name != null) result.name = name;
    if (kind != null) result.kind = kind;
    return result;
  }

  PrivateDirectoryEntry._();

  factory PrivateDirectoryEntry.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PrivateDirectoryEntry()..mergeFromBuffer(data, registry);
  factory PrivateDirectoryEntry.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PrivateDirectoryEntry()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PrivateDirectoryEntry',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: PrivateDirectoryEntry.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aE<PrivateDirectoryEntry_Kind>(2, _omitFieldNames ? '' : 'kind',
        enumValues: PrivateDirectoryEntry_Kind.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PrivateDirectoryEntry clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PrivateDirectoryEntry copyWith(
          void Function(PrivateDirectoryEntry) updates) =>
      super.copyWith((message) => updates(message as PrivateDirectoryEntry))
          as PrivateDirectoryEntry;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use PrivateDirectoryEntry() / PrivateDirectoryEntry.new instead')
  static PrivateDirectoryEntry create() => PrivateDirectoryEntry._();
  static $pb.GeneratedMessage $_createMessage() => PrivateDirectoryEntry._();
  @$core.override
  PrivateDirectoryEntry createEmptyInstance() => PrivateDirectoryEntry._();
  @$core.pragma('dart2js:noInline')
  static PrivateDirectoryEntry getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PrivateDirectoryEntry>(
          PrivateDirectoryEntry.$_createMessage);
  static PrivateDirectoryEntry? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  PrivateDirectoryEntry_Kind get kind => $_getN(1);
  @$pb.TagNumber(2)
  set kind(PrivateDirectoryEntry_Kind value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasKind() => $_has(1);
  @$pb.TagNumber(2)
  void clearKind() => $_clearField(2);
}

class PrivateDirectoryPage extends $pb.GeneratedMessage {
  factory PrivateDirectoryPage({
    GenerationRef? generation,
    $core.Iterable<PrivateDirectoryEntry>? entries,
    $core.bool? hasMore,
  }) {
    final result = PrivateDirectoryPage._();
    if (generation != null) result.generation = generation;
    if (entries != null) result.entries.addAll(entries);
    if (hasMore != null) result.hasMore = hasMore;
    return result;
  }

  PrivateDirectoryPage._();

  factory PrivateDirectoryPage.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PrivateDirectoryPage()..mergeFromBuffer(data, registry);
  factory PrivateDirectoryPage.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PrivateDirectoryPage()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PrivateDirectoryPage',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: PrivateDirectoryPage.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<PrivateDirectoryEntry>(2, _omitFieldNames ? '' : 'entries',
        subBuilder: PrivateDirectoryEntry.$_createMessage)
    ..aOB(3, _omitFieldNames ? '' : 'hasMore')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PrivateDirectoryPage clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PrivateDirectoryPage copyWith(void Function(PrivateDirectoryPage) updates) =>
      super.copyWith((message) => updates(message as PrivateDirectoryPage))
          as PrivateDirectoryPage;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use PrivateDirectoryPage() / PrivateDirectoryPage.new instead')
  static PrivateDirectoryPage create() => PrivateDirectoryPage._();
  static $pb.GeneratedMessage $_createMessage() => PrivateDirectoryPage._();
  @$core.override
  PrivateDirectoryPage createEmptyInstance() => PrivateDirectoryPage._();
  @$core.pragma('dart2js:noInline')
  static PrivateDirectoryPage getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PrivateDirectoryPage>(
          PrivateDirectoryPage.$_createMessage);
  static PrivateDirectoryPage? _defaultInstance;

  @$pb.TagNumber(1)
  GenerationRef get generation => $_getN(0);
  @$pb.TagNumber(1)
  set generation(GenerationRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasGeneration() => $_has(0);
  @$pb.TagNumber(1)
  void clearGeneration() => $_clearField(1);
  @$pb.TagNumber(1)
  GenerationRef ensureGeneration() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<PrivateDirectoryEntry> get entries => $_getList(1);

  @$pb.TagNumber(3)
  $core.bool get hasMore => $_getBF(2);
  @$pb.TagNumber(3)
  set hasMore($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasHasMore() => $_has(2);
  @$pb.TagNumber(3)
  void clearHasMore() => $_clearField(3);
}

class ProjectRevision extends $pb.GeneratedMessage {
  factory ProjectRevision({
    VolumeRef? volume,
    GenerationRef? generation,
  }) {
    final result = ProjectRevision._();
    if (volume != null) result.volume = volume;
    if (generation != null) result.generation = generation;
    return result;
  }

  ProjectRevision._();

  factory ProjectRevision.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProjectRevision()..mergeFromBuffer(data, registry);
  factory ProjectRevision.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProjectRevision()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProjectRevision',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ProjectRevision.$_createMessage)
    ..aOM<VolumeRef>(1, _omitFieldNames ? '' : 'volume',
        subBuilder: VolumeRef.$_createMessage)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProjectRevision clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProjectRevision copyWith(void Function(ProjectRevision) updates) =>
      super.copyWith((message) => updates(message as ProjectRevision))
          as ProjectRevision;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ProjectRevision() / ProjectRevision.new instead')
  static ProjectRevision create() => ProjectRevision._();
  static $pb.GeneratedMessage $_createMessage() => ProjectRevision._();
  @$core.override
  ProjectRevision createEmptyInstance() => ProjectRevision._();
  @$core.pragma('dart2js:noInline')
  static ProjectRevision getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ProjectRevision>(
          ProjectRevision.$_createMessage);
  static ProjectRevision? _defaultInstance;

  @$pb.TagNumber(1)
  VolumeRef get volume => $_getN(0);
  @$pb.TagNumber(1)
  set volume(VolumeRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasVolume() => $_has(0);
  @$pb.TagNumber(1)
  void clearVolume() => $_clearField(1);
  @$pb.TagNumber(1)
  VolumeRef ensureVolume() => $_ensure(0);

  @$pb.TagNumber(2)
  GenerationRef get generation => $_getN(1);
  @$pb.TagNumber(2)
  set generation(GenerationRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasGeneration() => $_has(1);
  @$pb.TagNumber(2)
  void clearGeneration() => $_clearField(2);
  @$pb.TagNumber(2)
  GenerationRef ensureGeneration() => $_ensure(1);
}

class ExtensionRevision extends $pb.GeneratedMessage {
  factory ExtensionRevision({
    $core.String? name,
    $core.int? version,
    ResourceRef? reference,
    $core.List<$core.int>? implementationDigest,
  }) {
    final result = ExtensionRevision._();
    if (name != null) result.name = name;
    if (version != null) result.version = version;
    if (reference != null) result.reference = reference;
    if (implementationDigest != null)
      result.implementationDigest = implementationDigest;
    return result;
  }

  ExtensionRevision._();

  factory ExtensionRevision.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionRevision()..mergeFromBuffer(data, registry);
  factory ExtensionRevision.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExtensionRevision()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExtensionRevision',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ExtensionRevision.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aI(2, _omitFieldNames ? '' : 'version', fieldType: $pb.PbFieldType.OU3)
    ..aOM<ResourceRef>(3, _omitFieldNames ? '' : 'reference',
        subBuilder: ResourceRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'implementationDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionRevision clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExtensionRevision copyWith(void Function(ExtensionRevision) updates) =>
      super.copyWith((message) => updates(message as ExtensionRevision))
          as ExtensionRevision;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExtensionRevision() / ExtensionRevision.new instead')
  static ExtensionRevision create() => ExtensionRevision._();
  static $pb.GeneratedMessage $_createMessage() => ExtensionRevision._();
  @$core.override
  ExtensionRevision createEmptyInstance() => ExtensionRevision._();
  @$core.pragma('dart2js:noInline')
  static ExtensionRevision getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ExtensionRevision>(
          ExtensionRevision.$_createMessage);
  static ExtensionRevision? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.int get version => $_getIZ(1);
  @$pb.TagNumber(2)
  set version($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);

  @$pb.TagNumber(3)
  ResourceRef get reference => $_getN(2);
  @$pb.TagNumber(3)
  set reference(ResourceRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasReference() => $_has(2);
  @$pb.TagNumber(3)
  void clearReference() => $_clearField(3);
  @$pb.TagNumber(3)
  ResourceRef ensureReference() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.List<$core.int> get implementationDigest => $_getN(3);
  @$pb.TagNumber(4)
  set implementationDigest($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasImplementationDigest() => $_has(3);
  @$pb.TagNumber(4)
  void clearImplementationDigest() => $_clearField(4);
}

enum ResourceRevision_Kind {
  history,
  project,
  context,
  process,
  artifact,
  sharedVolume,
  extension_7,
  notSet
}

class ResourceRevision extends $pb.GeneratedMessage {
  factory ResourceRevision({
    ResourceRef? history,
    ProjectRevision? project,
    ResourceRef? context,
    ResourceRef? process,
    ResourceRef? artifact,
    VolumeRef? sharedVolume,
    ExtensionRevision? extension_7,
  }) {
    final result = ResourceRevision._();
    if (history != null) result.history = history;
    if (project != null) result.project = project;
    if (context != null) result.context = context;
    if (process != null) result.process = process;
    if (artifact != null) result.artifact = artifact;
    if (sharedVolume != null) result.sharedVolume = sharedVolume;
    if (extension_7 != null) result.extension_7 = extension_7;
    return result;
  }

  ResourceRevision._();

  factory ResourceRevision.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResourceRevision()..mergeFromBuffer(data, registry);
  factory ResourceRevision.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResourceRevision()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ResourceRevision_Kind>
      _ResourceRevision_KindByTag = {
    1: ResourceRevision_Kind.history,
    2: ResourceRevision_Kind.project,
    3: ResourceRevision_Kind.context,
    4: ResourceRevision_Kind.process,
    5: ResourceRevision_Kind.artifact,
    6: ResourceRevision_Kind.sharedVolume,
    7: ResourceRevision_Kind.extension_7,
    0: ResourceRevision_Kind.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ResourceRevision',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ResourceRevision.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5, 6, 7])
    ..aOM<ResourceRef>(1, _omitFieldNames ? '' : 'history',
        subBuilder: ResourceRef.$_createMessage)
    ..aOM<ProjectRevision>(2, _omitFieldNames ? '' : 'project',
        subBuilder: ProjectRevision.$_createMessage)
    ..aOM<ResourceRef>(3, _omitFieldNames ? '' : 'context',
        subBuilder: ResourceRef.$_createMessage)
    ..aOM<ResourceRef>(4, _omitFieldNames ? '' : 'process',
        subBuilder: ResourceRef.$_createMessage)
    ..aOM<ResourceRef>(5, _omitFieldNames ? '' : 'artifact',
        subBuilder: ResourceRef.$_createMessage)
    ..aOM<VolumeRef>(6, _omitFieldNames ? '' : 'sharedVolume',
        subBuilder: VolumeRef.$_createMessage)
    ..aOM<ExtensionRevision>(7, _omitFieldNames ? '' : 'extension',
        subBuilder: ExtensionRevision.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResourceRevision clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResourceRevision copyWith(void Function(ResourceRevision) updates) =>
      super.copyWith((message) => updates(message as ResourceRevision))
          as ResourceRevision;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ResourceRevision() / ResourceRevision.new instead')
  static ResourceRevision create() => ResourceRevision._();
  static $pb.GeneratedMessage $_createMessage() => ResourceRevision._();
  @$core.override
  ResourceRevision createEmptyInstance() => ResourceRevision._();
  @$core.pragma('dart2js:noInline')
  static ResourceRevision getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ResourceRevision>(
          ResourceRevision.$_createMessage);
  static ResourceRevision? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  ResourceRevision_Kind whichKind() =>
      _ResourceRevision_KindByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  void clearKind() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  ResourceRef get history => $_getN(0);
  @$pb.TagNumber(1)
  set history(ResourceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHistory() => $_has(0);
  @$pb.TagNumber(1)
  void clearHistory() => $_clearField(1);
  @$pb.TagNumber(1)
  ResourceRef ensureHistory() => $_ensure(0);

  @$pb.TagNumber(2)
  ProjectRevision get project => $_getN(1);
  @$pb.TagNumber(2)
  set project(ProjectRevision value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasProject() => $_has(1);
  @$pb.TagNumber(2)
  void clearProject() => $_clearField(2);
  @$pb.TagNumber(2)
  ProjectRevision ensureProject() => $_ensure(1);

  @$pb.TagNumber(3)
  ResourceRef get context => $_getN(2);
  @$pb.TagNumber(3)
  set context(ResourceRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasContext() => $_has(2);
  @$pb.TagNumber(3)
  void clearContext() => $_clearField(3);
  @$pb.TagNumber(3)
  ResourceRef ensureContext() => $_ensure(2);

  @$pb.TagNumber(4)
  ResourceRef get process => $_getN(3);
  @$pb.TagNumber(4)
  set process(ResourceRef value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasProcess() => $_has(3);
  @$pb.TagNumber(4)
  void clearProcess() => $_clearField(4);
  @$pb.TagNumber(4)
  ResourceRef ensureProcess() => $_ensure(3);

  @$pb.TagNumber(5)
  ResourceRef get artifact => $_getN(4);
  @$pb.TagNumber(5)
  set artifact(ResourceRef value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasArtifact() => $_has(4);
  @$pb.TagNumber(5)
  void clearArtifact() => $_clearField(5);
  @$pb.TagNumber(5)
  ResourceRef ensureArtifact() => $_ensure(4);

  @$pb.TagNumber(6)
  VolumeRef get sharedVolume => $_getN(5);
  @$pb.TagNumber(6)
  set sharedVolume(VolumeRef value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasSharedVolume() => $_has(5);
  @$pb.TagNumber(6)
  void clearSharedVolume() => $_clearField(6);
  @$pb.TagNumber(6)
  VolumeRef ensureSharedVolume() => $_ensure(5);

  @$pb.TagNumber(7)
  ExtensionRevision get extension_7 => $_getN(6);
  @$pb.TagNumber(7)
  set extension_7(ExtensionRevision value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasExtension_7() => $_has(6);
  @$pb.TagNumber(7)
  void clearExtension_7() => $_clearField(7);
  @$pb.TagNumber(7)
  ExtensionRevision ensureExtension_7() => $_ensure(6);
}

class CapturedResource extends $pb.GeneratedMessage {
  factory CapturedResource({
    ResourceRevision? source,
    ResourceRevision? revision,
  }) {
    final result = CapturedResource._();
    if (source != null) result.source = source;
    if (revision != null) result.revision = revision;
    return result;
  }

  CapturedResource._();

  factory CapturedResource.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CapturedResource()..mergeFromBuffer(data, registry);
  factory CapturedResource.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CapturedResource()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CapturedResource',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: CapturedResource.$_createMessage)
    ..aOM<ResourceRevision>(1, _omitFieldNames ? '' : 'source',
        subBuilder: ResourceRevision.$_createMessage)
    ..aOM<ResourceRevision>(2, _omitFieldNames ? '' : 'revision',
        subBuilder: ResourceRevision.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CapturedResource clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CapturedResource copyWith(void Function(CapturedResource) updates) =>
      super.copyWith((message) => updates(message as CapturedResource))
          as CapturedResource;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CapturedResource() / CapturedResource.new instead')
  static CapturedResource create() => CapturedResource._();
  static $pb.GeneratedMessage $_createMessage() => CapturedResource._();
  @$core.override
  CapturedResource createEmptyInstance() => CapturedResource._();
  @$core.pragma('dart2js:noInline')
  static CapturedResource getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CapturedResource>(
          CapturedResource.$_createMessage);
  static CapturedResource? _defaultInstance;

  @$pb.TagNumber(1)
  ResourceRevision get source => $_getN(0);
  @$pb.TagNumber(1)
  set source(ResourceRevision value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);
  @$pb.TagNumber(1)
  ResourceRevision ensureSource() => $_ensure(0);

  @$pb.TagNumber(2)
  ResourceRevision get revision => $_getN(1);
  @$pb.TagNumber(2)
  set revision(ResourceRevision value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasRevision() => $_has(1);
  @$pb.TagNumber(2)
  void clearRevision() => $_clearField(2);
  @$pb.TagNumber(2)
  ResourceRevision ensureRevision() => $_ensure(1);
}

enum ForkOmission_Outcome {
  unsupportedReason,
  inFlightOperationId,
  indeterminateOperationId,
  notSet
}

class ForkOmission extends $pb.GeneratedMessage {
  factory ForkOmission({
    ResourceRevision? selection,
    $core.String? unsupportedReason,
    $core.String? inFlightOperationId,
    $core.String? indeterminateOperationId,
  }) {
    final result = ForkOmission._();
    if (selection != null) result.selection = selection;
    if (unsupportedReason != null) result.unsupportedReason = unsupportedReason;
    if (inFlightOperationId != null)
      result.inFlightOperationId = inFlightOperationId;
    if (indeterminateOperationId != null)
      result.indeterminateOperationId = indeterminateOperationId;
    return result;
  }

  ForkOmission._();

  factory ForkOmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkOmission()..mergeFromBuffer(data, registry);
  factory ForkOmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkOmission()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ForkOmission_Outcome>
      _ForkOmission_OutcomeByTag = {
    2: ForkOmission_Outcome.unsupportedReason,
    3: ForkOmission_Outcome.inFlightOperationId,
    4: ForkOmission_Outcome.indeterminateOperationId,
    0: ForkOmission_Outcome.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkOmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ForkOmission.$_createMessage)
    ..oo(0, [2, 3, 4])
    ..aOM<ResourceRevision>(1, _omitFieldNames ? '' : 'selection',
        subBuilder: ResourceRevision.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'unsupportedReason')
    ..aOS(3, _omitFieldNames ? '' : 'inFlightOperationId')
    ..aOS(4, _omitFieldNames ? '' : 'indeterminateOperationId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkOmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkOmission copyWith(void Function(ForkOmission) updates) =>
      super.copyWith((message) => updates(message as ForkOmission))
          as ForkOmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkOmission() / ForkOmission.new instead')
  static ForkOmission create() => ForkOmission._();
  static $pb.GeneratedMessage $_createMessage() => ForkOmission._();
  @$core.override
  ForkOmission createEmptyInstance() => ForkOmission._();
  @$core.pragma('dart2js:noInline')
  static ForkOmission getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkOmission>(
          ForkOmission.$_createMessage);
  static ForkOmission? _defaultInstance;

  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  ForkOmission_Outcome whichOutcome() =>
      _ForkOmission_OutcomeByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  void clearOutcome() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  ResourceRevision get selection => $_getN(0);
  @$pb.TagNumber(1)
  set selection(ResourceRevision value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSelection() => $_has(0);
  @$pb.TagNumber(1)
  void clearSelection() => $_clearField(1);
  @$pb.TagNumber(1)
  ResourceRevision ensureSelection() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get unsupportedReason => $_getSZ(1);
  @$pb.TagNumber(2)
  set unsupportedReason($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUnsupportedReason() => $_has(1);
  @$pb.TagNumber(2)
  void clearUnsupportedReason() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get inFlightOperationId => $_getSZ(2);
  @$pb.TagNumber(3)
  set inFlightOperationId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasInFlightOperationId() => $_has(2);
  @$pb.TagNumber(3)
  void clearInFlightOperationId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get indeterminateOperationId => $_getSZ(3);
  @$pb.TagNumber(4)
  set indeterminateOperationId($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIndeterminateOperationId() => $_has(3);
  @$pb.TagNumber(4)
  void clearIndeterminateOperationId() => $_clearField(4);
}

class AttestedBoundary extends $pb.GeneratedMessage {
  factory AttestedBoundary({
    ProviderRef? provider,
    $core.List<$core.int>? evidence,
  }) {
    final result = AttestedBoundary._();
    if (provider != null) result.provider = provider;
    if (evidence != null) result.evidence = evidence;
    return result;
  }

  AttestedBoundary._();

  factory AttestedBoundary.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AttestedBoundary()..mergeFromBuffer(data, registry);
  factory AttestedBoundary.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AttestedBoundary()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AttestedBoundary',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: AttestedBoundary.$_createMessage)
    ..aOM<ProviderRef>(1, _omitFieldNames ? '' : 'provider',
        subBuilder: ProviderRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'evidence', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AttestedBoundary clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AttestedBoundary copyWith(void Function(AttestedBoundary) updates) =>
      super.copyWith((message) => updates(message as AttestedBoundary))
          as AttestedBoundary;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use AttestedBoundary() / AttestedBoundary.new instead')
  static AttestedBoundary create() => AttestedBoundary._();
  static $pb.GeneratedMessage $_createMessage() => AttestedBoundary._();
  @$core.override
  AttestedBoundary createEmptyInstance() => AttestedBoundary._();
  @$core.pragma('dart2js:noInline')
  static AttestedBoundary getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<AttestedBoundary>(
          AttestedBoundary.$_createMessage);
  static AttestedBoundary? _defaultInstance;

  @$pb.TagNumber(1)
  ProviderRef get provider => $_getN(0);
  @$pb.TagNumber(1)
  set provider(ProviderRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProvider() => $_has(0);
  @$pb.TagNumber(1)
  void clearProvider() => $_clearField(1);
  @$pb.TagNumber(1)
  ProviderRef ensureProvider() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get evidence => $_getN(1);
  @$pb.TagNumber(2)
  set evidence($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasEvidence() => $_has(1);
  @$pb.TagNumber(2)
  void clearEvidence() => $_clearField(2);
}

class SharedGrant extends $pb.GeneratedMessage {
  factory SharedGrant({
    VolumeRef? volume,
    $core.String? childAgentId,
    $core.Iterable<SharedVolumeOperation>? operations,
  }) {
    final result = SharedGrant._();
    if (volume != null) result.volume = volume;
    if (childAgentId != null) result.childAgentId = childAgentId;
    if (operations != null) result.operations.addAll(operations);
    return result;
  }

  SharedGrant._();

  factory SharedGrant.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SharedGrant()..mergeFromBuffer(data, registry);
  factory SharedGrant.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SharedGrant()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SharedGrant',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: SharedGrant.$_createMessage)
    ..aOM<VolumeRef>(1, _omitFieldNames ? '' : 'volume',
        subBuilder: VolumeRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'childAgentId')
    ..pc<SharedVolumeOperation>(
        3, _omitFieldNames ? '' : 'operations', $pb.PbFieldType.KE,
        valueOf: SharedVolumeOperation.valueOf,
        enumValues: SharedVolumeOperation.values,
        defaultEnumValue:
            SharedVolumeOperation.SHARED_VOLUME_OPERATION_UNSPECIFIED)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SharedGrant clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SharedGrant copyWith(void Function(SharedGrant) updates) =>
      super.copyWith((message) => updates(message as SharedGrant))
          as SharedGrant;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SharedGrant() / SharedGrant.new instead')
  static SharedGrant create() => SharedGrant._();
  static $pb.GeneratedMessage $_createMessage() => SharedGrant._();
  @$core.override
  SharedGrant createEmptyInstance() => SharedGrant._();
  @$core.pragma('dart2js:noInline')
  static SharedGrant getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SharedGrant>(
          SharedGrant.$_createMessage);
  static SharedGrant? _defaultInstance;

  @$pb.TagNumber(1)
  VolumeRef get volume => $_getN(0);
  @$pb.TagNumber(1)
  set volume(VolumeRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasVolume() => $_has(0);
  @$pb.TagNumber(1)
  void clearVolume() => $_clearField(1);
  @$pb.TagNumber(1)
  VolumeRef ensureVolume() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get childAgentId => $_getSZ(1);
  @$pb.TagNumber(2)
  set childAgentId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasChildAgentId() => $_has(1);
  @$pb.TagNumber(2)
  void clearChildAgentId() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<SharedVolumeOperation> get operations => $_getList(2);
}

class ReferenceGrant extends $pb.GeneratedMessage {
  factory ReferenceGrant({
    FileRef? file,
    $core.String? readerAgentId,
    FileRef? attachmentManifest,
  }) {
    final result = ReferenceGrant._();
    if (file != null) result.file = file;
    if (readerAgentId != null) result.readerAgentId = readerAgentId;
    if (attachmentManifest != null)
      result.attachmentManifest = attachmentManifest;
    return result;
  }

  ReferenceGrant._();

  factory ReferenceGrant.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReferenceGrant()..mergeFromBuffer(data, registry);
  factory ReferenceGrant.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReferenceGrant()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReferenceGrant',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ReferenceGrant.$_createMessage)
    ..aOM<FileRef>(1, _omitFieldNames ? '' : 'file',
        subBuilder: FileRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'readerAgentId')
    ..aOM<FileRef>(3, _omitFieldNames ? '' : 'attachmentManifest',
        subBuilder: FileRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReferenceGrant clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReferenceGrant copyWith(void Function(ReferenceGrant) updates) =>
      super.copyWith((message) => updates(message as ReferenceGrant))
          as ReferenceGrant;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ReferenceGrant() / ReferenceGrant.new instead')
  static ReferenceGrant create() => ReferenceGrant._();
  static $pb.GeneratedMessage $_createMessage() => ReferenceGrant._();
  @$core.override
  ReferenceGrant createEmptyInstance() => ReferenceGrant._();
  @$core.pragma('dart2js:noInline')
  static ReferenceGrant getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ReferenceGrant>(
          ReferenceGrant.$_createMessage);
  static ReferenceGrant? _defaultInstance;

  @$pb.TagNumber(1)
  FileRef get file => $_getN(0);
  @$pb.TagNumber(1)
  set file(FileRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasFile() => $_has(0);
  @$pb.TagNumber(1)
  void clearFile() => $_clearField(1);
  @$pb.TagNumber(1)
  FileRef ensureFile() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get readerAgentId => $_getSZ(1);
  @$pb.TagNumber(2)
  set readerAgentId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasReaderAgentId() => $_has(1);
  @$pb.TagNumber(2)
  void clearReaderAgentId() => $_clearField(2);

  @$pb.TagNumber(3)
  FileRef get attachmentManifest => $_getN(2);
  @$pb.TagNumber(3)
  set attachmentManifest(FileRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasAttachmentManifest() => $_has(2);
  @$pb.TagNumber(3)
  void clearAttachmentManifest() => $_clearField(3);
  @$pb.TagNumber(3)
  FileRef ensureAttachmentManifest() => $_ensure(2);
}

class ForkSelection extends $pb.GeneratedMessage {
  factory ForkSelection({
    $core.bool? required,
    ResourceRevision? revision,
  }) {
    final result = ForkSelection._();
    if (required != null) result.required = required;
    if (revision != null) result.revision = revision;
    return result;
  }

  ForkSelection._();

  factory ForkSelection.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkSelection()..mergeFromBuffer(data, registry);
  factory ForkSelection.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkSelection()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkSelection',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ForkSelection.$_createMessage)
    ..aOB(1, _omitFieldNames ? '' : 'required')
    ..aOM<ResourceRevision>(2, _omitFieldNames ? '' : 'revision',
        subBuilder: ResourceRevision.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkSelection clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkSelection copyWith(void Function(ForkSelection) updates) =>
      super.copyWith((message) => updates(message as ForkSelection))
          as ForkSelection;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkSelection() / ForkSelection.new instead')
  static ForkSelection create() => ForkSelection._();
  static $pb.GeneratedMessage $_createMessage() => ForkSelection._();
  @$core.override
  ForkSelection createEmptyInstance() => ForkSelection._();
  @$core.pragma('dart2js:noInline')
  static ForkSelection getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkSelection>(
          ForkSelection.$_createMessage);
  static ForkSelection? _defaultInstance;

  @$pb.TagNumber(1)
  $core.bool get required => $_getBF(0);
  @$pb.TagNumber(1)
  set required($core.bool value) => $_setBool(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRequired() => $_has(0);
  @$pb.TagNumber(1)
  void clearRequired() => $_clearField(1);

  @$pb.TagNumber(2)
  ResourceRevision get revision => $_getN(1);
  @$pb.TagNumber(2)
  set revision(ResourceRevision value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasRevision() => $_has(1);
  @$pb.TagNumber(2)
  void clearRevision() => $_clearField(2);
  @$pb.TagNumber(2)
  ResourceRevision ensureRevision() => $_ensure(1);
}

class ForkPreparation extends $pb.GeneratedMessage {
  factory ForkPreparation({
    VolumeRef? childProjectVolume,
    VolumeRef? childPrivateVolume,
    $fixnum.Int64? inheritedThroughSequence,
    $fixnum.Int64? maximumInheritedMessages,
    $fixnum.Int64? maximumInheritedBytes,
    $core.int? maximumInheritedReferences,
  }) {
    final result = ForkPreparation._();
    if (childProjectVolume != null)
      result.childProjectVolume = childProjectVolume;
    if (childPrivateVolume != null)
      result.childPrivateVolume = childPrivateVolume;
    if (inheritedThroughSequence != null)
      result.inheritedThroughSequence = inheritedThroughSequence;
    if (maximumInheritedMessages != null)
      result.maximumInheritedMessages = maximumInheritedMessages;
    if (maximumInheritedBytes != null)
      result.maximumInheritedBytes = maximumInheritedBytes;
    if (maximumInheritedReferences != null)
      result.maximumInheritedReferences = maximumInheritedReferences;
    return result;
  }

  ForkPreparation._();

  factory ForkPreparation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkPreparation()..mergeFromBuffer(data, registry);
  factory ForkPreparation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkPreparation()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkPreparation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ForkPreparation.$_createMessage)
    ..aOM<VolumeRef>(1, _omitFieldNames ? '' : 'childProjectVolume',
        subBuilder: VolumeRef.$_createMessage)
    ..aOM<VolumeRef>(2, _omitFieldNames ? '' : 'childPrivateVolume',
        subBuilder: VolumeRef.$_createMessage)
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'inheritedThroughSequence',
        $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(4, _omitFieldNames ? '' : 'maximumInheritedMessages',
        $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'maximumInheritedBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aI(6, _omitFieldNames ? '' : 'maximumInheritedReferences',
        fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkPreparation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkPreparation copyWith(void Function(ForkPreparation) updates) =>
      super.copyWith((message) => updates(message as ForkPreparation))
          as ForkPreparation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkPreparation() / ForkPreparation.new instead')
  static ForkPreparation create() => ForkPreparation._();
  static $pb.GeneratedMessage $_createMessage() => ForkPreparation._();
  @$core.override
  ForkPreparation createEmptyInstance() => ForkPreparation._();
  @$core.pragma('dart2js:noInline')
  static ForkPreparation getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkPreparation>(
          ForkPreparation.$_createMessage);
  static ForkPreparation? _defaultInstance;

  @$pb.TagNumber(1)
  VolumeRef get childProjectVolume => $_getN(0);
  @$pb.TagNumber(1)
  set childProjectVolume(VolumeRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasChildProjectVolume() => $_has(0);
  @$pb.TagNumber(1)
  void clearChildProjectVolume() => $_clearField(1);
  @$pb.TagNumber(1)
  VolumeRef ensureChildProjectVolume() => $_ensure(0);

  @$pb.TagNumber(2)
  VolumeRef get childPrivateVolume => $_getN(1);
  @$pb.TagNumber(2)
  set childPrivateVolume(VolumeRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasChildPrivateVolume() => $_has(1);
  @$pb.TagNumber(2)
  void clearChildPrivateVolume() => $_clearField(2);
  @$pb.TagNumber(2)
  VolumeRef ensureChildPrivateVolume() => $_ensure(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get inheritedThroughSequence => $_getI64(2);
  @$pb.TagNumber(3)
  set inheritedThroughSequence($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasInheritedThroughSequence() => $_has(2);
  @$pb.TagNumber(3)
  void clearInheritedThroughSequence() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get maximumInheritedMessages => $_getI64(3);
  @$pb.TagNumber(4)
  set maximumInheritedMessages($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumInheritedMessages() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumInheritedMessages() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get maximumInheritedBytes => $_getI64(4);
  @$pb.TagNumber(5)
  set maximumInheritedBytes($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasMaximumInheritedBytes() => $_has(4);
  @$pb.TagNumber(5)
  void clearMaximumInheritedBytes() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.int get maximumInheritedReferences => $_getIZ(5);
  @$pb.TagNumber(6)
  set maximumInheritedReferences($core.int value) =>
      $_setUnsignedInt32(5, value);
  @$pb.TagNumber(6)
  $core.bool hasMaximumInheritedReferences() => $_has(5);
  @$pb.TagNumber(6)
  void clearMaximumInheritedReferences() => $_clearField(6);
}

class ForkRequest extends $pb.GeneratedMessage {
  factory ForkRequest({
    OperationIdentity? operation,
    Authority? parent,
    $fixnum.Int64? parentRevision,
    Authority? child,
    $core.String? childAgentId,
    $core.Iterable<ForkSelection>? selections,
    AttestedBoundary? boundary,
    $core.Iterable<$core.String>? attachedAgentIds,
    ForkPreparation? preparation,
  }) {
    final result = ForkRequest._();
    if (operation != null) result.operation = operation;
    if (parent != null) result.parent = parent;
    if (parentRevision != null) result.parentRevision = parentRevision;
    if (child != null) result.child = child;
    if (childAgentId != null) result.childAgentId = childAgentId;
    if (selections != null) result.selections.addAll(selections);
    if (boundary != null) result.boundary = boundary;
    if (attachedAgentIds != null)
      result.attachedAgentIds.addAll(attachedAgentIds);
    if (preparation != null) result.preparation = preparation;
    return result;
  }

  ForkRequest._();

  factory ForkRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkRequest()..mergeFromBuffer(data, registry);
  factory ForkRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ForkRequest.$_createMessage)
    ..aOM<OperationIdentity>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationIdentity.$_createMessage)
    ..aOM<Authority>(2, _omitFieldNames ? '' : 'parent',
        subBuilder: Authority.$_createMessage)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'parentRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<Authority>(4, _omitFieldNames ? '' : 'child',
        subBuilder: Authority.$_createMessage)
    ..aOS(5, _omitFieldNames ? '' : 'childAgentId')
    ..pPM<ForkSelection>(6, _omitFieldNames ? '' : 'selections',
        subBuilder: ForkSelection.$_createMessage)
    ..aOM<AttestedBoundary>(7, _omitFieldNames ? '' : 'boundary',
        subBuilder: AttestedBoundary.$_createMessage)
    ..pPS(8, _omitFieldNames ? '' : 'attachedAgentIds')
    ..aOM<ForkPreparation>(9, _omitFieldNames ? '' : 'preparation',
        subBuilder: ForkPreparation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkRequest copyWith(void Function(ForkRequest) updates) =>
      super.copyWith((message) => updates(message as ForkRequest))
          as ForkRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkRequest() / ForkRequest.new instead')
  static ForkRequest create() => ForkRequest._();
  static $pb.GeneratedMessage $_createMessage() => ForkRequest._();
  @$core.override
  ForkRequest createEmptyInstance() => ForkRequest._();
  @$core.pragma('dart2js:noInline')
  static ForkRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkRequest>(
          ForkRequest.$_createMessage);
  static ForkRequest? _defaultInstance;

  @$pb.TagNumber(1)
  OperationIdentity get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(OperationIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationIdentity ensureOperation() => $_ensure(0);

  @$pb.TagNumber(2)
  Authority get parent => $_getN(1);
  @$pb.TagNumber(2)
  set parent(Authority value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasParent() => $_has(1);
  @$pb.TagNumber(2)
  void clearParent() => $_clearField(2);
  @$pb.TagNumber(2)
  Authority ensureParent() => $_ensure(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get parentRevision => $_getI64(2);
  @$pb.TagNumber(3)
  set parentRevision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasParentRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearParentRevision() => $_clearField(3);

  @$pb.TagNumber(4)
  Authority get child => $_getN(3);
  @$pb.TagNumber(4)
  set child(Authority value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasChild() => $_has(3);
  @$pb.TagNumber(4)
  void clearChild() => $_clearField(4);
  @$pb.TagNumber(4)
  Authority ensureChild() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.String get childAgentId => $_getSZ(4);
  @$pb.TagNumber(5)
  set childAgentId($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasChildAgentId() => $_has(4);
  @$pb.TagNumber(5)
  void clearChildAgentId() => $_clearField(5);

  @$pb.TagNumber(6)
  $pb.PbList<ForkSelection> get selections => $_getList(5);

  @$pb.TagNumber(7)
  AttestedBoundary get boundary => $_getN(6);
  @$pb.TagNumber(7)
  set boundary(AttestedBoundary value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasBoundary() => $_has(6);
  @$pb.TagNumber(7)
  void clearBoundary() => $_clearField(7);
  @$pb.TagNumber(7)
  AttestedBoundary ensureBoundary() => $_ensure(6);

  @$pb.TagNumber(8)
  $pb.PbList<$core.String> get attachedAgentIds => $_getList(7);

  @$pb.TagNumber(9)
  ForkPreparation get preparation => $_getN(8);
  @$pb.TagNumber(9)
  set preparation(ForkPreparation value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasPreparation() => $_has(8);
  @$pb.TagNumber(9)
  void clearPreparation() => $_clearField(9);
  @$pb.TagNumber(9)
  ForkPreparation ensurePreparation() => $_ensure(8);
}

enum Capture_Outcome {
  captured,
  unsupportedReason,
  inFlightOperationId,
  indeterminateOperationId,
  notSet
}

class Capture extends $pb.GeneratedMessage {
  factory Capture({
    CapturedResource? captured,
    $core.String? unsupportedReason,
    $core.String? inFlightOperationId,
    $core.String? indeterminateOperationId,
  }) {
    final result = Capture._();
    if (captured != null) result.captured = captured;
    if (unsupportedReason != null) result.unsupportedReason = unsupportedReason;
    if (inFlightOperationId != null)
      result.inFlightOperationId = inFlightOperationId;
    if (indeterminateOperationId != null)
      result.indeterminateOperationId = indeterminateOperationId;
    return result;
  }

  Capture._();

  factory Capture.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Capture()..mergeFromBuffer(data, registry);
  factory Capture.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Capture()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, Capture_Outcome> _Capture_OutcomeByTag = {
    1: Capture_Outcome.captured,
    2: Capture_Outcome.unsupportedReason,
    3: Capture_Outcome.inFlightOperationId,
    4: Capture_Outcome.indeterminateOperationId,
    0: Capture_Outcome.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Capture',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: Capture.$_createMessage)
    ..oo(0, [1, 2, 3, 4])
    ..aOM<CapturedResource>(1, _omitFieldNames ? '' : 'captured',
        subBuilder: CapturedResource.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'unsupportedReason')
    ..aOS(3, _omitFieldNames ? '' : 'inFlightOperationId')
    ..aOS(4, _omitFieldNames ? '' : 'indeterminateOperationId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Capture clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Capture copyWith(void Function(Capture) updates) =>
      super.copyWith((message) => updates(message as Capture)) as Capture;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Capture() / Capture.new instead')
  static Capture create() => Capture._();
  static $pb.GeneratedMessage $_createMessage() => Capture._();
  @$core.override
  Capture createEmptyInstance() => Capture._();
  @$core.pragma('dart2js:noInline')
  static Capture getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Capture>(Capture.$_createMessage);
  static Capture? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  Capture_Outcome whichOutcome() => _Capture_OutcomeByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  void clearOutcome() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  CapturedResource get captured => $_getN(0);
  @$pb.TagNumber(1)
  set captured(CapturedResource value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCaptured() => $_has(0);
  @$pb.TagNumber(1)
  void clearCaptured() => $_clearField(1);
  @$pb.TagNumber(1)
  CapturedResource ensureCaptured() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get unsupportedReason => $_getSZ(1);
  @$pb.TagNumber(2)
  set unsupportedReason($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUnsupportedReason() => $_has(1);
  @$pb.TagNumber(2)
  void clearUnsupportedReason() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get inFlightOperationId => $_getSZ(2);
  @$pb.TagNumber(3)
  set inFlightOperationId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasInFlightOperationId() => $_has(2);
  @$pb.TagNumber(3)
  void clearInFlightOperationId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get indeterminateOperationId => $_getSZ(3);
  @$pb.TagNumber(4)
  set indeterminateOperationId($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIndeterminateOperationId() => $_has(3);
  @$pb.TagNumber(4)
  void clearIndeterminateOperationId() => $_clearField(4);
}

class ForkReport extends $pb.GeneratedMessage {
  factory ForkReport({
    ForkRequest? request,
    $core.Iterable<Capture>? captures,
    VolumeRef? childPrivateVolume,
    $core.Iterable<FileRef>? inheritedContext,
    $core.Iterable<SharedGrant>? sharedGrants,
    $core.Iterable<ReferenceGrant>? referenceGrants,
    $core.Iterable<FileRef>? attachmentManifests,
    $fixnum.Int64? inheritedThroughSequence,
    GenerationRef? childPrivateGeneration,
  }) {
    final result = ForkReport._();
    if (request != null) result.request = request;
    if (captures != null) result.captures.addAll(captures);
    if (childPrivateVolume != null)
      result.childPrivateVolume = childPrivateVolume;
    if (inheritedContext != null)
      result.inheritedContext.addAll(inheritedContext);
    if (sharedGrants != null) result.sharedGrants.addAll(sharedGrants);
    if (referenceGrants != null) result.referenceGrants.addAll(referenceGrants);
    if (attachmentManifests != null)
      result.attachmentManifests.addAll(attachmentManifests);
    if (inheritedThroughSequence != null)
      result.inheritedThroughSequence = inheritedThroughSequence;
    if (childPrivateGeneration != null)
      result.childPrivateGeneration = childPrivateGeneration;
    return result;
  }

  ForkReport._();

  factory ForkReport.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkReport()..mergeFromBuffer(data, registry);
  factory ForkReport.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkReport()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkReport',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ForkReport.$_createMessage)
    ..aOM<ForkRequest>(1, _omitFieldNames ? '' : 'request',
        subBuilder: ForkRequest.$_createMessage)
    ..pPM<Capture>(2, _omitFieldNames ? '' : 'captures',
        subBuilder: Capture.$_createMessage)
    ..aOM<VolumeRef>(3, _omitFieldNames ? '' : 'childPrivateVolume',
        subBuilder: VolumeRef.$_createMessage)
    ..pPM<FileRef>(4, _omitFieldNames ? '' : 'inheritedContext',
        subBuilder: FileRef.$_createMessage)
    ..pPM<SharedGrant>(5, _omitFieldNames ? '' : 'sharedGrants',
        subBuilder: SharedGrant.$_createMessage)
    ..pPM<ReferenceGrant>(6, _omitFieldNames ? '' : 'referenceGrants',
        subBuilder: ReferenceGrant.$_createMessage)
    ..pPM<FileRef>(7, _omitFieldNames ? '' : 'attachmentManifests',
        subBuilder: FileRef.$_createMessage)
    ..a<$fixnum.Int64>(8, _omitFieldNames ? '' : 'inheritedThroughSequence',
        $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<GenerationRef>(9, _omitFieldNames ? '' : 'childPrivateGeneration',
        subBuilder: GenerationRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkReport clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkReport copyWith(void Function(ForkReport) updates) =>
      super.copyWith((message) => updates(message as ForkReport)) as ForkReport;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkReport() / ForkReport.new instead')
  static ForkReport create() => ForkReport._();
  static $pb.GeneratedMessage $_createMessage() => ForkReport._();
  @$core.override
  ForkReport createEmptyInstance() => ForkReport._();
  @$core.pragma('dart2js:noInline')
  static ForkReport getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ForkReport>(ForkReport.$_createMessage);
  static ForkReport? _defaultInstance;

  @$pb.TagNumber(1)
  ForkRequest get request => $_getN(0);
  @$pb.TagNumber(1)
  set request(ForkRequest value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasRequest() => $_has(0);
  @$pb.TagNumber(1)
  void clearRequest() => $_clearField(1);
  @$pb.TagNumber(1)
  ForkRequest ensureRequest() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<Capture> get captures => $_getList(1);

  @$pb.TagNumber(3)
  VolumeRef get childPrivateVolume => $_getN(2);
  @$pb.TagNumber(3)
  set childPrivateVolume(VolumeRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasChildPrivateVolume() => $_has(2);
  @$pb.TagNumber(3)
  void clearChildPrivateVolume() => $_clearField(3);
  @$pb.TagNumber(3)
  VolumeRef ensureChildPrivateVolume() => $_ensure(2);

  @$pb.TagNumber(4)
  $pb.PbList<FileRef> get inheritedContext => $_getList(3);

  @$pb.TagNumber(5)
  $pb.PbList<SharedGrant> get sharedGrants => $_getList(4);

  @$pb.TagNumber(6)
  $pb.PbList<ReferenceGrant> get referenceGrants => $_getList(5);

  @$pb.TagNumber(7)
  $pb.PbList<FileRef> get attachmentManifests => $_getList(6);

  @$pb.TagNumber(8)
  $fixnum.Int64 get inheritedThroughSequence => $_getI64(7);
  @$pb.TagNumber(8)
  set inheritedThroughSequence($fixnum.Int64 value) => $_setInt64(7, value);
  @$pb.TagNumber(8)
  $core.bool hasInheritedThroughSequence() => $_has(7);
  @$pb.TagNumber(8)
  void clearInheritedThroughSequence() => $_clearField(8);

  @$pb.TagNumber(9)
  GenerationRef get childPrivateGeneration => $_getN(8);
  @$pb.TagNumber(9)
  set childPrivateGeneration(GenerationRef value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasChildPrivateGeneration() => $_has(8);
  @$pb.TagNumber(9)
  void clearChildPrivateGeneration() => $_clearField(9);
  @$pb.TagNumber(9)
  GenerationRef ensureChildPrivateGeneration() => $_ensure(8);
}

class ForkSeed extends $pb.GeneratedMessage {
  factory ForkSeed({
    OperationIdentity? operation,
    Authority? parent,
    $fixnum.Int64? parentRevision,
    Authority? child,
    $core.String? childAgentId,
    $core.Iterable<CapturedResource>? resources,
    $core.Iterable<ForkOmission>? omissions,
    VolumeRef? childPrivateVolume,
    $core.Iterable<FileRef>? inheritedContext,
    AttestedBoundary? boundary,
    $core.Iterable<SharedGrant>? sharedGrants,
    $core.Iterable<$core.String>? attachedAgentIds,
    $core.Iterable<ReferenceGrant>? referenceGrants,
    $core.Iterable<FileRef>? attachmentManifests,
    $fixnum.Int64? inheritedThroughSequence,
    GenerationRef? childPrivateGeneration,
  }) {
    final result = ForkSeed._();
    if (operation != null) result.operation = operation;
    if (parent != null) result.parent = parent;
    if (parentRevision != null) result.parentRevision = parentRevision;
    if (child != null) result.child = child;
    if (childAgentId != null) result.childAgentId = childAgentId;
    if (resources != null) result.resources.addAll(resources);
    if (omissions != null) result.omissions.addAll(omissions);
    if (childPrivateVolume != null)
      result.childPrivateVolume = childPrivateVolume;
    if (inheritedContext != null)
      result.inheritedContext.addAll(inheritedContext);
    if (boundary != null) result.boundary = boundary;
    if (sharedGrants != null) result.sharedGrants.addAll(sharedGrants);
    if (attachedAgentIds != null)
      result.attachedAgentIds.addAll(attachedAgentIds);
    if (referenceGrants != null) result.referenceGrants.addAll(referenceGrants);
    if (attachmentManifests != null)
      result.attachmentManifests.addAll(attachmentManifests);
    if (inheritedThroughSequence != null)
      result.inheritedThroughSequence = inheritedThroughSequence;
    if (childPrivateGeneration != null)
      result.childPrivateGeneration = childPrivateGeneration;
    return result;
  }

  ForkSeed._();

  factory ForkSeed.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkSeed()..mergeFromBuffer(data, registry);
  factory ForkSeed.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkSeed()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkSeed',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ForkSeed.$_createMessage)
    ..aOM<OperationIdentity>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationIdentity.$_createMessage)
    ..aOM<Authority>(2, _omitFieldNames ? '' : 'parent',
        subBuilder: Authority.$_createMessage)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'parentRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<Authority>(4, _omitFieldNames ? '' : 'child',
        subBuilder: Authority.$_createMessage)
    ..aOS(5, _omitFieldNames ? '' : 'childAgentId')
    ..pPM<CapturedResource>(6, _omitFieldNames ? '' : 'resources',
        subBuilder: CapturedResource.$_createMessage)
    ..pPM<ForkOmission>(7, _omitFieldNames ? '' : 'omissions',
        subBuilder: ForkOmission.$_createMessage)
    ..aOM<VolumeRef>(8, _omitFieldNames ? '' : 'childPrivateVolume',
        subBuilder: VolumeRef.$_createMessage)
    ..pPM<FileRef>(9, _omitFieldNames ? '' : 'inheritedContext',
        subBuilder: FileRef.$_createMessage)
    ..aOM<AttestedBoundary>(10, _omitFieldNames ? '' : 'boundary',
        subBuilder: AttestedBoundary.$_createMessage)
    ..pPM<SharedGrant>(11, _omitFieldNames ? '' : 'sharedGrants',
        subBuilder: SharedGrant.$_createMessage)
    ..pPS(12, _omitFieldNames ? '' : 'attachedAgentIds')
    ..pPM<ReferenceGrant>(13, _omitFieldNames ? '' : 'referenceGrants',
        subBuilder: ReferenceGrant.$_createMessage)
    ..pPM<FileRef>(14, _omitFieldNames ? '' : 'attachmentManifests',
        subBuilder: FileRef.$_createMessage)
    ..a<$fixnum.Int64>(15, _omitFieldNames ? '' : 'inheritedThroughSequence',
        $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<GenerationRef>(16, _omitFieldNames ? '' : 'childPrivateGeneration',
        subBuilder: GenerationRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkSeed clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkSeed copyWith(void Function(ForkSeed) updates) =>
      super.copyWith((message) => updates(message as ForkSeed)) as ForkSeed;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkSeed() / ForkSeed.new instead')
  static ForkSeed create() => ForkSeed._();
  static $pb.GeneratedMessage $_createMessage() => ForkSeed._();
  @$core.override
  ForkSeed createEmptyInstance() => ForkSeed._();
  @$core.pragma('dart2js:noInline')
  static ForkSeed getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ForkSeed>(ForkSeed.$_createMessage);
  static ForkSeed? _defaultInstance;

  @$pb.TagNumber(1)
  OperationIdentity get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(OperationIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationIdentity ensureOperation() => $_ensure(0);

  @$pb.TagNumber(2)
  Authority get parent => $_getN(1);
  @$pb.TagNumber(2)
  set parent(Authority value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasParent() => $_has(1);
  @$pb.TagNumber(2)
  void clearParent() => $_clearField(2);
  @$pb.TagNumber(2)
  Authority ensureParent() => $_ensure(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get parentRevision => $_getI64(2);
  @$pb.TagNumber(3)
  set parentRevision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasParentRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearParentRevision() => $_clearField(3);

  @$pb.TagNumber(4)
  Authority get child => $_getN(3);
  @$pb.TagNumber(4)
  set child(Authority value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasChild() => $_has(3);
  @$pb.TagNumber(4)
  void clearChild() => $_clearField(4);
  @$pb.TagNumber(4)
  Authority ensureChild() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.String get childAgentId => $_getSZ(4);
  @$pb.TagNumber(5)
  set childAgentId($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasChildAgentId() => $_has(4);
  @$pb.TagNumber(5)
  void clearChildAgentId() => $_clearField(5);

  @$pb.TagNumber(6)
  $pb.PbList<CapturedResource> get resources => $_getList(5);

  @$pb.TagNumber(7)
  $pb.PbList<ForkOmission> get omissions => $_getList(6);

  @$pb.TagNumber(8)
  VolumeRef get childPrivateVolume => $_getN(7);
  @$pb.TagNumber(8)
  set childPrivateVolume(VolumeRef value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasChildPrivateVolume() => $_has(7);
  @$pb.TagNumber(8)
  void clearChildPrivateVolume() => $_clearField(8);
  @$pb.TagNumber(8)
  VolumeRef ensureChildPrivateVolume() => $_ensure(7);

  @$pb.TagNumber(9)
  $pb.PbList<FileRef> get inheritedContext => $_getList(8);

  @$pb.TagNumber(10)
  AttestedBoundary get boundary => $_getN(9);
  @$pb.TagNumber(10)
  set boundary(AttestedBoundary value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasBoundary() => $_has(9);
  @$pb.TagNumber(10)
  void clearBoundary() => $_clearField(10);
  @$pb.TagNumber(10)
  AttestedBoundary ensureBoundary() => $_ensure(9);

  @$pb.TagNumber(11)
  $pb.PbList<SharedGrant> get sharedGrants => $_getList(10);

  @$pb.TagNumber(12)
  $pb.PbList<$core.String> get attachedAgentIds => $_getList(11);

  @$pb.TagNumber(13)
  $pb.PbList<ReferenceGrant> get referenceGrants => $_getList(12);

  @$pb.TagNumber(14)
  $pb.PbList<FileRef> get attachmentManifests => $_getList(13);

  @$pb.TagNumber(15)
  $fixnum.Int64 get inheritedThroughSequence => $_getI64(14);
  @$pb.TagNumber(15)
  set inheritedThroughSequence($fixnum.Int64 value) => $_setInt64(14, value);
  @$pb.TagNumber(15)
  $core.bool hasInheritedThroughSequence() => $_has(14);
  @$pb.TagNumber(15)
  void clearInheritedThroughSequence() => $_clearField(15);

  @$pb.TagNumber(16)
  GenerationRef get childPrivateGeneration => $_getN(15);
  @$pb.TagNumber(16)
  set childPrivateGeneration(GenerationRef value) => $_setField(16, value);
  @$pb.TagNumber(16)
  $core.bool hasChildPrivateGeneration() => $_has(15);
  @$pb.TagNumber(16)
  void clearChildPrivateGeneration() => $_clearField(16);
  @$pb.TagNumber(16)
  GenerationRef ensureChildPrivateGeneration() => $_ensure(15);
}

/// One parent-authorized Filesystem join. The notice is admitted atomically
/// with the receipt; no private volume, model context, or child history merges.
class ProviderJoinProof extends $pb.GeneratedMessage {
  factory ProviderJoinProof({
    ProviderRef? provider,
    $core.String? format,
    $core.List<$core.int>? canonicalJsonStatement,
  }) {
    final result = ProviderJoinProof._();
    if (provider != null) result.provider = provider;
    if (format != null) result.format = format;
    if (canonicalJsonStatement != null)
      result.canonicalJsonStatement = canonicalJsonStatement;
    return result;
  }

  ProviderJoinProof._();

  factory ProviderJoinProof.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProviderJoinProof()..mergeFromBuffer(data, registry);
  factory ProviderJoinProof.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProviderJoinProof()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProviderJoinProof',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ProviderJoinProof.$_createMessage)
    ..aOM<ProviderRef>(1, _omitFieldNames ? '' : 'provider',
        subBuilder: ProviderRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'format')
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'canonicalJsonStatement', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProviderJoinProof clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProviderJoinProof copyWith(void Function(ProviderJoinProof) updates) =>
      super.copyWith((message) => updates(message as ProviderJoinProof))
          as ProviderJoinProof;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ProviderJoinProof() / ProviderJoinProof.new instead')
  static ProviderJoinProof create() => ProviderJoinProof._();
  static $pb.GeneratedMessage $_createMessage() => ProviderJoinProof._();
  @$core.override
  ProviderJoinProof createEmptyInstance() => ProviderJoinProof._();
  @$core.pragma('dart2js:noInline')
  static ProviderJoinProof getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ProviderJoinProof>(
          ProviderJoinProof.$_createMessage);
  static ProviderJoinProof? _defaultInstance;

  @$pb.TagNumber(1)
  ProviderRef get provider => $_getN(0);
  @$pb.TagNumber(1)
  set provider(ProviderRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProvider() => $_has(0);
  @$pb.TagNumber(1)
  void clearProvider() => $_clearField(1);
  @$pb.TagNumber(1)
  ProviderRef ensureProvider() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get format => $_getSZ(1);
  @$pb.TagNumber(2)
  set format($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasFormat() => $_has(1);
  @$pb.TagNumber(2)
  void clearFormat() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get canonicalJsonStatement => $_getN(2);
  @$pb.TagNumber(3)
  set canonicalJsonStatement($core.List<$core.int> value) =>
      $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasCanonicalJsonStatement() => $_has(2);
  @$pb.TagNumber(3)
  void clearCanonicalJsonStatement() => $_clearField(3);
}

class ProjectMergeReceipt extends $pb.GeneratedMessage {
  factory ProjectMergeReceipt({
    OperationIdentity? operation,
    Authority? child,
    VolumeRef? sourceProject,
    GenerationRef? sourceGeneration,
    VolumeRef? targetProject,
    GenerationRef? expectedTargetGeneration,
    GenerationRef? resultGeneration,
    $core.List<$core.int>? providerOperationId,
    ProviderJoinProof? providerProof,
    ConversationMessage? notice,
  }) {
    final result = ProjectMergeReceipt._();
    if (operation != null) result.operation = operation;
    if (child != null) result.child = child;
    if (sourceProject != null) result.sourceProject = sourceProject;
    if (sourceGeneration != null) result.sourceGeneration = sourceGeneration;
    if (targetProject != null) result.targetProject = targetProject;
    if (expectedTargetGeneration != null)
      result.expectedTargetGeneration = expectedTargetGeneration;
    if (resultGeneration != null) result.resultGeneration = resultGeneration;
    if (providerOperationId != null)
      result.providerOperationId = providerOperationId;
    if (providerProof != null) result.providerProof = providerProof;
    if (notice != null) result.notice = notice;
    return result;
  }

  ProjectMergeReceipt._();

  factory ProjectMergeReceipt.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProjectMergeReceipt()..mergeFromBuffer(data, registry);
  factory ProjectMergeReceipt.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProjectMergeReceipt()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProjectMergeReceipt',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.harness.v2'),
      createEmptyInstance: ProjectMergeReceipt.$_createMessage)
    ..aOM<OperationIdentity>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationIdentity.$_createMessage)
    ..aOM<Authority>(2, _omitFieldNames ? '' : 'child',
        subBuilder: Authority.$_createMessage)
    ..aOM<VolumeRef>(3, _omitFieldNames ? '' : 'sourceProject',
        subBuilder: VolumeRef.$_createMessage)
    ..aOM<GenerationRef>(4, _omitFieldNames ? '' : 'sourceGeneration',
        subBuilder: GenerationRef.$_createMessage)
    ..aOM<VolumeRef>(5, _omitFieldNames ? '' : 'targetProject',
        subBuilder: VolumeRef.$_createMessage)
    ..aOM<GenerationRef>(6, _omitFieldNames ? '' : 'expectedTargetGeneration',
        subBuilder: GenerationRef.$_createMessage)
    ..aOM<GenerationRef>(7, _omitFieldNames ? '' : 'resultGeneration',
        subBuilder: GenerationRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        8, _omitFieldNames ? '' : 'providerOperationId', $pb.PbFieldType.OY)
    ..aOM<ProviderJoinProof>(9, _omitFieldNames ? '' : 'providerProof',
        subBuilder: ProviderJoinProof.$_createMessage)
    ..aOM<ConversationMessage>(10, _omitFieldNames ? '' : 'notice',
        subBuilder: ConversationMessage.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProjectMergeReceipt clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProjectMergeReceipt copyWith(void Function(ProjectMergeReceipt) updates) =>
      super.copyWith((message) => updates(message as ProjectMergeReceipt))
          as ProjectMergeReceipt;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use ProjectMergeReceipt() / ProjectMergeReceipt.new instead')
  static ProjectMergeReceipt create() => ProjectMergeReceipt._();
  static $pb.GeneratedMessage $_createMessage() => ProjectMergeReceipt._();
  @$core.override
  ProjectMergeReceipt createEmptyInstance() => ProjectMergeReceipt._();
  @$core.pragma('dart2js:noInline')
  static ProjectMergeReceipt getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ProjectMergeReceipt>(
          ProjectMergeReceipt.$_createMessage);
  static ProjectMergeReceipt? _defaultInstance;

  @$pb.TagNumber(1)
  OperationIdentity get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(OperationIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationIdentity ensureOperation() => $_ensure(0);

  @$pb.TagNumber(2)
  Authority get child => $_getN(1);
  @$pb.TagNumber(2)
  set child(Authority value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasChild() => $_has(1);
  @$pb.TagNumber(2)
  void clearChild() => $_clearField(2);
  @$pb.TagNumber(2)
  Authority ensureChild() => $_ensure(1);

  @$pb.TagNumber(3)
  VolumeRef get sourceProject => $_getN(2);
  @$pb.TagNumber(3)
  set sourceProject(VolumeRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasSourceProject() => $_has(2);
  @$pb.TagNumber(3)
  void clearSourceProject() => $_clearField(3);
  @$pb.TagNumber(3)
  VolumeRef ensureSourceProject() => $_ensure(2);

  @$pb.TagNumber(4)
  GenerationRef get sourceGeneration => $_getN(3);
  @$pb.TagNumber(4)
  set sourceGeneration(GenerationRef value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasSourceGeneration() => $_has(3);
  @$pb.TagNumber(4)
  void clearSourceGeneration() => $_clearField(4);
  @$pb.TagNumber(4)
  GenerationRef ensureSourceGeneration() => $_ensure(3);

  @$pb.TagNumber(5)
  VolumeRef get targetProject => $_getN(4);
  @$pb.TagNumber(5)
  set targetProject(VolumeRef value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasTargetProject() => $_has(4);
  @$pb.TagNumber(5)
  void clearTargetProject() => $_clearField(5);
  @$pb.TagNumber(5)
  VolumeRef ensureTargetProject() => $_ensure(4);

  @$pb.TagNumber(6)
  GenerationRef get expectedTargetGeneration => $_getN(5);
  @$pb.TagNumber(6)
  set expectedTargetGeneration(GenerationRef value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasExpectedTargetGeneration() => $_has(5);
  @$pb.TagNumber(6)
  void clearExpectedTargetGeneration() => $_clearField(6);
  @$pb.TagNumber(6)
  GenerationRef ensureExpectedTargetGeneration() => $_ensure(5);

  @$pb.TagNumber(7)
  GenerationRef get resultGeneration => $_getN(6);
  @$pb.TagNumber(7)
  set resultGeneration(GenerationRef value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasResultGeneration() => $_has(6);
  @$pb.TagNumber(7)
  void clearResultGeneration() => $_clearField(7);
  @$pb.TagNumber(7)
  GenerationRef ensureResultGeneration() => $_ensure(6);

  @$pb.TagNumber(8)
  $core.List<$core.int> get providerOperationId => $_getN(7);
  @$pb.TagNumber(8)
  set providerOperationId($core.List<$core.int> value) => $_setBytes(7, value);
  @$pb.TagNumber(8)
  $core.bool hasProviderOperationId() => $_has(7);
  @$pb.TagNumber(8)
  void clearProviderOperationId() => $_clearField(8);

  @$pb.TagNumber(9)
  ProviderJoinProof get providerProof => $_getN(8);
  @$pb.TagNumber(9)
  set providerProof(ProviderJoinProof value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasProviderProof() => $_has(8);
  @$pb.TagNumber(9)
  void clearProviderProof() => $_clearField(9);
  @$pb.TagNumber(9)
  ProviderJoinProof ensureProviderProof() => $_ensure(8);

  @$pb.TagNumber(10)
  ConversationMessage get notice => $_getN(9);
  @$pb.TagNumber(10)
  set notice(ConversationMessage value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasNotice() => $_has(9);
  @$pb.TagNumber(10)
  void clearNotice() => $_clearField(10);
  @$pb.TagNumber(10)
  ConversationMessage ensureNotice() => $_ensure(9);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
