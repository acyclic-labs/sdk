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

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'actors.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'actors.pbenum.dart';

class Binding extends $pb.GeneratedMessage {
  factory Binding({
    $core.String? name,
    $core.String? capability,
    $core.String? resource,
  }) {
    final result = Binding._();
    if (name != null) result.name = name;
    if (capability != null) result.capability = capability;
    if (resource != null) result.resource = resource;
    return result;
  }

  Binding._();

  factory Binding.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Binding()..mergeFromBuffer(data, registry);
  factory Binding.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Binding()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Binding',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: Binding.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aOS(2, _omitFieldNames ? '' : 'capability')
    ..aOS(3, _omitFieldNames ? '' : 'resource')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Binding clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Binding copyWith(void Function(Binding) updates) =>
      super.copyWith((message) => updates(message as Binding)) as Binding;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Binding() / Binding.new instead')
  static Binding create() => Binding._();
  static $pb.GeneratedMessage $_createMessage() => Binding._();
  @$core.override
  Binding createEmptyInstance() => Binding._();
  @$core.pragma('dart2js:noInline')
  static Binding getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Binding>(Binding.$_createMessage);
  static Binding? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get capability => $_getSZ(1);
  @$pb.TagNumber(2)
  set capability($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCapability() => $_has(1);
  @$pb.TagNumber(2)
  void clearCapability() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get resource => $_getSZ(2);
  @$pb.TagNumber(3)
  set resource($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasResource() => $_has(2);
  @$pb.TagNumber(3)
  void clearResource() => $_clearField(3);
}

class ActorLimits extends $pb.GeneratedMessage {
  factory ActorLimits({
    $fixnum.Int64? handlerTimeoutMillis,
    $fixnum.Int64? memoryBytes,
    $fixnum.Int64? checkpointBytes,
  }) {
    final result = ActorLimits._();
    if (handlerTimeoutMillis != null)
      result.handlerTimeoutMillis = handlerTimeoutMillis;
    if (memoryBytes != null) result.memoryBytes = memoryBytes;
    if (checkpointBytes != null) result.checkpointBytes = checkpointBytes;
    return result;
  }

  ActorLimits._();

  factory ActorLimits.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ActorLimits()..mergeFromBuffer(data, registry);
  factory ActorLimits.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ActorLimits()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ActorLimits',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: ActorLimits.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'handlerTimeoutMillis', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'memoryBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'checkpointBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ActorLimits clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ActorLimits copyWith(void Function(ActorLimits) updates) =>
      super.copyWith((message) => updates(message as ActorLimits))
          as ActorLimits;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ActorLimits() / ActorLimits.new instead')
  static ActorLimits create() => ActorLimits._();
  static $pb.GeneratedMessage $_createMessage() => ActorLimits._();
  @$core.override
  ActorLimits createEmptyInstance() => ActorLimits._();
  @$core.pragma('dart2js:noInline')
  static ActorLimits getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ActorLimits>(
          ActorLimits.$_createMessage);
  static ActorLimits? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get handlerTimeoutMillis => $_getI64(0);
  @$pb.TagNumber(1)
  set handlerTimeoutMillis($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasHandlerTimeoutMillis() => $_has(0);
  @$pb.TagNumber(1)
  void clearHandlerTimeoutMillis() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get memoryBytes => $_getI64(1);
  @$pb.TagNumber(2)
  set memoryBytes($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMemoryBytes() => $_has(1);
  @$pb.TagNumber(2)
  void clearMemoryBytes() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get checkpointBytes => $_getI64(2);
  @$pb.TagNumber(3)
  set checkpointBytes($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasCheckpointBytes() => $_has(2);
  @$pb.TagNumber(3)
  void clearCheckpointBytes() => $_clearField(3);
}

enum SubscriptionStart_Start { cursor, currentHead, notSet }

class SubscriptionStart extends $pb.GeneratedMessage {
  factory SubscriptionStart({
    $fixnum.Int64? cursor,
    $core.bool? currentHead,
  }) {
    final result = SubscriptionStart._();
    if (cursor != null) result.cursor = cursor;
    if (currentHead != null) result.currentHead = currentHead;
    return result;
  }

  SubscriptionStart._();

  factory SubscriptionStart.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubscriptionStart()..mergeFromBuffer(data, registry);
  factory SubscriptionStart.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubscriptionStart()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, SubscriptionStart_Start>
      _SubscriptionStart_StartByTag = {
    1: SubscriptionStart_Start.cursor,
    2: SubscriptionStart_Start.currentHead,
    0: SubscriptionStart_Start.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SubscriptionStart',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: SubscriptionStart.$_createMessage)
    ..oo(0, [1, 2])
    ..a<$fixnum.Int64>(1, _omitFieldNames ? '' : 'cursor', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOB(2, _omitFieldNames ? '' : 'currentHead')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubscriptionStart clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubscriptionStart copyWith(void Function(SubscriptionStart) updates) =>
      super.copyWith((message) => updates(message as SubscriptionStart))
          as SubscriptionStart;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SubscriptionStart() / SubscriptionStart.new instead')
  static SubscriptionStart create() => SubscriptionStart._();
  static $pb.GeneratedMessage $_createMessage() => SubscriptionStart._();
  @$core.override
  SubscriptionStart createEmptyInstance() => SubscriptionStart._();
  @$core.pragma('dart2js:noInline')
  static SubscriptionStart getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SubscriptionStart>(
          SubscriptionStart.$_createMessage);
  static SubscriptionStart? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  SubscriptionStart_Start whichStart() =>
      _SubscriptionStart_StartByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearStart() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $fixnum.Int64 get cursor => $_getI64(0);
  @$pb.TagNumber(1)
  set cursor($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCursor() => $_has(0);
  @$pb.TagNumber(1)
  void clearCursor() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.bool get currentHead => $_getBF(1);
  @$pb.TagNumber(2)
  set currentHead($core.bool value) => $_setBool(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCurrentHead() => $_has(1);
  @$pb.TagNumber(2)
  void clearCurrentHead() => $_clearField(2);
}

class SubscriptionSpec extends $pb.GeneratedMessage {
  factory SubscriptionSpec({
    $core.String? subscriptionId,
    $core.String? streamPath,
    SubscriptionStart? start,
    $core.bool? placementAnchor,
  }) {
    final result = SubscriptionSpec._();
    if (subscriptionId != null) result.subscriptionId = subscriptionId;
    if (streamPath != null) result.streamPath = streamPath;
    if (start != null) result.start = start;
    if (placementAnchor != null) result.placementAnchor = placementAnchor;
    return result;
  }

  SubscriptionSpec._();

  factory SubscriptionSpec.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubscriptionSpec()..mergeFromBuffer(data, registry);
  factory SubscriptionSpec.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubscriptionSpec()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SubscriptionSpec',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: SubscriptionSpec.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'subscriptionId')
    ..aOS(2, _omitFieldNames ? '' : 'streamPath')
    ..aOM<SubscriptionStart>(3, _omitFieldNames ? '' : 'start',
        subBuilder: SubscriptionStart.$_createMessage)
    ..aOB(4, _omitFieldNames ? '' : 'placementAnchor')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubscriptionSpec clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubscriptionSpec copyWith(void Function(SubscriptionSpec) updates) =>
      super.copyWith((message) => updates(message as SubscriptionSpec))
          as SubscriptionSpec;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SubscriptionSpec() / SubscriptionSpec.new instead')
  static SubscriptionSpec create() => SubscriptionSpec._();
  static $pb.GeneratedMessage $_createMessage() => SubscriptionSpec._();
  @$core.override
  SubscriptionSpec createEmptyInstance() => SubscriptionSpec._();
  @$core.pragma('dart2js:noInline')
  static SubscriptionSpec getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SubscriptionSpec>(
          SubscriptionSpec.$_createMessage);
  static SubscriptionSpec? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get subscriptionId => $_getSZ(0);
  @$pb.TagNumber(1)
  set subscriptionId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSubscriptionId() => $_has(0);
  @$pb.TagNumber(1)
  void clearSubscriptionId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get streamPath => $_getSZ(1);
  @$pb.TagNumber(2)
  set streamPath($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasStreamPath() => $_has(1);
  @$pb.TagNumber(2)
  void clearStreamPath() => $_clearField(2);

  @$pb.TagNumber(3)
  SubscriptionStart get start => $_getN(2);
  @$pb.TagNumber(3)
  set start(SubscriptionStart value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasStart() => $_has(2);
  @$pb.TagNumber(3)
  void clearStart() => $_clearField(3);
  @$pb.TagNumber(3)
  SubscriptionStart ensureStart() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.bool get placementAnchor => $_getBF(3);
  @$pb.TagNumber(4)
  set placementAnchor($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasPlacementAnchor() => $_has(3);
  @$pb.TagNumber(4)
  void clearPlacementAnchor() => $_clearField(4);
}

class SubscriptionObservation extends $pb.GeneratedMessage {
  factory SubscriptionObservation({
    $core.String? subscriptionId,
    $core.String? streamPath,
    SubscriptionState? state,
    $fixnum.Int64? deliveredCursor,
    $fixnum.Int64? completedCursor,
    $fixnum.Int64? recoverableCursor,
    $core.bool? placementAnchor,
    $core.int? retryCount,
    $core.String? failureCode,
    $fixnum.Int64? failedCursor,
  }) {
    final result = SubscriptionObservation._();
    if (subscriptionId != null) result.subscriptionId = subscriptionId;
    if (streamPath != null) result.streamPath = streamPath;
    if (state != null) result.state = state;
    if (deliveredCursor != null) result.deliveredCursor = deliveredCursor;
    if (completedCursor != null) result.completedCursor = completedCursor;
    if (recoverableCursor != null) result.recoverableCursor = recoverableCursor;
    if (placementAnchor != null) result.placementAnchor = placementAnchor;
    if (retryCount != null) result.retryCount = retryCount;
    if (failureCode != null) result.failureCode = failureCode;
    if (failedCursor != null) result.failedCursor = failedCursor;
    return result;
  }

  SubscriptionObservation._();

  factory SubscriptionObservation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubscriptionObservation()..mergeFromBuffer(data, registry);
  factory SubscriptionObservation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubscriptionObservation()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SubscriptionObservation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: SubscriptionObservation.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'subscriptionId')
    ..aOS(2, _omitFieldNames ? '' : 'streamPath')
    ..aE<SubscriptionState>(3, _omitFieldNames ? '' : 'state',
        enumValues: SubscriptionState.values)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'deliveredCursor', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'completedCursor', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        6, _omitFieldNames ? '' : 'recoverableCursor', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOB(7, _omitFieldNames ? '' : 'placementAnchor')
    ..aI(8, _omitFieldNames ? '' : 'retryCount', fieldType: $pb.PbFieldType.OU3)
    ..aOS(9, _omitFieldNames ? '' : 'failureCode')
    ..a<$fixnum.Int64>(
        10, _omitFieldNames ? '' : 'failedCursor', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubscriptionObservation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubscriptionObservation copyWith(
          void Function(SubscriptionObservation) updates) =>
      super.copyWith((message) => updates(message as SubscriptionObservation))
          as SubscriptionObservation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use SubscriptionObservation() / SubscriptionObservation.new instead')
  static SubscriptionObservation create() => SubscriptionObservation._();
  static $pb.GeneratedMessage $_createMessage() => SubscriptionObservation._();
  @$core.override
  SubscriptionObservation createEmptyInstance() => SubscriptionObservation._();
  @$core.pragma('dart2js:noInline')
  static SubscriptionObservation getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SubscriptionObservation>(
          SubscriptionObservation.$_createMessage);
  static SubscriptionObservation? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get subscriptionId => $_getSZ(0);
  @$pb.TagNumber(1)
  set subscriptionId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSubscriptionId() => $_has(0);
  @$pb.TagNumber(1)
  void clearSubscriptionId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get streamPath => $_getSZ(1);
  @$pb.TagNumber(2)
  set streamPath($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasStreamPath() => $_has(1);
  @$pb.TagNumber(2)
  void clearStreamPath() => $_clearField(2);

  @$pb.TagNumber(3)
  SubscriptionState get state => $_getN(2);
  @$pb.TagNumber(3)
  set state(SubscriptionState value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasState() => $_has(2);
  @$pb.TagNumber(3)
  void clearState() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get deliveredCursor => $_getI64(3);
  @$pb.TagNumber(4)
  set deliveredCursor($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasDeliveredCursor() => $_has(3);
  @$pb.TagNumber(4)
  void clearDeliveredCursor() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get completedCursor => $_getI64(4);
  @$pb.TagNumber(5)
  set completedCursor($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasCompletedCursor() => $_has(4);
  @$pb.TagNumber(5)
  void clearCompletedCursor() => $_clearField(5);

  @$pb.TagNumber(6)
  $fixnum.Int64 get recoverableCursor => $_getI64(5);
  @$pb.TagNumber(6)
  set recoverableCursor($fixnum.Int64 value) => $_setInt64(5, value);
  @$pb.TagNumber(6)
  $core.bool hasRecoverableCursor() => $_has(5);
  @$pb.TagNumber(6)
  void clearRecoverableCursor() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.bool get placementAnchor => $_getBF(6);
  @$pb.TagNumber(7)
  set placementAnchor($core.bool value) => $_setBool(6, value);
  @$pb.TagNumber(7)
  $core.bool hasPlacementAnchor() => $_has(6);
  @$pb.TagNumber(7)
  void clearPlacementAnchor() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.int get retryCount => $_getIZ(7);
  @$pb.TagNumber(8)
  set retryCount($core.int value) => $_setUnsignedInt32(7, value);
  @$pb.TagNumber(8)
  $core.bool hasRetryCount() => $_has(7);
  @$pb.TagNumber(8)
  void clearRetryCount() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.String get failureCode => $_getSZ(8);
  @$pb.TagNumber(9)
  set failureCode($core.String value) => $_setString(8, value);
  @$pb.TagNumber(9)
  $core.bool hasFailureCode() => $_has(8);
  @$pb.TagNumber(9)
  void clearFailureCode() => $_clearField(9);

  @$pb.TagNumber(10)
  $fixnum.Int64 get failedCursor => $_getI64(9);
  @$pb.TagNumber(10)
  set failedCursor($fixnum.Int64 value) => $_setInt64(9, value);
  @$pb.TagNumber(10)
  $core.bool hasFailedCursor() => $_has(9);
  @$pb.TagNumber(10)
  void clearFailedCursor() => $_clearField(10);
}

class ActorObservation extends $pb.GeneratedMessage {
  factory ActorObservation({
    $core.String? actorId,
    $core.List<$core.int>? codeSha256,
    $core.String? homeRegion,
    ActorState? state,
    $core.Iterable<SubscriptionObservation>? subscriptions,
    $fixnum.Int64? checkpointUnixMillis,
    $fixnum.Int64? checkpointEpoch,
    $fixnum.Int64? configurationRevision,
  }) {
    final result = ActorObservation._();
    if (actorId != null) result.actorId = actorId;
    if (codeSha256 != null) result.codeSha256 = codeSha256;
    if (homeRegion != null) result.homeRegion = homeRegion;
    if (state != null) result.state = state;
    if (subscriptions != null) result.subscriptions.addAll(subscriptions);
    if (checkpointUnixMillis != null)
      result.checkpointUnixMillis = checkpointUnixMillis;
    if (checkpointEpoch != null) result.checkpointEpoch = checkpointEpoch;
    if (configurationRevision != null)
      result.configurationRevision = configurationRevision;
    return result;
  }

  ActorObservation._();

  factory ActorObservation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ActorObservation()..mergeFromBuffer(data, registry);
  factory ActorObservation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ActorObservation()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ActorObservation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: ActorObservation.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'actorId')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'codeSha256', $pb.PbFieldType.OY)
    ..aOS(3, _omitFieldNames ? '' : 'homeRegion')
    ..aE<ActorState>(4, _omitFieldNames ? '' : 'state',
        enumValues: ActorState.values)
    ..pPM<SubscriptionObservation>(5, _omitFieldNames ? '' : 'subscriptions',
        subBuilder: SubscriptionObservation.$_createMessage)
    ..a<$fixnum.Int64>(
        6, _omitFieldNames ? '' : 'checkpointUnixMillis', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        7, _omitFieldNames ? '' : 'checkpointEpoch', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        8, _omitFieldNames ? '' : 'configurationRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ActorObservation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ActorObservation copyWith(void Function(ActorObservation) updates) =>
      super.copyWith((message) => updates(message as ActorObservation))
          as ActorObservation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ActorObservation() / ActorObservation.new instead')
  static ActorObservation create() => ActorObservation._();
  static $pb.GeneratedMessage $_createMessage() => ActorObservation._();
  @$core.override
  ActorObservation createEmptyInstance() => ActorObservation._();
  @$core.pragma('dart2js:noInline')
  static ActorObservation getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ActorObservation>(
          ActorObservation.$_createMessage);
  static ActorObservation? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get actorId => $_getSZ(0);
  @$pb.TagNumber(1)
  set actorId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActorId() => $_has(0);
  @$pb.TagNumber(1)
  void clearActorId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get codeSha256 => $_getN(1);
  @$pb.TagNumber(2)
  set codeSha256($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCodeSha256() => $_has(1);
  @$pb.TagNumber(2)
  void clearCodeSha256() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get homeRegion => $_getSZ(2);
  @$pb.TagNumber(3)
  set homeRegion($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasHomeRegion() => $_has(2);
  @$pb.TagNumber(3)
  void clearHomeRegion() => $_clearField(3);

  @$pb.TagNumber(4)
  ActorState get state => $_getN(3);
  @$pb.TagNumber(4)
  set state(ActorState value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasState() => $_has(3);
  @$pb.TagNumber(4)
  void clearState() => $_clearField(4);

  @$pb.TagNumber(5)
  $pb.PbList<SubscriptionObservation> get subscriptions => $_getList(4);

  @$pb.TagNumber(6)
  $fixnum.Int64 get checkpointUnixMillis => $_getI64(5);
  @$pb.TagNumber(6)
  set checkpointUnixMillis($fixnum.Int64 value) => $_setInt64(5, value);
  @$pb.TagNumber(6)
  $core.bool hasCheckpointUnixMillis() => $_has(5);
  @$pb.TagNumber(6)
  void clearCheckpointUnixMillis() => $_clearField(6);

  @$pb.TagNumber(7)
  $fixnum.Int64 get checkpointEpoch => $_getI64(6);
  @$pb.TagNumber(7)
  set checkpointEpoch($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasCheckpointEpoch() => $_has(6);
  @$pb.TagNumber(7)
  void clearCheckpointEpoch() => $_clearField(7);

  @$pb.TagNumber(8)
  $fixnum.Int64 get configurationRevision => $_getI64(7);
  @$pb.TagNumber(8)
  set configurationRevision($fixnum.Int64 value) => $_setInt64(7, value);
  @$pb.TagNumber(8)
  $core.bool hasConfigurationRevision() => $_has(7);
  @$pb.TagNumber(8)
  void clearConfigurationRevision() => $_clearField(8);
}

class CreateActorRequest extends $pb.GeneratedMessage {
  factory CreateActorRequest({
    $core.List<$core.int>? codeSha256,
    $core.String? homeRegion,
    $core.Iterable<Binding>? bindings,
    ActorLimits? limits,
    $core.Iterable<SubscriptionSpec>? subscriptions,
    $core.String? idempotencyKey,
  }) {
    final result = CreateActorRequest._();
    if (codeSha256 != null) result.codeSha256 = codeSha256;
    if (homeRegion != null) result.homeRegion = homeRegion;
    if (bindings != null) result.bindings.addAll(bindings);
    if (limits != null) result.limits = limits;
    if (subscriptions != null) result.subscriptions.addAll(subscriptions);
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  CreateActorRequest._();

  factory CreateActorRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateActorRequest()..mergeFromBuffer(data, registry);
  factory CreateActorRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateActorRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateActorRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: CreateActorRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'codeSha256', $pb.PbFieldType.OY)
    ..aOS(2, _omitFieldNames ? '' : 'homeRegion')
    ..pPM<Binding>(3, _omitFieldNames ? '' : 'bindings',
        subBuilder: Binding.$_createMessage)
    ..aOM<ActorLimits>(4, _omitFieldNames ? '' : 'limits',
        subBuilder: ActorLimits.$_createMessage)
    ..pPM<SubscriptionSpec>(5, _omitFieldNames ? '' : 'subscriptions',
        subBuilder: SubscriptionSpec.$_createMessage)
    ..aOS(6, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateActorRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateActorRequest copyWith(void Function(CreateActorRequest) updates) =>
      super.copyWith((message) => updates(message as CreateActorRequest))
          as CreateActorRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CreateActorRequest() / CreateActorRequest.new instead')
  static CreateActorRequest create() => CreateActorRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateActorRequest._();
  @$core.override
  CreateActorRequest createEmptyInstance() => CreateActorRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateActorRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateActorRequest>(
          CreateActorRequest.$_createMessage);
  static CreateActorRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get codeSha256 => $_getN(0);
  @$pb.TagNumber(1)
  set codeSha256($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCodeSha256() => $_has(0);
  @$pb.TagNumber(1)
  void clearCodeSha256() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get homeRegion => $_getSZ(1);
  @$pb.TagNumber(2)
  set homeRegion($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasHomeRegion() => $_has(1);
  @$pb.TagNumber(2)
  void clearHomeRegion() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<Binding> get bindings => $_getList(2);

  @$pb.TagNumber(4)
  ActorLimits get limits => $_getN(3);
  @$pb.TagNumber(4)
  set limits(ActorLimits value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasLimits() => $_has(3);
  @$pb.TagNumber(4)
  void clearLimits() => $_clearField(4);
  @$pb.TagNumber(4)
  ActorLimits ensureLimits() => $_ensure(3);

  @$pb.TagNumber(5)
  $pb.PbList<SubscriptionSpec> get subscriptions => $_getList(4);

  @$pb.TagNumber(6)
  $core.String get idempotencyKey => $_getSZ(5);
  @$pb.TagNumber(6)
  set idempotencyKey($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasIdempotencyKey() => $_has(5);
  @$pb.TagNumber(6)
  void clearIdempotencyKey() => $_clearField(6);
}

class CreateActorResponse extends $pb.GeneratedMessage {
  factory CreateActorResponse({
    ActorObservation? actor,
  }) {
    final result = CreateActorResponse._();
    if (actor != null) result.actor = actor;
    return result;
  }

  CreateActorResponse._();

  factory CreateActorResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateActorResponse()..mergeFromBuffer(data, registry);
  factory CreateActorResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateActorResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateActorResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: CreateActorResponse.$_createMessage)
    ..aOM<ActorObservation>(1, _omitFieldNames ? '' : 'actor',
        subBuilder: ActorObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateActorResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateActorResponse copyWith(void Function(CreateActorResponse) updates) =>
      super.copyWith((message) => updates(message as CreateActorResponse))
          as CreateActorResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use CreateActorResponse() / CreateActorResponse.new instead')
  static CreateActorResponse create() => CreateActorResponse._();
  static $pb.GeneratedMessage $_createMessage() => CreateActorResponse._();
  @$core.override
  CreateActorResponse createEmptyInstance() => CreateActorResponse._();
  @$core.pragma('dart2js:noInline')
  static CreateActorResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateActorResponse>(
          CreateActorResponse.$_createMessage);
  static CreateActorResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ActorObservation get actor => $_getN(0);
  @$pb.TagNumber(1)
  set actor(ActorObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasActor() => $_has(0);
  @$pb.TagNumber(1)
  void clearActor() => $_clearField(1);
  @$pb.TagNumber(1)
  ActorObservation ensureActor() => $_ensure(0);
}

/// Full configuration replacement with CAS. The new code's checkpoint schema
/// must be compatible or explicitly migrated before activation; failure keeps
/// the previous version active. Paused subscriptions stay paused until resumed.
class UpdateActorRequest extends $pb.GeneratedMessage {
  factory UpdateActorRequest({
    $core.String? actorId,
    $core.List<$core.int>? codeSha256,
    $core.Iterable<Binding>? bindings,
    ActorLimits? limits,
    $fixnum.Int64? expectedConfigurationRevision,
    $core.String? idempotencyKey,
  }) {
    final result = UpdateActorRequest._();
    if (actorId != null) result.actorId = actorId;
    if (codeSha256 != null) result.codeSha256 = codeSha256;
    if (bindings != null) result.bindings.addAll(bindings);
    if (limits != null) result.limits = limits;
    if (expectedConfigurationRevision != null)
      result.expectedConfigurationRevision = expectedConfigurationRevision;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  UpdateActorRequest._();

  factory UpdateActorRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UpdateActorRequest()..mergeFromBuffer(data, registry);
  factory UpdateActorRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UpdateActorRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UpdateActorRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: UpdateActorRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'actorId')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'codeSha256', $pb.PbFieldType.OY)
    ..pPM<Binding>(3, _omitFieldNames ? '' : 'bindings',
        subBuilder: Binding.$_createMessage)
    ..aOM<ActorLimits>(4, _omitFieldNames ? '' : 'limits',
        subBuilder: ActorLimits.$_createMessage)
    ..a<$fixnum.Int64>(
        5,
        _omitFieldNames ? '' : 'expectedConfigurationRevision',
        $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(6, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UpdateActorRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UpdateActorRequest copyWith(void Function(UpdateActorRequest) updates) =>
      super.copyWith((message) => updates(message as UpdateActorRequest))
          as UpdateActorRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use UpdateActorRequest() / UpdateActorRequest.new instead')
  static UpdateActorRequest create() => UpdateActorRequest._();
  static $pb.GeneratedMessage $_createMessage() => UpdateActorRequest._();
  @$core.override
  UpdateActorRequest createEmptyInstance() => UpdateActorRequest._();
  @$core.pragma('dart2js:noInline')
  static UpdateActorRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<UpdateActorRequest>(
          UpdateActorRequest.$_createMessage);
  static UpdateActorRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get actorId => $_getSZ(0);
  @$pb.TagNumber(1)
  set actorId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActorId() => $_has(0);
  @$pb.TagNumber(1)
  void clearActorId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get codeSha256 => $_getN(1);
  @$pb.TagNumber(2)
  set codeSha256($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCodeSha256() => $_has(1);
  @$pb.TagNumber(2)
  void clearCodeSha256() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<Binding> get bindings => $_getList(2);

  @$pb.TagNumber(4)
  ActorLimits get limits => $_getN(3);
  @$pb.TagNumber(4)
  set limits(ActorLimits value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasLimits() => $_has(3);
  @$pb.TagNumber(4)
  void clearLimits() => $_clearField(4);
  @$pb.TagNumber(4)
  ActorLimits ensureLimits() => $_ensure(3);

  @$pb.TagNumber(5)
  $fixnum.Int64 get expectedConfigurationRevision => $_getI64(4);
  @$pb.TagNumber(5)
  set expectedConfigurationRevision($fixnum.Int64 value) =>
      $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasExpectedConfigurationRevision() => $_has(4);
  @$pb.TagNumber(5)
  void clearExpectedConfigurationRevision() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get idempotencyKey => $_getSZ(5);
  @$pb.TagNumber(6)
  set idempotencyKey($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasIdempotencyKey() => $_has(5);
  @$pb.TagNumber(6)
  void clearIdempotencyKey() => $_clearField(6);
}

class UpdateActorResponse extends $pb.GeneratedMessage {
  factory UpdateActorResponse({
    ActorObservation? actor,
  }) {
    final result = UpdateActorResponse._();
    if (actor != null) result.actor = actor;
    return result;
  }

  UpdateActorResponse._();

  factory UpdateActorResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UpdateActorResponse()..mergeFromBuffer(data, registry);
  factory UpdateActorResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UpdateActorResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UpdateActorResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: UpdateActorResponse.$_createMessage)
    ..aOM<ActorObservation>(1, _omitFieldNames ? '' : 'actor',
        subBuilder: ActorObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UpdateActorResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UpdateActorResponse copyWith(void Function(UpdateActorResponse) updates) =>
      super.copyWith((message) => updates(message as UpdateActorResponse))
          as UpdateActorResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use UpdateActorResponse() / UpdateActorResponse.new instead')
  static UpdateActorResponse create() => UpdateActorResponse._();
  static $pb.GeneratedMessage $_createMessage() => UpdateActorResponse._();
  @$core.override
  UpdateActorResponse createEmptyInstance() => UpdateActorResponse._();
  @$core.pragma('dart2js:noInline')
  static UpdateActorResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<UpdateActorResponse>(
          UpdateActorResponse.$_createMessage);
  static UpdateActorResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ActorObservation get actor => $_getN(0);
  @$pb.TagNumber(1)
  set actor(ActorObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasActor() => $_has(0);
  @$pb.TagNumber(1)
  void clearActor() => $_clearField(1);
  @$pb.TagNumber(1)
  ActorObservation ensureActor() => $_ensure(0);
}

class InspectActorRequest extends $pb.GeneratedMessage {
  factory InspectActorRequest({
    $core.String? actorId,
  }) {
    final result = InspectActorRequest._();
    if (actorId != null) result.actorId = actorId;
    return result;
  }

  InspectActorRequest._();

  factory InspectActorRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectActorRequest()..mergeFromBuffer(data, registry);
  factory InspectActorRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectActorRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectActorRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: InspectActorRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'actorId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectActorRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectActorRequest copyWith(void Function(InspectActorRequest) updates) =>
      super.copyWith((message) => updates(message as InspectActorRequest))
          as InspectActorRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use InspectActorRequest() / InspectActorRequest.new instead')
  static InspectActorRequest create() => InspectActorRequest._();
  static $pb.GeneratedMessage $_createMessage() => InspectActorRequest._();
  @$core.override
  InspectActorRequest createEmptyInstance() => InspectActorRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectActorRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectActorRequest>(
          InspectActorRequest.$_createMessage);
  static InspectActorRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get actorId => $_getSZ(0);
  @$pb.TagNumber(1)
  set actorId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActorId() => $_has(0);
  @$pb.TagNumber(1)
  void clearActorId() => $_clearField(1);
}

class InspectActorResponse extends $pb.GeneratedMessage {
  factory InspectActorResponse({
    ActorObservation? actor,
  }) {
    final result = InspectActorResponse._();
    if (actor != null) result.actor = actor;
    return result;
  }

  InspectActorResponse._();

  factory InspectActorResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectActorResponse()..mergeFromBuffer(data, registry);
  factory InspectActorResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectActorResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectActorResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: InspectActorResponse.$_createMessage)
    ..aOM<ActorObservation>(1, _omitFieldNames ? '' : 'actor',
        subBuilder: ActorObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectActorResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectActorResponse copyWith(void Function(InspectActorResponse) updates) =>
      super.copyWith((message) => updates(message as InspectActorResponse))
          as InspectActorResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InspectActorResponse() / InspectActorResponse.new instead')
  static InspectActorResponse create() => InspectActorResponse._();
  static $pb.GeneratedMessage $_createMessage() => InspectActorResponse._();
  @$core.override
  InspectActorResponse createEmptyInstance() => InspectActorResponse._();
  @$core.pragma('dart2js:noInline')
  static InspectActorResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectActorResponse>(
          InspectActorResponse.$_createMessage);
  static InspectActorResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ActorObservation get actor => $_getN(0);
  @$pb.TagNumber(1)
  set actor(ActorObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasActor() => $_has(0);
  @$pb.TagNumber(1)
  void clearActor() => $_clearField(1);
  @$pb.TagNumber(1)
  ActorObservation ensureActor() => $_ensure(0);
}

class AddSubscriptionRequest extends $pb.GeneratedMessage {
  factory AddSubscriptionRequest({
    $core.String? actorId,
    SubscriptionSpec? subscription,
    $core.String? idempotencyKey,
  }) {
    final result = AddSubscriptionRequest._();
    if (actorId != null) result.actorId = actorId;
    if (subscription != null) result.subscription = subscription;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  AddSubscriptionRequest._();

  factory AddSubscriptionRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AddSubscriptionRequest()..mergeFromBuffer(data, registry);
  factory AddSubscriptionRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AddSubscriptionRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AddSubscriptionRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: AddSubscriptionRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'actorId')
    ..aOM<SubscriptionSpec>(2, _omitFieldNames ? '' : 'subscription',
        subBuilder: SubscriptionSpec.$_createMessage)
    ..aOS(3, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AddSubscriptionRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AddSubscriptionRequest copyWith(
          void Function(AddSubscriptionRequest) updates) =>
      super.copyWith((message) => updates(message as AddSubscriptionRequest))
          as AddSubscriptionRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use AddSubscriptionRequest() / AddSubscriptionRequest.new instead')
  static AddSubscriptionRequest create() => AddSubscriptionRequest._();
  static $pb.GeneratedMessage $_createMessage() => AddSubscriptionRequest._();
  @$core.override
  AddSubscriptionRequest createEmptyInstance() => AddSubscriptionRequest._();
  @$core.pragma('dart2js:noInline')
  static AddSubscriptionRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<AddSubscriptionRequest>(
          AddSubscriptionRequest.$_createMessage);
  static AddSubscriptionRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get actorId => $_getSZ(0);
  @$pb.TagNumber(1)
  set actorId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActorId() => $_has(0);
  @$pb.TagNumber(1)
  void clearActorId() => $_clearField(1);

  @$pb.TagNumber(2)
  SubscriptionSpec get subscription => $_getN(1);
  @$pb.TagNumber(2)
  set subscription(SubscriptionSpec value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSubscription() => $_has(1);
  @$pb.TagNumber(2)
  void clearSubscription() => $_clearField(2);
  @$pb.TagNumber(2)
  SubscriptionSpec ensureSubscription() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.String get idempotencyKey => $_getSZ(2);
  @$pb.TagNumber(3)
  set idempotencyKey($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIdempotencyKey() => $_has(2);
  @$pb.TagNumber(3)
  void clearIdempotencyKey() => $_clearField(3);
}

class AddSubscriptionResponse extends $pb.GeneratedMessage {
  factory AddSubscriptionResponse({
    ActorObservation? actor,
  }) {
    final result = AddSubscriptionResponse._();
    if (actor != null) result.actor = actor;
    return result;
  }

  AddSubscriptionResponse._();

  factory AddSubscriptionResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AddSubscriptionResponse()..mergeFromBuffer(data, registry);
  factory AddSubscriptionResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AddSubscriptionResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AddSubscriptionResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: AddSubscriptionResponse.$_createMessage)
    ..aOM<ActorObservation>(1, _omitFieldNames ? '' : 'actor',
        subBuilder: ActorObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AddSubscriptionResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AddSubscriptionResponse copyWith(
          void Function(AddSubscriptionResponse) updates) =>
      super.copyWith((message) => updates(message as AddSubscriptionResponse))
          as AddSubscriptionResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use AddSubscriptionResponse() / AddSubscriptionResponse.new instead')
  static AddSubscriptionResponse create() => AddSubscriptionResponse._();
  static $pb.GeneratedMessage $_createMessage() => AddSubscriptionResponse._();
  @$core.override
  AddSubscriptionResponse createEmptyInstance() => AddSubscriptionResponse._();
  @$core.pragma('dart2js:noInline')
  static AddSubscriptionResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<AddSubscriptionResponse>(
          AddSubscriptionResponse.$_createMessage);
  static AddSubscriptionResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ActorObservation get actor => $_getN(0);
  @$pb.TagNumber(1)
  set actor(ActorObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasActor() => $_has(0);
  @$pb.TagNumber(1)
  void clearActor() => $_clearField(1);
  @$pb.TagNumber(1)
  ActorObservation ensureActor() => $_ensure(0);
}

class RemoveSubscriptionRequest extends $pb.GeneratedMessage {
  factory RemoveSubscriptionRequest({
    $core.String? actorId,
    $core.String? subscriptionId,
    $core.String? idempotencyKey,
  }) {
    final result = RemoveSubscriptionRequest._();
    if (actorId != null) result.actorId = actorId;
    if (subscriptionId != null) result.subscriptionId = subscriptionId;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  RemoveSubscriptionRequest._();

  factory RemoveSubscriptionRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RemoveSubscriptionRequest()..mergeFromBuffer(data, registry);
  factory RemoveSubscriptionRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RemoveSubscriptionRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RemoveSubscriptionRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: RemoveSubscriptionRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'actorId')
    ..aOS(2, _omitFieldNames ? '' : 'subscriptionId')
    ..aOS(3, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RemoveSubscriptionRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RemoveSubscriptionRequest copyWith(
          void Function(RemoveSubscriptionRequest) updates) =>
      super.copyWith((message) => updates(message as RemoveSubscriptionRequest))
          as RemoveSubscriptionRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use RemoveSubscriptionRequest() / RemoveSubscriptionRequest.new instead')
  static RemoveSubscriptionRequest create() => RemoveSubscriptionRequest._();
  static $pb.GeneratedMessage $_createMessage() =>
      RemoveSubscriptionRequest._();
  @$core.override
  RemoveSubscriptionRequest createEmptyInstance() =>
      RemoveSubscriptionRequest._();
  @$core.pragma('dart2js:noInline')
  static RemoveSubscriptionRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RemoveSubscriptionRequest>(
          RemoveSubscriptionRequest.$_createMessage);
  static RemoveSubscriptionRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get actorId => $_getSZ(0);
  @$pb.TagNumber(1)
  set actorId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActorId() => $_has(0);
  @$pb.TagNumber(1)
  void clearActorId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get subscriptionId => $_getSZ(1);
  @$pb.TagNumber(2)
  set subscriptionId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSubscriptionId() => $_has(1);
  @$pb.TagNumber(2)
  void clearSubscriptionId() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get idempotencyKey => $_getSZ(2);
  @$pb.TagNumber(3)
  set idempotencyKey($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIdempotencyKey() => $_has(2);
  @$pb.TagNumber(3)
  void clearIdempotencyKey() => $_clearField(3);
}

class RemoveSubscriptionResponse extends $pb.GeneratedMessage {
  factory RemoveSubscriptionResponse({
    ActorObservation? actor,
  }) {
    final result = RemoveSubscriptionResponse._();
    if (actor != null) result.actor = actor;
    return result;
  }

  RemoveSubscriptionResponse._();

  factory RemoveSubscriptionResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RemoveSubscriptionResponse()..mergeFromBuffer(data, registry);
  factory RemoveSubscriptionResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RemoveSubscriptionResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RemoveSubscriptionResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: RemoveSubscriptionResponse.$_createMessage)
    ..aOM<ActorObservation>(1, _omitFieldNames ? '' : 'actor',
        subBuilder: ActorObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RemoveSubscriptionResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RemoveSubscriptionResponse copyWith(
          void Function(RemoveSubscriptionResponse) updates) =>
      super.copyWith(
              (message) => updates(message as RemoveSubscriptionResponse))
          as RemoveSubscriptionResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use RemoveSubscriptionResponse() / RemoveSubscriptionResponse.new instead')
  static RemoveSubscriptionResponse create() => RemoveSubscriptionResponse._();
  static $pb.GeneratedMessage $_createMessage() =>
      RemoveSubscriptionResponse._();
  @$core.override
  RemoveSubscriptionResponse createEmptyInstance() =>
      RemoveSubscriptionResponse._();
  @$core.pragma('dart2js:noInline')
  static RemoveSubscriptionResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RemoveSubscriptionResponse>(
          RemoveSubscriptionResponse.$_createMessage);
  static RemoveSubscriptionResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ActorObservation get actor => $_getN(0);
  @$pb.TagNumber(1)
  set actor(ActorObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasActor() => $_has(0);
  @$pb.TagNumber(1)
  void clearActor() => $_clearField(1);
  @$pb.TagNumber(1)
  ActorObservation ensureActor() => $_ensure(0);
}

/// Resumption may replay a previously delivered record and duplicate external effects.
class ResumeSubscriptionRequest extends $pb.GeneratedMessage {
  factory ResumeSubscriptionRequest({
    $core.String? actorId,
    $core.String? subscriptionId,
    $core.String? idempotencyKey,
  }) {
    final result = ResumeSubscriptionRequest._();
    if (actorId != null) result.actorId = actorId;
    if (subscriptionId != null) result.subscriptionId = subscriptionId;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  ResumeSubscriptionRequest._();

  factory ResumeSubscriptionRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResumeSubscriptionRequest()..mergeFromBuffer(data, registry);
  factory ResumeSubscriptionRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResumeSubscriptionRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ResumeSubscriptionRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: ResumeSubscriptionRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'actorId')
    ..aOS(2, _omitFieldNames ? '' : 'subscriptionId')
    ..aOS(3, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResumeSubscriptionRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResumeSubscriptionRequest copyWith(
          void Function(ResumeSubscriptionRequest) updates) =>
      super.copyWith((message) => updates(message as ResumeSubscriptionRequest))
          as ResumeSubscriptionRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ResumeSubscriptionRequest() / ResumeSubscriptionRequest.new instead')
  static ResumeSubscriptionRequest create() => ResumeSubscriptionRequest._();
  static $pb.GeneratedMessage $_createMessage() =>
      ResumeSubscriptionRequest._();
  @$core.override
  ResumeSubscriptionRequest createEmptyInstance() =>
      ResumeSubscriptionRequest._();
  @$core.pragma('dart2js:noInline')
  static ResumeSubscriptionRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ResumeSubscriptionRequest>(
          ResumeSubscriptionRequest.$_createMessage);
  static ResumeSubscriptionRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get actorId => $_getSZ(0);
  @$pb.TagNumber(1)
  set actorId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActorId() => $_has(0);
  @$pb.TagNumber(1)
  void clearActorId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get subscriptionId => $_getSZ(1);
  @$pb.TagNumber(2)
  set subscriptionId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSubscriptionId() => $_has(1);
  @$pb.TagNumber(2)
  void clearSubscriptionId() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get idempotencyKey => $_getSZ(2);
  @$pb.TagNumber(3)
  set idempotencyKey($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIdempotencyKey() => $_has(2);
  @$pb.TagNumber(3)
  void clearIdempotencyKey() => $_clearField(3);
}

class ResumeSubscriptionResponse extends $pb.GeneratedMessage {
  factory ResumeSubscriptionResponse({
    ActorObservation? actor,
  }) {
    final result = ResumeSubscriptionResponse._();
    if (actor != null) result.actor = actor;
    return result;
  }

  ResumeSubscriptionResponse._();

  factory ResumeSubscriptionResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResumeSubscriptionResponse()..mergeFromBuffer(data, registry);
  factory ResumeSubscriptionResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ResumeSubscriptionResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ResumeSubscriptionResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: ResumeSubscriptionResponse.$_createMessage)
    ..aOM<ActorObservation>(1, _omitFieldNames ? '' : 'actor',
        subBuilder: ActorObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResumeSubscriptionResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ResumeSubscriptionResponse copyWith(
          void Function(ResumeSubscriptionResponse) updates) =>
      super.copyWith(
              (message) => updates(message as ResumeSubscriptionResponse))
          as ResumeSubscriptionResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ResumeSubscriptionResponse() / ResumeSubscriptionResponse.new instead')
  static ResumeSubscriptionResponse create() => ResumeSubscriptionResponse._();
  static $pb.GeneratedMessage $_createMessage() =>
      ResumeSubscriptionResponse._();
  @$core.override
  ResumeSubscriptionResponse createEmptyInstance() =>
      ResumeSubscriptionResponse._();
  @$core.pragma('dart2js:noInline')
  static ResumeSubscriptionResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ResumeSubscriptionResponse>(
          ResumeSubscriptionResponse.$_createMessage);
  static ResumeSubscriptionResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ActorObservation get actor => $_getN(0);
  @$pb.TagNumber(1)
  set actor(ActorObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasActor() => $_has(0);
  @$pb.TagNumber(1)
  void clearActor() => $_clearField(1);
  @$pb.TagNumber(1)
  ActorObservation ensureActor() => $_ensure(0);
}

class CheckpointActorRequest extends $pb.GeneratedMessage {
  factory CheckpointActorRequest({
    $core.String? actorId,
    $core.String? idempotencyKey,
  }) {
    final result = CheckpointActorRequest._();
    if (actorId != null) result.actorId = actorId;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  CheckpointActorRequest._();

  factory CheckpointActorRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointActorRequest()..mergeFromBuffer(data, registry);
  factory CheckpointActorRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointActorRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CheckpointActorRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: CheckpointActorRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'actorId')
    ..aOS(2, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointActorRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointActorRequest copyWith(
          void Function(CheckpointActorRequest) updates) =>
      super.copyWith((message) => updates(message as CheckpointActorRequest))
          as CheckpointActorRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CheckpointActorRequest() / CheckpointActorRequest.new instead')
  static CheckpointActorRequest create() => CheckpointActorRequest._();
  static $pb.GeneratedMessage $_createMessage() => CheckpointActorRequest._();
  @$core.override
  CheckpointActorRequest createEmptyInstance() => CheckpointActorRequest._();
  @$core.pragma('dart2js:noInline')
  static CheckpointActorRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CheckpointActorRequest>(
          CheckpointActorRequest.$_createMessage);
  static CheckpointActorRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get actorId => $_getSZ(0);
  @$pb.TagNumber(1)
  set actorId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActorId() => $_has(0);
  @$pb.TagNumber(1)
  void clearActorId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get idempotencyKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set idempotencyKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
}

class CheckpointActorResponse extends $pb.GeneratedMessage {
  factory CheckpointActorResponse({
    ActorObservation? actor,
  }) {
    final result = CheckpointActorResponse._();
    if (actor != null) result.actor = actor;
    return result;
  }

  CheckpointActorResponse._();

  factory CheckpointActorResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointActorResponse()..mergeFromBuffer(data, registry);
  factory CheckpointActorResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointActorResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CheckpointActorResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: CheckpointActorResponse.$_createMessage)
    ..aOM<ActorObservation>(1, _omitFieldNames ? '' : 'actor',
        subBuilder: ActorObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointActorResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointActorResponse copyWith(
          void Function(CheckpointActorResponse) updates) =>
      super.copyWith((message) => updates(message as CheckpointActorResponse))
          as CheckpointActorResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CheckpointActorResponse() / CheckpointActorResponse.new instead')
  static CheckpointActorResponse create() => CheckpointActorResponse._();
  static $pb.GeneratedMessage $_createMessage() => CheckpointActorResponse._();
  @$core.override
  CheckpointActorResponse createEmptyInstance() => CheckpointActorResponse._();
  @$core.pragma('dart2js:noInline')
  static CheckpointActorResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CheckpointActorResponse>(
          CheckpointActorResponse.$_createMessage);
  static CheckpointActorResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ActorObservation get actor => $_getN(0);
  @$pb.TagNumber(1)
  set actor(ActorObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasActor() => $_has(0);
  @$pb.TagNumber(1)
  void clearActor() => $_clearField(1);
  @$pb.TagNumber(1)
  ActorObservation ensureActor() => $_ensure(0);
}

class Header extends $pb.GeneratedMessage {
  factory Header({
    $core.String? name,
    $core.String? value,
  }) {
    final result = Header._();
    if (name != null) result.name = name;
    if (value != null) result.value = value;
    return result;
  }

  Header._();

  factory Header.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Header()..mergeFromBuffer(data, registry);
  factory Header.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Header()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Header',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: Header.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aOS(2, _omitFieldNames ? '' : 'value')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Header clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Header copyWith(void Function(Header) updates) =>
      super.copyWith((message) => updates(message as Header)) as Header;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Header() / Header.new instead')
  static Header create() => Header._();
  static $pb.GeneratedMessage $_createMessage() => Header._();
  @$core.override
  Header createEmptyInstance() => Header._();
  @$core.pragma('dart2js:noInline')
  static Header getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Header>(Header.$_createMessage);
  static Header? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get value => $_getSZ(1);
  @$pb.TagNumber(2)
  set value($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasValue() => $_has(1);
  @$pb.TagNumber(2)
  void clearValue() => $_clearField(2);
}

/// Invocation is not an implicit Stream append or persistence guarantee.
class InvokeActorRequest extends $pb.GeneratedMessage {
  factory InvokeActorRequest({
    $core.String? actorId,
    $core.String? method,
    $core.String? url,
    $core.List<$core.int>? body,
    $core.Iterable<Header>? headers,
  }) {
    final result = InvokeActorRequest._();
    if (actorId != null) result.actorId = actorId;
    if (method != null) result.method = method;
    if (url != null) result.url = url;
    if (body != null) result.body = body;
    if (headers != null) result.headers.addAll(headers);
    return result;
  }

  InvokeActorRequest._();

  factory InvokeActorRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeActorRequest()..mergeFromBuffer(data, registry);
  factory InvokeActorRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeActorRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InvokeActorRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: InvokeActorRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'actorId')
    ..aOS(2, _omitFieldNames ? '' : 'method')
    ..aOS(3, _omitFieldNames ? '' : 'url')
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..pPM<Header>(5, _omitFieldNames ? '' : 'headers',
        subBuilder: Header.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeActorRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeActorRequest copyWith(void Function(InvokeActorRequest) updates) =>
      super.copyWith((message) => updates(message as InvokeActorRequest))
          as InvokeActorRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InvokeActorRequest() / InvokeActorRequest.new instead')
  static InvokeActorRequest create() => InvokeActorRequest._();
  static $pb.GeneratedMessage $_createMessage() => InvokeActorRequest._();
  @$core.override
  InvokeActorRequest createEmptyInstance() => InvokeActorRequest._();
  @$core.pragma('dart2js:noInline')
  static InvokeActorRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InvokeActorRequest>(
          InvokeActorRequest.$_createMessage);
  static InvokeActorRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get actorId => $_getSZ(0);
  @$pb.TagNumber(1)
  set actorId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActorId() => $_has(0);
  @$pb.TagNumber(1)
  void clearActorId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get method => $_getSZ(1);
  @$pb.TagNumber(2)
  set method($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMethod() => $_has(1);
  @$pb.TagNumber(2)
  void clearMethod() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get url => $_getSZ(2);
  @$pb.TagNumber(3)
  set url($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasUrl() => $_has(2);
  @$pb.TagNumber(3)
  void clearUrl() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get body => $_getN(3);
  @$pb.TagNumber(4)
  set body($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasBody() => $_has(3);
  @$pb.TagNumber(4)
  void clearBody() => $_clearField(4);

  @$pb.TagNumber(5)
  $pb.PbList<Header> get headers => $_getList(4);
}

class InvokeActorResponse extends $pb.GeneratedMessage {
  factory InvokeActorResponse({
    $core.int? status,
    $core.List<$core.int>? body,
    $core.Iterable<Header>? headers,
  }) {
    final result = InvokeActorResponse._();
    if (status != null) result.status = status;
    if (body != null) result.body = body;
    if (headers != null) result.headers.addAll(headers);
    return result;
  }

  InvokeActorResponse._();

  factory InvokeActorResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeActorResponse()..mergeFromBuffer(data, registry);
  factory InvokeActorResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeActorResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InvokeActorResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: InvokeActorResponse.$_createMessage)
    ..aI(1, _omitFieldNames ? '' : 'status', fieldType: $pb.PbFieldType.OU3)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..pPM<Header>(3, _omitFieldNames ? '' : 'headers',
        subBuilder: Header.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeActorResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeActorResponse copyWith(void Function(InvokeActorResponse) updates) =>
      super.copyWith((message) => updates(message as InvokeActorResponse))
          as InvokeActorResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use InvokeActorResponse() / InvokeActorResponse.new instead')
  static InvokeActorResponse create() => InvokeActorResponse._();
  static $pb.GeneratedMessage $_createMessage() => InvokeActorResponse._();
  @$core.override
  InvokeActorResponse createEmptyInstance() => InvokeActorResponse._();
  @$core.pragma('dart2js:noInline')
  static InvokeActorResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InvokeActorResponse>(
          InvokeActorResponse.$_createMessage);
  static InvokeActorResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.int get status => $_getIZ(0);
  @$pb.TagNumber(1)
  set status($core.int value) => $_setUnsignedInt32(0, value);
  @$pb.TagNumber(1)
  $core.bool hasStatus() => $_has(0);
  @$pb.TagNumber(1)
  void clearStatus() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get body => $_getN(1);
  @$pb.TagNumber(2)
  set body($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBody() => $_has(1);
  @$pb.TagNumber(2)
  void clearBody() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<Header> get headers => $_getList(2);
}

class Error extends $pb.GeneratedMessage {
  factory Error({
    ErrorCode? code,
    $core.String? message,
  }) {
    final result = Error._();
    if (code != null) result.code = code;
    if (message != null) result.message = message;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.actors.v1'),
      createEmptyInstance: Error.$_createMessage)
    ..aE<ErrorCode>(1, _omitFieldNames ? '' : 'code',
        enumValues: ErrorCode.values)
    ..aOS(2, _omitFieldNames ? '' : 'message')
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
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
