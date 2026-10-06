// This is a generated file - do not edit.
//
// Generated from stream/v2/stream.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'stream.pbenum.dart';

class Record extends $pb.GeneratedMessage {
  factory Record({
    $fixnum.Int64? sequence,
    $core.List<$core.int>? value,
    $core.List<$core.int>? commitId,
    $fixnum.Int64? committedAtMicros,
  }) {
    final result = Record._();
    if (sequence != null) result.sequence = sequence;
    if (value != null) result.value = value;
    if (commitId != null) result.commitId = commitId;
    if (committedAtMicros != null) result.committedAtMicros = committedAtMicros;
    return result;
  }

  Record._();

  factory Record.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Record()..mergeFromBuffer(data, registry);
  factory Record.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Record()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Record',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: Record.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'sequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'value', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'commitId', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'committedAtMicros', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Record clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Record copyWith(void Function(Record) updates) =>
      super.copyWith((message) => updates(message as Record)) as Record;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Record() / Record.new instead')
  static Record create() => Record._();
  static $pb.GeneratedMessage $_createMessage() => Record._();
  @$core.override
  Record createEmptyInstance() => Record._();
  @$core.pragma('dart2js:noInline')
  static Record getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Record>(Record.$_createMessage);
  static Record? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get sequence => $_getI64(0);
  @$pb.TagNumber(1)
  set sequence($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSequence() => $_has(0);
  @$pb.TagNumber(1)
  void clearSequence() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get value => $_getN(1);
  @$pb.TagNumber(2)
  set value($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasValue() => $_has(1);
  @$pb.TagNumber(2)
  void clearValue() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get commitId => $_getN(2);
  @$pb.TagNumber(3)
  set commitId($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasCommitId() => $_has(2);
  @$pb.TagNumber(3)
  void clearCommitId() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get committedAtMicros => $_getI64(3);
  @$pb.TagNumber(4)
  set committedAtMicros($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCommittedAtMicros() => $_has(3);
  @$pb.TagNumber(4)
  void clearCommittedAtMicros() => $_clearField(4);
}

class AppendRequest extends $pb.GeneratedMessage {
  factory AppendRequest({
    $core.String? path,
    $core.Iterable<$core.List<$core.int>>? records,
    $fixnum.Int64? ifTail,
    $core.List<$core.int>? idempotencyKey,
  }) {
    final result = AppendRequest._();
    if (path != null) result.path = path;
    if (records != null) result.records.addAll(records);
    if (ifTail != null) result.ifTail = ifTail;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  AppendRequest._();

  factory AppendRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AppendRequest()..mergeFromBuffer(data, registry);
  factory AppendRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AppendRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AppendRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: AppendRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..p<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'records', $pb.PbFieldType.PY)
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'ifTail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'idempotencyKey', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AppendRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AppendRequest copyWith(void Function(AppendRequest) updates) =>
      super.copyWith((message) => updates(message as AppendRequest))
          as AppendRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use AppendRequest() / AppendRequest.new instead')
  static AppendRequest create() => AppendRequest._();
  static $pb.GeneratedMessage $_createMessage() => AppendRequest._();
  @$core.override
  AppendRequest createEmptyInstance() => AppendRequest._();
  @$core.pragma('dart2js:noInline')
  static AppendRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<AppendRequest>(
          AppendRequest.$_createMessage);
  static AppendRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<$core.List<$core.int>> get records => $_getList(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get ifTail => $_getI64(2);
  @$pb.TagNumber(3)
  set ifTail($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIfTail() => $_has(2);
  @$pb.TagNumber(3)
  void clearIfTail() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get idempotencyKey => $_getN(3);
  @$pb.TagNumber(4)
  set idempotencyKey($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIdempotencyKey() => $_has(3);
  @$pb.TagNumber(4)
  void clearIdempotencyKey() => $_clearField(4);
}

class AppendReceipt extends $pb.GeneratedMessage {
  factory AppendReceipt({
    $fixnum.Int64? start,
    $fixnum.Int64? end,
    $fixnum.Int64? tail,
    $core.List<$core.int>? commitId,
  }) {
    final result = AppendReceipt._();
    if (start != null) result.start = start;
    if (end != null) result.end = end;
    if (tail != null) result.tail = tail;
    if (commitId != null) result.commitId = commitId;
    return result;
  }

  AppendReceipt._();

  factory AppendReceipt.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AppendReceipt()..mergeFromBuffer(data, registry);
  factory AppendReceipt.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AppendReceipt()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AppendReceipt',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: AppendReceipt.$_createMessage)
    ..a<$fixnum.Int64>(1, _omitFieldNames ? '' : 'start', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'end', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'tail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'commitId', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AppendReceipt clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AppendReceipt copyWith(void Function(AppendReceipt) updates) =>
      super.copyWith((message) => updates(message as AppendReceipt))
          as AppendReceipt;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use AppendReceipt() / AppendReceipt.new instead')
  static AppendReceipt create() => AppendReceipt._();
  static $pb.GeneratedMessage $_createMessage() => AppendReceipt._();
  @$core.override
  AppendReceipt createEmptyInstance() => AppendReceipt._();
  @$core.pragma('dart2js:noInline')
  static AppendReceipt getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<AppendReceipt>(
          AppendReceipt.$_createMessage);
  static AppendReceipt? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get start => $_getI64(0);
  @$pb.TagNumber(1)
  set start($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasStart() => $_has(0);
  @$pb.TagNumber(1)
  void clearStart() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get end => $_getI64(1);
  @$pb.TagNumber(2)
  set end($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasEnd() => $_has(1);
  @$pb.TagNumber(2)
  void clearEnd() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get tail => $_getI64(2);
  @$pb.TagNumber(3)
  set tail($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasTail() => $_has(2);
  @$pb.TagNumber(3)
  void clearTail() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get commitId => $_getN(3);
  @$pb.TagNumber(4)
  set commitId($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCommitId() => $_has(3);
  @$pb.TagNumber(4)
  void clearCommitId() => $_clearField(4);
}

class TailConflict extends $pb.GeneratedMessage {
  factory TailConflict({
    $fixnum.Int64? actualTail,
  }) {
    final result = TailConflict._();
    if (actualTail != null) result.actualTail = actualTail;
    return result;
  }

  TailConflict._();

  factory TailConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailConflict()..mergeFromBuffer(data, registry);
  factory TailConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailConflict()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TailConflict',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: TailConflict.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'actualTail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailConflict copyWith(void Function(TailConflict) updates) =>
      super.copyWith((message) => updates(message as TailConflict))
          as TailConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TailConflict() / TailConflict.new instead')
  static TailConflict create() => TailConflict._();
  static $pb.GeneratedMessage $_createMessage() => TailConflict._();
  @$core.override
  TailConflict createEmptyInstance() => TailConflict._();
  @$core.pragma('dart2js:noInline')
  static TailConflict getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TailConflict>(
          TailConflict.$_createMessage);
  static TailConflict? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get actualTail => $_getI64(0);
  @$pb.TagNumber(1)
  set actualTail($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasActualTail() => $_has(0);
  @$pb.TagNumber(1)
  void clearActualTail() => $_clearField(1);
}

enum AppendResponse_Outcome { committed, conflict, notSet }

class AppendResponse extends $pb.GeneratedMessage {
  factory AppendResponse({
    AppendReceipt? committed,
    TailConflict? conflict,
  }) {
    final result = AppendResponse._();
    if (committed != null) result.committed = committed;
    if (conflict != null) result.conflict = conflict;
    return result;
  }

  AppendResponse._();

  factory AppendResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AppendResponse()..mergeFromBuffer(data, registry);
  factory AppendResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AppendResponse()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, AppendResponse_Outcome>
      _AppendResponse_OutcomeByTag = {
    1: AppendResponse_Outcome.committed,
    2: AppendResponse_Outcome.conflict,
    0: AppendResponse_Outcome.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AppendResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: AppendResponse.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<AppendReceipt>(1, _omitFieldNames ? '' : 'committed',
        subBuilder: AppendReceipt.$_createMessage)
    ..aOM<TailConflict>(2, _omitFieldNames ? '' : 'conflict',
        subBuilder: TailConflict.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AppendResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AppendResponse copyWith(void Function(AppendResponse) updates) =>
      super.copyWith((message) => updates(message as AppendResponse))
          as AppendResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use AppendResponse() / AppendResponse.new instead')
  static AppendResponse create() => AppendResponse._();
  static $pb.GeneratedMessage $_createMessage() => AppendResponse._();
  @$core.override
  AppendResponse createEmptyInstance() => AppendResponse._();
  @$core.pragma('dart2js:noInline')
  static AppendResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<AppendResponse>(
          AppendResponse.$_createMessage);
  static AppendResponse? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  AppendResponse_Outcome whichOutcome() =>
      _AppendResponse_OutcomeByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearOutcome() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  AppendReceipt get committed => $_getN(0);
  @$pb.TagNumber(1)
  set committed(AppendReceipt value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCommitted() => $_has(0);
  @$pb.TagNumber(1)
  void clearCommitted() => $_clearField(1);
  @$pb.TagNumber(1)
  AppendReceipt ensureCommitted() => $_ensure(0);

  @$pb.TagNumber(2)
  TailConflict get conflict => $_getN(1);
  @$pb.TagNumber(2)
  set conflict(TailConflict value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasConflict() => $_has(1);
  @$pb.TagNumber(2)
  void clearConflict() => $_clearField(2);
  @$pb.TagNumber(2)
  TailConflict ensureConflict() => $_ensure(1);
}

class TailRequest extends $pb.GeneratedMessage {
  factory TailRequest({
    $core.String? path,
  }) {
    final result = TailRequest._();
    if (path != null) result.path = path;
    return result;
  }

  TailRequest._();

  factory TailRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailRequest()..mergeFromBuffer(data, registry);
  factory TailRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TailRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: TailRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailRequest copyWith(void Function(TailRequest) updates) =>
      super.copyWith((message) => updates(message as TailRequest))
          as TailRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TailRequest() / TailRequest.new instead')
  static TailRequest create() => TailRequest._();
  static $pb.GeneratedMessage $_createMessage() => TailRequest._();
  @$core.override
  TailRequest createEmptyInstance() => TailRequest._();
  @$core.pragma('dart2js:noInline')
  static TailRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TailRequest>(
          TailRequest.$_createMessage);
  static TailRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);
}

class TailResponse extends $pb.GeneratedMessage {
  factory TailResponse({
    $fixnum.Int64? tail,
  }) {
    final result = TailResponse._();
    if (tail != null) result.tail = tail;
    return result;
  }

  TailResponse._();

  factory TailResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailResponse()..mergeFromBuffer(data, registry);
  factory TailResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TailResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: TailResponse.$_createMessage)
    ..a<$fixnum.Int64>(1, _omitFieldNames ? '' : 'tail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailResponse copyWith(void Function(TailResponse) updates) =>
      super.copyWith((message) => updates(message as TailResponse))
          as TailResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TailResponse() / TailResponse.new instead')
  static TailResponse create() => TailResponse._();
  static $pb.GeneratedMessage $_createMessage() => TailResponse._();
  @$core.override
  TailResponse createEmptyInstance() => TailResponse._();
  @$core.pragma('dart2js:noInline')
  static TailResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TailResponse>(
          TailResponse.$_createMessage);
  static TailResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get tail => $_getI64(0);
  @$pb.TagNumber(1)
  set tail($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTail() => $_has(0);
  @$pb.TagNumber(1)
  void clearTail() => $_clearField(1);
}

class ForkRequest extends $pb.GeneratedMessage {
  factory ForkRequest({
    $core.String? source,
    $core.String? destination,
    $fixnum.Int64? atTail,
    $core.List<$core.int>? idempotencyKey,
  }) {
    final result = ForkRequest._();
    if (source != null) result.source = source;
    if (destination != null) result.destination = destination;
    if (atTail != null) result.atTail = atTail;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ForkRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'source')
    ..aOS(2, _omitFieldNames ? '' : 'destination')
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'atTail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'idempotencyKey', $pb.PbFieldType.OY)
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
  $core.String get source => $_getSZ(0);
  @$pb.TagNumber(1)
  set source($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get destination => $_getSZ(1);
  @$pb.TagNumber(2)
  set destination($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDestination() => $_has(1);
  @$pb.TagNumber(2)
  void clearDestination() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get atTail => $_getI64(2);
  @$pb.TagNumber(3)
  set atTail($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasAtTail() => $_has(2);
  @$pb.TagNumber(3)
  void clearAtTail() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get idempotencyKey => $_getN(3);
  @$pb.TagNumber(4)
  set idempotencyKey($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIdempotencyKey() => $_has(3);
  @$pb.TagNumber(4)
  void clearIdempotencyKey() => $_clearField(4);
}

class ForkReceipt extends $pb.GeneratedMessage {
  factory ForkReceipt({
    $core.String? source,
    $core.String? destination,
    $fixnum.Int64? forkedAt,
    $fixnum.Int64? tail,
    $core.List<$core.int>? commitId,
  }) {
    final result = ForkReceipt._();
    if (source != null) result.source = source;
    if (destination != null) result.destination = destination;
    if (forkedAt != null) result.forkedAt = forkedAt;
    if (tail != null) result.tail = tail;
    if (commitId != null) result.commitId = commitId;
    return result;
  }

  ForkReceipt._();

  factory ForkReceipt.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkReceipt()..mergeFromBuffer(data, registry);
  factory ForkReceipt.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkReceipt()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkReceipt',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ForkReceipt.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'source')
    ..aOS(2, _omitFieldNames ? '' : 'destination')
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'forkedAt', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(4, _omitFieldNames ? '' : 'tail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'commitId', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkReceipt clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkReceipt copyWith(void Function(ForkReceipt) updates) =>
      super.copyWith((message) => updates(message as ForkReceipt))
          as ForkReceipt;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkReceipt() / ForkReceipt.new instead')
  static ForkReceipt create() => ForkReceipt._();
  static $pb.GeneratedMessage $_createMessage() => ForkReceipt._();
  @$core.override
  ForkReceipt createEmptyInstance() => ForkReceipt._();
  @$core.pragma('dart2js:noInline')
  static ForkReceipt getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkReceipt>(
          ForkReceipt.$_createMessage);
  static ForkReceipt? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get source => $_getSZ(0);
  @$pb.TagNumber(1)
  set source($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get destination => $_getSZ(1);
  @$pb.TagNumber(2)
  set destination($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDestination() => $_has(1);
  @$pb.TagNumber(2)
  void clearDestination() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get forkedAt => $_getI64(2);
  @$pb.TagNumber(3)
  set forkedAt($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasForkedAt() => $_has(2);
  @$pb.TagNumber(3)
  void clearForkedAt() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get tail => $_getI64(3);
  @$pb.TagNumber(4)
  set tail($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasTail() => $_has(3);
  @$pb.TagNumber(4)
  void clearTail() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get commitId => $_getN(4);
  @$pb.TagNumber(5)
  set commitId($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasCommitId() => $_has(4);
  @$pb.TagNumber(5)
  void clearCommitId() => $_clearField(5);
}

class ReadRequest extends $pb.GeneratedMessage {
  factory ReadRequest({
    $core.String? path,
    $fixnum.Int64? from,
    $core.int? limit,
  }) {
    final result = ReadRequest._();
    if (path != null) result.path = path;
    if (from != null) result.from = from;
    if (limit != null) result.limit = limit;
    return result;
  }

  ReadRequest._();

  factory ReadRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadRequest()..mergeFromBuffer(data, registry);
  factory ReadRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReadRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ReadRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'from', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aI(3, _omitFieldNames ? '' : 'limit', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadRequest copyWith(void Function(ReadRequest) updates) =>
      super.copyWith((message) => updates(message as ReadRequest))
          as ReadRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ReadRequest() / ReadRequest.new instead')
  static ReadRequest create() => ReadRequest._();
  static $pb.GeneratedMessage $_createMessage() => ReadRequest._();
  @$core.override
  ReadRequest createEmptyInstance() => ReadRequest._();
  @$core.pragma('dart2js:noInline')
  static ReadRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ReadRequest>(
          ReadRequest.$_createMessage);
  static ReadRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get from => $_getI64(1);
  @$pb.TagNumber(2)
  set from($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasFrom() => $_has(1);
  @$pb.TagNumber(2)
  void clearFrom() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.int get limit => $_getIZ(2);
  @$pb.TagNumber(3)
  set limit($core.int value) => $_setUnsignedInt32(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLimit() => $_has(2);
  @$pb.TagNumber(3)
  void clearLimit() => $_clearField(3);
}

class FollowRequest extends $pb.GeneratedMessage {
  factory FollowRequest({
    $core.String? path,
    $fixnum.Int64? from,
  }) {
    final result = FollowRequest._();
    if (path != null) result.path = path;
    if (from != null) result.from = from;
    return result;
  }

  FollowRequest._();

  factory FollowRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FollowRequest()..mergeFromBuffer(data, registry);
  factory FollowRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FollowRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'FollowRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: FollowRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'from', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FollowRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FollowRequest copyWith(void Function(FollowRequest) updates) =>
      super.copyWith((message) => updates(message as FollowRequest))
          as FollowRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use FollowRequest() / FollowRequest.new instead')
  static FollowRequest create() => FollowRequest._();
  static $pb.GeneratedMessage $_createMessage() => FollowRequest._();
  @$core.override
  FollowRequest createEmptyInstance() => FollowRequest._();
  @$core.pragma('dart2js:noInline')
  static FollowRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<FollowRequest>(
          FollowRequest.$_createMessage);
  static FollowRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get from => $_getI64(1);
  @$pb.TagNumber(2)
  set from($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasFrom() => $_has(1);
  @$pb.TagNumber(2)
  void clearFrom() => $_clearField(2);
}

class ReadResponse extends $pb.GeneratedMessage {
  factory ReadResponse({
    Record? record,
  }) {
    final result = ReadResponse._();
    if (record != null) result.record = record;
    return result;
  }

  ReadResponse._();

  factory ReadResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadResponse()..mergeFromBuffer(data, registry);
  factory ReadResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReadResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ReadResponse.$_createMessage)
    ..aOM<Record>(1, _omitFieldNames ? '' : 'record',
        subBuilder: Record.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadResponse copyWith(void Function(ReadResponse) updates) =>
      super.copyWith((message) => updates(message as ReadResponse))
          as ReadResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ReadResponse() / ReadResponse.new instead')
  static ReadResponse create() => ReadResponse._();
  static $pb.GeneratedMessage $_createMessage() => ReadResponse._();
  @$core.override
  ReadResponse createEmptyInstance() => ReadResponse._();
  @$core.pragma('dart2js:noInline')
  static ReadResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ReadResponse>(
          ReadResponse.$_createMessage);
  static ReadResponse? _defaultInstance;

  @$pb.TagNumber(1)
  Record get record => $_getN(0);
  @$pb.TagNumber(1)
  set record(Record value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasRecord() => $_has(0);
  @$pb.TagNumber(1)
  void clearRecord() => $_clearField(1);
  @$pb.TagNumber(1)
  Record ensureRecord() => $_ensure(0);
}

class ChildrenRequest extends $pb.GeneratedMessage {
  factory ChildrenRequest({
    $core.String? parent,
    $core.int? limit,
  }) {
    final result = ChildrenRequest._();
    if (parent != null) result.parent = parent;
    if (limit != null) result.limit = limit;
    return result;
  }

  ChildrenRequest._();

  factory ChildrenRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ChildrenRequest()..mergeFromBuffer(data, registry);
  factory ChildrenRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ChildrenRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ChildrenRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ChildrenRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'parent')
    ..aI(2, _omitFieldNames ? '' : 'limit', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ChildrenRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ChildrenRequest copyWith(void Function(ChildrenRequest) updates) =>
      super.copyWith((message) => updates(message as ChildrenRequest))
          as ChildrenRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ChildrenRequest() / ChildrenRequest.new instead')
  static ChildrenRequest create() => ChildrenRequest._();
  static $pb.GeneratedMessage $_createMessage() => ChildrenRequest._();
  @$core.override
  ChildrenRequest createEmptyInstance() => ChildrenRequest._();
  @$core.pragma('dart2js:noInline')
  static ChildrenRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ChildrenRequest>(
          ChildrenRequest.$_createMessage);
  static ChildrenRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get parent => $_getSZ(0);
  @$pb.TagNumber(1)
  set parent($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasParent() => $_has(0);
  @$pb.TagNumber(1)
  void clearParent() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.int get limit => $_getIZ(1);
  @$pb.TagNumber(2)
  set limit($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(2)
  $core.bool hasLimit() => $_has(1);
  @$pb.TagNumber(2)
  void clearLimit() => $_clearField(2);
}

class Child extends $pb.GeneratedMessage {
  factory Child({
    $core.String? path,
  }) {
    final result = Child._();
    if (path != null) result.path = path;
    return result;
  }

  Child._();

  factory Child.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Child()..mergeFromBuffer(data, registry);
  factory Child.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Child()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Child',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: Child.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Child clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Child copyWith(void Function(Child) updates) =>
      super.copyWith((message) => updates(message as Child)) as Child;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Child() / Child.new instead')
  static Child create() => Child._();
  static $pb.GeneratedMessage $_createMessage() => Child._();
  @$core.override
  Child createEmptyInstance() => Child._();
  @$core.pragma('dart2js:noInline')
  static Child getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Child>(Child.$_createMessage);
  static Child? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);
}

class ChildrenResponse extends $pb.GeneratedMessage {
  factory ChildrenResponse({
    Child? child,
  }) {
    final result = ChildrenResponse._();
    if (child != null) result.child = child;
    return result;
  }

  ChildrenResponse._();

  factory ChildrenResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ChildrenResponse()..mergeFromBuffer(data, registry);
  factory ChildrenResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ChildrenResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ChildrenResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ChildrenResponse.$_createMessage)
    ..aOM<Child>(1, _omitFieldNames ? '' : 'child',
        subBuilder: Child.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ChildrenResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ChildrenResponse copyWith(void Function(ChildrenResponse) updates) =>
      super.copyWith((message) => updates(message as ChildrenResponse))
          as ChildrenResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ChildrenResponse() / ChildrenResponse.new instead')
  static ChildrenResponse create() => ChildrenResponse._();
  static $pb.GeneratedMessage $_createMessage() => ChildrenResponse._();
  @$core.override
  ChildrenResponse createEmptyInstance() => ChildrenResponse._();
  @$core.pragma('dart2js:noInline')
  static ChildrenResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ChildrenResponse>(
          ChildrenResponse.$_createMessage);
  static ChildrenResponse? _defaultInstance;

  @$pb.TagNumber(1)
  Child get child => $_getN(0);
  @$pb.TagNumber(1)
  set child(Child value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasChild() => $_has(0);
  @$pb.TagNumber(1)
  void clearChild() => $_clearField(1);
  @$pb.TagNumber(1)
  Child ensureChild() => $_ensure(0);
}

class ChildrenPageRequest extends $pb.GeneratedMessage {
  factory ChildrenPageRequest({
    $core.String? parent,
    $core.String? after,
    $core.List<$core.int>? hierarchyVersion,
    $core.int? limit,
  }) {
    final result = ChildrenPageRequest._();
    if (parent != null) result.parent = parent;
    if (after != null) result.after = after;
    if (hierarchyVersion != null) result.hierarchyVersion = hierarchyVersion;
    if (limit != null) result.limit = limit;
    return result;
  }

  ChildrenPageRequest._();

  factory ChildrenPageRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ChildrenPageRequest()..mergeFromBuffer(data, registry);
  factory ChildrenPageRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ChildrenPageRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ChildrenPageRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ChildrenPageRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'parent')
    ..aOS(2, _omitFieldNames ? '' : 'after')
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'hierarchyVersion', $pb.PbFieldType.OY)
    ..aI(4, _omitFieldNames ? '' : 'limit', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ChildrenPageRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ChildrenPageRequest copyWith(void Function(ChildrenPageRequest) updates) =>
      super.copyWith((message) => updates(message as ChildrenPageRequest))
          as ChildrenPageRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use ChildrenPageRequest() / ChildrenPageRequest.new instead')
  static ChildrenPageRequest create() => ChildrenPageRequest._();
  static $pb.GeneratedMessage $_createMessage() => ChildrenPageRequest._();
  @$core.override
  ChildrenPageRequest createEmptyInstance() => ChildrenPageRequest._();
  @$core.pragma('dart2js:noInline')
  static ChildrenPageRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ChildrenPageRequest>(
          ChildrenPageRequest.$_createMessage);
  static ChildrenPageRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get parent => $_getSZ(0);
  @$pb.TagNumber(1)
  set parent($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasParent() => $_has(0);
  @$pb.TagNumber(1)
  void clearParent() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get after => $_getSZ(1);
  @$pb.TagNumber(2)
  set after($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAfter() => $_has(1);
  @$pb.TagNumber(2)
  void clearAfter() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get hierarchyVersion => $_getN(2);
  @$pb.TagNumber(3)
  set hierarchyVersion($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasHierarchyVersion() => $_has(2);
  @$pb.TagNumber(3)
  void clearHierarchyVersion() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.int get limit => $_getIZ(3);
  @$pb.TagNumber(4)
  set limit($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasLimit() => $_has(3);
  @$pb.TagNumber(4)
  void clearLimit() => $_clearField(4);
}

class ChildrenPageResponse extends $pb.GeneratedMessage {
  factory ChildrenPageResponse({
    $core.List<$core.int>? hierarchyVersion,
    $core.Iterable<Child>? children,
    $core.String? nextAfter,
  }) {
    final result = ChildrenPageResponse._();
    if (hierarchyVersion != null) result.hierarchyVersion = hierarchyVersion;
    if (children != null) result.children.addAll(children);
    if (nextAfter != null) result.nextAfter = nextAfter;
    return result;
  }

  ChildrenPageResponse._();

  factory ChildrenPageResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ChildrenPageResponse()..mergeFromBuffer(data, registry);
  factory ChildrenPageResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ChildrenPageResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ChildrenPageResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ChildrenPageResponse.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'hierarchyVersion', $pb.PbFieldType.OY)
    ..pPM<Child>(2, _omitFieldNames ? '' : 'children',
        subBuilder: Child.$_createMessage)
    ..aOS(3, _omitFieldNames ? '' : 'nextAfter')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ChildrenPageResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ChildrenPageResponse copyWith(void Function(ChildrenPageResponse) updates) =>
      super.copyWith((message) => updates(message as ChildrenPageResponse))
          as ChildrenPageResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ChildrenPageResponse() / ChildrenPageResponse.new instead')
  static ChildrenPageResponse create() => ChildrenPageResponse._();
  static $pb.GeneratedMessage $_createMessage() => ChildrenPageResponse._();
  @$core.override
  ChildrenPageResponse createEmptyInstance() => ChildrenPageResponse._();
  @$core.pragma('dart2js:noInline')
  static ChildrenPageResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ChildrenPageResponse>(
          ChildrenPageResponse.$_createMessage);
  static ChildrenPageResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get hierarchyVersion => $_getN(0);
  @$pb.TagNumber(1)
  set hierarchyVersion($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasHierarchyVersion() => $_has(0);
  @$pb.TagNumber(1)
  void clearHierarchyVersion() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<Child> get children => $_getList(1);

  @$pb.TagNumber(3)
  $core.String get nextAfter => $_getSZ(2);
  @$pb.TagNumber(3)
  set nextAfter($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasNextAfter() => $_has(2);
  @$pb.TagNumber(3)
  void clearNextAfter() => $_clearField(3);
}

class TailCondition extends $pb.GeneratedMessage {
  factory TailCondition({
    $core.String? path,
    $fixnum.Int64? expected,
  }) {
    final result = TailCondition._();
    if (path != null) result.path = path;
    if (expected != null) result.expected = expected;
    return result;
  }

  TailCondition._();

  factory TailCondition.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailCondition()..mergeFromBuffer(data, registry);
  factory TailCondition.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailCondition()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TailCondition',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: TailCondition.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'expected', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailCondition clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailCondition copyWith(void Function(TailCondition) updates) =>
      super.copyWith((message) => updates(message as TailCondition))
          as TailCondition;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TailCondition() / TailCondition.new instead')
  static TailCondition create() => TailCondition._();
  static $pb.GeneratedMessage $_createMessage() => TailCondition._();
  @$core.override
  TailCondition createEmptyInstance() => TailCondition._();
  @$core.pragma('dart2js:noInline')
  static TailCondition getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TailCondition>(
          TailCondition.$_createMessage);
  static TailCondition? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get expected => $_getI64(1);
  @$pb.TagNumber(2)
  set expected($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasExpected() => $_has(1);
  @$pb.TagNumber(2)
  void clearExpected() => $_clearField(2);
}

class AbsentCondition extends $pb.GeneratedMessage {
  factory AbsentCondition({
    $core.String? path,
  }) {
    final result = AbsentCondition._();
    if (path != null) result.path = path;
    return result;
  }

  AbsentCondition._();

  factory AbsentCondition.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AbsentCondition()..mergeFromBuffer(data, registry);
  factory AbsentCondition.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AbsentCondition()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AbsentCondition',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: AbsentCondition.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AbsentCondition clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AbsentCondition copyWith(void Function(AbsentCondition) updates) =>
      super.copyWith((message) => updates(message as AbsentCondition))
          as AbsentCondition;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use AbsentCondition() / AbsentCondition.new instead')
  static AbsentCondition create() => AbsentCondition._();
  static $pb.GeneratedMessage $_createMessage() => AbsentCondition._();
  @$core.override
  AbsentCondition createEmptyInstance() => AbsentCondition._();
  @$core.pragma('dart2js:noInline')
  static AbsentCondition getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<AbsentCondition>(
          AbsentCondition.$_createMessage);
  static AbsentCondition? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);
}

enum CommitCondition_Condition { tail, absent, notSet }

class CommitCondition extends $pb.GeneratedMessage {
  factory CommitCondition({
    TailCondition? tail,
    AbsentCondition? absent,
  }) {
    final result = CommitCondition._();
    if (tail != null) result.tail = tail;
    if (absent != null) result.absent = absent;
    return result;
  }

  CommitCondition._();

  factory CommitCondition.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitCondition()..mergeFromBuffer(data, registry);
  factory CommitCondition.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitCondition()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, CommitCondition_Condition>
      _CommitCondition_ConditionByTag = {
    1: CommitCondition_Condition.tail,
    2: CommitCondition_Condition.absent,
    0: CommitCondition_Condition.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommitCondition',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommitCondition.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<TailCondition>(1, _omitFieldNames ? '' : 'tail',
        subBuilder: TailCondition.$_createMessage)
    ..aOM<AbsentCondition>(2, _omitFieldNames ? '' : 'absent',
        subBuilder: AbsentCondition.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitCondition clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitCondition copyWith(void Function(CommitCondition) updates) =>
      super.copyWith((message) => updates(message as CommitCondition))
          as CommitCondition;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommitCondition() / CommitCondition.new instead')
  static CommitCondition create() => CommitCondition._();
  static $pb.GeneratedMessage $_createMessage() => CommitCondition._();
  @$core.override
  CommitCondition createEmptyInstance() => CommitCondition._();
  @$core.pragma('dart2js:noInline')
  static CommitCondition getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommitCondition>(
          CommitCondition.$_createMessage);
  static CommitCondition? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  CommitCondition_Condition whichCondition() =>
      _CommitCondition_ConditionByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearCondition() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  TailCondition get tail => $_getN(0);
  @$pb.TagNumber(1)
  set tail(TailCondition value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasTail() => $_has(0);
  @$pb.TagNumber(1)
  void clearTail() => $_clearField(1);
  @$pb.TagNumber(1)
  TailCondition ensureTail() => $_ensure(0);

  @$pb.TagNumber(2)
  AbsentCondition get absent => $_getN(1);
  @$pb.TagNumber(2)
  set absent(AbsentCondition value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAbsent() => $_has(1);
  @$pb.TagNumber(2)
  void clearAbsent() => $_clearField(2);
  @$pb.TagNumber(2)
  AbsentCondition ensureAbsent() => $_ensure(1);
}

class AppendMutation extends $pb.GeneratedMessage {
  factory AppendMutation({
    $core.String? path,
    $core.Iterable<$core.List<$core.int>>? records,
  }) {
    final result = AppendMutation._();
    if (path != null) result.path = path;
    if (records != null) result.records.addAll(records);
    return result;
  }

  AppendMutation._();

  factory AppendMutation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AppendMutation()..mergeFromBuffer(data, registry);
  factory AppendMutation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AppendMutation()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AppendMutation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: AppendMutation.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..p<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'records', $pb.PbFieldType.PY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AppendMutation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AppendMutation copyWith(void Function(AppendMutation) updates) =>
      super.copyWith((message) => updates(message as AppendMutation))
          as AppendMutation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use AppendMutation() / AppendMutation.new instead')
  static AppendMutation create() => AppendMutation._();
  static $pb.GeneratedMessage $_createMessage() => AppendMutation._();
  @$core.override
  AppendMutation createEmptyInstance() => AppendMutation._();
  @$core.pragma('dart2js:noInline')
  static AppendMutation getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<AppendMutation>(
          AppendMutation.$_createMessage);
  static AppendMutation? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<$core.List<$core.int>> get records => $_getList(1);
}

/// Forks the source's prefix ending at `at_tail` into the new destination,
/// then appends `records` to the destination, all at one linearization point.
class ForkMutation extends $pb.GeneratedMessage {
  factory ForkMutation({
    $core.String? source,
    $core.String? destination,
    $fixnum.Int64? atTail,
    $core.Iterable<$core.List<$core.int>>? records,
  }) {
    final result = ForkMutation._();
    if (source != null) result.source = source;
    if (destination != null) result.destination = destination;
    if (atTail != null) result.atTail = atTail;
    if (records != null) result.records.addAll(records);
    return result;
  }

  ForkMutation._();

  factory ForkMutation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkMutation()..mergeFromBuffer(data, registry);
  factory ForkMutation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkMutation()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkMutation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ForkMutation.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'source')
    ..aOS(2, _omitFieldNames ? '' : 'destination')
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'atTail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..p<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'records', $pb.PbFieldType.PY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkMutation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkMutation copyWith(void Function(ForkMutation) updates) =>
      super.copyWith((message) => updates(message as ForkMutation))
          as ForkMutation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkMutation() / ForkMutation.new instead')
  static ForkMutation create() => ForkMutation._();
  static $pb.GeneratedMessage $_createMessage() => ForkMutation._();
  @$core.override
  ForkMutation createEmptyInstance() => ForkMutation._();
  @$core.pragma('dart2js:noInline')
  static ForkMutation getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkMutation>(
          ForkMutation.$_createMessage);
  static ForkMutation? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get source => $_getSZ(0);
  @$pb.TagNumber(1)
  set source($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get destination => $_getSZ(1);
  @$pb.TagNumber(2)
  set destination($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDestination() => $_has(1);
  @$pb.TagNumber(2)
  void clearDestination() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get atTail => $_getI64(2);
  @$pb.TagNumber(3)
  set atTail($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasAtTail() => $_has(2);
  @$pb.TagNumber(3)
  void clearAtTail() => $_clearField(3);

  @$pb.TagNumber(4)
  $pb.PbList<$core.List<$core.int>> get records => $_getList(3);
}

enum CommitMutation_Mutation { append, fork, notSet }

class CommitMutation extends $pb.GeneratedMessage {
  factory CommitMutation({
    AppendMutation? append,
    ForkMutation? fork,
  }) {
    final result = CommitMutation._();
    if (append != null) result.append = append;
    if (fork != null) result.fork = fork;
    return result;
  }

  CommitMutation._();

  factory CommitMutation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitMutation()..mergeFromBuffer(data, registry);
  factory CommitMutation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitMutation()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, CommitMutation_Mutation>
      _CommitMutation_MutationByTag = {
    1: CommitMutation_Mutation.append,
    2: CommitMutation_Mutation.fork,
    0: CommitMutation_Mutation.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommitMutation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommitMutation.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<AppendMutation>(1, _omitFieldNames ? '' : 'append',
        subBuilder: AppendMutation.$_createMessage)
    ..aOM<ForkMutation>(2, _omitFieldNames ? '' : 'fork',
        subBuilder: ForkMutation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitMutation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitMutation copyWith(void Function(CommitMutation) updates) =>
      super.copyWith((message) => updates(message as CommitMutation))
          as CommitMutation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommitMutation() / CommitMutation.new instead')
  static CommitMutation create() => CommitMutation._();
  static $pb.GeneratedMessage $_createMessage() => CommitMutation._();
  @$core.override
  CommitMutation createEmptyInstance() => CommitMutation._();
  @$core.pragma('dart2js:noInline')
  static CommitMutation getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommitMutation>(
          CommitMutation.$_createMessage);
  static CommitMutation? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  CommitMutation_Mutation whichMutation() =>
      _CommitMutation_MutationByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearMutation() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  AppendMutation get append => $_getN(0);
  @$pb.TagNumber(1)
  set append(AppendMutation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasAppend() => $_has(0);
  @$pb.TagNumber(1)
  void clearAppend() => $_clearField(1);
  @$pb.TagNumber(1)
  AppendMutation ensureAppend() => $_ensure(0);

  @$pb.TagNumber(2)
  ForkMutation get fork => $_getN(1);
  @$pb.TagNumber(2)
  set fork(ForkMutation value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasFork() => $_has(1);
  @$pb.TagNumber(2)
  void clearFork() => $_clearField(2);
  @$pb.TagNumber(2)
  ForkMutation ensureFork() => $_ensure(1);
}

class CommitRequest extends $pb.GeneratedMessage {
  factory CommitRequest({
    $core.Iterable<CommitCondition>? conditions,
    $core.Iterable<CommitMutation>? mutations,
    $core.List<$core.int>? idempotencyKey,
    $fixnum.Int64? deadlineUnixMillis,
  }) {
    final result = CommitRequest._();
    if (conditions != null) result.conditions.addAll(conditions);
    if (mutations != null) result.mutations.addAll(mutations);
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (deadlineUnixMillis != null)
      result.deadlineUnixMillis = deadlineUnixMillis;
    return result;
  }

  CommitRequest._();

  factory CommitRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitRequest()..mergeFromBuffer(data, registry);
  factory CommitRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommitRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommitRequest.$_createMessage)
    ..pPM<CommitCondition>(1, _omitFieldNames ? '' : 'conditions',
        subBuilder: CommitCondition.$_createMessage)
    ..pPM<CommitMutation>(2, _omitFieldNames ? '' : 'mutations',
        subBuilder: CommitMutation.$_createMessage)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'idempotencyKey', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'deadlineUnixMillis', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitRequest copyWith(void Function(CommitRequest) updates) =>
      super.copyWith((message) => updates(message as CommitRequest))
          as CommitRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommitRequest() / CommitRequest.new instead')
  static CommitRequest create() => CommitRequest._();
  static $pb.GeneratedMessage $_createMessage() => CommitRequest._();
  @$core.override
  CommitRequest createEmptyInstance() => CommitRequest._();
  @$core.pragma('dart2js:noInline')
  static CommitRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommitRequest>(
          CommitRequest.$_createMessage);
  static CommitRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<CommitCondition> get conditions => $_getList(0);

  @$pb.TagNumber(2)
  $pb.PbList<CommitMutation> get mutations => $_getList(1);

  @$pb.TagNumber(3)
  $core.List<$core.int> get idempotencyKey => $_getN(2);
  @$pb.TagNumber(3)
  set idempotencyKey($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIdempotencyKey() => $_has(2);
  @$pb.TagNumber(3)
  void clearIdempotencyKey() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get deadlineUnixMillis => $_getI64(3);
  @$pb.TagNumber(4)
  set deadlineUnixMillis($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasDeadlineUnixMillis() => $_has(3);
  @$pb.TagNumber(4)
  void clearDeadlineUnixMillis() => $_clearField(4);
}

class CommittedAppend extends $pb.GeneratedMessage {
  factory CommittedAppend({
    $core.String? path,
    $fixnum.Int64? start,
    $fixnum.Int64? end,
    $fixnum.Int64? tail,
    $core.Iterable<Record>? records,
  }) {
    final result = CommittedAppend._();
    if (path != null) result.path = path;
    if (start != null) result.start = start;
    if (end != null) result.end = end;
    if (tail != null) result.tail = tail;
    if (records != null) result.records.addAll(records);
    return result;
  }

  CommittedAppend._();

  factory CommittedAppend.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommittedAppend()..mergeFromBuffer(data, registry);
  factory CommittedAppend.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommittedAppend()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommittedAppend',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommittedAppend.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'start', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'end', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(4, _omitFieldNames ? '' : 'tail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..pPM<Record>(5, _omitFieldNames ? '' : 'records',
        subBuilder: Record.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommittedAppend clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommittedAppend copyWith(void Function(CommittedAppend) updates) =>
      super.copyWith((message) => updates(message as CommittedAppend))
          as CommittedAppend;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommittedAppend() / CommittedAppend.new instead')
  static CommittedAppend create() => CommittedAppend._();
  static $pb.GeneratedMessage $_createMessage() => CommittedAppend._();
  @$core.override
  CommittedAppend createEmptyInstance() => CommittedAppend._();
  @$core.pragma('dart2js:noInline')
  static CommittedAppend getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommittedAppend>(
          CommittedAppend.$_createMessage);
  static CommittedAppend? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get start => $_getI64(1);
  @$pb.TagNumber(2)
  set start($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasStart() => $_has(1);
  @$pb.TagNumber(2)
  void clearStart() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get end => $_getI64(2);
  @$pb.TagNumber(3)
  set end($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasEnd() => $_has(2);
  @$pb.TagNumber(3)
  void clearEnd() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get tail => $_getI64(3);
  @$pb.TagNumber(4)
  set tail($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasTail() => $_has(3);
  @$pb.TagNumber(4)
  void clearTail() => $_clearField(4);

  @$pb.TagNumber(5)
  $pb.PbList<Record> get records => $_getList(4);
}

class CommittedFork extends $pb.GeneratedMessage {
  factory CommittedFork({
    $core.String? source,
    $core.String? destination,
    $fixnum.Int64? forkedAt,
    $fixnum.Int64? tail,
    $core.Iterable<Record>? records,
  }) {
    final result = CommittedFork._();
    if (source != null) result.source = source;
    if (destination != null) result.destination = destination;
    if (forkedAt != null) result.forkedAt = forkedAt;
    if (tail != null) result.tail = tail;
    if (records != null) result.records.addAll(records);
    return result;
  }

  CommittedFork._();

  factory CommittedFork.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommittedFork()..mergeFromBuffer(data, registry);
  factory CommittedFork.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommittedFork()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommittedFork',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommittedFork.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'source')
    ..aOS(2, _omitFieldNames ? '' : 'destination')
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'forkedAt', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(4, _omitFieldNames ? '' : 'tail', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..pPM<Record>(5, _omitFieldNames ? '' : 'records',
        subBuilder: Record.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommittedFork clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommittedFork copyWith(void Function(CommittedFork) updates) =>
      super.copyWith((message) => updates(message as CommittedFork))
          as CommittedFork;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommittedFork() / CommittedFork.new instead')
  static CommittedFork create() => CommittedFork._();
  static $pb.GeneratedMessage $_createMessage() => CommittedFork._();
  @$core.override
  CommittedFork createEmptyInstance() => CommittedFork._();
  @$core.pragma('dart2js:noInline')
  static CommittedFork getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommittedFork>(
          CommittedFork.$_createMessage);
  static CommittedFork? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get source => $_getSZ(0);
  @$pb.TagNumber(1)
  set source($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get destination => $_getSZ(1);
  @$pb.TagNumber(2)
  set destination($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDestination() => $_has(1);
  @$pb.TagNumber(2)
  void clearDestination() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get forkedAt => $_getI64(2);
  @$pb.TagNumber(3)
  set forkedAt($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasForkedAt() => $_has(2);
  @$pb.TagNumber(3)
  void clearForkedAt() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get tail => $_getI64(3);
  @$pb.TagNumber(4)
  set tail($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasTail() => $_has(3);
  @$pb.TagNumber(4)
  void clearTail() => $_clearField(4);

  @$pb.TagNumber(5)
  $pb.PbList<Record> get records => $_getList(4);
}

enum CommittedMutation_Mutation { append, fork, notSet }

class CommittedMutation extends $pb.GeneratedMessage {
  factory CommittedMutation({
    CommittedAppend? append,
    CommittedFork? fork,
  }) {
    final result = CommittedMutation._();
    if (append != null) result.append = append;
    if (fork != null) result.fork = fork;
    return result;
  }

  CommittedMutation._();

  factory CommittedMutation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommittedMutation()..mergeFromBuffer(data, registry);
  factory CommittedMutation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommittedMutation()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, CommittedMutation_Mutation>
      _CommittedMutation_MutationByTag = {
    1: CommittedMutation_Mutation.append,
    2: CommittedMutation_Mutation.fork,
    0: CommittedMutation_Mutation.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommittedMutation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommittedMutation.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<CommittedAppend>(1, _omitFieldNames ? '' : 'append',
        subBuilder: CommittedAppend.$_createMessage)
    ..aOM<CommittedFork>(2, _omitFieldNames ? '' : 'fork',
        subBuilder: CommittedFork.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommittedMutation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommittedMutation copyWith(void Function(CommittedMutation) updates) =>
      super.copyWith((message) => updates(message as CommittedMutation))
          as CommittedMutation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommittedMutation() / CommittedMutation.new instead')
  static CommittedMutation create() => CommittedMutation._();
  static $pb.GeneratedMessage $_createMessage() => CommittedMutation._();
  @$core.override
  CommittedMutation createEmptyInstance() => CommittedMutation._();
  @$core.pragma('dart2js:noInline')
  static CommittedMutation getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommittedMutation>(
          CommittedMutation.$_createMessage);
  static CommittedMutation? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  CommittedMutation_Mutation whichMutation() =>
      _CommittedMutation_MutationByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearMutation() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  CommittedAppend get append => $_getN(0);
  @$pb.TagNumber(1)
  set append(CommittedAppend value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasAppend() => $_has(0);
  @$pb.TagNumber(1)
  void clearAppend() => $_clearField(1);
  @$pb.TagNumber(1)
  CommittedAppend ensureAppend() => $_ensure(0);

  @$pb.TagNumber(2)
  CommittedFork get fork => $_getN(1);
  @$pb.TagNumber(2)
  set fork(CommittedFork value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasFork() => $_has(1);
  @$pb.TagNumber(2)
  void clearFork() => $_clearField(2);
  @$pb.TagNumber(2)
  CommittedFork ensureFork() => $_ensure(1);
}

class CommittedEnvelope extends $pb.GeneratedMessage {
  factory CommittedEnvelope({
    $core.List<$core.int>? commitId,
    $core.Iterable<CommittedMutation>? mutations,
  }) {
    final result = CommittedEnvelope._();
    if (commitId != null) result.commitId = commitId;
    if (mutations != null) result.mutations.addAll(mutations);
    return result;
  }

  CommittedEnvelope._();

  factory CommittedEnvelope.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommittedEnvelope()..mergeFromBuffer(data, registry);
  factory CommittedEnvelope.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommittedEnvelope()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommittedEnvelope',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommittedEnvelope.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'commitId', $pb.PbFieldType.OY)
    ..pPM<CommittedMutation>(2, _omitFieldNames ? '' : 'mutations',
        subBuilder: CommittedMutation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommittedEnvelope clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommittedEnvelope copyWith(void Function(CommittedEnvelope) updates) =>
      super.copyWith((message) => updates(message as CommittedEnvelope))
          as CommittedEnvelope;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommittedEnvelope() / CommittedEnvelope.new instead')
  static CommittedEnvelope create() => CommittedEnvelope._();
  static $pb.GeneratedMessage $_createMessage() => CommittedEnvelope._();
  @$core.override
  CommittedEnvelope createEmptyInstance() => CommittedEnvelope._();
  @$core.pragma('dart2js:noInline')
  static CommittedEnvelope getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommittedEnvelope>(
          CommittedEnvelope.$_createMessage);
  static CommittedEnvelope? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get commitId => $_getN(0);
  @$pb.TagNumber(1)
  set commitId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCommitId() => $_has(0);
  @$pb.TagNumber(1)
  void clearCommitId() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<CommittedMutation> get mutations => $_getList(1);
}

class TailCommitConflict extends $pb.GeneratedMessage {
  factory TailCommitConflict({
    $core.String? path,
    $fixnum.Int64? expected,
    $fixnum.Int64? actual,
  }) {
    final result = TailCommitConflict._();
    if (path != null) result.path = path;
    if (expected != null) result.expected = expected;
    if (actual != null) result.actual = actual;
    return result;
  }

  TailCommitConflict._();

  factory TailCommitConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailCommitConflict()..mergeFromBuffer(data, registry);
  factory TailCommitConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TailCommitConflict()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TailCommitConflict',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: TailCommitConflict.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'expected', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'actual', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailCommitConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TailCommitConflict copyWith(void Function(TailCommitConflict) updates) =>
      super.copyWith((message) => updates(message as TailCommitConflict))
          as TailCommitConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TailCommitConflict() / TailCommitConflict.new instead')
  static TailCommitConflict create() => TailCommitConflict._();
  static $pb.GeneratedMessage $_createMessage() => TailCommitConflict._();
  @$core.override
  TailCommitConflict createEmptyInstance() => TailCommitConflict._();
  @$core.pragma('dart2js:noInline')
  static TailCommitConflict getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<TailCommitConflict>(
          TailCommitConflict.$_createMessage);
  static TailCommitConflict? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get expected => $_getI64(1);
  @$pb.TagNumber(2)
  set expected($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasExpected() => $_has(1);
  @$pb.TagNumber(2)
  void clearExpected() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get actual => $_getI64(2);
  @$pb.TagNumber(3)
  set actual($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasActual() => $_has(2);
  @$pb.TagNumber(3)
  void clearActual() => $_clearField(3);
}

class ExistsCommitConflict extends $pb.GeneratedMessage {
  factory ExistsCommitConflict({
    $core.String? path,
  }) {
    final result = ExistsCommitConflict._();
    if (path != null) result.path = path;
    return result;
  }

  ExistsCommitConflict._();

  factory ExistsCommitConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExistsCommitConflict()..mergeFromBuffer(data, registry);
  factory ExistsCommitConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExistsCommitConflict()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExistsCommitConflict',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ExistsCommitConflict.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExistsCommitConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExistsCommitConflict copyWith(void Function(ExistsCommitConflict) updates) =>
      super.copyWith((message) => updates(message as ExistsCommitConflict))
          as ExistsCommitConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ExistsCommitConflict() / ExistsCommitConflict.new instead')
  static ExistsCommitConflict create() => ExistsCommitConflict._();
  static $pb.GeneratedMessage $_createMessage() => ExistsCommitConflict._();
  @$core.override
  ExistsCommitConflict createEmptyInstance() => ExistsCommitConflict._();
  @$core.pragma('dart2js:noInline')
  static ExistsCommitConflict getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ExistsCommitConflict>(
          ExistsCommitConflict.$_createMessage);
  static ExistsCommitConflict? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);
}

enum CommitConflict_Conflict { tail, exists, notSet }

class CommitConflict extends $pb.GeneratedMessage {
  factory CommitConflict({
    TailCommitConflict? tail,
    ExistsCommitConflict? exists,
  }) {
    final result = CommitConflict._();
    if (tail != null) result.tail = tail;
    if (exists != null) result.exists = exists;
    return result;
  }

  CommitConflict._();

  factory CommitConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitConflict()..mergeFromBuffer(data, registry);
  factory CommitConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitConflict()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, CommitConflict_Conflict>
      _CommitConflict_ConflictByTag = {
    1: CommitConflict_Conflict.tail,
    2: CommitConflict_Conflict.exists,
    0: CommitConflict_Conflict.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommitConflict',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommitConflict.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<TailCommitConflict>(1, _omitFieldNames ? '' : 'tail',
        subBuilder: TailCommitConflict.$_createMessage)
    ..aOM<ExistsCommitConflict>(2, _omitFieldNames ? '' : 'exists',
        subBuilder: ExistsCommitConflict.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitConflict copyWith(void Function(CommitConflict) updates) =>
      super.copyWith((message) => updates(message as CommitConflict))
          as CommitConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommitConflict() / CommitConflict.new instead')
  static CommitConflict create() => CommitConflict._();
  static $pb.GeneratedMessage $_createMessage() => CommitConflict._();
  @$core.override
  CommitConflict createEmptyInstance() => CommitConflict._();
  @$core.pragma('dart2js:noInline')
  static CommitConflict getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommitConflict>(
          CommitConflict.$_createMessage);
  static CommitConflict? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  CommitConflict_Conflict whichConflict() =>
      _CommitConflict_ConflictByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearConflict() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  TailCommitConflict get tail => $_getN(0);
  @$pb.TagNumber(1)
  set tail(TailCommitConflict value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasTail() => $_has(0);
  @$pb.TagNumber(1)
  void clearTail() => $_clearField(1);
  @$pb.TagNumber(1)
  TailCommitConflict ensureTail() => $_ensure(0);

  @$pb.TagNumber(2)
  ExistsCommitConflict get exists => $_getN(1);
  @$pb.TagNumber(2)
  set exists(ExistsCommitConflict value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasExists() => $_has(1);
  @$pb.TagNumber(2)
  void clearExists() => $_clearField(2);
  @$pb.TagNumber(2)
  ExistsCommitConflict ensureExists() => $_ensure(1);
}

class CommitConflicts extends $pb.GeneratedMessage {
  factory CommitConflicts({
    $core.Iterable<CommitConflict>? conflicts,
  }) {
    final result = CommitConflicts._();
    if (conflicts != null) result.conflicts.addAll(conflicts);
    return result;
  }

  CommitConflicts._();

  factory CommitConflicts.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitConflicts()..mergeFromBuffer(data, registry);
  factory CommitConflicts.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitConflicts()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommitConflicts',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommitConflicts.$_createMessage)
    ..pPM<CommitConflict>(1, _omitFieldNames ? '' : 'conflicts',
        subBuilder: CommitConflict.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitConflicts clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitConflicts copyWith(void Function(CommitConflicts) updates) =>
      super.copyWith((message) => updates(message as CommitConflicts))
          as CommitConflicts;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommitConflicts() / CommitConflicts.new instead')
  static CommitConflicts create() => CommitConflicts._();
  static $pb.GeneratedMessage $_createMessage() => CommitConflicts._();
  @$core.override
  CommitConflicts createEmptyInstance() => CommitConflicts._();
  @$core.pragma('dart2js:noInline')
  static CommitConflicts getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommitConflicts>(
          CommitConflicts.$_createMessage);
  static CommitConflicts? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<CommitConflict> get conflicts => $_getList(0);
}

enum CommitResponse_Outcome { committed, conflict, notSet }

class CommitResponse extends $pb.GeneratedMessage {
  factory CommitResponse({
    CommittedEnvelope? committed,
    CommitConflicts? conflict,
  }) {
    final result = CommitResponse._();
    if (committed != null) result.committed = committed;
    if (conflict != null) result.conflict = conflict;
    return result;
  }

  CommitResponse._();

  factory CommitResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitResponse()..mergeFromBuffer(data, registry);
  factory CommitResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CommitResponse()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, CommitResponse_Outcome>
      _CommitResponse_OutcomeByTag = {
    1: CommitResponse_Outcome.committed,
    2: CommitResponse_Outcome.conflict,
    0: CommitResponse_Outcome.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CommitResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CommitResponse.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<CommittedEnvelope>(1, _omitFieldNames ? '' : 'committed',
        subBuilder: CommittedEnvelope.$_createMessage)
    ..aOM<CommitConflicts>(2, _omitFieldNames ? '' : 'conflict',
        subBuilder: CommitConflicts.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CommitResponse copyWith(void Function(CommitResponse) updates) =>
      super.copyWith((message) => updates(message as CommitResponse))
          as CommitResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CommitResponse() / CommitResponse.new instead')
  static CommitResponse create() => CommitResponse._();
  static $pb.GeneratedMessage $_createMessage() => CommitResponse._();
  @$core.override
  CommitResponse createEmptyInstance() => CommitResponse._();
  @$core.pragma('dart2js:noInline')
  static CommitResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CommitResponse>(
          CommitResponse.$_createMessage);
  static CommitResponse? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  CommitResponse_Outcome whichOutcome() =>
      _CommitResponse_OutcomeByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearOutcome() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  CommittedEnvelope get committed => $_getN(0);
  @$pb.TagNumber(1)
  set committed(CommittedEnvelope value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCommitted() => $_has(0);
  @$pb.TagNumber(1)
  void clearCommitted() => $_clearField(1);
  @$pb.TagNumber(1)
  CommittedEnvelope ensureCommitted() => $_ensure(0);

  @$pb.TagNumber(2)
  CommitConflicts get conflict => $_getN(1);
  @$pb.TagNumber(2)
  set conflict(CommitConflicts value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasConflict() => $_has(1);
  @$pb.TagNumber(2)
  void clearConflict() => $_clearField(2);
  @$pb.TagNumber(2)
  CommitConflicts ensureConflict() => $_ensure(1);
}

class ReadCommitRequest extends $pb.GeneratedMessage {
  factory ReadCommitRequest({
    $core.List<$core.int>? commitId,
  }) {
    final result = ReadCommitRequest._();
    if (commitId != null) result.commitId = commitId;
    return result;
  }

  ReadCommitRequest._();

  factory ReadCommitRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadCommitRequest()..mergeFromBuffer(data, registry);
  factory ReadCommitRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadCommitRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReadCommitRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: ReadCommitRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'commitId', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadCommitRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadCommitRequest copyWith(void Function(ReadCommitRequest) updates) =>
      super.copyWith((message) => updates(message as ReadCommitRequest))
          as ReadCommitRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ReadCommitRequest() / ReadCommitRequest.new instead')
  static ReadCommitRequest create() => ReadCommitRequest._();
  static $pb.GeneratedMessage $_createMessage() => ReadCommitRequest._();
  @$core.override
  ReadCommitRequest createEmptyInstance() => ReadCommitRequest._();
  @$core.pragma('dart2js:noInline')
  static ReadCommitRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ReadCommitRequest>(
          ReadCommitRequest.$_createMessage);
  static ReadCommitRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get commitId => $_getN(0);
  @$pb.TagNumber(1)
  set commitId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCommitId() => $_has(0);
  @$pb.TagNumber(1)
  void clearCommitId() => $_clearField(1);
}

class InspectIdempotencyRequest extends $pb.GeneratedMessage {
  factory InspectIdempotencyRequest({
    $core.List<$core.int>? idempotencyKey,
  }) {
    final result = InspectIdempotencyRequest._();
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  InspectIdempotencyRequest._();

  factory InspectIdempotencyRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectIdempotencyRequest()..mergeFromBuffer(data, registry);
  factory InspectIdempotencyRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectIdempotencyRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectIdempotencyRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: InspectIdempotencyRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'idempotencyKey', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectIdempotencyRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectIdempotencyRequest copyWith(
          void Function(InspectIdempotencyRequest) updates) =>
      super.copyWith((message) => updates(message as InspectIdempotencyRequest))
          as InspectIdempotencyRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InspectIdempotencyRequest() / InspectIdempotencyRequest.new instead')
  static InspectIdempotencyRequest create() => InspectIdempotencyRequest._();
  static $pb.GeneratedMessage $_createMessage() =>
      InspectIdempotencyRequest._();
  @$core.override
  InspectIdempotencyRequest createEmptyInstance() =>
      InspectIdempotencyRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectIdempotencyRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectIdempotencyRequest>(
          InspectIdempotencyRequest.$_createMessage);
  static InspectIdempotencyRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get idempotencyKey => $_getN(0);
  @$pb.TagNumber(1)
  set idempotencyKey($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIdempotencyKey() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdempotencyKey() => $_clearField(1);
}

enum IdempotencyObservation_Outcome { append, fork, commit, notSet }

class IdempotencyObservation extends $pb.GeneratedMessage {
  factory IdempotencyObservation({
    $core.List<$core.int>? idempotencyKey,
    $core.List<$core.int>? requestDigest,
    AppendResponse? append,
    ForkReceipt? fork,
    CommitResponse? commit,
  }) {
    final result = IdempotencyObservation._();
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (requestDigest != null) result.requestDigest = requestDigest;
    if (append != null) result.append = append;
    if (fork != null) result.fork = fork;
    if (commit != null) result.commit = commit;
    return result;
  }

  IdempotencyObservation._();

  factory IdempotencyObservation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      IdempotencyObservation()..mergeFromBuffer(data, registry);
  factory IdempotencyObservation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      IdempotencyObservation()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, IdempotencyObservation_Outcome>
      _IdempotencyObservation_OutcomeByTag = {
    3: IdempotencyObservation_Outcome.append,
    4: IdempotencyObservation_Outcome.fork,
    7: IdempotencyObservation_Outcome.commit,
    0: IdempotencyObservation_Outcome.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'IdempotencyObservation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: IdempotencyObservation.$_createMessage)
    ..oo(0, [3, 4, 7])
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'idempotencyKey', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'requestDigest', $pb.PbFieldType.OY)
    ..aOM<AppendResponse>(3, _omitFieldNames ? '' : 'append',
        subBuilder: AppendResponse.$_createMessage)
    ..aOM<ForkReceipt>(4, _omitFieldNames ? '' : 'fork',
        subBuilder: ForkReceipt.$_createMessage)
    ..aOM<CommitResponse>(7, _omitFieldNames ? '' : 'commit',
        subBuilder: CommitResponse.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IdempotencyObservation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IdempotencyObservation copyWith(
          void Function(IdempotencyObservation) updates) =>
      super.copyWith((message) => updates(message as IdempotencyObservation))
          as IdempotencyObservation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use IdempotencyObservation() / IdempotencyObservation.new instead')
  static IdempotencyObservation create() => IdempotencyObservation._();
  static $pb.GeneratedMessage $_createMessage() => IdempotencyObservation._();
  @$core.override
  IdempotencyObservation createEmptyInstance() => IdempotencyObservation._();
  @$core.pragma('dart2js:noInline')
  static IdempotencyObservation getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<IdempotencyObservation>(
          IdempotencyObservation.$_createMessage);
  static IdempotencyObservation? _defaultInstance;

  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(7)
  IdempotencyObservation_Outcome whichOutcome() =>
      _IdempotencyObservation_OutcomeByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(7)
  void clearOutcome() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.List<$core.int> get idempotencyKey => $_getN(0);
  @$pb.TagNumber(1)
  set idempotencyKey($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIdempotencyKey() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdempotencyKey() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get requestDigest => $_getN(1);
  @$pb.TagNumber(2)
  set requestDigest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRequestDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearRequestDigest() => $_clearField(2);

  @$pb.TagNumber(3)
  AppendResponse get append => $_getN(2);
  @$pb.TagNumber(3)
  set append(AppendResponse value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasAppend() => $_has(2);
  @$pb.TagNumber(3)
  void clearAppend() => $_clearField(3);
  @$pb.TagNumber(3)
  AppendResponse ensureAppend() => $_ensure(2);

  @$pb.TagNumber(4)
  ForkReceipt get fork => $_getN(3);
  @$pb.TagNumber(4)
  set fork(ForkReceipt value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasFork() => $_has(3);
  @$pb.TagNumber(4)
  void clearFork() => $_clearField(4);
  @$pb.TagNumber(4)
  ForkReceipt ensureFork() => $_ensure(3);

  @$pb.TagNumber(7)
  CommitResponse get commit => $_getN(4);
  @$pb.TagNumber(7)
  set commit(CommitResponse value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasCommit() => $_has(4);
  @$pb.TagNumber(7)
  void clearCommit() => $_clearField(7);
  @$pb.TagNumber(7)
  CommitResponse ensureCommit() => $_ensure(4);
}

class InspectIdempotencyResponse extends $pb.GeneratedMessage {
  factory InspectIdempotencyResponse({
    IdempotencyObservation? observation,
  }) {
    final result = InspectIdempotencyResponse._();
    if (observation != null) result.observation = observation;
    return result;
  }

  InspectIdempotencyResponse._();

  factory InspectIdempotencyResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectIdempotencyResponse()..mergeFromBuffer(data, registry);
  factory InspectIdempotencyResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectIdempotencyResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectIdempotencyResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: InspectIdempotencyResponse.$_createMessage)
    ..aOM<IdempotencyObservation>(1, _omitFieldNames ? '' : 'observation',
        subBuilder: IdempotencyObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectIdempotencyResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectIdempotencyResponse copyWith(
          void Function(InspectIdempotencyResponse) updates) =>
      super.copyWith(
              (message) => updates(message as InspectIdempotencyResponse))
          as InspectIdempotencyResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InspectIdempotencyResponse() / InspectIdempotencyResponse.new instead')
  static InspectIdempotencyResponse create() => InspectIdempotencyResponse._();
  static $pb.GeneratedMessage $_createMessage() =>
      InspectIdempotencyResponse._();
  @$core.override
  InspectIdempotencyResponse createEmptyInstance() =>
      InspectIdempotencyResponse._();
  @$core.pragma('dart2js:noInline')
  static InspectIdempotencyResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectIdempotencyResponse>(
          InspectIdempotencyResponse.$_createMessage);
  static InspectIdempotencyResponse? _defaultInstance;

  @$pb.TagNumber(1)
  IdempotencyObservation get observation => $_getN(0);
  @$pb.TagNumber(1)
  set observation(IdempotencyObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasObservation() => $_has(0);
  @$pb.TagNumber(1)
  void clearObservation() => $_clearField(1);
  @$pb.TagNumber(1)
  IdempotencyObservation ensureObservation() => $_ensure(0);
}

/// Hosted account token creation request.  This uses the same canonical path
/// and operation vocabulary as the Stream client, while the hosted adapter
/// owns its JSON spelling at the Rust/WASM boundary.
class TokenGrant extends $pb.GeneratedMessage {
  factory TokenGrant({
    $core.String? path,
    $core.bool? subtree,
    $core.Iterable<$core.String>? operations,
  }) {
    final result = TokenGrant._();
    if (path != null) result.path = path;
    if (subtree != null) result.subtree = subtree;
    if (operations != null) result.operations.addAll(operations);
    return result;
  }

  TokenGrant._();

  factory TokenGrant.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TokenGrant()..mergeFromBuffer(data, registry);
  factory TokenGrant.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TokenGrant()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TokenGrant',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: TokenGrant.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..aOB(2, _omitFieldNames ? '' : 'subtree')
    ..pPS(3, _omitFieldNames ? '' : 'operations')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TokenGrant clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TokenGrant copyWith(void Function(TokenGrant) updates) =>
      super.copyWith((message) => updates(message as TokenGrant)) as TokenGrant;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TokenGrant() / TokenGrant.new instead')
  static TokenGrant create() => TokenGrant._();
  static $pb.GeneratedMessage $_createMessage() => TokenGrant._();
  @$core.override
  TokenGrant createEmptyInstance() => TokenGrant._();
  @$core.pragma('dart2js:noInline')
  static TokenGrant getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<TokenGrant>(TokenGrant.$_createMessage);
  static TokenGrant? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.bool get subtree => $_getBF(1);
  @$pb.TagNumber(2)
  set subtree($core.bool value) => $_setBool(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSubtree() => $_has(1);
  @$pb.TagNumber(2)
  void clearSubtree() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<$core.String> get operations => $_getList(2);
}

class CreateTokenRequest extends $pb.GeneratedMessage {
  factory CreateTokenRequest({
    $core.String? expiresIn,
    $core.Iterable<TokenGrant>? allow,
  }) {
    final result = CreateTokenRequest._();
    if (expiresIn != null) result.expiresIn = expiresIn;
    if (allow != null) result.allow.addAll(allow);
    return result;
  }

  CreateTokenRequest._();

  factory CreateTokenRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateTokenRequest()..mergeFromBuffer(data, registry);
  factory CreateTokenRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateTokenRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateTokenRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.stream.v2'),
      createEmptyInstance: CreateTokenRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'expiresIn')
    ..pPM<TokenGrant>(2, _omitFieldNames ? '' : 'allow',
        subBuilder: TokenGrant.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateTokenRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateTokenRequest copyWith(void Function(CreateTokenRequest) updates) =>
      super.copyWith((message) => updates(message as CreateTokenRequest))
          as CreateTokenRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CreateTokenRequest() / CreateTokenRequest.new instead')
  static CreateTokenRequest create() => CreateTokenRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateTokenRequest._();
  @$core.override
  CreateTokenRequest createEmptyInstance() => CreateTokenRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateTokenRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateTokenRequest>(
          CreateTokenRequest.$_createMessage);
  static CreateTokenRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get expiresIn => $_getSZ(0);
  @$pb.TagNumber(1)
  set expiresIn($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasExpiresIn() => $_has(0);
  @$pb.TagNumber(1)
  void clearExpiresIn() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<TokenGrant> get allow => $_getList(1);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
