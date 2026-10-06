// This is a generated file - do not edit.
//
// Generated from inference/v1/inference.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'inference.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'inference.pbenum.dart';

class ListModelsRequest extends $pb.GeneratedMessage {
  factory ListModelsRequest() => ListModelsRequest._();

  ListModelsRequest._();

  factory ListModelsRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListModelsRequest()..mergeFromBuffer(data, registry);
  factory ListModelsRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListModelsRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListModelsRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: ListModelsRequest.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListModelsRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListModelsRequest copyWith(void Function(ListModelsRequest) updates) =>
      super.copyWith((message) => updates(message as ListModelsRequest))
          as ListModelsRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ListModelsRequest() / ListModelsRequest.new instead')
  static ListModelsRequest create() => ListModelsRequest._();
  static $pb.GeneratedMessage $_createMessage() => ListModelsRequest._();
  @$core.override
  ListModelsRequest createEmptyInstance() => ListModelsRequest._();
  @$core.pragma('dart2js:noInline')
  static ListModelsRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ListModelsRequest>(
          ListModelsRequest.$_createMessage);
  static ListModelsRequest? _defaultInstance;
}

class ListModelsResponse extends $pb.GeneratedMessage {
  factory ListModelsResponse({
    $core.Iterable<ModelCapability>? models,
  }) {
    final result = ListModelsResponse._();
    if (models != null) result.models.addAll(models);
    return result;
  }

  ListModelsResponse._();

  factory ListModelsResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListModelsResponse()..mergeFromBuffer(data, registry);
  factory ListModelsResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListModelsResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListModelsResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: ListModelsResponse.$_createMessage)
    ..pPM<ModelCapability>(1, _omitFieldNames ? '' : 'models',
        subBuilder: ModelCapability.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListModelsResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListModelsResponse copyWith(void Function(ListModelsResponse) updates) =>
      super.copyWith((message) => updates(message as ListModelsResponse))
          as ListModelsResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ListModelsResponse() / ListModelsResponse.new instead')
  static ListModelsResponse create() => ListModelsResponse._();
  static $pb.GeneratedMessage $_createMessage() => ListModelsResponse._();
  @$core.override
  ListModelsResponse createEmptyInstance() => ListModelsResponse._();
  @$core.pragma('dart2js:noInline')
  static ListModelsResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ListModelsResponse>(
          ListModelsResponse.$_createMessage);
  static ListModelsResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<ModelCapability> get models => $_getList(0);
}

class ModelCapability extends $pb.GeneratedMessage {
  factory ModelCapability({
    $core.String? model,
    $core.List<$core.int>? executionProfile,
    $fixnum.Int64? maximumContext,
    $fixnum.Int64? maximumOutput,
    $core.Iterable<$core.String>? features,
    $core.Iterable<RetentionProfile>? retentionProfiles,
    $core.Iterable<RetentionProfile>? idleKvProfiles,
  }) {
    final result = ModelCapability._();
    if (model != null) result.model = model;
    if (executionProfile != null) result.executionProfile = executionProfile;
    if (maximumContext != null) result.maximumContext = maximumContext;
    if (maximumOutput != null) result.maximumOutput = maximumOutput;
    if (features != null) result.features.addAll(features);
    if (retentionProfiles != null)
      result.retentionProfiles.addAll(retentionProfiles);
    if (idleKvProfiles != null) result.idleKvProfiles.addAll(idleKvProfiles);
    return result;
  }

  ModelCapability._();

  factory ModelCapability.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ModelCapability()..mergeFromBuffer(data, registry);
  factory ModelCapability.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ModelCapability()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ModelCapability',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: ModelCapability.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'model')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'executionProfile', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'maximumContext', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'maximumOutput', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..pPS(5, _omitFieldNames ? '' : 'features')
    ..pPM<RetentionProfile>(6, _omitFieldNames ? '' : 'retentionProfiles',
        subBuilder: RetentionProfile.$_createMessage)
    ..pPM<RetentionProfile>(7, _omitFieldNames ? '' : 'idleKvProfiles',
        subBuilder: RetentionProfile.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ModelCapability clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ModelCapability copyWith(void Function(ModelCapability) updates) =>
      super.copyWith((message) => updates(message as ModelCapability))
          as ModelCapability;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ModelCapability() / ModelCapability.new instead')
  static ModelCapability create() => ModelCapability._();
  static $pb.GeneratedMessage $_createMessage() => ModelCapability._();
  @$core.override
  ModelCapability createEmptyInstance() => ModelCapability._();
  @$core.pragma('dart2js:noInline')
  static ModelCapability getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ModelCapability>(
          ModelCapability.$_createMessage);
  static ModelCapability? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get model => $_getSZ(0);
  @$pb.TagNumber(1)
  set model($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasModel() => $_has(0);
  @$pb.TagNumber(1)
  void clearModel() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get executionProfile => $_getN(1);
  @$pb.TagNumber(2)
  set executionProfile($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasExecutionProfile() => $_has(1);
  @$pb.TagNumber(2)
  void clearExecutionProfile() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get maximumContext => $_getI64(2);
  @$pb.TagNumber(3)
  set maximumContext($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumContext() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumContext() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get maximumOutput => $_getI64(3);
  @$pb.TagNumber(4)
  set maximumOutput($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumOutput() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumOutput() => $_clearField(4);

  @$pb.TagNumber(5)
  $pb.PbList<$core.String> get features => $_getList(4);

  @$pb.TagNumber(6)
  $pb.PbList<RetentionProfile> get retentionProfiles => $_getList(5);

  /// Paid KV pin policies; duration bounds apply to idle_timeout_ms.
  @$pb.TagNumber(7)
  $pb.PbList<RetentionProfile> get idleKvProfiles => $_getList(6);
}

class RetentionProfile extends $pb.GeneratedMessage {
  factory RetentionProfile({
    $core.List<$core.int>? profile,
    $fixnum.Int64? minimumDurationMs,
    $fixnum.Int64? maximumDurationMs,
  }) {
    final result = RetentionProfile._();
    if (profile != null) result.profile = profile;
    if (minimumDurationMs != null) result.minimumDurationMs = minimumDurationMs;
    if (maximumDurationMs != null) result.maximumDurationMs = maximumDurationMs;
    return result;
  }

  RetentionProfile._();

  factory RetentionProfile.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetentionProfile()..mergeFromBuffer(data, registry);
  factory RetentionProfile.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetentionProfile()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RetentionProfile',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RetentionProfile.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'profile', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'minimumDurationMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'maximumDurationMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetentionProfile clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetentionProfile copyWith(void Function(RetentionProfile) updates) =>
      super.copyWith((message) => updates(message as RetentionProfile))
          as RetentionProfile;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RetentionProfile() / RetentionProfile.new instead')
  static RetentionProfile create() => RetentionProfile._();
  static $pb.GeneratedMessage $_createMessage() => RetentionProfile._();
  @$core.override
  RetentionProfile createEmptyInstance() => RetentionProfile._();
  @$core.pragma('dart2js:noInline')
  static RetentionProfile getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RetentionProfile>(
          RetentionProfile.$_createMessage);
  static RetentionProfile? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get profile => $_getN(0);
  @$pb.TagNumber(1)
  set profile($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasProfile() => $_has(0);
  @$pb.TagNumber(1)
  void clearProfile() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get minimumDurationMs => $_getI64(1);
  @$pb.TagNumber(2)
  set minimumDurationMs($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMinimumDurationMs() => $_has(1);
  @$pb.TagNumber(2)
  void clearMinimumDurationMs() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get maximumDurationMs => $_getI64(2);
  @$pb.TagNumber(3)
  set maximumDurationMs($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumDurationMs() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumDurationMs() => $_clearField(3);
}

class RetainWarmRequest extends $pb.GeneratedMessage {
  factory RetainWarmRequest({
    RequestIdentity? identity,
    $core.List<$core.int>? context,
    $core.List<$core.int>? latencyProfile,
    $fixnum.Int64? expiresAtMs,
    IdleKvPolicy? idleKv,
  }) {
    final result = RetainWarmRequest._();
    if (identity != null) result.identity = identity;
    if (context != null) result.context = context;
    if (latencyProfile != null) result.latencyProfile = latencyProfile;
    if (expiresAtMs != null) result.expiresAtMs = expiresAtMs;
    if (idleKv != null) result.idleKv = idleKv;
    return result;
  }

  RetainWarmRequest._();

  factory RetainWarmRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetainWarmRequest()..mergeFromBuffer(data, registry);
  factory RetainWarmRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetainWarmRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RetainWarmRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RetainWarmRequest.$_createMessage)
    ..aOM<RequestIdentity>(1, _omitFieldNames ? '' : 'identity',
        subBuilder: RequestIdentity.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'context', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'latencyProfile', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'expiresAtMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<IdleKvPolicy>(5, _omitFieldNames ? '' : 'idleKv',
        subBuilder: IdleKvPolicy.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetainWarmRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetainWarmRequest copyWith(void Function(RetainWarmRequest) updates) =>
      super.copyWith((message) => updates(message as RetainWarmRequest))
          as RetainWarmRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RetainWarmRequest() / RetainWarmRequest.new instead')
  static RetainWarmRequest create() => RetainWarmRequest._();
  static $pb.GeneratedMessage $_createMessage() => RetainWarmRequest._();
  @$core.override
  RetainWarmRequest createEmptyInstance() => RetainWarmRequest._();
  @$core.pragma('dart2js:noInline')
  static RetainWarmRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RetainWarmRequest>(
          RetainWarmRequest.$_createMessage);
  static RetainWarmRequest? _defaultInstance;

  @$pb.TagNumber(1)
  RequestIdentity get identity => $_getN(0);
  @$pb.TagNumber(1)
  set identity(RequestIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);
  @$pb.TagNumber(1)
  RequestIdentity ensureIdentity() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get context => $_getN(1);
  @$pb.TagNumber(2)
  set context($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasContext() => $_has(1);
  @$pb.TagNumber(2)
  void clearContext() => $_clearField(2);

  /// Legacy absolute-expiry policy. Mutually exclusive with idle_kv.
  @$pb.TagNumber(3)
  $core.List<$core.int> get latencyProfile => $_getN(2);
  @$pb.TagNumber(3)
  set latencyProfile($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLatencyProfile() => $_has(2);
  @$pb.TagNumber(3)
  void clearLatencyProfile() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get expiresAtMs => $_getI64(3);
  @$pb.TagNumber(4)
  set expiresAtMs($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasExpiresAtMs() => $_has(3);
  @$pb.TagNumber(4)
  void clearExpiresAtMs() => $_clearField(4);

  @$pb.TagNumber(5)
  IdleKvPolicy get idleKv => $_getN(4);
  @$pb.TagNumber(5)
  set idleKv(IdleKvPolicy value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasIdleKv() => $_has(4);
  @$pb.TagNumber(5)
  void clearIdleKv() => $_clearField(5);
  @$pb.TagNumber(5)
  IdleKvPolicy ensureIdleKv() => $_ensure(4);
}

/// Paid retention of verified KV, without capacity, throughput or latency guarantees.
/// Only verified actual Run reuse of the pinned revision or descendant prefix
/// advances last-use. Fork, edit, admission, inspect and recovery do not move the
/// pin or reset its idle window. Retried identities return committed receipts.
class IdleKvPolicy extends $pb.GeneratedMessage {
  factory IdleKvPolicy({
    $core.List<$core.int>? profile,
    $fixnum.Int64? idleTimeoutMs,
  }) {
    final result = IdleKvPolicy._();
    if (profile != null) result.profile = profile;
    if (idleTimeoutMs != null) result.idleTimeoutMs = idleTimeoutMs;
    return result;
  }

  IdleKvPolicy._();

  factory IdleKvPolicy.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      IdleKvPolicy()..mergeFromBuffer(data, registry);
  factory IdleKvPolicy.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      IdleKvPolicy()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'IdleKvPolicy',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: IdleKvPolicy.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'profile', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'idleTimeoutMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IdleKvPolicy clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IdleKvPolicy copyWith(void Function(IdleKvPolicy) updates) =>
      super.copyWith((message) => updates(message as IdleKvPolicy))
          as IdleKvPolicy;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use IdleKvPolicy() / IdleKvPolicy.new instead')
  static IdleKvPolicy create() => IdleKvPolicy._();
  static $pb.GeneratedMessage $_createMessage() => IdleKvPolicy._();
  @$core.override
  IdleKvPolicy createEmptyInstance() => IdleKvPolicy._();
  @$core.pragma('dart2js:noInline')
  static IdleKvPolicy getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<IdleKvPolicy>(
          IdleKvPolicy.$_createMessage);
  static IdleKvPolicy? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get profile => $_getN(0);
  @$pb.TagNumber(1)
  set profile($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasProfile() => $_has(0);
  @$pb.TagNumber(1)
  void clearProfile() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get idleTimeoutMs => $_getI64(1);
  @$pb.TagNumber(2)
  set idleTimeoutMs($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdleTimeoutMs() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdleTimeoutMs() => $_clearField(2);
}

class IdleKvRetention extends $pb.GeneratedMessage {
  factory IdleKvRetention({
    IdleKvPolicy? policy,
    $fixnum.Int64? retainedAtMs,
    $fixnum.Int64? lastUsedAtMs,
    $core.List<$core.int>? lastRunId,
  }) {
    final result = IdleKvRetention._();
    if (policy != null) result.policy = policy;
    if (retainedAtMs != null) result.retainedAtMs = retainedAtMs;
    if (lastUsedAtMs != null) result.lastUsedAtMs = lastUsedAtMs;
    if (lastRunId != null) result.lastRunId = lastRunId;
    return result;
  }

  IdleKvRetention._();

  factory IdleKvRetention.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      IdleKvRetention()..mergeFromBuffer(data, registry);
  factory IdleKvRetention.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      IdleKvRetention()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'IdleKvRetention',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: IdleKvRetention.$_createMessage)
    ..aOM<IdleKvPolicy>(1, _omitFieldNames ? '' : 'policy',
        subBuilder: IdleKvPolicy.$_createMessage)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'retainedAtMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'lastUsedAtMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'lastRunId', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IdleKvRetention clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IdleKvRetention copyWith(void Function(IdleKvRetention) updates) =>
      super.copyWith((message) => updates(message as IdleKvRetention))
          as IdleKvRetention;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use IdleKvRetention() / IdleKvRetention.new instead')
  static IdleKvRetention create() => IdleKvRetention._();
  static $pb.GeneratedMessage $_createMessage() => IdleKvRetention._();
  @$core.override
  IdleKvRetention createEmptyInstance() => IdleKvRetention._();
  @$core.pragma('dart2js:noInline')
  static IdleKvRetention getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<IdleKvRetention>(
          IdleKvRetention.$_createMessage);
  static IdleKvRetention? _defaultInstance;

  @$pb.TagNumber(1)
  IdleKvPolicy get policy => $_getN(0);
  @$pb.TagNumber(1)
  set policy(IdleKvPolicy value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasPolicy() => $_has(0);
  @$pb.TagNumber(1)
  void clearPolicy() => $_clearField(1);
  @$pb.TagNumber(1)
  IdleKvPolicy ensurePolicy() => $_ensure(0);

  /// Trusted service Unix milliseconds after verified initial KV pin.
  @$pb.TagNumber(2)
  $fixnum.Int64 get retainedAtMs => $_getI64(1);
  @$pb.TagNumber(2)
  set retainedAtMs($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRetainedAtMs() => $_has(1);
  @$pb.TagNumber(2)
  void clearRetainedAtMs() => $_clearField(2);

  /// Absent until verified actual reuse; never inferred from admission.
  @$pb.TagNumber(3)
  $fixnum.Int64 get lastUsedAtMs => $_getI64(2);
  @$pb.TagNumber(3)
  set lastUsedAtMs($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLastUsedAtMs() => $_has(2);
  @$pb.TagNumber(3)
  void clearLastUsedAtMs() => $_clearField(3);

  /// The authoritative Run that verified actual reuse of this pinned revision
  /// or its descendant prefix, in the same authenticated owner scope.
  @$pb.TagNumber(4)
  $core.List<$core.int> get lastRunId => $_getN(3);
  @$pb.TagNumber(4)
  set lastRunId($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasLastRunId() => $_has(3);
  @$pb.TagNumber(4)
  void clearLastRunId() => $_clearField(4);
}

class InspectWarmRequest extends $pb.GeneratedMessage {
  factory InspectWarmRequest({
    $core.List<$core.int>? commitment,
  }) {
    final result = InspectWarmRequest._();
    if (commitment != null) result.commitment = commitment;
    return result;
  }

  InspectWarmRequest._();

  factory InspectWarmRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectWarmRequest()..mergeFromBuffer(data, registry);
  factory InspectWarmRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectWarmRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectWarmRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: InspectWarmRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'commitment', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectWarmRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectWarmRequest copyWith(void Function(InspectWarmRequest) updates) =>
      super.copyWith((message) => updates(message as InspectWarmRequest))
          as InspectWarmRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InspectWarmRequest() / InspectWarmRequest.new instead')
  static InspectWarmRequest create() => InspectWarmRequest._();
  static $pb.GeneratedMessage $_createMessage() => InspectWarmRequest._();
  @$core.override
  InspectWarmRequest createEmptyInstance() => InspectWarmRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectWarmRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectWarmRequest>(
          InspectWarmRequest.$_createMessage);
  static InspectWarmRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get commitment => $_getN(0);
  @$pb.TagNumber(1)
  set commitment($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCommitment() => $_has(0);
  @$pb.TagNumber(1)
  void clearCommitment() => $_clearField(1);
}

class RenewWarmRequest extends $pb.GeneratedMessage {
  factory RenewWarmRequest({
    RequestIdentity? identity,
    $core.List<$core.int>? commitment,
    $fixnum.Int64? expiresAtMs,
    $fixnum.Int64? idleTimeoutMs,
  }) {
    final result = RenewWarmRequest._();
    if (identity != null) result.identity = identity;
    if (commitment != null) result.commitment = commitment;
    if (expiresAtMs != null) result.expiresAtMs = expiresAtMs;
    if (idleTimeoutMs != null) result.idleTimeoutMs = idleTimeoutMs;
    return result;
  }

  RenewWarmRequest._();

  factory RenewWarmRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RenewWarmRequest()..mergeFromBuffer(data, registry);
  factory RenewWarmRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RenewWarmRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RenewWarmRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RenewWarmRequest.$_createMessage)
    ..aOM<RequestIdentity>(1, _omitFieldNames ? '' : 'identity',
        subBuilder: RequestIdentity.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'commitment', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'expiresAtMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'idleTimeoutMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RenewWarmRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RenewWarmRequest copyWith(void Function(RenewWarmRequest) updates) =>
      super.copyWith((message) => updates(message as RenewWarmRequest))
          as RenewWarmRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RenewWarmRequest() / RenewWarmRequest.new instead')
  static RenewWarmRequest create() => RenewWarmRequest._();
  static $pb.GeneratedMessage $_createMessage() => RenewWarmRequest._();
  @$core.override
  RenewWarmRequest createEmptyInstance() => RenewWarmRequest._();
  @$core.pragma('dart2js:noInline')
  static RenewWarmRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RenewWarmRequest>(
          RenewWarmRequest.$_createMessage);
  static RenewWarmRequest? _defaultInstance;

  @$pb.TagNumber(1)
  RequestIdentity get identity => $_getN(0);
  @$pb.TagNumber(1)
  set identity(RequestIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);
  @$pb.TagNumber(1)
  RequestIdentity ensureIdentity() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get commitment => $_getN(1);
  @$pb.TagNumber(2)
  set commitment($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCommitment() => $_has(1);
  @$pb.TagNumber(2)
  void clearCommitment() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get expiresAtMs => $_getI64(2);
  @$pb.TagNumber(3)
  set expiresAtMs($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasExpiresAtMs() => $_has(2);
  @$pb.TagNumber(3)
  void clearExpiresAtMs() => $_clearField(3);

  /// Changes timeout from last actual use, or retained_at_ms before first use.
  /// Does not reset the idle window. Expired/released pins require a new Retain
  /// identity; renew/replay cannot resurrect them. Inspect reports current state.
  @$pb.TagNumber(4)
  $fixnum.Int64 get idleTimeoutMs => $_getI64(3);
  @$pb.TagNumber(4)
  set idleTimeoutMs($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIdleTimeoutMs() => $_has(3);
  @$pb.TagNumber(4)
  void clearIdleTimeoutMs() => $_clearField(4);
}

class ReleaseWarmRequest extends $pb.GeneratedMessage {
  factory ReleaseWarmRequest({
    RequestIdentity? identity,
    $core.List<$core.int>? commitment,
  }) {
    final result = ReleaseWarmRequest._();
    if (identity != null) result.identity = identity;
    if (commitment != null) result.commitment = commitment;
    return result;
  }

  ReleaseWarmRequest._();

  factory ReleaseWarmRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReleaseWarmRequest()..mergeFromBuffer(data, registry);
  factory ReleaseWarmRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReleaseWarmRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReleaseWarmRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: ReleaseWarmRequest.$_createMessage)
    ..aOM<RequestIdentity>(1, _omitFieldNames ? '' : 'identity',
        subBuilder: RequestIdentity.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'commitment', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReleaseWarmRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReleaseWarmRequest copyWith(void Function(ReleaseWarmRequest) updates) =>
      super.copyWith((message) => updates(message as ReleaseWarmRequest))
          as ReleaseWarmRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ReleaseWarmRequest() / ReleaseWarmRequest.new instead')
  static ReleaseWarmRequest create() => ReleaseWarmRequest._();
  static $pb.GeneratedMessage $_createMessage() => ReleaseWarmRequest._();
  @$core.override
  ReleaseWarmRequest createEmptyInstance() => ReleaseWarmRequest._();
  @$core.pragma('dart2js:noInline')
  static ReleaseWarmRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ReleaseWarmRequest>(
          ReleaseWarmRequest.$_createMessage);
  static ReleaseWarmRequest? _defaultInstance;

  @$pb.TagNumber(1)
  RequestIdentity get identity => $_getN(0);
  @$pb.TagNumber(1)
  set identity(RequestIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);
  @$pb.TagNumber(1)
  RequestIdentity ensureIdentity() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get commitment => $_getN(1);
  @$pb.TagNumber(2)
  set commitment($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCommitment() => $_has(1);
  @$pb.TagNumber(2)
  void clearCommitment() => $_clearField(2);
}

class WarmView extends $pb.GeneratedMessage {
  factory WarmView({
    $core.List<$core.int>? commitment,
    $core.List<$core.int>? context,
    $core.List<$core.int>? modelProfile,
    $core.List<$core.int>? latencyProfile,
    $fixnum.Int64? expiresAtMs,
    WarmState? state,
    $core.List<$core.int>? evidenceDigest,
    $core.List<$core.int>? admissionReceiptId,
    $fixnum.Int64? sequence,
    IdleKvRetention? idleKv,
  }) {
    final result = WarmView._();
    if (commitment != null) result.commitment = commitment;
    if (context != null) result.context = context;
    if (modelProfile != null) result.modelProfile = modelProfile;
    if (latencyProfile != null) result.latencyProfile = latencyProfile;
    if (expiresAtMs != null) result.expiresAtMs = expiresAtMs;
    if (state != null) result.state = state;
    if (evidenceDigest != null) result.evidenceDigest = evidenceDigest;
    if (admissionReceiptId != null)
      result.admissionReceiptId = admissionReceiptId;
    if (sequence != null) result.sequence = sequence;
    if (idleKv != null) result.idleKv = idleKv;
    return result;
  }

  WarmView._();

  factory WarmView.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WarmView()..mergeFromBuffer(data, registry);
  factory WarmView.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WarmView()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WarmView',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: WarmView.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'commitment', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'context', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'modelProfile', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'latencyProfile', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'expiresAtMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aE<WarmState>(6, _omitFieldNames ? '' : 'state',
        enumValues: WarmState.values)
    ..a<$core.List<$core.int>>(
        7, _omitFieldNames ? '' : 'evidenceDigest', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        8, _omitFieldNames ? '' : 'admissionReceiptId', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        9, _omitFieldNames ? '' : 'sequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<IdleKvRetention>(10, _omitFieldNames ? '' : 'idleKv',
        subBuilder: IdleKvRetention.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WarmView clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WarmView copyWith(void Function(WarmView) updates) =>
      super.copyWith((message) => updates(message as WarmView)) as WarmView;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WarmView() / WarmView.new instead')
  static WarmView create() => WarmView._();
  static $pb.GeneratedMessage $_createMessage() => WarmView._();
  @$core.override
  WarmView createEmptyInstance() => WarmView._();
  @$core.pragma('dart2js:noInline')
  static WarmView getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<WarmView>(WarmView.$_createMessage);
  static WarmView? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get commitment => $_getN(0);
  @$pb.TagNumber(1)
  set commitment($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCommitment() => $_has(0);
  @$pb.TagNumber(1)
  void clearCommitment() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get context => $_getN(1);
  @$pb.TagNumber(2)
  set context($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasContext() => $_has(1);
  @$pb.TagNumber(2)
  void clearContext() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get modelProfile => $_getN(2);
  @$pb.TagNumber(3)
  set modelProfile($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasModelProfile() => $_has(2);
  @$pb.TagNumber(3)
  void clearModelProfile() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get latencyProfile => $_getN(3);
  @$pb.TagNumber(4)
  set latencyProfile($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasLatencyProfile() => $_has(3);
  @$pb.TagNumber(4)
  void clearLatencyProfile() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get expiresAtMs => $_getI64(4);
  @$pb.TagNumber(5)
  set expiresAtMs($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasExpiresAtMs() => $_has(4);
  @$pb.TagNumber(5)
  void clearExpiresAtMs() => $_clearField(5);

  @$pb.TagNumber(6)
  WarmState get state => $_getN(5);
  @$pb.TagNumber(6)
  set state(WarmState value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasState() => $_has(5);
  @$pb.TagNumber(6)
  void clearState() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.List<$core.int> get evidenceDigest => $_getN(6);
  @$pb.TagNumber(7)
  set evidenceDigest($core.List<$core.int> value) => $_setBytes(6, value);
  @$pb.TagNumber(7)
  $core.bool hasEvidenceDigest() => $_has(6);
  @$pb.TagNumber(7)
  void clearEvidenceDigest() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.List<$core.int> get admissionReceiptId => $_getN(7);
  @$pb.TagNumber(8)
  set admissionReceiptId($core.List<$core.int> value) => $_setBytes(7, value);
  @$pb.TagNumber(8)
  $core.bool hasAdmissionReceiptId() => $_has(7);
  @$pb.TagNumber(8)
  void clearAdmissionReceiptId() => $_clearField(8);

  @$pb.TagNumber(9)
  $fixnum.Int64 get sequence => $_getI64(8);
  @$pb.TagNumber(9)
  set sequence($fixnum.Int64 value) => $_setInt64(8, value);
  @$pb.TagNumber(9)
  $core.bool hasSequence() => $_has(8);
  @$pb.TagNumber(9)
  void clearSequence() => $_clearField(9);

  /// Present only for idle KV pins; latency_profile is then empty. expires_at_ms
  /// equals checked (last_used_at_ms or retained_at_ms) + idle_timeout_ms.
  @$pb.TagNumber(10)
  IdleKvRetention get idleKv => $_getN(9);
  @$pb.TagNumber(10)
  set idleKv(IdleKvRetention value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasIdleKv() => $_has(9);
  @$pb.TagNumber(10)
  void clearIdleKv() => $_clearField(10);
  @$pb.TagNumber(10)
  IdleKvRetention ensureIdleKv() => $_ensure(9);
}

class EvaluationArtifact extends $pb.GeneratedMessage {
  factory EvaluationArtifact({
    $core.List<$core.int>? digest,
    $core.String? mediaType,
    $fixnum.Int64? logicalSize,
  }) {
    final result = EvaluationArtifact._();
    if (digest != null) result.digest = digest;
    if (mediaType != null) result.mediaType = mediaType;
    if (logicalSize != null) result.logicalSize = logicalSize;
    return result;
  }

  EvaluationArtifact._();

  factory EvaluationArtifact.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationArtifact()..mergeFromBuffer(data, registry);
  factory EvaluationArtifact.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationArtifact()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationArtifact',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationArtifact.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'digest', $pb.PbFieldType.OY)
    ..aOS(2, _omitFieldNames ? '' : 'mediaType')
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'logicalSize', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationArtifact clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationArtifact copyWith(void Function(EvaluationArtifact) updates) =>
      super.copyWith((message) => updates(message as EvaluationArtifact))
          as EvaluationArtifact;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EvaluationArtifact() / EvaluationArtifact.new instead')
  static EvaluationArtifact create() => EvaluationArtifact._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationArtifact._();
  @$core.override
  EvaluationArtifact createEmptyInstance() => EvaluationArtifact._();
  @$core.pragma('dart2js:noInline')
  static EvaluationArtifact getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<EvaluationArtifact>(
          EvaluationArtifact.$_createMessage);
  static EvaluationArtifact? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get digest => $_getN(0);
  @$pb.TagNumber(1)
  set digest($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasDigest() => $_has(0);
  @$pb.TagNumber(1)
  void clearDigest() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get mediaType => $_getSZ(1);
  @$pb.TagNumber(2)
  set mediaType($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMediaType() => $_has(1);
  @$pb.TagNumber(2)
  void clearMediaType() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get logicalSize => $_getI64(2);
  @$pb.TagNumber(3)
  set logicalSize($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLogicalSize() => $_has(2);
  @$pb.TagNumber(3)
  void clearLogicalSize() => $_clearField(3);
}

class EvaluationCase extends $pb.GeneratedMessage {
  factory EvaluationCase({
    $core.List<$core.int>? caseId,
    $core.List<$core.int>? input,
    $core.List<$core.int>? inputArtifactDigest,
  }) {
    final result = EvaluationCase._();
    if (caseId != null) result.caseId = caseId;
    if (input != null) result.input = input;
    if (inputArtifactDigest != null)
      result.inputArtifactDigest = inputArtifactDigest;
    return result;
  }

  EvaluationCase._();

  factory EvaluationCase.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationCase()..mergeFromBuffer(data, registry);
  factory EvaluationCase.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationCase()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationCase',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationCase.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'caseId', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'input', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'inputArtifactDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationCase clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationCase copyWith(void Function(EvaluationCase) updates) =>
      super.copyWith((message) => updates(message as EvaluationCase))
          as EvaluationCase;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EvaluationCase() / EvaluationCase.new instead')
  static EvaluationCase create() => EvaluationCase._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationCase._();
  @$core.override
  EvaluationCase createEmptyInstance() => EvaluationCase._();
  @$core.pragma('dart2js:noInline')
  static EvaluationCase getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EvaluationCase>(
          EvaluationCase.$_createMessage);
  static EvaluationCase? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get caseId => $_getN(0);
  @$pb.TagNumber(1)
  set caseId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCaseId() => $_has(0);
  @$pb.TagNumber(1)
  void clearCaseId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get input => $_getN(1);
  @$pb.TagNumber(2)
  set input($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasInput() => $_has(1);
  @$pb.TagNumber(2)
  void clearInput() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get inputArtifactDigest => $_getN(2);
  @$pb.TagNumber(3)
  set inputArtifactDigest($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasInputArtifactDigest() => $_has(2);
  @$pb.TagNumber(3)
  void clearInputArtifactDigest() => $_clearField(3);
}

class EvaluationSuite extends $pb.GeneratedMessage {
  factory EvaluationSuite({
    $core.String? identity,
    $core.List<$core.int>? digest,
    $core.Iterable<EvaluationCase>? cases,
  }) {
    final result = EvaluationSuite._();
    if (identity != null) result.identity = identity;
    if (digest != null) result.digest = digest;
    if (cases != null) result.cases.addAll(cases);
    return result;
  }

  EvaluationSuite._();

  factory EvaluationSuite.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationSuite()..mergeFromBuffer(data, registry);
  factory EvaluationSuite.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationSuite()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationSuite',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationSuite.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'identity')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'digest', $pb.PbFieldType.OY)
    ..pPM<EvaluationCase>(3, _omitFieldNames ? '' : 'cases',
        subBuilder: EvaluationCase.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationSuite clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationSuite copyWith(void Function(EvaluationSuite) updates) =>
      super.copyWith((message) => updates(message as EvaluationSuite))
          as EvaluationSuite;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EvaluationSuite() / EvaluationSuite.new instead')
  static EvaluationSuite create() => EvaluationSuite._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationSuite._();
  @$core.override
  EvaluationSuite createEmptyInstance() => EvaluationSuite._();
  @$core.pragma('dart2js:noInline')
  static EvaluationSuite getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EvaluationSuite>(
          EvaluationSuite.$_createMessage);
  static EvaluationSuite? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get identity => $_getSZ(0);
  @$pb.TagNumber(1)
  set identity($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get digest => $_getN(1);
  @$pb.TagNumber(2)
  set digest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearDigest() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<EvaluationCase> get cases => $_getList(2);
}

class EvaluationGrader extends $pb.GeneratedMessage {
  factory EvaluationGrader({
    $core.List<$core.int>? handle,
    $core.List<$core.int>? artifactDigest,
  }) {
    final result = EvaluationGrader._();
    if (handle != null) result.handle = handle;
    if (artifactDigest != null) result.artifactDigest = artifactDigest;
    return result;
  }

  EvaluationGrader._();

  factory EvaluationGrader.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationGrader()..mergeFromBuffer(data, registry);
  factory EvaluationGrader.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationGrader()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationGrader',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationGrader.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'handle', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'artifactDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationGrader clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationGrader copyWith(void Function(EvaluationGrader) updates) =>
      super.copyWith((message) => updates(message as EvaluationGrader))
          as EvaluationGrader;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EvaluationGrader() / EvaluationGrader.new instead')
  static EvaluationGrader create() => EvaluationGrader._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationGrader._();
  @$core.override
  EvaluationGrader createEmptyInstance() => EvaluationGrader._();
  @$core.pragma('dart2js:noInline')
  static EvaluationGrader getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EvaluationGrader>(
          EvaluationGrader.$_createMessage);
  static EvaluationGrader? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get handle => $_getN(0);
  @$pb.TagNumber(1)
  set handle($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasHandle() => $_has(0);
  @$pb.TagNumber(1)
  void clearHandle() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get artifactDigest => $_getN(1);
  @$pb.TagNumber(2)
  set artifactDigest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasArtifactDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearArtifactDigest() => $_clearField(2);
}

class EvaluationMetric extends $pb.GeneratedMessage {
  factory EvaluationMetric({
    $core.String? identity,
    EvaluationAggregation? aggregation,
  }) {
    final result = EvaluationMetric._();
    if (identity != null) result.identity = identity;
    if (aggregation != null) result.aggregation = aggregation;
    return result;
  }

  EvaluationMetric._();

  factory EvaluationMetric.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationMetric()..mergeFromBuffer(data, registry);
  factory EvaluationMetric.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationMetric()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationMetric',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationMetric.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'identity')
    ..aE<EvaluationAggregation>(2, _omitFieldNames ? '' : 'aggregation',
        enumValues: EvaluationAggregation.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationMetric clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationMetric copyWith(void Function(EvaluationMetric) updates) =>
      super.copyWith((message) => updates(message as EvaluationMetric))
          as EvaluationMetric;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EvaluationMetric() / EvaluationMetric.new instead')
  static EvaluationMetric create() => EvaluationMetric._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationMetric._();
  @$core.override
  EvaluationMetric createEmptyInstance() => EvaluationMetric._();
  @$core.pragma('dart2js:noInline')
  static EvaluationMetric getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EvaluationMetric>(
          EvaluationMetric.$_createMessage);
  static EvaluationMetric? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get identity => $_getSZ(0);
  @$pb.TagNumber(1)
  set identity($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);

  @$pb.TagNumber(2)
  EvaluationAggregation get aggregation => $_getN(1);
  @$pb.TagNumber(2)
  set aggregation(EvaluationAggregation value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAggregation() => $_has(1);
  @$pb.TagNumber(2)
  void clearAggregation() => $_clearField(2);
}

class EvaluationSpec extends $pb.GeneratedMessage {
  factory EvaluationSpec({
    $core.Iterable<EvaluationArtifact>? candidates,
    EvaluationSuite? suite,
    EvaluationGrader? grader,
    $core.Iterable<EvaluationMetric>? metrics,
    $fixnum.Int64? maximumCaseResults,
    $core.List<$core.int>? specDigest,
  }) {
    final result = EvaluationSpec._();
    if (candidates != null) result.candidates.addAll(candidates);
    if (suite != null) result.suite = suite;
    if (grader != null) result.grader = grader;
    if (metrics != null) result.metrics.addAll(metrics);
    if (maximumCaseResults != null)
      result.maximumCaseResults = maximumCaseResults;
    if (specDigest != null) result.specDigest = specDigest;
    return result;
  }

  EvaluationSpec._();

  factory EvaluationSpec.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationSpec()..mergeFromBuffer(data, registry);
  factory EvaluationSpec.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationSpec()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationSpec',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationSpec.$_createMessage)
    ..pPM<EvaluationArtifact>(1, _omitFieldNames ? '' : 'candidates',
        subBuilder: EvaluationArtifact.$_createMessage)
    ..aOM<EvaluationSuite>(2, _omitFieldNames ? '' : 'suite',
        subBuilder: EvaluationSuite.$_createMessage)
    ..aOM<EvaluationGrader>(3, _omitFieldNames ? '' : 'grader',
        subBuilder: EvaluationGrader.$_createMessage)
    ..pPM<EvaluationMetric>(4, _omitFieldNames ? '' : 'metrics',
        subBuilder: EvaluationMetric.$_createMessage)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'maximumCaseResults', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        6, _omitFieldNames ? '' : 'specDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationSpec clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationSpec copyWith(void Function(EvaluationSpec) updates) =>
      super.copyWith((message) => updates(message as EvaluationSpec))
          as EvaluationSpec;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EvaluationSpec() / EvaluationSpec.new instead')
  static EvaluationSpec create() => EvaluationSpec._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationSpec._();
  @$core.override
  EvaluationSpec createEmptyInstance() => EvaluationSpec._();
  @$core.pragma('dart2js:noInline')
  static EvaluationSpec getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EvaluationSpec>(
          EvaluationSpec.$_createMessage);
  static EvaluationSpec? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<EvaluationArtifact> get candidates => $_getList(0);

  @$pb.TagNumber(2)
  EvaluationSuite get suite => $_getN(1);
  @$pb.TagNumber(2)
  set suite(EvaluationSuite value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSuite() => $_has(1);
  @$pb.TagNumber(2)
  void clearSuite() => $_clearField(2);
  @$pb.TagNumber(2)
  EvaluationSuite ensureSuite() => $_ensure(1);

  @$pb.TagNumber(3)
  EvaluationGrader get grader => $_getN(2);
  @$pb.TagNumber(3)
  set grader(EvaluationGrader value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasGrader() => $_has(2);
  @$pb.TagNumber(3)
  void clearGrader() => $_clearField(3);
  @$pb.TagNumber(3)
  EvaluationGrader ensureGrader() => $_ensure(2);

  @$pb.TagNumber(4)
  $pb.PbList<EvaluationMetric> get metrics => $_getList(3);

  @$pb.TagNumber(5)
  $fixnum.Int64 get maximumCaseResults => $_getI64(4);
  @$pb.TagNumber(5)
  set maximumCaseResults($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasMaximumCaseResults() => $_has(4);
  @$pb.TagNumber(5)
  void clearMaximumCaseResults() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.List<$core.int> get specDigest => $_getN(5);
  @$pb.TagNumber(6)
  set specDigest($core.List<$core.int> value) => $_setBytes(5, value);
  @$pb.TagNumber(6)
  $core.bool hasSpecDigest() => $_has(5);
  @$pb.TagNumber(6)
  void clearSpecDigest() => $_clearField(6);
}

class CreateEvaluationRequest extends $pb.GeneratedMessage {
  factory CreateEvaluationRequest({
    RequestIdentity? identity,
    EvaluationSpec? spec,
  }) {
    final result = CreateEvaluationRequest._();
    if (identity != null) result.identity = identity;
    if (spec != null) result.spec = spec;
    return result;
  }

  CreateEvaluationRequest._();

  factory CreateEvaluationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateEvaluationRequest()..mergeFromBuffer(data, registry);
  factory CreateEvaluationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateEvaluationRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateEvaluationRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: CreateEvaluationRequest.$_createMessage)
    ..aOM<RequestIdentity>(1, _omitFieldNames ? '' : 'identity',
        subBuilder: RequestIdentity.$_createMessage)
    ..aOM<EvaluationSpec>(2, _omitFieldNames ? '' : 'spec',
        subBuilder: EvaluationSpec.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateEvaluationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateEvaluationRequest copyWith(
          void Function(CreateEvaluationRequest) updates) =>
      super.copyWith((message) => updates(message as CreateEvaluationRequest))
          as CreateEvaluationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CreateEvaluationRequest() / CreateEvaluationRequest.new instead')
  static CreateEvaluationRequest create() => CreateEvaluationRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateEvaluationRequest._();
  @$core.override
  CreateEvaluationRequest createEmptyInstance() => CreateEvaluationRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateEvaluationRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateEvaluationRequest>(
          CreateEvaluationRequest.$_createMessage);
  static CreateEvaluationRequest? _defaultInstance;

  @$pb.TagNumber(1)
  RequestIdentity get identity => $_getN(0);
  @$pb.TagNumber(1)
  set identity(RequestIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);
  @$pb.TagNumber(1)
  RequestIdentity ensureIdentity() => $_ensure(0);

  @$pb.TagNumber(2)
  EvaluationSpec get spec => $_getN(1);
  @$pb.TagNumber(2)
  set spec(EvaluationSpec value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSpec() => $_has(1);
  @$pb.TagNumber(2)
  void clearSpec() => $_clearField(2);
  @$pb.TagNumber(2)
  EvaluationSpec ensureSpec() => $_ensure(1);
}

class InspectEvaluationRequest extends $pb.GeneratedMessage {
  factory InspectEvaluationRequest({
    $core.List<$core.int>? evaluationId,
  }) {
    final result = InspectEvaluationRequest._();
    if (evaluationId != null) result.evaluationId = evaluationId;
    return result;
  }

  InspectEvaluationRequest._();

  factory InspectEvaluationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectEvaluationRequest()..mergeFromBuffer(data, registry);
  factory InspectEvaluationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectEvaluationRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectEvaluationRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: InspectEvaluationRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'evaluationId', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectEvaluationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectEvaluationRequest copyWith(
          void Function(InspectEvaluationRequest) updates) =>
      super.copyWith((message) => updates(message as InspectEvaluationRequest))
          as InspectEvaluationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InspectEvaluationRequest() / InspectEvaluationRequest.new instead')
  static InspectEvaluationRequest create() => InspectEvaluationRequest._();
  static $pb.GeneratedMessage $_createMessage() => InspectEvaluationRequest._();
  @$core.override
  InspectEvaluationRequest createEmptyInstance() =>
      InspectEvaluationRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectEvaluationRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectEvaluationRequest>(
          InspectEvaluationRequest.$_createMessage);
  static InspectEvaluationRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get evaluationId => $_getN(0);
  @$pb.TagNumber(1)
  set evaluationId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasEvaluationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearEvaluationId() => $_clearField(1);
}

class ExactRational extends $pb.GeneratedMessage {
  factory ExactRational({
    $fixnum.Int64? numerator,
    $fixnum.Int64? denominator,
  }) {
    final result = ExactRational._();
    if (numerator != null) result.numerator = numerator;
    if (denominator != null) result.denominator = denominator;
    return result;
  }

  ExactRational._();

  factory ExactRational.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExactRational()..mergeFromBuffer(data, registry);
  factory ExactRational.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExactRational()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExactRational',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: ExactRational.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'numerator', $pb.PbFieldType.OS6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'denominator', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExactRational clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExactRational copyWith(void Function(ExactRational) updates) =>
      super.copyWith((message) => updates(message as ExactRational))
          as ExactRational;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExactRational() / ExactRational.new instead')
  static ExactRational create() => ExactRational._();
  static $pb.GeneratedMessage $_createMessage() => ExactRational._();
  @$core.override
  ExactRational createEmptyInstance() => ExactRational._();
  @$core.pragma('dart2js:noInline')
  static ExactRational getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ExactRational>(
          ExactRational.$_createMessage);
  static ExactRational? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get numerator => $_getI64(0);
  @$pb.TagNumber(1)
  set numerator($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasNumerator() => $_has(0);
  @$pb.TagNumber(1)
  void clearNumerator() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get denominator => $_getI64(1);
  @$pb.TagNumber(2)
  set denominator($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDenominator() => $_has(1);
  @$pb.TagNumber(2)
  void clearDenominator() => $_clearField(2);
}

class EvaluationMetricValue extends $pb.GeneratedMessage {
  factory EvaluationMetricValue({
    $core.String? metricIdentity,
    ExactRational? value,
  }) {
    final result = EvaluationMetricValue._();
    if (metricIdentity != null) result.metricIdentity = metricIdentity;
    if (value != null) result.value = value;
    return result;
  }

  EvaluationMetricValue._();

  factory EvaluationMetricValue.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationMetricValue()..mergeFromBuffer(data, registry);
  factory EvaluationMetricValue.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationMetricValue()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationMetricValue',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationMetricValue.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'metricIdentity')
    ..aOM<ExactRational>(2, _omitFieldNames ? '' : 'value',
        subBuilder: ExactRational.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationMetricValue clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationMetricValue copyWith(
          void Function(EvaluationMetricValue) updates) =>
      super.copyWith((message) => updates(message as EvaluationMetricValue))
          as EvaluationMetricValue;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use EvaluationMetricValue() / EvaluationMetricValue.new instead')
  static EvaluationMetricValue create() => EvaluationMetricValue._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationMetricValue._();
  @$core.override
  EvaluationMetricValue createEmptyInstance() => EvaluationMetricValue._();
  @$core.pragma('dart2js:noInline')
  static EvaluationMetricValue getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<EvaluationMetricValue>(
          EvaluationMetricValue.$_createMessage);
  static EvaluationMetricValue? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get metricIdentity => $_getSZ(0);
  @$pb.TagNumber(1)
  set metricIdentity($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasMetricIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearMetricIdentity() => $_clearField(1);

  @$pb.TagNumber(2)
  ExactRational get value => $_getN(1);
  @$pb.TagNumber(2)
  set value(ExactRational value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasValue() => $_has(1);
  @$pb.TagNumber(2)
  void clearValue() => $_clearField(2);
  @$pb.TagNumber(2)
  ExactRational ensureValue() => $_ensure(1);
}

class EvaluationCaseResult extends $pb.GeneratedMessage {
  factory EvaluationCaseResult({
    $core.List<$core.int>? candidateDigest,
    $core.List<$core.int>? caseId,
    EvaluationGraderObservation? observation,
    $core.Iterable<EvaluationMetricValue>? metrics,
    EvaluationCaseOutcome? outcome,
  }) {
    final result = EvaluationCaseResult._();
    if (candidateDigest != null) result.candidateDigest = candidateDigest;
    if (caseId != null) result.caseId = caseId;
    if (observation != null) result.observation = observation;
    if (metrics != null) result.metrics.addAll(metrics);
    if (outcome != null) result.outcome = outcome;
    return result;
  }

  EvaluationCaseResult._();

  factory EvaluationCaseResult.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationCaseResult()..mergeFromBuffer(data, registry);
  factory EvaluationCaseResult.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationCaseResult()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationCaseResult',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationCaseResult.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'candidateDigest', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'caseId', $pb.PbFieldType.OY)
    ..aOM<EvaluationGraderObservation>(3, _omitFieldNames ? '' : 'observation',
        subBuilder: EvaluationGraderObservation.$_createMessage)
    ..pPM<EvaluationMetricValue>(4, _omitFieldNames ? '' : 'metrics',
        subBuilder: EvaluationMetricValue.$_createMessage)
    ..aE<EvaluationCaseOutcome>(5, _omitFieldNames ? '' : 'outcome',
        enumValues: EvaluationCaseOutcome.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationCaseResult clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationCaseResult copyWith(void Function(EvaluationCaseResult) updates) =>
      super.copyWith((message) => updates(message as EvaluationCaseResult))
          as EvaluationCaseResult;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use EvaluationCaseResult() / EvaluationCaseResult.new instead')
  static EvaluationCaseResult create() => EvaluationCaseResult._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationCaseResult._();
  @$core.override
  EvaluationCaseResult createEmptyInstance() => EvaluationCaseResult._();
  @$core.pragma('dart2js:noInline')
  static EvaluationCaseResult getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<EvaluationCaseResult>(
          EvaluationCaseResult.$_createMessage);
  static EvaluationCaseResult? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get candidateDigest => $_getN(0);
  @$pb.TagNumber(1)
  set candidateDigest($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCandidateDigest() => $_has(0);
  @$pb.TagNumber(1)
  void clearCandidateDigest() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get caseId => $_getN(1);
  @$pb.TagNumber(2)
  set caseId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCaseId() => $_has(1);
  @$pb.TagNumber(2)
  void clearCaseId() => $_clearField(2);

  @$pb.TagNumber(3)
  EvaluationGraderObservation get observation => $_getN(2);
  @$pb.TagNumber(3)
  set observation(EvaluationGraderObservation value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasObservation() => $_has(2);
  @$pb.TagNumber(3)
  void clearObservation() => $_clearField(3);
  @$pb.TagNumber(3)
  EvaluationGraderObservation ensureObservation() => $_ensure(2);

  @$pb.TagNumber(4)
  $pb.PbList<EvaluationMetricValue> get metrics => $_getList(3);

  @$pb.TagNumber(5)
  EvaluationCaseOutcome get outcome => $_getN(4);
  @$pb.TagNumber(5)
  set outcome(EvaluationCaseOutcome value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasOutcome() => $_has(4);
  @$pb.TagNumber(5)
  void clearOutcome() => $_clearField(5);
}

/// The binding is SHA-256("acyclic.inference.grader-observation.v1\0" ||
/// native_output_digest || observation_digest). It proves which exact native
/// device output the grader observed without exposing either payload.
class EvaluationGraderObservation extends $pb.GeneratedMessage {
  factory EvaluationGraderObservation({
    $core.List<$core.int>? nativeOutputDigest,
    $core.List<$core.int>? observationDigest,
    $core.List<$core.int>? bindingDigest,
  }) {
    final result = EvaluationGraderObservation._();
    if (nativeOutputDigest != null)
      result.nativeOutputDigest = nativeOutputDigest;
    if (observationDigest != null) result.observationDigest = observationDigest;
    if (bindingDigest != null) result.bindingDigest = bindingDigest;
    return result;
  }

  EvaluationGraderObservation._();

  factory EvaluationGraderObservation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationGraderObservation()..mergeFromBuffer(data, registry);
  factory EvaluationGraderObservation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationGraderObservation()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationGraderObservation',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationGraderObservation.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'nativeOutputDigest', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'observationDigest', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'bindingDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationGraderObservation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationGraderObservation copyWith(
          void Function(EvaluationGraderObservation) updates) =>
      super.copyWith(
              (message) => updates(message as EvaluationGraderObservation))
          as EvaluationGraderObservation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use EvaluationGraderObservation() / EvaluationGraderObservation.new instead')
  static EvaluationGraderObservation create() =>
      EvaluationGraderObservation._();
  static $pb.GeneratedMessage $_createMessage() =>
      EvaluationGraderObservation._();
  @$core.override
  EvaluationGraderObservation createEmptyInstance() =>
      EvaluationGraderObservation._();
  @$core.pragma('dart2js:noInline')
  static EvaluationGraderObservation getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<EvaluationGraderObservation>(
          EvaluationGraderObservation.$_createMessage);
  static EvaluationGraderObservation? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get nativeOutputDigest => $_getN(0);
  @$pb.TagNumber(1)
  set nativeOutputDigest($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasNativeOutputDigest() => $_has(0);
  @$pb.TagNumber(1)
  void clearNativeOutputDigest() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get observationDigest => $_getN(1);
  @$pb.TagNumber(2)
  set observationDigest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObservationDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearObservationDigest() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get bindingDigest => $_getN(2);
  @$pb.TagNumber(3)
  set bindingDigest($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasBindingDigest() => $_has(2);
  @$pb.TagNumber(3)
  void clearBindingDigest() => $_clearField(3);
}

class EvaluationAggregate extends $pb.GeneratedMessage {
  factory EvaluationAggregate({
    $core.List<$core.int>? candidateDigest,
    $core.String? metricIdentity,
    EvaluationAggregation? aggregation,
    ExactRational? value,
  }) {
    final result = EvaluationAggregate._();
    if (candidateDigest != null) result.candidateDigest = candidateDigest;
    if (metricIdentity != null) result.metricIdentity = metricIdentity;
    if (aggregation != null) result.aggregation = aggregation;
    if (value != null) result.value = value;
    return result;
  }

  EvaluationAggregate._();

  factory EvaluationAggregate.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationAggregate()..mergeFromBuffer(data, registry);
  factory EvaluationAggregate.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationAggregate()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationAggregate',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationAggregate.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'candidateDigest', $pb.PbFieldType.OY)
    ..aOS(2, _omitFieldNames ? '' : 'metricIdentity')
    ..aE<EvaluationAggregation>(3, _omitFieldNames ? '' : 'aggregation',
        enumValues: EvaluationAggregation.values)
    ..aOM<ExactRational>(4, _omitFieldNames ? '' : 'value',
        subBuilder: ExactRational.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationAggregate clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationAggregate copyWith(void Function(EvaluationAggregate) updates) =>
      super.copyWith((message) => updates(message as EvaluationAggregate))
          as EvaluationAggregate;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use EvaluationAggregate() / EvaluationAggregate.new instead')
  static EvaluationAggregate create() => EvaluationAggregate._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationAggregate._();
  @$core.override
  EvaluationAggregate createEmptyInstance() => EvaluationAggregate._();
  @$core.pragma('dart2js:noInline')
  static EvaluationAggregate getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<EvaluationAggregate>(
          EvaluationAggregate.$_createMessage);
  static EvaluationAggregate? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get candidateDigest => $_getN(0);
  @$pb.TagNumber(1)
  set candidateDigest($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCandidateDigest() => $_has(0);
  @$pb.TagNumber(1)
  void clearCandidateDigest() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get metricIdentity => $_getSZ(1);
  @$pb.TagNumber(2)
  set metricIdentity($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMetricIdentity() => $_has(1);
  @$pb.TagNumber(2)
  void clearMetricIdentity() => $_clearField(2);

  @$pb.TagNumber(3)
  EvaluationAggregation get aggregation => $_getN(2);
  @$pb.TagNumber(3)
  set aggregation(EvaluationAggregation value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasAggregation() => $_has(2);
  @$pb.TagNumber(3)
  void clearAggregation() => $_clearField(3);

  @$pb.TagNumber(4)
  ExactRational get value => $_getN(3);
  @$pb.TagNumber(4)
  set value(ExactRational value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasValue() => $_has(3);
  @$pb.TagNumber(4)
  void clearValue() => $_clearField(4);
  @$pb.TagNumber(4)
  ExactRational ensureValue() => $_ensure(3);
}

class EvaluationResult extends $pb.GeneratedMessage {
  factory EvaluationResult({
    $core.List<$core.int>? specDigest,
    $core.Iterable<EvaluationCaseResult>? caseResults,
    $core.Iterable<EvaluationAggregate>? aggregates,
    $core.List<$core.int>? resultDigest,
  }) {
    final result = EvaluationResult._();
    if (specDigest != null) result.specDigest = specDigest;
    if (caseResults != null) result.caseResults.addAll(caseResults);
    if (aggregates != null) result.aggregates.addAll(aggregates);
    if (resultDigest != null) result.resultDigest = resultDigest;
    return result;
  }

  EvaluationResult._();

  factory EvaluationResult.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationResult()..mergeFromBuffer(data, registry);
  factory EvaluationResult.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationResult()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationResult',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationResult.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'specDigest', $pb.PbFieldType.OY)
    ..pPM<EvaluationCaseResult>(2, _omitFieldNames ? '' : 'caseResults',
        subBuilder: EvaluationCaseResult.$_createMessage)
    ..pPM<EvaluationAggregate>(3, _omitFieldNames ? '' : 'aggregates',
        subBuilder: EvaluationAggregate.$_createMessage)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'resultDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationResult clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationResult copyWith(void Function(EvaluationResult) updates) =>
      super.copyWith((message) => updates(message as EvaluationResult))
          as EvaluationResult;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EvaluationResult() / EvaluationResult.new instead')
  static EvaluationResult create() => EvaluationResult._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationResult._();
  @$core.override
  EvaluationResult createEmptyInstance() => EvaluationResult._();
  @$core.pragma('dart2js:noInline')
  static EvaluationResult getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EvaluationResult>(
          EvaluationResult.$_createMessage);
  static EvaluationResult? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get specDigest => $_getN(0);
  @$pb.TagNumber(1)
  set specDigest($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSpecDigest() => $_has(0);
  @$pb.TagNumber(1)
  void clearSpecDigest() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<EvaluationCaseResult> get caseResults => $_getList(1);

  @$pb.TagNumber(3)
  $pb.PbList<EvaluationAggregate> get aggregates => $_getList(2);

  @$pb.TagNumber(4)
  $core.List<$core.int> get resultDigest => $_getN(3);
  @$pb.TagNumber(4)
  set resultDigest($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasResultDigest() => $_has(3);
  @$pb.TagNumber(4)
  void clearResultDigest() => $_clearField(4);
}

class EvaluationView extends $pb.GeneratedMessage {
  factory EvaluationView({
    $core.List<$core.int>? evaluationId,
    EvaluationSpec? spec,
    EvaluationState? state,
    EvaluationResult? result,
    $fixnum.Int64? sequence,
  }) {
    final result$ = EvaluationView._();
    if (evaluationId != null) result$.evaluationId = evaluationId;
    if (spec != null) result$.spec = spec;
    if (state != null) result$.state = state;
    if (result != null) result$.result = result;
    if (sequence != null) result$.sequence = sequence;
    return result$;
  }

  EvaluationView._();

  factory EvaluationView.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationView()..mergeFromBuffer(data, registry);
  factory EvaluationView.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EvaluationView()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EvaluationView',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: EvaluationView.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'evaluationId', $pb.PbFieldType.OY)
    ..aOM<EvaluationSpec>(2, _omitFieldNames ? '' : 'spec',
        subBuilder: EvaluationSpec.$_createMessage)
    ..aE<EvaluationState>(3, _omitFieldNames ? '' : 'state',
        enumValues: EvaluationState.values)
    ..aOM<EvaluationResult>(4, _omitFieldNames ? '' : 'result',
        subBuilder: EvaluationResult.$_createMessage)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'sequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationView clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EvaluationView copyWith(void Function(EvaluationView) updates) =>
      super.copyWith((message) => updates(message as EvaluationView))
          as EvaluationView;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EvaluationView() / EvaluationView.new instead')
  static EvaluationView create() => EvaluationView._();
  static $pb.GeneratedMessage $_createMessage() => EvaluationView._();
  @$core.override
  EvaluationView createEmptyInstance() => EvaluationView._();
  @$core.pragma('dart2js:noInline')
  static EvaluationView getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EvaluationView>(
          EvaluationView.$_createMessage);
  static EvaluationView? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get evaluationId => $_getN(0);
  @$pb.TagNumber(1)
  set evaluationId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasEvaluationId() => $_has(0);
  @$pb.TagNumber(1)
  void clearEvaluationId() => $_clearField(1);

  @$pb.TagNumber(2)
  EvaluationSpec get spec => $_getN(1);
  @$pb.TagNumber(2)
  set spec(EvaluationSpec value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSpec() => $_has(1);
  @$pb.TagNumber(2)
  void clearSpec() => $_clearField(2);
  @$pb.TagNumber(2)
  EvaluationSpec ensureSpec() => $_ensure(1);

  @$pb.TagNumber(3)
  EvaluationState get state => $_getN(2);
  @$pb.TagNumber(3)
  set state(EvaluationState value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasState() => $_has(2);
  @$pb.TagNumber(3)
  void clearState() => $_clearField(3);

  @$pb.TagNumber(4)
  EvaluationResult get result => $_getN(3);
  @$pb.TagNumber(4)
  set result(EvaluationResult value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasResult() => $_has(3);
  @$pb.TagNumber(4)
  void clearResult() => $_clearField(4);
  @$pb.TagNumber(4)
  EvaluationResult ensureResult() => $_ensure(3);

  @$pb.TagNumber(5)
  $fixnum.Int64 get sequence => $_getI64(4);
  @$pb.TagNumber(5)
  set sequence($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasSequence() => $_has(4);
  @$pb.TagNumber(5)
  void clearSequence() => $_clearField(5);
}

class RequestIdentity extends $pb.GeneratedMessage {
  factory RequestIdentity({
    $core.List<$core.int>? clientInstance,
    $core.List<$core.int>? requestId,
  }) {
    final result = RequestIdentity._();
    if (clientInstance != null) result.clientInstance = clientInstance;
    if (requestId != null) result.requestId = requestId;
    return result;
  }

  RequestIdentity._();

  factory RequestIdentity.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RequestIdentity()..mergeFromBuffer(data, registry);
  factory RequestIdentity.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RequestIdentity()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RequestIdentity',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RequestIdentity.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'clientInstance', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'requestId', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RequestIdentity clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RequestIdentity copyWith(void Function(RequestIdentity) updates) =>
      super.copyWith((message) => updates(message as RequestIdentity))
          as RequestIdentity;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RequestIdentity() / RequestIdentity.new instead')
  static RequestIdentity create() => RequestIdentity._();
  static $pb.GeneratedMessage $_createMessage() => RequestIdentity._();
  @$core.override
  RequestIdentity createEmptyInstance() => RequestIdentity._();
  @$core.pragma('dart2js:noInline')
  static RequestIdentity getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RequestIdentity>(
          RequestIdentity.$_createMessage);
  static RequestIdentity? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get clientInstance => $_getN(0);
  @$pb.TagNumber(1)
  set clientInstance($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasClientInstance() => $_has(0);
  @$pb.TagNumber(1)
  void clearClientInstance() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get requestId => $_getN(1);
  @$pb.TagNumber(2)
  set requestId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRequestId() => $_has(1);
  @$pb.TagNumber(2)
  void clearRequestId() => $_clearField(2);
}

class Item extends $pb.GeneratedMessage {
  factory Item({
    $core.List<$core.int>? id,
    ItemKind? kind,
    $core.List<$core.int>? payload,
    $core.List<$core.int>? link,
    $core.List<$core.int>? continuationProfile,
  }) {
    final result = Item._();
    if (id != null) result.id = id;
    if (kind != null) result.kind = kind;
    if (payload != null) result.payload = payload;
    if (link != null) result.link = link;
    if (continuationProfile != null)
      result.continuationProfile = continuationProfile;
    return result;
  }

  Item._();

  factory Item.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Item()..mergeFromBuffer(data, registry);
  factory Item.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Item()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Item',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Item.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'id', $pb.PbFieldType.OY)
    ..aE<ItemKind>(2, _omitFieldNames ? '' : 'kind',
        enumValues: ItemKind.values)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'payload', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'link', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'continuationProfile', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Item clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Item copyWith(void Function(Item) updates) =>
      super.copyWith((message) => updates(message as Item)) as Item;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Item() / Item.new instead')
  static Item create() => Item._();
  static $pb.GeneratedMessage $_createMessage() => Item._();
  @$core.override
  Item createEmptyInstance() => Item._();
  @$core.pragma('dart2js:noInline')
  static Item getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Item>(Item.$_createMessage);
  static Item? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get id => $_getN(0);
  @$pb.TagNumber(1)
  set id($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasId() => $_has(0);
  @$pb.TagNumber(1)
  void clearId() => $_clearField(1);

  @$pb.TagNumber(2)
  ItemKind get kind => $_getN(1);
  @$pb.TagNumber(2)
  set kind(ItemKind value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasKind() => $_has(1);
  @$pb.TagNumber(2)
  void clearKind() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get payload => $_getN(2);
  @$pb.TagNumber(3)
  set payload($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasPayload() => $_has(2);
  @$pb.TagNumber(3)
  void clearPayload() => $_clearField(3);

  /// Exactly one logical link for tool call/result; absent for all other kinds.
  @$pb.TagNumber(4)
  $core.List<$core.int> get link => $_getN(3);
  @$pb.TagNumber(4)
  set link($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasLink() => $_has(3);
  @$pb.TagNumber(4)
  void clearLink() => $_clearField(4);

  /// Only a continuation may contain its exact originating execution profile.
  @$pb.TagNumber(5)
  $core.List<$core.int> get continuationProfile => $_getN(4);
  @$pb.TagNumber(5)
  set continuationProfile($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasContinuationProfile() => $_has(4);
  @$pb.TagNumber(5)
  void clearContinuationProfile() => $_clearField(5);
}

class CreateContextRequest extends $pb.GeneratedMessage {
  factory CreateContextRequest({
    RequestIdentity? identity,
    $core.String? model,
    $core.Iterable<Item>? items,
  }) {
    final result = CreateContextRequest._();
    if (identity != null) result.identity = identity;
    if (model != null) result.model = model;
    if (items != null) result.items.addAll(items);
    return result;
  }

  CreateContextRequest._();

  factory CreateContextRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateContextRequest()..mergeFromBuffer(data, registry);
  factory CreateContextRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateContextRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateContextRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: CreateContextRequest.$_createMessage)
    ..aOM<RequestIdentity>(1, _omitFieldNames ? '' : 'identity',
        subBuilder: RequestIdentity.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'model')
    ..pPM<Item>(3, _omitFieldNames ? '' : 'items',
        subBuilder: Item.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateContextRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateContextRequest copyWith(void Function(CreateContextRequest) updates) =>
      super.copyWith((message) => updates(message as CreateContextRequest))
          as CreateContextRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CreateContextRequest() / CreateContextRequest.new instead')
  static CreateContextRequest create() => CreateContextRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateContextRequest._();
  @$core.override
  CreateContextRequest createEmptyInstance() => CreateContextRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateContextRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateContextRequest>(
          CreateContextRequest.$_createMessage);
  static CreateContextRequest? _defaultInstance;

  @$pb.TagNumber(1)
  RequestIdentity get identity => $_getN(0);
  @$pb.TagNumber(1)
  set identity(RequestIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);
  @$pb.TagNumber(1)
  RequestIdentity ensureIdentity() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get model => $_getSZ(1);
  @$pb.TagNumber(2)
  set model($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasModel() => $_has(1);
  @$pb.TagNumber(2)
  void clearModel() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<Item> get items => $_getList(2);
}

class InspectContextRequest extends $pb.GeneratedMessage {
  factory InspectContextRequest({
    $core.List<$core.int>? revision,
  }) {
    final result = InspectContextRequest._();
    if (revision != null) result.revision = revision;
    return result;
  }

  InspectContextRequest._();

  factory InspectContextRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectContextRequest()..mergeFromBuffer(data, registry);
  factory InspectContextRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectContextRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectContextRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: InspectContextRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectContextRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectContextRequest copyWith(
          void Function(InspectContextRequest) updates) =>
      super.copyWith((message) => updates(message as InspectContextRequest))
          as InspectContextRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InspectContextRequest() / InspectContextRequest.new instead')
  static InspectContextRequest create() => InspectContextRequest._();
  static $pb.GeneratedMessage $_createMessage() => InspectContextRequest._();
  @$core.override
  InspectContextRequest createEmptyInstance() => InspectContextRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectContextRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectContextRequest>(
          InspectContextRequest.$_createMessage);
  static InspectContextRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get revision => $_getN(0);
  @$pb.TagNumber(1)
  set revision($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRevision() => $_has(0);
  @$pb.TagNumber(1)
  void clearRevision() => $_clearField(1);
}

class Empty extends $pb.GeneratedMessage {
  factory Empty() => Empty._();

  Empty._();

  factory Empty.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Empty()..mergeFromBuffer(data, registry);
  factory Empty.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Empty()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Empty',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Empty.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Empty clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Empty copyWith(void Function(Empty) updates) =>
      super.copyWith((message) => updates(message as Empty)) as Empty;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Empty() / Empty.new instead')
  static Empty create() => Empty._();
  static $pb.GeneratedMessage $_createMessage() => Empty._();
  @$core.override
  Empty createEmptyInstance() => Empty._();
  @$core.pragma('dart2js:noInline')
  static Empty getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Empty>(Empty.$_createMessage);
  static Empty? _defaultInstance;
}

class Insert extends $pb.GeneratedMessage {
  factory Insert({
    $core.List<$core.int>? target,
    Item? item,
  }) {
    final result = Insert._();
    if (target != null) result.target = target;
    if (item != null) result.item = item;
    return result;
  }

  Insert._();

  factory Insert.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Insert()..mergeFromBuffer(data, registry);
  factory Insert.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Insert()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Insert',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Insert.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'target', $pb.PbFieldType.OY)
    ..aOM<Item>(2, _omitFieldNames ? '' : 'item',
        subBuilder: Item.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Insert clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Insert copyWith(void Function(Insert) updates) =>
      super.copyWith((message) => updates(message as Insert)) as Insert;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Insert() / Insert.new instead')
  static Insert create() => Insert._();
  static $pb.GeneratedMessage $_createMessage() => Insert._();
  @$core.override
  Insert createEmptyInstance() => Insert._();
  @$core.pragma('dart2js:noInline')
  static Insert getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Insert>(Insert.$_createMessage);
  static Insert? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get target => $_getN(0);
  @$pb.TagNumber(1)
  set target($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTarget() => $_has(0);
  @$pb.TagNumber(1)
  void clearTarget() => $_clearField(1);

  @$pb.TagNumber(2)
  Item get item => $_getN(1);
  @$pb.TagNumber(2)
  set item(Item value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasItem() => $_has(1);
  @$pb.TagNumber(2)
  void clearItem() => $_clearField(2);
  @$pb.TagNumber(2)
  Item ensureItem() => $_ensure(1);
}

class Replace extends $pb.GeneratedMessage {
  factory Replace({
    $core.List<$core.int>? target,
    $core.List<$core.int>? payload,
  }) {
    final result = Replace._();
    if (target != null) result.target = target;
    if (payload != null) result.payload = payload;
    return result;
  }

  Replace._();

  factory Replace.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Replace()..mergeFromBuffer(data, registry);
  factory Replace.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Replace()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Replace',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Replace.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'target', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'payload', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Replace clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Replace copyWith(void Function(Replace) updates) =>
      super.copyWith((message) => updates(message as Replace)) as Replace;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Replace() / Replace.new instead')
  static Replace create() => Replace._();
  static $pb.GeneratedMessage $_createMessage() => Replace._();
  @$core.override
  Replace createEmptyInstance() => Replace._();
  @$core.pragma('dart2js:noInline')
  static Replace getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Replace>(Replace.$_createMessage);
  static Replace? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get target => $_getN(0);
  @$pb.TagNumber(1)
  set target($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTarget() => $_has(0);
  @$pb.TagNumber(1)
  void clearTarget() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get payload => $_getN(1);
  @$pb.TagNumber(2)
  set payload($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPayload() => $_has(1);
  @$pb.TagNumber(2)
  void clearPayload() => $_clearField(2);
}

enum Edit_Action { append, insertBefore, insertAfter, replace, delete, notSet }

class Edit extends $pb.GeneratedMessage {
  factory Edit({
    Item? append,
    Insert? insertBefore,
    Insert? insertAfter,
    Replace? replace,
    $core.List<$core.int>? delete,
  }) {
    final result = Edit._();
    if (append != null) result.append = append;
    if (insertBefore != null) result.insertBefore = insertBefore;
    if (insertAfter != null) result.insertAfter = insertAfter;
    if (replace != null) result.replace = replace;
    if (delete != null) result.delete = delete;
    return result;
  }

  Edit._();

  factory Edit.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Edit()..mergeFromBuffer(data, registry);
  factory Edit.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Edit()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, Edit_Action> _Edit_ActionByTag = {
    1: Edit_Action.append,
    2: Edit_Action.insertBefore,
    3: Edit_Action.insertAfter,
    4: Edit_Action.replace,
    5: Edit_Action.delete,
    0: Edit_Action.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Edit',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Edit.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5])
    ..aOM<Item>(1, _omitFieldNames ? '' : 'append',
        subBuilder: Item.$_createMessage)
    ..aOM<Insert>(2, _omitFieldNames ? '' : 'insertBefore',
        subBuilder: Insert.$_createMessage)
    ..aOM<Insert>(3, _omitFieldNames ? '' : 'insertAfter',
        subBuilder: Insert.$_createMessage)
    ..aOM<Replace>(4, _omitFieldNames ? '' : 'replace',
        subBuilder: Replace.$_createMessage)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'delete', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Edit clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Edit copyWith(void Function(Edit) updates) =>
      super.copyWith((message) => updates(message as Edit)) as Edit;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Edit() / Edit.new instead')
  static Edit create() => Edit._();
  static $pb.GeneratedMessage $_createMessage() => Edit._();
  @$core.override
  Edit createEmptyInstance() => Edit._();
  @$core.pragma('dart2js:noInline')
  static Edit getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Edit>(Edit.$_createMessage);
  static Edit? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  Edit_Action whichAction() => _Edit_ActionByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  void clearAction() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  Item get append => $_getN(0);
  @$pb.TagNumber(1)
  set append(Item value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasAppend() => $_has(0);
  @$pb.TagNumber(1)
  void clearAppend() => $_clearField(1);
  @$pb.TagNumber(1)
  Item ensureAppend() => $_ensure(0);

  @$pb.TagNumber(2)
  Insert get insertBefore => $_getN(1);
  @$pb.TagNumber(2)
  set insertBefore(Insert value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasInsertBefore() => $_has(1);
  @$pb.TagNumber(2)
  void clearInsertBefore() => $_clearField(2);
  @$pb.TagNumber(2)
  Insert ensureInsertBefore() => $_ensure(1);

  @$pb.TagNumber(3)
  Insert get insertAfter => $_getN(2);
  @$pb.TagNumber(3)
  set insertAfter(Insert value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasInsertAfter() => $_has(2);
  @$pb.TagNumber(3)
  void clearInsertAfter() => $_clearField(3);
  @$pb.TagNumber(3)
  Insert ensureInsertAfter() => $_ensure(2);

  @$pb.TagNumber(4)
  Replace get replace => $_getN(3);
  @$pb.TagNumber(4)
  set replace(Replace value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasReplace() => $_has(3);
  @$pb.TagNumber(4)
  void clearReplace() => $_clearField(4);
  @$pb.TagNumber(4)
  Replace ensureReplace() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.List<$core.int> get delete => $_getN(4);
  @$pb.TagNumber(5)
  set delete($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasDelete() => $_has(4);
  @$pb.TagNumber(5)
  void clearDelete() => $_clearField(5);
}

class Edits extends $pb.GeneratedMessage {
  factory Edits({
    $core.Iterable<Edit>? edits,
  }) {
    final result = Edits._();
    if (edits != null) result.edits.addAll(edits);
    return result;
  }

  Edits._();

  factory Edits.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Edits()..mergeFromBuffer(data, registry);
  factory Edits.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Edits()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Edits',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Edits.$_createMessage)
    ..pPM<Edit>(1, _omitFieldNames ? '' : 'edits',
        subBuilder: Edit.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Edits clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Edits copyWith(void Function(Edits) updates) =>
      super.copyWith((message) => updates(message as Edits)) as Edits;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Edits() / Edits.new instead')
  static Edits create() => Edits._();
  static $pb.GeneratedMessage $_createMessage() => Edits._();
  @$core.override
  Edits createEmptyInstance() => Edits._();
  @$core.pragma('dart2js:noInline')
  static Edits getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Edits>(Edits.$_createMessage);
  static Edits? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<Edit> get edits => $_getList(0);
}

class Truncate extends $pb.GeneratedMessage {
  factory Truncate({
    $core.List<$core.int>? through,
  }) {
    final result = Truncate._();
    if (through != null) result.through = through;
    return result;
  }

  Truncate._();

  factory Truncate.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Truncate()..mergeFromBuffer(data, registry);
  factory Truncate.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Truncate()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Truncate',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Truncate.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'through', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Truncate clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Truncate copyWith(void Function(Truncate) updates) =>
      super.copyWith((message) => updates(message as Truncate)) as Truncate;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Truncate() / Truncate.new instead')
  static Truncate create() => Truncate._();
  static $pb.GeneratedMessage $_createMessage() => Truncate._();
  @$core.override
  Truncate createEmptyInstance() => Truncate._();
  @$core.pragma('dart2js:noInline')
  static Truncate getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Truncate>(Truncate.$_createMessage);
  static Truncate? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get through => $_getN(0);
  @$pb.TagNumber(1)
  set through($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasThrough() => $_has(0);
  @$pb.TagNumber(1)
  void clearThrough() => $_clearField(1);
}

class Compact extends $pb.GeneratedMessage {
  factory Compact({
    $core.Iterable<$core.List<$core.int>>? selected,
    $core.Iterable<Item>? replacement,
  }) {
    final result = Compact._();
    if (selected != null) result.selected.addAll(selected);
    if (replacement != null) result.replacement.addAll(replacement);
    return result;
  }

  Compact._();

  factory Compact.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Compact()..mergeFromBuffer(data, registry);
  factory Compact.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Compact()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Compact',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Compact.$_createMessage)
    ..p<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'selected', $pb.PbFieldType.PY)
    ..pPM<Item>(2, _omitFieldNames ? '' : 'replacement',
        subBuilder: Item.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Compact clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Compact copyWith(void Function(Compact) updates) =>
      super.copyWith((message) => updates(message as Compact)) as Compact;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Compact() / Compact.new instead')
  static Compact create() => Compact._();
  static $pb.GeneratedMessage $_createMessage() => Compact._();
  @$core.override
  Compact createEmptyInstance() => Compact._();
  @$core.pragma('dart2js:noInline')
  static Compact getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Compact>(Compact.$_createMessage);
  static Compact? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<$core.List<$core.int>> get selected => $_getList(0);

  @$pb.TagNumber(2)
  $pb.PbList<Item> get replacement => $_getList(1);
}

class Transfer extends $pb.GeneratedMessage {
  factory Transfer({
    $core.String? model,
  }) {
    final result = Transfer._();
    if (model != null) result.model = model;
    return result;
  }

  Transfer._();

  factory Transfer.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Transfer()..mergeFromBuffer(data, registry);
  factory Transfer.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Transfer()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Transfer',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: Transfer.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'model')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Transfer clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Transfer copyWith(void Function(Transfer) updates) =>
      super.copyWith((message) => updates(message as Transfer)) as Transfer;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Transfer() / Transfer.new instead')
  static Transfer create() => Transfer._();
  static $pb.GeneratedMessage $_createMessage() => Transfer._();
  @$core.override
  Transfer createEmptyInstance() => Transfer._();
  @$core.pragma('dart2js:noInline')
  static Transfer getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Transfer>(Transfer.$_createMessage);
  static Transfer? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get model => $_getSZ(0);
  @$pb.TagNumber(1)
  set model($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasModel() => $_has(0);
  @$pb.TagNumber(1)
  void clearModel() => $_clearField(1);
}

enum MutateContextRequest_Action {
  edit,
  fork,
  truncate,
  compact,
  release,
  transfer,
  notSet
}

class MutateContextRequest extends $pb.GeneratedMessage {
  factory MutateContextRequest({
    RequestIdentity? identity,
    $core.List<$core.int>? source,
    Edits? edit,
    Empty? fork,
    Truncate? truncate,
    Compact? compact,
    Empty? release,
    Transfer? transfer,
  }) {
    final result = MutateContextRequest._();
    if (identity != null) result.identity = identity;
    if (source != null) result.source = source;
    if (edit != null) result.edit = edit;
    if (fork != null) result.fork = fork;
    if (truncate != null) result.truncate = truncate;
    if (compact != null) result.compact = compact;
    if (release != null) result.release = release;
    if (transfer != null) result.transfer = transfer;
    return result;
  }

  MutateContextRequest._();

  factory MutateContextRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutateContextRequest()..mergeFromBuffer(data, registry);
  factory MutateContextRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutateContextRequest()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, MutateContextRequest_Action>
      _MutateContextRequest_ActionByTag = {
    3: MutateContextRequest_Action.edit,
    4: MutateContextRequest_Action.fork,
    5: MutateContextRequest_Action.truncate,
    6: MutateContextRequest_Action.compact,
    7: MutateContextRequest_Action.release,
    8: MutateContextRequest_Action.transfer,
    0: MutateContextRequest_Action.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MutateContextRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: MutateContextRequest.$_createMessage)
    ..oo(0, [3, 4, 5, 6, 7, 8])
    ..aOM<RequestIdentity>(1, _omitFieldNames ? '' : 'identity',
        subBuilder: RequestIdentity.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'source', $pb.PbFieldType.OY)
    ..aOM<Edits>(3, _omitFieldNames ? '' : 'edit',
        subBuilder: Edits.$_createMessage)
    ..aOM<Empty>(4, _omitFieldNames ? '' : 'fork',
        subBuilder: Empty.$_createMessage)
    ..aOM<Truncate>(5, _omitFieldNames ? '' : 'truncate',
        subBuilder: Truncate.$_createMessage)
    ..aOM<Compact>(6, _omitFieldNames ? '' : 'compact',
        subBuilder: Compact.$_createMessage)
    ..aOM<Empty>(7, _omitFieldNames ? '' : 'release',
        subBuilder: Empty.$_createMessage)
    ..aOM<Transfer>(8, _omitFieldNames ? '' : 'transfer',
        subBuilder: Transfer.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutateContextRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutateContextRequest copyWith(void Function(MutateContextRequest) updates) =>
      super.copyWith((message) => updates(message as MutateContextRequest))
          as MutateContextRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use MutateContextRequest() / MutateContextRequest.new instead')
  static MutateContextRequest create() => MutateContextRequest._();
  static $pb.GeneratedMessage $_createMessage() => MutateContextRequest._();
  @$core.override
  MutateContextRequest createEmptyInstance() => MutateContextRequest._();
  @$core.pragma('dart2js:noInline')
  static MutateContextRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<MutateContextRequest>(
          MutateContextRequest.$_createMessage);
  static MutateContextRequest? _defaultInstance;

  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  @$pb.TagNumber(8)
  MutateContextRequest_Action whichAction() =>
      _MutateContextRequest_ActionByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  @$pb.TagNumber(8)
  void clearAction() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  RequestIdentity get identity => $_getN(0);
  @$pb.TagNumber(1)
  set identity(RequestIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);
  @$pb.TagNumber(1)
  RequestIdentity ensureIdentity() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get source => $_getN(1);
  @$pb.TagNumber(2)
  set source($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSource() => $_has(1);
  @$pb.TagNumber(2)
  void clearSource() => $_clearField(2);

  @$pb.TagNumber(3)
  Edits get edit => $_getN(2);
  @$pb.TagNumber(3)
  set edit(Edits value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasEdit() => $_has(2);
  @$pb.TagNumber(3)
  void clearEdit() => $_clearField(3);
  @$pb.TagNumber(3)
  Edits ensureEdit() => $_ensure(2);

  @$pb.TagNumber(4)
  Empty get fork => $_getN(3);
  @$pb.TagNumber(4)
  set fork(Empty value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasFork() => $_has(3);
  @$pb.TagNumber(4)
  void clearFork() => $_clearField(4);
  @$pb.TagNumber(4)
  Empty ensureFork() => $_ensure(3);

  @$pb.TagNumber(5)
  Truncate get truncate => $_getN(4);
  @$pb.TagNumber(5)
  set truncate(Truncate value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasTruncate() => $_has(4);
  @$pb.TagNumber(5)
  void clearTruncate() => $_clearField(5);
  @$pb.TagNumber(5)
  Truncate ensureTruncate() => $_ensure(4);

  @$pb.TagNumber(6)
  Compact get compact => $_getN(5);
  @$pb.TagNumber(6)
  set compact(Compact value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasCompact() => $_has(5);
  @$pb.TagNumber(6)
  void clearCompact() => $_clearField(6);
  @$pb.TagNumber(6)
  Compact ensureCompact() => $_ensure(5);

  @$pb.TagNumber(7)
  Empty get release => $_getN(6);
  @$pb.TagNumber(7)
  set release(Empty value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasRelease() => $_has(6);
  @$pb.TagNumber(7)
  void clearRelease() => $_clearField(7);
  @$pb.TagNumber(7)
  Empty ensureRelease() => $_ensure(6);

  @$pb.TagNumber(8)
  Transfer get transfer => $_getN(7);
  @$pb.TagNumber(8)
  set transfer(Transfer value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasTransfer() => $_has(7);
  @$pb.TagNumber(8)
  void clearTransfer() => $_clearField(8);
  @$pb.TagNumber(8)
  Transfer ensureTransfer() => $_ensure(7);
}

class MutationReceipt extends $pb.GeneratedMessage {
  factory MutationReceipt({
    $core.List<$core.int>? revision,
    $core.List<$core.int>? commandDigest,
    $fixnum.Int64? sequence,
    $core.bool? retained,
  }) {
    final result = MutationReceipt._();
    if (revision != null) result.revision = revision;
    if (commandDigest != null) result.commandDigest = commandDigest;
    if (sequence != null) result.sequence = sequence;
    if (retained != null) result.retained = retained;
    return result;
  }

  MutationReceipt._();

  factory MutationReceipt.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationReceipt()..mergeFromBuffer(data, registry);
  factory MutationReceipt.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationReceipt()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MutationReceipt',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: MutationReceipt.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'commandDigest', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'sequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOB(4, _omitFieldNames ? '' : 'retained')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationReceipt clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationReceipt copyWith(void Function(MutationReceipt) updates) =>
      super.copyWith((message) => updates(message as MutationReceipt))
          as MutationReceipt;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MutationReceipt() / MutationReceipt.new instead')
  static MutationReceipt create() => MutationReceipt._();
  static $pb.GeneratedMessage $_createMessage() => MutationReceipt._();
  @$core.override
  MutationReceipt createEmptyInstance() => MutationReceipt._();
  @$core.pragma('dart2js:noInline')
  static MutationReceipt getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MutationReceipt>(
          MutationReceipt.$_createMessage);
  static MutationReceipt? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get revision => $_getN(0);
  @$pb.TagNumber(1)
  set revision($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRevision() => $_has(0);
  @$pb.TagNumber(1)
  void clearRevision() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get commandDigest => $_getN(1);
  @$pb.TagNumber(2)
  set commandDigest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasCommandDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearCommandDigest() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get sequence => $_getI64(2);
  @$pb.TagNumber(3)
  set sequence($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasSequence() => $_has(2);
  @$pb.TagNumber(3)
  void clearSequence() => $_clearField(3);

  /// The recorded command's effect, not a promise of current or warm retention.
  @$pb.TagNumber(4)
  $core.bool get retained => $_getBF(3);
  @$pb.TagNumber(4)
  set retained($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasRetained() => $_has(3);
  @$pb.TagNumber(4)
  void clearRetained() => $_clearField(4);
}

class ContextView extends $pb.GeneratedMessage {
  factory ContextView({
    $core.List<$core.int>? revision,
    $core.List<$core.int>? parent,
    $core.List<$core.int>? lineage,
    $core.List<$core.int>? executionProfile,
    $core.List<$core.int>? contentDigest,
    $core.Iterable<Item>? items,
    $core.String? model,
    ContextProvenance? provenance,
  }) {
    final result = ContextView._();
    if (revision != null) result.revision = revision;
    if (parent != null) result.parent = parent;
    if (lineage != null) result.lineage = lineage;
    if (executionProfile != null) result.executionProfile = executionProfile;
    if (contentDigest != null) result.contentDigest = contentDigest;
    if (items != null) result.items.addAll(items);
    if (model != null) result.model = model;
    if (provenance != null) result.provenance = provenance;
    return result;
  }

  ContextView._();

  factory ContextView.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ContextView()..mergeFromBuffer(data, registry);
  factory ContextView.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ContextView()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ContextView',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: ContextView.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'parent', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'lineage', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'executionProfile', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'contentDigest', $pb.PbFieldType.OY)
    ..pPM<Item>(6, _omitFieldNames ? '' : 'items',
        subBuilder: Item.$_createMessage)
    ..aOS(7, _omitFieldNames ? '' : 'model')
    ..aOM<ContextProvenance>(8, _omitFieldNames ? '' : 'provenance',
        subBuilder: ContextProvenance.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ContextView clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ContextView copyWith(void Function(ContextView) updates) =>
      super.copyWith((message) => updates(message as ContextView))
          as ContextView;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ContextView() / ContextView.new instead')
  static ContextView create() => ContextView._();
  static $pb.GeneratedMessage $_createMessage() => ContextView._();
  @$core.override
  ContextView createEmptyInstance() => ContextView._();
  @$core.pragma('dart2js:noInline')
  static ContextView getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ContextView>(
          ContextView.$_createMessage);
  static ContextView? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get revision => $_getN(0);
  @$pb.TagNumber(1)
  set revision($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRevision() => $_has(0);
  @$pb.TagNumber(1)
  void clearRevision() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get parent => $_getN(1);
  @$pb.TagNumber(2)
  set parent($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasParent() => $_has(1);
  @$pb.TagNumber(2)
  void clearParent() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get lineage => $_getN(2);
  @$pb.TagNumber(3)
  set lineage($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLineage() => $_has(2);
  @$pb.TagNumber(3)
  void clearLineage() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get executionProfile => $_getN(3);
  @$pb.TagNumber(4)
  set executionProfile($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasExecutionProfile() => $_has(3);
  @$pb.TagNumber(4)
  void clearExecutionProfile() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get contentDigest => $_getN(4);
  @$pb.TagNumber(5)
  set contentDigest($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasContentDigest() => $_has(4);
  @$pb.TagNumber(5)
  void clearContentDigest() => $_clearField(5);

  @$pb.TagNumber(6)
  $pb.PbList<Item> get items => $_getList(5);

  @$pb.TagNumber(7)
  $core.String get model => $_getSZ(6);
  @$pb.TagNumber(7)
  set model($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasModel() => $_has(6);
  @$pb.TagNumber(7)
  void clearModel() => $_clearField(7);

  @$pb.TagNumber(8)
  ContextProvenance get provenance => $_getN(7);
  @$pb.TagNumber(8)
  set provenance(ContextProvenance value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasProvenance() => $_has(7);
  @$pb.TagNumber(8)
  void clearProvenance() => $_clearField(8);
  @$pb.TagNumber(8)
  ContextProvenance ensureProvenance() => $_ensure(7);
}

enum ContextProvenance_Origin {
  created,
  derived,
  forked,
  transferred,
  generated,
  runInput,
  notSet
}

class ContextProvenance extends $pb.GeneratedMessage {
  factory ContextProvenance({
    Empty? created,
    ProvenanceSource? derived,
    ProvenanceSource? forked,
    TransferProvenance? transferred,
    GenerationProvenance? generated,
    RunInputProvenance? runInput,
  }) {
    final result = ContextProvenance._();
    if (created != null) result.created = created;
    if (derived != null) result.derived = derived;
    if (forked != null) result.forked = forked;
    if (transferred != null) result.transferred = transferred;
    if (generated != null) result.generated = generated;
    if (runInput != null) result.runInput = runInput;
    return result;
  }

  ContextProvenance._();

  factory ContextProvenance.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ContextProvenance()..mergeFromBuffer(data, registry);
  factory ContextProvenance.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ContextProvenance()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ContextProvenance_Origin>
      _ContextProvenance_OriginByTag = {
    1: ContextProvenance_Origin.created,
    2: ContextProvenance_Origin.derived,
    3: ContextProvenance_Origin.forked,
    4: ContextProvenance_Origin.transferred,
    5: ContextProvenance_Origin.generated,
    6: ContextProvenance_Origin.runInput,
    0: ContextProvenance_Origin.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ContextProvenance',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: ContextProvenance.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5, 6])
    ..aOM<Empty>(1, _omitFieldNames ? '' : 'created',
        subBuilder: Empty.$_createMessage)
    ..aOM<ProvenanceSource>(2, _omitFieldNames ? '' : 'derived',
        subBuilder: ProvenanceSource.$_createMessage)
    ..aOM<ProvenanceSource>(3, _omitFieldNames ? '' : 'forked',
        subBuilder: ProvenanceSource.$_createMessage)
    ..aOM<TransferProvenance>(4, _omitFieldNames ? '' : 'transferred',
        subBuilder: TransferProvenance.$_createMessage)
    ..aOM<GenerationProvenance>(5, _omitFieldNames ? '' : 'generated',
        subBuilder: GenerationProvenance.$_createMessage)
    ..aOM<RunInputProvenance>(6, _omitFieldNames ? '' : 'runInput',
        subBuilder: RunInputProvenance.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ContextProvenance clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ContextProvenance copyWith(void Function(ContextProvenance) updates) =>
      super.copyWith((message) => updates(message as ContextProvenance))
          as ContextProvenance;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ContextProvenance() / ContextProvenance.new instead')
  static ContextProvenance create() => ContextProvenance._();
  static $pb.GeneratedMessage $_createMessage() => ContextProvenance._();
  @$core.override
  ContextProvenance createEmptyInstance() => ContextProvenance._();
  @$core.pragma('dart2js:noInline')
  static ContextProvenance getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ContextProvenance>(
          ContextProvenance.$_createMessage);
  static ContextProvenance? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  ContextProvenance_Origin whichOrigin() =>
      _ContextProvenance_OriginByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  void clearOrigin() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  Empty get created => $_getN(0);
  @$pb.TagNumber(1)
  set created(Empty value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCreated() => $_has(0);
  @$pb.TagNumber(1)
  void clearCreated() => $_clearField(1);
  @$pb.TagNumber(1)
  Empty ensureCreated() => $_ensure(0);

  @$pb.TagNumber(2)
  ProvenanceSource get derived => $_getN(1);
  @$pb.TagNumber(2)
  set derived(ProvenanceSource value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasDerived() => $_has(1);
  @$pb.TagNumber(2)
  void clearDerived() => $_clearField(2);
  @$pb.TagNumber(2)
  ProvenanceSource ensureDerived() => $_ensure(1);

  @$pb.TagNumber(3)
  ProvenanceSource get forked => $_getN(2);
  @$pb.TagNumber(3)
  set forked(ProvenanceSource value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasForked() => $_has(2);
  @$pb.TagNumber(3)
  void clearForked() => $_clearField(3);
  @$pb.TagNumber(3)
  ProvenanceSource ensureForked() => $_ensure(2);

  @$pb.TagNumber(4)
  TransferProvenance get transferred => $_getN(3);
  @$pb.TagNumber(4)
  set transferred(TransferProvenance value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasTransferred() => $_has(3);
  @$pb.TagNumber(4)
  void clearTransferred() => $_clearField(4);
  @$pb.TagNumber(4)
  TransferProvenance ensureTransferred() => $_ensure(3);

  @$pb.TagNumber(5)
  GenerationProvenance get generated => $_getN(4);
  @$pb.TagNumber(5)
  set generated(GenerationProvenance value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasGenerated() => $_has(4);
  @$pb.TagNumber(5)
  void clearGenerated() => $_clearField(5);
  @$pb.TagNumber(5)
  GenerationProvenance ensureGenerated() => $_ensure(4);

  @$pb.TagNumber(6)
  RunInputProvenance get runInput => $_getN(5);
  @$pb.TagNumber(6)
  set runInput(RunInputProvenance value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasRunInput() => $_has(5);
  @$pb.TagNumber(6)
  void clearRunInput() => $_clearField(6);
  @$pb.TagNumber(6)
  RunInputProvenance ensureRunInput() => $_ensure(5);
}

class ProvenanceSource extends $pb.GeneratedMessage {
  factory ProvenanceSource({
    $core.List<$core.int>? source,
  }) {
    final result = ProvenanceSource._();
    if (source != null) result.source = source;
    return result;
  }

  ProvenanceSource._();

  factory ProvenanceSource.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProvenanceSource()..mergeFromBuffer(data, registry);
  factory ProvenanceSource.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProvenanceSource()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProvenanceSource',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: ProvenanceSource.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'source', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProvenanceSource clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProvenanceSource copyWith(void Function(ProvenanceSource) updates) =>
      super.copyWith((message) => updates(message as ProvenanceSource))
          as ProvenanceSource;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ProvenanceSource() / ProvenanceSource.new instead')
  static ProvenanceSource create() => ProvenanceSource._();
  static $pb.GeneratedMessage $_createMessage() => ProvenanceSource._();
  @$core.override
  ProvenanceSource createEmptyInstance() => ProvenanceSource._();
  @$core.pragma('dart2js:noInline')
  static ProvenanceSource getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ProvenanceSource>(
          ProvenanceSource.$_createMessage);
  static ProvenanceSource? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get source => $_getN(0);
  @$pb.TagNumber(1)
  set source($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);
}

class TransferProvenance extends $pb.GeneratedMessage {
  factory TransferProvenance({
    $core.List<$core.int>? source,
    $core.bool? reusedCompatibleState,
  }) {
    final result = TransferProvenance._();
    if (source != null) result.source = source;
    if (reusedCompatibleState != null)
      result.reusedCompatibleState = reusedCompatibleState;
    return result;
  }

  TransferProvenance._();

  factory TransferProvenance.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TransferProvenance()..mergeFromBuffer(data, registry);
  factory TransferProvenance.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TransferProvenance()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TransferProvenance',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: TransferProvenance.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'source', $pb.PbFieldType.OY)
    ..aOB(2, _omitFieldNames ? '' : 'reusedCompatibleState')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TransferProvenance clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TransferProvenance copyWith(void Function(TransferProvenance) updates) =>
      super.copyWith((message) => updates(message as TransferProvenance))
          as TransferProvenance;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TransferProvenance() / TransferProvenance.new instead')
  static TransferProvenance create() => TransferProvenance._();
  static $pb.GeneratedMessage $_createMessage() => TransferProvenance._();
  @$core.override
  TransferProvenance createEmptyInstance() => TransferProvenance._();
  @$core.pragma('dart2js:noInline')
  static TransferProvenance getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<TransferProvenance>(
          TransferProvenance.$_createMessage);
  static TransferProvenance? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get source => $_getN(0);
  @$pb.TagNumber(1)
  set source($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.bool get reusedCompatibleState => $_getBF(1);
  @$pb.TagNumber(2)
  set reusedCompatibleState($core.bool value) => $_setBool(1, value);
  @$pb.TagNumber(2)
  $core.bool hasReusedCompatibleState() => $_has(1);
  @$pb.TagNumber(2)
  void clearReusedCompatibleState() => $_clearField(2);
}

class GenerationProvenance extends $pb.GeneratedMessage {
  factory GenerationProvenance({
    $core.List<$core.int>? runId,
    $core.List<$core.int>? terminalReceiptDigest,
  }) {
    final result = GenerationProvenance._();
    if (runId != null) result.runId = runId;
    if (terminalReceiptDigest != null)
      result.terminalReceiptDigest = terminalReceiptDigest;
    return result;
  }

  GenerationProvenance._();

  factory GenerationProvenance.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerationProvenance()..mergeFromBuffer(data, registry);
  factory GenerationProvenance.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerationProvenance()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GenerationProvenance',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: GenerationProvenance.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'runId', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'terminalReceiptDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerationProvenance clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerationProvenance copyWith(void Function(GenerationProvenance) updates) =>
      super.copyWith((message) => updates(message as GenerationProvenance))
          as GenerationProvenance;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use GenerationProvenance() / GenerationProvenance.new instead')
  static GenerationProvenance create() => GenerationProvenance._();
  static $pb.GeneratedMessage $_createMessage() => GenerationProvenance._();
  @$core.override
  GenerationProvenance createEmptyInstance() => GenerationProvenance._();
  @$core.pragma('dart2js:noInline')
  static GenerationProvenance getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<GenerationProvenance>(
          GenerationProvenance.$_createMessage);
  static GenerationProvenance? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get runId => $_getN(0);
  @$pb.TagNumber(1)
  set runId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRunId() => $_has(0);
  @$pb.TagNumber(1)
  void clearRunId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get terminalReceiptDigest => $_getN(1);
  @$pb.TagNumber(2)
  set terminalReceiptDigest($core.List<$core.int> value) =>
      $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasTerminalReceiptDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearTerminalReceiptDigest() => $_clearField(2);
}

class RunInputProvenance extends $pb.GeneratedMessage {
  factory RunInputProvenance({
    $core.List<$core.int>? source,
    $core.List<$core.int>? runId,
    $fixnum.Int64? maximumOutput,
    $fixnum.Int64? seed,
  }) {
    final result = RunInputProvenance._();
    if (source != null) result.source = source;
    if (runId != null) result.runId = runId;
    if (maximumOutput != null) result.maximumOutput = maximumOutput;
    if (seed != null) result.seed = seed;
    return result;
  }

  RunInputProvenance._();

  factory RunInputProvenance.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunInputProvenance()..mergeFromBuffer(data, registry);
  factory RunInputProvenance.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunInputProvenance()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RunInputProvenance',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RunInputProvenance.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'source', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'runId', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'maximumOutput', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(4, _omitFieldNames ? '' : 'seed', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunInputProvenance clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunInputProvenance copyWith(void Function(RunInputProvenance) updates) =>
      super.copyWith((message) => updates(message as RunInputProvenance))
          as RunInputProvenance;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RunInputProvenance() / RunInputProvenance.new instead')
  static RunInputProvenance create() => RunInputProvenance._();
  static $pb.GeneratedMessage $_createMessage() => RunInputProvenance._();
  @$core.override
  RunInputProvenance createEmptyInstance() => RunInputProvenance._();
  @$core.pragma('dart2js:noInline')
  static RunInputProvenance getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RunInputProvenance>(
          RunInputProvenance.$_createMessage);
  static RunInputProvenance? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get source => $_getN(0);
  @$pb.TagNumber(1)
  set source($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get runId => $_getN(1);
  @$pb.TagNumber(2)
  set runId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRunId() => $_has(1);
  @$pb.TagNumber(2)
  void clearRunId() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get maximumOutput => $_getI64(2);
  @$pb.TagNumber(3)
  set maximumOutput($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumOutput() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumOutput() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get seed => $_getI64(3);
  @$pb.TagNumber(4)
  set seed($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasSeed() => $_has(3);
  @$pb.TagNumber(4)
  void clearSeed() => $_clearField(4);
}

class GenerateRunRequest extends $pb.GeneratedMessage {
  factory GenerateRunRequest({
    RequestIdentity? identity,
    $core.List<$core.int>? context,
    Item? input,
    $fixnum.Int64? maximumOutput,
    $fixnum.Int64? seed,
  }) {
    final result = GenerateRunRequest._();
    if (identity != null) result.identity = identity;
    if (context != null) result.context = context;
    if (input != null) result.input = input;
    if (maximumOutput != null) result.maximumOutput = maximumOutput;
    if (seed != null) result.seed = seed;
    return result;
  }

  GenerateRunRequest._();

  factory GenerateRunRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerateRunRequest()..mergeFromBuffer(data, registry);
  factory GenerateRunRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerateRunRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GenerateRunRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: GenerateRunRequest.$_createMessage)
    ..aOM<RequestIdentity>(1, _omitFieldNames ? '' : 'identity',
        subBuilder: RequestIdentity.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'context', $pb.PbFieldType.OY)
    ..aOM<Item>(3, _omitFieldNames ? '' : 'input',
        subBuilder: Item.$_createMessage)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'maximumOutput', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(5, _omitFieldNames ? '' : 'seed', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerateRunRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerateRunRequest copyWith(void Function(GenerateRunRequest) updates) =>
      super.copyWith((message) => updates(message as GenerateRunRequest))
          as GenerateRunRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use GenerateRunRequest() / GenerateRunRequest.new instead')
  static GenerateRunRequest create() => GenerateRunRequest._();
  static $pb.GeneratedMessage $_createMessage() => GenerateRunRequest._();
  @$core.override
  GenerateRunRequest createEmptyInstance() => GenerateRunRequest._();
  @$core.pragma('dart2js:noInline')
  static GenerateRunRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<GenerateRunRequest>(
          GenerateRunRequest.$_createMessage);
  static GenerateRunRequest? _defaultInstance;

  @$pb.TagNumber(1)
  RequestIdentity get identity => $_getN(0);
  @$pb.TagNumber(1)
  set identity(RequestIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasIdentity() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdentity() => $_clearField(1);
  @$pb.TagNumber(1)
  RequestIdentity ensureIdentity() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get context => $_getN(1);
  @$pb.TagNumber(2)
  set context($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasContext() => $_has(1);
  @$pb.TagNumber(2)
  void clearContext() => $_clearField(2);

  @$pb.TagNumber(3)
  Item get input => $_getN(2);
  @$pb.TagNumber(3)
  set input(Item value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasInput() => $_has(2);
  @$pb.TagNumber(3)
  void clearInput() => $_clearField(3);
  @$pb.TagNumber(3)
  Item ensureInput() => $_ensure(2);

  @$pb.TagNumber(4)
  $fixnum.Int64 get maximumOutput => $_getI64(3);
  @$pb.TagNumber(4)
  set maximumOutput($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumOutput() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumOutput() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get seed => $_getI64(4);
  @$pb.TagNumber(5)
  set seed($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasSeed() => $_has(4);
  @$pb.TagNumber(5)
  void clearSeed() => $_clearField(5);
}

class GenerateRunResponse extends $pb.GeneratedMessage {
  factory GenerateRunResponse({
    RunView? run,
  }) {
    final result = GenerateRunResponse._();
    if (run != null) result.run = run;
    return result;
  }

  GenerateRunResponse._();

  factory GenerateRunResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerateRunResponse()..mergeFromBuffer(data, registry);
  factory GenerateRunResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerateRunResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GenerateRunResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: GenerateRunResponse.$_createMessage)
    ..aOM<RunView>(1, _omitFieldNames ? '' : 'run',
        subBuilder: RunView.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerateRunResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerateRunResponse copyWith(void Function(GenerateRunResponse) updates) =>
      super.copyWith((message) => updates(message as GenerateRunResponse))
          as GenerateRunResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use GenerateRunResponse() / GenerateRunResponse.new instead')
  static GenerateRunResponse create() => GenerateRunResponse._();
  static $pb.GeneratedMessage $_createMessage() => GenerateRunResponse._();
  @$core.override
  GenerateRunResponse createEmptyInstance() => GenerateRunResponse._();
  @$core.pragma('dart2js:noInline')
  static GenerateRunResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<GenerateRunResponse>(
          GenerateRunResponse.$_createMessage);
  static GenerateRunResponse? _defaultInstance;

  @$pb.TagNumber(1)
  RunView get run => $_getN(0);
  @$pb.TagNumber(1)
  set run(RunView value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasRun() => $_has(0);
  @$pb.TagNumber(1)
  void clearRun() => $_clearField(1);
  @$pb.TagNumber(1)
  RunView ensureRun() => $_ensure(0);
}

class InspectRunRequest extends $pb.GeneratedMessage {
  factory InspectRunRequest({
    $core.List<$core.int>? runId,
  }) {
    final result = InspectRunRequest._();
    if (runId != null) result.runId = runId;
    return result;
  }

  InspectRunRequest._();

  factory InspectRunRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectRunRequest()..mergeFromBuffer(data, registry);
  factory InspectRunRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectRunRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectRunRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: InspectRunRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'runId', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectRunRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectRunRequest copyWith(void Function(InspectRunRequest) updates) =>
      super.copyWith((message) => updates(message as InspectRunRequest))
          as InspectRunRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InspectRunRequest() / InspectRunRequest.new instead')
  static InspectRunRequest create() => InspectRunRequest._();
  static $pb.GeneratedMessage $_createMessage() => InspectRunRequest._();
  @$core.override
  InspectRunRequest createEmptyInstance() => InspectRunRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectRunRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<InspectRunRequest>(
          InspectRunRequest.$_createMessage);
  static InspectRunRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get runId => $_getN(0);
  @$pb.TagNumber(1)
  set runId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRunId() => $_has(0);
  @$pb.TagNumber(1)
  void clearRunId() => $_clearField(1);
}

class WatchRunRequest extends $pb.GeneratedMessage {
  factory WatchRunRequest({
    $core.List<$core.int>? runId,
    $fixnum.Int64? fromSequence,
  }) {
    final result = WatchRunRequest._();
    if (runId != null) result.runId = runId;
    if (fromSequence != null) result.fromSequence = fromSequence;
    return result;
  }

  WatchRunRequest._();

  factory WatchRunRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WatchRunRequest()..mergeFromBuffer(data, registry);
  factory WatchRunRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WatchRunRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WatchRunRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: WatchRunRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'runId', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'fromSequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WatchRunRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WatchRunRequest copyWith(void Function(WatchRunRequest) updates) =>
      super.copyWith((message) => updates(message as WatchRunRequest))
          as WatchRunRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WatchRunRequest() / WatchRunRequest.new instead')
  static WatchRunRequest create() => WatchRunRequest._();
  static $pb.GeneratedMessage $_createMessage() => WatchRunRequest._();
  @$core.override
  WatchRunRequest createEmptyInstance() => WatchRunRequest._();
  @$core.pragma('dart2js:noInline')
  static WatchRunRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<WatchRunRequest>(
          WatchRunRequest.$_createMessage);
  static WatchRunRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get runId => $_getN(0);
  @$pb.TagNumber(1)
  set runId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRunId() => $_has(0);
  @$pb.TagNumber(1)
  void clearRunId() => $_clearField(1);

  /// Inclusive, zero-based public cursor.
  @$pb.TagNumber(2)
  $fixnum.Int64 get fromSequence => $_getI64(1);
  @$pb.TagNumber(2)
  set fromSequence($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasFromSequence() => $_has(1);
  @$pb.TagNumber(2)
  void clearFromSequence() => $_clearField(2);
}

class LogicalUsage extends $pb.GeneratedMessage {
  factory LogicalUsage({
    $fixnum.Int64? newPrefill,
    $fixnum.Int64? generatedOutput,
    $fixnum.Int64? effectiveContextReads,
    $fixnum.Int64? retainedByteMillis,
  }) {
    final result = LogicalUsage._();
    if (newPrefill != null) result.newPrefill = newPrefill;
    if (generatedOutput != null) result.generatedOutput = generatedOutput;
    if (effectiveContextReads != null)
      result.effectiveContextReads = effectiveContextReads;
    if (retainedByteMillis != null)
      result.retainedByteMillis = retainedByteMillis;
    return result;
  }

  LogicalUsage._();

  factory LogicalUsage.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      LogicalUsage()..mergeFromBuffer(data, registry);
  factory LogicalUsage.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      LogicalUsage()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LogicalUsage',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: LogicalUsage.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'newPrefill', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'generatedOutput', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'effectiveContextReads', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'retainedByteMillis', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LogicalUsage clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LogicalUsage copyWith(void Function(LogicalUsage) updates) =>
      super.copyWith((message) => updates(message as LogicalUsage))
          as LogicalUsage;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use LogicalUsage() / LogicalUsage.new instead')
  static LogicalUsage create() => LogicalUsage._();
  static $pb.GeneratedMessage $_createMessage() => LogicalUsage._();
  @$core.override
  LogicalUsage createEmptyInstance() => LogicalUsage._();
  @$core.pragma('dart2js:noInline')
  static LogicalUsage getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<LogicalUsage>(
          LogicalUsage.$_createMessage);
  static LogicalUsage? _defaultInstance;

  /// Prompt tokens newly computed at this Run's first verified execution.
  /// Frozen with effective_context_reads as a partition of the exact rendered
  /// prompt-token total, bound by native tokenizer/render/model/runtime/KV proof.
  /// Recovery, retry and replay never reclassify or add to this input partition.
  @$pb.TagNumber(1)
  $fixnum.Int64 get newPrefill => $_getI64(0);
  @$pb.TagNumber(1)
  set newPrefill($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasNewPrefill() => $_has(0);
  @$pb.TagNumber(1)
  void clearNewPrefill() => $_clearField(1);

  /// Uniquely committed native output-token records, counted once. Decoded UTF-8
  /// bytes or re-tokenization cannot establish this count. A non-output EOS
  /// sentinel is excluded; persisted special/stop output records require explicit
  /// meter-revision semantics. Failed/discarded pre-checkpoint device work has no
  /// agreed eligibility rule here and must not be inferred as zero eligible work.
  @$pb.TagNumber(2)
  $fixnum.Int64 get generatedOutput => $_getI64(1);
  @$pb.TagNumber(2)
  set generatedOutput($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasGeneratedOutput() => $_has(1);
  @$pb.TagNumber(2)
  void clearGeneratedOutput() => $_clearField(2);

  /// Prompt tokens actually served from verified KV reuse in the same frozen
  /// first-execution partition. Together with new_prefill this equals the exact
  /// rendered prompt-token total; repeated admission/watch adds no new units.
  @$pb.TagNumber(3)
  $fixnum.Int64 get effectiveContextReads => $_getI64(2);
  @$pb.TagNumber(3)
  set effectiveContextReads($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasEffectiveContextReads() => $_has(2);
  @$pb.TagNumber(3)
  void clearEffectiveContextReads() => $_clearField(3);

  /// Logical Context retention measure. Logical custody identity,
  /// interval events, pending-intent eligibility and dedup scope remain unagreed.
  /// Neither device/cache allocations nor Objects physical storage establish
  /// this measure; absent lifecycle evidence must not imply zero eligible work.
  @$pb.TagNumber(4)
  $fixnum.Int64 get retainedByteMillis => $_getI64(3);
  @$pb.TagNumber(4)
  set retainedByteMillis($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasRetainedByteMillis() => $_has(3);
  @$pb.TagNumber(4)
  void clearRetainedByteMillis() => $_clearField(4);
}

class UsageReceipt extends $pb.GeneratedMessage {
  factory UsageReceipt({
    $core.List<$core.int>? receiptId,
    $core.List<$core.int>? modelProfile,
    $core.List<$core.int>? meterRevision,
    LogicalUsage? usage,
    $core.List<$core.int>? rateCardRevision,
  }) {
    final result = UsageReceipt._();
    if (receiptId != null) result.receiptId = receiptId;
    if (modelProfile != null) result.modelProfile = modelProfile;
    if (meterRevision != null) result.meterRevision = meterRevision;
    if (usage != null) result.usage = usage;
    if (rateCardRevision != null) result.rateCardRevision = rateCardRevision;
    return result;
  }

  UsageReceipt._();

  factory UsageReceipt.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UsageReceipt()..mergeFromBuffer(data, registry);
  factory UsageReceipt.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UsageReceipt()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UsageReceipt',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: UsageReceipt.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'receiptId', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'modelProfile', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'meterRevision', $pb.PbFieldType.OY)
    ..aOM<LogicalUsage>(4, _omitFieldNames ? '' : 'usage',
        subBuilder: LogicalUsage.$_createMessage)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'rateCardRevision', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UsageReceipt clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UsageReceipt copyWith(void Function(UsageReceipt) updates) =>
      super.copyWith((message) => updates(message as UsageReceipt))
          as UsageReceipt;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use UsageReceipt() / UsageReceipt.new instead')
  static UsageReceipt create() => UsageReceipt._();
  static $pb.GeneratedMessage $_createMessage() => UsageReceipt._();
  @$core.override
  UsageReceipt createEmptyInstance() => UsageReceipt._();
  @$core.pragma('dart2js:noInline')
  static UsageReceipt getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<UsageReceipt>(
          UsageReceipt.$_createMessage);
  static UsageReceipt? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get receiptId => $_getN(0);
  @$pb.TagNumber(1)
  set receiptId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasReceiptId() => $_has(0);
  @$pb.TagNumber(1)
  void clearReceiptId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get modelProfile => $_getN(1);
  @$pb.TagNumber(2)
  set modelProfile($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasModelProfile() => $_has(1);
  @$pb.TagNumber(2)
  void clearModelProfile() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get meterRevision => $_getN(2);
  @$pb.TagNumber(3)
  set meterRevision($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMeterRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearMeterRevision() => $_clearField(3);

  /// Immutable final totals for this receipt, never an incremental charge delta.
  /// Meter semantics are independent of pricing or charging authorization.
  @$pb.TagNumber(4)
  LogicalUsage get usage => $_getN(3);
  @$pb.TagNumber(4)
  set usage(LogicalUsage value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasUsage() => $_has(3);
  @$pb.TagNumber(4)
  void clearUsage() => $_clearField(4);
  @$pb.TagNumber(4)
  LogicalUsage ensureUsage() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.List<$core.int> get rateCardRevision => $_getN(4);
  @$pb.TagNumber(5)
  set rateCardRevision($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasRateCardRevision() => $_has(4);
  @$pb.TagNumber(5)
  void clearRateCardRevision() => $_clearField(5);
}

class RunResult extends $pb.GeneratedMessage {
  factory RunResult({
    $core.List<$core.int>? output,
    ContextView? context,
    RunTerminal? terminal,
    UsageReceipt? receipt,
  }) {
    final result = RunResult._();
    if (output != null) result.output = output;
    if (context != null) result.context = context;
    if (terminal != null) result.terminal = terminal;
    if (receipt != null) result.receipt = receipt;
    return result;
  }

  RunResult._();

  factory RunResult.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunResult()..mergeFromBuffer(data, registry);
  factory RunResult.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunResult()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RunResult',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RunResult.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'output', $pb.PbFieldType.OY)
    ..aOM<ContextView>(2, _omitFieldNames ? '' : 'context',
        subBuilder: ContextView.$_createMessage)
    ..aE<RunTerminal>(3, _omitFieldNames ? '' : 'terminal',
        enumValues: RunTerminal.values)
    ..aOM<UsageReceipt>(4, _omitFieldNames ? '' : 'receipt',
        subBuilder: UsageReceipt.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunResult clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunResult copyWith(void Function(RunResult) updates) =>
      super.copyWith((message) => updates(message as RunResult)) as RunResult;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RunResult() / RunResult.new instead')
  static RunResult create() => RunResult._();
  static $pb.GeneratedMessage $_createMessage() => RunResult._();
  @$core.override
  RunResult createEmptyInstance() => RunResult._();
  @$core.pragma('dart2js:noInline')
  static RunResult getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RunResult>(RunResult.$_createMessage);
  static RunResult? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get output => $_getN(0);
  @$pb.TagNumber(1)
  set output($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOutput() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutput() => $_clearField(1);

  @$pb.TagNumber(2)
  ContextView get context => $_getN(1);
  @$pb.TagNumber(2)
  set context(ContextView value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasContext() => $_has(1);
  @$pb.TagNumber(2)
  void clearContext() => $_clearField(2);
  @$pb.TagNumber(2)
  ContextView ensureContext() => $_ensure(1);

  @$pb.TagNumber(3)
  RunTerminal get terminal => $_getN(2);
  @$pb.TagNumber(3)
  set terminal(RunTerminal value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasTerminal() => $_has(2);
  @$pb.TagNumber(3)
  void clearTerminal() => $_clearField(3);

  @$pb.TagNumber(4)
  UsageReceipt get receipt => $_getN(3);
  @$pb.TagNumber(4)
  set receipt(UsageReceipt value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasReceipt() => $_has(3);
  @$pb.TagNumber(4)
  void clearReceipt() => $_clearField(4);
  @$pb.TagNumber(4)
  UsageReceipt ensureReceipt() => $_ensure(3);
}

class RunView extends $pb.GeneratedMessage {
  factory RunView({
    $core.List<$core.int>? runId,
    $core.List<$core.int>? input,
    $core.String? model,
    $fixnum.Int64? lastSequence,
    $core.bool? cancellationRequested,
    RunResult? result,
  }) {
    final result$ = RunView._();
    if (runId != null) result$.runId = runId;
    if (input != null) result$.input = input;
    if (model != null) result$.model = model;
    if (lastSequence != null) result$.lastSequence = lastSequence;
    if (cancellationRequested != null)
      result$.cancellationRequested = cancellationRequested;
    if (result != null) result$.result = result;
    return result$;
  }

  RunView._();

  factory RunView.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunView()..mergeFromBuffer(data, registry);
  factory RunView.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunView()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RunView',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RunView.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'runId', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'input', $pb.PbFieldType.OY)
    ..aOS(3, _omitFieldNames ? '' : 'model')
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'lastSequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOB(5, _omitFieldNames ? '' : 'cancellationRequested')
    ..aOM<RunResult>(6, _omitFieldNames ? '' : 'result',
        subBuilder: RunResult.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunView clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunView copyWith(void Function(RunView) updates) =>
      super.copyWith((message) => updates(message as RunView)) as RunView;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RunView() / RunView.new instead')
  static RunView create() => RunView._();
  static $pb.GeneratedMessage $_createMessage() => RunView._();
  @$core.override
  RunView createEmptyInstance() => RunView._();
  @$core.pragma('dart2js:noInline')
  static RunView getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RunView>(RunView.$_createMessage);
  static RunView? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get runId => $_getN(0);
  @$pb.TagNumber(1)
  set runId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRunId() => $_has(0);
  @$pb.TagNumber(1)
  void clearRunId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get input => $_getN(1);
  @$pb.TagNumber(2)
  set input($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasInput() => $_has(1);
  @$pb.TagNumber(2)
  void clearInput() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get model => $_getSZ(2);
  @$pb.TagNumber(3)
  set model($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasModel() => $_has(2);
  @$pb.TagNumber(3)
  void clearModel() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get lastSequence => $_getI64(3);
  @$pb.TagNumber(4)
  set lastSequence($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasLastSequence() => $_has(3);
  @$pb.TagNumber(4)
  void clearLastSequence() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.bool get cancellationRequested => $_getBF(4);
  @$pb.TagNumber(5)
  set cancellationRequested($core.bool value) => $_setBool(4, value);
  @$pb.TagNumber(5)
  $core.bool hasCancellationRequested() => $_has(4);
  @$pb.TagNumber(5)
  void clearCancellationRequested() => $_clearField(5);

  @$pb.TagNumber(6)
  RunResult get result => $_getN(5);
  @$pb.TagNumber(6)
  set result(RunResult value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasResult() => $_has(5);
  @$pb.TagNumber(6)
  void clearResult() => $_clearField(6);
  @$pb.TagNumber(6)
  RunResult ensureResult() => $_ensure(5);
}

enum RunEvent_Event { output, usage, terminal, progress, notSet }

class RunEvent extends $pb.GeneratedMessage {
  factory RunEvent({
    $fixnum.Int64? sequence,
    $core.List<$core.int>? output,
    LogicalUsage? usage,
    RunTerminal? terminal,
    RunProgress? progress,
  }) {
    final result = RunEvent._();
    if (sequence != null) result.sequence = sequence;
    if (output != null) result.output = output;
    if (usage != null) result.usage = usage;
    if (terminal != null) result.terminal = terminal;
    if (progress != null) result.progress = progress;
    return result;
  }

  RunEvent._();

  factory RunEvent.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunEvent()..mergeFromBuffer(data, registry);
  factory RunEvent.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunEvent()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, RunEvent_Event> _RunEvent_EventByTag = {
    2: RunEvent_Event.output,
    3: RunEvent_Event.usage,
    4: RunEvent_Event.terminal,
    5: RunEvent_Event.progress,
    0: RunEvent_Event.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RunEvent',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RunEvent.$_createMessage)
    ..oo(0, [2, 3, 4, 5])
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'sequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'output', $pb.PbFieldType.OY)
    ..aOM<LogicalUsage>(3, _omitFieldNames ? '' : 'usage',
        subBuilder: LogicalUsage.$_createMessage)
    ..aE<RunTerminal>(4, _omitFieldNames ? '' : 'terminal',
        enumValues: RunTerminal.values)
    ..aOM<RunProgress>(5, _omitFieldNames ? '' : 'progress',
        subBuilder: RunProgress.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunEvent clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunEvent copyWith(void Function(RunEvent) updates) =>
      super.copyWith((message) => updates(message as RunEvent)) as RunEvent;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RunEvent() / RunEvent.new instead')
  static RunEvent create() => RunEvent._();
  static $pb.GeneratedMessage $_createMessage() => RunEvent._();
  @$core.override
  RunEvent createEmptyInstance() => RunEvent._();
  @$core.pragma('dart2js:noInline')
  static RunEvent getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RunEvent>(RunEvent.$_createMessage);
  static RunEvent? _defaultInstance;

  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  RunEvent_Event whichEvent() => _RunEvent_EventByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  void clearEvent() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $fixnum.Int64 get sequence => $_getI64(0);
  @$pb.TagNumber(1)
  set sequence($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSequence() => $_has(0);
  @$pb.TagNumber(1)
  void clearSequence() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get output => $_getN(1);
  @$pb.TagNumber(2)
  set output($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasOutput() => $_has(1);
  @$pb.TagNumber(2)
  void clearOutput() => $_clearField(2);

  /// Authoritative cumulative snapshot for this Run. Recovered/replayed watch
  /// events are snapshots of the same units, never incremental charge deltas.
  @$pb.TagNumber(3)
  LogicalUsage get usage => $_getN(2);
  @$pb.TagNumber(3)
  set usage(LogicalUsage value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasUsage() => $_has(2);
  @$pb.TagNumber(3)
  void clearUsage() => $_clearField(3);
  @$pb.TagNumber(3)
  LogicalUsage ensureUsage() => $_ensure(2);

  @$pb.TagNumber(4)
  RunTerminal get terminal => $_getN(3);
  @$pb.TagNumber(4)
  set terminal(RunTerminal value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasTerminal() => $_has(3);
  @$pb.TagNumber(4)
  void clearTerminal() => $_clearField(4);

  @$pb.TagNumber(5)
  RunProgress get progress => $_getN(4);
  @$pb.TagNumber(5)
  set progress(RunProgress value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasProgress() => $_has(4);
  @$pb.TagNumber(5)
  void clearProgress() => $_clearField(5);
  @$pb.TagNumber(5)
  RunProgress ensureProgress() => $_ensure(4);
}

class RunProgress extends $pb.GeneratedMessage {
  factory RunProgress({
    $core.String? kind,
  }) {
    final result = RunProgress._();
    if (kind != null) result.kind = kind;
    return result;
  }

  RunProgress._();

  factory RunProgress.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunProgress()..mergeFromBuffer(data, registry);
  factory RunProgress.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RunProgress()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RunProgress',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'inference.customer.v1'),
      createEmptyInstance: RunProgress.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'kind')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunProgress clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RunProgress copyWith(void Function(RunProgress) updates) =>
      super.copyWith((message) => updates(message as RunProgress))
          as RunProgress;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RunProgress() / RunProgress.new instead')
  static RunProgress create() => RunProgress._();
  static $pb.GeneratedMessage $_createMessage() => RunProgress._();
  @$core.override
  RunProgress createEmptyInstance() => RunProgress._();
  @$core.pragma('dart2js:noInline')
  static RunProgress getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RunProgress>(
          RunProgress.$_createMessage);
  static RunProgress? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get kind => $_getSZ(0);
  @$pb.TagNumber(1)
  set kind($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasKind() => $_has(0);
  @$pb.TagNumber(1)
  void clearKind() => $_clearField(1);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
