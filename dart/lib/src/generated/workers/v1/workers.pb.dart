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

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'workers.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'workers.pbenum.dart';

/// A digest identifies exact immutable JavaScript module bytes.
class CodeVersion extends $pb.GeneratedMessage {
  factory CodeVersion({
    $core.List<$core.int>? sha256,
    $fixnum.Int64? sizeBytes,
  }) {
    final result = CodeVersion._();
    if (sha256 != null) result.sha256 = sha256;
    if (sizeBytes != null) result.sizeBytes = sizeBytes;
    return result;
  }

  CodeVersion._();

  factory CodeVersion.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CodeVersion()..mergeFromBuffer(data, registry);
  factory CodeVersion.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CodeVersion()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CodeVersion',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: CodeVersion.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'sha256', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'sizeBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CodeVersion clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CodeVersion copyWith(void Function(CodeVersion) updates) =>
      super.copyWith((message) => updates(message as CodeVersion))
          as CodeVersion;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CodeVersion() / CodeVersion.new instead')
  static CodeVersion create() => CodeVersion._();
  static $pb.GeneratedMessage $_createMessage() => CodeVersion._();
  @$core.override
  CodeVersion createEmptyInstance() => CodeVersion._();
  @$core.pragma('dart2js:noInline')
  static CodeVersion getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CodeVersion>(
          CodeVersion.$_createMessage);
  static CodeVersion? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get sha256 => $_getN(0);
  @$pb.TagNumber(1)
  set sha256($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSha256() => $_has(0);
  @$pb.TagNumber(1)
  void clearSha256() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get sizeBytes => $_getI64(1);
  @$pb.TagNumber(2)
  set sizeBytes($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSizeBytes() => $_has(1);
  @$pb.TagNumber(2)
  void clearSizeBytes() => $_clearField(2);
}

/// A deployment alias may change; revision increases on every selection.
class Deployment extends $pb.GeneratedMessage {
  factory Deployment({
    $core.String? alias,
    CodeVersion? version,
    $fixnum.Int64? revision,
  }) {
    final result = Deployment._();
    if (alias != null) result.alias = alias;
    if (version != null) result.version = version;
    if (revision != null) result.revision = revision;
    return result;
  }

  Deployment._();

  factory Deployment.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Deployment()..mergeFromBuffer(data, registry);
  factory Deployment.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Deployment()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Deployment',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: Deployment.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'alias')
    ..aOM<CodeVersion>(2, _omitFieldNames ? '' : 'version',
        subBuilder: CodeVersion.$_createMessage)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Deployment clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Deployment copyWith(void Function(Deployment) updates) =>
      super.copyWith((message) => updates(message as Deployment)) as Deployment;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Deployment() / Deployment.new instead')
  static Deployment create() => Deployment._();
  static $pb.GeneratedMessage $_createMessage() => Deployment._();
  @$core.override
  Deployment createEmptyInstance() => Deployment._();
  @$core.pragma('dart2js:noInline')
  static Deployment getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Deployment>(Deployment.$_createMessage);
  static Deployment? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get alias => $_getSZ(0);
  @$pb.TagNumber(1)
  set alias($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasAlias() => $_has(0);
  @$pb.TagNumber(1)
  void clearAlias() => $_clearField(1);

  @$pb.TagNumber(2)
  CodeVersion get version => $_getN(1);
  @$pb.TagNumber(2)
  set version(CodeVersion value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);
  @$pb.TagNumber(2)
  CodeVersion ensureVersion() => $_ensure(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get revision => $_getI64(2);
  @$pb.TagNumber(3)
  set revision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearRevision() => $_clearField(3);
}

class PublishVersionRequest extends $pb.GeneratedMessage {
  factory PublishVersionRequest({
    $core.List<$core.int>? javascriptModule,
    $core.List<$core.int>? expectedSha256,
    $core.String? idempotencyKey,
  }) {
    final result = PublishVersionRequest._();
    if (javascriptModule != null) result.javascriptModule = javascriptModule;
    if (expectedSha256 != null) result.expectedSha256 = expectedSha256;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  PublishVersionRequest._();

  factory PublishVersionRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PublishVersionRequest()..mergeFromBuffer(data, registry);
  factory PublishVersionRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PublishVersionRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PublishVersionRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: PublishVersionRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'javascriptModule', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'expectedSha256', $pb.PbFieldType.OY)
    ..aOS(3, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PublishVersionRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PublishVersionRequest copyWith(
          void Function(PublishVersionRequest) updates) =>
      super.copyWith((message) => updates(message as PublishVersionRequest))
          as PublishVersionRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use PublishVersionRequest() / PublishVersionRequest.new instead')
  static PublishVersionRequest create() => PublishVersionRequest._();
  static $pb.GeneratedMessage $_createMessage() => PublishVersionRequest._();
  @$core.override
  PublishVersionRequest createEmptyInstance() => PublishVersionRequest._();
  @$core.pragma('dart2js:noInline')
  static PublishVersionRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PublishVersionRequest>(
          PublishVersionRequest.$_createMessage);
  static PublishVersionRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get javascriptModule => $_getN(0);
  @$pb.TagNumber(1)
  set javascriptModule($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasJavascriptModule() => $_has(0);
  @$pb.TagNumber(1)
  void clearJavascriptModule() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get expectedSha256 => $_getN(1);
  @$pb.TagNumber(2)
  set expectedSha256($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasExpectedSha256() => $_has(1);
  @$pb.TagNumber(2)
  void clearExpectedSha256() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get idempotencyKey => $_getSZ(2);
  @$pb.TagNumber(3)
  set idempotencyKey($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIdempotencyKey() => $_has(2);
  @$pb.TagNumber(3)
  void clearIdempotencyKey() => $_clearField(3);
}

class PublishVersionResponse extends $pb.GeneratedMessage {
  factory PublishVersionResponse({
    CodeVersion? version,
  }) {
    final result = PublishVersionResponse._();
    if (version != null) result.version = version;
    return result;
  }

  PublishVersionResponse._();

  factory PublishVersionResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PublishVersionResponse()..mergeFromBuffer(data, registry);
  factory PublishVersionResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PublishVersionResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PublishVersionResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: PublishVersionResponse.$_createMessage)
    ..aOM<CodeVersion>(1, _omitFieldNames ? '' : 'version',
        subBuilder: CodeVersion.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PublishVersionResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PublishVersionResponse copyWith(
          void Function(PublishVersionResponse) updates) =>
      super.copyWith((message) => updates(message as PublishVersionResponse))
          as PublishVersionResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use PublishVersionResponse() / PublishVersionResponse.new instead')
  static PublishVersionResponse create() => PublishVersionResponse._();
  static $pb.GeneratedMessage $_createMessage() => PublishVersionResponse._();
  @$core.override
  PublishVersionResponse createEmptyInstance() => PublishVersionResponse._();
  @$core.pragma('dart2js:noInline')
  static PublishVersionResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PublishVersionResponse>(
          PublishVersionResponse.$_createMessage);
  static PublishVersionResponse? _defaultInstance;

  @$pb.TagNumber(1)
  CodeVersion get version => $_getN(0);
  @$pb.TagNumber(1)
  set version(CodeVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasVersion() => $_has(0);
  @$pb.TagNumber(1)
  void clearVersion() => $_clearField(1);
  @$pb.TagNumber(1)
  CodeVersion ensureVersion() => $_ensure(0);
}

class SelectDeploymentRequest extends $pb.GeneratedMessage {
  factory SelectDeploymentRequest({
    $core.String? alias,
    $core.List<$core.int>? versionSha256,
    $fixnum.Int64? expectedRevision,
    $core.String? idempotencyKey,
  }) {
    final result = SelectDeploymentRequest._();
    if (alias != null) result.alias = alias;
    if (versionSha256 != null) result.versionSha256 = versionSha256;
    if (expectedRevision != null) result.expectedRevision = expectedRevision;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  SelectDeploymentRequest._();

  factory SelectDeploymentRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SelectDeploymentRequest()..mergeFromBuffer(data, registry);
  factory SelectDeploymentRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SelectDeploymentRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SelectDeploymentRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: SelectDeploymentRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'alias')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'versionSha256', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'expectedRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(4, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SelectDeploymentRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SelectDeploymentRequest copyWith(
          void Function(SelectDeploymentRequest) updates) =>
      super.copyWith((message) => updates(message as SelectDeploymentRequest))
          as SelectDeploymentRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use SelectDeploymentRequest() / SelectDeploymentRequest.new instead')
  static SelectDeploymentRequest create() => SelectDeploymentRequest._();
  static $pb.GeneratedMessage $_createMessage() => SelectDeploymentRequest._();
  @$core.override
  SelectDeploymentRequest createEmptyInstance() => SelectDeploymentRequest._();
  @$core.pragma('dart2js:noInline')
  static SelectDeploymentRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SelectDeploymentRequest>(
          SelectDeploymentRequest.$_createMessage);
  static SelectDeploymentRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get alias => $_getSZ(0);
  @$pb.TagNumber(1)
  set alias($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasAlias() => $_has(0);
  @$pb.TagNumber(1)
  void clearAlias() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get versionSha256 => $_getN(1);
  @$pb.TagNumber(2)
  set versionSha256($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVersionSha256() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersionSha256() => $_clearField(2);

  /// Omitted means create only if absent. A present positive value selects only
  /// when it matches the current revision; every successful selection advances it.
  @$pb.TagNumber(3)
  $fixnum.Int64 get expectedRevision => $_getI64(2);
  @$pb.TagNumber(3)
  set expectedRevision($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasExpectedRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearExpectedRevision() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get idempotencyKey => $_getSZ(3);
  @$pb.TagNumber(4)
  set idempotencyKey($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIdempotencyKey() => $_has(3);
  @$pb.TagNumber(4)
  void clearIdempotencyKey() => $_clearField(4);
}

class SelectDeploymentResponse extends $pb.GeneratedMessage {
  factory SelectDeploymentResponse({
    Deployment? deployment,
  }) {
    final result = SelectDeploymentResponse._();
    if (deployment != null) result.deployment = deployment;
    return result;
  }

  SelectDeploymentResponse._();

  factory SelectDeploymentResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SelectDeploymentResponse()..mergeFromBuffer(data, registry);
  factory SelectDeploymentResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SelectDeploymentResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SelectDeploymentResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: SelectDeploymentResponse.$_createMessage)
    ..aOM<Deployment>(1, _omitFieldNames ? '' : 'deployment',
        subBuilder: Deployment.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SelectDeploymentResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SelectDeploymentResponse copyWith(
          void Function(SelectDeploymentResponse) updates) =>
      super.copyWith((message) => updates(message as SelectDeploymentResponse))
          as SelectDeploymentResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use SelectDeploymentResponse() / SelectDeploymentResponse.new instead')
  static SelectDeploymentResponse create() => SelectDeploymentResponse._();
  static $pb.GeneratedMessage $_createMessage() => SelectDeploymentResponse._();
  @$core.override
  SelectDeploymentResponse createEmptyInstance() =>
      SelectDeploymentResponse._();
  @$core.pragma('dart2js:noInline')
  static SelectDeploymentResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SelectDeploymentResponse>(
          SelectDeploymentResponse.$_createMessage);
  static SelectDeploymentResponse? _defaultInstance;

  @$pb.TagNumber(1)
  Deployment get deployment => $_getN(0);
  @$pb.TagNumber(1)
  set deployment(Deployment value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasDeployment() => $_has(0);
  @$pb.TagNumber(1)
  void clearDeployment() => $_clearField(1);
  @$pb.TagNumber(1)
  Deployment ensureDeployment() => $_ensure(0);
}

/// Logical S3 object selected and privately retained at durable job acceptance.
/// Retries read the same retained bytes even if this public key is replaced.
class ObjectRef extends $pb.GeneratedMessage {
  factory ObjectRef({
    $core.String? bucket,
    $core.String? key,
  }) {
    final result = ObjectRef._();
    if (bucket != null) result.bucket = bucket;
    if (key != null) result.key = key;
    return result;
  }

  ObjectRef._();

  factory ObjectRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObjectRef()..mergeFromBuffer(data, registry);
  factory ObjectRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObjectRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ObjectRef',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: ObjectRef.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'bucket')
    ..aOS(2, _omitFieldNames ? '' : 'key')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObjectRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObjectRef copyWith(void Function(ObjectRef) updates) =>
      super.copyWith((message) => updates(message as ObjectRef)) as ObjectRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ObjectRef() / ObjectRef.new instead')
  static ObjectRef create() => ObjectRef._();
  static $pb.GeneratedMessage $_createMessage() => ObjectRef._();
  @$core.override
  ObjectRef createEmptyInstance() => ObjectRef._();
  @$core.pragma('dart2js:noInline')
  static ObjectRef getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ObjectRef>(ObjectRef.$_createMessage);
  static ObjectRef? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get bucket => $_getSZ(0);
  @$pb.TagNumber(1)
  set bucket($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get key => $_getSZ(1);
  @$pb.TagNumber(2)
  set key($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearKey() => $_clearField(2);
}

enum Payload_Source { inlineBytes, object, notSet }

class Payload extends $pb.GeneratedMessage {
  factory Payload({
    $core.List<$core.int>? inlineBytes,
    ObjectRef? object,
  }) {
    final result = Payload._();
    if (inlineBytes != null) result.inlineBytes = inlineBytes;
    if (object != null) result.object = object;
    return result;
  }

  Payload._();

  factory Payload.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Payload()..mergeFromBuffer(data, registry);
  factory Payload.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Payload()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, Payload_Source> _Payload_SourceByTag = {
    1: Payload_Source.inlineBytes,
    2: Payload_Source.object,
    0: Payload_Source.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Payload',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: Payload.$_createMessage)
    ..oo(0, [1, 2])
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'inlineBytes', $pb.PbFieldType.OY)
    ..aOM<ObjectRef>(2, _omitFieldNames ? '' : 'object',
        subBuilder: ObjectRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Payload clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Payload copyWith(void Function(Payload) updates) =>
      super.copyWith((message) => updates(message as Payload)) as Payload;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Payload() / Payload.new instead')
  static Payload create() => Payload._();
  static $pb.GeneratedMessage $_createMessage() => Payload._();
  @$core.override
  Payload createEmptyInstance() => Payload._();
  @$core.pragma('dart2js:noInline')
  static Payload getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Payload>(Payload.$_createMessage);
  static Payload? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  Payload_Source whichSource() => _Payload_SourceByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearSource() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.List<$core.int> get inlineBytes => $_getN(0);
  @$pb.TagNumber(1)
  set inlineBytes($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasInlineBytes() => $_has(0);
  @$pb.TagNumber(1)
  void clearInlineBytes() => $_clearField(1);

  @$pb.TagNumber(2)
  ObjectRef get object => $_getN(1);
  @$pb.TagNumber(2)
  set object(ObjectRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasObject() => $_has(1);
  @$pb.TagNumber(2)
  void clearObject() => $_clearField(2);
  @$pb.TagNumber(2)
  ObjectRef ensureObject() => $_ensure(1);
}

/// Exact accepted job output, bounded by JobLimits.output_bytes. Storage and
/// retention are service-owned; no replaceable public Object pointer is exposed.
class JobResult extends $pb.GeneratedMessage {
  factory JobResult({
    $core.List<$core.int>? body,
  }) {
    final result = JobResult._();
    if (body != null) result.body = body;
    return result;
  }

  JobResult._();

  factory JobResult.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JobResult()..mergeFromBuffer(data, registry);
  factory JobResult.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JobResult()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'JobResult',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: JobResult.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JobResult clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JobResult copyWith(void Function(JobResult) updates) =>
      super.copyWith((message) => updates(message as JobResult)) as JobResult;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use JobResult() / JobResult.new instead')
  static JobResult create() => JobResult._();
  static $pb.GeneratedMessage $_createMessage() => JobResult._();
  @$core.override
  JobResult createEmptyInstance() => JobResult._();
  @$core.pragma('dart2js:noInline')
  static JobResult getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<JobResult>(JobResult.$_createMessage);
  static JobResult? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get body => $_getN(0);
  @$pb.TagNumber(1)
  set body($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasBody() => $_has(0);
  @$pb.TagNumber(1)
  void clearBody() => $_clearField(1);
}

class JobLimits extends $pb.GeneratedMessage {
  factory JobLimits({
    $fixnum.Int64? timeoutMillis,
    $fixnum.Int64? memoryBytes,
    $fixnum.Int64? outputBytes,
  }) {
    final result = JobLimits._();
    if (timeoutMillis != null) result.timeoutMillis = timeoutMillis;
    if (memoryBytes != null) result.memoryBytes = memoryBytes;
    if (outputBytes != null) result.outputBytes = outputBytes;
    return result;
  }

  JobLimits._();

  factory JobLimits.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JobLimits()..mergeFromBuffer(data, registry);
  factory JobLimits.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JobLimits()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'JobLimits',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: JobLimits.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'timeoutMillis', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'memoryBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'outputBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JobLimits clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JobLimits copyWith(void Function(JobLimits) updates) =>
      super.copyWith((message) => updates(message as JobLimits)) as JobLimits;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use JobLimits() / JobLimits.new instead')
  static JobLimits create() => JobLimits._();
  static $pb.GeneratedMessage $_createMessage() => JobLimits._();
  @$core.override
  JobLimits createEmptyInstance() => JobLimits._();
  @$core.pragma('dart2js:noInline')
  static JobLimits getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<JobLimits>(JobLimits.$_createMessage);
  static JobLimits? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get timeoutMillis => $_getI64(0);
  @$pb.TagNumber(1)
  set timeoutMillis($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasTimeoutMillis() => $_has(0);
  @$pb.TagNumber(1)
  void clearTimeoutMillis() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get memoryBytes => $_getI64(1);
  @$pb.TagNumber(2)
  set memoryBytes($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMemoryBytes() => $_has(1);
  @$pb.TagNumber(2)
  void clearMemoryBytes() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get outputBytes => $_getI64(2);
  @$pb.TagNumber(3)
  set outputBytes($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasOutputBytes() => $_has(2);
  @$pb.TagNumber(3)
  void clearOutputBytes() => $_clearField(3);
}

class RetryPolicy extends $pb.GeneratedMessage {
  factory RetryPolicy({
    $core.int? maxAttempts,
    $fixnum.Int64? backoffMillis,
  }) {
    final result = RetryPolicy._();
    if (maxAttempts != null) result.maxAttempts = maxAttempts;
    if (backoffMillis != null) result.backoffMillis = backoffMillis;
    return result;
  }

  RetryPolicy._();

  factory RetryPolicy.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetryPolicy()..mergeFromBuffer(data, registry);
  factory RetryPolicy.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetryPolicy()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RetryPolicy',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: RetryPolicy.$_createMessage)
    ..aI(1, _omitFieldNames ? '' : 'maxAttempts',
        fieldType: $pb.PbFieldType.OU3)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'backoffMillis', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetryPolicy clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetryPolicy copyWith(void Function(RetryPolicy) updates) =>
      super.copyWith((message) => updates(message as RetryPolicy))
          as RetryPolicy;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RetryPolicy() / RetryPolicy.new instead')
  static RetryPolicy create() => RetryPolicy._();
  static $pb.GeneratedMessage $_createMessage() => RetryPolicy._();
  @$core.override
  RetryPolicy createEmptyInstance() => RetryPolicy._();
  @$core.pragma('dart2js:noInline')
  static RetryPolicy getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RetryPolicy>(
          RetryPolicy.$_createMessage);
  static RetryPolicy? _defaultInstance;

  @$pb.TagNumber(1)
  $core.int get maxAttempts => $_getIZ(0);
  @$pb.TagNumber(1)
  set maxAttempts($core.int value) => $_setUnsignedInt32(0, value);
  @$pb.TagNumber(1)
  $core.bool hasMaxAttempts() => $_has(0);
  @$pb.TagNumber(1)
  void clearMaxAttempts() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get backoffMillis => $_getI64(1);
  @$pb.TagNumber(2)
  set backoffMillis($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBackoffMillis() => $_has(1);
  @$pb.TagNumber(2)
  void clearBackoffMillis() => $_clearField(2);
}

enum JobTarget_Target { deploymentAlias, versionSha256, notSet }

class JobTarget extends $pb.GeneratedMessage {
  factory JobTarget({
    $core.String? deploymentAlias,
    $core.List<$core.int>? versionSha256,
  }) {
    final result = JobTarget._();
    if (deploymentAlias != null) result.deploymentAlias = deploymentAlias;
    if (versionSha256 != null) result.versionSha256 = versionSha256;
    return result;
  }

  JobTarget._();

  factory JobTarget.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JobTarget()..mergeFromBuffer(data, registry);
  factory JobTarget.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JobTarget()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, JobTarget_Target> _JobTarget_TargetByTag = {
    1: JobTarget_Target.deploymentAlias,
    2: JobTarget_Target.versionSha256,
    0: JobTarget_Target.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'JobTarget',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: JobTarget.$_createMessage)
    ..oo(0, [1, 2])
    ..aOS(1, _omitFieldNames ? '' : 'deploymentAlias')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'versionSha256', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JobTarget clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JobTarget copyWith(void Function(JobTarget) updates) =>
      super.copyWith((message) => updates(message as JobTarget)) as JobTarget;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use JobTarget() / JobTarget.new instead')
  static JobTarget create() => JobTarget._();
  static $pb.GeneratedMessage $_createMessage() => JobTarget._();
  @$core.override
  JobTarget createEmptyInstance() => JobTarget._();
  @$core.pragma('dart2js:noInline')
  static JobTarget getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<JobTarget>(JobTarget.$_createMessage);
  static JobTarget? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  JobTarget_Target whichTarget() => _JobTarget_TargetByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearTarget() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.String get deploymentAlias => $_getSZ(0);
  @$pb.TagNumber(1)
  set deploymentAlias($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasDeploymentAlias() => $_has(0);
  @$pb.TagNumber(1)
  void clearDeploymentAlias() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get versionSha256 => $_getN(1);
  @$pb.TagNumber(2)
  set versionSha256($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasVersionSha256() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersionSha256() => $_clearField(2);
}

/// Accepted input is delivered to default.run, never to default.fetch.
/// The same job ID and input recur on retry; attempt numbering starts at one.
class SubmitJobRequest extends $pb.GeneratedMessage {
  factory SubmitJobRequest({
    JobTarget? target,
    Payload? input,
    JobLimits? limits,
    RetryPolicy? retry,
    $core.String? idempotencyKey,
  }) {
    final result = SubmitJobRequest._();
    if (target != null) result.target = target;
    if (input != null) result.input = input;
    if (limits != null) result.limits = limits;
    if (retry != null) result.retry = retry;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  SubmitJobRequest._();

  factory SubmitJobRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubmitJobRequest()..mergeFromBuffer(data, registry);
  factory SubmitJobRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubmitJobRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SubmitJobRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: SubmitJobRequest.$_createMessage)
    ..aOM<JobTarget>(1, _omitFieldNames ? '' : 'target',
        subBuilder: JobTarget.$_createMessage)
    ..aOM<Payload>(2, _omitFieldNames ? '' : 'input',
        subBuilder: Payload.$_createMessage)
    ..aOM<JobLimits>(3, _omitFieldNames ? '' : 'limits',
        subBuilder: JobLimits.$_createMessage)
    ..aOM<RetryPolicy>(4, _omitFieldNames ? '' : 'retry',
        subBuilder: RetryPolicy.$_createMessage)
    ..aOS(5, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubmitJobRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubmitJobRequest copyWith(void Function(SubmitJobRequest) updates) =>
      super.copyWith((message) => updates(message as SubmitJobRequest))
          as SubmitJobRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SubmitJobRequest() / SubmitJobRequest.new instead')
  static SubmitJobRequest create() => SubmitJobRequest._();
  static $pb.GeneratedMessage $_createMessage() => SubmitJobRequest._();
  @$core.override
  SubmitJobRequest createEmptyInstance() => SubmitJobRequest._();
  @$core.pragma('dart2js:noInline')
  static SubmitJobRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SubmitJobRequest>(
          SubmitJobRequest.$_createMessage);
  static SubmitJobRequest? _defaultInstance;

  @$pb.TagNumber(1)
  JobTarget get target => $_getN(0);
  @$pb.TagNumber(1)
  set target(JobTarget value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasTarget() => $_has(0);
  @$pb.TagNumber(1)
  void clearTarget() => $_clearField(1);
  @$pb.TagNumber(1)
  JobTarget ensureTarget() => $_ensure(0);

  @$pb.TagNumber(2)
  Payload get input => $_getN(1);
  @$pb.TagNumber(2)
  set input(Payload value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasInput() => $_has(1);
  @$pb.TagNumber(2)
  void clearInput() => $_clearField(2);
  @$pb.TagNumber(2)
  Payload ensureInput() => $_ensure(1);

  @$pb.TagNumber(3)
  JobLimits get limits => $_getN(2);
  @$pb.TagNumber(3)
  set limits(JobLimits value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasLimits() => $_has(2);
  @$pb.TagNumber(3)
  void clearLimits() => $_clearField(3);
  @$pb.TagNumber(3)
  JobLimits ensureLimits() => $_ensure(2);

  @$pb.TagNumber(4)
  RetryPolicy get retry => $_getN(3);
  @$pb.TagNumber(4)
  set retry(RetryPolicy value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasRetry() => $_has(3);
  @$pb.TagNumber(4)
  void clearRetry() => $_clearField(4);
  @$pb.TagNumber(4)
  RetryPolicy ensureRetry() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.String get idempotencyKey => $_getSZ(4);
  @$pb.TagNumber(5)
  set idempotencyKey($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasIdempotencyKey() => $_has(4);
  @$pb.TagNumber(5)
  void clearIdempotencyKey() => $_clearField(5);
}

class SubmitJobResponse extends $pb.GeneratedMessage {
  factory SubmitJobResponse({
    JobObservation? job,
  }) {
    final result = SubmitJobResponse._();
    if (job != null) result.job = job;
    return result;
  }

  SubmitJobResponse._();

  factory SubmitJobResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubmitJobResponse()..mergeFromBuffer(data, registry);
  factory SubmitJobResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SubmitJobResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SubmitJobResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: SubmitJobResponse.$_createMessage)
    ..aOM<JobObservation>(1, _omitFieldNames ? '' : 'job',
        subBuilder: JobObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubmitJobResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SubmitJobResponse copyWith(void Function(SubmitJobResponse) updates) =>
      super.copyWith((message) => updates(message as SubmitJobResponse))
          as SubmitJobResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SubmitJobResponse() / SubmitJobResponse.new instead')
  static SubmitJobResponse create() => SubmitJobResponse._();
  static $pb.GeneratedMessage $_createMessage() => SubmitJobResponse._();
  @$core.override
  SubmitJobResponse createEmptyInstance() => SubmitJobResponse._();
  @$core.pragma('dart2js:noInline')
  static SubmitJobResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SubmitJobResponse>(
          SubmitJobResponse.$_createMessage);
  static SubmitJobResponse? _defaultInstance;

  @$pb.TagNumber(1)
  JobObservation get job => $_getN(0);
  @$pb.TagNumber(1)
  set job(JobObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasJob() => $_has(0);
  @$pb.TagNumber(1)
  void clearJob() => $_clearField(1);
  @$pb.TagNumber(1)
  JobObservation ensureJob() => $_ensure(0);
}

class JobObservation extends $pb.GeneratedMessage {
  factory JobObservation({
    $core.String? jobId,
    JobState? state,
    $core.List<$core.int>? resolvedSha256,
    $core.int? attempt,
    JobResult? result,
    $core.String? failureCode,
    $core.bool? cancellationRequested,
  }) {
    final result$ = JobObservation._();
    if (jobId != null) result$.jobId = jobId;
    if (state != null) result$.state = state;
    if (resolvedSha256 != null) result$.resolvedSha256 = resolvedSha256;
    if (attempt != null) result$.attempt = attempt;
    if (result != null) result$.result = result;
    if (failureCode != null) result$.failureCode = failureCode;
    if (cancellationRequested != null)
      result$.cancellationRequested = cancellationRequested;
    return result$;
  }

  JobObservation._();

  factory JobObservation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JobObservation()..mergeFromBuffer(data, registry);
  factory JobObservation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JobObservation()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'JobObservation',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: JobObservation.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'jobId')
    ..aE<JobState>(2, _omitFieldNames ? '' : 'state',
        enumValues: JobState.values)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'resolvedSha256', $pb.PbFieldType.OY)
    ..aI(4, _omitFieldNames ? '' : 'attempt', fieldType: $pb.PbFieldType.OU3)
    ..aOM<JobResult>(5, _omitFieldNames ? '' : 'result',
        subBuilder: JobResult.$_createMessage)
    ..aOS(6, _omitFieldNames ? '' : 'failureCode')
    ..aOB(7, _omitFieldNames ? '' : 'cancellationRequested')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JobObservation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JobObservation copyWith(void Function(JobObservation) updates) =>
      super.copyWith((message) => updates(message as JobObservation))
          as JobObservation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use JobObservation() / JobObservation.new instead')
  static JobObservation create() => JobObservation._();
  static $pb.GeneratedMessage $_createMessage() => JobObservation._();
  @$core.override
  JobObservation createEmptyInstance() => JobObservation._();
  @$core.pragma('dart2js:noInline')
  static JobObservation getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<JobObservation>(
          JobObservation.$_createMessage);
  static JobObservation? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get jobId => $_getSZ(0);
  @$pb.TagNumber(1)
  set jobId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasJobId() => $_has(0);
  @$pb.TagNumber(1)
  void clearJobId() => $_clearField(1);

  @$pb.TagNumber(2)
  JobState get state => $_getN(1);
  @$pb.TagNumber(2)
  set state(JobState value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasState() => $_has(1);
  @$pb.TagNumber(2)
  void clearState() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get resolvedSha256 => $_getN(2);
  @$pb.TagNumber(3)
  set resolvedSha256($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasResolvedSha256() => $_has(2);
  @$pb.TagNumber(3)
  void clearResolvedSha256() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.int get attempt => $_getIZ(3);
  @$pb.TagNumber(4)
  set attempt($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasAttempt() => $_has(3);
  @$pb.TagNumber(4)
  void clearAttempt() => $_clearField(4);

  @$pb.TagNumber(5)
  JobResult get result => $_getN(4);
  @$pb.TagNumber(5)
  set result(JobResult value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasResult() => $_has(4);
  @$pb.TagNumber(5)
  void clearResult() => $_clearField(5);
  @$pb.TagNumber(5)
  JobResult ensureResult() => $_ensure(4);

  @$pb.TagNumber(6)
  $core.String get failureCode => $_getSZ(5);
  @$pb.TagNumber(6)
  set failureCode($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasFailureCode() => $_has(5);
  @$pb.TagNumber(6)
  void clearFailureCode() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.bool get cancellationRequested => $_getBF(6);
  @$pb.TagNumber(7)
  set cancellationRequested($core.bool value) => $_setBool(6, value);
  @$pb.TagNumber(7)
  $core.bool hasCancellationRequested() => $_has(6);
  @$pb.TagNumber(7)
  void clearCancellationRequested() => $_clearField(7);
}

class InspectJobRequest extends $pb.GeneratedMessage {
  factory InspectJobRequest({
    $core.String? jobId,
  }) {
    final result = InspectJobRequest._();
    if (jobId != null) result.jobId = jobId;
    return result;
  }

  InspectJobRequest._();

  factory InspectJobRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectJobRequest()..mergeFromBuffer(data, registry);
  factory InspectJobRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectJobRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectJobRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: InspectJobRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'jobId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectJobRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectJobRequest copyWith(void Function(InspectJobRequest) updates) =>
      super.copyWith((message) => updates(message as InspectJobRequest))
          as InspectJobRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InspectJobRequest() / InspectJobRequest.new instead')
  static InspectJobRequest create() => InspectJobRequest._();
  static $pb.GeneratedMessage $_createMessage() => InspectJobRequest._();
  @$core.override
  InspectJobRequest createEmptyInstance() => InspectJobRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectJobRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<InspectJobRequest>(
          InspectJobRequest.$_createMessage);
  static InspectJobRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get jobId => $_getSZ(0);
  @$pb.TagNumber(1)
  set jobId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasJobId() => $_has(0);
  @$pb.TagNumber(1)
  void clearJobId() => $_clearField(1);
}

class InspectJobResponse extends $pb.GeneratedMessage {
  factory InspectJobResponse({
    JobObservation? job,
  }) {
    final result = InspectJobResponse._();
    if (job != null) result.job = job;
    return result;
  }

  InspectJobResponse._();

  factory InspectJobResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectJobResponse()..mergeFromBuffer(data, registry);
  factory InspectJobResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectJobResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectJobResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: InspectJobResponse.$_createMessage)
    ..aOM<JobObservation>(1, _omitFieldNames ? '' : 'job',
        subBuilder: JobObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectJobResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectJobResponse copyWith(void Function(InspectJobResponse) updates) =>
      super.copyWith((message) => updates(message as InspectJobResponse))
          as InspectJobResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InspectJobResponse() / InspectJobResponse.new instead')
  static InspectJobResponse create() => InspectJobResponse._();
  static $pb.GeneratedMessage $_createMessage() => InspectJobResponse._();
  @$core.override
  InspectJobResponse createEmptyInstance() => InspectJobResponse._();
  @$core.pragma('dart2js:noInline')
  static InspectJobResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectJobResponse>(
          InspectJobResponse.$_createMessage);
  static InspectJobResponse? _defaultInstance;

  @$pb.TagNumber(1)
  JobObservation get job => $_getN(0);
  @$pb.TagNumber(1)
  set job(JobObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasJob() => $_has(0);
  @$pb.TagNumber(1)
  void clearJob() => $_clearField(1);
  @$pb.TagNumber(1)
  JobObservation ensureJob() => $_ensure(0);
}

class CancelJobRequest extends $pb.GeneratedMessage {
  factory CancelJobRequest({
    $core.String? jobId,
    $core.String? idempotencyKey,
  }) {
    final result = CancelJobRequest._();
    if (jobId != null) result.jobId = jobId;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  CancelJobRequest._();

  factory CancelJobRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CancelJobRequest()..mergeFromBuffer(data, registry);
  factory CancelJobRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CancelJobRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CancelJobRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: CancelJobRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'jobId')
    ..aOS(2, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CancelJobRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CancelJobRequest copyWith(void Function(CancelJobRequest) updates) =>
      super.copyWith((message) => updates(message as CancelJobRequest))
          as CancelJobRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CancelJobRequest() / CancelJobRequest.new instead')
  static CancelJobRequest create() => CancelJobRequest._();
  static $pb.GeneratedMessage $_createMessage() => CancelJobRequest._();
  @$core.override
  CancelJobRequest createEmptyInstance() => CancelJobRequest._();
  @$core.pragma('dart2js:noInline')
  static CancelJobRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CancelJobRequest>(
          CancelJobRequest.$_createMessage);
  static CancelJobRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get jobId => $_getSZ(0);
  @$pb.TagNumber(1)
  set jobId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasJobId() => $_has(0);
  @$pb.TagNumber(1)
  void clearJobId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get idempotencyKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set idempotencyKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
}

class CancelJobResponse extends $pb.GeneratedMessage {
  factory CancelJobResponse({
    JobObservation? job,
  }) {
    final result = CancelJobResponse._();
    if (job != null) result.job = job;
    return result;
  }

  CancelJobResponse._();

  factory CancelJobResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CancelJobResponse()..mergeFromBuffer(data, registry);
  factory CancelJobResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CancelJobResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CancelJobResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: CancelJobResponse.$_createMessage)
    ..aOM<JobObservation>(1, _omitFieldNames ? '' : 'job',
        subBuilder: JobObservation.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CancelJobResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CancelJobResponse copyWith(void Function(CancelJobResponse) updates) =>
      super.copyWith((message) => updates(message as CancelJobResponse))
          as CancelJobResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CancelJobResponse() / CancelJobResponse.new instead')
  static CancelJobResponse create() => CancelJobResponse._();
  static $pb.GeneratedMessage $_createMessage() => CancelJobResponse._();
  @$core.override
  CancelJobResponse createEmptyInstance() => CancelJobResponse._();
  @$core.pragma('dart2js:noInline')
  static CancelJobResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CancelJobResponse>(
          CancelJobResponse.$_createMessage);
  static CancelJobResponse? _defaultInstance;

  @$pb.TagNumber(1)
  JobObservation get job => $_getN(0);
  @$pb.TagNumber(1)
  set job(JobObservation value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasJob() => $_has(0);
  @$pb.TagNumber(1)
  void clearJob() => $_clearField(1);
  @$pb.TagNumber(1)
  JobObservation ensureJob() => $_ensure(0);
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
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

/// Invocation is ordinary HTTP work, not durable job acceptance.
class InvokeVersionRequest extends $pb.GeneratedMessage {
  factory InvokeVersionRequest({
    $core.List<$core.int>? versionSha256,
    $core.String? method,
    $core.String? url,
    $core.Iterable<Header>? headers,
    $core.List<$core.int>? body,
  }) {
    final result = InvokeVersionRequest._();
    if (versionSha256 != null) result.versionSha256 = versionSha256;
    if (method != null) result.method = method;
    if (url != null) result.url = url;
    if (headers != null) result.headers.addAll(headers);
    if (body != null) result.body = body;
    return result;
  }

  InvokeVersionRequest._();

  factory InvokeVersionRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeVersionRequest()..mergeFromBuffer(data, registry);
  factory InvokeVersionRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeVersionRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InvokeVersionRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: InvokeVersionRequest.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'versionSha256', $pb.PbFieldType.OY)
    ..aOS(2, _omitFieldNames ? '' : 'method')
    ..aOS(3, _omitFieldNames ? '' : 'url')
    ..pPM<Header>(4, _omitFieldNames ? '' : 'headers',
        subBuilder: Header.$_createMessage)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeVersionRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeVersionRequest copyWith(void Function(InvokeVersionRequest) updates) =>
      super.copyWith((message) => updates(message as InvokeVersionRequest))
          as InvokeVersionRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InvokeVersionRequest() / InvokeVersionRequest.new instead')
  static InvokeVersionRequest create() => InvokeVersionRequest._();
  static $pb.GeneratedMessage $_createMessage() => InvokeVersionRequest._();
  @$core.override
  InvokeVersionRequest createEmptyInstance() => InvokeVersionRequest._();
  @$core.pragma('dart2js:noInline')
  static InvokeVersionRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InvokeVersionRequest>(
          InvokeVersionRequest.$_createMessage);
  static InvokeVersionRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get versionSha256 => $_getN(0);
  @$pb.TagNumber(1)
  set versionSha256($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasVersionSha256() => $_has(0);
  @$pb.TagNumber(1)
  void clearVersionSha256() => $_clearField(1);

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
  $pb.PbList<Header> get headers => $_getList(3);

  @$pb.TagNumber(5)
  $core.List<$core.int> get body => $_getN(4);
  @$pb.TagNumber(5)
  set body($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasBody() => $_has(4);
  @$pb.TagNumber(5)
  void clearBody() => $_clearField(5);
}

class InvokeDeploymentRequest extends $pb.GeneratedMessage {
  factory InvokeDeploymentRequest({
    $core.String? alias,
    $core.String? method,
    $core.String? url,
    $core.Iterable<Header>? headers,
    $core.List<$core.int>? body,
  }) {
    final result = InvokeDeploymentRequest._();
    if (alias != null) result.alias = alias;
    if (method != null) result.method = method;
    if (url != null) result.url = url;
    if (headers != null) result.headers.addAll(headers);
    if (body != null) result.body = body;
    return result;
  }

  InvokeDeploymentRequest._();

  factory InvokeDeploymentRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeDeploymentRequest()..mergeFromBuffer(data, registry);
  factory InvokeDeploymentRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeDeploymentRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InvokeDeploymentRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: InvokeDeploymentRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'alias')
    ..aOS(2, _omitFieldNames ? '' : 'method')
    ..aOS(3, _omitFieldNames ? '' : 'url')
    ..pPM<Header>(4, _omitFieldNames ? '' : 'headers',
        subBuilder: Header.$_createMessage)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeDeploymentRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeDeploymentRequest copyWith(
          void Function(InvokeDeploymentRequest) updates) =>
      super.copyWith((message) => updates(message as InvokeDeploymentRequest))
          as InvokeDeploymentRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InvokeDeploymentRequest() / InvokeDeploymentRequest.new instead')
  static InvokeDeploymentRequest create() => InvokeDeploymentRequest._();
  static $pb.GeneratedMessage $_createMessage() => InvokeDeploymentRequest._();
  @$core.override
  InvokeDeploymentRequest createEmptyInstance() => InvokeDeploymentRequest._();
  @$core.pragma('dart2js:noInline')
  static InvokeDeploymentRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InvokeDeploymentRequest>(
          InvokeDeploymentRequest.$_createMessage);
  static InvokeDeploymentRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get alias => $_getSZ(0);
  @$pb.TagNumber(1)
  set alias($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasAlias() => $_has(0);
  @$pb.TagNumber(1)
  void clearAlias() => $_clearField(1);

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
  $pb.PbList<Header> get headers => $_getList(3);

  @$pb.TagNumber(5)
  $core.List<$core.int> get body => $_getN(4);
  @$pb.TagNumber(5)
  set body($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasBody() => $_has(4);
  @$pb.TagNumber(5)
  void clearBody() => $_clearField(5);
}

class InvokeResponse extends $pb.GeneratedMessage {
  factory InvokeResponse({
    $core.int? status,
    $core.Iterable<Header>? headers,
    $core.List<$core.int>? body,
    $core.List<$core.int>? resolvedSha256,
    $fixnum.Int64? resolvedRevision,
  }) {
    final result = InvokeResponse._();
    if (status != null) result.status = status;
    if (headers != null) result.headers.addAll(headers);
    if (body != null) result.body = body;
    if (resolvedSha256 != null) result.resolvedSha256 = resolvedSha256;
    if (resolvedRevision != null) result.resolvedRevision = resolvedRevision;
    return result;
  }

  InvokeResponse._();

  factory InvokeResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeResponse()..mergeFromBuffer(data, registry);
  factory InvokeResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InvokeResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InvokeResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
      createEmptyInstance: InvokeResponse.$_createMessage)
    ..aI(1, _omitFieldNames ? '' : 'status', fieldType: $pb.PbFieldType.OU3)
    ..pPM<Header>(2, _omitFieldNames ? '' : 'headers',
        subBuilder: Header.$_createMessage)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'resolvedSha256', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'resolvedRevision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InvokeResponse copyWith(void Function(InvokeResponse) updates) =>
      super.copyWith((message) => updates(message as InvokeResponse))
          as InvokeResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InvokeResponse() / InvokeResponse.new instead')
  static InvokeResponse create() => InvokeResponse._();
  static $pb.GeneratedMessage $_createMessage() => InvokeResponse._();
  @$core.override
  InvokeResponse createEmptyInstance() => InvokeResponse._();
  @$core.pragma('dart2js:noInline')
  static InvokeResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<InvokeResponse>(
          InvokeResponse.$_createMessage);
  static InvokeResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.int get status => $_getIZ(0);
  @$pb.TagNumber(1)
  set status($core.int value) => $_setUnsignedInt32(0, value);
  @$pb.TagNumber(1)
  $core.bool hasStatus() => $_has(0);
  @$pb.TagNumber(1)
  void clearStatus() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<Header> get headers => $_getList(1);

  @$pb.TagNumber(3)
  $core.List<$core.int> get body => $_getN(2);
  @$pb.TagNumber(3)
  set body($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasBody() => $_has(2);
  @$pb.TagNumber(3)
  void clearBody() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get resolvedSha256 => $_getN(3);
  @$pb.TagNumber(4)
  set resolvedSha256($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasResolvedSha256() => $_has(3);
  @$pb.TagNumber(4)
  void clearResolvedSha256() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get resolvedRevision => $_getI64(4);
  @$pb.TagNumber(5)
  set resolvedRevision($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasResolvedRevision() => $_has(4);
  @$pb.TagNumber(5)
  void clearResolvedRevision() => $_clearField(5);
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.workers.v1'),
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
