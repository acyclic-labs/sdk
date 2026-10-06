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

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;
import 'package:protobuf/well_known_types/google/protobuf/timestamp.pb.dart'
    as $1;

import 'objects.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'objects.pbenum.dart';

/// Tenant-scoped logical bucket name. Placement and storage identities are private.
class BucketRef extends $pb.GeneratedMessage {
  factory BucketRef({
    $core.String? name,
  }) {
    final result = BucketRef._();
    if (name != null) result.name = name;
    return result;
  }

  BucketRef._();

  factory BucketRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      BucketRef()..mergeFromBuffer(data, registry);
  factory BucketRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      BucketRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'BucketRef',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: BucketRef.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  BucketRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  BucketRef copyWith(void Function(BucketRef) updates) =>
      super.copyWith((message) => updates(message as BucketRef)) as BucketRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use BucketRef() / BucketRef.new instead')
  static BucketRef create() => BucketRef._();
  static $pb.GeneratedMessage $_createMessage() => BucketRef._();
  @$core.override
  BucketRef createEmptyInstance() => BucketRef._();
  @$core.pragma('dart2js:noInline')
  static BucketRef getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<BucketRef>(BucketRef.$_createMessage);
  static BucketRef? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);
}

class Bucket extends $pb.GeneratedMessage {
  factory Bucket({
    BucketRef? bucket,
    $1.Timestamp? createdAt,
  }) {
    final result = Bucket._();
    if (bucket != null) result.bucket = bucket;
    if (createdAt != null) result.createdAt = createdAt;
    return result;
  }

  Bucket._();

  factory Bucket.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Bucket()..mergeFromBuffer(data, registry);
  factory Bucket.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Bucket()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Bucket',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: Bucket.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOM<$1.Timestamp>(2, _omitFieldNames ? '' : 'createdAt',
        subBuilder: $1.Timestamp.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Bucket clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Bucket copyWith(void Function(Bucket) updates) =>
      super.copyWith((message) => updates(message as Bucket)) as Bucket;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Bucket() / Bucket.new instead')
  static Bucket create() => Bucket._();
  static $pb.GeneratedMessage $_createMessage() => Bucket._();
  @$core.override
  Bucket createEmptyInstance() => Bucket._();
  @$core.pragma('dart2js:noInline')
  static Bucket getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Bucket>(Bucket.$_createMessage);
  static Bucket? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $1.Timestamp get createdAt => $_getN(1);
  @$pb.TagNumber(2)
  set createdAt($1.Timestamp value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasCreatedAt() => $_has(1);
  @$pb.TagNumber(2)
  void clearCreatedAt() => $_clearField(2);
  @$pb.TagNumber(2)
  $1.Timestamp ensureCreatedAt() => $_ensure(1);
}

class ObjectMetadata extends $pb.GeneratedMessage {
  factory ObjectMetadata({
    $core.String? contentType,
    $core.Iterable<$core.MapEntry<$core.String, $core.String>>? user,
    $core.String? contentEncoding,
    $core.String? cacheControl,
    $core.String? contentDisposition,
    $core.String? contentLanguage,
    $fixnum.Int64? expiresUnixSeconds,
  }) {
    final result = ObjectMetadata._();
    if (contentType != null) result.contentType = contentType;
    if (user != null) result.user.addEntries(user);
    if (contentEncoding != null) result.contentEncoding = contentEncoding;
    if (cacheControl != null) result.cacheControl = cacheControl;
    if (contentDisposition != null)
      result.contentDisposition = contentDisposition;
    if (contentLanguage != null) result.contentLanguage = contentLanguage;
    if (expiresUnixSeconds != null)
      result.expiresUnixSeconds = expiresUnixSeconds;
    return result;
  }

  ObjectMetadata._();

  factory ObjectMetadata.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObjectMetadata()..mergeFromBuffer(data, registry);
  factory ObjectMetadata.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObjectMetadata()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ObjectMetadata',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ObjectMetadata.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'contentType')
    ..m<$core.String, $core.String>(2, _omitFieldNames ? '' : 'user',
        entryClassName: 'ObjectMetadata.UserEntry',
        keyFieldType: $pb.PbFieldType.OS,
        valueFieldType: $pb.PbFieldType.OS,
        packageName: const $pb.PackageName('acyclic.objects.v2'))
    ..aOS(3, _omitFieldNames ? '' : 'contentEncoding')
    ..aOS(4, _omitFieldNames ? '' : 'cacheControl')
    ..aOS(5, _omitFieldNames ? '' : 'contentDisposition')
    ..aOS(6, _omitFieldNames ? '' : 'contentLanguage')
    ..aInt64(7, _omitFieldNames ? '' : 'expiresUnixSeconds')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObjectMetadata clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObjectMetadata copyWith(void Function(ObjectMetadata) updates) =>
      super.copyWith((message) => updates(message as ObjectMetadata))
          as ObjectMetadata;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ObjectMetadata() / ObjectMetadata.new instead')
  static ObjectMetadata create() => ObjectMetadata._();
  static $pb.GeneratedMessage $_createMessage() => ObjectMetadata._();
  @$core.override
  ObjectMetadata createEmptyInstance() => ObjectMetadata._();
  @$core.pragma('dart2js:noInline')
  static ObjectMetadata getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ObjectMetadata>(
          ObjectMetadata.$_createMessage);
  static ObjectMetadata? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get contentType => $_getSZ(0);
  @$pb.TagNumber(1)
  set contentType($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasContentType() => $_has(0);
  @$pb.TagNumber(1)
  void clearContentType() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbMap<$core.String, $core.String> get user => $_getMap(1);

  @$pb.TagNumber(3)
  $core.String get contentEncoding => $_getSZ(2);
  @$pb.TagNumber(3)
  set contentEncoding($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasContentEncoding() => $_has(2);
  @$pb.TagNumber(3)
  void clearContentEncoding() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get cacheControl => $_getSZ(3);
  @$pb.TagNumber(4)
  set cacheControl($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCacheControl() => $_has(3);
  @$pb.TagNumber(4)
  void clearCacheControl() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get contentDisposition => $_getSZ(4);
  @$pb.TagNumber(5)
  set contentDisposition($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasContentDisposition() => $_has(4);
  @$pb.TagNumber(5)
  void clearContentDisposition() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get contentLanguage => $_getSZ(5);
  @$pb.TagNumber(6)
  set contentLanguage($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasContentLanguage() => $_has(5);
  @$pb.TagNumber(6)
  void clearContentLanguage() => $_clearField(6);

  @$pb.TagNumber(7)
  $fixnum.Int64 get expiresUnixSeconds => $_getI64(6);
  @$pb.TagNumber(7)
  set expiresUnixSeconds($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasExpiresUnixSeconds() => $_has(6);
  @$pb.TagNumber(7)
  void clearExpiresUnixSeconds() => $_clearField(7);
}

enum Preconditions_Condition { ifAbsent, ifMatch, notSet }

/// Evaluated atomically with single-object publication against the current value.
/// An eventual HEAD/GET may be stale and cause a safe precondition failure.
class Preconditions extends $pb.GeneratedMessage {
  factory Preconditions({
    $core.bool? ifAbsent,
    $core.String? ifMatch,
  }) {
    final result = Preconditions._();
    if (ifAbsent != null) result.ifAbsent = ifAbsent;
    if (ifMatch != null) result.ifMatch = ifMatch;
    return result;
  }

  Preconditions._();

  factory Preconditions.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Preconditions()..mergeFromBuffer(data, registry);
  factory Preconditions.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Preconditions()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, Preconditions_Condition>
      _Preconditions_ConditionByTag = {
    1: Preconditions_Condition.ifAbsent,
    2: Preconditions_Condition.ifMatch,
    0: Preconditions_Condition.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Preconditions',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: Preconditions.$_createMessage)
    ..oo(0, [1, 2])
    ..aOB(1, _omitFieldNames ? '' : 'ifAbsent')
    ..aOS(2, _omitFieldNames ? '' : 'ifMatch')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Preconditions clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Preconditions copyWith(void Function(Preconditions) updates) =>
      super.copyWith((message) => updates(message as Preconditions))
          as Preconditions;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Preconditions() / Preconditions.new instead')
  static Preconditions create() => Preconditions._();
  static $pb.GeneratedMessage $_createMessage() => Preconditions._();
  @$core.override
  Preconditions createEmptyInstance() => Preconditions._();
  @$core.pragma('dart2js:noInline')
  static Preconditions getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Preconditions>(
          Preconditions.$_createMessage);
  static Preconditions? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  Preconditions_Condition whichCondition() =>
      _Preconditions_ConditionByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearCondition() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.bool get ifAbsent => $_getBF(0);
  @$pb.TagNumber(1)
  set ifAbsent($core.bool value) => $_setBool(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIfAbsent() => $_has(0);
  @$pb.TagNumber(1)
  void clearIfAbsent() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get ifMatch => $_getSZ(1);
  @$pb.TagNumber(2)
  set ifMatch($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIfMatch() => $_has(1);
  @$pb.TagNumber(2)
  void clearIfMatch() => $_clearField(2);
}

class MutationIdentity extends $pb.GeneratedMessage {
  factory MutationIdentity({
    $core.String? idempotencyKey,
  }) {
    final result = MutationIdentity._();
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  MutationIdentity._();

  factory MutationIdentity.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationIdentity()..mergeFromBuffer(data, registry);
  factory MutationIdentity.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationIdentity()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MutationIdentity',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: MutationIdentity.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'idempotencyKey')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationIdentity clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationIdentity copyWith(void Function(MutationIdentity) updates) =>
      super.copyWith((message) => updates(message as MutationIdentity))
          as MutationIdentity;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MutationIdentity() / MutationIdentity.new instead')
  static MutationIdentity create() => MutationIdentity._();
  static $pb.GeneratedMessage $_createMessage() => MutationIdentity._();
  @$core.override
  MutationIdentity createEmptyInstance() => MutationIdentity._();
  @$core.pragma('dart2js:noInline')
  static MutationIdentity getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MutationIdentity>(
          MutationIdentity.$_createMessage);
  static MutationIdentity? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get idempotencyKey => $_getSZ(0);
  @$pb.TagNumber(1)
  set idempotencyKey($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIdempotencyKey() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdempotencyKey() => $_clearField(1);
}

/// One complete current representation, with an opaque ETag rather than history identity.
class ObjectInfo extends $pb.GeneratedMessage {
  factory ObjectInfo({
    $core.String? etag,
    $fixnum.Int64? size,
    ObjectMetadata? metadata,
    $1.Timestamp? lastModified,
  }) {
    final result = ObjectInfo._();
    if (etag != null) result.etag = etag;
    if (size != null) result.size = size;
    if (metadata != null) result.metadata = metadata;
    if (lastModified != null) result.lastModified = lastModified;
    return result;
  }

  ObjectInfo._();

  factory ObjectInfo.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObjectInfo()..mergeFromBuffer(data, registry);
  factory ObjectInfo.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObjectInfo()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ObjectInfo',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ObjectInfo.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'etag')
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'size', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<ObjectMetadata>(3, _omitFieldNames ? '' : 'metadata',
        subBuilder: ObjectMetadata.$_createMessage)
    ..aOM<$1.Timestamp>(4, _omitFieldNames ? '' : 'lastModified',
        subBuilder: $1.Timestamp.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObjectInfo clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObjectInfo copyWith(void Function(ObjectInfo) updates) =>
      super.copyWith((message) => updates(message as ObjectInfo)) as ObjectInfo;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ObjectInfo() / ObjectInfo.new instead')
  static ObjectInfo create() => ObjectInfo._();
  static $pb.GeneratedMessage $_createMessage() => ObjectInfo._();
  @$core.override
  ObjectInfo createEmptyInstance() => ObjectInfo._();
  @$core.pragma('dart2js:noInline')
  static ObjectInfo getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ObjectInfo>(ObjectInfo.$_createMessage);
  static ObjectInfo? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get etag => $_getSZ(0);
  @$pb.TagNumber(1)
  set etag($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasEtag() => $_has(0);
  @$pb.TagNumber(1)
  void clearEtag() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get size => $_getI64(1);
  @$pb.TagNumber(2)
  set size($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSize() => $_has(1);
  @$pb.TagNumber(2)
  void clearSize() => $_clearField(2);

  @$pb.TagNumber(3)
  ObjectMetadata get metadata => $_getN(2);
  @$pb.TagNumber(3)
  set metadata(ObjectMetadata value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMetadata() => $_has(2);
  @$pb.TagNumber(3)
  void clearMetadata() => $_clearField(3);
  @$pb.TagNumber(3)
  ObjectMetadata ensureMetadata() => $_ensure(2);

  @$pb.TagNumber(4)
  $1.Timestamp get lastModified => $_getN(3);
  @$pb.TagNumber(4)
  set lastModified($1.Timestamp value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasLastModified() => $_has(3);
  @$pb.TagNumber(4)
  void clearLastModified() => $_clearField(4);
  @$pb.TagNumber(4)
  $1.Timestamp ensureLastModified() => $_ensure(3);
}

class CreateBucketRequest extends $pb.GeneratedMessage {
  factory CreateBucketRequest({
    $core.String? name,
    MutationIdentity? mutation,
  }) {
    final result = CreateBucketRequest._();
    if (name != null) result.name = name;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  CreateBucketRequest._();

  factory CreateBucketRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateBucketRequest()..mergeFromBuffer(data, registry);
  factory CreateBucketRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateBucketRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateBucketRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: CreateBucketRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aOM<MutationIdentity>(2, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateBucketRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateBucketRequest copyWith(void Function(CreateBucketRequest) updates) =>
      super.copyWith((message) => updates(message as CreateBucketRequest))
          as CreateBucketRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use CreateBucketRequest() / CreateBucketRequest.new instead')
  static CreateBucketRequest create() => CreateBucketRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateBucketRequest._();
  @$core.override
  CreateBucketRequest createEmptyInstance() => CreateBucketRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateBucketRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateBucketRequest>(
          CreateBucketRequest.$_createMessage);
  static CreateBucketRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  MutationIdentity get mutation => $_getN(1);
  @$pb.TagNumber(2)
  set mutation(MutationIdentity value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMutation() => $_has(1);
  @$pb.TagNumber(2)
  void clearMutation() => $_clearField(2);
  @$pb.TagNumber(2)
  MutationIdentity ensureMutation() => $_ensure(1);
}

class HeadBucketRequest extends $pb.GeneratedMessage {
  factory HeadBucketRequest({
    BucketRef? bucket,
  }) {
    final result = HeadBucketRequest._();
    if (bucket != null) result.bucket = bucket;
    return result;
  }

  HeadBucketRequest._();

  factory HeadBucketRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HeadBucketRequest()..mergeFromBuffer(data, registry);
  factory HeadBucketRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HeadBucketRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'HeadBucketRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: HeadBucketRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HeadBucketRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HeadBucketRequest copyWith(void Function(HeadBucketRequest) updates) =>
      super.copyWith((message) => updates(message as HeadBucketRequest))
          as HeadBucketRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use HeadBucketRequest() / HeadBucketRequest.new instead')
  static HeadBucketRequest create() => HeadBucketRequest._();
  static $pb.GeneratedMessage $_createMessage() => HeadBucketRequest._();
  @$core.override
  HeadBucketRequest createEmptyInstance() => HeadBucketRequest._();
  @$core.pragma('dart2js:noInline')
  static HeadBucketRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<HeadBucketRequest>(
          HeadBucketRequest.$_createMessage);
  static HeadBucketRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);
}

class DeleteBucketRequest extends $pb.GeneratedMessage {
  factory DeleteBucketRequest({
    BucketRef? bucket,
    MutationIdentity? mutation,
  }) {
    final result = DeleteBucketRequest._();
    if (bucket != null) result.bucket = bucket;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  DeleteBucketRequest._();

  factory DeleteBucketRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteBucketRequest()..mergeFromBuffer(data, registry);
  factory DeleteBucketRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteBucketRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DeleteBucketRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: DeleteBucketRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOM<MutationIdentity>(2, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteBucketRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteBucketRequest copyWith(void Function(DeleteBucketRequest) updates) =>
      super.copyWith((message) => updates(message as DeleteBucketRequest))
          as DeleteBucketRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use DeleteBucketRequest() / DeleteBucketRequest.new instead')
  static DeleteBucketRequest create() => DeleteBucketRequest._();
  static $pb.GeneratedMessage $_createMessage() => DeleteBucketRequest._();
  @$core.override
  DeleteBucketRequest createEmptyInstance() => DeleteBucketRequest._();
  @$core.pragma('dart2js:noInline')
  static DeleteBucketRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DeleteBucketRequest>(
          DeleteBucketRequest.$_createMessage);
  static DeleteBucketRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  MutationIdentity get mutation => $_getN(1);
  @$pb.TagNumber(2)
  set mutation(MutationIdentity value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMutation() => $_has(1);
  @$pb.TagNumber(2)
  void clearMutation() => $_clearField(2);
  @$pb.TagNumber(2)
  MutationIdentity ensureMutation() => $_ensure(1);
}

class DeleteBucketResponse extends $pb.GeneratedMessage {
  factory DeleteBucketResponse({
    $core.bool? existed,
  }) {
    final result = DeleteBucketResponse._();
    if (existed != null) result.existed = existed;
    return result;
  }

  DeleteBucketResponse._();

  factory DeleteBucketResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteBucketResponse()..mergeFromBuffer(data, registry);
  factory DeleteBucketResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteBucketResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DeleteBucketResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: DeleteBucketResponse.$_createMessage)
    ..aOB(1, _omitFieldNames ? '' : 'existed')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteBucketResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteBucketResponse copyWith(void Function(DeleteBucketResponse) updates) =>
      super.copyWith((message) => updates(message as DeleteBucketResponse))
          as DeleteBucketResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use DeleteBucketResponse() / DeleteBucketResponse.new instead')
  static DeleteBucketResponse create() => DeleteBucketResponse._();
  static $pb.GeneratedMessage $_createMessage() => DeleteBucketResponse._();
  @$core.override
  DeleteBucketResponse createEmptyInstance() => DeleteBucketResponse._();
  @$core.pragma('dart2js:noInline')
  static DeleteBucketResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DeleteBucketResponse>(
          DeleteBucketResponse.$_createMessage);
  static DeleteBucketResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.bool get existed => $_getBF(0);
  @$pb.TagNumber(1)
  set existed($core.bool value) => $_setBool(0, value);
  @$pb.TagNumber(1)
  $core.bool hasExisted() => $_has(0);
  @$pb.TagNumber(1)
  void clearExisted() => $_clearField(1);
}

class PutObjectHeader extends $pb.GeneratedMessage {
  factory PutObjectHeader({
    BucketRef? bucket,
    $core.String? objectKey,
    ObjectMetadata? metadata,
    Preconditions? preconditions,
    MutationIdentity? mutation,
  }) {
    final result = PutObjectHeader._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (metadata != null) result.metadata = metadata;
    if (preconditions != null) result.preconditions = preconditions;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  PutObjectHeader._();

  factory PutObjectHeader.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PutObjectHeader()..mergeFromBuffer(data, registry);
  factory PutObjectHeader.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PutObjectHeader()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PutObjectHeader',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: PutObjectHeader.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOM<ObjectMetadata>(3, _omitFieldNames ? '' : 'metadata',
        subBuilder: ObjectMetadata.$_createMessage)
    ..aOM<Preconditions>(4, _omitFieldNames ? '' : 'preconditions',
        subBuilder: Preconditions.$_createMessage)
    ..aOM<MutationIdentity>(5, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PutObjectHeader clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PutObjectHeader copyWith(void Function(PutObjectHeader) updates) =>
      super.copyWith((message) => updates(message as PutObjectHeader))
          as PutObjectHeader;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use PutObjectHeader() / PutObjectHeader.new instead')
  static PutObjectHeader create() => PutObjectHeader._();
  static $pb.GeneratedMessage $_createMessage() => PutObjectHeader._();
  @$core.override
  PutObjectHeader createEmptyInstance() => PutObjectHeader._();
  @$core.pragma('dart2js:noInline')
  static PutObjectHeader getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<PutObjectHeader>(
          PutObjectHeader.$_createMessage);
  static PutObjectHeader? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  ObjectMetadata get metadata => $_getN(2);
  @$pb.TagNumber(3)
  set metadata(ObjectMetadata value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMetadata() => $_has(2);
  @$pb.TagNumber(3)
  void clearMetadata() => $_clearField(3);
  @$pb.TagNumber(3)
  ObjectMetadata ensureMetadata() => $_ensure(2);

  @$pb.TagNumber(4)
  Preconditions get preconditions => $_getN(3);
  @$pb.TagNumber(4)
  set preconditions(Preconditions value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasPreconditions() => $_has(3);
  @$pb.TagNumber(4)
  void clearPreconditions() => $_clearField(4);
  @$pb.TagNumber(4)
  Preconditions ensurePreconditions() => $_ensure(3);

  @$pb.TagNumber(5)
  MutationIdentity get mutation => $_getN(4);
  @$pb.TagNumber(5)
  set mutation(MutationIdentity value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasMutation() => $_has(4);
  @$pb.TagNumber(5)
  void clearMutation() => $_clearField(5);
  @$pb.TagNumber(5)
  MutationIdentity ensureMutation() => $_ensure(4);
}

enum PutObjectRequest_Frame { header, body, complete, notSet }

/// Header first, bounded body frames, then complete=true and EOF. Missing completion
/// never publishes bytes, even if transport cancellation appears as a clean EOF.
class PutObjectRequest extends $pb.GeneratedMessage {
  factory PutObjectRequest({
    PutObjectHeader? header,
    $core.List<$core.int>? body,
    $core.bool? complete,
  }) {
    final result = PutObjectRequest._();
    if (header != null) result.header = header;
    if (body != null) result.body = body;
    if (complete != null) result.complete = complete;
    return result;
  }

  PutObjectRequest._();

  factory PutObjectRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PutObjectRequest()..mergeFromBuffer(data, registry);
  factory PutObjectRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PutObjectRequest()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, PutObjectRequest_Frame>
      _PutObjectRequest_FrameByTag = {
    1: PutObjectRequest_Frame.header,
    2: PutObjectRequest_Frame.body,
    3: PutObjectRequest_Frame.complete,
    0: PutObjectRequest_Frame.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PutObjectRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: PutObjectRequest.$_createMessage)
    ..oo(0, [1, 2, 3])
    ..aOM<PutObjectHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: PutObjectHeader.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..aOB(3, _omitFieldNames ? '' : 'complete')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PutObjectRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PutObjectRequest copyWith(void Function(PutObjectRequest) updates) =>
      super.copyWith((message) => updates(message as PutObjectRequest))
          as PutObjectRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use PutObjectRequest() / PutObjectRequest.new instead')
  static PutObjectRequest create() => PutObjectRequest._();
  static $pb.GeneratedMessage $_createMessage() => PutObjectRequest._();
  @$core.override
  PutObjectRequest createEmptyInstance() => PutObjectRequest._();
  @$core.pragma('dart2js:noInline')
  static PutObjectRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<PutObjectRequest>(
          PutObjectRequest.$_createMessage);
  static PutObjectRequest? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  PutObjectRequest_Frame whichFrame() =>
      _PutObjectRequest_FrameByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  void clearFrame() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  PutObjectHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header(PutObjectHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  PutObjectHeader ensureHeader() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get body => $_getN(1);
  @$pb.TagNumber(2)
  set body($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBody() => $_has(1);
  @$pb.TagNumber(2)
  void clearBody() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.bool get complete => $_getBF(2);
  @$pb.TagNumber(3)
  set complete($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasComplete() => $_has(2);
  @$pb.TagNumber(3)
  void clearComplete() => $_clearField(3);
}

class InclusiveRange extends $pb.GeneratedMessage {
  factory InclusiveRange({
    $fixnum.Int64? start,
    $fixnum.Int64? end,
  }) {
    final result = InclusiveRange._();
    if (start != null) result.start = start;
    if (end != null) result.end = end;
    return result;
  }

  InclusiveRange._();

  factory InclusiveRange.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InclusiveRange()..mergeFromBuffer(data, registry);
  factory InclusiveRange.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InclusiveRange()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InclusiveRange',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: InclusiveRange.$_createMessage)
    ..a<$fixnum.Int64>(1, _omitFieldNames ? '' : 'start', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'end', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InclusiveRange clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InclusiveRange copyWith(void Function(InclusiveRange) updates) =>
      super.copyWith((message) => updates(message as InclusiveRange))
          as InclusiveRange;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use InclusiveRange() / InclusiveRange.new instead')
  static InclusiveRange create() => InclusiveRange._();
  static $pb.GeneratedMessage $_createMessage() => InclusiveRange._();
  @$core.override
  InclusiveRange createEmptyInstance() => InclusiveRange._();
  @$core.pragma('dart2js:noInline')
  static InclusiveRange getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<InclusiveRange>(
          InclusiveRange.$_createMessage);
  static InclusiveRange? _defaultInstance;

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
}

enum ByteRange_Selection { bytes, suffixLength, notSet }

class ByteRange extends $pb.GeneratedMessage {
  factory ByteRange({
    InclusiveRange? bytes,
    $fixnum.Int64? suffixLength,
  }) {
    final result = ByteRange._();
    if (bytes != null) result.bytes = bytes;
    if (suffixLength != null) result.suffixLength = suffixLength;
    return result;
  }

  ByteRange._();

  factory ByteRange.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ByteRange()..mergeFromBuffer(data, registry);
  factory ByteRange.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ByteRange()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ByteRange_Selection>
      _ByteRange_SelectionByTag = {
    1: ByteRange_Selection.bytes,
    2: ByteRange_Selection.suffixLength,
    0: ByteRange_Selection.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ByteRange',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ByteRange.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<InclusiveRange>(1, _omitFieldNames ? '' : 'bytes',
        subBuilder: InclusiveRange.$_createMessage)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'suffixLength', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ByteRange clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ByteRange copyWith(void Function(ByteRange) updates) =>
      super.copyWith((message) => updates(message as ByteRange)) as ByteRange;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ByteRange() / ByteRange.new instead')
  static ByteRange create() => ByteRange._();
  static $pb.GeneratedMessage $_createMessage() => ByteRange._();
  @$core.override
  ByteRange createEmptyInstance() => ByteRange._();
  @$core.pragma('dart2js:noInline')
  static ByteRange getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ByteRange>(ByteRange.$_createMessage);
  static ByteRange? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  ByteRange_Selection whichSelection() =>
      _ByteRange_SelectionByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearSelection() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  InclusiveRange get bytes => $_getN(0);
  @$pb.TagNumber(1)
  set bytes(InclusiveRange value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBytes() => $_has(0);
  @$pb.TagNumber(1)
  void clearBytes() => $_clearField(1);
  @$pb.TagNumber(1)
  InclusiveRange ensureBytes() => $_ensure(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get suffixLength => $_getI64(1);
  @$pb.TagNumber(2)
  set suffixLength($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSuffixLength() => $_has(1);
  @$pb.TagNumber(2)
  void clearSuffixLength() => $_clearField(2);
}

class GetObjectRequest extends $pb.GeneratedMessage {
  factory GetObjectRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    ByteRange? range,
    $core.String? ifMatch,
    $core.String? ifNoneMatch,
  }) {
    final result = GetObjectRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (range != null) result.range = range;
    if (ifMatch != null) result.ifMatch = ifMatch;
    if (ifNoneMatch != null) result.ifNoneMatch = ifNoneMatch;
    return result;
  }

  GetObjectRequest._();

  factory GetObjectRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetObjectRequest()..mergeFromBuffer(data, registry);
  factory GetObjectRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetObjectRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GetObjectRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: GetObjectRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOM<ByteRange>(3, _omitFieldNames ? '' : 'range',
        subBuilder: ByteRange.$_createMessage)
    ..aOS(4, _omitFieldNames ? '' : 'ifMatch')
    ..aOS(5, _omitFieldNames ? '' : 'ifNoneMatch')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetObjectRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetObjectRequest copyWith(void Function(GetObjectRequest) updates) =>
      super.copyWith((message) => updates(message as GetObjectRequest))
          as GetObjectRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use GetObjectRequest() / GetObjectRequest.new instead')
  static GetObjectRequest create() => GetObjectRequest._();
  static $pb.GeneratedMessage $_createMessage() => GetObjectRequest._();
  @$core.override
  GetObjectRequest createEmptyInstance() => GetObjectRequest._();
  @$core.pragma('dart2js:noInline')
  static GetObjectRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetObjectRequest>(
          GetObjectRequest.$_createMessage);
  static GetObjectRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  ByteRange get range => $_getN(2);
  @$pb.TagNumber(3)
  set range(ByteRange value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasRange() => $_has(2);
  @$pb.TagNumber(3)
  void clearRange() => $_clearField(3);
  @$pb.TagNumber(3)
  ByteRange ensureRange() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.String get ifMatch => $_getSZ(3);
  @$pb.TagNumber(4)
  set ifMatch($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIfMatch() => $_has(3);
  @$pb.TagNumber(4)
  void clearIfMatch() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get ifNoneMatch => $_getSZ(4);
  @$pb.TagNumber(5)
  set ifNoneMatch($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasIfNoneMatch() => $_has(4);
  @$pb.TagNumber(5)
  void clearIfNoneMatch() => $_clearField(5);
}

class ContentRange extends $pb.GeneratedMessage {
  factory ContentRange({
    $fixnum.Int64? start,
    $fixnum.Int64? end,
    $fixnum.Int64? total,
  }) {
    final result = ContentRange._();
    if (start != null) result.start = start;
    if (end != null) result.end = end;
    if (total != null) result.total = total;
    return result;
  }

  ContentRange._();

  factory ContentRange.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ContentRange()..mergeFromBuffer(data, registry);
  factory ContentRange.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ContentRange()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ContentRange',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ContentRange.$_createMessage)
    ..a<$fixnum.Int64>(1, _omitFieldNames ? '' : 'start', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'end', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'total', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ContentRange clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ContentRange copyWith(void Function(ContentRange) updates) =>
      super.copyWith((message) => updates(message as ContentRange))
          as ContentRange;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ContentRange() / ContentRange.new instead')
  static ContentRange create() => ContentRange._();
  static $pb.GeneratedMessage $_createMessage() => ContentRange._();
  @$core.override
  ContentRange createEmptyInstance() => ContentRange._();
  @$core.pragma('dart2js:noInline')
  static ContentRange getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ContentRange>(
          ContentRange.$_createMessage);
  static ContentRange? _defaultInstance;

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
  $fixnum.Int64 get total => $_getI64(2);
  @$pb.TagNumber(3)
  set total($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasTotal() => $_has(2);
  @$pb.TagNumber(3)
  void clearTotal() => $_clearField(3);
}

class GetObjectHeader extends $pb.GeneratedMessage {
  factory GetObjectHeader({
    ObjectInfo? object,
    ContentRange? contentRange,
  }) {
    final result = GetObjectHeader._();
    if (object != null) result.object = object;
    if (contentRange != null) result.contentRange = contentRange;
    return result;
  }

  GetObjectHeader._();

  factory GetObjectHeader.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetObjectHeader()..mergeFromBuffer(data, registry);
  factory GetObjectHeader.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetObjectHeader()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GetObjectHeader',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: GetObjectHeader.$_createMessage)
    ..aOM<ObjectInfo>(1, _omitFieldNames ? '' : 'object',
        subBuilder: ObjectInfo.$_createMessage)
    ..aOM<ContentRange>(2, _omitFieldNames ? '' : 'contentRange',
        subBuilder: ContentRange.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetObjectHeader clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetObjectHeader copyWith(void Function(GetObjectHeader) updates) =>
      super.copyWith((message) => updates(message as GetObjectHeader))
          as GetObjectHeader;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use GetObjectHeader() / GetObjectHeader.new instead')
  static GetObjectHeader create() => GetObjectHeader._();
  static $pb.GeneratedMessage $_createMessage() => GetObjectHeader._();
  @$core.override
  GetObjectHeader createEmptyInstance() => GetObjectHeader._();
  @$core.pragma('dart2js:noInline')
  static GetObjectHeader getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetObjectHeader>(
          GetObjectHeader.$_createMessage);
  static GetObjectHeader? _defaultInstance;

  @$pb.TagNumber(1)
  ObjectInfo get object => $_getN(0);
  @$pb.TagNumber(1)
  set object(ObjectInfo value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasObject() => $_has(0);
  @$pb.TagNumber(1)
  void clearObject() => $_clearField(1);
  @$pb.TagNumber(1)
  ObjectInfo ensureObject() => $_ensure(0);

  @$pb.TagNumber(2)
  ContentRange get contentRange => $_getN(1);
  @$pb.TagNumber(2)
  set contentRange(ContentRange value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasContentRange() => $_has(1);
  @$pb.TagNumber(2)
  void clearContentRange() => $_clearField(2);
  @$pb.TagNumber(2)
  ContentRange ensureContentRange() => $_ensure(1);
}

enum GetObjectResponse_Frame { header, body, error, notSet }

/// Exactly one metadata header first. The body belongs to that complete representation.
/// A terminal semantic error can follow the header when an HTTP stream is already open.
class GetObjectResponse extends $pb.GeneratedMessage {
  factory GetObjectResponse({
    GetObjectHeader? header,
    $core.List<$core.int>? body,
    ErrorDetail? error,
  }) {
    final result = GetObjectResponse._();
    if (header != null) result.header = header;
    if (body != null) result.body = body;
    if (error != null) result.error = error;
    return result;
  }

  GetObjectResponse._();

  factory GetObjectResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetObjectResponse()..mergeFromBuffer(data, registry);
  factory GetObjectResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetObjectResponse()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, GetObjectResponse_Frame>
      _GetObjectResponse_FrameByTag = {
    1: GetObjectResponse_Frame.header,
    2: GetObjectResponse_Frame.body,
    3: GetObjectResponse_Frame.error,
    0: GetObjectResponse_Frame.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GetObjectResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: GetObjectResponse.$_createMessage)
    ..oo(0, [1, 2, 3])
    ..aOM<GetObjectHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: GetObjectHeader.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..aOM<ErrorDetail>(3, _omitFieldNames ? '' : 'error',
        subBuilder: ErrorDetail.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetObjectResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetObjectResponse copyWith(void Function(GetObjectResponse) updates) =>
      super.copyWith((message) => updates(message as GetObjectResponse))
          as GetObjectResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use GetObjectResponse() / GetObjectResponse.new instead')
  static GetObjectResponse create() => GetObjectResponse._();
  static $pb.GeneratedMessage $_createMessage() => GetObjectResponse._();
  @$core.override
  GetObjectResponse createEmptyInstance() => GetObjectResponse._();
  @$core.pragma('dart2js:noInline')
  static GetObjectResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetObjectResponse>(
          GetObjectResponse.$_createMessage);
  static GetObjectResponse? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  GetObjectResponse_Frame whichFrame() =>
      _GetObjectResponse_FrameByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  void clearFrame() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  GetObjectHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header(GetObjectHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  GetObjectHeader ensureHeader() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get body => $_getN(1);
  @$pb.TagNumber(2)
  set body($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBody() => $_has(1);
  @$pb.TagNumber(2)
  void clearBody() => $_clearField(2);

  @$pb.TagNumber(3)
  ErrorDetail get error => $_getN(2);
  @$pb.TagNumber(3)
  set error(ErrorDetail value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasError() => $_has(2);
  @$pb.TagNumber(3)
  void clearError() => $_clearField(3);
  @$pb.TagNumber(3)
  ErrorDetail ensureError() => $_ensure(2);
}

class HeadObjectRequest extends $pb.GeneratedMessage {
  factory HeadObjectRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    $core.String? ifMatch,
    $core.String? ifNoneMatch,
  }) {
    final result = HeadObjectRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (ifMatch != null) result.ifMatch = ifMatch;
    if (ifNoneMatch != null) result.ifNoneMatch = ifNoneMatch;
    return result;
  }

  HeadObjectRequest._();

  factory HeadObjectRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HeadObjectRequest()..mergeFromBuffer(data, registry);
  factory HeadObjectRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HeadObjectRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'HeadObjectRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: HeadObjectRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'ifMatch')
    ..aOS(4, _omitFieldNames ? '' : 'ifNoneMatch')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HeadObjectRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HeadObjectRequest copyWith(void Function(HeadObjectRequest) updates) =>
      super.copyWith((message) => updates(message as HeadObjectRequest))
          as HeadObjectRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use HeadObjectRequest() / HeadObjectRequest.new instead')
  static HeadObjectRequest create() => HeadObjectRequest._();
  static $pb.GeneratedMessage $_createMessage() => HeadObjectRequest._();
  @$core.override
  HeadObjectRequest createEmptyInstance() => HeadObjectRequest._();
  @$core.pragma('dart2js:noInline')
  static HeadObjectRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<HeadObjectRequest>(
          HeadObjectRequest.$_createMessage);
  static HeadObjectRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get ifMatch => $_getSZ(2);
  @$pb.TagNumber(3)
  set ifMatch($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIfMatch() => $_has(2);
  @$pb.TagNumber(3)
  void clearIfMatch() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get ifNoneMatch => $_getSZ(3);
  @$pb.TagNumber(4)
  set ifNoneMatch($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIfNoneMatch() => $_has(3);
  @$pb.TagNumber(4)
  void clearIfNoneMatch() => $_clearField(4);
}

class HeadObjectResponse extends $pb.GeneratedMessage {
  factory HeadObjectResponse({
    ObjectInfo? object,
  }) {
    final result = HeadObjectResponse._();
    if (object != null) result.object = object;
    return result;
  }

  HeadObjectResponse._();

  factory HeadObjectResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HeadObjectResponse()..mergeFromBuffer(data, registry);
  factory HeadObjectResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HeadObjectResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'HeadObjectResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: HeadObjectResponse.$_createMessage)
    ..aOM<ObjectInfo>(1, _omitFieldNames ? '' : 'object',
        subBuilder: ObjectInfo.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HeadObjectResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HeadObjectResponse copyWith(void Function(HeadObjectResponse) updates) =>
      super.copyWith((message) => updates(message as HeadObjectResponse))
          as HeadObjectResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use HeadObjectResponse() / HeadObjectResponse.new instead')
  static HeadObjectResponse create() => HeadObjectResponse._();
  static $pb.GeneratedMessage $_createMessage() => HeadObjectResponse._();
  @$core.override
  HeadObjectResponse createEmptyInstance() => HeadObjectResponse._();
  @$core.pragma('dart2js:noInline')
  static HeadObjectResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<HeadObjectResponse>(
          HeadObjectResponse.$_createMessage);
  static HeadObjectResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ObjectInfo get object => $_getN(0);
  @$pb.TagNumber(1)
  set object(ObjectInfo value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasObject() => $_has(0);
  @$pb.TagNumber(1)
  void clearObject() => $_clearField(1);
  @$pb.TagNumber(1)
  ObjectInfo ensureObject() => $_ensure(0);
}

class DeleteObjectRequest extends $pb.GeneratedMessage {
  factory DeleteObjectRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    Preconditions? preconditions,
    MutationIdentity? mutation,
  }) {
    final result = DeleteObjectRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (preconditions != null) result.preconditions = preconditions;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  DeleteObjectRequest._();

  factory DeleteObjectRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteObjectRequest()..mergeFromBuffer(data, registry);
  factory DeleteObjectRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteObjectRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DeleteObjectRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: DeleteObjectRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOM<Preconditions>(3, _omitFieldNames ? '' : 'preconditions',
        subBuilder: Preconditions.$_createMessage)
    ..aOM<MutationIdentity>(4, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteObjectRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteObjectRequest copyWith(void Function(DeleteObjectRequest) updates) =>
      super.copyWith((message) => updates(message as DeleteObjectRequest))
          as DeleteObjectRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use DeleteObjectRequest() / DeleteObjectRequest.new instead')
  static DeleteObjectRequest create() => DeleteObjectRequest._();
  static $pb.GeneratedMessage $_createMessage() => DeleteObjectRequest._();
  @$core.override
  DeleteObjectRequest createEmptyInstance() => DeleteObjectRequest._();
  @$core.pragma('dart2js:noInline')
  static DeleteObjectRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DeleteObjectRequest>(
          DeleteObjectRequest.$_createMessage);
  static DeleteObjectRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  Preconditions get preconditions => $_getN(2);
  @$pb.TagNumber(3)
  set preconditions(Preconditions value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasPreconditions() => $_has(2);
  @$pb.TagNumber(3)
  void clearPreconditions() => $_clearField(3);
  @$pb.TagNumber(3)
  Preconditions ensurePreconditions() => $_ensure(2);

  @$pb.TagNumber(4)
  MutationIdentity get mutation => $_getN(3);
  @$pb.TagNumber(4)
  set mutation(MutationIdentity value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasMutation() => $_has(3);
  @$pb.TagNumber(4)
  void clearMutation() => $_clearField(4);
  @$pb.TagNumber(4)
  MutationIdentity ensureMutation() => $_ensure(3);
}

class DeleteObjectResponse extends $pb.GeneratedMessage {
  factory DeleteObjectResponse({
    $core.bool? existed,
  }) {
    final result = DeleteObjectResponse._();
    if (existed != null) result.existed = existed;
    return result;
  }

  DeleteObjectResponse._();

  factory DeleteObjectResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteObjectResponse()..mergeFromBuffer(data, registry);
  factory DeleteObjectResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteObjectResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DeleteObjectResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: DeleteObjectResponse.$_createMessage)
    ..aOB(1, _omitFieldNames ? '' : 'existed')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteObjectResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteObjectResponse copyWith(void Function(DeleteObjectResponse) updates) =>
      super.copyWith((message) => updates(message as DeleteObjectResponse))
          as DeleteObjectResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use DeleteObjectResponse() / DeleteObjectResponse.new instead')
  static DeleteObjectResponse create() => DeleteObjectResponse._();
  static $pb.GeneratedMessage $_createMessage() => DeleteObjectResponse._();
  @$core.override
  DeleteObjectResponse createEmptyInstance() => DeleteObjectResponse._();
  @$core.pragma('dart2js:noInline')
  static DeleteObjectResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DeleteObjectResponse>(
          DeleteObjectResponse.$_createMessage);
  static DeleteObjectResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.bool get existed => $_getBF(0);
  @$pb.TagNumber(1)
  set existed($core.bool value) => $_setBool(0, value);
  @$pb.TagNumber(1)
  $core.bool hasExisted() => $_has(0);
  @$pb.TagNumber(1)
  void clearExisted() => $_clearField(1);
}

/// Lexicographic S3 ListObjectsV2 traversal of an eventual listing, never a captured view.
/// Concurrent insertions before the cursor can be missed. Tokens are opaque and query-bound.
class ListObjectsRequest extends $pb.GeneratedMessage {
  factory ListObjectsRequest({
    BucketRef? bucket,
    $core.String? prefix,
    $core.String? delimiter,
    $core.int? pageSize,
    $core.String? continuationToken,
  }) {
    final result = ListObjectsRequest._();
    if (bucket != null) result.bucket = bucket;
    if (prefix != null) result.prefix = prefix;
    if (delimiter != null) result.delimiter = delimiter;
    if (pageSize != null) result.pageSize = pageSize;
    if (continuationToken != null) result.continuationToken = continuationToken;
    return result;
  }

  ListObjectsRequest._();

  factory ListObjectsRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListObjectsRequest()..mergeFromBuffer(data, registry);
  factory ListObjectsRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListObjectsRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListObjectsRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ListObjectsRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'prefix')
    ..aOS(3, _omitFieldNames ? '' : 'delimiter')
    ..aI(4, _omitFieldNames ? '' : 'pageSize', fieldType: $pb.PbFieldType.OU3)
    ..aOS(5, _omitFieldNames ? '' : 'continuationToken')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListObjectsRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListObjectsRequest copyWith(void Function(ListObjectsRequest) updates) =>
      super.copyWith((message) => updates(message as ListObjectsRequest))
          as ListObjectsRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ListObjectsRequest() / ListObjectsRequest.new instead')
  static ListObjectsRequest create() => ListObjectsRequest._();
  static $pb.GeneratedMessage $_createMessage() => ListObjectsRequest._();
  @$core.override
  ListObjectsRequest createEmptyInstance() => ListObjectsRequest._();
  @$core.pragma('dart2js:noInline')
  static ListObjectsRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ListObjectsRequest>(
          ListObjectsRequest.$_createMessage);
  static ListObjectsRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get prefix => $_getSZ(1);
  @$pb.TagNumber(2)
  set prefix($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPrefix() => $_has(1);
  @$pb.TagNumber(2)
  void clearPrefix() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get delimiter => $_getSZ(2);
  @$pb.TagNumber(3)
  set delimiter($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasDelimiter() => $_has(2);
  @$pb.TagNumber(3)
  void clearDelimiter() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.int get pageSize => $_getIZ(3);
  @$pb.TagNumber(4)
  set pageSize($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasPageSize() => $_has(3);
  @$pb.TagNumber(4)
  void clearPageSize() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get continuationToken => $_getSZ(4);
  @$pb.TagNumber(5)
  set continuationToken($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasContinuationToken() => $_has(4);
  @$pb.TagNumber(5)
  void clearContinuationToken() => $_clearField(5);
}

class ListEntry extends $pb.GeneratedMessage {
  factory ListEntry({
    $core.String? objectKey,
    ObjectInfo? object,
  }) {
    final result = ListEntry._();
    if (objectKey != null) result.objectKey = objectKey;
    if (object != null) result.object = object;
    return result;
  }

  ListEntry._();

  factory ListEntry.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListEntry()..mergeFromBuffer(data, registry);
  factory ListEntry.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListEntry()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListEntry',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ListEntry.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'objectKey')
    ..aOM<ObjectInfo>(2, _omitFieldNames ? '' : 'object',
        subBuilder: ObjectInfo.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListEntry clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListEntry copyWith(void Function(ListEntry) updates) =>
      super.copyWith((message) => updates(message as ListEntry)) as ListEntry;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ListEntry() / ListEntry.new instead')
  static ListEntry create() => ListEntry._();
  static $pb.GeneratedMessage $_createMessage() => ListEntry._();
  @$core.override
  ListEntry createEmptyInstance() => ListEntry._();
  @$core.pragma('dart2js:noInline')
  static ListEntry getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ListEntry>(ListEntry.$_createMessage);
  static ListEntry? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get objectKey => $_getSZ(0);
  @$pb.TagNumber(1)
  set objectKey($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasObjectKey() => $_has(0);
  @$pb.TagNumber(1)
  void clearObjectKey() => $_clearField(1);

  @$pb.TagNumber(2)
  ObjectInfo get object => $_getN(1);
  @$pb.TagNumber(2)
  set object(ObjectInfo value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasObject() => $_has(1);
  @$pb.TagNumber(2)
  void clearObject() => $_clearField(2);
  @$pb.TagNumber(2)
  ObjectInfo ensureObject() => $_ensure(1);
}

class ListObjectsResponse extends $pb.GeneratedMessage {
  factory ListObjectsResponse({
    $core.Iterable<ListEntry>? entries,
    $core.Iterable<$core.String>? commonPrefixes,
    $core.String? continuationToken,
    $core.bool? isTruncated,
  }) {
    final result = ListObjectsResponse._();
    if (entries != null) result.entries.addAll(entries);
    if (commonPrefixes != null) result.commonPrefixes.addAll(commonPrefixes);
    if (continuationToken != null) result.continuationToken = continuationToken;
    if (isTruncated != null) result.isTruncated = isTruncated;
    return result;
  }

  ListObjectsResponse._();

  factory ListObjectsResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListObjectsResponse()..mergeFromBuffer(data, registry);
  factory ListObjectsResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListObjectsResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListObjectsResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ListObjectsResponse.$_createMessage)
    ..pPM<ListEntry>(1, _omitFieldNames ? '' : 'entries',
        subBuilder: ListEntry.$_createMessage)
    ..pPS(2, _omitFieldNames ? '' : 'commonPrefixes')
    ..aOS(3, _omitFieldNames ? '' : 'continuationToken')
    ..aOB(4, _omitFieldNames ? '' : 'isTruncated')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListObjectsResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListObjectsResponse copyWith(void Function(ListObjectsResponse) updates) =>
      super.copyWith((message) => updates(message as ListObjectsResponse))
          as ListObjectsResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use ListObjectsResponse() / ListObjectsResponse.new instead')
  static ListObjectsResponse create() => ListObjectsResponse._();
  static $pb.GeneratedMessage $_createMessage() => ListObjectsResponse._();
  @$core.override
  ListObjectsResponse createEmptyInstance() => ListObjectsResponse._();
  @$core.pragma('dart2js:noInline')
  static ListObjectsResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ListObjectsResponse>(
          ListObjectsResponse.$_createMessage);
  static ListObjectsResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<ListEntry> get entries => $_getList(0);

  @$pb.TagNumber(2)
  $pb.PbList<$core.String> get commonPrefixes => $_getList(1);

  @$pb.TagNumber(3)
  $core.String get continuationToken => $_getSZ(2);
  @$pb.TagNumber(3)
  set continuationToken($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasContinuationToken() => $_has(2);
  @$pb.TagNumber(3)
  void clearContinuationToken() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.bool get isTruncated => $_getBF(3);
  @$pb.TagNumber(4)
  set isTruncated($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasIsTruncated() => $_has(3);
  @$pb.TagNumber(4)
  void clearIsTruncated() => $_clearField(4);
}

class CreateMultipartRequest extends $pb.GeneratedMessage {
  factory CreateMultipartRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    ObjectMetadata? metadata,
    MutationIdentity? mutation,
  }) {
    final result = CreateMultipartRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (metadata != null) result.metadata = metadata;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  CreateMultipartRequest._();

  factory CreateMultipartRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateMultipartRequest()..mergeFromBuffer(data, registry);
  factory CreateMultipartRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateMultipartRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateMultipartRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: CreateMultipartRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOM<ObjectMetadata>(3, _omitFieldNames ? '' : 'metadata',
        subBuilder: ObjectMetadata.$_createMessage)
    ..aOM<MutationIdentity>(4, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateMultipartRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateMultipartRequest copyWith(
          void Function(CreateMultipartRequest) updates) =>
      super.copyWith((message) => updates(message as CreateMultipartRequest))
          as CreateMultipartRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CreateMultipartRequest() / CreateMultipartRequest.new instead')
  static CreateMultipartRequest create() => CreateMultipartRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateMultipartRequest._();
  @$core.override
  CreateMultipartRequest createEmptyInstance() => CreateMultipartRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateMultipartRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateMultipartRequest>(
          CreateMultipartRequest.$_createMessage);
  static CreateMultipartRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  ObjectMetadata get metadata => $_getN(2);
  @$pb.TagNumber(3)
  set metadata(ObjectMetadata value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMetadata() => $_has(2);
  @$pb.TagNumber(3)
  void clearMetadata() => $_clearField(3);
  @$pb.TagNumber(3)
  ObjectMetadata ensureMetadata() => $_ensure(2);

  @$pb.TagNumber(4)
  MutationIdentity get mutation => $_getN(3);
  @$pb.TagNumber(4)
  set mutation(MutationIdentity value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasMutation() => $_has(3);
  @$pb.TagNumber(4)
  void clearMutation() => $_clearField(4);
  @$pb.TagNumber(4)
  MutationIdentity ensureMutation() => $_ensure(3);
}

class MultipartUpload extends $pb.GeneratedMessage {
  factory MultipartUpload({
    $core.String? uploadId,
  }) {
    final result = MultipartUpload._();
    if (uploadId != null) result.uploadId = uploadId;
    return result;
  }

  MultipartUpload._();

  factory MultipartUpload.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MultipartUpload()..mergeFromBuffer(data, registry);
  factory MultipartUpload.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MultipartUpload()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MultipartUpload',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: MultipartUpload.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'uploadId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MultipartUpload clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MultipartUpload copyWith(void Function(MultipartUpload) updates) =>
      super.copyWith((message) => updates(message as MultipartUpload))
          as MultipartUpload;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MultipartUpload() / MultipartUpload.new instead')
  static MultipartUpload create() => MultipartUpload._();
  static $pb.GeneratedMessage $_createMessage() => MultipartUpload._();
  @$core.override
  MultipartUpload createEmptyInstance() => MultipartUpload._();
  @$core.pragma('dart2js:noInline')
  static MultipartUpload getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MultipartUpload>(
          MultipartUpload.$_createMessage);
  static MultipartUpload? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get uploadId => $_getSZ(0);
  @$pb.TagNumber(1)
  set uploadId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasUploadId() => $_has(0);
  @$pb.TagNumber(1)
  void clearUploadId() => $_clearField(1);
}

class UploadPartHeader extends $pb.GeneratedMessage {
  factory UploadPartHeader({
    BucketRef? bucket,
    $core.String? objectKey,
    $core.String? uploadId,
    $core.int? partNumber,
    MutationIdentity? mutation,
  }) {
    final result = UploadPartHeader._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (uploadId != null) result.uploadId = uploadId;
    if (partNumber != null) result.partNumber = partNumber;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  UploadPartHeader._();

  factory UploadPartHeader.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UploadPartHeader()..mergeFromBuffer(data, registry);
  factory UploadPartHeader.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UploadPartHeader()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UploadPartHeader',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: UploadPartHeader.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'uploadId')
    ..aI(4, _omitFieldNames ? '' : 'partNumber', fieldType: $pb.PbFieldType.OU3)
    ..aOM<MutationIdentity>(5, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UploadPartHeader clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UploadPartHeader copyWith(void Function(UploadPartHeader) updates) =>
      super.copyWith((message) => updates(message as UploadPartHeader))
          as UploadPartHeader;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use UploadPartHeader() / UploadPartHeader.new instead')
  static UploadPartHeader create() => UploadPartHeader._();
  static $pb.GeneratedMessage $_createMessage() => UploadPartHeader._();
  @$core.override
  UploadPartHeader createEmptyInstance() => UploadPartHeader._();
  @$core.pragma('dart2js:noInline')
  static UploadPartHeader getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<UploadPartHeader>(
          UploadPartHeader.$_createMessage);
  static UploadPartHeader? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get uploadId => $_getSZ(2);
  @$pb.TagNumber(3)
  set uploadId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasUploadId() => $_has(2);
  @$pb.TagNumber(3)
  void clearUploadId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.int get partNumber => $_getIZ(3);
  @$pb.TagNumber(4)
  set partNumber($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasPartNumber() => $_has(3);
  @$pb.TagNumber(4)
  void clearPartNumber() => $_clearField(4);

  @$pb.TagNumber(5)
  MutationIdentity get mutation => $_getN(4);
  @$pb.TagNumber(5)
  set mutation(MutationIdentity value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasMutation() => $_has(4);
  @$pb.TagNumber(5)
  void clearMutation() => $_clearField(5);
  @$pb.TagNumber(5)
  MutationIdentity ensureMutation() => $_ensure(4);
}

enum UploadPartRequest_Frame { header, body, complete, notSet }

class UploadPartRequest extends $pb.GeneratedMessage {
  factory UploadPartRequest({
    UploadPartHeader? header,
    $core.List<$core.int>? body,
    $core.bool? complete,
  }) {
    final result = UploadPartRequest._();
    if (header != null) result.header = header;
    if (body != null) result.body = body;
    if (complete != null) result.complete = complete;
    return result;
  }

  UploadPartRequest._();

  factory UploadPartRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UploadPartRequest()..mergeFromBuffer(data, registry);
  factory UploadPartRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UploadPartRequest()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, UploadPartRequest_Frame>
      _UploadPartRequest_FrameByTag = {
    1: UploadPartRequest_Frame.header,
    2: UploadPartRequest_Frame.body,
    3: UploadPartRequest_Frame.complete,
    0: UploadPartRequest_Frame.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UploadPartRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: UploadPartRequest.$_createMessage)
    ..oo(0, [1, 2, 3])
    ..aOM<UploadPartHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: UploadPartHeader.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
    ..aOB(3, _omitFieldNames ? '' : 'complete')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UploadPartRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UploadPartRequest copyWith(void Function(UploadPartRequest) updates) =>
      super.copyWith((message) => updates(message as UploadPartRequest))
          as UploadPartRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use UploadPartRequest() / UploadPartRequest.new instead')
  static UploadPartRequest create() => UploadPartRequest._();
  static $pb.GeneratedMessage $_createMessage() => UploadPartRequest._();
  @$core.override
  UploadPartRequest createEmptyInstance() => UploadPartRequest._();
  @$core.pragma('dart2js:noInline')
  static UploadPartRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<UploadPartRequest>(
          UploadPartRequest.$_createMessage);
  static UploadPartRequest? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  UploadPartRequest_Frame whichFrame() =>
      _UploadPartRequest_FrameByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  void clearFrame() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  UploadPartHeader get header => $_getN(0);
  @$pb.TagNumber(1)
  set header(UploadPartHeader value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasHeader() => $_has(0);
  @$pb.TagNumber(1)
  void clearHeader() => $_clearField(1);
  @$pb.TagNumber(1)
  UploadPartHeader ensureHeader() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get body => $_getN(1);
  @$pb.TagNumber(2)
  set body($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBody() => $_has(1);
  @$pb.TagNumber(2)
  void clearBody() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.bool get complete => $_getBF(2);
  @$pb.TagNumber(3)
  set complete($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasComplete() => $_has(2);
  @$pb.TagNumber(3)
  void clearComplete() => $_clearField(3);
}

class UploadedPart extends $pb.GeneratedMessage {
  factory UploadedPart({
    $core.int? partNumber,
    $core.String? etag,
    $fixnum.Int64? size,
  }) {
    final result = UploadedPart._();
    if (partNumber != null) result.partNumber = partNumber;
    if (etag != null) result.etag = etag;
    if (size != null) result.size = size;
    return result;
  }

  UploadedPart._();

  factory UploadedPart.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UploadedPart()..mergeFromBuffer(data, registry);
  factory UploadedPart.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UploadedPart()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UploadedPart',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: UploadedPart.$_createMessage)
    ..aI(1, _omitFieldNames ? '' : 'partNumber', fieldType: $pb.PbFieldType.OU3)
    ..aOS(2, _omitFieldNames ? '' : 'etag')
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'size', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UploadedPart clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UploadedPart copyWith(void Function(UploadedPart) updates) =>
      super.copyWith((message) => updates(message as UploadedPart))
          as UploadedPart;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use UploadedPart() / UploadedPart.new instead')
  static UploadedPart create() => UploadedPart._();
  static $pb.GeneratedMessage $_createMessage() => UploadedPart._();
  @$core.override
  UploadedPart createEmptyInstance() => UploadedPart._();
  @$core.pragma('dart2js:noInline')
  static UploadedPart getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<UploadedPart>(
          UploadedPart.$_createMessage);
  static UploadedPart? _defaultInstance;

  @$pb.TagNumber(1)
  $core.int get partNumber => $_getIZ(0);
  @$pb.TagNumber(1)
  set partNumber($core.int value) => $_setUnsignedInt32(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPartNumber() => $_has(0);
  @$pb.TagNumber(1)
  void clearPartNumber() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get etag => $_getSZ(1);
  @$pb.TagNumber(2)
  set etag($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasEtag() => $_has(1);
  @$pb.TagNumber(2)
  void clearEtag() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get size => $_getI64(2);
  @$pb.TagNumber(3)
  set size($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasSize() => $_has(2);
  @$pb.TagNumber(3)
  void clearSize() => $_clearField(3);
}

class ListPartsRequest extends $pb.GeneratedMessage {
  factory ListPartsRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    $core.String? uploadId,
    $core.int? afterPartNumber,
    $core.int? pageSize,
  }) {
    final result = ListPartsRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (uploadId != null) result.uploadId = uploadId;
    if (afterPartNumber != null) result.afterPartNumber = afterPartNumber;
    if (pageSize != null) result.pageSize = pageSize;
    return result;
  }

  ListPartsRequest._();

  factory ListPartsRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListPartsRequest()..mergeFromBuffer(data, registry);
  factory ListPartsRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListPartsRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListPartsRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ListPartsRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'uploadId')
    ..aI(4, _omitFieldNames ? '' : 'afterPartNumber',
        fieldType: $pb.PbFieldType.OU3)
    ..aI(5, _omitFieldNames ? '' : 'pageSize', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListPartsRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListPartsRequest copyWith(void Function(ListPartsRequest) updates) =>
      super.copyWith((message) => updates(message as ListPartsRequest))
          as ListPartsRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ListPartsRequest() / ListPartsRequest.new instead')
  static ListPartsRequest create() => ListPartsRequest._();
  static $pb.GeneratedMessage $_createMessage() => ListPartsRequest._();
  @$core.override
  ListPartsRequest createEmptyInstance() => ListPartsRequest._();
  @$core.pragma('dart2js:noInline')
  static ListPartsRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ListPartsRequest>(
          ListPartsRequest.$_createMessage);
  static ListPartsRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get uploadId => $_getSZ(2);
  @$pb.TagNumber(3)
  set uploadId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasUploadId() => $_has(2);
  @$pb.TagNumber(3)
  void clearUploadId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.int get afterPartNumber => $_getIZ(3);
  @$pb.TagNumber(4)
  set afterPartNumber($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasAfterPartNumber() => $_has(3);
  @$pb.TagNumber(4)
  void clearAfterPartNumber() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.int get pageSize => $_getIZ(4);
  @$pb.TagNumber(5)
  set pageSize($core.int value) => $_setUnsignedInt32(4, value);
  @$pb.TagNumber(5)
  $core.bool hasPageSize() => $_has(4);
  @$pb.TagNumber(5)
  void clearPageSize() => $_clearField(5);
}

class ListPartsResponse extends $pb.GeneratedMessage {
  factory ListPartsResponse({
    $core.Iterable<UploadedPart>? parts,
    $core.int? nextPartNumber,
    $core.bool? isTruncated,
  }) {
    final result = ListPartsResponse._();
    if (parts != null) result.parts.addAll(parts);
    if (nextPartNumber != null) result.nextPartNumber = nextPartNumber;
    if (isTruncated != null) result.isTruncated = isTruncated;
    return result;
  }

  ListPartsResponse._();

  factory ListPartsResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListPartsResponse()..mergeFromBuffer(data, registry);
  factory ListPartsResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListPartsResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListPartsResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ListPartsResponse.$_createMessage)
    ..pPM<UploadedPart>(1, _omitFieldNames ? '' : 'parts',
        subBuilder: UploadedPart.$_createMessage)
    ..aI(2, _omitFieldNames ? '' : 'nextPartNumber',
        fieldType: $pb.PbFieldType.OU3)
    ..aOB(3, _omitFieldNames ? '' : 'isTruncated')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListPartsResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListPartsResponse copyWith(void Function(ListPartsResponse) updates) =>
      super.copyWith((message) => updates(message as ListPartsResponse))
          as ListPartsResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ListPartsResponse() / ListPartsResponse.new instead')
  static ListPartsResponse create() => ListPartsResponse._();
  static $pb.GeneratedMessage $_createMessage() => ListPartsResponse._();
  @$core.override
  ListPartsResponse createEmptyInstance() => ListPartsResponse._();
  @$core.pragma('dart2js:noInline')
  static ListPartsResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ListPartsResponse>(
          ListPartsResponse.$_createMessage);
  static ListPartsResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<UploadedPart> get parts => $_getList(0);

  @$pb.TagNumber(2)
  $core.int get nextPartNumber => $_getIZ(1);
  @$pb.TagNumber(2)
  set nextPartNumber($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(2)
  $core.bool hasNextPartNumber() => $_has(1);
  @$pb.TagNumber(2)
  void clearNextPartNumber() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.bool get isTruncated => $_getBF(2);
  @$pb.TagNumber(3)
  set isTruncated($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIsTruncated() => $_has(2);
  @$pb.TagNumber(3)
  void clearIsTruncated() => $_clearField(3);
}

/// Ordered part numbers and exact part ETags select the staged bytes.
/// Non-final parts are at least 5 MiB. Publication and preconditions are atomic for this key.
class CompleteMultipartRequest extends $pb.GeneratedMessage {
  factory CompleteMultipartRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    $core.String? uploadId,
    $core.Iterable<UploadedPart>? parts,
    Preconditions? preconditions,
    MutationIdentity? mutation,
  }) {
    final result = CompleteMultipartRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (uploadId != null) result.uploadId = uploadId;
    if (parts != null) result.parts.addAll(parts);
    if (preconditions != null) result.preconditions = preconditions;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  CompleteMultipartRequest._();

  factory CompleteMultipartRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CompleteMultipartRequest()..mergeFromBuffer(data, registry);
  factory CompleteMultipartRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CompleteMultipartRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CompleteMultipartRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: CompleteMultipartRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'uploadId')
    ..pPM<UploadedPart>(4, _omitFieldNames ? '' : 'parts',
        subBuilder: UploadedPart.$_createMessage)
    ..aOM<Preconditions>(5, _omitFieldNames ? '' : 'preconditions',
        subBuilder: Preconditions.$_createMessage)
    ..aOM<MutationIdentity>(6, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CompleteMultipartRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CompleteMultipartRequest copyWith(
          void Function(CompleteMultipartRequest) updates) =>
      super.copyWith((message) => updates(message as CompleteMultipartRequest))
          as CompleteMultipartRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CompleteMultipartRequest() / CompleteMultipartRequest.new instead')
  static CompleteMultipartRequest create() => CompleteMultipartRequest._();
  static $pb.GeneratedMessage $_createMessage() => CompleteMultipartRequest._();
  @$core.override
  CompleteMultipartRequest createEmptyInstance() =>
      CompleteMultipartRequest._();
  @$core.pragma('dart2js:noInline')
  static CompleteMultipartRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CompleteMultipartRequest>(
          CompleteMultipartRequest.$_createMessage);
  static CompleteMultipartRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get uploadId => $_getSZ(2);
  @$pb.TagNumber(3)
  set uploadId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasUploadId() => $_has(2);
  @$pb.TagNumber(3)
  void clearUploadId() => $_clearField(3);

  @$pb.TagNumber(4)
  $pb.PbList<UploadedPart> get parts => $_getList(3);

  @$pb.TagNumber(5)
  Preconditions get preconditions => $_getN(4);
  @$pb.TagNumber(5)
  set preconditions(Preconditions value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasPreconditions() => $_has(4);
  @$pb.TagNumber(5)
  void clearPreconditions() => $_clearField(5);
  @$pb.TagNumber(5)
  Preconditions ensurePreconditions() => $_ensure(4);

  @$pb.TagNumber(6)
  MutationIdentity get mutation => $_getN(5);
  @$pb.TagNumber(6)
  set mutation(MutationIdentity value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasMutation() => $_has(5);
  @$pb.TagNumber(6)
  void clearMutation() => $_clearField(6);
  @$pb.TagNumber(6)
  MutationIdentity ensureMutation() => $_ensure(5);
}

class AbortMultipartRequest extends $pb.GeneratedMessage {
  factory AbortMultipartRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    $core.String? uploadId,
    MutationIdentity? mutation,
  }) {
    final result = AbortMultipartRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (uploadId != null) result.uploadId = uploadId;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  AbortMultipartRequest._();

  factory AbortMultipartRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AbortMultipartRequest()..mergeFromBuffer(data, registry);
  factory AbortMultipartRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AbortMultipartRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AbortMultipartRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: AbortMultipartRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'uploadId')
    ..aOM<MutationIdentity>(4, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AbortMultipartRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AbortMultipartRequest copyWith(
          void Function(AbortMultipartRequest) updates) =>
      super.copyWith((message) => updates(message as AbortMultipartRequest))
          as AbortMultipartRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use AbortMultipartRequest() / AbortMultipartRequest.new instead')
  static AbortMultipartRequest create() => AbortMultipartRequest._();
  static $pb.GeneratedMessage $_createMessage() => AbortMultipartRequest._();
  @$core.override
  AbortMultipartRequest createEmptyInstance() => AbortMultipartRequest._();
  @$core.pragma('dart2js:noInline')
  static AbortMultipartRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<AbortMultipartRequest>(
          AbortMultipartRequest.$_createMessage);
  static AbortMultipartRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get bucket => $_getN(0);
  @$pb.TagNumber(1)
  set bucket(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureBucket() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get uploadId => $_getSZ(2);
  @$pb.TagNumber(3)
  set uploadId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasUploadId() => $_has(2);
  @$pb.TagNumber(3)
  void clearUploadId() => $_clearField(3);

  @$pb.TagNumber(4)
  MutationIdentity get mutation => $_getN(3);
  @$pb.TagNumber(4)
  set mutation(MutationIdentity value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasMutation() => $_has(3);
  @$pb.TagNumber(4)
  void clearMutation() => $_clearField(4);
  @$pb.TagNumber(4)
  MutationIdentity ensureMutation() => $_ensure(3);
}

class AbortMultipartResponse extends $pb.GeneratedMessage {
  factory AbortMultipartResponse({
    $core.bool? existed,
  }) {
    final result = AbortMultipartResponse._();
    if (existed != null) result.existed = existed;
    return result;
  }

  AbortMultipartResponse._();

  factory AbortMultipartResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AbortMultipartResponse()..mergeFromBuffer(data, registry);
  factory AbortMultipartResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      AbortMultipartResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'AbortMultipartResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: AbortMultipartResponse.$_createMessage)
    ..aOB(1, _omitFieldNames ? '' : 'existed')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AbortMultipartResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  AbortMultipartResponse copyWith(
          void Function(AbortMultipartResponse) updates) =>
      super.copyWith((message) => updates(message as AbortMultipartResponse))
          as AbortMultipartResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use AbortMultipartResponse() / AbortMultipartResponse.new instead')
  static AbortMultipartResponse create() => AbortMultipartResponse._();
  static $pb.GeneratedMessage $_createMessage() => AbortMultipartResponse._();
  @$core.override
  AbortMultipartResponse createEmptyInstance() => AbortMultipartResponse._();
  @$core.pragma('dart2js:noInline')
  static AbortMultipartResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<AbortMultipartResponse>(
          AbortMultipartResponse.$_createMessage);
  static AbortMultipartResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.bool get existed => $_getBF(0);
  @$pb.TagNumber(1)
  set existed($core.bool value) => $_setBool(0, value);
  @$pb.TagNumber(1)
  $core.bool hasExisted() => $_has(0);
  @$pb.TagNumber(1)
  void clearExisted() => $_clearField(1);
}

/// Customer diagnostic identity; operator topology stays in protected evidence.
class ErrorDetail extends $pb.GeneratedMessage {
  factory ErrorDetail({
    ErrorCode? code,
    $core.String? requestId,
  }) {
    final result = ErrorDetail._();
    if (code != null) result.code = code;
    if (requestId != null) result.requestId = requestId;
    return result;
  }

  ErrorDetail._();

  factory ErrorDetail.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ErrorDetail()..mergeFromBuffer(data, registry);
  factory ErrorDetail.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ErrorDetail()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ErrorDetail',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v2'),
      createEmptyInstance: ErrorDetail.$_createMessage)
    ..aE<ErrorCode>(1, _omitFieldNames ? '' : 'code',
        enumValues: ErrorCode.values)
    ..aOS(2, _omitFieldNames ? '' : 'requestId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ErrorDetail clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ErrorDetail copyWith(void Function(ErrorDetail) updates) =>
      super.copyWith((message) => updates(message as ErrorDetail))
          as ErrorDetail;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ErrorDetail() / ErrorDetail.new instead')
  static ErrorDetail create() => ErrorDetail._();
  static $pb.GeneratedMessage $_createMessage() => ErrorDetail._();
  @$core.override
  ErrorDetail createEmptyInstance() => ErrorDetail._();
  @$core.pragma('dart2js:noInline')
  static ErrorDetail getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ErrorDetail>(
          ErrorDetail.$_createMessage);
  static ErrorDetail? _defaultInstance;

  @$pb.TagNumber(1)
  ErrorCode get code => $_getN(0);
  @$pb.TagNumber(1)
  set code(ErrorCode value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCode() => $_has(0);
  @$pb.TagNumber(1)
  void clearCode() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get requestId => $_getSZ(1);
  @$pb.TagNumber(2)
  set requestId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRequestId() => $_has(1);
  @$pb.TagNumber(2)
  void clearRequestId() => $_clearField(2);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
