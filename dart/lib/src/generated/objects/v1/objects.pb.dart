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

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;
import 'package:protobuf/well_known_types/google/protobuf/timestamp.pb.dart'
    as $1;

import 'objects.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'objects.pbenum.dart';

class BucketRef extends $pb.GeneratedMessage {
  factory BucketRef({
    $core.String? bucketId,
    $core.String? name,
  }) {
    final result = BucketRef._();
    if (bucketId != null) result.bucketId = bucketId;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: BucketRef.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'bucketId')
    ..aOS(2, _omitFieldNames ? '' : 'name')
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
  $core.String get bucketId => $_getSZ(0);
  @$pb.TagNumber(1)
  set bucketId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasBucketId() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucketId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get name => $_getSZ(1);
  @$pb.TagNumber(2)
  set name($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => $_clearField(2);
}

class SnapshotRef extends $pb.GeneratedMessage {
  factory SnapshotRef({
    $core.String? snapshotId,
    $core.String? sourceBucketId,
  }) {
    final result = SnapshotRef._();
    if (snapshotId != null) result.snapshotId = snapshotId;
    if (sourceBucketId != null) result.sourceBucketId = sourceBucketId;
    return result;
  }

  SnapshotRef._();

  factory SnapshotRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SnapshotRef()..mergeFromBuffer(data, registry);
  factory SnapshotRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SnapshotRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SnapshotRef',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: SnapshotRef.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'snapshotId')
    ..aOS(2, _omitFieldNames ? '' : 'sourceBucketId')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SnapshotRef copyWith(void Function(SnapshotRef) updates) =>
      super.copyWith((message) => updates(message as SnapshotRef))
          as SnapshotRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SnapshotRef() / SnapshotRef.new instead')
  static SnapshotRef create() => SnapshotRef._();
  static $pb.GeneratedMessage $_createMessage() => SnapshotRef._();
  @$core.override
  SnapshotRef createEmptyInstance() => SnapshotRef._();
  @$core.pragma('dart2js:noInline')
  static SnapshotRef getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SnapshotRef>(
          SnapshotRef.$_createMessage);
  static SnapshotRef? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get snapshotId => $_getSZ(0);
  @$pb.TagNumber(1)
  set snapshotId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSnapshotId() => $_has(0);
  @$pb.TagNumber(1)
  void clearSnapshotId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get sourceBucketId => $_getSZ(1);
  @$pb.TagNumber(2)
  set sourceBucketId($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSourceBucketId() => $_has(1);
  @$pb.TagNumber(2)
  void clearSourceBucketId() => $_clearField(2);
}

enum ReadTarget_Target { bucket, snapshot, notSet }

class ReadTarget extends $pb.GeneratedMessage {
  factory ReadTarget({
    BucketRef? bucket,
    SnapshotRef? snapshot,
  }) {
    final result = ReadTarget._();
    if (bucket != null) result.bucket = bucket;
    if (snapshot != null) result.snapshot = snapshot;
    return result;
  }

  ReadTarget._();

  factory ReadTarget.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadTarget()..mergeFromBuffer(data, registry);
  factory ReadTarget.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadTarget()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, ReadTarget_Target> _ReadTarget_TargetByTag =
      {
    1: ReadTarget_Target.bucket,
    2: ReadTarget_Target.snapshot,
    0: ReadTarget_Target.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReadTarget',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ReadTarget.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOM<SnapshotRef>(2, _omitFieldNames ? '' : 'snapshot',
        subBuilder: SnapshotRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadTarget clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadTarget copyWith(void Function(ReadTarget) updates) =>
      super.copyWith((message) => updates(message as ReadTarget)) as ReadTarget;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ReadTarget() / ReadTarget.new instead')
  static ReadTarget create() => ReadTarget._();
  static $pb.GeneratedMessage $_createMessage() => ReadTarget._();
  @$core.override
  ReadTarget createEmptyInstance() => ReadTarget._();
  @$core.pragma('dart2js:noInline')
  static ReadTarget getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ReadTarget>(ReadTarget.$_createMessage);
  static ReadTarget? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  ReadTarget_Target whichTarget() => _ReadTarget_TargetByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearTarget() => $_clearField($_whichOneof(0));

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
  SnapshotRef get snapshot => $_getN(1);
  @$pb.TagNumber(2)
  set snapshot(SnapshotRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSnapshot() => $_has(1);
  @$pb.TagNumber(2)
  void clearSnapshot() => $_clearField(2);
  @$pb.TagNumber(2)
  SnapshotRef ensureSnapshot() => $_ensure(1);
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ObjectMetadata.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'contentType')
    ..m<$core.String, $core.String>(2, _omitFieldNames ? '' : 'user',
        entryClassName: 'ObjectMetadata.UserEntry',
        keyFieldType: $pb.PbFieldType.OS,
        valueFieldType: $pb.PbFieldType.OS,
        packageName: const $pb.PackageName('acyclic.objects.v1'))
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

enum Preconditions_Condition { ifAbsent, ifMatch, ifVersion, notSet }

class Preconditions extends $pb.GeneratedMessage {
  factory Preconditions({
    $core.bool? ifAbsent,
    $core.String? ifMatch,
    $core.String? ifVersion,
  }) {
    final result = Preconditions._();
    if (ifAbsent != null) result.ifAbsent = ifAbsent;
    if (ifMatch != null) result.ifMatch = ifMatch;
    if (ifVersion != null) result.ifVersion = ifVersion;
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
    3: Preconditions_Condition.ifVersion,
    0: Preconditions_Condition.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Preconditions',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: Preconditions.$_createMessage)
    ..oo(0, [1, 2, 3])
    ..aOB(1, _omitFieldNames ? '' : 'ifAbsent')
    ..aOS(2, _omitFieldNames ? '' : 'ifMatch')
    ..aOS(3, _omitFieldNames ? '' : 'ifVersion')
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
  @$pb.TagNumber(3)
  Preconditions_Condition whichCondition() =>
      _Preconditions_ConditionByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
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

  @$pb.TagNumber(3)
  $core.String get ifVersion => $_getSZ(2);
  @$pb.TagNumber(3)
  set ifVersion($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasIfVersion() => $_has(2);
  @$pb.TagNumber(3)
  void clearIfVersion() => $_clearField(3);
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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

enum PutObjectRequest_Frame { header, body, notSet }

class PutObjectRequest extends $pb.GeneratedMessage {
  factory PutObjectRequest({
    PutObjectHeader? header,
    $core.List<$core.int>? body,
  }) {
    final result = PutObjectRequest._();
    if (header != null) result.header = header;
    if (body != null) result.body = body;
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
    0: PutObjectRequest_Frame.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PutObjectRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: PutObjectRequest.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<PutObjectHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: PutObjectHeader.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
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
  PutObjectRequest_Frame whichFrame() =>
      _PutObjectRequest_FrameByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
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
}

class ObjectVersion extends $pb.GeneratedMessage {
  factory ObjectVersion({
    $core.String? versionId,
    $core.String? etag,
    $fixnum.Int64? size,
    $core.bool? deleteMarker,
    ObjectMetadata? metadata,
    $1.Timestamp? createdAt,
  }) {
    final result = ObjectVersion._();
    if (versionId != null) result.versionId = versionId;
    if (etag != null) result.etag = etag;
    if (size != null) result.size = size;
    if (deleteMarker != null) result.deleteMarker = deleteMarker;
    if (metadata != null) result.metadata = metadata;
    if (createdAt != null) result.createdAt = createdAt;
    return result;
  }

  ObjectVersion._();

  factory ObjectVersion.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObjectVersion()..mergeFromBuffer(data, registry);
  factory ObjectVersion.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObjectVersion()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ObjectVersion',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ObjectVersion.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'versionId')
    ..aOS(2, _omitFieldNames ? '' : 'etag')
    ..a<$fixnum.Int64>(3, _omitFieldNames ? '' : 'size', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOB(4, _omitFieldNames ? '' : 'deleteMarker')
    ..aOM<ObjectMetadata>(5, _omitFieldNames ? '' : 'metadata',
        subBuilder: ObjectMetadata.$_createMessage)
    ..aOM<$1.Timestamp>(6, _omitFieldNames ? '' : 'createdAt',
        subBuilder: $1.Timestamp.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObjectVersion clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObjectVersion copyWith(void Function(ObjectVersion) updates) =>
      super.copyWith((message) => updates(message as ObjectVersion))
          as ObjectVersion;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ObjectVersion() / ObjectVersion.new instead')
  static ObjectVersion create() => ObjectVersion._();
  static $pb.GeneratedMessage $_createMessage() => ObjectVersion._();
  @$core.override
  ObjectVersion createEmptyInstance() => ObjectVersion._();
  @$core.pragma('dart2js:noInline')
  static ObjectVersion getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ObjectVersion>(
          ObjectVersion.$_createMessage);
  static ObjectVersion? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get versionId => $_getSZ(0);
  @$pb.TagNumber(1)
  set versionId($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasVersionId() => $_has(0);
  @$pb.TagNumber(1)
  void clearVersionId() => $_clearField(1);

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

  @$pb.TagNumber(4)
  $core.bool get deleteMarker => $_getBF(3);
  @$pb.TagNumber(4)
  set deleteMarker($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasDeleteMarker() => $_has(3);
  @$pb.TagNumber(4)
  void clearDeleteMarker() => $_clearField(4);

  @$pb.TagNumber(5)
  ObjectMetadata get metadata => $_getN(4);
  @$pb.TagNumber(5)
  set metadata(ObjectMetadata value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasMetadata() => $_has(4);
  @$pb.TagNumber(5)
  void clearMetadata() => $_clearField(5);
  @$pb.TagNumber(5)
  ObjectMetadata ensureMetadata() => $_ensure(4);

  @$pb.TagNumber(6)
  $1.Timestamp get createdAt => $_getN(5);
  @$pb.TagNumber(6)
  set createdAt($1.Timestamp value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasCreatedAt() => $_has(5);
  @$pb.TagNumber(6)
  void clearCreatedAt() => $_clearField(6);
  @$pb.TagNumber(6)
  $1.Timestamp ensureCreatedAt() => $_ensure(5);
}

class GetObjectRequest extends $pb.GeneratedMessage {
  factory GetObjectRequest({
    ReadTarget? target,
    $core.String? objectKey,
    $core.String? versionId,
    $fixnum.Int64? rangeStart,
    $fixnum.Int64? rangeEndInclusive,
    $core.String? ifMatch,
    $core.String? ifNoneMatch,
    $core.bool? rangeRequested,
  }) {
    final result = GetObjectRequest._();
    if (target != null) result.target = target;
    if (objectKey != null) result.objectKey = objectKey;
    if (versionId != null) result.versionId = versionId;
    if (rangeStart != null) result.rangeStart = rangeStart;
    if (rangeEndInclusive != null) result.rangeEndInclusive = rangeEndInclusive;
    if (ifMatch != null) result.ifMatch = ifMatch;
    if (ifNoneMatch != null) result.ifNoneMatch = ifNoneMatch;
    if (rangeRequested != null) result.rangeRequested = rangeRequested;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: GetObjectRequest.$_createMessage)
    ..aOM<ReadTarget>(1, _omitFieldNames ? '' : 'target',
        subBuilder: ReadTarget.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'versionId')
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'rangeStart', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'rangeEndInclusive', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(6, _omitFieldNames ? '' : 'ifMatch')
    ..aOS(7, _omitFieldNames ? '' : 'ifNoneMatch')
    ..aOB(8, _omitFieldNames ? '' : 'rangeRequested')
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
  ReadTarget get target => $_getN(0);
  @$pb.TagNumber(1)
  set target(ReadTarget value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasTarget() => $_has(0);
  @$pb.TagNumber(1)
  void clearTarget() => $_clearField(1);
  @$pb.TagNumber(1)
  ReadTarget ensureTarget() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get versionId => $_getSZ(2);
  @$pb.TagNumber(3)
  set versionId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasVersionId() => $_has(2);
  @$pb.TagNumber(3)
  void clearVersionId() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get rangeStart => $_getI64(3);
  @$pb.TagNumber(4)
  set rangeStart($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasRangeStart() => $_has(3);
  @$pb.TagNumber(4)
  void clearRangeStart() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get rangeEndInclusive => $_getI64(4);
  @$pb.TagNumber(5)
  set rangeEndInclusive($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasRangeEndInclusive() => $_has(4);
  @$pb.TagNumber(5)
  void clearRangeEndInclusive() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get ifMatch => $_getSZ(5);
  @$pb.TagNumber(6)
  set ifMatch($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasIfMatch() => $_has(5);
  @$pb.TagNumber(6)
  void clearIfMatch() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.String get ifNoneMatch => $_getSZ(6);
  @$pb.TagNumber(7)
  set ifNoneMatch($core.String value) => $_setString(6, value);
  @$pb.TagNumber(7)
  $core.bool hasIfNoneMatch() => $_has(6);
  @$pb.TagNumber(7)
  void clearIfNoneMatch() => $_clearField(7);

  /// Required for every ranged read, distinguishing an open-ended range
  /// beginning at byte zero from no range. Range fields are invalid when this
  /// bit is false.
  @$pb.TagNumber(8)
  $core.bool get rangeRequested => $_getBF(7);
  @$pb.TagNumber(8)
  set rangeRequested($core.bool value) => $_setBool(7, value);
  @$pb.TagNumber(8)
  $core.bool hasRangeRequested() => $_has(7);
  @$pb.TagNumber(8)
  void clearRangeRequested() => $_clearField(8);
}

enum GetObjectResponse_Frame { version, body, notSet }

class GetObjectResponse extends $pb.GeneratedMessage {
  factory GetObjectResponse({
    ObjectVersion? version,
    $core.List<$core.int>? body,
  }) {
    final result = GetObjectResponse._();
    if (version != null) result.version = version;
    if (body != null) result.body = body;
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
    1: GetObjectResponse_Frame.version,
    2: GetObjectResponse_Frame.body,
    0: GetObjectResponse_Frame.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GetObjectResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: GetObjectResponse.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<ObjectVersion>(1, _omitFieldNames ? '' : 'version',
        subBuilder: ObjectVersion.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
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
  GetObjectResponse_Frame whichFrame() =>
      _GetObjectResponse_FrameByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearFrame() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  ObjectVersion get version => $_getN(0);
  @$pb.TagNumber(1)
  set version(ObjectVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasVersion() => $_has(0);
  @$pb.TagNumber(1)
  void clearVersion() => $_clearField(1);
  @$pb.TagNumber(1)
  ObjectVersion ensureVersion() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get body => $_getN(1);
  @$pb.TagNumber(2)
  set body($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBody() => $_has(1);
  @$pb.TagNumber(2)
  void clearBody() => $_clearField(2);
}

class HeadObjectRequest extends $pb.GeneratedMessage {
  factory HeadObjectRequest({
    ReadTarget? target,
    $core.String? objectKey,
    $core.String? versionId,
    $core.String? ifMatch,
    $core.String? ifNoneMatch,
  }) {
    final result = HeadObjectRequest._();
    if (target != null) result.target = target;
    if (objectKey != null) result.objectKey = objectKey;
    if (versionId != null) result.versionId = versionId;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: HeadObjectRequest.$_createMessage)
    ..aOM<ReadTarget>(1, _omitFieldNames ? '' : 'target',
        subBuilder: ReadTarget.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'versionId')
    ..aOS(4, _omitFieldNames ? '' : 'ifMatch')
    ..aOS(5, _omitFieldNames ? '' : 'ifNoneMatch')
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
  ReadTarget get target => $_getN(0);
  @$pb.TagNumber(1)
  set target(ReadTarget value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasTarget() => $_has(0);
  @$pb.TagNumber(1)
  void clearTarget() => $_clearField(1);
  @$pb.TagNumber(1)
  ReadTarget ensureTarget() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get objectKey => $_getSZ(1);
  @$pb.TagNumber(2)
  set objectKey($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectKey() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get versionId => $_getSZ(2);
  @$pb.TagNumber(3)
  set versionId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasVersionId() => $_has(2);
  @$pb.TagNumber(3)
  void clearVersionId() => $_clearField(3);

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

class HeadObjectResponse extends $pb.GeneratedMessage {
  factory HeadObjectResponse({
    ObjectVersion? version,
  }) {
    final result = HeadObjectResponse._();
    if (version != null) result.version = version;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: HeadObjectResponse.$_createMessage)
    ..aOM<ObjectVersion>(1, _omitFieldNames ? '' : 'version',
        subBuilder: ObjectVersion.$_createMessage)
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
  ObjectVersion get version => $_getN(0);
  @$pb.TagNumber(1)
  set version(ObjectVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasVersion() => $_has(0);
  @$pb.TagNumber(1)
  void clearVersion() => $_clearField(1);
  @$pb.TagNumber(1)
  ObjectVersion ensureVersion() => $_ensure(0);
}

class DeleteObjectRequest extends $pb.GeneratedMessage {
  factory DeleteObjectRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    $core.String? versionId,
    Preconditions? preconditions,
    MutationIdentity? mutation,
  }) {
    final result = DeleteObjectRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (versionId != null) result.versionId = versionId;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: DeleteObjectRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'versionId')
    ..aOM<Preconditions>(4, _omitFieldNames ? '' : 'preconditions',
        subBuilder: Preconditions.$_createMessage)
    ..aOM<MutationIdentity>(5, _omitFieldNames ? '' : 'mutation',
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
  $core.String get versionId => $_getSZ(2);
  @$pb.TagNumber(3)
  set versionId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasVersionId() => $_has(2);
  @$pb.TagNumber(3)
  void clearVersionId() => $_clearField(3);

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

class DeleteObjectResponse extends $pb.GeneratedMessage {
  factory DeleteObjectResponse({
    $core.bool? existed,
    ObjectVersion? version,
  }) {
    final result = DeleteObjectResponse._();
    if (existed != null) result.existed = existed;
    if (version != null) result.version = version;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: DeleteObjectResponse.$_createMessage)
    ..aOB(1, _omitFieldNames ? '' : 'existed')
    ..aOM<ObjectVersion>(2, _omitFieldNames ? '' : 'version',
        subBuilder: ObjectVersion.$_createMessage)
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

  @$pb.TagNumber(2)
  ObjectVersion get version => $_getN(1);
  @$pb.TagNumber(2)
  set version(ObjectVersion value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);
  @$pb.TagNumber(2)
  ObjectVersion ensureVersion() => $_ensure(1);
}

class ListObjectsRequest extends $pb.GeneratedMessage {
  factory ListObjectsRequest({
    ReadTarget? target,
    $core.String? prefix,
    $core.String? delimiter,
    ListingMode? mode,
    $core.int? pageSize,
    $core.String? continuationToken,
  }) {
    final result = ListObjectsRequest._();
    if (target != null) result.target = target;
    if (prefix != null) result.prefix = prefix;
    if (delimiter != null) result.delimiter = delimiter;
    if (mode != null) result.mode = mode;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ListObjectsRequest.$_createMessage)
    ..aOM<ReadTarget>(1, _omitFieldNames ? '' : 'target',
        subBuilder: ReadTarget.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'prefix')
    ..aOS(3, _omitFieldNames ? '' : 'delimiter')
    ..aE<ListingMode>(4, _omitFieldNames ? '' : 'mode',
        enumValues: ListingMode.values)
    ..aI(5, _omitFieldNames ? '' : 'pageSize', fieldType: $pb.PbFieldType.OU3)
    ..aOS(6, _omitFieldNames ? '' : 'continuationToken')
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
  ReadTarget get target => $_getN(0);
  @$pb.TagNumber(1)
  set target(ReadTarget value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasTarget() => $_has(0);
  @$pb.TagNumber(1)
  void clearTarget() => $_clearField(1);
  @$pb.TagNumber(1)
  ReadTarget ensureTarget() => $_ensure(0);

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
  ListingMode get mode => $_getN(3);
  @$pb.TagNumber(4)
  set mode(ListingMode value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasMode() => $_has(3);
  @$pb.TagNumber(4)
  void clearMode() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.int get pageSize => $_getIZ(4);
  @$pb.TagNumber(5)
  set pageSize($core.int value) => $_setUnsignedInt32(4, value);
  @$pb.TagNumber(5)
  $core.bool hasPageSize() => $_has(4);
  @$pb.TagNumber(5)
  void clearPageSize() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get continuationToken => $_getSZ(5);
  @$pb.TagNumber(6)
  set continuationToken($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasContinuationToken() => $_has(5);
  @$pb.TagNumber(6)
  void clearContinuationToken() => $_clearField(6);
}

class ListEntry extends $pb.GeneratedMessage {
  factory ListEntry({
    $core.String? objectKey,
    ObjectVersion? version,
  }) {
    final result = ListEntry._();
    if (objectKey != null) result.objectKey = objectKey;
    if (version != null) result.version = version;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ListEntry.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'objectKey')
    ..aOM<ObjectVersion>(2, _omitFieldNames ? '' : 'version',
        subBuilder: ObjectVersion.$_createMessage)
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
  ObjectVersion get version => $_getN(1);
  @$pb.TagNumber(2)
  set version(ObjectVersion value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearVersion() => $_clearField(2);
  @$pb.TagNumber(2)
  ObjectVersion ensureVersion() => $_ensure(1);
}

class ListObjectsResponse extends $pb.GeneratedMessage {
  factory ListObjectsResponse({
    $core.Iterable<ListEntry>? entries,
    $core.Iterable<$core.String>? commonPrefixes,
    $core.String? continuationToken,
  }) {
    final result = ListObjectsResponse._();
    if (entries != null) result.entries.addAll(entries);
    if (commonPrefixes != null) result.commonPrefixes.addAll(commonPrefixes);
    if (continuationToken != null) result.continuationToken = continuationToken;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ListObjectsResponse.$_createMessage)
    ..pPM<ListEntry>(1, _omitFieldNames ? '' : 'entries',
        subBuilder: ListEntry.$_createMessage)
    ..pPS(2, _omitFieldNames ? '' : 'commonPrefixes')
    ..aOS(3, _omitFieldNames ? '' : 'continuationToken')
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
}

class CreateMultipartRequest extends $pb.GeneratedMessage {
  factory CreateMultipartRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    ObjectMetadata? metadata,
    Preconditions? preconditions,
    MutationIdentity? mutation,
  }) {
    final result = CreateMultipartRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (metadata != null) result.metadata = metadata;
    if (preconditions != null) result.preconditions = preconditions;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: CreateMultipartRequest.$_createMessage)
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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

enum UploadPartRequest_Frame { header, body, notSet }

class UploadPartRequest extends $pb.GeneratedMessage {
  factory UploadPartRequest({
    UploadPartHeader? header,
    $core.List<$core.int>? body,
  }) {
    final result = UploadPartRequest._();
    if (header != null) result.header = header;
    if (body != null) result.body = body;
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
    0: UploadPartRequest_Frame.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UploadPartRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: UploadPartRequest.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<UploadPartHeader>(1, _omitFieldNames ? '' : 'header',
        subBuilder: UploadPartHeader.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'body', $pb.PbFieldType.OY)
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
  UploadPartRequest_Frame whichFrame() =>
      _UploadPartRequest_FrameByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
  }) {
    final result = ListPartsRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (uploadId != null) result.uploadId = uploadId;
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ListPartsRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'uploadId')
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
}

class ListPartsResponse extends $pb.GeneratedMessage {
  factory ListPartsResponse({
    $core.Iterable<UploadedPart>? parts,
  }) {
    final result = ListPartsResponse._();
    if (parts != null) result.parts.addAll(parts);
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ListPartsResponse.$_createMessage)
    ..pPM<UploadedPart>(1, _omitFieldNames ? '' : 'parts',
        subBuilder: UploadedPart.$_createMessage)
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
}

class CompleteMultipartRequest extends $pb.GeneratedMessage {
  factory CompleteMultipartRequest({
    BucketRef? bucket,
    $core.String? objectKey,
    $core.String? uploadId,
    $core.Iterable<UploadedPart>? parts,
    MutationIdentity? mutation,
  }) {
    final result = CompleteMultipartRequest._();
    if (bucket != null) result.bucket = bucket;
    if (objectKey != null) result.objectKey = objectKey;
    if (uploadId != null) result.uploadId = uploadId;
    if (parts != null) result.parts.addAll(parts);
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: CompleteMultipartRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'objectKey')
    ..aOS(3, _omitFieldNames ? '' : 'uploadId')
    ..pPM<UploadedPart>(4, _omitFieldNames ? '' : 'parts',
        subBuilder: UploadedPart.$_createMessage)
    ..aOM<MutationIdentity>(5, _omitFieldNames ? '' : 'mutation',
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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

class Snapshot extends $pb.GeneratedMessage {
  factory Snapshot({
    SnapshotRef? snapshot,
    $1.Timestamp? createdAt,
  }) {
    final result = Snapshot._();
    if (snapshot != null) result.snapshot = snapshot;
    if (createdAt != null) result.createdAt = createdAt;
    return result;
  }

  Snapshot._();

  factory Snapshot.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Snapshot()..mergeFromBuffer(data, registry);
  factory Snapshot.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Snapshot()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Snapshot',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: Snapshot.$_createMessage)
    ..aOM<SnapshotRef>(1, _omitFieldNames ? '' : 'snapshot',
        subBuilder: SnapshotRef.$_createMessage)
    ..aOM<$1.Timestamp>(2, _omitFieldNames ? '' : 'createdAt',
        subBuilder: $1.Timestamp.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Snapshot clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Snapshot copyWith(void Function(Snapshot) updates) =>
      super.copyWith((message) => updates(message as Snapshot)) as Snapshot;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Snapshot() / Snapshot.new instead')
  static Snapshot create() => Snapshot._();
  static $pb.GeneratedMessage $_createMessage() => Snapshot._();
  @$core.override
  Snapshot createEmptyInstance() => Snapshot._();
  @$core.pragma('dart2js:noInline')
  static Snapshot getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Snapshot>(Snapshot.$_createMessage);
  static Snapshot? _defaultInstance;

  @$pb.TagNumber(1)
  SnapshotRef get snapshot => $_getN(0);
  @$pb.TagNumber(1)
  set snapshot(SnapshotRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSnapshot() => $_has(0);
  @$pb.TagNumber(1)
  void clearSnapshot() => $_clearField(1);
  @$pb.TagNumber(1)
  SnapshotRef ensureSnapshot() => $_ensure(0);

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

class CreateSnapshotRequest extends $pb.GeneratedMessage {
  factory CreateSnapshotRequest({
    BucketRef? bucket,
    MutationIdentity? mutation,
  }) {
    final result = CreateSnapshotRequest._();
    if (bucket != null) result.bucket = bucket;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  CreateSnapshotRequest._();

  factory CreateSnapshotRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateSnapshotRequest()..mergeFromBuffer(data, registry);
  factory CreateSnapshotRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateSnapshotRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateSnapshotRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: CreateSnapshotRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'bucket',
        subBuilder: BucketRef.$_createMessage)
    ..aOM<MutationIdentity>(2, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateSnapshotRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateSnapshotRequest copyWith(
          void Function(CreateSnapshotRequest) updates) =>
      super.copyWith((message) => updates(message as CreateSnapshotRequest))
          as CreateSnapshotRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CreateSnapshotRequest() / CreateSnapshotRequest.new instead')
  static CreateSnapshotRequest create() => CreateSnapshotRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateSnapshotRequest._();
  @$core.override
  CreateSnapshotRequest createEmptyInstance() => CreateSnapshotRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateSnapshotRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateSnapshotRequest>(
          CreateSnapshotRequest.$_createMessage);
  static CreateSnapshotRequest? _defaultInstance;

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

class DestroySnapshotRequest extends $pb.GeneratedMessage {
  factory DestroySnapshotRequest({
    SnapshotRef? snapshot,
    MutationIdentity? mutation,
  }) {
    final result = DestroySnapshotRequest._();
    if (snapshot != null) result.snapshot = snapshot;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  DestroySnapshotRequest._();

  factory DestroySnapshotRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DestroySnapshotRequest()..mergeFromBuffer(data, registry);
  factory DestroySnapshotRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DestroySnapshotRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DestroySnapshotRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: DestroySnapshotRequest.$_createMessage)
    ..aOM<SnapshotRef>(1, _omitFieldNames ? '' : 'snapshot',
        subBuilder: SnapshotRef.$_createMessage)
    ..aOM<MutationIdentity>(2, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DestroySnapshotRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DestroySnapshotRequest copyWith(
          void Function(DestroySnapshotRequest) updates) =>
      super.copyWith((message) => updates(message as DestroySnapshotRequest))
          as DestroySnapshotRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use DestroySnapshotRequest() / DestroySnapshotRequest.new instead')
  static DestroySnapshotRequest create() => DestroySnapshotRequest._();
  static $pb.GeneratedMessage $_createMessage() => DestroySnapshotRequest._();
  @$core.override
  DestroySnapshotRequest createEmptyInstance() => DestroySnapshotRequest._();
  @$core.pragma('dart2js:noInline')
  static DestroySnapshotRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DestroySnapshotRequest>(
          DestroySnapshotRequest.$_createMessage);
  static DestroySnapshotRequest? _defaultInstance;

  @$pb.TagNumber(1)
  SnapshotRef get snapshot => $_getN(0);
  @$pb.TagNumber(1)
  set snapshot(SnapshotRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSnapshot() => $_has(0);
  @$pb.TagNumber(1)
  void clearSnapshot() => $_clearField(1);
  @$pb.TagNumber(1)
  SnapshotRef ensureSnapshot() => $_ensure(0);

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

class DestroySnapshotResponse extends $pb.GeneratedMessage {
  factory DestroySnapshotResponse({
    $core.bool? existed,
  }) {
    final result = DestroySnapshotResponse._();
    if (existed != null) result.existed = existed;
    return result;
  }

  DestroySnapshotResponse._();

  factory DestroySnapshotResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DestroySnapshotResponse()..mergeFromBuffer(data, registry);
  factory DestroySnapshotResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DestroySnapshotResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DestroySnapshotResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: DestroySnapshotResponse.$_createMessage)
    ..aOB(1, _omitFieldNames ? '' : 'existed')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DestroySnapshotResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DestroySnapshotResponse copyWith(
          void Function(DestroySnapshotResponse) updates) =>
      super.copyWith((message) => updates(message as DestroySnapshotResponse))
          as DestroySnapshotResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use DestroySnapshotResponse() / DestroySnapshotResponse.new instead')
  static DestroySnapshotResponse create() => DestroySnapshotResponse._();
  static $pb.GeneratedMessage $_createMessage() => DestroySnapshotResponse._();
  @$core.override
  DestroySnapshotResponse createEmptyInstance() => DestroySnapshotResponse._();
  @$core.pragma('dart2js:noInline')
  static DestroySnapshotResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DestroySnapshotResponse>(
          DestroySnapshotResponse.$_createMessage);
  static DestroySnapshotResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.bool get existed => $_getBF(0);
  @$pb.TagNumber(1)
  set existed($core.bool value) => $_setBool(0, value);
  @$pb.TagNumber(1)
  $core.bool hasExisted() => $_has(0);
  @$pb.TagNumber(1)
  void clearExisted() => $_clearField(1);
}

class ForkSnapshotRequest extends $pb.GeneratedMessage {
  factory ForkSnapshotRequest({
    SnapshotRef? snapshot,
    $core.String? destinationName,
    MutationIdentity? mutation,
  }) {
    final result = ForkSnapshotRequest._();
    if (snapshot != null) result.snapshot = snapshot;
    if (destinationName != null) result.destinationName = destinationName;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  ForkSnapshotRequest._();

  factory ForkSnapshotRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkSnapshotRequest()..mergeFromBuffer(data, registry);
  factory ForkSnapshotRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkSnapshotRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkSnapshotRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ForkSnapshotRequest.$_createMessage)
    ..aOM<SnapshotRef>(1, _omitFieldNames ? '' : 'snapshot',
        subBuilder: SnapshotRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'destinationName')
    ..aOM<MutationIdentity>(3, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkSnapshotRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkSnapshotRequest copyWith(void Function(ForkSnapshotRequest) updates) =>
      super.copyWith((message) => updates(message as ForkSnapshotRequest))
          as ForkSnapshotRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use ForkSnapshotRequest() / ForkSnapshotRequest.new instead')
  static ForkSnapshotRequest create() => ForkSnapshotRequest._();
  static $pb.GeneratedMessage $_createMessage() => ForkSnapshotRequest._();
  @$core.override
  ForkSnapshotRequest createEmptyInstance() => ForkSnapshotRequest._();
  @$core.pragma('dart2js:noInline')
  static ForkSnapshotRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ForkSnapshotRequest>(
          ForkSnapshotRequest.$_createMessage);
  static ForkSnapshotRequest? _defaultInstance;

  @$pb.TagNumber(1)
  SnapshotRef get snapshot => $_getN(0);
  @$pb.TagNumber(1)
  set snapshot(SnapshotRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSnapshot() => $_has(0);
  @$pb.TagNumber(1)
  void clearSnapshot() => $_clearField(1);
  @$pb.TagNumber(1)
  SnapshotRef ensureSnapshot() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get destinationName => $_getSZ(1);
  @$pb.TagNumber(2)
  set destinationName($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDestinationName() => $_has(1);
  @$pb.TagNumber(2)
  void clearDestinationName() => $_clearField(2);

  @$pb.TagNumber(3)
  MutationIdentity get mutation => $_getN(2);
  @$pb.TagNumber(3)
  set mutation(MutationIdentity value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMutation() => $_has(2);
  @$pb.TagNumber(3)
  void clearMutation() => $_clearField(3);
  @$pb.TagNumber(3)
  MutationIdentity ensureMutation() => $_ensure(2);
}

class ForkBucketRequest extends $pb.GeneratedMessage {
  factory ForkBucketRequest({
    BucketRef? source,
    $core.String? destinationName,
    MutationIdentity? mutation,
  }) {
    final result = ForkBucketRequest._();
    if (source != null) result.source = source;
    if (destinationName != null) result.destinationName = destinationName;
    if (mutation != null) result.mutation = mutation;
    return result;
  }

  ForkBucketRequest._();

  factory ForkBucketRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkBucketRequest()..mergeFromBuffer(data, registry);
  factory ForkBucketRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkBucketRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkBucketRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
      createEmptyInstance: ForkBucketRequest.$_createMessage)
    ..aOM<BucketRef>(1, _omitFieldNames ? '' : 'source',
        subBuilder: BucketRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'destinationName')
    ..aOM<MutationIdentity>(3, _omitFieldNames ? '' : 'mutation',
        subBuilder: MutationIdentity.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkBucketRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkBucketRequest copyWith(void Function(ForkBucketRequest) updates) =>
      super.copyWith((message) => updates(message as ForkBucketRequest))
          as ForkBucketRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkBucketRequest() / ForkBucketRequest.new instead')
  static ForkBucketRequest create() => ForkBucketRequest._();
  static $pb.GeneratedMessage $_createMessage() => ForkBucketRequest._();
  @$core.override
  ForkBucketRequest createEmptyInstance() => ForkBucketRequest._();
  @$core.pragma('dart2js:noInline')
  static ForkBucketRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkBucketRequest>(
          ForkBucketRequest.$_createMessage);
  static ForkBucketRequest? _defaultInstance;

  @$pb.TagNumber(1)
  BucketRef get source => $_getN(0);
  @$pb.TagNumber(1)
  set source(BucketRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);
  @$pb.TagNumber(1)
  BucketRef ensureSource() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get destinationName => $_getSZ(1);
  @$pb.TagNumber(2)
  set destinationName($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDestinationName() => $_has(1);
  @$pb.TagNumber(2)
  void clearDestinationName() => $_clearField(2);

  @$pb.TagNumber(3)
  MutationIdentity get mutation => $_getN(2);
  @$pb.TagNumber(3)
  set mutation(MutationIdentity value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMutation() => $_has(2);
  @$pb.TagNumber(3)
  void clearMutation() => $_clearField(3);
  @$pb.TagNumber(3)
  MutationIdentity ensureMutation() => $_ensure(2);
}

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
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.objects.v1'),
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
