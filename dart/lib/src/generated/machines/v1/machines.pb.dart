// This is a generated file - do not edit.
//
// Generated from machines/v1/machines.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'machines.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'machines.pbenum.dart';

class ProtocolVersion extends $pb.GeneratedMessage {
  factory ProtocolVersion({
    $core.int? major,
    $core.int? minor,
  }) {
    final result = ProtocolVersion._();
    if (major != null) result.major = major;
    if (minor != null) result.minor = minor;
    return result;
  }

  ProtocolVersion._();

  factory ProtocolVersion.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProtocolVersion()..mergeFromBuffer(data, registry);
  factory ProtocolVersion.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProtocolVersion()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProtocolVersion',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ProtocolVersion.$_createMessage)
    ..aI(1, _omitFieldNames ? '' : 'major', fieldType: $pb.PbFieldType.OU3)
    ..aI(2, _omitFieldNames ? '' : 'minor', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtocolVersion clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtocolVersion copyWith(void Function(ProtocolVersion) updates) =>
      super.copyWith((message) => updates(message as ProtocolVersion))
          as ProtocolVersion;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ProtocolVersion() / ProtocolVersion.new instead')
  static ProtocolVersion create() => ProtocolVersion._();
  static $pb.GeneratedMessage $_createMessage() => ProtocolVersion._();
  @$core.override
  ProtocolVersion createEmptyInstance() => ProtocolVersion._();
  @$core.pragma('dart2js:noInline')
  static ProtocolVersion getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ProtocolVersion>(
          ProtocolVersion.$_createMessage);
  static ProtocolVersion? _defaultInstance;

  @$pb.TagNumber(1)
  $core.int get major => $_getIZ(0);
  @$pb.TagNumber(1)
  set major($core.int value) => $_setUnsignedInt32(0, value);
  @$pb.TagNumber(1)
  $core.bool hasMajor() => $_has(0);
  @$pb.TagNumber(1)
  void clearMajor() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.int get minor => $_getIZ(1);
  @$pb.TagNumber(2)
  set minor($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(2)
  $core.bool hasMinor() => $_has(1);
  @$pb.TagNumber(2)
  void clearMinor() => $_clearField(2);
}

class OperationId extends $pb.GeneratedMessage {
  factory OperationId({
    $core.List<$core.int>? value,
  }) {
    final result = OperationId._();
    if (value != null) result.value = value;
    return result;
  }

  OperationId._();

  factory OperationId.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationId()..mergeFromBuffer(data, registry);
  factory OperationId.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationId()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OperationId',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: OperationId.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'value', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationId clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationId copyWith(void Function(OperationId) updates) =>
      super.copyWith((message) => updates(message as OperationId))
          as OperationId;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OperationId() / OperationId.new instead')
  static OperationId create() => OperationId._();
  static $pb.GeneratedMessage $_createMessage() => OperationId._();
  @$core.override
  OperationId createEmptyInstance() => OperationId._();
  @$core.pragma('dart2js:noInline')
  static OperationId getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OperationId>(
          OperationId.$_createMessage);
  static OperationId? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get value => $_getN(0);
  @$pb.TagNumber(1)
  set value($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasValue() => $_has(0);
  @$pb.TagNumber(1)
  void clearValue() => $_clearField(1);
}

class IdempotencyKey extends $pb.GeneratedMessage {
  factory IdempotencyKey({
    $core.List<$core.int>? value,
  }) {
    final result = IdempotencyKey._();
    if (value != null) result.value = value;
    return result;
  }

  IdempotencyKey._();

  factory IdempotencyKey.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      IdempotencyKey()..mergeFromBuffer(data, registry);
  factory IdempotencyKey.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      IdempotencyKey()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'IdempotencyKey',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: IdempotencyKey.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'value', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IdempotencyKey clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  IdempotencyKey copyWith(void Function(IdempotencyKey) updates) =>
      super.copyWith((message) => updates(message as IdempotencyKey))
          as IdempotencyKey;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use IdempotencyKey() / IdempotencyKey.new instead')
  static IdempotencyKey create() => IdempotencyKey._();
  static $pb.GeneratedMessage $_createMessage() => IdempotencyKey._();
  @$core.override
  IdempotencyKey createEmptyInstance() => IdempotencyKey._();
  @$core.pragma('dart2js:noInline')
  static IdempotencyKey getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<IdempotencyKey>(
          IdempotencyKey.$_createMessage);
  static IdempotencyKey? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get value => $_getN(0);
  @$pb.TagNumber(1)
  set value($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasValue() => $_has(0);
  @$pb.TagNumber(1)
  void clearValue() => $_clearField(1);
}

class MachineId extends $pb.GeneratedMessage {
  factory MachineId({
    $core.List<$core.int>? value,
  }) {
    final result = MachineId._();
    if (value != null) result.value = value;
    return result;
  }

  MachineId._();

  factory MachineId.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineId()..mergeFromBuffer(data, registry);
  factory MachineId.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineId()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachineId',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MachineId.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'value', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineId clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineId copyWith(void Function(MachineId) updates) =>
      super.copyWith((message) => updates(message as MachineId)) as MachineId;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MachineId() / MachineId.new instead')
  static MachineId create() => MachineId._();
  static $pb.GeneratedMessage $_createMessage() => MachineId._();
  @$core.override
  MachineId createEmptyInstance() => MachineId._();
  @$core.pragma('dart2js:noInline')
  static MachineId getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<MachineId>(MachineId.$_createMessage);
  static MachineId? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get value => $_getN(0);
  @$pb.TagNumber(1)
  set value($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasValue() => $_has(0);
  @$pb.TagNumber(1)
  void clearValue() => $_clearField(1);
}

class CheckpointId extends $pb.GeneratedMessage {
  factory CheckpointId({
    $core.List<$core.int>? value,
  }) {
    final result = CheckpointId._();
    if (value != null) result.value = value;
    return result;
  }

  CheckpointId._();

  factory CheckpointId.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointId()..mergeFromBuffer(data, registry);
  factory CheckpointId.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointId()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CheckpointId',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: CheckpointId.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'value', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointId clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointId copyWith(void Function(CheckpointId) updates) =>
      super.copyWith((message) => updates(message as CheckpointId))
          as CheckpointId;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CheckpointId() / CheckpointId.new instead')
  static CheckpointId create() => CheckpointId._();
  static $pb.GeneratedMessage $_createMessage() => CheckpointId._();
  @$core.override
  CheckpointId createEmptyInstance() => CheckpointId._();
  @$core.pragma('dart2js:noInline')
  static CheckpointId getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CheckpointId>(
          CheckpointId.$_createMessage);
  static CheckpointId? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get value => $_getN(0);
  @$pb.TagNumber(1)
  set value($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasValue() => $_has(0);
  @$pb.TagNumber(1)
  void clearValue() => $_clearField(1);
}

enum Image_ImmutableReference {
  managedDigest,
  customDigest,
  checkpoint,
  notSet
}

class Image extends $pb.GeneratedMessage {
  factory Image({
    ImageKind? kind,
    $core.List<$core.int>? managedDigest,
    $core.List<$core.int>? customDigest,
    CheckpointId? checkpoint,
  }) {
    final result = Image._();
    if (kind != null) result.kind = kind;
    if (managedDigest != null) result.managedDigest = managedDigest;
    if (customDigest != null) result.customDigest = customDigest;
    if (checkpoint != null) result.checkpoint = checkpoint;
    return result;
  }

  Image._();

  factory Image.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Image()..mergeFromBuffer(data, registry);
  factory Image.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Image()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, Image_ImmutableReference>
      _Image_ImmutableReferenceByTag = {
    2: Image_ImmutableReference.managedDigest,
    3: Image_ImmutableReference.customDigest,
    4: Image_ImmutableReference.checkpoint,
    0: Image_ImmutableReference.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Image',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: Image.$_createMessage)
    ..oo(0, [2, 3, 4])
    ..aE<ImageKind>(1, _omitFieldNames ? '' : 'kind',
        enumValues: ImageKind.values)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'managedDigest', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'customDigest', $pb.PbFieldType.OY)
    ..aOM<CheckpointId>(4, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Image clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Image copyWith(void Function(Image) updates) =>
      super.copyWith((message) => updates(message as Image)) as Image;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Image() / Image.new instead')
  static Image create() => Image._();
  static $pb.GeneratedMessage $_createMessage() => Image._();
  @$core.override
  Image createEmptyInstance() => Image._();
  @$core.pragma('dart2js:noInline')
  static Image getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Image>(Image.$_createMessage);
  static Image? _defaultInstance;

  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  Image_ImmutableReference whichImmutableReference() =>
      _Image_ImmutableReferenceByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  void clearImmutableReference() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  ImageKind get kind => $_getN(0);
  @$pb.TagNumber(1)
  set kind(ImageKind value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasKind() => $_has(0);
  @$pb.TagNumber(1)
  void clearKind() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get managedDigest => $_getN(1);
  @$pb.TagNumber(2)
  set managedDigest($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasManagedDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearManagedDigest() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get customDigest => $_getN(2);
  @$pb.TagNumber(3)
  set customDigest($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasCustomDigest() => $_has(2);
  @$pb.TagNumber(3)
  void clearCustomDigest() => $_clearField(3);

  @$pb.TagNumber(4)
  CheckpointId get checkpoint => $_getN(3);
  @$pb.TagNumber(4)
  set checkpoint(CheckpointId value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasCheckpoint() => $_has(3);
  @$pb.TagNumber(4)
  void clearCheckpoint() => $_clearField(4);
  @$pb.TagNumber(4)
  CheckpointId ensureCheckpoint() => $_ensure(3);
}

class CompatibilityPolicy extends $pb.GeneratedMessage {
  factory CompatibilityPolicy({
    CompatibilityMode? mode,
    $core.Iterable<Capability>? required,
  }) {
    final result = CompatibilityPolicy._();
    if (mode != null) result.mode = mode;
    if (required != null) result.required.addAll(required);
    return result;
  }

  CompatibilityPolicy._();

  factory CompatibilityPolicy.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CompatibilityPolicy()..mergeFromBuffer(data, registry);
  factory CompatibilityPolicy.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CompatibilityPolicy()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CompatibilityPolicy',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: CompatibilityPolicy.$_createMessage)
    ..aE<CompatibilityMode>(1, _omitFieldNames ? '' : 'mode',
        enumValues: CompatibilityMode.values)
    ..pc<Capability>(2, _omitFieldNames ? '' : 'required', $pb.PbFieldType.KE,
        valueOf: Capability.valueOf,
        enumValues: Capability.values,
        defaultEnumValue: Capability.CAPABILITY_UNSPECIFIED)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CompatibilityPolicy clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CompatibilityPolicy copyWith(void Function(CompatibilityPolicy) updates) =>
      super.copyWith((message) => updates(message as CompatibilityPolicy))
          as CompatibilityPolicy;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use CompatibilityPolicy() / CompatibilityPolicy.new instead')
  static CompatibilityPolicy create() => CompatibilityPolicy._();
  static $pb.GeneratedMessage $_createMessage() => CompatibilityPolicy._();
  @$core.override
  CompatibilityPolicy createEmptyInstance() => CompatibilityPolicy._();
  @$core.pragma('dart2js:noInline')
  static CompatibilityPolicy getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CompatibilityPolicy>(
          CompatibilityPolicy.$_createMessage);
  static CompatibilityPolicy? _defaultInstance;

  @$pb.TagNumber(1)
  CompatibilityMode get mode => $_getN(0);
  @$pb.TagNumber(1)
  set mode(CompatibilityMode value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasMode() => $_has(0);
  @$pb.TagNumber(1)
  void clearMode() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<Capability> get required => $_getList(1);
}

class ImageQualification extends $pb.GeneratedMessage {
  factory ImageQualification({
    Image? image,
    $core.Iterable<Capability>? capabilities,
    $core.List<$core.int>? compatibilityRevision,
  }) {
    final result = ImageQualification._();
    if (image != null) result.image = image;
    if (capabilities != null) result.capabilities.addAll(capabilities);
    if (compatibilityRevision != null)
      result.compatibilityRevision = compatibilityRevision;
    return result;
  }

  ImageQualification._();

  factory ImageQualification.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ImageQualification()..mergeFromBuffer(data, registry);
  factory ImageQualification.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ImageQualification()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ImageQualification',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ImageQualification.$_createMessage)
    ..aOM<Image>(1, _omitFieldNames ? '' : 'image',
        subBuilder: Image.$_createMessage)
    ..pc<Capability>(
        2, _omitFieldNames ? '' : 'capabilities', $pb.PbFieldType.KE,
        valueOf: Capability.valueOf,
        enumValues: Capability.values,
        defaultEnumValue: Capability.CAPABILITY_UNSPECIFIED)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'compatibilityRevision', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ImageQualification clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ImageQualification copyWith(void Function(ImageQualification) updates) =>
      super.copyWith((message) => updates(message as ImageQualification))
          as ImageQualification;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ImageQualification() / ImageQualification.new instead')
  static ImageQualification create() => ImageQualification._();
  static $pb.GeneratedMessage $_createMessage() => ImageQualification._();
  @$core.override
  ImageQualification createEmptyInstance() => ImageQualification._();
  @$core.pragma('dart2js:noInline')
  static ImageQualification getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ImageQualification>(
          ImageQualification.$_createMessage);
  static ImageQualification? _defaultInstance;

  @$pb.TagNumber(1)
  Image get image => $_getN(0);
  @$pb.TagNumber(1)
  set image(Image value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasImage() => $_has(0);
  @$pb.TagNumber(1)
  void clearImage() => $_clearField(1);
  @$pb.TagNumber(1)
  Image ensureImage() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<Capability> get capabilities => $_getList(1);

  @$pb.TagNumber(3)
  $core.List<$core.int> get compatibilityRevision => $_getN(2);
  @$pb.TagNumber(3)
  set compatibilityRevision($core.List<$core.int> value) =>
      $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasCompatibilityRevision() => $_has(2);
  @$pb.TagNumber(3)
  void clearCompatibilityRevision() => $_clearField(3);
}

enum SuspensionPolicy_Policy { manual, afterIdleMs, notSet }

class SuspensionPolicy extends $pb.GeneratedMessage {
  factory SuspensionPolicy({
    $core.bool? manual,
    $fixnum.Int64? afterIdleMs,
  }) {
    final result = SuspensionPolicy._();
    if (manual != null) result.manual = manual;
    if (afterIdleMs != null) result.afterIdleMs = afterIdleMs;
    return result;
  }

  SuspensionPolicy._();

  factory SuspensionPolicy.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SuspensionPolicy()..mergeFromBuffer(data, registry);
  factory SuspensionPolicy.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SuspensionPolicy()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, SuspensionPolicy_Policy>
      _SuspensionPolicy_PolicyByTag = {
    1: SuspensionPolicy_Policy.manual,
    2: SuspensionPolicy_Policy.afterIdleMs,
    0: SuspensionPolicy_Policy.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SuspensionPolicy',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: SuspensionPolicy.$_createMessage)
    ..oo(0, [1, 2])
    ..aOB(1, _omitFieldNames ? '' : 'manual')
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'afterIdleMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SuspensionPolicy clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SuspensionPolicy copyWith(void Function(SuspensionPolicy) updates) =>
      super.copyWith((message) => updates(message as SuspensionPolicy))
          as SuspensionPolicy;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SuspensionPolicy() / SuspensionPolicy.new instead')
  static SuspensionPolicy create() => SuspensionPolicy._();
  static $pb.GeneratedMessage $_createMessage() => SuspensionPolicy._();
  @$core.override
  SuspensionPolicy createEmptyInstance() => SuspensionPolicy._();
  @$core.pragma('dart2js:noInline')
  static SuspensionPolicy getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SuspensionPolicy>(
          SuspensionPolicy.$_createMessage);
  static SuspensionPolicy? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  SuspensionPolicy_Policy whichPolicy() =>
      _SuspensionPolicy_PolicyByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearPolicy() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.bool get manual => $_getBF(0);
  @$pb.TagNumber(1)
  set manual($core.bool value) => $_setBool(0, value);
  @$pb.TagNumber(1)
  $core.bool hasManual() => $_has(0);
  @$pb.TagNumber(1)
  void clearManual() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get afterIdleMs => $_getI64(1);
  @$pb.TagNumber(2)
  set afterIdleMs($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAfterIdleMs() => $_has(1);
  @$pb.TagNumber(2)
  void clearAfterIdleMs() => $_clearField(2);
}

class ExpirationPolicy extends $pb.GeneratedMessage {
  factory ExpirationPolicy({
    ExpirationKind? kind,
    $fixnum.Int64? valueMs,
  }) {
    final result = ExpirationPolicy._();
    if (kind != null) result.kind = kind;
    if (valueMs != null) result.valueMs = valueMs;
    return result;
  }

  ExpirationPolicy._();

  factory ExpirationPolicy.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExpirationPolicy()..mergeFromBuffer(data, registry);
  factory ExpirationPolicy.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExpirationPolicy()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExpirationPolicy',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ExpirationPolicy.$_createMessage)
    ..aE<ExpirationKind>(1, _omitFieldNames ? '' : 'kind',
        enumValues: ExpirationKind.values)
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'valueMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExpirationPolicy clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExpirationPolicy copyWith(void Function(ExpirationPolicy) updates) =>
      super.copyWith((message) => updates(message as ExpirationPolicy))
          as ExpirationPolicy;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExpirationPolicy() / ExpirationPolicy.new instead')
  static ExpirationPolicy create() => ExpirationPolicy._();
  static $pb.GeneratedMessage $_createMessage() => ExpirationPolicy._();
  @$core.override
  ExpirationPolicy createEmptyInstance() => ExpirationPolicy._();
  @$core.pragma('dart2js:noInline')
  static ExpirationPolicy getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ExpirationPolicy>(
          ExpirationPolicy.$_createMessage);
  static ExpirationPolicy? _defaultInstance;

  @$pb.TagNumber(1)
  ExpirationKind get kind => $_getN(0);
  @$pb.TagNumber(1)
  set kind(ExpirationKind value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasKind() => $_has(0);
  @$pb.TagNumber(1)
  void clearKind() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get valueMs => $_getI64(1);
  @$pb.TagNumber(2)
  set valueMs($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasValueMs() => $_has(1);
  @$pb.TagNumber(2)
  void clearValueMs() => $_clearField(2);
}

class Budgets extends $pb.GeneratedMessage {
  factory Budgets({
    $fixnum.Int64? spendMicros,
    $core.int? concurrency,
  }) {
    final result = Budgets._();
    if (spendMicros != null) result.spendMicros = spendMicros;
    if (concurrency != null) result.concurrency = concurrency;
    return result;
  }

  Budgets._();

  factory Budgets.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Budgets()..mergeFromBuffer(data, registry);
  factory Budgets.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Budgets()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Budgets',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: Budgets.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'spendMicros', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aI(2, _omitFieldNames ? '' : 'concurrency',
        fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Budgets clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Budgets copyWith(void Function(Budgets) updates) =>
      super.copyWith((message) => updates(message as Budgets)) as Budgets;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Budgets() / Budgets.new instead')
  static Budgets create() => Budgets._();
  static $pb.GeneratedMessage $_createMessage() => Budgets._();
  @$core.override
  Budgets createEmptyInstance() => Budgets._();
  @$core.pragma('dart2js:noInline')
  static Budgets getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Budgets>(Budgets.$_createMessage);
  static Budgets? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get spendMicros => $_getI64(0);
  @$pb.TagNumber(1)
  set spendMicros($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSpendMicros() => $_has(0);
  @$pb.TagNumber(1)
  void clearSpendMicros() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.int get concurrency => $_getIZ(1);
  @$pb.TagNumber(2)
  set concurrency($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(2)
  $core.bool hasConcurrency() => $_has(1);
  @$pb.TagNumber(2)
  void clearConcurrency() => $_clearField(2);
}

class MachineContract extends $pb.GeneratedMessage {
  factory MachineContract({
    Image? image,
    $core.Iterable<Capability>? capabilities,
    CompatibilityPolicy? compatibility,
    $core.List<$core.int>? compatibilityRevision,
    SuspensionPolicy? suspension,
    ExpirationPolicy? expiration,
    $core.List<$core.int>? networkPolicyDigest,
    Budgets? budgets,
  }) {
    final result = MachineContract._();
    if (image != null) result.image = image;
    if (capabilities != null) result.capabilities.addAll(capabilities);
    if (compatibility != null) result.compatibility = compatibility;
    if (compatibilityRevision != null)
      result.compatibilityRevision = compatibilityRevision;
    if (suspension != null) result.suspension = suspension;
    if (expiration != null) result.expiration = expiration;
    if (networkPolicyDigest != null)
      result.networkPolicyDigest = networkPolicyDigest;
    if (budgets != null) result.budgets = budgets;
    return result;
  }

  MachineContract._();

  factory MachineContract.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineContract()..mergeFromBuffer(data, registry);
  factory MachineContract.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineContract()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachineContract',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MachineContract.$_createMessage)
    ..aOM<Image>(1, _omitFieldNames ? '' : 'image',
        subBuilder: Image.$_createMessage)
    ..pc<Capability>(
        2, _omitFieldNames ? '' : 'capabilities', $pb.PbFieldType.KE,
        valueOf: Capability.valueOf,
        enumValues: Capability.values,
        defaultEnumValue: Capability.CAPABILITY_UNSPECIFIED)
    ..aOM<CompatibilityPolicy>(3, _omitFieldNames ? '' : 'compatibility',
        subBuilder: CompatibilityPolicy.$_createMessage)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'compatibilityRevision', $pb.PbFieldType.OY)
    ..aOM<SuspensionPolicy>(6, _omitFieldNames ? '' : 'suspension',
        subBuilder: SuspensionPolicy.$_createMessage)
    ..aOM<ExpirationPolicy>(7, _omitFieldNames ? '' : 'expiration',
        subBuilder: ExpirationPolicy.$_createMessage)
    ..a<$core.List<$core.int>>(
        8, _omitFieldNames ? '' : 'networkPolicyDigest', $pb.PbFieldType.OY)
    ..aOM<Budgets>(9, _omitFieldNames ? '' : 'budgets',
        subBuilder: Budgets.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineContract clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineContract copyWith(void Function(MachineContract) updates) =>
      super.copyWith((message) => updates(message as MachineContract))
          as MachineContract;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MachineContract() / MachineContract.new instead')
  static MachineContract create() => MachineContract._();
  static $pb.GeneratedMessage $_createMessage() => MachineContract._();
  @$core.override
  MachineContract createEmptyInstance() => MachineContract._();
  @$core.pragma('dart2js:noInline')
  static MachineContract getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MachineContract>(
          MachineContract.$_createMessage);
  static MachineContract? _defaultInstance;

  @$pb.TagNumber(1)
  Image get image => $_getN(0);
  @$pb.TagNumber(1)
  set image(Image value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasImage() => $_has(0);
  @$pb.TagNumber(1)
  void clearImage() => $_clearField(1);
  @$pb.TagNumber(1)
  Image ensureImage() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<Capability> get capabilities => $_getList(1);

  @$pb.TagNumber(3)
  CompatibilityPolicy get compatibility => $_getN(2);
  @$pb.TagNumber(3)
  set compatibility(CompatibilityPolicy value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasCompatibility() => $_has(2);
  @$pb.TagNumber(3)
  void clearCompatibility() => $_clearField(3);
  @$pb.TagNumber(3)
  CompatibilityPolicy ensureCompatibility() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.List<$core.int> get compatibilityRevision => $_getN(3);
  @$pb.TagNumber(4)
  set compatibilityRevision($core.List<$core.int> value) =>
      $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCompatibilityRevision() => $_has(3);
  @$pb.TagNumber(4)
  void clearCompatibilityRevision() => $_clearField(4);

  @$pb.TagNumber(6)
  SuspensionPolicy get suspension => $_getN(4);
  @$pb.TagNumber(6)
  set suspension(SuspensionPolicy value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasSuspension() => $_has(4);
  @$pb.TagNumber(6)
  void clearSuspension() => $_clearField(6);
  @$pb.TagNumber(6)
  SuspensionPolicy ensureSuspension() => $_ensure(4);

  @$pb.TagNumber(7)
  ExpirationPolicy get expiration => $_getN(5);
  @$pb.TagNumber(7)
  set expiration(ExpirationPolicy value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasExpiration() => $_has(5);
  @$pb.TagNumber(7)
  void clearExpiration() => $_clearField(7);
  @$pb.TagNumber(7)
  ExpirationPolicy ensureExpiration() => $_ensure(5);

  @$pb.TagNumber(8)
  $core.List<$core.int> get networkPolicyDigest => $_getN(6);
  @$pb.TagNumber(8)
  set networkPolicyDigest($core.List<$core.int> value) => $_setBytes(6, value);
  @$pb.TagNumber(8)
  $core.bool hasNetworkPolicyDigest() => $_has(6);
  @$pb.TagNumber(8)
  void clearNetworkPolicyDigest() => $_clearField(8);

  @$pb.TagNumber(9)
  Budgets get budgets => $_getN(7);
  @$pb.TagNumber(9)
  set budgets(Budgets value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasBudgets() => $_has(7);
  @$pb.TagNumber(9)
  void clearBudgets() => $_clearField(9);
  @$pb.TagNumber(9)
  Budgets ensureBudgets() => $_ensure(7);
}

class QualifyImageRequest extends $pb.GeneratedMessage {
  factory QualifyImageRequest({
    ProtocolVersion? protocol,
    Image? image,
  }) {
    final result = QualifyImageRequest._();
    if (protocol != null) result.protocol = protocol;
    if (image != null) result.image = image;
    return result;
  }

  QualifyImageRequest._();

  factory QualifyImageRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      QualifyImageRequest()..mergeFromBuffer(data, registry);
  factory QualifyImageRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      QualifyImageRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'QualifyImageRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: QualifyImageRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<Image>(2, _omitFieldNames ? '' : 'image',
        subBuilder: Image.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  QualifyImageRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  QualifyImageRequest copyWith(void Function(QualifyImageRequest) updates) =>
      super.copyWith((message) => updates(message as QualifyImageRequest))
          as QualifyImageRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use QualifyImageRequest() / QualifyImageRequest.new instead')
  static QualifyImageRequest create() => QualifyImageRequest._();
  static $pb.GeneratedMessage $_createMessage() => QualifyImageRequest._();
  @$core.override
  QualifyImageRequest createEmptyInstance() => QualifyImageRequest._();
  @$core.pragma('dart2js:noInline')
  static QualifyImageRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<QualifyImageRequest>(
          QualifyImageRequest.$_createMessage);
  static QualifyImageRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  Image get image => $_getN(1);
  @$pb.TagNumber(2)
  set image(Image value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasImage() => $_has(1);
  @$pb.TagNumber(2)
  void clearImage() => $_clearField(2);
  @$pb.TagNumber(2)
  Image ensureImage() => $_ensure(1);
}

class CreateMachineRequest extends $pb.GeneratedMessage {
  factory CreateMachineRequest({
    ProtocolVersion? protocol,
    IdempotencyKey? idempotencyKey,
    Image? image,
    CompatibilityPolicy? compatibility,
    SuspensionPolicy? suspension,
    ExpirationPolicy? expiration,
    $core.List<$core.int>? networkPolicyDigest,
    Budgets? budgets,
  }) {
    final result = CreateMachineRequest._();
    if (protocol != null) result.protocol = protocol;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (image != null) result.image = image;
    if (compatibility != null) result.compatibility = compatibility;
    if (suspension != null) result.suspension = suspension;
    if (expiration != null) result.expiration = expiration;
    if (networkPolicyDigest != null)
      result.networkPolicyDigest = networkPolicyDigest;
    if (budgets != null) result.budgets = budgets;
    return result;
  }

  CreateMachineRequest._();

  factory CreateMachineRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateMachineRequest()..mergeFromBuffer(data, registry);
  factory CreateMachineRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateMachineRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateMachineRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: CreateMachineRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<IdempotencyKey>(2, _omitFieldNames ? '' : 'idempotencyKey',
        subBuilder: IdempotencyKey.$_createMessage)
    ..aOM<Image>(3, _omitFieldNames ? '' : 'image',
        subBuilder: Image.$_createMessage)
    ..aOM<CompatibilityPolicy>(4, _omitFieldNames ? '' : 'compatibility',
        subBuilder: CompatibilityPolicy.$_createMessage)
    ..aOM<SuspensionPolicy>(6, _omitFieldNames ? '' : 'suspension',
        subBuilder: SuspensionPolicy.$_createMessage)
    ..aOM<ExpirationPolicy>(7, _omitFieldNames ? '' : 'expiration',
        subBuilder: ExpirationPolicy.$_createMessage)
    ..a<$core.List<$core.int>>(
        8, _omitFieldNames ? '' : 'networkPolicyDigest', $pb.PbFieldType.OY)
    ..aOM<Budgets>(9, _omitFieldNames ? '' : 'budgets',
        subBuilder: Budgets.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateMachineRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateMachineRequest copyWith(void Function(CreateMachineRequest) updates) =>
      super.copyWith((message) => updates(message as CreateMachineRequest))
          as CreateMachineRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CreateMachineRequest() / CreateMachineRequest.new instead')
  static CreateMachineRequest create() => CreateMachineRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateMachineRequest._();
  @$core.override
  CreateMachineRequest createEmptyInstance() => CreateMachineRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateMachineRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateMachineRequest>(
          CreateMachineRequest.$_createMessage);
  static CreateMachineRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  IdempotencyKey get idempotencyKey => $_getN(1);
  @$pb.TagNumber(2)
  set idempotencyKey(IdempotencyKey value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
  @$pb.TagNumber(2)
  IdempotencyKey ensureIdempotencyKey() => $_ensure(1);

  @$pb.TagNumber(3)
  Image get image => $_getN(2);
  @$pb.TagNumber(3)
  set image(Image value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasImage() => $_has(2);
  @$pb.TagNumber(3)
  void clearImage() => $_clearField(3);
  @$pb.TagNumber(3)
  Image ensureImage() => $_ensure(2);

  @$pb.TagNumber(4)
  CompatibilityPolicy get compatibility => $_getN(3);
  @$pb.TagNumber(4)
  set compatibility(CompatibilityPolicy value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasCompatibility() => $_has(3);
  @$pb.TagNumber(4)
  void clearCompatibility() => $_clearField(4);
  @$pb.TagNumber(4)
  CompatibilityPolicy ensureCompatibility() => $_ensure(3);

  @$pb.TagNumber(6)
  SuspensionPolicy get suspension => $_getN(4);
  @$pb.TagNumber(6)
  set suspension(SuspensionPolicy value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasSuspension() => $_has(4);
  @$pb.TagNumber(6)
  void clearSuspension() => $_clearField(6);
  @$pb.TagNumber(6)
  SuspensionPolicy ensureSuspension() => $_ensure(4);

  @$pb.TagNumber(7)
  ExpirationPolicy get expiration => $_getN(5);
  @$pb.TagNumber(7)
  set expiration(ExpirationPolicy value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasExpiration() => $_has(5);
  @$pb.TagNumber(7)
  void clearExpiration() => $_clearField(7);
  @$pb.TagNumber(7)
  ExpirationPolicy ensureExpiration() => $_ensure(5);

  @$pb.TagNumber(8)
  $core.List<$core.int> get networkPolicyDigest => $_getN(6);
  @$pb.TagNumber(8)
  set networkPolicyDigest($core.List<$core.int> value) => $_setBytes(6, value);
  @$pb.TagNumber(8)
  $core.bool hasNetworkPolicyDigest() => $_has(6);
  @$pb.TagNumber(8)
  void clearNetworkPolicyDigest() => $_clearField(8);

  @$pb.TagNumber(9)
  Budgets get budgets => $_getN(7);
  @$pb.TagNumber(9)
  set budgets(Budgets value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasBudgets() => $_has(7);
  @$pb.TagNumber(9)
  void clearBudgets() => $_clearField(9);
  @$pb.TagNumber(9)
  Budgets ensureBudgets() => $_ensure(7);
}

class MachineMutationRequest extends $pb.GeneratedMessage {
  factory MachineMutationRequest({
    ProtocolVersion? protocol,
    IdempotencyKey? idempotencyKey,
    MachineId? machine,
  }) {
    final result = MachineMutationRequest._();
    if (protocol != null) result.protocol = protocol;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (machine != null) result.machine = machine;
    return result;
  }

  MachineMutationRequest._();

  factory MachineMutationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineMutationRequest()..mergeFromBuffer(data, registry);
  factory MachineMutationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineMutationRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachineMutationRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MachineMutationRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<IdempotencyKey>(2, _omitFieldNames ? '' : 'idempotencyKey',
        subBuilder: IdempotencyKey.$_createMessage)
    ..aOM<MachineId>(3, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineMutationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineMutationRequest copyWith(
          void Function(MachineMutationRequest) updates) =>
      super.copyWith((message) => updates(message as MachineMutationRequest))
          as MachineMutationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use MachineMutationRequest() / MachineMutationRequest.new instead')
  static MachineMutationRequest create() => MachineMutationRequest._();
  static $pb.GeneratedMessage $_createMessage() => MachineMutationRequest._();
  @$core.override
  MachineMutationRequest createEmptyInstance() => MachineMutationRequest._();
  @$core.pragma('dart2js:noInline')
  static MachineMutationRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<MachineMutationRequest>(
          MachineMutationRequest.$_createMessage);
  static MachineMutationRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  IdempotencyKey get idempotencyKey => $_getN(1);
  @$pb.TagNumber(2)
  set idempotencyKey(IdempotencyKey value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
  @$pb.TagNumber(2)
  IdempotencyKey ensureIdempotencyKey() => $_ensure(1);

  @$pb.TagNumber(3)
  MachineId get machine => $_getN(2);
  @$pb.TagNumber(3)
  set machine(MachineId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMachine() => $_has(2);
  @$pb.TagNumber(3)
  void clearMachine() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineId ensureMachine() => $_ensure(2);
}

class CheckpointMachineRequest extends $pb.GeneratedMessage {
  factory CheckpointMachineRequest({
    ProtocolVersion? protocol,
    IdempotencyKey? idempotencyKey,
    MachineId? machine,
  }) {
    final result = CheckpointMachineRequest._();
    if (protocol != null) result.protocol = protocol;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (machine != null) result.machine = machine;
    return result;
  }

  CheckpointMachineRequest._();

  factory CheckpointMachineRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointMachineRequest()..mergeFromBuffer(data, registry);
  factory CheckpointMachineRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointMachineRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CheckpointMachineRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: CheckpointMachineRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<IdempotencyKey>(2, _omitFieldNames ? '' : 'idempotencyKey',
        subBuilder: IdempotencyKey.$_createMessage)
    ..aOM<MachineId>(3, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointMachineRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointMachineRequest copyWith(
          void Function(CheckpointMachineRequest) updates) =>
      super.copyWith((message) => updates(message as CheckpointMachineRequest))
          as CheckpointMachineRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CheckpointMachineRequest() / CheckpointMachineRequest.new instead')
  static CheckpointMachineRequest create() => CheckpointMachineRequest._();
  static $pb.GeneratedMessage $_createMessage() => CheckpointMachineRequest._();
  @$core.override
  CheckpointMachineRequest createEmptyInstance() =>
      CheckpointMachineRequest._();
  @$core.pragma('dart2js:noInline')
  static CheckpointMachineRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CheckpointMachineRequest>(
          CheckpointMachineRequest.$_createMessage);
  static CheckpointMachineRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  IdempotencyKey get idempotencyKey => $_getN(1);
  @$pb.TagNumber(2)
  set idempotencyKey(IdempotencyKey value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
  @$pb.TagNumber(2)
  IdempotencyKey ensureIdempotencyKey() => $_ensure(1);

  @$pb.TagNumber(3)
  MachineId get machine => $_getN(2);
  @$pb.TagNumber(3)
  set machine(MachineId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMachine() => $_has(2);
  @$pb.TagNumber(3)
  void clearMachine() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineId ensureMachine() => $_ensure(2);
}

class ForkCheckpointRequest extends $pb.GeneratedMessage {
  factory ForkCheckpointRequest({
    ProtocolVersion? protocol,
    IdempotencyKey? idempotencyKey,
    CheckpointId? checkpoint,
    $core.int? count,
  }) {
    final result = ForkCheckpointRequest._();
    if (protocol != null) result.protocol = protocol;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (checkpoint != null) result.checkpoint = checkpoint;
    if (count != null) result.count = count;
    return result;
  }

  ForkCheckpointRequest._();

  factory ForkCheckpointRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkCheckpointRequest()..mergeFromBuffer(data, registry);
  factory ForkCheckpointRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkCheckpointRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkCheckpointRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ForkCheckpointRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<IdempotencyKey>(2, _omitFieldNames ? '' : 'idempotencyKey',
        subBuilder: IdempotencyKey.$_createMessage)
    ..aOM<CheckpointId>(3, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..aI(4, _omitFieldNames ? '' : 'count', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkCheckpointRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkCheckpointRequest copyWith(
          void Function(ForkCheckpointRequest) updates) =>
      super.copyWith((message) => updates(message as ForkCheckpointRequest))
          as ForkCheckpointRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ForkCheckpointRequest() / ForkCheckpointRequest.new instead')
  static ForkCheckpointRequest create() => ForkCheckpointRequest._();
  static $pb.GeneratedMessage $_createMessage() => ForkCheckpointRequest._();
  @$core.override
  ForkCheckpointRequest createEmptyInstance() => ForkCheckpointRequest._();
  @$core.pragma('dart2js:noInline')
  static ForkCheckpointRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ForkCheckpointRequest>(
          ForkCheckpointRequest.$_createMessage);
  static ForkCheckpointRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  IdempotencyKey get idempotencyKey => $_getN(1);
  @$pb.TagNumber(2)
  set idempotencyKey(IdempotencyKey value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
  @$pb.TagNumber(2)
  IdempotencyKey ensureIdempotencyKey() => $_ensure(1);

  @$pb.TagNumber(3)
  CheckpointId get checkpoint => $_getN(2);
  @$pb.TagNumber(3)
  set checkpoint(CheckpointId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasCheckpoint() => $_has(2);
  @$pb.TagNumber(3)
  void clearCheckpoint() => $_clearField(3);
  @$pb.TagNumber(3)
  CheckpointId ensureCheckpoint() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.int get count => $_getIZ(3);
  @$pb.TagNumber(4)
  set count($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCount() => $_has(3);
  @$pb.TagNumber(4)
  void clearCount() => $_clearField(4);
}

/// Forks a running machine, without an intermediate checkpoint, into `count` fresh
/// children. Admission requires CAPABILITY_LIVE_FORK or CAPABILITY_DISK_FORK in the source
/// machine's contract; a provider or machine without either rejects with UNIMPLEMENTED so
/// callers fall back to Checkpoint + Fork or a restart. The admitted ForkFidelity states
/// what the children inherited. Children inherit the source's exact MachineContract and
/// receive fresh MachineIds and endpoints; open network connections are never carried over.
/// See the acyclic-machines crate documentation for the complete semantics.
class ForkMachineRequest extends $pb.GeneratedMessage {
  factory ForkMachineRequest({
    ProtocolVersion? protocol,
    IdempotencyKey? idempotencyKey,
    MachineId? machine,
    $core.int? count,
  }) {
    final result = ForkMachineRequest._();
    if (protocol != null) result.protocol = protocol;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (machine != null) result.machine = machine;
    if (count != null) result.count = count;
    return result;
  }

  ForkMachineRequest._();

  factory ForkMachineRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkMachineRequest()..mergeFromBuffer(data, registry);
  factory ForkMachineRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkMachineRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkMachineRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ForkMachineRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<IdempotencyKey>(2, _omitFieldNames ? '' : 'idempotencyKey',
        subBuilder: IdempotencyKey.$_createMessage)
    ..aOM<MachineId>(3, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..aI(4, _omitFieldNames ? '' : 'count', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkMachineRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkMachineRequest copyWith(void Function(ForkMachineRequest) updates) =>
      super.copyWith((message) => updates(message as ForkMachineRequest))
          as ForkMachineRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkMachineRequest() / ForkMachineRequest.new instead')
  static ForkMachineRequest create() => ForkMachineRequest._();
  static $pb.GeneratedMessage $_createMessage() => ForkMachineRequest._();
  @$core.override
  ForkMachineRequest createEmptyInstance() => ForkMachineRequest._();
  @$core.pragma('dart2js:noInline')
  static ForkMachineRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ForkMachineRequest>(
          ForkMachineRequest.$_createMessage);
  static ForkMachineRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  IdempotencyKey get idempotencyKey => $_getN(1);
  @$pb.TagNumber(2)
  set idempotencyKey(IdempotencyKey value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
  @$pb.TagNumber(2)
  IdempotencyKey ensureIdempotencyKey() => $_ensure(1);

  @$pb.TagNumber(3)
  MachineId get machine => $_getN(2);
  @$pb.TagNumber(3)
  set machine(MachineId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMachine() => $_has(2);
  @$pb.TagNumber(3)
  void clearMachine() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineId ensureMachine() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.int get count => $_getIZ(3);
  @$pb.TagNumber(4)
  set count($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasCount() => $_has(3);
  @$pb.TagNumber(4)
  void clearCount() => $_clearField(4);
}

class SetSuspensionPolicyRequest extends $pb.GeneratedMessage {
  factory SetSuspensionPolicyRequest({
    ProtocolVersion? protocol,
    IdempotencyKey? idempotencyKey,
    MachineId? machine,
    SuspensionPolicy? policy,
  }) {
    final result = SetSuspensionPolicyRequest._();
    if (protocol != null) result.protocol = protocol;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (machine != null) result.machine = machine;
    if (policy != null) result.policy = policy;
    return result;
  }

  SetSuspensionPolicyRequest._();

  factory SetSuspensionPolicyRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SetSuspensionPolicyRequest()..mergeFromBuffer(data, registry);
  factory SetSuspensionPolicyRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SetSuspensionPolicyRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SetSuspensionPolicyRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: SetSuspensionPolicyRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<IdempotencyKey>(2, _omitFieldNames ? '' : 'idempotencyKey',
        subBuilder: IdempotencyKey.$_createMessage)
    ..aOM<MachineId>(3, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..aOM<SuspensionPolicy>(4, _omitFieldNames ? '' : 'policy',
        subBuilder: SuspensionPolicy.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SetSuspensionPolicyRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SetSuspensionPolicyRequest copyWith(
          void Function(SetSuspensionPolicyRequest) updates) =>
      super.copyWith(
              (message) => updates(message as SetSuspensionPolicyRequest))
          as SetSuspensionPolicyRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use SetSuspensionPolicyRequest() / SetSuspensionPolicyRequest.new instead')
  static SetSuspensionPolicyRequest create() => SetSuspensionPolicyRequest._();
  static $pb.GeneratedMessage $_createMessage() =>
      SetSuspensionPolicyRequest._();
  @$core.override
  SetSuspensionPolicyRequest createEmptyInstance() =>
      SetSuspensionPolicyRequest._();
  @$core.pragma('dart2js:noInline')
  static SetSuspensionPolicyRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SetSuspensionPolicyRequest>(
          SetSuspensionPolicyRequest.$_createMessage);
  static SetSuspensionPolicyRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  IdempotencyKey get idempotencyKey => $_getN(1);
  @$pb.TagNumber(2)
  set idempotencyKey(IdempotencyKey value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
  @$pb.TagNumber(2)
  IdempotencyKey ensureIdempotencyKey() => $_ensure(1);

  @$pb.TagNumber(3)
  MachineId get machine => $_getN(2);
  @$pb.TagNumber(3)
  set machine(MachineId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMachine() => $_has(2);
  @$pb.TagNumber(3)
  void clearMachine() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineId ensureMachine() => $_ensure(2);

  @$pb.TagNumber(4)
  SuspensionPolicy get policy => $_getN(3);
  @$pb.TagNumber(4)
  set policy(SuspensionPolicy value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasPolicy() => $_has(3);
  @$pb.TagNumber(4)
  void clearPolicy() => $_clearField(4);
  @$pb.TagNumber(4)
  SuspensionPolicy ensurePolicy() => $_ensure(3);
}

class CheckpointMutationRequest extends $pb.GeneratedMessage {
  factory CheckpointMutationRequest({
    ProtocolVersion? protocol,
    IdempotencyKey? idempotencyKey,
    CheckpointId? checkpoint,
  }) {
    final result = CheckpointMutationRequest._();
    if (protocol != null) result.protocol = protocol;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    if (checkpoint != null) result.checkpoint = checkpoint;
    return result;
  }

  CheckpointMutationRequest._();

  factory CheckpointMutationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointMutationRequest()..mergeFromBuffer(data, registry);
  factory CheckpointMutationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointMutationRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CheckpointMutationRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: CheckpointMutationRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<IdempotencyKey>(2, _omitFieldNames ? '' : 'idempotencyKey',
        subBuilder: IdempotencyKey.$_createMessage)
    ..aOM<CheckpointId>(3, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointMutationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointMutationRequest copyWith(
          void Function(CheckpointMutationRequest) updates) =>
      super.copyWith((message) => updates(message as CheckpointMutationRequest))
          as CheckpointMutationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CheckpointMutationRequest() / CheckpointMutationRequest.new instead')
  static CheckpointMutationRequest create() => CheckpointMutationRequest._();
  static $pb.GeneratedMessage $_createMessage() =>
      CheckpointMutationRequest._();
  @$core.override
  CheckpointMutationRequest createEmptyInstance() =>
      CheckpointMutationRequest._();
  @$core.pragma('dart2js:noInline')
  static CheckpointMutationRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CheckpointMutationRequest>(
          CheckpointMutationRequest.$_createMessage);
  static CheckpointMutationRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  IdempotencyKey get idempotencyKey => $_getN(1);
  @$pb.TagNumber(2)
  set idempotencyKey(IdempotencyKey value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
  @$pb.TagNumber(2)
  IdempotencyKey ensureIdempotencyKey() => $_ensure(1);

  @$pb.TagNumber(3)
  CheckpointId get checkpoint => $_getN(2);
  @$pb.TagNumber(3)
  set checkpoint(CheckpointId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasCheckpoint() => $_has(2);
  @$pb.TagNumber(3)
  void clearCheckpoint() => $_clearField(3);
  @$pb.TagNumber(3)
  CheckpointId ensureCheckpoint() => $_ensure(2);
}

class RecoverRequest extends $pb.GeneratedMessage {
  factory RecoverRequest({
    ProtocolVersion? protocol,
    IdempotencyKey? idempotencyKey,
  }) {
    final result = RecoverRequest._();
    if (protocol != null) result.protocol = protocol;
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  RecoverRequest._();

  factory RecoverRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RecoverRequest()..mergeFromBuffer(data, registry);
  factory RecoverRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RecoverRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RecoverRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: RecoverRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<IdempotencyKey>(2, _omitFieldNames ? '' : 'idempotencyKey',
        subBuilder: IdempotencyKey.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RecoverRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RecoverRequest copyWith(void Function(RecoverRequest) updates) =>
      super.copyWith((message) => updates(message as RecoverRequest))
          as RecoverRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RecoverRequest() / RecoverRequest.new instead')
  static RecoverRequest create() => RecoverRequest._();
  static $pb.GeneratedMessage $_createMessage() => RecoverRequest._();
  @$core.override
  RecoverRequest createEmptyInstance() => RecoverRequest._();
  @$core.pragma('dart2js:noInline')
  static RecoverRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RecoverRequest>(
          RecoverRequest.$_createMessage);
  static RecoverRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  IdempotencyKey get idempotencyKey => $_getN(1);
  @$pb.TagNumber(2)
  set idempotencyKey(IdempotencyKey value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasIdempotencyKey() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdempotencyKey() => $_clearField(2);
  @$pb.TagNumber(2)
  IdempotencyKey ensureIdempotencyKey() => $_ensure(1);
}

class InspectMachineRequest extends $pb.GeneratedMessage {
  factory InspectMachineRequest({
    ProtocolVersion? protocol,
    MachineId? machine,
  }) {
    final result = InspectMachineRequest._();
    if (protocol != null) result.protocol = protocol;
    if (machine != null) result.machine = machine;
    return result;
  }

  InspectMachineRequest._();

  factory InspectMachineRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectMachineRequest()..mergeFromBuffer(data, registry);
  factory InspectMachineRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectMachineRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectMachineRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: InspectMachineRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<MachineId>(2, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectMachineRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectMachineRequest copyWith(
          void Function(InspectMachineRequest) updates) =>
      super.copyWith((message) => updates(message as InspectMachineRequest))
          as InspectMachineRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InspectMachineRequest() / InspectMachineRequest.new instead')
  static InspectMachineRequest create() => InspectMachineRequest._();
  static $pb.GeneratedMessage $_createMessage() => InspectMachineRequest._();
  @$core.override
  InspectMachineRequest createEmptyInstance() => InspectMachineRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectMachineRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectMachineRequest>(
          InspectMachineRequest.$_createMessage);
  static InspectMachineRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineId get machine => $_getN(1);
  @$pb.TagNumber(2)
  set machine(MachineId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMachine() => $_has(1);
  @$pb.TagNumber(2)
  void clearMachine() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineId ensureMachine() => $_ensure(1);
}

class InspectCheckpointRequest extends $pb.GeneratedMessage {
  factory InspectCheckpointRequest({
    ProtocolVersion? protocol,
    CheckpointId? checkpoint,
  }) {
    final result = InspectCheckpointRequest._();
    if (protocol != null) result.protocol = protocol;
    if (checkpoint != null) result.checkpoint = checkpoint;
    return result;
  }

  InspectCheckpointRequest._();

  factory InspectCheckpointRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectCheckpointRequest()..mergeFromBuffer(data, registry);
  factory InspectCheckpointRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      InspectCheckpointRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'InspectCheckpointRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: InspectCheckpointRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<CheckpointId>(2, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectCheckpointRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  InspectCheckpointRequest copyWith(
          void Function(InspectCheckpointRequest) updates) =>
      super.copyWith((message) => updates(message as InspectCheckpointRequest))
          as InspectCheckpointRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use InspectCheckpointRequest() / InspectCheckpointRequest.new instead')
  static InspectCheckpointRequest create() => InspectCheckpointRequest._();
  static $pb.GeneratedMessage $_createMessage() => InspectCheckpointRequest._();
  @$core.override
  InspectCheckpointRequest createEmptyInstance() =>
      InspectCheckpointRequest._();
  @$core.pragma('dart2js:noInline')
  static InspectCheckpointRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<InspectCheckpointRequest>(
          InspectCheckpointRequest.$_createMessage);
  static InspectCheckpointRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  CheckpointId get checkpoint => $_getN(1);
  @$pb.TagNumber(2)
  set checkpoint(CheckpointId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasCheckpoint() => $_has(1);
  @$pb.TagNumber(2)
  void clearCheckpoint() => $_clearField(2);
  @$pb.TagNumber(2)
  CheckpointId ensureCheckpoint() => $_ensure(1);
}

class ListMachinesRequest extends $pb.GeneratedMessage {
  factory ListMachinesRequest({
    ProtocolVersion? protocol,
    MachineId? after,
    $core.int? limit,
  }) {
    final result = ListMachinesRequest._();
    if (protocol != null) result.protocol = protocol;
    if (after != null) result.after = after;
    if (limit != null) result.limit = limit;
    return result;
  }

  ListMachinesRequest._();

  factory ListMachinesRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListMachinesRequest()..mergeFromBuffer(data, registry);
  factory ListMachinesRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListMachinesRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListMachinesRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ListMachinesRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<MachineId>(2, _omitFieldNames ? '' : 'after',
        subBuilder: MachineId.$_createMessage)
    ..aI(3, _omitFieldNames ? '' : 'limit', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListMachinesRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListMachinesRequest copyWith(void Function(ListMachinesRequest) updates) =>
      super.copyWith((message) => updates(message as ListMachinesRequest))
          as ListMachinesRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use ListMachinesRequest() / ListMachinesRequest.new instead')
  static ListMachinesRequest create() => ListMachinesRequest._();
  static $pb.GeneratedMessage $_createMessage() => ListMachinesRequest._();
  @$core.override
  ListMachinesRequest createEmptyInstance() => ListMachinesRequest._();
  @$core.pragma('dart2js:noInline')
  static ListMachinesRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ListMachinesRequest>(
          ListMachinesRequest.$_createMessage);
  static ListMachinesRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineId get after => $_getN(1);
  @$pb.TagNumber(2)
  set after(MachineId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAfter() => $_has(1);
  @$pb.TagNumber(2)
  void clearAfter() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineId ensureAfter() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.int get limit => $_getIZ(2);
  @$pb.TagNumber(3)
  set limit($core.int value) => $_setUnsignedInt32(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLimit() => $_has(2);
  @$pb.TagNumber(3)
  void clearLimit() => $_clearField(3);
}

class OperationRequest extends $pb.GeneratedMessage {
  factory OperationRequest({
    ProtocolVersion? protocol,
    OperationId? operation,
  }) {
    final result = OperationRequest._();
    if (protocol != null) result.protocol = protocol;
    if (operation != null) result.operation = operation;
    return result;
  }

  OperationRequest._();

  factory OperationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationRequest()..mergeFromBuffer(data, registry);
  factory OperationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OperationRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: OperationRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<OperationId>(2, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationRequest copyWith(void Function(OperationRequest) updates) =>
      super.copyWith((message) => updates(message as OperationRequest))
          as OperationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OperationRequest() / OperationRequest.new instead')
  static OperationRequest create() => OperationRequest._();
  static $pb.GeneratedMessage $_createMessage() => OperationRequest._();
  @$core.override
  OperationRequest createEmptyInstance() => OperationRequest._();
  @$core.pragma('dart2js:noInline')
  static OperationRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OperationRequest>(
          OperationRequest.$_createMessage);
  static OperationRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  OperationId get operation => $_getN(1);
  @$pb.TagNumber(2)
  set operation(OperationId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOperation() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperation() => $_clearField(2);
  @$pb.TagNumber(2)
  OperationId ensureOperation() => $_ensure(1);
}

class OperationState extends $pb.GeneratedMessage {
  factory OperationState({
    OperationId? operation,
    OperationStatus? status,
  }) {
    final result = OperationState._();
    if (operation != null) result.operation = operation;
    if (status != null) result.status = status;
    return result;
  }

  OperationState._();

  factory OperationState.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationState()..mergeFromBuffer(data, registry);
  factory OperationState.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationState()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OperationState',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: OperationState.$_createMessage)
    ..aOM<OperationId>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..aE<OperationStatus>(2, _omitFieldNames ? '' : 'status',
        enumValues: OperationStatus.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationState clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationState copyWith(void Function(OperationState) updates) =>
      super.copyWith((message) => updates(message as OperationState))
          as OperationState;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OperationState() / OperationState.new instead')
  static OperationState create() => OperationState._();
  static $pb.GeneratedMessage $_createMessage() => OperationState._();
  @$core.override
  OperationState createEmptyInstance() => OperationState._();
  @$core.pragma('dart2js:noInline')
  static OperationState getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OperationState>(
          OperationState.$_createMessage);
  static OperationState? _defaultInstance;

  @$pb.TagNumber(1)
  OperationId get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(OperationId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationId ensureOperation() => $_ensure(0);

  @$pb.TagNumber(2)
  OperationStatus get status => $_getN(1);
  @$pb.TagNumber(2)
  set status(OperationStatus value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasStatus() => $_has(1);
  @$pb.TagNumber(2)
  void clearStatus() => $_clearField(2);
}

class Endpoint extends $pb.GeneratedMessage {
  factory Endpoint({
    $core.String? name,
    $core.String? uri,
  }) {
    final result = Endpoint._();
    if (name != null) result.name = name;
    if (uri != null) result.uri = uri;
    return result;
  }

  Endpoint._();

  factory Endpoint.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Endpoint()..mergeFromBuffer(data, registry);
  factory Endpoint.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Endpoint()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Endpoint',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: Endpoint.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aOS(2, _omitFieldNames ? '' : 'uri')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Endpoint clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Endpoint copyWith(void Function(Endpoint) updates) =>
      super.copyWith((message) => updates(message as Endpoint)) as Endpoint;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Endpoint() / Endpoint.new instead')
  static Endpoint create() => Endpoint._();
  static $pb.GeneratedMessage $_createMessage() => Endpoint._();
  @$core.override
  Endpoint createEmptyInstance() => Endpoint._();
  @$core.pragma('dart2js:noInline')
  static Endpoint getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Endpoint>(Endpoint.$_createMessage);
  static Endpoint? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get uri => $_getSZ(1);
  @$pb.TagNumber(2)
  set uri($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUri() => $_has(1);
  @$pb.TagNumber(2)
  void clearUri() => $_clearField(2);
}

class MachineState extends $pb.GeneratedMessage {
  factory MachineState({
    MachineId? machine,
    MachineStatus? status,
    MachineContract? contract,
    $core.Iterable<Endpoint>? endpoints,
    CheckpointId? lastCheckpoint,
    $fixnum.Int64? createdAtUnixMs,
    $fixnum.Int64? changedAtUnixMs,
  }) {
    final result = MachineState._();
    if (machine != null) result.machine = machine;
    if (status != null) result.status = status;
    if (contract != null) result.contract = contract;
    if (endpoints != null) result.endpoints.addAll(endpoints);
    if (lastCheckpoint != null) result.lastCheckpoint = lastCheckpoint;
    if (createdAtUnixMs != null) result.createdAtUnixMs = createdAtUnixMs;
    if (changedAtUnixMs != null) result.changedAtUnixMs = changedAtUnixMs;
    return result;
  }

  MachineState._();

  factory MachineState.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineState()..mergeFromBuffer(data, registry);
  factory MachineState.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineState()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachineState',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MachineState.$_createMessage)
    ..aOM<MachineId>(1, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..aE<MachineStatus>(2, _omitFieldNames ? '' : 'status',
        enumValues: MachineStatus.values)
    ..aOM<MachineContract>(3, _omitFieldNames ? '' : 'contract',
        subBuilder: MachineContract.$_createMessage)
    ..pPM<Endpoint>(4, _omitFieldNames ? '' : 'endpoints',
        subBuilder: Endpoint.$_createMessage)
    ..aOM<CheckpointId>(5, _omitFieldNames ? '' : 'lastCheckpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..a<$fixnum.Int64>(
        6, _omitFieldNames ? '' : 'createdAtUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        7, _omitFieldNames ? '' : 'changedAtUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineState clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineState copyWith(void Function(MachineState) updates) =>
      super.copyWith((message) => updates(message as MachineState))
          as MachineState;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MachineState() / MachineState.new instead')
  static MachineState create() => MachineState._();
  static $pb.GeneratedMessage $_createMessage() => MachineState._();
  @$core.override
  MachineState createEmptyInstance() => MachineState._();
  @$core.pragma('dart2js:noInline')
  static MachineState getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MachineState>(
          MachineState.$_createMessage);
  static MachineState? _defaultInstance;

  @$pb.TagNumber(1)
  MachineId get machine => $_getN(0);
  @$pb.TagNumber(1)
  set machine(MachineId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasMachine() => $_has(0);
  @$pb.TagNumber(1)
  void clearMachine() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineId ensureMachine() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineStatus get status => $_getN(1);
  @$pb.TagNumber(2)
  set status(MachineStatus value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasStatus() => $_has(1);
  @$pb.TagNumber(2)
  void clearStatus() => $_clearField(2);

  @$pb.TagNumber(3)
  MachineContract get contract => $_getN(2);
  @$pb.TagNumber(3)
  set contract(MachineContract value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasContract() => $_has(2);
  @$pb.TagNumber(3)
  void clearContract() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineContract ensureContract() => $_ensure(2);

  @$pb.TagNumber(4)
  $pb.PbList<Endpoint> get endpoints => $_getList(3);

  @$pb.TagNumber(5)
  CheckpointId get lastCheckpoint => $_getN(4);
  @$pb.TagNumber(5)
  set lastCheckpoint(CheckpointId value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasLastCheckpoint() => $_has(4);
  @$pb.TagNumber(5)
  void clearLastCheckpoint() => $_clearField(5);
  @$pb.TagNumber(5)
  CheckpointId ensureLastCheckpoint() => $_ensure(4);

  @$pb.TagNumber(6)
  $fixnum.Int64 get createdAtUnixMs => $_getI64(5);
  @$pb.TagNumber(6)
  set createdAtUnixMs($fixnum.Int64 value) => $_setInt64(5, value);
  @$pb.TagNumber(6)
  $core.bool hasCreatedAtUnixMs() => $_has(5);
  @$pb.TagNumber(6)
  void clearCreatedAtUnixMs() => $_clearField(6);

  @$pb.TagNumber(7)
  $fixnum.Int64 get changedAtUnixMs => $_getI64(6);
  @$pb.TagNumber(7)
  set changedAtUnixMs($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasChangedAtUnixMs() => $_has(6);
  @$pb.TagNumber(7)
  void clearChangedAtUnixMs() => $_clearField(7);
}

class MachinePage extends $pb.GeneratedMessage {
  factory MachinePage({
    $core.Iterable<MachineState>? machines,
    MachineId? next,
  }) {
    final result = MachinePage._();
    if (machines != null) result.machines.addAll(machines);
    if (next != null) result.next = next;
    return result;
  }

  MachinePage._();

  factory MachinePage.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachinePage()..mergeFromBuffer(data, registry);
  factory MachinePage.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachinePage()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachinePage',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MachinePage.$_createMessage)
    ..pPM<MachineState>(1, _omitFieldNames ? '' : 'machines',
        subBuilder: MachineState.$_createMessage)
    ..aOM<MachineId>(2, _omitFieldNames ? '' : 'next',
        subBuilder: MachineId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachinePage clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachinePage copyWith(void Function(MachinePage) updates) =>
      super.copyWith((message) => updates(message as MachinePage))
          as MachinePage;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MachinePage() / MachinePage.new instead')
  static MachinePage create() => MachinePage._();
  static $pb.GeneratedMessage $_createMessage() => MachinePage._();
  @$core.override
  MachinePage createEmptyInstance() => MachinePage._();
  @$core.pragma('dart2js:noInline')
  static MachinePage getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MachinePage>(
          MachinePage.$_createMessage);
  static MachinePage? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<MachineState> get machines => $_getList(0);

  @$pb.TagNumber(2)
  MachineId get next => $_getN(1);
  @$pb.TagNumber(2)
  set next(MachineId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasNext() => $_has(1);
  @$pb.TagNumber(2)
  void clearNext() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineId ensureNext() => $_ensure(1);
}

class CheckpointState extends $pb.GeneratedMessage {
  factory CheckpointState({
    CheckpointId? checkpoint,
    MachineId? source,
    MachineContract? contract,
    $core.bool? forkable,
    $fixnum.Int64? createdAtUnixMs,
  }) {
    final result = CheckpointState._();
    if (checkpoint != null) result.checkpoint = checkpoint;
    if (source != null) result.source = source;
    if (contract != null) result.contract = contract;
    if (forkable != null) result.forkable = forkable;
    if (createdAtUnixMs != null) result.createdAtUnixMs = createdAtUnixMs;
    return result;
  }

  CheckpointState._();

  factory CheckpointState.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointState()..mergeFromBuffer(data, registry);
  factory CheckpointState.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointState()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CheckpointState',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: CheckpointState.$_createMessage)
    ..aOM<CheckpointId>(1, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..aOM<MachineId>(2, _omitFieldNames ? '' : 'source',
        subBuilder: MachineId.$_createMessage)
    ..aOM<MachineContract>(3, _omitFieldNames ? '' : 'contract',
        subBuilder: MachineContract.$_createMessage)
    ..aOB(4, _omitFieldNames ? '' : 'forkable')
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'createdAtUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointState clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointState copyWith(void Function(CheckpointState) updates) =>
      super.copyWith((message) => updates(message as CheckpointState))
          as CheckpointState;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CheckpointState() / CheckpointState.new instead')
  static CheckpointState create() => CheckpointState._();
  static $pb.GeneratedMessage $_createMessage() => CheckpointState._();
  @$core.override
  CheckpointState createEmptyInstance() => CheckpointState._();
  @$core.pragma('dart2js:noInline')
  static CheckpointState getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CheckpointState>(
          CheckpointState.$_createMessage);
  static CheckpointState? _defaultInstance;

  @$pb.TagNumber(1)
  CheckpointId get checkpoint => $_getN(0);
  @$pb.TagNumber(1)
  set checkpoint(CheckpointId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCheckpoint() => $_has(0);
  @$pb.TagNumber(1)
  void clearCheckpoint() => $_clearField(1);
  @$pb.TagNumber(1)
  CheckpointId ensureCheckpoint() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineId get source => $_getN(1);
  @$pb.TagNumber(2)
  set source(MachineId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSource() => $_has(1);
  @$pb.TagNumber(2)
  void clearSource() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineId ensureSource() => $_ensure(1);

  @$pb.TagNumber(3)
  MachineContract get contract => $_getN(2);
  @$pb.TagNumber(3)
  set contract(MachineContract value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasContract() => $_has(2);
  @$pb.TagNumber(3)
  void clearContract() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineContract ensureContract() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.bool get forkable => $_getBF(3);
  @$pb.TagNumber(4)
  set forkable($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasForkable() => $_has(3);
  @$pb.TagNumber(4)
  void clearForkable() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get createdAtUnixMs => $_getI64(4);
  @$pb.TagNumber(5)
  set createdAtUnixMs($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasCreatedAtUnixMs() => $_has(4);
  @$pb.TagNumber(5)
  void clearCreatedAtUnixMs() => $_clearField(5);
}

class MachineAdmission extends $pb.GeneratedMessage {
  factory MachineAdmission({
    MachineId? machine,
    OperationId? operation,
    MachineContract? contract,
  }) {
    final result = MachineAdmission._();
    if (machine != null) result.machine = machine;
    if (operation != null) result.operation = operation;
    if (contract != null) result.contract = contract;
    return result;
  }

  MachineAdmission._();

  factory MachineAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineAdmission()..mergeFromBuffer(data, registry);
  factory MachineAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineAdmission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachineAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MachineAdmission.$_createMessage)
    ..aOM<MachineId>(1, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..aOM<OperationId>(2, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..aOM<MachineContract>(3, _omitFieldNames ? '' : 'contract',
        subBuilder: MachineContract.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineAdmission copyWith(void Function(MachineAdmission) updates) =>
      super.copyWith((message) => updates(message as MachineAdmission))
          as MachineAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MachineAdmission() / MachineAdmission.new instead')
  static MachineAdmission create() => MachineAdmission._();
  static $pb.GeneratedMessage $_createMessage() => MachineAdmission._();
  @$core.override
  MachineAdmission createEmptyInstance() => MachineAdmission._();
  @$core.pragma('dart2js:noInline')
  static MachineAdmission getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MachineAdmission>(
          MachineAdmission.$_createMessage);
  static MachineAdmission? _defaultInstance;

  @$pb.TagNumber(1)
  MachineId get machine => $_getN(0);
  @$pb.TagNumber(1)
  set machine(MachineId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasMachine() => $_has(0);
  @$pb.TagNumber(1)
  void clearMachine() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineId ensureMachine() => $_ensure(0);

  @$pb.TagNumber(2)
  OperationId get operation => $_getN(1);
  @$pb.TagNumber(2)
  set operation(OperationId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOperation() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperation() => $_clearField(2);
  @$pb.TagNumber(2)
  OperationId ensureOperation() => $_ensure(1);

  @$pb.TagNumber(3)
  MachineContract get contract => $_getN(2);
  @$pb.TagNumber(3)
  set contract(MachineContract value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasContract() => $_has(2);
  @$pb.TagNumber(3)
  void clearContract() => $_clearField(3);
  @$pb.TagNumber(3)
  MachineContract ensureContract() => $_ensure(2);
}

class CheckpointAdmission extends $pb.GeneratedMessage {
  factory CheckpointAdmission({
    CheckpointId? checkpoint,
    MachineId? source,
    OperationId? operation,
    MachineContract? contract,
  }) {
    final result = CheckpointAdmission._();
    if (checkpoint != null) result.checkpoint = checkpoint;
    if (source != null) result.source = source;
    if (operation != null) result.operation = operation;
    if (contract != null) result.contract = contract;
    return result;
  }

  CheckpointAdmission._();

  factory CheckpointAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointAdmission()..mergeFromBuffer(data, registry);
  factory CheckpointAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CheckpointAdmission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CheckpointAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: CheckpointAdmission.$_createMessage)
    ..aOM<CheckpointId>(1, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..aOM<MachineId>(2, _omitFieldNames ? '' : 'source',
        subBuilder: MachineId.$_createMessage)
    ..aOM<OperationId>(3, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..aOM<MachineContract>(4, _omitFieldNames ? '' : 'contract',
        subBuilder: MachineContract.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CheckpointAdmission copyWith(void Function(CheckpointAdmission) updates) =>
      super.copyWith((message) => updates(message as CheckpointAdmission))
          as CheckpointAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use CheckpointAdmission() / CheckpointAdmission.new instead')
  static CheckpointAdmission create() => CheckpointAdmission._();
  static $pb.GeneratedMessage $_createMessage() => CheckpointAdmission._();
  @$core.override
  CheckpointAdmission createEmptyInstance() => CheckpointAdmission._();
  @$core.pragma('dart2js:noInline')
  static CheckpointAdmission getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CheckpointAdmission>(
          CheckpointAdmission.$_createMessage);
  static CheckpointAdmission? _defaultInstance;

  @$pb.TagNumber(1)
  CheckpointId get checkpoint => $_getN(0);
  @$pb.TagNumber(1)
  set checkpoint(CheckpointId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCheckpoint() => $_has(0);
  @$pb.TagNumber(1)
  void clearCheckpoint() => $_clearField(1);
  @$pb.TagNumber(1)
  CheckpointId ensureCheckpoint() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineId get source => $_getN(1);
  @$pb.TagNumber(2)
  set source(MachineId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSource() => $_has(1);
  @$pb.TagNumber(2)
  void clearSource() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineId ensureSource() => $_ensure(1);

  @$pb.TagNumber(3)
  OperationId get operation => $_getN(2);
  @$pb.TagNumber(3)
  set operation(OperationId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperation() => $_clearField(3);
  @$pb.TagNumber(3)
  OperationId ensureOperation() => $_ensure(2);

  @$pb.TagNumber(4)
  MachineContract get contract => $_getN(3);
  @$pb.TagNumber(4)
  set contract(MachineContract value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasContract() => $_has(3);
  @$pb.TagNumber(4)
  void clearContract() => $_clearField(4);
  @$pb.TagNumber(4)
  MachineContract ensureContract() => $_ensure(3);
}

class ForkAdmission extends $pb.GeneratedMessage {
  factory ForkAdmission({
    CheckpointId? checkpoint,
    $core.Iterable<MachineId>? children,
    OperationId? operation,
    MachineContract? contract,
  }) {
    final result = ForkAdmission._();
    if (checkpoint != null) result.checkpoint = checkpoint;
    if (children != null) result.children.addAll(children);
    if (operation != null) result.operation = operation;
    if (contract != null) result.contract = contract;
    return result;
  }

  ForkAdmission._();

  factory ForkAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkAdmission()..mergeFromBuffer(data, registry);
  factory ForkAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkAdmission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ForkAdmission.$_createMessage)
    ..aOM<CheckpointId>(1, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..pPM<MachineId>(2, _omitFieldNames ? '' : 'children',
        subBuilder: MachineId.$_createMessage)
    ..aOM<OperationId>(3, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..aOM<MachineContract>(4, _omitFieldNames ? '' : 'contract',
        subBuilder: MachineContract.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkAdmission copyWith(void Function(ForkAdmission) updates) =>
      super.copyWith((message) => updates(message as ForkAdmission))
          as ForkAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkAdmission() / ForkAdmission.new instead')
  static ForkAdmission create() => ForkAdmission._();
  static $pb.GeneratedMessage $_createMessage() => ForkAdmission._();
  @$core.override
  ForkAdmission createEmptyInstance() => ForkAdmission._();
  @$core.pragma('dart2js:noInline')
  static ForkAdmission getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkAdmission>(
          ForkAdmission.$_createMessage);
  static ForkAdmission? _defaultInstance;

  @$pb.TagNumber(1)
  CheckpointId get checkpoint => $_getN(0);
  @$pb.TagNumber(1)
  set checkpoint(CheckpointId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCheckpoint() => $_has(0);
  @$pb.TagNumber(1)
  void clearCheckpoint() => $_clearField(1);
  @$pb.TagNumber(1)
  CheckpointId ensureCheckpoint() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<MachineId> get children => $_getList(1);

  @$pb.TagNumber(3)
  OperationId get operation => $_getN(2);
  @$pb.TagNumber(3)
  set operation(OperationId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperation() => $_clearField(3);
  @$pb.TagNumber(3)
  OperationId ensureOperation() => $_ensure(2);

  @$pb.TagNumber(4)
  MachineContract get contract => $_getN(3);
  @$pb.TagNumber(4)
  set contract(MachineContract value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasContract() => $_has(3);
  @$pb.TagNumber(4)
  void clearContract() => $_clearField(4);
  @$pb.TagNumber(4)
  MachineContract ensureContract() => $_ensure(3);
}

class ForkMachineAdmission extends $pb.GeneratedMessage {
  factory ForkMachineAdmission({
    MachineId? source,
    $core.Iterable<MachineId>? children,
    OperationId? operation,
    MachineContract? contract,
    ForkFidelity? fidelity,
  }) {
    final result = ForkMachineAdmission._();
    if (source != null) result.source = source;
    if (children != null) result.children.addAll(children);
    if (operation != null) result.operation = operation;
    if (contract != null) result.contract = contract;
    if (fidelity != null) result.fidelity = fidelity;
    return result;
  }

  ForkMachineAdmission._();

  factory ForkMachineAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkMachineAdmission()..mergeFromBuffer(data, registry);
  factory ForkMachineAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkMachineAdmission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkMachineAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ForkMachineAdmission.$_createMessage)
    ..aOM<MachineId>(1, _omitFieldNames ? '' : 'source',
        subBuilder: MachineId.$_createMessage)
    ..pPM<MachineId>(2, _omitFieldNames ? '' : 'children',
        subBuilder: MachineId.$_createMessage)
    ..aOM<OperationId>(3, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..aOM<MachineContract>(4, _omitFieldNames ? '' : 'contract',
        subBuilder: MachineContract.$_createMessage)
    ..aE<ForkFidelity>(5, _omitFieldNames ? '' : 'fidelity',
        enumValues: ForkFidelity.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkMachineAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkMachineAdmission copyWith(void Function(ForkMachineAdmission) updates) =>
      super.copyWith((message) => updates(message as ForkMachineAdmission))
          as ForkMachineAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ForkMachineAdmission() / ForkMachineAdmission.new instead')
  static ForkMachineAdmission create() => ForkMachineAdmission._();
  static $pb.GeneratedMessage $_createMessage() => ForkMachineAdmission._();
  @$core.override
  ForkMachineAdmission createEmptyInstance() => ForkMachineAdmission._();
  @$core.pragma('dart2js:noInline')
  static ForkMachineAdmission getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ForkMachineAdmission>(
          ForkMachineAdmission.$_createMessage);
  static ForkMachineAdmission? _defaultInstance;

  @$pb.TagNumber(1)
  MachineId get source => $_getN(0);
  @$pb.TagNumber(1)
  set source(MachineId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineId ensureSource() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<MachineId> get children => $_getList(1);

  @$pb.TagNumber(3)
  OperationId get operation => $_getN(2);
  @$pb.TagNumber(3)
  set operation(OperationId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperation() => $_clearField(3);
  @$pb.TagNumber(3)
  OperationId ensureOperation() => $_ensure(2);

  @$pb.TagNumber(4)
  MachineContract get contract => $_getN(3);
  @$pb.TagNumber(4)
  set contract(MachineContract value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasContract() => $_has(3);
  @$pb.TagNumber(4)
  void clearContract() => $_clearField(4);
  @$pb.TagNumber(4)
  MachineContract ensureContract() => $_ensure(3);

  @$pb.TagNumber(5)
  ForkFidelity get fidelity => $_getN(4);
  @$pb.TagNumber(5)
  set fidelity(ForkFidelity value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasFidelity() => $_has(4);
  @$pb.TagNumber(5)
  void clearFidelity() => $_clearField(5);
}

class PolicyAdmission extends $pb.GeneratedMessage {
  factory PolicyAdmission({
    MachineId? machine,
    OperationId? operation,
    SuspensionPolicy? policy,
  }) {
    final result = PolicyAdmission._();
    if (machine != null) result.machine = machine;
    if (operation != null) result.operation = operation;
    if (policy != null) result.policy = policy;
    return result;
  }

  PolicyAdmission._();

  factory PolicyAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PolicyAdmission()..mergeFromBuffer(data, registry);
  factory PolicyAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PolicyAdmission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PolicyAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: PolicyAdmission.$_createMessage)
    ..aOM<MachineId>(1, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..aOM<OperationId>(2, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..aOM<SuspensionPolicy>(3, _omitFieldNames ? '' : 'policy',
        subBuilder: SuspensionPolicy.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PolicyAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PolicyAdmission copyWith(void Function(PolicyAdmission) updates) =>
      super.copyWith((message) => updates(message as PolicyAdmission))
          as PolicyAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use PolicyAdmission() / PolicyAdmission.new instead')
  static PolicyAdmission create() => PolicyAdmission._();
  static $pb.GeneratedMessage $_createMessage() => PolicyAdmission._();
  @$core.override
  PolicyAdmission createEmptyInstance() => PolicyAdmission._();
  @$core.pragma('dart2js:noInline')
  static PolicyAdmission getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<PolicyAdmission>(
          PolicyAdmission.$_createMessage);
  static PolicyAdmission? _defaultInstance;

  @$pb.TagNumber(1)
  MachineId get machine => $_getN(0);
  @$pb.TagNumber(1)
  set machine(MachineId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasMachine() => $_has(0);
  @$pb.TagNumber(1)
  void clearMachine() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineId ensureMachine() => $_ensure(0);

  @$pb.TagNumber(2)
  OperationId get operation => $_getN(1);
  @$pb.TagNumber(2)
  set operation(OperationId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOperation() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperation() => $_clearField(2);
  @$pb.TagNumber(2)
  OperationId ensureOperation() => $_ensure(1);

  @$pb.TagNumber(3)
  SuspensionPolicy get policy => $_getN(2);
  @$pb.TagNumber(3)
  set policy(SuspensionPolicy value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasPolicy() => $_has(2);
  @$pb.TagNumber(3)
  void clearPolicy() => $_clearField(3);
  @$pb.TagNumber(3)
  SuspensionPolicy ensurePolicy() => $_ensure(2);
}

class MutationAdmission extends $pb.GeneratedMessage {
  factory MutationAdmission({
    OperationId? operation,
    MachineId? machine,
    CheckpointId? checkpoint,
  }) {
    final result = MutationAdmission._();
    if (operation != null) result.operation = operation;
    if (machine != null) result.machine = machine;
    if (checkpoint != null) result.checkpoint = checkpoint;
    return result;
  }

  MutationAdmission._();

  factory MutationAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationAdmission()..mergeFromBuffer(data, registry);
  factory MutationAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationAdmission()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MutationAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MutationAdmission.$_createMessage)
    ..aOM<OperationId>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..aOM<MachineId>(2, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..aOM<CheckpointId>(3, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointId.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationAdmission copyWith(void Function(MutationAdmission) updates) =>
      super.copyWith((message) => updates(message as MutationAdmission))
          as MutationAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MutationAdmission() / MutationAdmission.new instead')
  static MutationAdmission create() => MutationAdmission._();
  static $pb.GeneratedMessage $_createMessage() => MutationAdmission._();
  @$core.override
  MutationAdmission createEmptyInstance() => MutationAdmission._();
  @$core.pragma('dart2js:noInline')
  static MutationAdmission getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MutationAdmission>(
          MutationAdmission.$_createMessage);
  static MutationAdmission? _defaultInstance;

  @$pb.TagNumber(1)
  OperationId get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(OperationId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationId ensureOperation() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineId get machine => $_getN(1);
  @$pb.TagNumber(2)
  set machine(MachineId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMachine() => $_has(1);
  @$pb.TagNumber(2)
  void clearMachine() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineId ensureMachine() => $_ensure(1);

  @$pb.TagNumber(3)
  CheckpointId get checkpoint => $_getN(2);
  @$pb.TagNumber(3)
  set checkpoint(CheckpointId value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasCheckpoint() => $_has(2);
  @$pb.TagNumber(3)
  void clearCheckpoint() => $_clearField(3);
  @$pb.TagNumber(3)
  CheckpointId ensureCheckpoint() => $_ensure(2);
}

enum RecoveredAdmission_Result {
  create_2,
  checkpoint,
  fork,
  suspend,
  wake,
  destroyMachine,
  setSuspensionPolicy,
  destroyCheckpoint,
  forkMachine,
  notSet
}

class RecoveredAdmission extends $pb.GeneratedMessage {
  factory RecoveredAdmission({
    OperationId? operation,
    MachineAdmission? create_2,
    CheckpointAdmission? checkpoint,
    ForkAdmission? fork,
    MutationAdmission? suspend,
    MutationAdmission? wake,
    MutationAdmission? destroyMachine,
    PolicyAdmission? setSuspensionPolicy,
    MutationAdmission? destroyCheckpoint,
    ForkMachineAdmission? forkMachine,
  }) {
    final result = RecoveredAdmission._();
    if (operation != null) result.operation = operation;
    if (create_2 != null) result.create_2 = create_2;
    if (checkpoint != null) result.checkpoint = checkpoint;
    if (fork != null) result.fork = fork;
    if (suspend != null) result.suspend = suspend;
    if (wake != null) result.wake = wake;
    if (destroyMachine != null) result.destroyMachine = destroyMachine;
    if (setSuspensionPolicy != null)
      result.setSuspensionPolicy = setSuspensionPolicy;
    if (destroyCheckpoint != null) result.destroyCheckpoint = destroyCheckpoint;
    if (forkMachine != null) result.forkMachine = forkMachine;
    return result;
  }

  RecoveredAdmission._();

  factory RecoveredAdmission.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RecoveredAdmission()..mergeFromBuffer(data, registry);
  factory RecoveredAdmission.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RecoveredAdmission()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, RecoveredAdmission_Result>
      _RecoveredAdmission_ResultByTag = {
    2: RecoveredAdmission_Result.create_2,
    3: RecoveredAdmission_Result.checkpoint,
    4: RecoveredAdmission_Result.fork,
    5: RecoveredAdmission_Result.suspend,
    6: RecoveredAdmission_Result.wake,
    7: RecoveredAdmission_Result.destroyMachine,
    8: RecoveredAdmission_Result.setSuspensionPolicy,
    9: RecoveredAdmission_Result.destroyCheckpoint,
    10: RecoveredAdmission_Result.forkMachine,
    0: RecoveredAdmission_Result.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RecoveredAdmission',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: RecoveredAdmission.$_createMessage)
    ..oo(0, [2, 3, 4, 5, 6, 7, 8, 9, 10])
    ..aOM<OperationId>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationId.$_createMessage)
    ..aOM<MachineAdmission>(2, _omitFieldNames ? '' : 'create',
        subBuilder: MachineAdmission.$_createMessage)
    ..aOM<CheckpointAdmission>(3, _omitFieldNames ? '' : 'checkpoint',
        subBuilder: CheckpointAdmission.$_createMessage)
    ..aOM<ForkAdmission>(4, _omitFieldNames ? '' : 'fork',
        subBuilder: ForkAdmission.$_createMessage)
    ..aOM<MutationAdmission>(5, _omitFieldNames ? '' : 'suspend',
        subBuilder: MutationAdmission.$_createMessage)
    ..aOM<MutationAdmission>(6, _omitFieldNames ? '' : 'wake',
        subBuilder: MutationAdmission.$_createMessage)
    ..aOM<MutationAdmission>(7, _omitFieldNames ? '' : 'destroyMachine',
        subBuilder: MutationAdmission.$_createMessage)
    ..aOM<PolicyAdmission>(8, _omitFieldNames ? '' : 'setSuspensionPolicy',
        subBuilder: PolicyAdmission.$_createMessage)
    ..aOM<MutationAdmission>(9, _omitFieldNames ? '' : 'destroyCheckpoint',
        subBuilder: MutationAdmission.$_createMessage)
    ..aOM<ForkMachineAdmission>(10, _omitFieldNames ? '' : 'forkMachine',
        subBuilder: ForkMachineAdmission.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RecoveredAdmission clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RecoveredAdmission copyWith(void Function(RecoveredAdmission) updates) =>
      super.copyWith((message) => updates(message as RecoveredAdmission))
          as RecoveredAdmission;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RecoveredAdmission() / RecoveredAdmission.new instead')
  static RecoveredAdmission create() => RecoveredAdmission._();
  static $pb.GeneratedMessage $_createMessage() => RecoveredAdmission._();
  @$core.override
  RecoveredAdmission createEmptyInstance() => RecoveredAdmission._();
  @$core.pragma('dart2js:noInline')
  static RecoveredAdmission getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RecoveredAdmission>(
          RecoveredAdmission.$_createMessage);
  static RecoveredAdmission? _defaultInstance;

  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  @$pb.TagNumber(8)
  @$pb.TagNumber(9)
  @$pb.TagNumber(10)
  RecoveredAdmission_Result whichResult() =>
      _RecoveredAdmission_ResultByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  @$pb.TagNumber(8)
  @$pb.TagNumber(9)
  @$pb.TagNumber(10)
  void clearResult() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  OperationId get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(OperationId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  OperationId ensureOperation() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineAdmission get create_2 => $_getN(1);
  @$pb.TagNumber(2)
  set create_2(MachineAdmission value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasCreate_2() => $_has(1);
  @$pb.TagNumber(2)
  void clearCreate_2() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineAdmission ensureCreate_2() => $_ensure(1);

  @$pb.TagNumber(3)
  CheckpointAdmission get checkpoint => $_getN(2);
  @$pb.TagNumber(3)
  set checkpoint(CheckpointAdmission value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasCheckpoint() => $_has(2);
  @$pb.TagNumber(3)
  void clearCheckpoint() => $_clearField(3);
  @$pb.TagNumber(3)
  CheckpointAdmission ensureCheckpoint() => $_ensure(2);

  @$pb.TagNumber(4)
  ForkAdmission get fork => $_getN(3);
  @$pb.TagNumber(4)
  set fork(ForkAdmission value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasFork() => $_has(3);
  @$pb.TagNumber(4)
  void clearFork() => $_clearField(4);
  @$pb.TagNumber(4)
  ForkAdmission ensureFork() => $_ensure(3);

  @$pb.TagNumber(5)
  MutationAdmission get suspend => $_getN(4);
  @$pb.TagNumber(5)
  set suspend(MutationAdmission value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasSuspend() => $_has(4);
  @$pb.TagNumber(5)
  void clearSuspend() => $_clearField(5);
  @$pb.TagNumber(5)
  MutationAdmission ensureSuspend() => $_ensure(4);

  @$pb.TagNumber(6)
  MutationAdmission get wake => $_getN(5);
  @$pb.TagNumber(6)
  set wake(MutationAdmission value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasWake() => $_has(5);
  @$pb.TagNumber(6)
  void clearWake() => $_clearField(6);
  @$pb.TagNumber(6)
  MutationAdmission ensureWake() => $_ensure(5);

  @$pb.TagNumber(7)
  MutationAdmission get destroyMachine => $_getN(6);
  @$pb.TagNumber(7)
  set destroyMachine(MutationAdmission value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasDestroyMachine() => $_has(6);
  @$pb.TagNumber(7)
  void clearDestroyMachine() => $_clearField(7);
  @$pb.TagNumber(7)
  MutationAdmission ensureDestroyMachine() => $_ensure(6);

  @$pb.TagNumber(8)
  PolicyAdmission get setSuspensionPolicy => $_getN(7);
  @$pb.TagNumber(8)
  set setSuspensionPolicy(PolicyAdmission value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasSetSuspensionPolicy() => $_has(7);
  @$pb.TagNumber(8)
  void clearSetSuspensionPolicy() => $_clearField(8);
  @$pb.TagNumber(8)
  PolicyAdmission ensureSetSuspensionPolicy() => $_ensure(7);

  @$pb.TagNumber(9)
  MutationAdmission get destroyCheckpoint => $_getN(8);
  @$pb.TagNumber(9)
  set destroyCheckpoint(MutationAdmission value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasDestroyCheckpoint() => $_has(8);
  @$pb.TagNumber(9)
  void clearDestroyCheckpoint() => $_clearField(9);
  @$pb.TagNumber(9)
  MutationAdmission ensureDestroyCheckpoint() => $_ensure(8);

  @$pb.TagNumber(10)
  ForkMachineAdmission get forkMachine => $_getN(9);
  @$pb.TagNumber(10)
  set forkMachine(ForkMachineAdmission value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasForkMachine() => $_has(9);
  @$pb.TagNumber(10)
  void clearForkMachine() => $_clearField(10);
  @$pb.TagNumber(10)
  ForkMachineAdmission ensureForkMachine() => $_ensure(9);
}

/// Terminal simulator result. Unlike an admission, this contains the checked
/// observations produced after the operation has completed.
class ForkedMachines extends $pb.GeneratedMessage {
  factory ForkedMachines({
    $core.Iterable<MachineState>? machines,
  }) {
    final result = ForkedMachines._();
    if (machines != null) result.machines.addAll(machines);
    return result;
  }

  ForkedMachines._();

  factory ForkedMachines.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkedMachines()..mergeFromBuffer(data, registry);
  factory ForkedMachines.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkedMachines()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkedMachines',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ForkedMachines.$_createMessage)
    ..pPM<MachineState>(1, _omitFieldNames ? '' : 'machines',
        subBuilder: MachineState.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkedMachines clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkedMachines copyWith(void Function(ForkedMachines) updates) =>
      super.copyWith((message) => updates(message as ForkedMachines))
          as ForkedMachines;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkedMachines() / ForkedMachines.new instead')
  static ForkedMachines create() => ForkedMachines._();
  static $pb.GeneratedMessage $_createMessage() => ForkedMachines._();
  @$core.override
  ForkedMachines createEmptyInstance() => ForkedMachines._();
  @$core.pragma('dart2js:noInline')
  static ForkedMachines getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ForkedMachines>(
          ForkedMachines.$_createMessage);
  static ForkedMachines? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<MachineState> get machines => $_getList(0);
}

class ForkedLiveMachines extends $pb.GeneratedMessage {
  factory ForkedLiveMachines({
    MachineId? source,
    ForkFidelity? fidelity,
    $core.Iterable<MachineState>? children,
  }) {
    final result = ForkedLiveMachines._();
    if (source != null) result.source = source;
    if (fidelity != null) result.fidelity = fidelity;
    if (children != null) result.children.addAll(children);
    return result;
  }

  ForkedLiveMachines._();

  factory ForkedLiveMachines.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkedLiveMachines()..mergeFromBuffer(data, registry);
  factory ForkedLiveMachines.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkedLiveMachines()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkedLiveMachines',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: ForkedLiveMachines.$_createMessage)
    ..aOM<MachineId>(1, _omitFieldNames ? '' : 'source',
        subBuilder: MachineId.$_createMessage)
    ..aE<ForkFidelity>(2, _omitFieldNames ? '' : 'fidelity',
        enumValues: ForkFidelity.values)
    ..pPM<MachineState>(3, _omitFieldNames ? '' : 'children',
        subBuilder: MachineState.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkedLiveMachines clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkedLiveMachines copyWith(void Function(ForkedLiveMachines) updates) =>
      super.copyWith((message) => updates(message as ForkedLiveMachines))
          as ForkedLiveMachines;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ForkedLiveMachines() / ForkedLiveMachines.new instead')
  static ForkedLiveMachines create() => ForkedLiveMachines._();
  static $pb.GeneratedMessage $_createMessage() => ForkedLiveMachines._();
  @$core.override
  ForkedLiveMachines createEmptyInstance() => ForkedLiveMachines._();
  @$core.pragma('dart2js:noInline')
  static ForkedLiveMachines getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ForkedLiveMachines>(
          ForkedLiveMachines.$_createMessage);
  static ForkedLiveMachines? _defaultInstance;

  @$pb.TagNumber(1)
  MachineId get source => $_getN(0);
  @$pb.TagNumber(1)
  set source(MachineId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineId ensureSource() => $_ensure(0);

  @$pb.TagNumber(2)
  ForkFidelity get fidelity => $_getN(1);
  @$pb.TagNumber(2)
  set fidelity(ForkFidelity value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasFidelity() => $_has(1);
  @$pb.TagNumber(2)
  void clearFidelity() => $_clearField(2);

  @$pb.TagNumber(3)
  $pb.PbList<MachineState> get children => $_getList(2);
}

class PolicySet extends $pb.GeneratedMessage {
  factory PolicySet({
    MachineId? machine,
    SuspensionPolicy? policy,
  }) {
    final result = PolicySet._();
    if (machine != null) result.machine = machine;
    if (policy != null) result.policy = policy;
    return result;
  }

  PolicySet._();

  factory PolicySet.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PolicySet()..mergeFromBuffer(data, registry);
  factory PolicySet.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PolicySet()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PolicySet',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: PolicySet.$_createMessage)
    ..aOM<MachineId>(1, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..aOM<SuspensionPolicy>(2, _omitFieldNames ? '' : 'policy',
        subBuilder: SuspensionPolicy.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PolicySet clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PolicySet copyWith(void Function(PolicySet) updates) =>
      super.copyWith((message) => updates(message as PolicySet)) as PolicySet;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use PolicySet() / PolicySet.new instead')
  static PolicySet create() => PolicySet._();
  static $pb.GeneratedMessage $_createMessage() => PolicySet._();
  @$core.override
  PolicySet createEmptyInstance() => PolicySet._();
  @$core.pragma('dart2js:noInline')
  static PolicySet getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PolicySet>(PolicySet.$_createMessage);
  static PolicySet? _defaultInstance;

  @$pb.TagNumber(1)
  MachineId get machine => $_getN(0);
  @$pb.TagNumber(1)
  set machine(MachineId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasMachine() => $_has(0);
  @$pb.TagNumber(1)
  void clearMachine() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineId ensureMachine() => $_ensure(0);

  @$pb.TagNumber(2)
  SuspensionPolicy get policy => $_getN(1);
  @$pb.TagNumber(2)
  set policy(SuspensionPolicy value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasPolicy() => $_has(1);
  @$pb.TagNumber(2)
  void clearPolicy() => $_clearField(2);
  @$pb.TagNumber(2)
  SuspensionPolicy ensurePolicy() => $_ensure(1);
}

enum MutationOutcome_Result {
  created,
  checkpointed,
  forked,
  suspended,
  woken,
  suspensionPolicySet,
  machineDestroyed,
  checkpointDestroyed,
  machineForked,
  notSet
}

class MutationOutcome extends $pb.GeneratedMessage {
  factory MutationOutcome({
    MachineState? created,
    CheckpointState? checkpointed,
    ForkedMachines? forked,
    MachineId? suspended,
    MachineId? woken,
    PolicySet? suspensionPolicySet,
    MachineId? machineDestroyed,
    CheckpointId? checkpointDestroyed,
    ForkedLiveMachines? machineForked,
  }) {
    final result = MutationOutcome._();
    if (created != null) result.created = created;
    if (checkpointed != null) result.checkpointed = checkpointed;
    if (forked != null) result.forked = forked;
    if (suspended != null) result.suspended = suspended;
    if (woken != null) result.woken = woken;
    if (suspensionPolicySet != null)
      result.suspensionPolicySet = suspensionPolicySet;
    if (machineDestroyed != null) result.machineDestroyed = machineDestroyed;
    if (checkpointDestroyed != null)
      result.checkpointDestroyed = checkpointDestroyed;
    if (machineForked != null) result.machineForked = machineForked;
    return result;
  }

  MutationOutcome._();

  factory MutationOutcome.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationOutcome()..mergeFromBuffer(data, registry);
  factory MutationOutcome.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationOutcome()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, MutationOutcome_Result>
      _MutationOutcome_ResultByTag = {
    1: MutationOutcome_Result.created,
    2: MutationOutcome_Result.checkpointed,
    3: MutationOutcome_Result.forked,
    4: MutationOutcome_Result.suspended,
    5: MutationOutcome_Result.woken,
    6: MutationOutcome_Result.suspensionPolicySet,
    7: MutationOutcome_Result.machineDestroyed,
    8: MutationOutcome_Result.checkpointDestroyed,
    9: MutationOutcome_Result.machineForked,
    0: MutationOutcome_Result.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MutationOutcome',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MutationOutcome.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5, 6, 7, 8, 9])
    ..aOM<MachineState>(1, _omitFieldNames ? '' : 'created',
        subBuilder: MachineState.$_createMessage)
    ..aOM<CheckpointState>(2, _omitFieldNames ? '' : 'checkpointed',
        subBuilder: CheckpointState.$_createMessage)
    ..aOM<ForkedMachines>(3, _omitFieldNames ? '' : 'forked',
        subBuilder: ForkedMachines.$_createMessage)
    ..aOM<MachineId>(4, _omitFieldNames ? '' : 'suspended',
        subBuilder: MachineId.$_createMessage)
    ..aOM<MachineId>(5, _omitFieldNames ? '' : 'woken',
        subBuilder: MachineId.$_createMessage)
    ..aOM<PolicySet>(6, _omitFieldNames ? '' : 'suspensionPolicySet',
        subBuilder: PolicySet.$_createMessage)
    ..aOM<MachineId>(7, _omitFieldNames ? '' : 'machineDestroyed',
        subBuilder: MachineId.$_createMessage)
    ..aOM<CheckpointId>(8, _omitFieldNames ? '' : 'checkpointDestroyed',
        subBuilder: CheckpointId.$_createMessage)
    ..aOM<ForkedLiveMachines>(9, _omitFieldNames ? '' : 'machineForked',
        subBuilder: ForkedLiveMachines.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationOutcome clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationOutcome copyWith(void Function(MutationOutcome) updates) =>
      super.copyWith((message) => updates(message as MutationOutcome))
          as MutationOutcome;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MutationOutcome() / MutationOutcome.new instead')
  static MutationOutcome create() => MutationOutcome._();
  static $pb.GeneratedMessage $_createMessage() => MutationOutcome._();
  @$core.override
  MutationOutcome createEmptyInstance() => MutationOutcome._();
  @$core.pragma('dart2js:noInline')
  static MutationOutcome getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MutationOutcome>(
          MutationOutcome.$_createMessage);
  static MutationOutcome? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  @$pb.TagNumber(8)
  @$pb.TagNumber(9)
  MutationOutcome_Result whichResult() =>
      _MutationOutcome_ResultByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  @$pb.TagNumber(8)
  @$pb.TagNumber(9)
  void clearResult() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  MachineState get created => $_getN(0);
  @$pb.TagNumber(1)
  set created(MachineState value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCreated() => $_has(0);
  @$pb.TagNumber(1)
  void clearCreated() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineState ensureCreated() => $_ensure(0);

  @$pb.TagNumber(2)
  CheckpointState get checkpointed => $_getN(1);
  @$pb.TagNumber(2)
  set checkpointed(CheckpointState value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasCheckpointed() => $_has(1);
  @$pb.TagNumber(2)
  void clearCheckpointed() => $_clearField(2);
  @$pb.TagNumber(2)
  CheckpointState ensureCheckpointed() => $_ensure(1);

  @$pb.TagNumber(3)
  ForkedMachines get forked => $_getN(2);
  @$pb.TagNumber(3)
  set forked(ForkedMachines value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasForked() => $_has(2);
  @$pb.TagNumber(3)
  void clearForked() => $_clearField(3);
  @$pb.TagNumber(3)
  ForkedMachines ensureForked() => $_ensure(2);

  @$pb.TagNumber(4)
  MachineId get suspended => $_getN(3);
  @$pb.TagNumber(4)
  set suspended(MachineId value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasSuspended() => $_has(3);
  @$pb.TagNumber(4)
  void clearSuspended() => $_clearField(4);
  @$pb.TagNumber(4)
  MachineId ensureSuspended() => $_ensure(3);

  @$pb.TagNumber(5)
  MachineId get woken => $_getN(4);
  @$pb.TagNumber(5)
  set woken(MachineId value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasWoken() => $_has(4);
  @$pb.TagNumber(5)
  void clearWoken() => $_clearField(5);
  @$pb.TagNumber(5)
  MachineId ensureWoken() => $_ensure(4);

  @$pb.TagNumber(6)
  PolicySet get suspensionPolicySet => $_getN(5);
  @$pb.TagNumber(6)
  set suspensionPolicySet(PolicySet value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasSuspensionPolicySet() => $_has(5);
  @$pb.TagNumber(6)
  void clearSuspensionPolicySet() => $_clearField(6);
  @$pb.TagNumber(6)
  PolicySet ensureSuspensionPolicySet() => $_ensure(5);

  @$pb.TagNumber(7)
  MachineId get machineDestroyed => $_getN(6);
  @$pb.TagNumber(7)
  set machineDestroyed(MachineId value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasMachineDestroyed() => $_has(6);
  @$pb.TagNumber(7)
  void clearMachineDestroyed() => $_clearField(7);
  @$pb.TagNumber(7)
  MachineId ensureMachineDestroyed() => $_ensure(6);

  @$pb.TagNumber(8)
  CheckpointId get checkpointDestroyed => $_getN(7);
  @$pb.TagNumber(8)
  set checkpointDestroyed(CheckpointId value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasCheckpointDestroyed() => $_has(7);
  @$pb.TagNumber(8)
  void clearCheckpointDestroyed() => $_clearField(8);
  @$pb.TagNumber(8)
  CheckpointId ensureCheckpointDestroyed() => $_ensure(7);

  @$pb.TagNumber(9)
  ForkedLiveMachines get machineForked => $_getN(8);
  @$pb.TagNumber(9)
  set machineForked(ForkedLiveMachines value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasMachineForked() => $_has(8);
  @$pb.TagNumber(9)
  void clearMachineForked() => $_clearField(9);
  @$pb.TagNumber(9)
  ForkedLiveMachines ensureMachineForked() => $_ensure(8);
}

class OperationPage extends $pb.GeneratedMessage {
  factory OperationPage({
    $core.Iterable<OperationState>? operations,
  }) {
    final result = OperationPage._();
    if (operations != null) result.operations.addAll(operations);
    return result;
  }

  OperationPage._();

  factory OperationPage.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationPage()..mergeFromBuffer(data, registry);
  factory OperationPage.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationPage()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OperationPage',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: OperationPage.$_createMessage)
    ..pPM<OperationState>(1, _omitFieldNames ? '' : 'operations',
        subBuilder: OperationState.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationPage clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationPage copyWith(void Function(OperationPage) updates) =>
      super.copyWith((message) => updates(message as OperationPage))
          as OperationPage;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OperationPage() / OperationPage.new instead')
  static OperationPage create() => OperationPage._();
  static $pb.GeneratedMessage $_createMessage() => OperationPage._();
  @$core.override
  OperationPage createEmptyInstance() => OperationPage._();
  @$core.pragma('dart2js:noInline')
  static OperationPage getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OperationPage>(
          OperationPage.$_createMessage);
  static OperationPage? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<OperationState> get operations => $_getList(0);
}

class MachineEvent extends $pb.GeneratedMessage {
  factory MachineEvent({
    MachineId? machine,
    $fixnum.Int64? sequence,
    $fixnum.Int64? observedAtUnixMs,
    EventKind? kind,
    MachineStatus? state,
    PressureKind? pressure,
  }) {
    final result = MachineEvent._();
    if (machine != null) result.machine = machine;
    if (sequence != null) result.sequence = sequence;
    if (observedAtUnixMs != null) result.observedAtUnixMs = observedAtUnixMs;
    if (kind != null) result.kind = kind;
    if (state != null) result.state = state;
    if (pressure != null) result.pressure = pressure;
    return result;
  }

  MachineEvent._();

  factory MachineEvent.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineEvent()..mergeFromBuffer(data, registry);
  factory MachineEvent.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MachineEvent()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MachineEvent',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: MachineEvent.$_createMessage)
    ..aOM<MachineId>(1, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'sequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'observedAtUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aE<EventKind>(4, _omitFieldNames ? '' : 'kind',
        enumValues: EventKind.values)
    ..aE<MachineStatus>(5, _omitFieldNames ? '' : 'state',
        enumValues: MachineStatus.values)
    ..aE<PressureKind>(6, _omitFieldNames ? '' : 'pressure',
        enumValues: PressureKind.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineEvent clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MachineEvent copyWith(void Function(MachineEvent) updates) =>
      super.copyWith((message) => updates(message as MachineEvent))
          as MachineEvent;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MachineEvent() / MachineEvent.new instead')
  static MachineEvent create() => MachineEvent._();
  static $pb.GeneratedMessage $_createMessage() => MachineEvent._();
  @$core.override
  MachineEvent createEmptyInstance() => MachineEvent._();
  @$core.pragma('dart2js:noInline')
  static MachineEvent getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MachineEvent>(
          MachineEvent.$_createMessage);
  static MachineEvent? _defaultInstance;

  @$pb.TagNumber(1)
  MachineId get machine => $_getN(0);
  @$pb.TagNumber(1)
  set machine(MachineId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasMachine() => $_has(0);
  @$pb.TagNumber(1)
  void clearMachine() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineId ensureMachine() => $_ensure(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get sequence => $_getI64(1);
  @$pb.TagNumber(2)
  set sequence($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSequence() => $_has(1);
  @$pb.TagNumber(2)
  void clearSequence() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get observedAtUnixMs => $_getI64(2);
  @$pb.TagNumber(3)
  set observedAtUnixMs($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasObservedAtUnixMs() => $_has(2);
  @$pb.TagNumber(3)
  void clearObservedAtUnixMs() => $_clearField(3);

  @$pb.TagNumber(4)
  EventKind get kind => $_getN(3);
  @$pb.TagNumber(4)
  set kind(EventKind value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasKind() => $_has(3);
  @$pb.TagNumber(4)
  void clearKind() => $_clearField(4);

  @$pb.TagNumber(5)
  MachineStatus get state => $_getN(4);
  @$pb.TagNumber(5)
  set state(MachineStatus value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasState() => $_has(4);
  @$pb.TagNumber(5)
  void clearState() => $_clearField(5);

  @$pb.TagNumber(6)
  PressureKind get pressure => $_getN(5);
  @$pb.TagNumber(6)
  set pressure(PressureKind value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasPressure() => $_has(5);
  @$pb.TagNumber(6)
  void clearPressure() => $_clearField(6);
}

class EventsRequest extends $pb.GeneratedMessage {
  factory EventsRequest({
    ProtocolVersion? protocol,
    MachineId? machine,
    $fixnum.Int64? afterSequence,
    $core.int? limit,
  }) {
    final result = EventsRequest._();
    if (protocol != null) result.protocol = protocol;
    if (machine != null) result.machine = machine;
    if (afterSequence != null) result.afterSequence = afterSequence;
    if (limit != null) result.limit = limit;
    return result;
  }

  EventsRequest._();

  factory EventsRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EventsRequest()..mergeFromBuffer(data, registry);
  factory EventsRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EventsRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EventsRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: EventsRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<MachineId>(2, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'afterSequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aI(4, _omitFieldNames ? '' : 'limit', fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EventsRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EventsRequest copyWith(void Function(EventsRequest) updates) =>
      super.copyWith((message) => updates(message as EventsRequest))
          as EventsRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EventsRequest() / EventsRequest.new instead')
  static EventsRequest create() => EventsRequest._();
  static $pb.GeneratedMessage $_createMessage() => EventsRequest._();
  @$core.override
  EventsRequest createEmptyInstance() => EventsRequest._();
  @$core.pragma('dart2js:noInline')
  static EventsRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<EventsRequest>(
          EventsRequest.$_createMessage);
  static EventsRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineId get machine => $_getN(1);
  @$pb.TagNumber(2)
  set machine(MachineId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMachine() => $_has(1);
  @$pb.TagNumber(2)
  void clearMachine() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineId ensureMachine() => $_ensure(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get afterSequence => $_getI64(2);
  @$pb.TagNumber(3)
  set afterSequence($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasAfterSequence() => $_has(2);
  @$pb.TagNumber(3)
  void clearAfterSequence() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.int get limit => $_getIZ(3);
  @$pb.TagNumber(4)
  set limit($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasLimit() => $_has(3);
  @$pb.TagNumber(4)
  void clearLimit() => $_clearField(4);
}

class EventPage extends $pb.GeneratedMessage {
  factory EventPage({
    $core.Iterable<MachineEvent>? events,
    $fixnum.Int64? nextSequence,
  }) {
    final result = EventPage._();
    if (events != null) result.events.addAll(events);
    if (nextSequence != null) result.nextSequence = nextSequence;
    return result;
  }

  EventPage._();

  factory EventPage.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EventPage()..mergeFromBuffer(data, registry);
  factory EventPage.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      EventPage()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'EventPage',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: EventPage.$_createMessage)
    ..pPM<MachineEvent>(1, _omitFieldNames ? '' : 'events',
        subBuilder: MachineEvent.$_createMessage)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'nextSequence', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EventPage clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  EventPage copyWith(void Function(EventPage) updates) =>
      super.copyWith((message) => updates(message as EventPage)) as EventPage;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use EventPage() / EventPage.new instead')
  static EventPage create() => EventPage._();
  static $pb.GeneratedMessage $_createMessage() => EventPage._();
  @$core.override
  EventPage createEmptyInstance() => EventPage._();
  @$core.pragma('dart2js:noInline')
  static EventPage getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<EventPage>(EventPage.$_createMessage);
  static EventPage? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<MachineEvent> get events => $_getList(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get nextSequence => $_getI64(1);
  @$pb.TagNumber(2)
  set nextSequence($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasNextSequence() => $_has(1);
  @$pb.TagNumber(2)
  void clearNextSequence() => $_clearField(2);
}

class UsageRequest extends $pb.GeneratedMessage {
  factory UsageRequest({
    ProtocolVersion? protocol,
    MachineId? machine,
    $fixnum.Int64? startUnixMs,
    $fixnum.Int64? endUnixMs,
  }) {
    final result = UsageRequest._();
    if (protocol != null) result.protocol = protocol;
    if (machine != null) result.machine = machine;
    if (startUnixMs != null) result.startUnixMs = startUnixMs;
    if (endUnixMs != null) result.endUnixMs = endUnixMs;
    return result;
  }

  UsageRequest._();

  factory UsageRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UsageRequest()..mergeFromBuffer(data, registry);
  factory UsageRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      UsageRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'UsageRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: UsageRequest.$_createMessage)
    ..aOM<ProtocolVersion>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolVersion.$_createMessage)
    ..aOM<MachineId>(2, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'startUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'endUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UsageRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  UsageRequest copyWith(void Function(UsageRequest) updates) =>
      super.copyWith((message) => updates(message as UsageRequest))
          as UsageRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use UsageRequest() / UsageRequest.new instead')
  static UsageRequest create() => UsageRequest._();
  static $pb.GeneratedMessage $_createMessage() => UsageRequest._();
  @$core.override
  UsageRequest createEmptyInstance() => UsageRequest._();
  @$core.pragma('dart2js:noInline')
  static UsageRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<UsageRequest>(
          UsageRequest.$_createMessage);
  static UsageRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolVersion get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolVersion value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolVersion ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  MachineId get machine => $_getN(1);
  @$pb.TagNumber(2)
  set machine(MachineId value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMachine() => $_has(1);
  @$pb.TagNumber(2)
  void clearMachine() => $_clearField(2);
  @$pb.TagNumber(2)
  MachineId ensureMachine() => $_ensure(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get startUnixMs => $_getI64(2);
  @$pb.TagNumber(3)
  set startUnixMs($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasStartUnixMs() => $_has(2);
  @$pb.TagNumber(3)
  void clearStartUnixMs() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get endUnixMs => $_getI64(3);
  @$pb.TagNumber(4)
  set endUnixMs($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasEndUnixMs() => $_has(3);
  @$pb.TagNumber(4)
  void clearEndUnixMs() => $_clearField(4);
}

class UsageReceipt extends $pb.GeneratedMessage {
  factory UsageReceipt({
    MachineId? machine,
    $fixnum.Int64? startUnixMs,
    $fixnum.Int64? endUnixMs,
    $fixnum.Int64? elasticCpuNs,
    $fixnum.Int64? dedicatedCpuNs,
    $fixnum.Int64? privateResidentByteSeconds,
    $fixnum.Int64? durablePrivateBytes,
    $fixnum.Int64? egressBytes,
    $core.List<$core.int>? receipt,
    $core.List<$core.int>? lineageReceiptSha256,
  }) {
    final result = UsageReceipt._();
    if (machine != null) result.machine = machine;
    if (startUnixMs != null) result.startUnixMs = startUnixMs;
    if (endUnixMs != null) result.endUnixMs = endUnixMs;
    if (elasticCpuNs != null) result.elasticCpuNs = elasticCpuNs;
    if (dedicatedCpuNs != null) result.dedicatedCpuNs = dedicatedCpuNs;
    if (privateResidentByteSeconds != null)
      result.privateResidentByteSeconds = privateResidentByteSeconds;
    if (durablePrivateBytes != null)
      result.durablePrivateBytes = durablePrivateBytes;
    if (egressBytes != null) result.egressBytes = egressBytes;
    if (receipt != null) result.receipt = receipt;
    if (lineageReceiptSha256 != null)
      result.lineageReceiptSha256 = lineageReceiptSha256;
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
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.machines.v1'),
      createEmptyInstance: UsageReceipt.$_createMessage)
    ..aOM<MachineId>(1, _omitFieldNames ? '' : 'machine',
        subBuilder: MachineId.$_createMessage)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'startUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'endUnixMs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'elasticCpuNs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'dedicatedCpuNs', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(6, _omitFieldNames ? '' : 'privateResidentByteSeconds',
        $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        7, _omitFieldNames ? '' : 'durablePrivateBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        9, _omitFieldNames ? '' : 'egressBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        10, _omitFieldNames ? '' : 'receipt', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        11, _omitFieldNames ? '' : 'lineageReceiptSha256', $pb.PbFieldType.OY)
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
  MachineId get machine => $_getN(0);
  @$pb.TagNumber(1)
  set machine(MachineId value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasMachine() => $_has(0);
  @$pb.TagNumber(1)
  void clearMachine() => $_clearField(1);
  @$pb.TagNumber(1)
  MachineId ensureMachine() => $_ensure(0);

  @$pb.TagNumber(2)
  $fixnum.Int64 get startUnixMs => $_getI64(1);
  @$pb.TagNumber(2)
  set startUnixMs($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasStartUnixMs() => $_has(1);
  @$pb.TagNumber(2)
  void clearStartUnixMs() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get endUnixMs => $_getI64(2);
  @$pb.TagNumber(3)
  set endUnixMs($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasEndUnixMs() => $_has(2);
  @$pb.TagNumber(3)
  void clearEndUnixMs() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get elasticCpuNs => $_getI64(3);
  @$pb.TagNumber(4)
  set elasticCpuNs($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasElasticCpuNs() => $_has(3);
  @$pb.TagNumber(4)
  void clearElasticCpuNs() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get dedicatedCpuNs => $_getI64(4);
  @$pb.TagNumber(5)
  set dedicatedCpuNs($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasDedicatedCpuNs() => $_has(4);
  @$pb.TagNumber(5)
  void clearDedicatedCpuNs() => $_clearField(5);

  @$pb.TagNumber(6)
  $fixnum.Int64 get privateResidentByteSeconds => $_getI64(5);
  @$pb.TagNumber(6)
  set privateResidentByteSeconds($fixnum.Int64 value) => $_setInt64(5, value);
  @$pb.TagNumber(6)
  $core.bool hasPrivateResidentByteSeconds() => $_has(5);
  @$pb.TagNumber(6)
  void clearPrivateResidentByteSeconds() => $_clearField(6);

  @$pb.TagNumber(7)
  $fixnum.Int64 get durablePrivateBytes => $_getI64(6);
  @$pb.TagNumber(7)
  set durablePrivateBytes($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasDurablePrivateBytes() => $_has(6);
  @$pb.TagNumber(7)
  void clearDurablePrivateBytes() => $_clearField(7);

  @$pb.TagNumber(9)
  $fixnum.Int64 get egressBytes => $_getI64(7);
  @$pb.TagNumber(9)
  set egressBytes($fixnum.Int64 value) => $_setInt64(7, value);
  @$pb.TagNumber(9)
  $core.bool hasEgressBytes() => $_has(7);
  @$pb.TagNumber(9)
  void clearEgressBytes() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.List<$core.int> get receipt => $_getN(8);
  @$pb.TagNumber(10)
  set receipt($core.List<$core.int> value) => $_setBytes(8, value);
  @$pb.TagNumber(10)
  $core.bool hasReceipt() => $_has(8);
  @$pb.TagNumber(10)
  void clearReceipt() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.List<$core.int> get lineageReceiptSha256 => $_getN(9);
  @$pb.TagNumber(11)
  set lineageReceiptSha256($core.List<$core.int> value) => $_setBytes(9, value);
  @$pb.TagNumber(11)
  $core.bool hasLineageReceiptSha256() => $_has(9);
  @$pb.TagNumber(11)
  void clearLineageReceiptSha256() => $_clearField(11);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
