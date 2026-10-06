// This is a generated file - do not edit.
//
// Generated from protocol/v1/protocol.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

class ProtocolIdentity extends $pb.GeneratedMessage {
  factory ProtocolIdentity({
    $core.String? version,
    $core.String? descriptorDigest,
  }) {
    final result = ProtocolIdentity._();
    if (version != null) result.version = version;
    if (descriptorDigest != null) result.descriptorDigest = descriptorDigest;
    return result;
  }

  ProtocolIdentity._();

  factory ProtocolIdentity.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProtocolIdentity()..mergeFromBuffer(data, registry);
  factory ProtocolIdentity.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ProtocolIdentity()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ProtocolIdentity',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.protocol.v1'),
      createEmptyInstance: ProtocolIdentity.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'version')
    ..aOS(2, _omitFieldNames ? '' : 'descriptorDigest')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtocolIdentity clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ProtocolIdentity copyWith(void Function(ProtocolIdentity) updates) =>
      super.copyWith((message) => updates(message as ProtocolIdentity))
          as ProtocolIdentity;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ProtocolIdentity() / ProtocolIdentity.new instead')
  static ProtocolIdentity create() => ProtocolIdentity._();
  static $pb.GeneratedMessage $_createMessage() => ProtocolIdentity._();
  @$core.override
  ProtocolIdentity createEmptyInstance() => ProtocolIdentity._();
  @$core.pragma('dart2js:noInline')
  static ProtocolIdentity getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ProtocolIdentity>(
          ProtocolIdentity.$_createMessage);
  static ProtocolIdentity? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get version => $_getSZ(0);
  @$pb.TagNumber(1)
  set version($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasVersion() => $_has(0);
  @$pb.TagNumber(1)
  void clearVersion() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get descriptorDigest => $_getSZ(1);
  @$pb.TagNumber(2)
  set descriptorDigest($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDescriptorDigest() => $_has(1);
  @$pb.TagNumber(2)
  void clearDescriptorDigest() => $_clearField(2);
}

class Capability extends $pb.GeneratedMessage {
  factory Capability({
    $core.String? name,
    $core.String? version,
  }) {
    final result = Capability._();
    if (name != null) result.name = name;
    if (version != null) result.version = version;
    return result;
  }

  Capability._();

  factory Capability.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Capability()..mergeFromBuffer(data, registry);
  factory Capability.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Capability()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Capability',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.protocol.v1'),
      createEmptyInstance: Capability.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aOS(2, _omitFieldNames ? '' : 'version')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Capability clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Capability copyWith(void Function(Capability) updates) =>
      super.copyWith((message) => updates(message as Capability)) as Capability;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Capability() / Capability.new instead')
  static Capability create() => Capability._();
  static $pb.GeneratedMessage $_createMessage() => Capability._();
  @$core.override
  Capability createEmptyInstance() => Capability._();
  @$core.pragma('dart2js:noInline')
  static Capability getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Capability>(Capability.$_createMessage);
  static Capability? _defaultInstance;

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
}

class CapabilitySet extends $pb.GeneratedMessage {
  factory CapabilitySet({
    $core.Iterable<Capability>? capabilities,
  }) {
    final result = CapabilitySet._();
    if (capabilities != null) result.capabilities.addAll(capabilities);
    return result;
  }

  CapabilitySet._();

  factory CapabilitySet.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CapabilitySet()..mergeFromBuffer(data, registry);
  factory CapabilitySet.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CapabilitySet()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CapabilitySet',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.protocol.v1'),
      createEmptyInstance: CapabilitySet.$_createMessage)
    ..pPM<Capability>(1, _omitFieldNames ? '' : 'capabilities',
        subBuilder: Capability.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CapabilitySet clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CapabilitySet copyWith(void Function(CapabilitySet) updates) =>
      super.copyWith((message) => updates(message as CapabilitySet))
          as CapabilitySet;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CapabilitySet() / CapabilitySet.new instead')
  static CapabilitySet create() => CapabilitySet._();
  static $pb.GeneratedMessage $_createMessage() => CapabilitySet._();
  @$core.override
  CapabilitySet createEmptyInstance() => CapabilitySet._();
  @$core.pragma('dart2js:noInline')
  static CapabilitySet getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CapabilitySet>(
          CapabilitySet.$_createMessage);
  static CapabilitySet? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<Capability> get capabilities => $_getList(0);
}

class HandshakeRequest extends $pb.GeneratedMessage {
  factory HandshakeRequest({
    ProtocolIdentity? protocol,
    CapabilitySet? required,
  }) {
    final result = HandshakeRequest._();
    if (protocol != null) result.protocol = protocol;
    if (required != null) result.required = required;
    return result;
  }

  HandshakeRequest._();

  factory HandshakeRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HandshakeRequest()..mergeFromBuffer(data, registry);
  factory HandshakeRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HandshakeRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'HandshakeRequest',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.protocol.v1'),
      createEmptyInstance: HandshakeRequest.$_createMessage)
    ..aOM<ProtocolIdentity>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolIdentity.$_createMessage)
    ..aOM<CapabilitySet>(2, _omitFieldNames ? '' : 'required',
        subBuilder: CapabilitySet.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HandshakeRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HandshakeRequest copyWith(void Function(HandshakeRequest) updates) =>
      super.copyWith((message) => updates(message as HandshakeRequest))
          as HandshakeRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use HandshakeRequest() / HandshakeRequest.new instead')
  static HandshakeRequest create() => HandshakeRequest._();
  static $pb.GeneratedMessage $_createMessage() => HandshakeRequest._();
  @$core.override
  HandshakeRequest createEmptyInstance() => HandshakeRequest._();
  @$core.pragma('dart2js:noInline')
  static HandshakeRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<HandshakeRequest>(
          HandshakeRequest.$_createMessage);
  static HandshakeRequest? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolIdentity get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolIdentity ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  CapabilitySet get required => $_getN(1);
  @$pb.TagNumber(2)
  set required(CapabilitySet value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasRequired() => $_has(1);
  @$pb.TagNumber(2)
  void clearRequired() => $_clearField(2);
  @$pb.TagNumber(2)
  CapabilitySet ensureRequired() => $_ensure(1);
}

class HandshakeResponse extends $pb.GeneratedMessage {
  factory HandshakeResponse({
    ProtocolIdentity? protocol,
    CapabilitySet? supported,
  }) {
    final result = HandshakeResponse._();
    if (protocol != null) result.protocol = protocol;
    if (supported != null) result.supported = supported;
    return result;
  }

  HandshakeResponse._();

  factory HandshakeResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HandshakeResponse()..mergeFromBuffer(data, registry);
  factory HandshakeResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HandshakeResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'HandshakeResponse',
      package:
          const $pb.PackageName(_omitMessageNames ? '' : 'acyclic.protocol.v1'),
      createEmptyInstance: HandshakeResponse.$_createMessage)
    ..aOM<ProtocolIdentity>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: ProtocolIdentity.$_createMessage)
    ..aOM<CapabilitySet>(2, _omitFieldNames ? '' : 'supported',
        subBuilder: CapabilitySet.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HandshakeResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HandshakeResponse copyWith(void Function(HandshakeResponse) updates) =>
      super.copyWith((message) => updates(message as HandshakeResponse))
          as HandshakeResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use HandshakeResponse() / HandshakeResponse.new instead')
  static HandshakeResponse create() => HandshakeResponse._();
  static $pb.GeneratedMessage $_createMessage() => HandshakeResponse._();
  @$core.override
  HandshakeResponse createEmptyInstance() => HandshakeResponse._();
  @$core.pragma('dart2js:noInline')
  static HandshakeResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<HandshakeResponse>(
          HandshakeResponse.$_createMessage);
  static HandshakeResponse? _defaultInstance;

  @$pb.TagNumber(1)
  ProtocolIdentity get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol(ProtocolIdentity value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  ProtocolIdentity ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  CapabilitySet get supported => $_getN(1);
  @$pb.TagNumber(2)
  set supported(CapabilitySet value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSupported() => $_has(1);
  @$pb.TagNumber(2)
  void clearSupported() => $_clearField(2);
  @$pb.TagNumber(2)
  CapabilitySet ensureSupported() => $_ensure(1);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
