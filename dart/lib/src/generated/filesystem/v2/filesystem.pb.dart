// This is a generated file - do not edit.
//
// Generated from filesystem/v2/filesystem.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import '../../protocol/v1/protocol.pb.dart' as $1;
import 'filesystem.pbenum.dart';

export 'package:protobuf/protobuf.dart' show GeneratedMessageGenericExtensions;

export 'filesystem.pbenum.dart';

class WorkspaceRef extends $pb.GeneratedMessage {
  factory WorkspaceRef({
    $core.List<$core.int>? workspaceId,
    $core.String? name,
  }) {
    final result = WorkspaceRef._();
    if (workspaceId != null) result.workspaceId = workspaceId;
    if (name != null) result.name = name;
    return result;
  }

  WorkspaceRef._();

  factory WorkspaceRef.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceRef()..mergeFromBuffer(data, registry);
  factory WorkspaceRef.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceRef()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkspaceRef',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: WorkspaceRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'workspaceId', $pb.PbFieldType.OY)
    ..aOS(2, _omitFieldNames ? '' : 'name')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceRef clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceRef copyWith(void Function(WorkspaceRef) updates) =>
      super.copyWith((message) => updates(message as WorkspaceRef))
          as WorkspaceRef;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WorkspaceRef() / WorkspaceRef.new instead')
  static WorkspaceRef create() => WorkspaceRef._();
  static $pb.GeneratedMessage $_createMessage() => WorkspaceRef._();
  @$core.override
  WorkspaceRef createEmptyInstance() => WorkspaceRef._();
  @$core.pragma('dart2js:noInline')
  static WorkspaceRef getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<WorkspaceRef>(
          WorkspaceRef.$_createMessage);
  static WorkspaceRef? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get workspaceId => $_getN(0);
  @$pb.TagNumber(1)
  set workspaceId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspaceId() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspaceId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get name => $_getSZ(1);
  @$pb.TagNumber(2)
  set name($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => $_clearField(2);
}

class GenerationRef extends $pb.GeneratedMessage {
  factory GenerationRef({
    WorkspaceRef? workspace,
    $core.List<$core.int>? generationId,
  }) {
    final result = GenerationRef._();
    if (workspace != null) result.workspace = workspace;
    if (generationId != null) result.generationId = generationId;
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
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: GenerationRef.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'generationId', $pb.PbFieldType.OY)
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
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get generationId => $_getN(1);
  @$pb.TagNumber(2)
  set generationId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasGenerationId() => $_has(1);
  @$pb.TagNumber(2)
  void clearGenerationId() => $_clearField(2);
}

/// Durable context control-plane records. Paths are UTF-8 native absolute paths;
/// file contents and credentials never travel in these records. The repeated
/// roots are ordered by root_id, and duplicate IDs are invalid.
class WorkspaceContextRoot extends $pb.GeneratedMessage {
  factory WorkspaceContextRoot({
    $core.List<$core.int>? rootId,
    $core.String? sourcePath,
    $core.List<$core.int>? workspaceId,
    $core.String? workspaceName,
    $core.List<$core.int>? parentWorkspaceId,
    $core.String? mountPath,
  }) {
    final result = WorkspaceContextRoot._();
    if (rootId != null) result.rootId = rootId;
    if (sourcePath != null) result.sourcePath = sourcePath;
    if (workspaceId != null) result.workspaceId = workspaceId;
    if (workspaceName != null) result.workspaceName = workspaceName;
    if (parentWorkspaceId != null) result.parentWorkspaceId = parentWorkspaceId;
    if (mountPath != null) result.mountPath = mountPath;
    return result;
  }

  WorkspaceContextRoot._();

  factory WorkspaceContextRoot.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceContextRoot()..mergeFromBuffer(data, registry);
  factory WorkspaceContextRoot.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceContextRoot()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkspaceContextRoot',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: WorkspaceContextRoot.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'rootId', $pb.PbFieldType.OY)
    ..aOS(2, _omitFieldNames ? '' : 'sourcePath')
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'workspaceId', $pb.PbFieldType.OY)
    ..aOS(4, _omitFieldNames ? '' : 'workspaceName')
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'parentWorkspaceId', $pb.PbFieldType.OY)
    ..aOS(6, _omitFieldNames ? '' : 'mountPath')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceContextRoot clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceContextRoot copyWith(void Function(WorkspaceContextRoot) updates) =>
      super.copyWith((message) => updates(message as WorkspaceContextRoot))
          as WorkspaceContextRoot;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use WorkspaceContextRoot() / WorkspaceContextRoot.new instead')
  static WorkspaceContextRoot create() => WorkspaceContextRoot._();
  static $pb.GeneratedMessage $_createMessage() => WorkspaceContextRoot._();
  @$core.override
  WorkspaceContextRoot createEmptyInstance() => WorkspaceContextRoot._();
  @$core.pragma('dart2js:noInline')
  static WorkspaceContextRoot getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<WorkspaceContextRoot>(
          WorkspaceContextRoot.$_createMessage);
  static WorkspaceContextRoot? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get rootId => $_getN(0);
  @$pb.TagNumber(1)
  set rootId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasRootId() => $_has(0);
  @$pb.TagNumber(1)
  void clearRootId() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get sourcePath => $_getSZ(1);
  @$pb.TagNumber(2)
  set sourcePath($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSourcePath() => $_has(1);
  @$pb.TagNumber(2)
  void clearSourcePath() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get workspaceId => $_getN(2);
  @$pb.TagNumber(3)
  set workspaceId($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasWorkspaceId() => $_has(2);
  @$pb.TagNumber(3)
  void clearWorkspaceId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get workspaceName => $_getSZ(3);
  @$pb.TagNumber(4)
  set workspaceName($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasWorkspaceName() => $_has(3);
  @$pb.TagNumber(4)
  void clearWorkspaceName() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get parentWorkspaceId => $_getN(4);
  @$pb.TagNumber(5)
  set parentWorkspaceId($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasParentWorkspaceId() => $_has(4);
  @$pb.TagNumber(5)
  void clearParentWorkspaceId() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.String get mountPath => $_getSZ(5);
  @$pb.TagNumber(6)
  set mountPath($core.String value) => $_setString(5, value);
  @$pb.TagNumber(6)
  $core.bool hasMountPath() => $_has(5);
  @$pb.TagNumber(6)
  void clearMountPath() => $_clearField(6);
}

class WorkspaceContextRoots extends $pb.GeneratedMessage {
  factory WorkspaceContextRoots({
    $core.Iterable<WorkspaceContextRoot>? roots,
  }) {
    final result = WorkspaceContextRoots._();
    if (roots != null) result.roots.addAll(roots);
    return result;
  }

  WorkspaceContextRoots._();

  factory WorkspaceContextRoots.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceContextRoots()..mergeFromBuffer(data, registry);
  factory WorkspaceContextRoots.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceContextRoots()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkspaceContextRoots',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: WorkspaceContextRoots.$_createMessage)
    ..pPM<WorkspaceContextRoot>(1, _omitFieldNames ? '' : 'roots',
        subBuilder: WorkspaceContextRoot.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceContextRoots clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceContextRoots copyWith(
          void Function(WorkspaceContextRoots) updates) =>
      super.copyWith((message) => updates(message as WorkspaceContextRoots))
          as WorkspaceContextRoots;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use WorkspaceContextRoots() / WorkspaceContextRoots.new instead')
  static WorkspaceContextRoots create() => WorkspaceContextRoots._();
  static $pb.GeneratedMessage $_createMessage() => WorkspaceContextRoots._();
  @$core.override
  WorkspaceContextRoots createEmptyInstance() => WorkspaceContextRoots._();
  @$core.pragma('dart2js:noInline')
  static WorkspaceContextRoots getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<WorkspaceContextRoots>(
          WorkspaceContextRoots.$_createMessage);
  static WorkspaceContextRoots? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<WorkspaceContextRoot> get roots => $_getList(0);
}

class WorkspaceContextSnapshot extends $pb.GeneratedMessage {
  factory WorkspaceContextSnapshot({
    $core.int? version,
    $fixnum.Int64? revision,
    $core.List<$core.int>? contextId,
    $core.List<$core.int>? parentContextId,
    $core.Iterable<WorkspaceContextRoot>? roots,
    WorkspaceContextState? state,
  }) {
    final result = WorkspaceContextSnapshot._();
    if (version != null) result.version = version;
    if (revision != null) result.revision = revision;
    if (contextId != null) result.contextId = contextId;
    if (parentContextId != null) result.parentContextId = parentContextId;
    if (roots != null) result.roots.addAll(roots);
    if (state != null) result.state = state;
    return result;
  }

  WorkspaceContextSnapshot._();

  factory WorkspaceContextSnapshot.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceContextSnapshot()..mergeFromBuffer(data, registry);
  factory WorkspaceContextSnapshot.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceContextSnapshot()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkspaceContextSnapshot',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: WorkspaceContextSnapshot.$_createMessage)
    ..aI(1, _omitFieldNames ? '' : 'version', fieldType: $pb.PbFieldType.OU3)
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'revision', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'contextId', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'parentContextId', $pb.PbFieldType.OY)
    ..pPM<WorkspaceContextRoot>(5, _omitFieldNames ? '' : 'roots',
        subBuilder: WorkspaceContextRoot.$_createMessage)
    ..aE<WorkspaceContextState>(6, _omitFieldNames ? '' : 'state',
        enumValues: WorkspaceContextState.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceContextSnapshot clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceContextSnapshot copyWith(
          void Function(WorkspaceContextSnapshot) updates) =>
      super.copyWith((message) => updates(message as WorkspaceContextSnapshot))
          as WorkspaceContextSnapshot;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use WorkspaceContextSnapshot() / WorkspaceContextSnapshot.new instead')
  static WorkspaceContextSnapshot create() => WorkspaceContextSnapshot._();
  static $pb.GeneratedMessage $_createMessage() => WorkspaceContextSnapshot._();
  @$core.override
  WorkspaceContextSnapshot createEmptyInstance() =>
      WorkspaceContextSnapshot._();
  @$core.pragma('dart2js:noInline')
  static WorkspaceContextSnapshot getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<WorkspaceContextSnapshot>(
          WorkspaceContextSnapshot.$_createMessage);
  static WorkspaceContextSnapshot? _defaultInstance;

  @$pb.TagNumber(1)
  $core.int get version => $_getIZ(0);
  @$pb.TagNumber(1)
  set version($core.int value) => $_setUnsignedInt32(0, value);
  @$pb.TagNumber(1)
  $core.bool hasVersion() => $_has(0);
  @$pb.TagNumber(1)
  void clearVersion() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get revision => $_getI64(1);
  @$pb.TagNumber(2)
  set revision($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRevision() => $_has(1);
  @$pb.TagNumber(2)
  void clearRevision() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get contextId => $_getN(2);
  @$pb.TagNumber(3)
  set contextId($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasContextId() => $_has(2);
  @$pb.TagNumber(3)
  void clearContextId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get parentContextId => $_getN(3);
  @$pb.TagNumber(4)
  set parentContextId($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasParentContextId() => $_has(3);
  @$pb.TagNumber(4)
  void clearParentContextId() => $_clearField(4);

  @$pb.TagNumber(5)
  $pb.PbList<WorkspaceContextRoot> get roots => $_getList(4);

  @$pb.TagNumber(6)
  WorkspaceContextState get state => $_getN(5);
  @$pb.TagNumber(6)
  set state(WorkspaceContextState value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasState() => $_has(5);
  @$pb.TagNumber(6)
  void clearState() => $_clearField(6);
}

class WorkspaceContextDiscard extends $pb.GeneratedMessage {
  factory WorkspaceContextDiscard({
    $core.Iterable<$core.List<$core.int>>? contextIds,
  }) {
    final result = WorkspaceContextDiscard._();
    if (contextIds != null) result.contextIds.addAll(contextIds);
    return result;
  }

  WorkspaceContextDiscard._();

  factory WorkspaceContextDiscard.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceContextDiscard()..mergeFromBuffer(data, registry);
  factory WorkspaceContextDiscard.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceContextDiscard()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkspaceContextDiscard',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: WorkspaceContextDiscard.$_createMessage)
    ..p<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'contextIds', $pb.PbFieldType.PY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceContextDiscard clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceContextDiscard copyWith(
          void Function(WorkspaceContextDiscard) updates) =>
      super.copyWith((message) => updates(message as WorkspaceContextDiscard))
          as WorkspaceContextDiscard;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use WorkspaceContextDiscard() / WorkspaceContextDiscard.new instead')
  static WorkspaceContextDiscard create() => WorkspaceContextDiscard._();
  static $pb.GeneratedMessage $_createMessage() => WorkspaceContextDiscard._();
  @$core.override
  WorkspaceContextDiscard createEmptyInstance() => WorkspaceContextDiscard._();
  @$core.pragma('dart2js:noInline')
  static WorkspaceContextDiscard getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<WorkspaceContextDiscard>(
          WorkspaceContextDiscard.$_createMessage);
  static WorkspaceContextDiscard? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<$core.List<$core.int>> get contextIds => $_getList(0);
}

class OperationOptions extends $pb.GeneratedMessage {
  factory OperationOptions({
    $core.List<$core.int>? idempotencyKey,
  }) {
    final result = OperationOptions._();
    if (idempotencyKey != null) result.idempotencyKey = idempotencyKey;
    return result;
  }

  OperationOptions._();

  factory OperationOptions.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationOptions()..mergeFromBuffer(data, registry);
  factory OperationOptions.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OperationOptions()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OperationOptions',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: OperationOptions.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'idempotencyKey', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationOptions clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OperationOptions copyWith(void Function(OperationOptions) updates) =>
      super.copyWith((message) => updates(message as OperationOptions))
          as OperationOptions;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OperationOptions() / OperationOptions.new instead')
  static OperationOptions create() => OperationOptions._();
  static $pb.GeneratedMessage $_createMessage() => OperationOptions._();
  @$core.override
  OperationOptions createEmptyInstance() => OperationOptions._();
  @$core.pragma('dart2js:noInline')
  static OperationOptions getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OperationOptions>(
          OperationOptions.$_createMessage);
  static OperationOptions? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get idempotencyKey => $_getN(0);
  @$pb.TagNumber(1)
  set idempotencyKey($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasIdempotencyKey() => $_has(0);
  @$pb.TagNumber(1)
  void clearIdempotencyKey() => $_clearField(1);
}

class PageOptions extends $pb.GeneratedMessage {
  factory PageOptions({
    $core.int? maximumItems,
    LogicalName? after,
  }) {
    final result = PageOptions._();
    if (maximumItems != null) result.maximumItems = maximumItems;
    if (after != null) result.after = after;
    return result;
  }

  PageOptions._();

  factory PageOptions.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PageOptions()..mergeFromBuffer(data, registry);
  factory PageOptions.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PageOptions()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PageOptions',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: PageOptions.$_createMessage)
    ..aI(1, _omitFieldNames ? '' : 'maximumItems',
        fieldType: $pb.PbFieldType.OU3)
    ..aOM<LogicalName>(2, _omitFieldNames ? '' : 'after',
        subBuilder: LogicalName.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PageOptions clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PageOptions copyWith(void Function(PageOptions) updates) =>
      super.copyWith((message) => updates(message as PageOptions))
          as PageOptions;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use PageOptions() / PageOptions.new instead')
  static PageOptions create() => PageOptions._();
  static $pb.GeneratedMessage $_createMessage() => PageOptions._();
  @$core.override
  PageOptions createEmptyInstance() => PageOptions._();
  @$core.pragma('dart2js:noInline')
  static PageOptions getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<PageOptions>(
          PageOptions.$_createMessage);
  static PageOptions? _defaultInstance;

  @$pb.TagNumber(1)
  $core.int get maximumItems => $_getIZ(0);
  @$pb.TagNumber(1)
  set maximumItems($core.int value) => $_setUnsignedInt32(0, value);
  @$pb.TagNumber(1)
  $core.bool hasMaximumItems() => $_has(0);
  @$pb.TagNumber(1)
  void clearMaximumItems() => $_clearField(1);

  @$pb.TagNumber(2)
  LogicalName get after => $_getN(1);
  @$pb.TagNumber(2)
  set after(LogicalName value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAfter() => $_has(1);
  @$pb.TagNumber(2)
  void clearAfter() => $_clearField(2);
  @$pb.TagNumber(2)
  LogicalName ensureAfter() => $_ensure(1);
}

class ByteRange extends $pb.GeneratedMessage {
  factory ByteRange({
    $fixnum.Int64? offset,
    $fixnum.Int64? length,
  }) {
    final result = ByteRange._();
    if (offset != null) result.offset = offset;
    if (length != null) result.length = length;
    return result;
  }

  ByteRange._();

  factory ByteRange.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ByteRange()..mergeFromBuffer(data, registry);
  factory ByteRange.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ByteRange()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ByteRange',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ByteRange.$_createMessage)
    ..a<$fixnum.Int64>(1, _omitFieldNames ? '' : 'offset', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'length', $pb.PbFieldType.OU6,
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
  $fixnum.Int64 get offset => $_getI64(0);
  @$pb.TagNumber(1)
  set offset($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasOffset() => $_has(0);
  @$pb.TagNumber(1)
  void clearOffset() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get length => $_getI64(1);
  @$pb.TagNumber(2)
  set length($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasLength() => $_has(1);
  @$pb.TagNumber(2)
  void clearLength() => $_clearField(2);
}

enum OptionalU32_Value { present, unavailable, notSet }

class OptionalU32 extends $pb.GeneratedMessage {
  factory OptionalU32({
    $core.int? present,
    $core.bool? unavailable,
  }) {
    final result = OptionalU32._();
    if (present != null) result.present = present;
    if (unavailable != null) result.unavailable = unavailable;
    return result;
  }

  OptionalU32._();

  factory OptionalU32.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OptionalU32()..mergeFromBuffer(data, registry);
  factory OptionalU32.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OptionalU32()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, OptionalU32_Value> _OptionalU32_ValueByTag =
      {
    1: OptionalU32_Value.present,
    2: OptionalU32_Value.unavailable,
    0: OptionalU32_Value.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OptionalU32',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: OptionalU32.$_createMessage)
    ..oo(0, [1, 2])
    ..aI(1, _omitFieldNames ? '' : 'present', fieldType: $pb.PbFieldType.OU3)
    ..aOB(2, _omitFieldNames ? '' : 'unavailable')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OptionalU32 clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OptionalU32 copyWith(void Function(OptionalU32) updates) =>
      super.copyWith((message) => updates(message as OptionalU32))
          as OptionalU32;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OptionalU32() / OptionalU32.new instead')
  static OptionalU32 create() => OptionalU32._();
  static $pb.GeneratedMessage $_createMessage() => OptionalU32._();
  @$core.override
  OptionalU32 createEmptyInstance() => OptionalU32._();
  @$core.pragma('dart2js:noInline')
  static OptionalU32 getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OptionalU32>(
          OptionalU32.$_createMessage);
  static OptionalU32? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  OptionalU32_Value whichValue() => _OptionalU32_ValueByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearValue() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.int get present => $_getIZ(0);
  @$pb.TagNumber(1)
  set present($core.int value) => $_setUnsignedInt32(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPresent() => $_has(0);
  @$pb.TagNumber(1)
  void clearPresent() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.bool get unavailable => $_getBF(1);
  @$pb.TagNumber(2)
  set unavailable($core.bool value) => $_setBool(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUnavailable() => $_has(1);
  @$pb.TagNumber(2)
  void clearUnavailable() => $_clearField(2);
}

enum OptionalU64_Value { present, unavailable, notSet }

class OptionalU64 extends $pb.GeneratedMessage {
  factory OptionalU64({
    $fixnum.Int64? present,
    $core.bool? unavailable,
  }) {
    final result = OptionalU64._();
    if (present != null) result.present = present;
    if (unavailable != null) result.unavailable = unavailable;
    return result;
  }

  OptionalU64._();

  factory OptionalU64.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OptionalU64()..mergeFromBuffer(data, registry);
  factory OptionalU64.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OptionalU64()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, OptionalU64_Value> _OptionalU64_ValueByTag =
      {
    1: OptionalU64_Value.present,
    2: OptionalU64_Value.unavailable,
    0: OptionalU64_Value.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OptionalU64',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: OptionalU64.$_createMessage)
    ..oo(0, [1, 2])
    ..a<$fixnum.Int64>(1, _omitFieldNames ? '' : 'present', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOB(2, _omitFieldNames ? '' : 'unavailable')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OptionalU64 clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OptionalU64 copyWith(void Function(OptionalU64) updates) =>
      super.copyWith((message) => updates(message as OptionalU64))
          as OptionalU64;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OptionalU64() / OptionalU64.new instead')
  static OptionalU64 create() => OptionalU64._();
  static $pb.GeneratedMessage $_createMessage() => OptionalU64._();
  @$core.override
  OptionalU64 createEmptyInstance() => OptionalU64._();
  @$core.pragma('dart2js:noInline')
  static OptionalU64 getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OptionalU64>(
          OptionalU64.$_createMessage);
  static OptionalU64? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  OptionalU64_Value whichValue() => _OptionalU64_ValueByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearValue() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $fixnum.Int64 get present => $_getI64(0);
  @$pb.TagNumber(1)
  set present($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPresent() => $_has(0);
  @$pb.TagNumber(1)
  void clearPresent() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.bool get unavailable => $_getBF(1);
  @$pb.TagNumber(2)
  set unavailable($core.bool value) => $_setBool(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUnavailable() => $_has(1);
  @$pb.TagNumber(2)
  void clearUnavailable() => $_clearField(2);
}

enum OptionalI64_Value { present, unavailable, notSet }

class OptionalI64 extends $pb.GeneratedMessage {
  factory OptionalI64({
    $fixnum.Int64? present,
    $core.bool? unavailable,
  }) {
    final result = OptionalI64._();
    if (present != null) result.present = present;
    if (unavailable != null) result.unavailable = unavailable;
    return result;
  }

  OptionalI64._();

  factory OptionalI64.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OptionalI64()..mergeFromBuffer(data, registry);
  factory OptionalI64.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OptionalI64()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, OptionalI64_Value> _OptionalI64_ValueByTag =
      {
    1: OptionalI64_Value.present,
    2: OptionalI64_Value.unavailable,
    0: OptionalI64_Value.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OptionalI64',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: OptionalI64.$_createMessage)
    ..oo(0, [1, 2])
    ..a<$fixnum.Int64>(1, _omitFieldNames ? '' : 'present', $pb.PbFieldType.OS6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOB(2, _omitFieldNames ? '' : 'unavailable')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OptionalI64 clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OptionalI64 copyWith(void Function(OptionalI64) updates) =>
      super.copyWith((message) => updates(message as OptionalI64))
          as OptionalI64;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use OptionalI64() / OptionalI64.new instead')
  static OptionalI64 create() => OptionalI64._();
  static $pb.GeneratedMessage $_createMessage() => OptionalI64._();
  @$core.override
  OptionalI64 createEmptyInstance() => OptionalI64._();
  @$core.pragma('dart2js:noInline')
  static OptionalI64 getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<OptionalI64>(
          OptionalI64.$_createMessage);
  static OptionalI64? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  OptionalI64_Value whichValue() => _OptionalI64_ValueByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearValue() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $fixnum.Int64 get present => $_getI64(0);
  @$pb.TagNumber(1)
  set present($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPresent() => $_has(0);
  @$pb.TagNumber(1)
  void clearPresent() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.bool get unavailable => $_getBF(1);
  @$pb.TagNumber(2)
  set unavailable($core.bool value) => $_setBool(1, value);
  @$pb.TagNumber(2)
  $core.bool hasUnavailable() => $_has(1);
  @$pb.TagNumber(2)
  void clearUnavailable() => $_clearField(2);
}

class Metadata extends $pb.GeneratedMessage {
  factory Metadata({
    OptionalU32? posixMode,
    OptionalU32? posixUid,
    OptionalU32? posixGid,
    OptionalU64? posixFlags,
    OptionalU32? windowsAttributes,
    OptionalI64? createdNs,
    OptionalI64? modifiedNs,
    OptionalI64? accessedNs,
    OptionalI64? changedNs,
    $core.bool? hasNamedAttributes,
    $core.bool? hasAcl,
    $core.bool? hasSecurityDescriptor,
  }) {
    final result = Metadata._();
    if (posixMode != null) result.posixMode = posixMode;
    if (posixUid != null) result.posixUid = posixUid;
    if (posixGid != null) result.posixGid = posixGid;
    if (posixFlags != null) result.posixFlags = posixFlags;
    if (windowsAttributes != null) result.windowsAttributes = windowsAttributes;
    if (createdNs != null) result.createdNs = createdNs;
    if (modifiedNs != null) result.modifiedNs = modifiedNs;
    if (accessedNs != null) result.accessedNs = accessedNs;
    if (changedNs != null) result.changedNs = changedNs;
    if (hasNamedAttributes != null)
      result.hasNamedAttributes = hasNamedAttributes;
    if (hasAcl != null) result.hasAcl = hasAcl;
    if (hasSecurityDescriptor != null)
      result.hasSecurityDescriptor = hasSecurityDescriptor;
    return result;
  }

  Metadata._();

  factory Metadata.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Metadata()..mergeFromBuffer(data, registry);
  factory Metadata.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Metadata()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Metadata',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Metadata.$_createMessage)
    ..aOM<OptionalU32>(1, _omitFieldNames ? '' : 'posixMode',
        subBuilder: OptionalU32.$_createMessage)
    ..aOM<OptionalU32>(2, _omitFieldNames ? '' : 'posixUid',
        subBuilder: OptionalU32.$_createMessage)
    ..aOM<OptionalU32>(3, _omitFieldNames ? '' : 'posixGid',
        subBuilder: OptionalU32.$_createMessage)
    ..aOM<OptionalU64>(4, _omitFieldNames ? '' : 'posixFlags',
        subBuilder: OptionalU64.$_createMessage)
    ..aOM<OptionalU32>(5, _omitFieldNames ? '' : 'windowsAttributes',
        subBuilder: OptionalU32.$_createMessage)
    ..aOM<OptionalI64>(6, _omitFieldNames ? '' : 'createdNs',
        subBuilder: OptionalI64.$_createMessage)
    ..aOM<OptionalI64>(7, _omitFieldNames ? '' : 'modifiedNs',
        subBuilder: OptionalI64.$_createMessage)
    ..aOM<OptionalI64>(8, _omitFieldNames ? '' : 'accessedNs',
        subBuilder: OptionalI64.$_createMessage)
    ..aOM<OptionalI64>(9, _omitFieldNames ? '' : 'changedNs',
        subBuilder: OptionalI64.$_createMessage)
    ..aOB(10, _omitFieldNames ? '' : 'hasNamedAttributes')
    ..aOB(11, _omitFieldNames ? '' : 'hasAcl')
    ..aOB(12, _omitFieldNames ? '' : 'hasSecurityDescriptor')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Metadata clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Metadata copyWith(void Function(Metadata) updates) =>
      super.copyWith((message) => updates(message as Metadata)) as Metadata;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Metadata() / Metadata.new instead')
  static Metadata create() => Metadata._();
  static $pb.GeneratedMessage $_createMessage() => Metadata._();
  @$core.override
  Metadata createEmptyInstance() => Metadata._();
  @$core.pragma('dart2js:noInline')
  static Metadata getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Metadata>(Metadata.$_createMessage);
  static Metadata? _defaultInstance;

  @$pb.TagNumber(1)
  OptionalU32 get posixMode => $_getN(0);
  @$pb.TagNumber(1)
  set posixMode(OptionalU32 value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasPosixMode() => $_has(0);
  @$pb.TagNumber(1)
  void clearPosixMode() => $_clearField(1);
  @$pb.TagNumber(1)
  OptionalU32 ensurePosixMode() => $_ensure(0);

  @$pb.TagNumber(2)
  OptionalU32 get posixUid => $_getN(1);
  @$pb.TagNumber(2)
  set posixUid(OptionalU32 value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasPosixUid() => $_has(1);
  @$pb.TagNumber(2)
  void clearPosixUid() => $_clearField(2);
  @$pb.TagNumber(2)
  OptionalU32 ensurePosixUid() => $_ensure(1);

  @$pb.TagNumber(3)
  OptionalU32 get posixGid => $_getN(2);
  @$pb.TagNumber(3)
  set posixGid(OptionalU32 value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasPosixGid() => $_has(2);
  @$pb.TagNumber(3)
  void clearPosixGid() => $_clearField(3);
  @$pb.TagNumber(3)
  OptionalU32 ensurePosixGid() => $_ensure(2);

  @$pb.TagNumber(4)
  OptionalU64 get posixFlags => $_getN(3);
  @$pb.TagNumber(4)
  set posixFlags(OptionalU64 value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasPosixFlags() => $_has(3);
  @$pb.TagNumber(4)
  void clearPosixFlags() => $_clearField(4);
  @$pb.TagNumber(4)
  OptionalU64 ensurePosixFlags() => $_ensure(3);

  @$pb.TagNumber(5)
  OptionalU32 get windowsAttributes => $_getN(4);
  @$pb.TagNumber(5)
  set windowsAttributes(OptionalU32 value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasWindowsAttributes() => $_has(4);
  @$pb.TagNumber(5)
  void clearWindowsAttributes() => $_clearField(5);
  @$pb.TagNumber(5)
  OptionalU32 ensureWindowsAttributes() => $_ensure(4);

  @$pb.TagNumber(6)
  OptionalI64 get createdNs => $_getN(5);
  @$pb.TagNumber(6)
  set createdNs(OptionalI64 value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasCreatedNs() => $_has(5);
  @$pb.TagNumber(6)
  void clearCreatedNs() => $_clearField(6);
  @$pb.TagNumber(6)
  OptionalI64 ensureCreatedNs() => $_ensure(5);

  @$pb.TagNumber(7)
  OptionalI64 get modifiedNs => $_getN(6);
  @$pb.TagNumber(7)
  set modifiedNs(OptionalI64 value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasModifiedNs() => $_has(6);
  @$pb.TagNumber(7)
  void clearModifiedNs() => $_clearField(7);
  @$pb.TagNumber(7)
  OptionalI64 ensureModifiedNs() => $_ensure(6);

  @$pb.TagNumber(8)
  OptionalI64 get accessedNs => $_getN(7);
  @$pb.TagNumber(8)
  set accessedNs(OptionalI64 value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasAccessedNs() => $_has(7);
  @$pb.TagNumber(8)
  void clearAccessedNs() => $_clearField(8);
  @$pb.TagNumber(8)
  OptionalI64 ensureAccessedNs() => $_ensure(7);

  @$pb.TagNumber(9)
  OptionalI64 get changedNs => $_getN(8);
  @$pb.TagNumber(9)
  set changedNs(OptionalI64 value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasChangedNs() => $_has(8);
  @$pb.TagNumber(9)
  void clearChangedNs() => $_clearField(9);
  @$pb.TagNumber(9)
  OptionalI64 ensureChangedNs() => $_ensure(8);

  @$pb.TagNumber(10)
  $core.bool get hasNamedAttributes => $_getBF(9);
  @$pb.TagNumber(10)
  set hasNamedAttributes($core.bool value) => $_setBool(9, value);
  @$pb.TagNumber(10)
  $core.bool hasHasNamedAttributes() => $_has(9);
  @$pb.TagNumber(10)
  void clearHasNamedAttributes() => $_clearField(10);

  @$pb.TagNumber(11)
  $core.bool get hasAcl => $_getBF(10);
  @$pb.TagNumber(11)
  set hasAcl($core.bool value) => $_setBool(10, value);
  @$pb.TagNumber(11)
  $core.bool hasHasAcl() => $_has(10);
  @$pb.TagNumber(11)
  void clearHasAcl() => $_clearField(11);

  @$pb.TagNumber(12)
  $core.bool get hasSecurityDescriptor => $_getBF(11);
  @$pb.TagNumber(12)
  set hasSecurityDescriptor($core.bool value) => $_setBool(11, value);
  @$pb.TagNumber(12)
  $core.bool hasHasSecurityDescriptor() => $_has(11);
  @$pb.TagNumber(12)
  void clearHasSecurityDescriptor() => $_clearField(12);
}

class FileStat extends $pb.GeneratedMessage {
  factory FileStat({
    $core.List<$core.int>? fileId,
    FileKind? kind,
    $fixnum.Int64? linkCount,
    OptionalU64? logicalBytes,
    Metadata? metadata,
  }) {
    final result = FileStat._();
    if (fileId != null) result.fileId = fileId;
    if (kind != null) result.kind = kind;
    if (linkCount != null) result.linkCount = linkCount;
    if (logicalBytes != null) result.logicalBytes = logicalBytes;
    if (metadata != null) result.metadata = metadata;
    return result;
  }

  FileStat._();

  factory FileStat.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileStat()..mergeFromBuffer(data, registry);
  factory FileStat.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileStat()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'FileStat',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: FileStat.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'fileId', $pb.PbFieldType.OY)
    ..aE<FileKind>(2, _omitFieldNames ? '' : 'kind',
        enumValues: FileKind.values)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'linkCount', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<OptionalU64>(4, _omitFieldNames ? '' : 'logicalBytes',
        subBuilder: OptionalU64.$_createMessage)
    ..aOM<Metadata>(5, _omitFieldNames ? '' : 'metadata',
        subBuilder: Metadata.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileStat clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileStat copyWith(void Function(FileStat) updates) =>
      super.copyWith((message) => updates(message as FileStat)) as FileStat;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use FileStat() / FileStat.new instead')
  static FileStat create() => FileStat._();
  static $pb.GeneratedMessage $_createMessage() => FileStat._();
  @$core.override
  FileStat createEmptyInstance() => FileStat._();
  @$core.pragma('dart2js:noInline')
  static FileStat getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<FileStat>(FileStat.$_createMessage);
  static FileStat? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get fileId => $_getN(0);
  @$pb.TagNumber(1)
  set fileId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasFileId() => $_has(0);
  @$pb.TagNumber(1)
  void clearFileId() => $_clearField(1);

  @$pb.TagNumber(2)
  FileKind get kind => $_getN(1);
  @$pb.TagNumber(2)
  set kind(FileKind value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasKind() => $_has(1);
  @$pb.TagNumber(2)
  void clearKind() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get linkCount => $_getI64(2);
  @$pb.TagNumber(3)
  set linkCount($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLinkCount() => $_has(2);
  @$pb.TagNumber(3)
  void clearLinkCount() => $_clearField(3);

  @$pb.TagNumber(4)
  OptionalU64 get logicalBytes => $_getN(3);
  @$pb.TagNumber(4)
  set logicalBytes(OptionalU64 value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasLogicalBytes() => $_has(3);
  @$pb.TagNumber(4)
  void clearLogicalBytes() => $_clearField(4);
  @$pb.TagNumber(4)
  OptionalU64 ensureLogicalBytes() => $_ensure(3);

  @$pb.TagNumber(5)
  Metadata get metadata => $_getN(4);
  @$pb.TagNumber(5)
  set metadata(Metadata value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasMetadata() => $_has(4);
  @$pb.TagNumber(5)
  void clearMetadata() => $_clearField(5);
  @$pb.TagNumber(5)
  Metadata ensureMetadata() => $_ensure(4);
}

class DirectoryEntry extends $pb.GeneratedMessage {
  factory DirectoryEntry({
    LogicalName? name,
    FileStat? stat,
  }) {
    final result = DirectoryEntry._();
    if (name != null) result.name = name;
    if (stat != null) result.stat = stat;
    return result;
  }

  DirectoryEntry._();

  factory DirectoryEntry.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryEntry()..mergeFromBuffer(data, registry);
  factory DirectoryEntry.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryEntry()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DirectoryEntry',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: DirectoryEntry.$_createMessage)
    ..aOM<LogicalName>(1, _omitFieldNames ? '' : 'name',
        subBuilder: LogicalName.$_createMessage)
    ..aOM<FileStat>(2, _omitFieldNames ? '' : 'stat',
        subBuilder: FileStat.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryEntry clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryEntry copyWith(void Function(DirectoryEntry) updates) =>
      super.copyWith((message) => updates(message as DirectoryEntry))
          as DirectoryEntry;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use DirectoryEntry() / DirectoryEntry.new instead')
  static DirectoryEntry create() => DirectoryEntry._();
  static $pb.GeneratedMessage $_createMessage() => DirectoryEntry._();
  @$core.override
  DirectoryEntry createEmptyInstance() => DirectoryEntry._();
  @$core.pragma('dart2js:noInline')
  static DirectoryEntry getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<DirectoryEntry>(
          DirectoryEntry.$_createMessage);
  static DirectoryEntry? _defaultInstance;

  @$pb.TagNumber(1)
  LogicalName get name => $_getN(0);
  @$pb.TagNumber(1)
  set name(LogicalName value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);
  @$pb.TagNumber(1)
  LogicalName ensureName() => $_ensure(0);

  @$pb.TagNumber(2)
  FileStat get stat => $_getN(1);
  @$pb.TagNumber(2)
  set stat(FileStat value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasStat() => $_has(1);
  @$pb.TagNumber(2)
  void clearStat() => $_clearField(2);
  @$pb.TagNumber(2)
  FileStat ensureStat() => $_ensure(1);
}

class DirectoryPage extends $pb.GeneratedMessage {
  factory DirectoryPage({
    $core.Iterable<DirectoryEntry>? entries,
    LogicalName? next,
  }) {
    final result = DirectoryPage._();
    if (entries != null) result.entries.addAll(entries);
    if (next != null) result.next = next;
    return result;
  }

  DirectoryPage._();

  factory DirectoryPage.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryPage()..mergeFromBuffer(data, registry);
  factory DirectoryPage.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryPage()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DirectoryPage',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: DirectoryPage.$_createMessage)
    ..pPM<DirectoryEntry>(1, _omitFieldNames ? '' : 'entries',
        subBuilder: DirectoryEntry.$_createMessage)
    ..aOM<LogicalName>(2, _omitFieldNames ? '' : 'next',
        subBuilder: LogicalName.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryPage clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryPage copyWith(void Function(DirectoryPage) updates) =>
      super.copyWith((message) => updates(message as DirectoryPage))
          as DirectoryPage;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use DirectoryPage() / DirectoryPage.new instead')
  static DirectoryPage create() => DirectoryPage._();
  static $pb.GeneratedMessage $_createMessage() => DirectoryPage._();
  @$core.override
  DirectoryPage createEmptyInstance() => DirectoryPage._();
  @$core.pragma('dart2js:noInline')
  static DirectoryPage getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<DirectoryPage>(
          DirectoryPage.$_createMessage);
  static DirectoryPage? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<DirectoryEntry> get entries => $_getList(0);

  @$pb.TagNumber(2)
  LogicalName get next => $_getN(1);
  @$pb.TagNumber(2)
  set next(LogicalName value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasNext() => $_has(1);
  @$pb.TagNumber(2)
  void clearNext() => $_clearField(2);
  @$pb.TagNumber(2)
  LogicalName ensureNext() => $_ensure(1);
}

class Extent extends $pb.GeneratedMessage {
  factory Extent({
    ByteRange? range,
    ExtentKind? kind,
  }) {
    final result = Extent._();
    if (range != null) result.range = range;
    if (kind != null) result.kind = kind;
    return result;
  }

  Extent._();

  factory Extent.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Extent()..mergeFromBuffer(data, registry);
  factory Extent.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Extent()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Extent',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Extent.$_createMessage)
    ..aOM<ByteRange>(1, _omitFieldNames ? '' : 'range',
        subBuilder: ByteRange.$_createMessage)
    ..aE<ExtentKind>(2, _omitFieldNames ? '' : 'kind',
        enumValues: ExtentKind.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Extent clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Extent copyWith(void Function(Extent) updates) =>
      super.copyWith((message) => updates(message as Extent)) as Extent;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Extent() / Extent.new instead')
  static Extent create() => Extent._();
  static $pb.GeneratedMessage $_createMessage() => Extent._();
  @$core.override
  Extent createEmptyInstance() => Extent._();
  @$core.pragma('dart2js:noInline')
  static Extent getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Extent>(Extent.$_createMessage);
  static Extent? _defaultInstance;

  @$pb.TagNumber(1)
  ByteRange get range => $_getN(0);
  @$pb.TagNumber(1)
  set range(ByteRange value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasRange() => $_has(0);
  @$pb.TagNumber(1)
  void clearRange() => $_clearField(1);
  @$pb.TagNumber(1)
  ByteRange ensureRange() => $_ensure(0);

  @$pb.TagNumber(2)
  ExtentKind get kind => $_getN(1);
  @$pb.TagNumber(2)
  set kind(ExtentKind value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasKind() => $_has(1);
  @$pb.TagNumber(2)
  void clearKind() => $_clearField(2);
}

class Capabilities extends $pb.GeneratedMessage {
  factory Capabilities({
    $core.String? contractVersion,
    $core.Iterable<FilesystemProfile>? profiles,
    $fixnum.Int64? maximumRequestBytes,
    $fixnum.Int64? maximumResponseBytes,
    $core.int? maximumTransactionMutations,
    $core.int? maximumPageItems,
    $core.bool? nativeMountCredentials,
    $core.bool? s3Credentials,
    $core.bool? sourceReconciliation,
  }) {
    final result = Capabilities._();
    if (contractVersion != null) result.contractVersion = contractVersion;
    if (profiles != null) result.profiles.addAll(profiles);
    if (maximumRequestBytes != null)
      result.maximumRequestBytes = maximumRequestBytes;
    if (maximumResponseBytes != null)
      result.maximumResponseBytes = maximumResponseBytes;
    if (maximumTransactionMutations != null)
      result.maximumTransactionMutations = maximumTransactionMutations;
    if (maximumPageItems != null) result.maximumPageItems = maximumPageItems;
    if (nativeMountCredentials != null)
      result.nativeMountCredentials = nativeMountCredentials;
    if (s3Credentials != null) result.s3Credentials = s3Credentials;
    if (sourceReconciliation != null)
      result.sourceReconciliation = sourceReconciliation;
    return result;
  }

  Capabilities._();

  factory Capabilities.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Capabilities()..mergeFromBuffer(data, registry);
  factory Capabilities.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Capabilities()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Capabilities',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Capabilities.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'contractVersion')
    ..pc<FilesystemProfile>(
        2, _omitFieldNames ? '' : 'profiles', $pb.PbFieldType.KE,
        valueOf: FilesystemProfile.valueOf,
        enumValues: FilesystemProfile.values,
        defaultEnumValue: FilesystemProfile.FILESYSTEM_PROFILE_UNSPECIFIED)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'maximumRequestBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'maximumResponseBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aI(5, _omitFieldNames ? '' : 'maximumTransactionMutations',
        fieldType: $pb.PbFieldType.OU3)
    ..aI(6, _omitFieldNames ? '' : 'maximumPageItems',
        fieldType: $pb.PbFieldType.OU3)
    ..aOB(7, _omitFieldNames ? '' : 'nativeMountCredentials')
    ..aOB(8, _omitFieldNames ? '' : 's3Credentials')
    ..aOB(9, _omitFieldNames ? '' : 'sourceReconciliation')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Capabilities clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Capabilities copyWith(void Function(Capabilities) updates) =>
      super.copyWith((message) => updates(message as Capabilities))
          as Capabilities;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Capabilities() / Capabilities.new instead')
  static Capabilities create() => Capabilities._();
  static $pb.GeneratedMessage $_createMessage() => Capabilities._();
  @$core.override
  Capabilities createEmptyInstance() => Capabilities._();
  @$core.pragma('dart2js:noInline')
  static Capabilities getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Capabilities>(
          Capabilities.$_createMessage);
  static Capabilities? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get contractVersion => $_getSZ(0);
  @$pb.TagNumber(1)
  set contractVersion($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasContractVersion() => $_has(0);
  @$pb.TagNumber(1)
  void clearContractVersion() => $_clearField(1);

  @$pb.TagNumber(2)
  $pb.PbList<FilesystemProfile> get profiles => $_getList(1);

  @$pb.TagNumber(3)
  $fixnum.Int64 get maximumRequestBytes => $_getI64(2);
  @$pb.TagNumber(3)
  set maximumRequestBytes($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumRequestBytes() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumRequestBytes() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get maximumResponseBytes => $_getI64(3);
  @$pb.TagNumber(4)
  set maximumResponseBytes($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumResponseBytes() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumResponseBytes() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.int get maximumTransactionMutations => $_getIZ(4);
  @$pb.TagNumber(5)
  set maximumTransactionMutations($core.int value) =>
      $_setUnsignedInt32(4, value);
  @$pb.TagNumber(5)
  $core.bool hasMaximumTransactionMutations() => $_has(4);
  @$pb.TagNumber(5)
  void clearMaximumTransactionMutations() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.int get maximumPageItems => $_getIZ(5);
  @$pb.TagNumber(6)
  set maximumPageItems($core.int value) => $_setUnsignedInt32(5, value);
  @$pb.TagNumber(6)
  $core.bool hasMaximumPageItems() => $_has(5);
  @$pb.TagNumber(6)
  void clearMaximumPageItems() => $_clearField(6);

  @$pb.TagNumber(7)
  $core.bool get nativeMountCredentials => $_getBF(6);
  @$pb.TagNumber(7)
  set nativeMountCredentials($core.bool value) => $_setBool(6, value);
  @$pb.TagNumber(7)
  $core.bool hasNativeMountCredentials() => $_has(6);
  @$pb.TagNumber(7)
  void clearNativeMountCredentials() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.bool get s3Credentials => $_getBF(7);
  @$pb.TagNumber(8)
  set s3Credentials($core.bool value) => $_setBool(7, value);
  @$pb.TagNumber(8)
  $core.bool hasS3Credentials() => $_has(7);
  @$pb.TagNumber(8)
  void clearS3Credentials() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.bool get sourceReconciliation => $_getBF(8);
  @$pb.TagNumber(9)
  set sourceReconciliation($core.bool value) => $_setBool(8, value);
  @$pb.TagNumber(9)
  $core.bool hasSourceReconciliation() => $_has(8);
  @$pb.TagNumber(9)
  void clearSourceReconciliation() => $_clearField(9);
}

class SourceStateRequest extends $pb.GeneratedMessage {
  factory SourceStateRequest({
    WorkspaceRef? workspace,
  }) {
    final result = SourceStateRequest._();
    if (workspace != null) result.workspace = workspace;
    return result;
  }

  SourceStateRequest._();

  factory SourceStateRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SourceStateRequest()..mergeFromBuffer(data, registry);
  factory SourceStateRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SourceStateRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SourceStateRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: SourceStateRequest.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SourceStateRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SourceStateRequest copyWith(void Function(SourceStateRequest) updates) =>
      super.copyWith((message) => updates(message as SourceStateRequest))
          as SourceStateRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SourceStateRequest() / SourceStateRequest.new instead')
  static SourceStateRequest create() => SourceStateRequest._();
  static $pb.GeneratedMessage $_createMessage() => SourceStateRequest._();
  @$core.override
  SourceStateRequest createEmptyInstance() => SourceStateRequest._();
  @$core.pragma('dart2js:noInline')
  static SourceStateRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SourceStateRequest>(
          SourceStateRequest.$_createMessage);
  static SourceStateRequest? _defaultInstance;

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);
}

class SourceOperationRequest extends $pb.GeneratedMessage {
  factory SourceOperationRequest({
    WorkspaceRef? workspace,
    OperationOptions? operation,
  }) {
    final result = SourceOperationRequest._();
    if (workspace != null) result.workspace = workspace;
    if (operation != null) result.operation = operation;
    return result;
  }

  SourceOperationRequest._();

  factory SourceOperationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SourceOperationRequest()..mergeFromBuffer(data, registry);
  factory SourceOperationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SourceOperationRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SourceOperationRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: SourceOperationRequest.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..aOM<OperationOptions>(2, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SourceOperationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SourceOperationRequest copyWith(
          void Function(SourceOperationRequest) updates) =>
      super.copyWith((message) => updates(message as SourceOperationRequest))
          as SourceOperationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use SourceOperationRequest() / SourceOperationRequest.new instead')
  static SourceOperationRequest create() => SourceOperationRequest._();
  static $pb.GeneratedMessage $_createMessage() => SourceOperationRequest._();
  @$core.override
  SourceOperationRequest createEmptyInstance() => SourceOperationRequest._();
  @$core.pragma('dart2js:noInline')
  static SourceOperationRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<SourceOperationRequest>(
          SourceOperationRequest.$_createMessage);
  static SourceOperationRequest? _defaultInstance;

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  OperationOptions get operation => $_getN(1);
  @$pb.TagNumber(2)
  set operation(OperationOptions value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOperation() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperation() => $_clearField(2);
  @$pb.TagNumber(2)
  OperationOptions ensureOperation() => $_ensure(1);
}

class SourceResponse extends $pb.GeneratedMessage {
  factory SourceResponse({
    SourceState? state,
    SourceInvalidationReason? reason,
    GenerationRef? generation,
  }) {
    final result = SourceResponse._();
    if (state != null) result.state = state;
    if (reason != null) result.reason = reason;
    if (generation != null) result.generation = generation;
    return result;
  }

  SourceResponse._();

  factory SourceResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SourceResponse()..mergeFromBuffer(data, registry);
  factory SourceResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SourceResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SourceResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: SourceResponse.$_createMessage)
    ..aE<SourceState>(1, _omitFieldNames ? '' : 'state',
        enumValues: SourceState.values)
    ..aE<SourceInvalidationReason>(2, _omitFieldNames ? '' : 'reason',
        enumValues: SourceInvalidationReason.values)
    ..aOM<GenerationRef>(3, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SourceResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SourceResponse copyWith(void Function(SourceResponse) updates) =>
      super.copyWith((message) => updates(message as SourceResponse))
          as SourceResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SourceResponse() / SourceResponse.new instead')
  static SourceResponse create() => SourceResponse._();
  static $pb.GeneratedMessage $_createMessage() => SourceResponse._();
  @$core.override
  SourceResponse createEmptyInstance() => SourceResponse._();
  @$core.pragma('dart2js:noInline')
  static SourceResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SourceResponse>(
          SourceResponse.$_createMessage);
  static SourceResponse? _defaultInstance;

  @$pb.TagNumber(1)
  SourceState get state => $_getN(0);
  @$pb.TagNumber(1)
  set state(SourceState value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasState() => $_has(0);
  @$pb.TagNumber(1)
  void clearState() => $_clearField(1);

  @$pb.TagNumber(2)
  SourceInvalidationReason get reason => $_getN(1);
  @$pb.TagNumber(2)
  set reason(SourceInvalidationReason value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasReason() => $_has(1);
  @$pb.TagNumber(2)
  void clearReason() => $_clearField(2);

  @$pb.TagNumber(3)
  GenerationRef get generation => $_getN(2);
  @$pb.TagNumber(3)
  set generation(GenerationRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasGeneration() => $_has(2);
  @$pb.TagNumber(3)
  void clearGeneration() => $_clearField(3);
  @$pb.TagNumber(3)
  GenerationRef ensureGeneration() => $_ensure(2);
}

class Workspace extends $pb.GeneratedMessage {
  factory Workspace({
    WorkspaceRef? workspace,
    $core.String? name,
    FilesystemProfile? profile,
    GenerationRef? head,
    $core.bool? deleted,
  }) {
    final result = Workspace._();
    if (workspace != null) result.workspace = workspace;
    if (name != null) result.name = name;
    if (profile != null) result.profile = profile;
    if (head != null) result.head = head;
    if (deleted != null) result.deleted = deleted;
    return result;
  }

  Workspace._();

  factory Workspace.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Workspace()..mergeFromBuffer(data, registry);
  factory Workspace.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Workspace()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Workspace',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Workspace.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'name')
    ..aE<FilesystemProfile>(3, _omitFieldNames ? '' : 'profile',
        enumValues: FilesystemProfile.values)
    ..aOM<GenerationRef>(4, _omitFieldNames ? '' : 'head',
        subBuilder: GenerationRef.$_createMessage)
    ..aOB(5, _omitFieldNames ? '' : 'deleted')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Workspace clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Workspace copyWith(void Function(Workspace) updates) =>
      super.copyWith((message) => updates(message as Workspace)) as Workspace;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Workspace() / Workspace.new instead')
  static Workspace create() => Workspace._();
  static $pb.GeneratedMessage $_createMessage() => Workspace._();
  @$core.override
  Workspace createEmptyInstance() => Workspace._();
  @$core.pragma('dart2js:noInline')
  static Workspace getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Workspace>(Workspace.$_createMessage);
  static Workspace? _defaultInstance;

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get name => $_getSZ(1);
  @$pb.TagNumber(2)
  set name($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => $_clearField(2);

  @$pb.TagNumber(3)
  FilesystemProfile get profile => $_getN(2);
  @$pb.TagNumber(3)
  set profile(FilesystemProfile value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasProfile() => $_has(2);
  @$pb.TagNumber(3)
  void clearProfile() => $_clearField(3);

  @$pb.TagNumber(4)
  GenerationRef get head => $_getN(3);
  @$pb.TagNumber(4)
  set head(GenerationRef value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasHead() => $_has(3);
  @$pb.TagNumber(4)
  void clearHead() => $_clearField(4);
  @$pb.TagNumber(4)
  GenerationRef ensureHead() => $_ensure(3);

  @$pb.TagNumber(5)
  $core.bool get deleted => $_getBF(4);
  @$pb.TagNumber(5)
  set deleted($core.bool value) => $_setBool(4, value);
  @$pb.TagNumber(5)
  $core.bool hasDeleted() => $_has(4);
  @$pb.TagNumber(5)
  void clearDeleted() => $_clearField(5);
}

class HandshakeRequest extends $pb.GeneratedMessage {
  factory HandshakeRequest({
    $1.HandshakeRequest? protocol,
  }) {
    final result = HandshakeRequest._();
    if (protocol != null) result.protocol = protocol;
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
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: HandshakeRequest.$_createMessage)
    ..aOM<$1.HandshakeRequest>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: $1.HandshakeRequest.$_createMessage)
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
  $1.HandshakeRequest get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol($1.HandshakeRequest value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  $1.HandshakeRequest ensureProtocol() => $_ensure(0);
}

class HandshakeResponse extends $pb.GeneratedMessage {
  factory HandshakeResponse({
    $1.HandshakeResponse? protocol,
    Capabilities? capabilities,
  }) {
    final result = HandshakeResponse._();
    if (protocol != null) result.protocol = protocol;
    if (capabilities != null) result.capabilities = capabilities;
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
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: HandshakeResponse.$_createMessage)
    ..aOM<$1.HandshakeResponse>(1, _omitFieldNames ? '' : 'protocol',
        subBuilder: $1.HandshakeResponse.$_createMessage)
    ..aOM<Capabilities>(2, _omitFieldNames ? '' : 'capabilities',
        subBuilder: Capabilities.$_createMessage)
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
  $1.HandshakeResponse get protocol => $_getN(0);
  @$pb.TagNumber(1)
  set protocol($1.HandshakeResponse value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasProtocol() => $_has(0);
  @$pb.TagNumber(1)
  void clearProtocol() => $_clearField(1);
  @$pb.TagNumber(1)
  $1.HandshakeResponse ensureProtocol() => $_ensure(0);

  @$pb.TagNumber(2)
  Capabilities get capabilities => $_getN(1);
  @$pb.TagNumber(2)
  set capabilities(Capabilities value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasCapabilities() => $_has(1);
  @$pb.TagNumber(2)
  void clearCapabilities() => $_clearField(2);
  @$pb.TagNumber(2)
  Capabilities ensureCapabilities() => $_ensure(1);
}

class CreateWorkspaceRequest extends $pb.GeneratedMessage {
  factory CreateWorkspaceRequest({
    $core.String? name,
    FilesystemProfile? profile,
    OperationOptions? operation,
  }) {
    final result = CreateWorkspaceRequest._();
    if (name != null) result.name = name;
    if (profile != null) result.profile = profile;
    if (operation != null) result.operation = operation;
    return result;
  }

  CreateWorkspaceRequest._();

  factory CreateWorkspaceRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateWorkspaceRequest()..mergeFromBuffer(data, registry);
  factory CreateWorkspaceRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateWorkspaceRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateWorkspaceRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CreateWorkspaceRequest.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'name')
    ..aE<FilesystemProfile>(2, _omitFieldNames ? '' : 'profile',
        enumValues: FilesystemProfile.values)
    ..aOM<OperationOptions>(3, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateWorkspaceRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateWorkspaceRequest copyWith(
          void Function(CreateWorkspaceRequest) updates) =>
      super.copyWith((message) => updates(message as CreateWorkspaceRequest))
          as CreateWorkspaceRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use CreateWorkspaceRequest() / CreateWorkspaceRequest.new instead')
  static CreateWorkspaceRequest create() => CreateWorkspaceRequest._();
  static $pb.GeneratedMessage $_createMessage() => CreateWorkspaceRequest._();
  @$core.override
  CreateWorkspaceRequest createEmptyInstance() => CreateWorkspaceRequest._();
  @$core.pragma('dart2js:noInline')
  static CreateWorkspaceRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateWorkspaceRequest>(
          CreateWorkspaceRequest.$_createMessage);
  static CreateWorkspaceRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get name => $_getSZ(0);
  @$pb.TagNumber(1)
  set name($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);

  @$pb.TagNumber(2)
  FilesystemProfile get profile => $_getN(1);
  @$pb.TagNumber(2)
  set profile(FilesystemProfile value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasProfile() => $_has(1);
  @$pb.TagNumber(2)
  void clearProfile() => $_clearField(2);

  @$pb.TagNumber(3)
  OperationOptions get operation => $_getN(2);
  @$pb.TagNumber(3)
  set operation(OperationOptions value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperation() => $_clearField(3);
  @$pb.TagNumber(3)
  OperationOptions ensureOperation() => $_ensure(2);
}

enum OpenWorkspaceRequest_Selector { workspace, name, notSet }

class OpenWorkspaceRequest extends $pb.GeneratedMessage {
  factory OpenWorkspaceRequest({
    WorkspaceRef? workspace,
    $core.String? name,
  }) {
    final result = OpenWorkspaceRequest._();
    if (workspace != null) result.workspace = workspace;
    if (name != null) result.name = name;
    return result;
  }

  OpenWorkspaceRequest._();

  factory OpenWorkspaceRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OpenWorkspaceRequest()..mergeFromBuffer(data, registry);
  factory OpenWorkspaceRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      OpenWorkspaceRequest()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, OpenWorkspaceRequest_Selector>
      _OpenWorkspaceRequest_SelectorByTag = {
    1: OpenWorkspaceRequest_Selector.workspace,
    2: OpenWorkspaceRequest_Selector.name,
    0: OpenWorkspaceRequest_Selector.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'OpenWorkspaceRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: OpenWorkspaceRequest.$_createMessage)
    ..oo(0, [1, 2])
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'name')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OpenWorkspaceRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  OpenWorkspaceRequest copyWith(void Function(OpenWorkspaceRequest) updates) =>
      super.copyWith((message) => updates(message as OpenWorkspaceRequest))
          as OpenWorkspaceRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use OpenWorkspaceRequest() / OpenWorkspaceRequest.new instead')
  static OpenWorkspaceRequest create() => OpenWorkspaceRequest._();
  static $pb.GeneratedMessage $_createMessage() => OpenWorkspaceRequest._();
  @$core.override
  OpenWorkspaceRequest createEmptyInstance() => OpenWorkspaceRequest._();
  @$core.pragma('dart2js:noInline')
  static OpenWorkspaceRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<OpenWorkspaceRequest>(
          OpenWorkspaceRequest.$_createMessage);
  static OpenWorkspaceRequest? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  OpenWorkspaceRequest_Selector whichSelector() =>
      _OpenWorkspaceRequest_SelectorByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  void clearSelector() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get name => $_getSZ(1);
  @$pb.TagNumber(2)
  set name($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => $_clearField(2);
}

class WorkspaceResponse extends $pb.GeneratedMessage {
  factory WorkspaceResponse({
    Workspace? workspace,
    MutationStatus? status,
  }) {
    final result = WorkspaceResponse._();
    if (workspace != null) result.workspace = workspace;
    if (status != null) result.status = status;
    return result;
  }

  WorkspaceResponse._();

  factory WorkspaceResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceResponse()..mergeFromBuffer(data, registry);
  factory WorkspaceResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkspaceResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkspaceResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: WorkspaceResponse.$_createMessage)
    ..aOM<Workspace>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: Workspace.$_createMessage)
    ..aE<MutationStatus>(2, _omitFieldNames ? '' : 'status',
        enumValues: MutationStatus.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkspaceResponse copyWith(void Function(WorkspaceResponse) updates) =>
      super.copyWith((message) => updates(message as WorkspaceResponse))
          as WorkspaceResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WorkspaceResponse() / WorkspaceResponse.new instead')
  static WorkspaceResponse create() => WorkspaceResponse._();
  static $pb.GeneratedMessage $_createMessage() => WorkspaceResponse._();
  @$core.override
  WorkspaceResponse createEmptyInstance() => WorkspaceResponse._();
  @$core.pragma('dart2js:noInline')
  static WorkspaceResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<WorkspaceResponse>(
          WorkspaceResponse.$_createMessage);
  static WorkspaceResponse? _defaultInstance;

  @$pb.TagNumber(1)
  Workspace get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(Workspace value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  Workspace ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  MutationStatus get status => $_getN(1);
  @$pb.TagNumber(2)
  set status(MutationStatus value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasStatus() => $_has(1);
  @$pb.TagNumber(2)
  void clearStatus() => $_clearField(2);
}

class DeleteWorkspaceRequest extends $pb.GeneratedMessage {
  factory DeleteWorkspaceRequest({
    WorkspaceRef? workspace,
    OperationOptions? operation,
  }) {
    final result = DeleteWorkspaceRequest._();
    if (workspace != null) result.workspace = workspace;
    if (operation != null) result.operation = operation;
    return result;
  }

  DeleteWorkspaceRequest._();

  factory DeleteWorkspaceRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteWorkspaceRequest()..mergeFromBuffer(data, registry);
  factory DeleteWorkspaceRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DeleteWorkspaceRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DeleteWorkspaceRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: DeleteWorkspaceRequest.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..aOM<OperationOptions>(2, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteWorkspaceRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DeleteWorkspaceRequest copyWith(
          void Function(DeleteWorkspaceRequest) updates) =>
      super.copyWith((message) => updates(message as DeleteWorkspaceRequest))
          as DeleteWorkspaceRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use DeleteWorkspaceRequest() / DeleteWorkspaceRequest.new instead')
  static DeleteWorkspaceRequest create() => DeleteWorkspaceRequest._();
  static $pb.GeneratedMessage $_createMessage() => DeleteWorkspaceRequest._();
  @$core.override
  DeleteWorkspaceRequest createEmptyInstance() => DeleteWorkspaceRequest._();
  @$core.pragma('dart2js:noInline')
  static DeleteWorkspaceRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DeleteWorkspaceRequest>(
          DeleteWorkspaceRequest.$_createMessage);
  static DeleteWorkspaceRequest? _defaultInstance;

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  OperationOptions get operation => $_getN(1);
  @$pb.TagNumber(2)
  set operation(OperationOptions value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOperation() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperation() => $_clearField(2);
  @$pb.TagNumber(2)
  OperationOptions ensureOperation() => $_ensure(1);
}

class MutationResponse extends $pb.GeneratedMessage {
  factory MutationResponse({
    MutationStatus? status,
    GenerationRef? generation,
    GenerationRef? actualHead,
    $core.Iterable<Conflict>? conflicts,
    $core.bool? truncated,
  }) {
    final result = MutationResponse._();
    if (status != null) result.status = status;
    if (generation != null) result.generation = generation;
    if (actualHead != null) result.actualHead = actualHead;
    if (conflicts != null) result.conflicts.addAll(conflicts);
    if (truncated != null) result.truncated = truncated;
    return result;
  }

  MutationResponse._();

  factory MutationResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationResponse()..mergeFromBuffer(data, registry);
  factory MutationResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      MutationResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'MutationResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: MutationResponse.$_createMessage)
    ..aE<MutationStatus>(1, _omitFieldNames ? '' : 'status',
        enumValues: MutationStatus.values)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOM<GenerationRef>(3, _omitFieldNames ? '' : 'actualHead',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<Conflict>(4, _omitFieldNames ? '' : 'conflicts',
        subBuilder: Conflict.$_createMessage)
    ..aOB(5, _omitFieldNames ? '' : 'truncated')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  MutationResponse copyWith(void Function(MutationResponse) updates) =>
      super.copyWith((message) => updates(message as MutationResponse))
          as MutationResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use MutationResponse() / MutationResponse.new instead')
  static MutationResponse create() => MutationResponse._();
  static $pb.GeneratedMessage $_createMessage() => MutationResponse._();
  @$core.override
  MutationResponse createEmptyInstance() => MutationResponse._();
  @$core.pragma('dart2js:noInline')
  static MutationResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<MutationResponse>(
          MutationResponse.$_createMessage);
  static MutationResponse? _defaultInstance;

  @$pb.TagNumber(1)
  MutationStatus get status => $_getN(0);
  @$pb.TagNumber(1)
  set status(MutationStatus value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStatus() => $_has(0);
  @$pb.TagNumber(1)
  void clearStatus() => $_clearField(1);

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

  @$pb.TagNumber(3)
  GenerationRef get actualHead => $_getN(2);
  @$pb.TagNumber(3)
  set actualHead(GenerationRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasActualHead() => $_has(2);
  @$pb.TagNumber(3)
  void clearActualHead() => $_clearField(3);
  @$pb.TagNumber(3)
  GenerationRef ensureActualHead() => $_ensure(2);

  @$pb.TagNumber(4)
  $pb.PbList<Conflict> get conflicts => $_getList(3);

  @$pb.TagNumber(5)
  $core.bool get truncated => $_getBF(4);
  @$pb.TagNumber(5)
  set truncated($core.bool value) => $_setBool(4, value);
  @$pb.TagNumber(5)
  $core.bool hasTruncated() => $_has(4);
  @$pb.TagNumber(5)
  void clearTruncated() => $_clearField(5);
}

class GetHeadRequest extends $pb.GeneratedMessage {
  factory GetHeadRequest({
    WorkspaceRef? workspace,
  }) {
    final result = GetHeadRequest._();
    if (workspace != null) result.workspace = workspace;
    return result;
  }

  GetHeadRequest._();

  factory GetHeadRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetHeadRequest()..mergeFromBuffer(data, registry);
  factory GetHeadRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetHeadRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GetHeadRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: GetHeadRequest.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetHeadRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetHeadRequest copyWith(void Function(GetHeadRequest) updates) =>
      super.copyWith((message) => updates(message as GetHeadRequest))
          as GetHeadRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use GetHeadRequest() / GetHeadRequest.new instead')
  static GetHeadRequest create() => GetHeadRequest._();
  static $pb.GeneratedMessage $_createMessage() => GetHeadRequest._();
  @$core.override
  GetHeadRequest createEmptyInstance() => GetHeadRequest._();
  @$core.pragma('dart2js:noInline')
  static GetHeadRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<GetHeadRequest>(
          GetHeadRequest.$_createMessage);
  static GetHeadRequest? _defaultInstance;

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);
}

class GetGenerationRequest extends $pb.GeneratedMessage {
  factory GetGenerationRequest({
    GenerationRef? generation,
  }) {
    final result = GetGenerationRequest._();
    if (generation != null) result.generation = generation;
    return result;
  }

  GetGenerationRequest._();

  factory GetGenerationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetGenerationRequest()..mergeFromBuffer(data, registry);
  factory GetGenerationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GetGenerationRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GetGenerationRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: GetGenerationRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetGenerationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GetGenerationRequest copyWith(void Function(GetGenerationRequest) updates) =>
      super.copyWith((message) => updates(message as GetGenerationRequest))
          as GetGenerationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use GetGenerationRequest() / GetGenerationRequest.new instead')
  static GetGenerationRequest create() => GetGenerationRequest._();
  static $pb.GeneratedMessage $_createMessage() => GetGenerationRequest._();
  @$core.override
  GetGenerationRequest createEmptyInstance() => GetGenerationRequest._();
  @$core.pragma('dart2js:noInline')
  static GetGenerationRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<GetGenerationRequest>(
          GetGenerationRequest.$_createMessage);
  static GetGenerationRequest? _defaultInstance;

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
}

class GenerationResponse extends $pb.GeneratedMessage {
  factory GenerationResponse({
    GenerationRef? generation,
    $core.Iterable<GenerationRef>? parents,
  }) {
    final result = GenerationResponse._();
    if (generation != null) result.generation = generation;
    if (parents != null) result.parents.addAll(parents);
    return result;
  }

  GenerationResponse._();

  factory GenerationResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerationResponse()..mergeFromBuffer(data, registry);
  factory GenerationResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      GenerationResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'GenerationResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: GenerationResponse.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<GenerationRef>(2, _omitFieldNames ? '' : 'parents',
        subBuilder: GenerationRef.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerationResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  GenerationResponse copyWith(void Function(GenerationResponse) updates) =>
      super.copyWith((message) => updates(message as GenerationResponse))
          as GenerationResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use GenerationResponse() / GenerationResponse.new instead')
  static GenerationResponse create() => GenerationResponse._();
  static $pb.GeneratedMessage $_createMessage() => GenerationResponse._();
  @$core.override
  GenerationResponse createEmptyInstance() => GenerationResponse._();
  @$core.pragma('dart2js:noInline')
  static GenerationResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<GenerationResponse>(
          GenerationResponse.$_createMessage);
  static GenerationResponse? _defaultInstance;

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
  $pb.PbList<GenerationRef> get parents => $_getList(1);
}

class ReadRequest extends $pb.GeneratedMessage {
  factory ReadRequest({
    GenerationRef? generation,
    $core.String? path,
    ByteRange? range,
    $fixnum.Int64? maximumBytes,
  }) {
    final result = ReadRequest._();
    if (generation != null) result.generation = generation;
    if (path != null) result.path = path;
    if (range != null) result.range = range;
    if (maximumBytes != null) result.maximumBytes = maximumBytes;
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
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ReadRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'path')
    ..aOM<ByteRange>(3, _omitFieldNames ? '' : 'range',
        subBuilder: ByteRange.$_createMessage)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'maximumBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
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
  $core.String get path => $_getSZ(1);
  @$pb.TagNumber(2)
  set path($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPath() => $_has(1);
  @$pb.TagNumber(2)
  void clearPath() => $_clearField(2);

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
  $fixnum.Int64 get maximumBytes => $_getI64(3);
  @$pb.TagNumber(4)
  set maximumBytes($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumBytes() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumBytes() => $_clearField(4);
}

class ReadResponse extends $pb.GeneratedMessage {
  factory ReadResponse({
    $core.List<$core.int>? contents,
  }) {
    final result = ReadResponse._();
    if (contents != null) result.contents = contents;
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
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ReadResponse.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'contents', $pb.PbFieldType.OY)
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
  $core.List<$core.int> get contents => $_getN(0);
  @$pb.TagNumber(1)
  set contents($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasContents() => $_has(0);
  @$pb.TagNumber(1)
  void clearContents() => $_clearField(1);
}

class StatRequest extends $pb.GeneratedMessage {
  factory StatRequest({
    GenerationRef? generation,
    $core.String? path,
  }) {
    final result = StatRequest._();
    if (generation != null) result.generation = generation;
    if (path != null) result.path = path;
    return result;
  }

  StatRequest._();

  factory StatRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      StatRequest()..mergeFromBuffer(data, registry);
  factory StatRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      StatRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'StatRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: StatRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'path')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  StatRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  StatRequest copyWith(void Function(StatRequest) updates) =>
      super.copyWith((message) => updates(message as StatRequest))
          as StatRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use StatRequest() / StatRequest.new instead')
  static StatRequest create() => StatRequest._();
  static $pb.GeneratedMessage $_createMessage() => StatRequest._();
  @$core.override
  StatRequest createEmptyInstance() => StatRequest._();
  @$core.pragma('dart2js:noInline')
  static StatRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<StatRequest>(
          StatRequest.$_createMessage);
  static StatRequest? _defaultInstance;

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
  $core.String get path => $_getSZ(1);
  @$pb.TagNumber(2)
  set path($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPath() => $_has(1);
  @$pb.TagNumber(2)
  void clearPath() => $_clearField(2);
}

class StatResponse extends $pb.GeneratedMessage {
  factory StatResponse({
    FileStat? stat,
  }) {
    final result = StatResponse._();
    if (stat != null) result.stat = stat;
    return result;
  }

  StatResponse._();

  factory StatResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      StatResponse()..mergeFromBuffer(data, registry);
  factory StatResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      StatResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'StatResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: StatResponse.$_createMessage)
    ..aOM<FileStat>(1, _omitFieldNames ? '' : 'stat',
        subBuilder: FileStat.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  StatResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  StatResponse copyWith(void Function(StatResponse) updates) =>
      super.copyWith((message) => updates(message as StatResponse))
          as StatResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use StatResponse() / StatResponse.new instead')
  static StatResponse create() => StatResponse._();
  static $pb.GeneratedMessage $_createMessage() => StatResponse._();
  @$core.override
  StatResponse createEmptyInstance() => StatResponse._();
  @$core.pragma('dart2js:noInline')
  static StatResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<StatResponse>(
          StatResponse.$_createMessage);
  static StatResponse? _defaultInstance;

  @$pb.TagNumber(1)
  FileStat get stat => $_getN(0);
  @$pb.TagNumber(1)
  set stat(FileStat value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStat() => $_has(0);
  @$pb.TagNumber(1)
  void clearStat() => $_clearField(1);
  @$pb.TagNumber(1)
  FileStat ensureStat() => $_ensure(0);
}

class ListDirectoryRequest extends $pb.GeneratedMessage {
  factory ListDirectoryRequest({
    GenerationRef? generation,
    $core.String? path,
    PageOptions? page,
  }) {
    final result = ListDirectoryRequest._();
    if (generation != null) result.generation = generation;
    if (path != null) result.path = path;
    if (page != null) result.page = page;
    return result;
  }

  ListDirectoryRequest._();

  factory ListDirectoryRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListDirectoryRequest()..mergeFromBuffer(data, registry);
  factory ListDirectoryRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListDirectoryRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListDirectoryRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ListDirectoryRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'path')
    ..aOM<PageOptions>(3, _omitFieldNames ? '' : 'page',
        subBuilder: PageOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListDirectoryRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListDirectoryRequest copyWith(void Function(ListDirectoryRequest) updates) =>
      super.copyWith((message) => updates(message as ListDirectoryRequest))
          as ListDirectoryRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ListDirectoryRequest() / ListDirectoryRequest.new instead')
  static ListDirectoryRequest create() => ListDirectoryRequest._();
  static $pb.GeneratedMessage $_createMessage() => ListDirectoryRequest._();
  @$core.override
  ListDirectoryRequest createEmptyInstance() => ListDirectoryRequest._();
  @$core.pragma('dart2js:noInline')
  static ListDirectoryRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ListDirectoryRequest>(
          ListDirectoryRequest.$_createMessage);
  static ListDirectoryRequest? _defaultInstance;

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
  $core.String get path => $_getSZ(1);
  @$pb.TagNumber(2)
  set path($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPath() => $_has(1);
  @$pb.TagNumber(2)
  void clearPath() => $_clearField(2);

  @$pb.TagNumber(3)
  PageOptions get page => $_getN(2);
  @$pb.TagNumber(3)
  set page(PageOptions value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasPage() => $_has(2);
  @$pb.TagNumber(3)
  void clearPage() => $_clearField(3);
  @$pb.TagNumber(3)
  PageOptions ensurePage() => $_ensure(2);
}

class ListDirectoryResponse extends $pb.GeneratedMessage {
  factory ListDirectoryResponse({
    DirectoryPage? page,
  }) {
    final result = ListDirectoryResponse._();
    if (page != null) result.page = page;
    return result;
  }

  ListDirectoryResponse._();

  factory ListDirectoryResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListDirectoryResponse()..mergeFromBuffer(data, registry);
  factory ListDirectoryResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ListDirectoryResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ListDirectoryResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ListDirectoryResponse.$_createMessage)
    ..aOM<DirectoryPage>(1, _omitFieldNames ? '' : 'page',
        subBuilder: DirectoryPage.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListDirectoryResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ListDirectoryResponse copyWith(
          void Function(ListDirectoryResponse) updates) =>
      super.copyWith((message) => updates(message as ListDirectoryResponse))
          as ListDirectoryResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ListDirectoryResponse() / ListDirectoryResponse.new instead')
  static ListDirectoryResponse create() => ListDirectoryResponse._();
  static $pb.GeneratedMessage $_createMessage() => ListDirectoryResponse._();
  @$core.override
  ListDirectoryResponse createEmptyInstance() => ListDirectoryResponse._();
  @$core.pragma('dart2js:noInline')
  static ListDirectoryResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ListDirectoryResponse>(
          ListDirectoryResponse.$_createMessage);
  static ListDirectoryResponse? _defaultInstance;

  @$pb.TagNumber(1)
  DirectoryPage get page => $_getN(0);
  @$pb.TagNumber(1)
  set page(DirectoryPage value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasPage() => $_has(0);
  @$pb.TagNumber(1)
  void clearPage() => $_clearField(1);
  @$pb.TagNumber(1)
  DirectoryPage ensurePage() => $_ensure(0);
}

class ReadLinkRequest extends $pb.GeneratedMessage {
  factory ReadLinkRequest({
    GenerationRef? generation,
    $core.String? path,
    $fixnum.Int64? maximumBytes,
  }) {
    final result = ReadLinkRequest._();
    if (generation != null) result.generation = generation;
    if (path != null) result.path = path;
    if (maximumBytes != null) result.maximumBytes = maximumBytes;
    return result;
  }

  ReadLinkRequest._();

  factory ReadLinkRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadLinkRequest()..mergeFromBuffer(data, registry);
  factory ReadLinkRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ReadLinkRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ReadLinkRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ReadLinkRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'path')
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'maximumBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadLinkRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ReadLinkRequest copyWith(void Function(ReadLinkRequest) updates) =>
      super.copyWith((message) => updates(message as ReadLinkRequest))
          as ReadLinkRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ReadLinkRequest() / ReadLinkRequest.new instead')
  static ReadLinkRequest create() => ReadLinkRequest._();
  static $pb.GeneratedMessage $_createMessage() => ReadLinkRequest._();
  @$core.override
  ReadLinkRequest createEmptyInstance() => ReadLinkRequest._();
  @$core.pragma('dart2js:noInline')
  static ReadLinkRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ReadLinkRequest>(
          ReadLinkRequest.$_createMessage);
  static ReadLinkRequest? _defaultInstance;

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
  $core.String get path => $_getSZ(1);
  @$pb.TagNumber(2)
  set path($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPath() => $_has(1);
  @$pb.TagNumber(2)
  void clearPath() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get maximumBytes => $_getI64(2);
  @$pb.TagNumber(3)
  set maximumBytes($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumBytes() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumBytes() => $_clearField(3);
}

class PlanExtentsRequest extends $pb.GeneratedMessage {
  factory PlanExtentsRequest({
    GenerationRef? generation,
    $core.String? path,
    ByteRange? range,
    $core.int? maximumExtents,
  }) {
    final result = PlanExtentsRequest._();
    if (generation != null) result.generation = generation;
    if (path != null) result.path = path;
    if (range != null) result.range = range;
    if (maximumExtents != null) result.maximumExtents = maximumExtents;
    return result;
  }

  PlanExtentsRequest._();

  factory PlanExtentsRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PlanExtentsRequest()..mergeFromBuffer(data, registry);
  factory PlanExtentsRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PlanExtentsRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PlanExtentsRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: PlanExtentsRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'path')
    ..aOM<ByteRange>(3, _omitFieldNames ? '' : 'range',
        subBuilder: ByteRange.$_createMessage)
    ..aI(4, _omitFieldNames ? '' : 'maximumExtents',
        fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PlanExtentsRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PlanExtentsRequest copyWith(void Function(PlanExtentsRequest) updates) =>
      super.copyWith((message) => updates(message as PlanExtentsRequest))
          as PlanExtentsRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use PlanExtentsRequest() / PlanExtentsRequest.new instead')
  static PlanExtentsRequest create() => PlanExtentsRequest._();
  static $pb.GeneratedMessage $_createMessage() => PlanExtentsRequest._();
  @$core.override
  PlanExtentsRequest createEmptyInstance() => PlanExtentsRequest._();
  @$core.pragma('dart2js:noInline')
  static PlanExtentsRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PlanExtentsRequest>(
          PlanExtentsRequest.$_createMessage);
  static PlanExtentsRequest? _defaultInstance;

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
  $core.String get path => $_getSZ(1);
  @$pb.TagNumber(2)
  set path($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasPath() => $_has(1);
  @$pb.TagNumber(2)
  void clearPath() => $_clearField(2);

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
  $core.int get maximumExtents => $_getIZ(3);
  @$pb.TagNumber(4)
  set maximumExtents($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumExtents() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumExtents() => $_clearField(4);
}

class PlanExtentsResponse extends $pb.GeneratedMessage {
  factory PlanExtentsResponse({
    $core.Iterable<Extent>? extents,
    $core.bool? truncated,
  }) {
    final result = PlanExtentsResponse._();
    if (extents != null) result.extents.addAll(extents);
    if (truncated != null) result.truncated = truncated;
    return result;
  }

  PlanExtentsResponse._();

  factory PlanExtentsResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PlanExtentsResponse()..mergeFromBuffer(data, registry);
  factory PlanExtentsResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PlanExtentsResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PlanExtentsResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: PlanExtentsResponse.$_createMessage)
    ..pPM<Extent>(1, _omitFieldNames ? '' : 'extents',
        subBuilder: Extent.$_createMessage)
    ..aOB(2, _omitFieldNames ? '' : 'truncated')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PlanExtentsResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PlanExtentsResponse copyWith(void Function(PlanExtentsResponse) updates) =>
      super.copyWith((message) => updates(message as PlanExtentsResponse))
          as PlanExtentsResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core
      .Deprecated('Use PlanExtentsResponse() / PlanExtentsResponse.new instead')
  static PlanExtentsResponse create() => PlanExtentsResponse._();
  static $pb.GeneratedMessage $_createMessage() => PlanExtentsResponse._();
  @$core.override
  PlanExtentsResponse createEmptyInstance() => PlanExtentsResponse._();
  @$core.pragma('dart2js:noInline')
  static PlanExtentsResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PlanExtentsResponse>(
          PlanExtentsResponse.$_createMessage);
  static PlanExtentsResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $pb.PbList<Extent> get extents => $_getList(0);

  @$pb.TagNumber(2)
  $core.bool get truncated => $_getBF(1);
  @$pb.TagNumber(2)
  set truncated($core.bool value) => $_setBool(1, value);
  @$pb.TagNumber(2)
  $core.bool hasTruncated() => $_has(1);
  @$pb.TagNumber(2)
  void clearTruncated() => $_clearField(2);
}

class CreateFile extends $pb.GeneratedMessage {
  factory CreateFile({
    $core.String? path,
    $core.List<$core.int>? contents,
    Metadata? metadata,
  }) {
    final result = CreateFile._();
    if (path != null) result.path = path;
    if (contents != null) result.contents = contents;
    if (metadata != null) result.metadata = metadata;
    return result;
  }

  CreateFile._();

  factory CreateFile.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateFile()..mergeFromBuffer(data, registry);
  factory CreateFile.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateFile()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateFile',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CreateFile.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'contents', $pb.PbFieldType.OY)
    ..aOM<Metadata>(3, _omitFieldNames ? '' : 'metadata',
        subBuilder: Metadata.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateFile clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateFile copyWith(void Function(CreateFile) updates) =>
      super.copyWith((message) => updates(message as CreateFile)) as CreateFile;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CreateFile() / CreateFile.new instead')
  static CreateFile create() => CreateFile._();
  static $pb.GeneratedMessage $_createMessage() => CreateFile._();
  @$core.override
  CreateFile createEmptyInstance() => CreateFile._();
  @$core.pragma('dart2js:noInline')
  static CreateFile getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateFile>(CreateFile.$_createMessage);
  static CreateFile? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get contents => $_getN(1);
  @$pb.TagNumber(2)
  set contents($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasContents() => $_has(1);
  @$pb.TagNumber(2)
  void clearContents() => $_clearField(2);

  @$pb.TagNumber(3)
  Metadata get metadata => $_getN(2);
  @$pb.TagNumber(3)
  set metadata(Metadata value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMetadata() => $_has(2);
  @$pb.TagNumber(3)
  void clearMetadata() => $_clearField(3);
  @$pb.TagNumber(3)
  Metadata ensureMetadata() => $_ensure(2);
}

class CreateDirectory extends $pb.GeneratedMessage {
  factory CreateDirectory({
    $core.String? path,
    Metadata? metadata,
  }) {
    final result = CreateDirectory._();
    if (path != null) result.path = path;
    if (metadata != null) result.metadata = metadata;
    return result;
  }

  CreateDirectory._();

  factory CreateDirectory.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateDirectory()..mergeFromBuffer(data, registry);
  factory CreateDirectory.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateDirectory()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateDirectory',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CreateDirectory.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..aOM<Metadata>(2, _omitFieldNames ? '' : 'metadata',
        subBuilder: Metadata.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateDirectory clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateDirectory copyWith(void Function(CreateDirectory) updates) =>
      super.copyWith((message) => updates(message as CreateDirectory))
          as CreateDirectory;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CreateDirectory() / CreateDirectory.new instead')
  static CreateDirectory create() => CreateDirectory._();
  static $pb.GeneratedMessage $_createMessage() => CreateDirectory._();
  @$core.override
  CreateDirectory createEmptyInstance() => CreateDirectory._();
  @$core.pragma('dart2js:noInline')
  static CreateDirectory getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CreateDirectory>(
          CreateDirectory.$_createMessage);
  static CreateDirectory? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  Metadata get metadata => $_getN(1);
  @$pb.TagNumber(2)
  set metadata(Metadata value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMetadata() => $_has(1);
  @$pb.TagNumber(2)
  void clearMetadata() => $_clearField(2);
  @$pb.TagNumber(2)
  Metadata ensureMetadata() => $_ensure(1);
}

class CreateSymbolicLink extends $pb.GeneratedMessage {
  factory CreateSymbolicLink({
    $core.String? path,
    $core.List<$core.int>? target,
    Metadata? metadata,
  }) {
    final result = CreateSymbolicLink._();
    if (path != null) result.path = path;
    if (target != null) result.target = target;
    if (metadata != null) result.metadata = metadata;
    return result;
  }

  CreateSymbolicLink._();

  factory CreateSymbolicLink.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateSymbolicLink()..mergeFromBuffer(data, registry);
  factory CreateSymbolicLink.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateSymbolicLink()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateSymbolicLink',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CreateSymbolicLink.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'target', $pb.PbFieldType.OY)
    ..aOM<Metadata>(3, _omitFieldNames ? '' : 'metadata',
        subBuilder: Metadata.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateSymbolicLink clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateSymbolicLink copyWith(void Function(CreateSymbolicLink) updates) =>
      super.copyWith((message) => updates(message as CreateSymbolicLink))
          as CreateSymbolicLink;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CreateSymbolicLink() / CreateSymbolicLink.new instead')
  static CreateSymbolicLink create() => CreateSymbolicLink._();
  static $pb.GeneratedMessage $_createMessage() => CreateSymbolicLink._();
  @$core.override
  CreateSymbolicLink createEmptyInstance() => CreateSymbolicLink._();
  @$core.pragma('dart2js:noInline')
  static CreateSymbolicLink getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CreateSymbolicLink>(
          CreateSymbolicLink.$_createMessage);
  static CreateSymbolicLink? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get target => $_getN(1);
  @$pb.TagNumber(2)
  set target($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasTarget() => $_has(1);
  @$pb.TagNumber(2)
  void clearTarget() => $_clearField(2);

  @$pb.TagNumber(3)
  Metadata get metadata => $_getN(2);
  @$pb.TagNumber(3)
  set metadata(Metadata value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasMetadata() => $_has(2);
  @$pb.TagNumber(3)
  void clearMetadata() => $_clearField(3);
  @$pb.TagNumber(3)
  Metadata ensureMetadata() => $_ensure(2);
}

class Remove extends $pb.GeneratedMessage {
  factory Remove({
    $core.String? path,
  }) {
    final result = Remove._();
    if (path != null) result.path = path;
    return result;
  }

  Remove._();

  factory Remove.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Remove()..mergeFromBuffer(data, registry);
  factory Remove.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Remove()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Remove',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Remove.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Remove clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Remove copyWith(void Function(Remove) updates) =>
      super.copyWith((message) => updates(message as Remove)) as Remove;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Remove() / Remove.new instead')
  static Remove create() => Remove._();
  static $pb.GeneratedMessage $_createMessage() => Remove._();
  @$core.override
  Remove createEmptyInstance() => Remove._();
  @$core.pragma('dart2js:noInline')
  static Remove getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Remove>(Remove.$_createMessage);
  static Remove? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);
}

class Rename extends $pb.GeneratedMessage {
  factory Rename({
    $core.String? source,
    $core.String? destination,
    $core.bool? replace,
  }) {
    final result = Rename._();
    if (source != null) result.source = source;
    if (destination != null) result.destination = destination;
    if (replace != null) result.replace = replace;
    return result;
  }

  Rename._();

  factory Rename.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Rename()..mergeFromBuffer(data, registry);
  factory Rename.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Rename()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Rename',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Rename.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'source')
    ..aOS(2, _omitFieldNames ? '' : 'destination')
    ..aOB(3, _omitFieldNames ? '' : 'replace')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Rename clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Rename copyWith(void Function(Rename) updates) =>
      super.copyWith((message) => updates(message as Rename)) as Rename;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Rename() / Rename.new instead')
  static Rename create() => Rename._();
  static $pb.GeneratedMessage $_createMessage() => Rename._();
  @$core.override
  Rename createEmptyInstance() => Rename._();
  @$core.pragma('dart2js:noInline')
  static Rename getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Rename>(Rename.$_createMessage);
  static Rename? _defaultInstance;

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
  $core.bool get replace => $_getBF(2);
  @$pb.TagNumber(3)
  set replace($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasReplace() => $_has(2);
  @$pb.TagNumber(3)
  void clearReplace() => $_clearField(3);
}

class HardLink extends $pb.GeneratedMessage {
  factory HardLink({
    $core.String? source,
    $core.String? destination,
  }) {
    final result = HardLink._();
    if (source != null) result.source = source;
    if (destination != null) result.destination = destination;
    return result;
  }

  HardLink._();

  factory HardLink.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HardLink()..mergeFromBuffer(data, registry);
  factory HardLink.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      HardLink()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'HardLink',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: HardLink.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'source')
    ..aOS(2, _omitFieldNames ? '' : 'destination')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HardLink clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  HardLink copyWith(void Function(HardLink) updates) =>
      super.copyWith((message) => updates(message as HardLink)) as HardLink;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use HardLink() / HardLink.new instead')
  static HardLink create() => HardLink._();
  static $pb.GeneratedMessage $_createMessage() => HardLink._();
  @$core.override
  HardLink createEmptyInstance() => HardLink._();
  @$core.pragma('dart2js:noInline')
  static HardLink getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<HardLink>(HardLink.$_createMessage);
  static HardLink? _defaultInstance;

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
}

class Write extends $pb.GeneratedMessage {
  factory Write({
    $core.String? path,
    $fixnum.Int64? offset,
    $core.List<$core.int>? contents,
  }) {
    final result = Write._();
    if (path != null) result.path = path;
    if (offset != null) result.offset = offset;
    if (contents != null) result.contents = contents;
    return result;
  }

  Write._();

  factory Write.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Write()..mergeFromBuffer(data, registry);
  factory Write.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Write()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Write',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Write.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'offset', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'contents', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Write clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Write copyWith(void Function(Write) updates) =>
      super.copyWith((message) => updates(message as Write)) as Write;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Write() / Write.new instead')
  static Write create() => Write._();
  static $pb.GeneratedMessage $_createMessage() => Write._();
  @$core.override
  Write createEmptyInstance() => Write._();
  @$core.pragma('dart2js:noInline')
  static Write getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Write>(Write.$_createMessage);
  static Write? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get offset => $_getI64(1);
  @$pb.TagNumber(2)
  set offset($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasOffset() => $_has(1);
  @$pb.TagNumber(2)
  void clearOffset() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get contents => $_getN(2);
  @$pb.TagNumber(3)
  set contents($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasContents() => $_has(2);
  @$pb.TagNumber(3)
  void clearContents() => $_clearField(3);
}

class Resize extends $pb.GeneratedMessage {
  factory Resize({
    $core.String? path,
    $fixnum.Int64? logicalBytes,
  }) {
    final result = Resize._();
    if (path != null) result.path = path;
    if (logicalBytes != null) result.logicalBytes = logicalBytes;
    return result;
  }

  Resize._();

  factory Resize.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Resize()..mergeFromBuffer(data, registry);
  factory Resize.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Resize()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Resize',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Resize.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'logicalBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Resize clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Resize copyWith(void Function(Resize) updates) =>
      super.copyWith((message) => updates(message as Resize)) as Resize;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Resize() / Resize.new instead')
  static Resize create() => Resize._();
  static $pb.GeneratedMessage $_createMessage() => Resize._();
  @$core.override
  Resize createEmptyInstance() => Resize._();
  @$core.pragma('dart2js:noInline')
  static Resize getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Resize>(Resize.$_createMessage);
  static Resize? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get logicalBytes => $_getI64(1);
  @$pb.TagNumber(2)
  set logicalBytes($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasLogicalBytes() => $_has(1);
  @$pb.TagNumber(2)
  void clearLogicalBytes() => $_clearField(2);
}

class ZeroRange extends $pb.GeneratedMessage {
  factory ZeroRange({
    $core.String? path,
    ByteRange? range,
    $core.bool? allocated,
    $core.bool? extend,
  }) {
    final result = ZeroRange._();
    if (path != null) result.path = path;
    if (range != null) result.range = range;
    if (allocated != null) result.allocated = allocated;
    if (extend != null) result.extend = extend;
    return result;
  }

  ZeroRange._();

  factory ZeroRange.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ZeroRange()..mergeFromBuffer(data, registry);
  factory ZeroRange.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ZeroRange()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ZeroRange',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ZeroRange.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..aOM<ByteRange>(2, _omitFieldNames ? '' : 'range',
        subBuilder: ByteRange.$_createMessage)
    ..aOB(3, _omitFieldNames ? '' : 'allocated')
    ..aOB(4, _omitFieldNames ? '' : 'extend')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ZeroRange clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ZeroRange copyWith(void Function(ZeroRange) updates) =>
      super.copyWith((message) => updates(message as ZeroRange)) as ZeroRange;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ZeroRange() / ZeroRange.new instead')
  static ZeroRange create() => ZeroRange._();
  static $pb.GeneratedMessage $_createMessage() => ZeroRange._();
  @$core.override
  ZeroRange createEmptyInstance() => ZeroRange._();
  @$core.pragma('dart2js:noInline')
  static ZeroRange getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ZeroRange>(ZeroRange.$_createMessage);
  static ZeroRange? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  ByteRange get range => $_getN(1);
  @$pb.TagNumber(2)
  set range(ByteRange value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasRange() => $_has(1);
  @$pb.TagNumber(2)
  void clearRange() => $_clearField(2);
  @$pb.TagNumber(2)
  ByteRange ensureRange() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.bool get allocated => $_getBF(2);
  @$pb.TagNumber(3)
  set allocated($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasAllocated() => $_has(2);
  @$pb.TagNumber(3)
  void clearAllocated() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.bool get extend => $_getBF(3);
  @$pb.TagNumber(4)
  set extend($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasExtend() => $_has(3);
  @$pb.TagNumber(4)
  void clearExtend() => $_clearField(4);
}

class Preallocate extends $pb.GeneratedMessage {
  factory Preallocate({
    $core.String? path,
    ByteRange? range,
    $core.bool? keepSize,
  }) {
    final result = Preallocate._();
    if (path != null) result.path = path;
    if (range != null) result.range = range;
    if (keepSize != null) result.keepSize = keepSize;
    return result;
  }

  Preallocate._();

  factory Preallocate.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Preallocate()..mergeFromBuffer(data, registry);
  factory Preallocate.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Preallocate()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Preallocate',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Preallocate.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..aOM<ByteRange>(2, _omitFieldNames ? '' : 'range',
        subBuilder: ByteRange.$_createMessage)
    ..aOB(3, _omitFieldNames ? '' : 'keepSize')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Preallocate clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Preallocate copyWith(void Function(Preallocate) updates) =>
      super.copyWith((message) => updates(message as Preallocate))
          as Preallocate;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Preallocate() / Preallocate.new instead')
  static Preallocate create() => Preallocate._();
  static $pb.GeneratedMessage $_createMessage() => Preallocate._();
  @$core.override
  Preallocate createEmptyInstance() => Preallocate._();
  @$core.pragma('dart2js:noInline')
  static Preallocate getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Preallocate>(
          Preallocate.$_createMessage);
  static Preallocate? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  ByteRange get range => $_getN(1);
  @$pb.TagNumber(2)
  set range(ByteRange value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasRange() => $_has(1);
  @$pb.TagNumber(2)
  void clearRange() => $_clearField(2);
  @$pb.TagNumber(2)
  ByteRange ensureRange() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.bool get keepSize => $_getBF(2);
  @$pb.TagNumber(3)
  set keepSize($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasKeepSize() => $_has(2);
  @$pb.TagNumber(3)
  void clearKeepSize() => $_clearField(3);
}

class CloneRange extends $pb.GeneratedMessage {
  factory CloneRange({
    $core.String? source,
    $fixnum.Int64? sourceOffset,
    $core.String? destination,
    $fixnum.Int64? destinationOffset,
    $fixnum.Int64? length,
  }) {
    final result = CloneRange._();
    if (source != null) result.source = source;
    if (sourceOffset != null) result.sourceOffset = sourceOffset;
    if (destination != null) result.destination = destination;
    if (destinationOffset != null) result.destinationOffset = destinationOffset;
    if (length != null) result.length = length;
    return result;
  }

  CloneRange._();

  factory CloneRange.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CloneRange()..mergeFromBuffer(data, registry);
  factory CloneRange.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CloneRange()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CloneRange',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CloneRange.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'source')
    ..a<$fixnum.Int64>(
        2, _omitFieldNames ? '' : 'sourceOffset', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(3, _omitFieldNames ? '' : 'destination')
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'destinationOffset', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(5, _omitFieldNames ? '' : 'length', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CloneRange clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CloneRange copyWith(void Function(CloneRange) updates) =>
      super.copyWith((message) => updates(message as CloneRange)) as CloneRange;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CloneRange() / CloneRange.new instead')
  static CloneRange create() => CloneRange._();
  static $pb.GeneratedMessage $_createMessage() => CloneRange._();
  @$core.override
  CloneRange createEmptyInstance() => CloneRange._();
  @$core.pragma('dart2js:noInline')
  static CloneRange getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CloneRange>(CloneRange.$_createMessage);
  static CloneRange? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get source => $_getSZ(0);
  @$pb.TagNumber(1)
  set source($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get sourceOffset => $_getI64(1);
  @$pb.TagNumber(2)
  set sourceOffset($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasSourceOffset() => $_has(1);
  @$pb.TagNumber(2)
  void clearSourceOffset() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get destination => $_getSZ(2);
  @$pb.TagNumber(3)
  set destination($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasDestination() => $_has(2);
  @$pb.TagNumber(3)
  void clearDestination() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get destinationOffset => $_getI64(3);
  @$pb.TagNumber(4)
  set destinationOffset($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasDestinationOffset() => $_has(3);
  @$pb.TagNumber(4)
  void clearDestinationOffset() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get length => $_getI64(4);
  @$pb.TagNumber(5)
  set length($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasLength() => $_has(4);
  @$pb.TagNumber(5)
  void clearLength() => $_clearField(5);
}

class SetMetadata extends $pb.GeneratedMessage {
  factory SetMetadata({
    $core.String? path,
    Metadata? metadata,
  }) {
    final result = SetMetadata._();
    if (path != null) result.path = path;
    if (metadata != null) result.metadata = metadata;
    return result;
  }

  SetMetadata._();

  factory SetMetadata.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SetMetadata()..mergeFromBuffer(data, registry);
  factory SetMetadata.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SetMetadata()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SetMetadata',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: SetMetadata.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..aOM<Metadata>(2, _omitFieldNames ? '' : 'metadata',
        subBuilder: Metadata.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SetMetadata clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SetMetadata copyWith(void Function(SetMetadata) updates) =>
      super.copyWith((message) => updates(message as SetMetadata))
          as SetMetadata;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SetMetadata() / SetMetadata.new instead')
  static SetMetadata create() => SetMetadata._();
  static $pb.GeneratedMessage $_createMessage() => SetMetadata._();
  @$core.override
  SetMetadata createEmptyInstance() => SetMetadata._();
  @$core.pragma('dart2js:noInline')
  static SetMetadata getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SetMetadata>(
          SetMetadata.$_createMessage);
  static SetMetadata? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  Metadata get metadata => $_getN(1);
  @$pb.TagNumber(2)
  set metadata(Metadata value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMetadata() => $_has(1);
  @$pb.TagNumber(2)
  void clearMetadata() => $_clearField(2);
  @$pb.TagNumber(2)
  Metadata ensureMetadata() => $_ensure(1);
}

class CreateDirectories extends $pb.GeneratedMessage {
  factory CreateDirectories({
    $core.String? path,
  }) {
    final result = CreateDirectories._();
    if (path != null) result.path = path;
    return result;
  }

  CreateDirectories._();

  factory CreateDirectories.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateDirectories()..mergeFromBuffer(data, registry);
  factory CreateDirectories.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CreateDirectories()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CreateDirectories',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CreateDirectories.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateDirectories clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CreateDirectories copyWith(void Function(CreateDirectories) updates) =>
      super.copyWith((message) => updates(message as CreateDirectories))
          as CreateDirectories;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CreateDirectories() / CreateDirectories.new instead')
  static CreateDirectories create() => CreateDirectories._();
  static $pb.GeneratedMessage $_createMessage() => CreateDirectories._();
  @$core.override
  CreateDirectories createEmptyInstance() => CreateDirectories._();
  @$core.pragma('dart2js:noInline')
  static CreateDirectories getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CreateDirectories>(
          CreateDirectories.$_createMessage);
  static CreateDirectories? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);
}

class PutFile extends $pb.GeneratedMessage {
  factory PutFile({
    $core.String? path,
    $core.List<$core.int>? contents,
  }) {
    final result = PutFile._();
    if (path != null) result.path = path;
    if (contents != null) result.contents = contents;
    return result;
  }

  PutFile._();

  factory PutFile.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PutFile()..mergeFromBuffer(data, registry);
  factory PutFile.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PutFile()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PutFile',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: PutFile.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'path')
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'contents', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PutFile clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PutFile copyWith(void Function(PutFile) updates) =>
      super.copyWith((message) => updates(message as PutFile)) as PutFile;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use PutFile() / PutFile.new instead')
  static PutFile create() => PutFile._();
  static $pb.GeneratedMessage $_createMessage() => PutFile._();
  @$core.override
  PutFile createEmptyInstance() => PutFile._();
  @$core.pragma('dart2js:noInline')
  static PutFile getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<PutFile>(PutFile.$_createMessage);
  static PutFile? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get path => $_getSZ(0);
  @$pb.TagNumber(1)
  set path($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPath() => $_has(0);
  @$pb.TagNumber(1)
  void clearPath() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get contents => $_getN(1);
  @$pb.TagNumber(2)
  set contents($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasContents() => $_has(1);
  @$pb.TagNumber(2)
  void clearContents() => $_clearField(2);
}

class CopyFile extends $pb.GeneratedMessage {
  factory CopyFile({
    $core.String? source,
    $core.String? destination,
  }) {
    final result = CopyFile._();
    if (source != null) result.source = source;
    if (destination != null) result.destination = destination;
    return result;
  }

  CopyFile._();

  factory CopyFile.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CopyFile()..mergeFromBuffer(data, registry);
  factory CopyFile.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CopyFile()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CopyFile',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CopyFile.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'source')
    ..aOS(2, _omitFieldNames ? '' : 'destination')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CopyFile clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CopyFile copyWith(void Function(CopyFile) updates) =>
      super.copyWith((message) => updates(message as CopyFile)) as CopyFile;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CopyFile() / CopyFile.new instead')
  static CopyFile create() => CopyFile._();
  static $pb.GeneratedMessage $_createMessage() => CopyFile._();
  @$core.override
  CopyFile createEmptyInstance() => CopyFile._();
  @$core.pragma('dart2js:noInline')
  static CopyFile getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CopyFile>(CopyFile.$_createMessage);
  static CopyFile? _defaultInstance;

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
}

enum Mutation_Mutation {
  createFile,
  createDirectory,
  createSymbolicLink,
  remove,
  rename,
  hardLink,
  write,
  resize,
  zeroRange,
  preallocate,
  cloneRange,
  setMetadata,
  createDirectories,
  putFile,
  copyFile,
  notSet
}

class Mutation extends $pb.GeneratedMessage {
  factory Mutation({
    CreateFile? createFile,
    CreateDirectory? createDirectory,
    CreateSymbolicLink? createSymbolicLink,
    Remove? remove,
    Rename? rename,
    HardLink? hardLink,
    Write? write,
    Resize? resize,
    ZeroRange? zeroRange,
    Preallocate? preallocate,
    CloneRange? cloneRange,
    SetMetadata? setMetadata,
    CreateDirectories? createDirectories,
    PutFile? putFile,
    CopyFile? copyFile,
  }) {
    final result = Mutation._();
    if (createFile != null) result.createFile = createFile;
    if (createDirectory != null) result.createDirectory = createDirectory;
    if (createSymbolicLink != null)
      result.createSymbolicLink = createSymbolicLink;
    if (remove != null) result.remove = remove;
    if (rename != null) result.rename = rename;
    if (hardLink != null) result.hardLink = hardLink;
    if (write != null) result.write = write;
    if (resize != null) result.resize = resize;
    if (zeroRange != null) result.zeroRange = zeroRange;
    if (preallocate != null) result.preallocate = preallocate;
    if (cloneRange != null) result.cloneRange = cloneRange;
    if (setMetadata != null) result.setMetadata = setMetadata;
    if (createDirectories != null) result.createDirectories = createDirectories;
    if (putFile != null) result.putFile = putFile;
    if (copyFile != null) result.copyFile = copyFile;
    return result;
  }

  Mutation._();

  factory Mutation.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Mutation()..mergeFromBuffer(data, registry);
  factory Mutation.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Mutation()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, Mutation_Mutation> _Mutation_MutationByTag =
      {
    1: Mutation_Mutation.createFile,
    2: Mutation_Mutation.createDirectory,
    3: Mutation_Mutation.createSymbolicLink,
    4: Mutation_Mutation.remove,
    5: Mutation_Mutation.rename,
    6: Mutation_Mutation.hardLink,
    7: Mutation_Mutation.write,
    8: Mutation_Mutation.resize,
    9: Mutation_Mutation.zeroRange,
    10: Mutation_Mutation.preallocate,
    11: Mutation_Mutation.cloneRange,
    12: Mutation_Mutation.setMetadata,
    13: Mutation_Mutation.createDirectories,
    14: Mutation_Mutation.putFile,
    15: Mutation_Mutation.copyFile,
    0: Mutation_Mutation.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Mutation',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Mutation.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15])
    ..aOM<CreateFile>(1, _omitFieldNames ? '' : 'createFile',
        subBuilder: CreateFile.$_createMessage)
    ..aOM<CreateDirectory>(2, _omitFieldNames ? '' : 'createDirectory',
        subBuilder: CreateDirectory.$_createMessage)
    ..aOM<CreateSymbolicLink>(3, _omitFieldNames ? '' : 'createSymbolicLink',
        subBuilder: CreateSymbolicLink.$_createMessage)
    ..aOM<Remove>(4, _omitFieldNames ? '' : 'remove',
        subBuilder: Remove.$_createMessage)
    ..aOM<Rename>(5, _omitFieldNames ? '' : 'rename',
        subBuilder: Rename.$_createMessage)
    ..aOM<HardLink>(6, _omitFieldNames ? '' : 'hardLink',
        subBuilder: HardLink.$_createMessage)
    ..aOM<Write>(7, _omitFieldNames ? '' : 'write',
        subBuilder: Write.$_createMessage)
    ..aOM<Resize>(8, _omitFieldNames ? '' : 'resize',
        subBuilder: Resize.$_createMessage)
    ..aOM<ZeroRange>(9, _omitFieldNames ? '' : 'zeroRange',
        subBuilder: ZeroRange.$_createMessage)
    ..aOM<Preallocate>(10, _omitFieldNames ? '' : 'preallocate',
        subBuilder: Preallocate.$_createMessage)
    ..aOM<CloneRange>(11, _omitFieldNames ? '' : 'cloneRange',
        subBuilder: CloneRange.$_createMessage)
    ..aOM<SetMetadata>(12, _omitFieldNames ? '' : 'setMetadata',
        subBuilder: SetMetadata.$_createMessage)
    ..aOM<CreateDirectories>(13, _omitFieldNames ? '' : 'createDirectories',
        subBuilder: CreateDirectories.$_createMessage)
    ..aOM<PutFile>(14, _omitFieldNames ? '' : 'putFile',
        subBuilder: PutFile.$_createMessage)
    ..aOM<CopyFile>(15, _omitFieldNames ? '' : 'copyFile',
        subBuilder: CopyFile.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Mutation clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Mutation copyWith(void Function(Mutation) updates) =>
      super.copyWith((message) => updates(message as Mutation)) as Mutation;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Mutation() / Mutation.new instead')
  static Mutation create() => Mutation._();
  static $pb.GeneratedMessage $_createMessage() => Mutation._();
  @$core.override
  Mutation createEmptyInstance() => Mutation._();
  @$core.pragma('dart2js:noInline')
  static Mutation getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Mutation>(Mutation.$_createMessage);
  static Mutation? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  @$pb.TagNumber(8)
  @$pb.TagNumber(9)
  @$pb.TagNumber(10)
  @$pb.TagNumber(11)
  @$pb.TagNumber(12)
  @$pb.TagNumber(13)
  @$pb.TagNumber(14)
  @$pb.TagNumber(15)
  Mutation_Mutation whichMutation() =>
      _Mutation_MutationByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  @$pb.TagNumber(8)
  @$pb.TagNumber(9)
  @$pb.TagNumber(10)
  @$pb.TagNumber(11)
  @$pb.TagNumber(12)
  @$pb.TagNumber(13)
  @$pb.TagNumber(14)
  @$pb.TagNumber(15)
  void clearMutation() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  CreateFile get createFile => $_getN(0);
  @$pb.TagNumber(1)
  set createFile(CreateFile value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasCreateFile() => $_has(0);
  @$pb.TagNumber(1)
  void clearCreateFile() => $_clearField(1);
  @$pb.TagNumber(1)
  CreateFile ensureCreateFile() => $_ensure(0);

  @$pb.TagNumber(2)
  CreateDirectory get createDirectory => $_getN(1);
  @$pb.TagNumber(2)
  set createDirectory(CreateDirectory value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasCreateDirectory() => $_has(1);
  @$pb.TagNumber(2)
  void clearCreateDirectory() => $_clearField(2);
  @$pb.TagNumber(2)
  CreateDirectory ensureCreateDirectory() => $_ensure(1);

  @$pb.TagNumber(3)
  CreateSymbolicLink get createSymbolicLink => $_getN(2);
  @$pb.TagNumber(3)
  set createSymbolicLink(CreateSymbolicLink value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasCreateSymbolicLink() => $_has(2);
  @$pb.TagNumber(3)
  void clearCreateSymbolicLink() => $_clearField(3);
  @$pb.TagNumber(3)
  CreateSymbolicLink ensureCreateSymbolicLink() => $_ensure(2);

  @$pb.TagNumber(4)
  Remove get remove => $_getN(3);
  @$pb.TagNumber(4)
  set remove(Remove value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasRemove() => $_has(3);
  @$pb.TagNumber(4)
  void clearRemove() => $_clearField(4);
  @$pb.TagNumber(4)
  Remove ensureRemove() => $_ensure(3);

  @$pb.TagNumber(5)
  Rename get rename => $_getN(4);
  @$pb.TagNumber(5)
  set rename(Rename value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasRename() => $_has(4);
  @$pb.TagNumber(5)
  void clearRename() => $_clearField(5);
  @$pb.TagNumber(5)
  Rename ensureRename() => $_ensure(4);

  @$pb.TagNumber(6)
  HardLink get hardLink => $_getN(5);
  @$pb.TagNumber(6)
  set hardLink(HardLink value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasHardLink() => $_has(5);
  @$pb.TagNumber(6)
  void clearHardLink() => $_clearField(6);
  @$pb.TagNumber(6)
  HardLink ensureHardLink() => $_ensure(5);

  @$pb.TagNumber(7)
  Write get write => $_getN(6);
  @$pb.TagNumber(7)
  set write(Write value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasWrite() => $_has(6);
  @$pb.TagNumber(7)
  void clearWrite() => $_clearField(7);
  @$pb.TagNumber(7)
  Write ensureWrite() => $_ensure(6);

  @$pb.TagNumber(8)
  Resize get resize => $_getN(7);
  @$pb.TagNumber(8)
  set resize(Resize value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasResize() => $_has(7);
  @$pb.TagNumber(8)
  void clearResize() => $_clearField(8);
  @$pb.TagNumber(8)
  Resize ensureResize() => $_ensure(7);

  @$pb.TagNumber(9)
  ZeroRange get zeroRange => $_getN(8);
  @$pb.TagNumber(9)
  set zeroRange(ZeroRange value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasZeroRange() => $_has(8);
  @$pb.TagNumber(9)
  void clearZeroRange() => $_clearField(9);
  @$pb.TagNumber(9)
  ZeroRange ensureZeroRange() => $_ensure(8);

  @$pb.TagNumber(10)
  Preallocate get preallocate => $_getN(9);
  @$pb.TagNumber(10)
  set preallocate(Preallocate value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasPreallocate() => $_has(9);
  @$pb.TagNumber(10)
  void clearPreallocate() => $_clearField(10);
  @$pb.TagNumber(10)
  Preallocate ensurePreallocate() => $_ensure(9);

  @$pb.TagNumber(11)
  CloneRange get cloneRange => $_getN(10);
  @$pb.TagNumber(11)
  set cloneRange(CloneRange value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasCloneRange() => $_has(10);
  @$pb.TagNumber(11)
  void clearCloneRange() => $_clearField(11);
  @$pb.TagNumber(11)
  CloneRange ensureCloneRange() => $_ensure(10);

  @$pb.TagNumber(12)
  SetMetadata get setMetadata => $_getN(11);
  @$pb.TagNumber(12)
  set setMetadata(SetMetadata value) => $_setField(12, value);
  @$pb.TagNumber(12)
  $core.bool hasSetMetadata() => $_has(11);
  @$pb.TagNumber(12)
  void clearSetMetadata() => $_clearField(12);
  @$pb.TagNumber(12)
  SetMetadata ensureSetMetadata() => $_ensure(11);

  @$pb.TagNumber(13)
  CreateDirectories get createDirectories => $_getN(12);
  @$pb.TagNumber(13)
  set createDirectories(CreateDirectories value) => $_setField(13, value);
  @$pb.TagNumber(13)
  $core.bool hasCreateDirectories() => $_has(12);
  @$pb.TagNumber(13)
  void clearCreateDirectories() => $_clearField(13);
  @$pb.TagNumber(13)
  CreateDirectories ensureCreateDirectories() => $_ensure(12);

  @$pb.TagNumber(14)
  PutFile get putFile => $_getN(13);
  @$pb.TagNumber(14)
  set putFile(PutFile value) => $_setField(14, value);
  @$pb.TagNumber(14)
  $core.bool hasPutFile() => $_has(13);
  @$pb.TagNumber(14)
  void clearPutFile() => $_clearField(14);
  @$pb.TagNumber(14)
  PutFile ensurePutFile() => $_ensure(13);

  @$pb.TagNumber(15)
  CopyFile get copyFile => $_getN(14);
  @$pb.TagNumber(15)
  set copyFile(CopyFile value) => $_setField(15, value);
  @$pb.TagNumber(15)
  $core.bool hasCopyFile() => $_has(14);
  @$pb.TagNumber(15)
  void clearCopyFile() => $_clearField(15);
  @$pb.TagNumber(15)
  CopyFile ensureCopyFile() => $_ensure(14);
}

class ApplyTransactionRequest extends $pb.GeneratedMessage {
  factory ApplyTransactionRequest({
    GenerationRef? base,
    $core.Iterable<Mutation>? mutations,
    OperationOptions? operation,
    $core.int? maximumConflicts,
  }) {
    final result = ApplyTransactionRequest._();
    if (base != null) result.base = base;
    if (mutations != null) result.mutations.addAll(mutations);
    if (operation != null) result.operation = operation;
    if (maximumConflicts != null) result.maximumConflicts = maximumConflicts;
    return result;
  }

  ApplyTransactionRequest._();

  factory ApplyTransactionRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ApplyTransactionRequest()..mergeFromBuffer(data, registry);
  factory ApplyTransactionRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ApplyTransactionRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ApplyTransactionRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ApplyTransactionRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'base',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<Mutation>(2, _omitFieldNames ? '' : 'mutations',
        subBuilder: Mutation.$_createMessage)
    ..aOM<OperationOptions>(3, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..aI(4, _omitFieldNames ? '' : 'maximumConflicts',
        fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ApplyTransactionRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ApplyTransactionRequest copyWith(
          void Function(ApplyTransactionRequest) updates) =>
      super.copyWith((message) => updates(message as ApplyTransactionRequest))
          as ApplyTransactionRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ApplyTransactionRequest() / ApplyTransactionRequest.new instead')
  static ApplyTransactionRequest create() => ApplyTransactionRequest._();
  static $pb.GeneratedMessage $_createMessage() => ApplyTransactionRequest._();
  @$core.override
  ApplyTransactionRequest createEmptyInstance() => ApplyTransactionRequest._();
  @$core.pragma('dart2js:noInline')
  static ApplyTransactionRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ApplyTransactionRequest>(
          ApplyTransactionRequest.$_createMessage);
  static ApplyTransactionRequest? _defaultInstance;

  /// Exact immutable base observed by the caller. Independent head changes are
  /// rebased from this state; dependency overlap is returned as conflicts.
  @$pb.TagNumber(1)
  GenerationRef get base => $_getN(0);
  @$pb.TagNumber(1)
  set base(GenerationRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBase() => $_has(0);
  @$pb.TagNumber(1)
  void clearBase() => $_clearField(1);
  @$pb.TagNumber(1)
  GenerationRef ensureBase() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<Mutation> get mutations => $_getList(1);

  @$pb.TagNumber(3)
  OperationOptions get operation => $_getN(2);
  @$pb.TagNumber(3)
  set operation(OperationOptions value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperation() => $_clearField(3);
  @$pb.TagNumber(3)
  OperationOptions ensureOperation() => $_ensure(2);

  @$pb.TagNumber(4)
  $core.int get maximumConflicts => $_getIZ(3);
  @$pb.TagNumber(4)
  set maximumConflicts($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumConflicts() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumConflicts() => $_clearField(4);
}

class RebaseTransactionRequest extends $pb.GeneratedMessage {
  factory RebaseTransactionRequest({
    GenerationRef? base,
    $core.Iterable<Mutation>? mutations,
    $core.int? maximumConflicts,
    OperationOptions? operation,
  }) {
    final result = RebaseTransactionRequest._();
    if (base != null) result.base = base;
    if (mutations != null) result.mutations.addAll(mutations);
    if (maximumConflicts != null) result.maximumConflicts = maximumConflicts;
    if (operation != null) result.operation = operation;
    return result;
  }

  RebaseTransactionRequest._();

  factory RebaseTransactionRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RebaseTransactionRequest()..mergeFromBuffer(data, registry);
  factory RebaseTransactionRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RebaseTransactionRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RebaseTransactionRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: RebaseTransactionRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'base',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<Mutation>(2, _omitFieldNames ? '' : 'mutations',
        subBuilder: Mutation.$_createMessage)
    ..aI(3, _omitFieldNames ? '' : 'maximumConflicts',
        fieldType: $pb.PbFieldType.OU3)
    ..aOM<OperationOptions>(4, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RebaseTransactionRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RebaseTransactionRequest copyWith(
          void Function(RebaseTransactionRequest) updates) =>
      super.copyWith((message) => updates(message as RebaseTransactionRequest))
          as RebaseTransactionRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use RebaseTransactionRequest() / RebaseTransactionRequest.new instead')
  static RebaseTransactionRequest create() => RebaseTransactionRequest._();
  static $pb.GeneratedMessage $_createMessage() => RebaseTransactionRequest._();
  @$core.override
  RebaseTransactionRequest createEmptyInstance() =>
      RebaseTransactionRequest._();
  @$core.pragma('dart2js:noInline')
  static RebaseTransactionRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RebaseTransactionRequest>(
          RebaseTransactionRequest.$_createMessage);
  static RebaseTransactionRequest? _defaultInstance;

  @$pb.TagNumber(1)
  GenerationRef get base => $_getN(0);
  @$pb.TagNumber(1)
  set base(GenerationRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBase() => $_has(0);
  @$pb.TagNumber(1)
  void clearBase() => $_clearField(1);
  @$pb.TagNumber(1)
  GenerationRef ensureBase() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<Mutation> get mutations => $_getList(1);

  @$pb.TagNumber(3)
  $core.int get maximumConflicts => $_getIZ(2);
  @$pb.TagNumber(3)
  set maximumConflicts($core.int value) => $_setUnsignedInt32(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumConflicts() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumConflicts() => $_clearField(3);

  @$pb.TagNumber(4)
  OperationOptions get operation => $_getN(3);
  @$pb.TagNumber(4)
  set operation(OperationOptions value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasOperation() => $_has(3);
  @$pb.TagNumber(4)
  void clearOperation() => $_clearField(4);
  @$pb.TagNumber(4)
  OperationOptions ensureOperation() => $_ensure(3);
}

class RebaseTransactionResponse extends $pb.GeneratedMessage {
  factory RebaseTransactionResponse({
    GenerationRef? base,
    $core.Iterable<Conflict>? conflicts,
    $core.bool? truncated,
  }) {
    final result = RebaseTransactionResponse._();
    if (base != null) result.base = base;
    if (conflicts != null) result.conflicts.addAll(conflicts);
    if (truncated != null) result.truncated = truncated;
    return result;
  }

  RebaseTransactionResponse._();

  factory RebaseTransactionResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RebaseTransactionResponse()..mergeFromBuffer(data, registry);
  factory RebaseTransactionResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RebaseTransactionResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RebaseTransactionResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: RebaseTransactionResponse.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'base',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<Conflict>(2, _omitFieldNames ? '' : 'conflicts',
        subBuilder: Conflict.$_createMessage)
    ..aOB(3, _omitFieldNames ? '' : 'truncated')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RebaseTransactionResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RebaseTransactionResponse copyWith(
          void Function(RebaseTransactionResponse) updates) =>
      super.copyWith((message) => updates(message as RebaseTransactionResponse))
          as RebaseTransactionResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use RebaseTransactionResponse() / RebaseTransactionResponse.new instead')
  static RebaseTransactionResponse create() => RebaseTransactionResponse._();
  static $pb.GeneratedMessage $_createMessage() =>
      RebaseTransactionResponse._();
  @$core.override
  RebaseTransactionResponse createEmptyInstance() =>
      RebaseTransactionResponse._();
  @$core.pragma('dart2js:noInline')
  static RebaseTransactionResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RebaseTransactionResponse>(
          RebaseTransactionResponse.$_createMessage);
  static RebaseTransactionResponse? _defaultInstance;

  @$pb.TagNumber(1)
  GenerationRef get base => $_getN(0);
  @$pb.TagNumber(1)
  set base(GenerationRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasBase() => $_has(0);
  @$pb.TagNumber(1)
  void clearBase() => $_clearField(1);
  @$pb.TagNumber(1)
  GenerationRef ensureBase() => $_ensure(0);

  @$pb.TagNumber(2)
  $pb.PbList<Conflict> get conflicts => $_getList(1);

  @$pb.TagNumber(3)
  $core.bool get truncated => $_getBF(2);
  @$pb.TagNumber(3)
  set truncated($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasTruncated() => $_has(2);
  @$pb.TagNumber(3)
  void clearTruncated() => $_clearField(3);
}

class ForkWorkspaceRequest extends $pb.GeneratedMessage {
  factory ForkWorkspaceRequest({
    GenerationRef? source,
    $core.String? destinationName,
    OperationOptions? operation,
  }) {
    final result = ForkWorkspaceRequest._();
    if (source != null) result.source = source;
    if (destinationName != null) result.destinationName = destinationName;
    if (operation != null) result.operation = operation;
    return result;
  }

  ForkWorkspaceRequest._();

  factory ForkWorkspaceRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkWorkspaceRequest()..mergeFromBuffer(data, registry);
  factory ForkWorkspaceRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ForkWorkspaceRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ForkWorkspaceRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ForkWorkspaceRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'source',
        subBuilder: GenerationRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'destinationName')
    ..aOM<OperationOptions>(3, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkWorkspaceRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ForkWorkspaceRequest copyWith(void Function(ForkWorkspaceRequest) updates) =>
      super.copyWith((message) => updates(message as ForkWorkspaceRequest))
          as ForkWorkspaceRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use ForkWorkspaceRequest() / ForkWorkspaceRequest.new instead')
  static ForkWorkspaceRequest create() => ForkWorkspaceRequest._();
  static $pb.GeneratedMessage $_createMessage() => ForkWorkspaceRequest._();
  @$core.override
  ForkWorkspaceRequest createEmptyInstance() => ForkWorkspaceRequest._();
  @$core.pragma('dart2js:noInline')
  static ForkWorkspaceRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<ForkWorkspaceRequest>(
          ForkWorkspaceRequest.$_createMessage);
  static ForkWorkspaceRequest? _defaultInstance;

  @$pb.TagNumber(1)
  GenerationRef get source => $_getN(0);
  @$pb.TagNumber(1)
  set source(GenerationRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);
  @$pb.TagNumber(1)
  GenerationRef ensureSource() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.String get destinationName => $_getSZ(1);
  @$pb.TagNumber(2)
  set destinationName($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasDestinationName() => $_has(1);
  @$pb.TagNumber(2)
  void clearDestinationName() => $_clearField(2);

  @$pb.TagNumber(3)
  OperationOptions get operation => $_getN(2);
  @$pb.TagNumber(3)
  set operation(OperationOptions value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperation() => $_clearField(3);
  @$pb.TagNumber(3)
  OperationOptions ensureOperation() => $_ensure(2);
}

class DiffRequest extends $pb.GeneratedMessage {
  factory DiffRequest({
    GenerationRef? from,
    GenerationRef? to,
    $core.int? maximumChanges,
  }) {
    final result = DiffRequest._();
    if (from != null) result.from = from;
    if (to != null) result.to = to;
    if (maximumChanges != null) result.maximumChanges = maximumChanges;
    return result;
  }

  DiffRequest._();

  factory DiffRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DiffRequest()..mergeFromBuffer(data, registry);
  factory DiffRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DiffRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DiffRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: DiffRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'from',
        subBuilder: GenerationRef.$_createMessage)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'to',
        subBuilder: GenerationRef.$_createMessage)
    ..aI(3, _omitFieldNames ? '' : 'maximumChanges',
        fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DiffRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DiffRequest copyWith(void Function(DiffRequest) updates) =>
      super.copyWith((message) => updates(message as DiffRequest))
          as DiffRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use DiffRequest() / DiffRequest.new instead')
  static DiffRequest create() => DiffRequest._();
  static $pb.GeneratedMessage $_createMessage() => DiffRequest._();
  @$core.override
  DiffRequest createEmptyInstance() => DiffRequest._();
  @$core.pragma('dart2js:noInline')
  static DiffRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<DiffRequest>(
          DiffRequest.$_createMessage);
  static DiffRequest? _defaultInstance;

  @$pb.TagNumber(1)
  GenerationRef get from => $_getN(0);
  @$pb.TagNumber(1)
  set from(GenerationRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasFrom() => $_has(0);
  @$pb.TagNumber(1)
  void clearFrom() => $_clearField(1);
  @$pb.TagNumber(1)
  GenerationRef ensureFrom() => $_ensure(0);

  @$pb.TagNumber(2)
  GenerationRef get to => $_getN(1);
  @$pb.TagNumber(2)
  set to(GenerationRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasTo() => $_has(1);
  @$pb.TagNumber(2)
  void clearTo() => $_clearField(2);
  @$pb.TagNumber(2)
  GenerationRef ensureTo() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.int get maximumChanges => $_getIZ(2);
  @$pb.TagNumber(3)
  set maximumChanges($core.int value) => $_setUnsignedInt32(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumChanges() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumChanges() => $_clearField(3);
}

class LogicalName extends $pb.GeneratedMessage {
  factory LogicalName({
    NameEncoding? encoding,
    $core.List<$core.int>? bytes,
  }) {
    final result = LogicalName._();
    if (encoding != null) result.encoding = encoding;
    if (bytes != null) result.bytes = bytes;
    return result;
  }

  LogicalName._();

  factory LogicalName.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      LogicalName()..mergeFromBuffer(data, registry);
  factory LogicalName.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      LogicalName()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'LogicalName',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: LogicalName.$_createMessage)
    ..aE<NameEncoding>(1, _omitFieldNames ? '' : 'encoding',
        enumValues: NameEncoding.values)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'bytes', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LogicalName clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  LogicalName copyWith(void Function(LogicalName) updates) =>
      super.copyWith((message) => updates(message as LogicalName))
          as LogicalName;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use LogicalName() / LogicalName.new instead')
  static LogicalName create() => LogicalName._();
  static $pb.GeneratedMessage $_createMessage() => LogicalName._();
  @$core.override
  LogicalName createEmptyInstance() => LogicalName._();
  @$core.pragma('dart2js:noInline')
  static LogicalName getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<LogicalName>(
          LogicalName.$_createMessage);
  static LogicalName? _defaultInstance;

  @$pb.TagNumber(1)
  NameEncoding get encoding => $_getN(0);
  @$pb.TagNumber(1)
  set encoding(NameEncoding value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasEncoding() => $_has(0);
  @$pb.TagNumber(1)
  void clearEncoding() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get bytes => $_getN(1);
  @$pb.TagNumber(2)
  set bytes($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasBytes() => $_has(1);
  @$pb.TagNumber(2)
  void clearBytes() => $_clearField(2);
}

class FileRecordSnapshot extends $pb.GeneratedMessage {
  factory FileRecordSnapshot({
    $core.List<$core.int>? fileId,
    FileKind? fileKind,
    $fixnum.Int64? linkCount,
    $core.List<$core.int>? metadataObject,
    $core.String? payloadKind,
    OptionalU64? logicalBytes,
    $core.List<$core.int>? payloadObject,
    $core.List<$core.int>? inlineBytes,
    OptionalU32? deviceMajor,
    OptionalU32? deviceMinor,
  }) {
    final result = FileRecordSnapshot._();
    if (fileId != null) result.fileId = fileId;
    if (fileKind != null) result.fileKind = fileKind;
    if (linkCount != null) result.linkCount = linkCount;
    if (metadataObject != null) result.metadataObject = metadataObject;
    if (payloadKind != null) result.payloadKind = payloadKind;
    if (logicalBytes != null) result.logicalBytes = logicalBytes;
    if (payloadObject != null) result.payloadObject = payloadObject;
    if (inlineBytes != null) result.inlineBytes = inlineBytes;
    if (deviceMajor != null) result.deviceMajor = deviceMajor;
    if (deviceMinor != null) result.deviceMinor = deviceMinor;
    return result;
  }

  FileRecordSnapshot._();

  factory FileRecordSnapshot.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileRecordSnapshot()..mergeFromBuffer(data, registry);
  factory FileRecordSnapshot.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileRecordSnapshot()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'FileRecordSnapshot',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: FileRecordSnapshot.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'fileId', $pb.PbFieldType.OY)
    ..aE<FileKind>(2, _omitFieldNames ? '' : 'fileKind',
        enumValues: FileKind.values)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'linkCount', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'metadataObject', $pb.PbFieldType.OY)
    ..aOS(5, _omitFieldNames ? '' : 'payloadKind')
    ..aOM<OptionalU64>(6, _omitFieldNames ? '' : 'logicalBytes',
        subBuilder: OptionalU64.$_createMessage)
    ..a<$core.List<$core.int>>(
        7, _omitFieldNames ? '' : 'payloadObject', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        8, _omitFieldNames ? '' : 'inlineBytes', $pb.PbFieldType.OY)
    ..aOM<OptionalU32>(9, _omitFieldNames ? '' : 'deviceMajor',
        subBuilder: OptionalU32.$_createMessage)
    ..aOM<OptionalU32>(10, _omitFieldNames ? '' : 'deviceMinor',
        subBuilder: OptionalU32.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileRecordSnapshot clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileRecordSnapshot copyWith(void Function(FileRecordSnapshot) updates) =>
      super.copyWith((message) => updates(message as FileRecordSnapshot))
          as FileRecordSnapshot;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use FileRecordSnapshot() / FileRecordSnapshot.new instead')
  static FileRecordSnapshot create() => FileRecordSnapshot._();
  static $pb.GeneratedMessage $_createMessage() => FileRecordSnapshot._();
  @$core.override
  FileRecordSnapshot createEmptyInstance() => FileRecordSnapshot._();
  @$core.pragma('dart2js:noInline')
  static FileRecordSnapshot getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<FileRecordSnapshot>(
          FileRecordSnapshot.$_createMessage);
  static FileRecordSnapshot? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get fileId => $_getN(0);
  @$pb.TagNumber(1)
  set fileId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasFileId() => $_has(0);
  @$pb.TagNumber(1)
  void clearFileId() => $_clearField(1);

  @$pb.TagNumber(2)
  FileKind get fileKind => $_getN(1);
  @$pb.TagNumber(2)
  set fileKind(FileKind value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasFileKind() => $_has(1);
  @$pb.TagNumber(2)
  void clearFileKind() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get linkCount => $_getI64(2);
  @$pb.TagNumber(3)
  set linkCount($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasLinkCount() => $_has(2);
  @$pb.TagNumber(3)
  void clearLinkCount() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get metadataObject => $_getN(3);
  @$pb.TagNumber(4)
  set metadataObject($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMetadataObject() => $_has(3);
  @$pb.TagNumber(4)
  void clearMetadataObject() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get payloadKind => $_getSZ(4);
  @$pb.TagNumber(5)
  set payloadKind($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasPayloadKind() => $_has(4);
  @$pb.TagNumber(5)
  void clearPayloadKind() => $_clearField(5);

  @$pb.TagNumber(6)
  OptionalU64 get logicalBytes => $_getN(5);
  @$pb.TagNumber(6)
  set logicalBytes(OptionalU64 value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasLogicalBytes() => $_has(5);
  @$pb.TagNumber(6)
  void clearLogicalBytes() => $_clearField(6);
  @$pb.TagNumber(6)
  OptionalU64 ensureLogicalBytes() => $_ensure(5);

  @$pb.TagNumber(7)
  $core.List<$core.int> get payloadObject => $_getN(6);
  @$pb.TagNumber(7)
  set payloadObject($core.List<$core.int> value) => $_setBytes(6, value);
  @$pb.TagNumber(7)
  $core.bool hasPayloadObject() => $_has(6);
  @$pb.TagNumber(7)
  void clearPayloadObject() => $_clearField(7);

  @$pb.TagNumber(8)
  $core.List<$core.int> get inlineBytes => $_getN(7);
  @$pb.TagNumber(8)
  set inlineBytes($core.List<$core.int> value) => $_setBytes(7, value);
  @$pb.TagNumber(8)
  $core.bool hasInlineBytes() => $_has(7);
  @$pb.TagNumber(8)
  void clearInlineBytes() => $_clearField(8);

  @$pb.TagNumber(9)
  OptionalU32 get deviceMajor => $_getN(8);
  @$pb.TagNumber(9)
  set deviceMajor(OptionalU32 value) => $_setField(9, value);
  @$pb.TagNumber(9)
  $core.bool hasDeviceMajor() => $_has(8);
  @$pb.TagNumber(9)
  void clearDeviceMajor() => $_clearField(9);
  @$pb.TagNumber(9)
  OptionalU32 ensureDeviceMajor() => $_ensure(8);

  @$pb.TagNumber(10)
  OptionalU32 get deviceMinor => $_getN(9);
  @$pb.TagNumber(10)
  set deviceMinor(OptionalU32 value) => $_setField(10, value);
  @$pb.TagNumber(10)
  $core.bool hasDeviceMinor() => $_has(9);
  @$pb.TagNumber(10)
  void clearDeviceMinor() => $_clearField(10);
  @$pb.TagNumber(10)
  OptionalU32 ensureDeviceMinor() => $_ensure(9);
}

class FileRecordChange extends $pb.GeneratedMessage {
  factory FileRecordChange({
    $core.List<$core.int>? fileId,
    FileRecordSnapshot? before,
    FileRecordSnapshot? after,
  }) {
    final result = FileRecordChange._();
    if (fileId != null) result.fileId = fileId;
    if (before != null) result.before = before;
    if (after != null) result.after = after;
    return result;
  }

  FileRecordChange._();

  factory FileRecordChange.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileRecordChange()..mergeFromBuffer(data, registry);
  factory FileRecordChange.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileRecordChange()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'FileRecordChange',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: FileRecordChange.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'fileId', $pb.PbFieldType.OY)
    ..aOM<FileRecordSnapshot>(2, _omitFieldNames ? '' : 'before',
        subBuilder: FileRecordSnapshot.$_createMessage)
    ..aOM<FileRecordSnapshot>(3, _omitFieldNames ? '' : 'after',
        subBuilder: FileRecordSnapshot.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileRecordChange clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileRecordChange copyWith(void Function(FileRecordChange) updates) =>
      super.copyWith((message) => updates(message as FileRecordChange))
          as FileRecordChange;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use FileRecordChange() / FileRecordChange.new instead')
  static FileRecordChange create() => FileRecordChange._();
  static $pb.GeneratedMessage $_createMessage() => FileRecordChange._();
  @$core.override
  FileRecordChange createEmptyInstance() => FileRecordChange._();
  @$core.pragma('dart2js:noInline')
  static FileRecordChange getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<FileRecordChange>(
          FileRecordChange.$_createMessage);
  static FileRecordChange? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get fileId => $_getN(0);
  @$pb.TagNumber(1)
  set fileId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasFileId() => $_has(0);
  @$pb.TagNumber(1)
  void clearFileId() => $_clearField(1);

  @$pb.TagNumber(2)
  FileRecordSnapshot get before => $_getN(1);
  @$pb.TagNumber(2)
  set before(FileRecordSnapshot value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasBefore() => $_has(1);
  @$pb.TagNumber(2)
  void clearBefore() => $_clearField(2);
  @$pb.TagNumber(2)
  FileRecordSnapshot ensureBefore() => $_ensure(1);

  @$pb.TagNumber(3)
  FileRecordSnapshot get after => $_getN(2);
  @$pb.TagNumber(3)
  set after(FileRecordSnapshot value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasAfter() => $_has(2);
  @$pb.TagNumber(3)
  void clearAfter() => $_clearField(3);
  @$pb.TagNumber(3)
  FileRecordSnapshot ensureAfter() => $_ensure(2);
}

class TreeEntrySnapshot extends $pb.GeneratedMessage {
  factory TreeEntrySnapshot({
    LogicalName? name,
    $core.List<$core.int>? fileId,
    FileKind? fileKind,
  }) {
    final result = TreeEntrySnapshot._();
    if (name != null) result.name = name;
    if (fileId != null) result.fileId = fileId;
    if (fileKind != null) result.fileKind = fileKind;
    return result;
  }

  TreeEntrySnapshot._();

  factory TreeEntrySnapshot.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TreeEntrySnapshot()..mergeFromBuffer(data, registry);
  factory TreeEntrySnapshot.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      TreeEntrySnapshot()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'TreeEntrySnapshot',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: TreeEntrySnapshot.$_createMessage)
    ..aOM<LogicalName>(1, _omitFieldNames ? '' : 'name',
        subBuilder: LogicalName.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'fileId', $pb.PbFieldType.OY)
    ..aE<FileKind>(3, _omitFieldNames ? '' : 'fileKind',
        enumValues: FileKind.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TreeEntrySnapshot clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  TreeEntrySnapshot copyWith(void Function(TreeEntrySnapshot) updates) =>
      super.copyWith((message) => updates(message as TreeEntrySnapshot))
          as TreeEntrySnapshot;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use TreeEntrySnapshot() / TreeEntrySnapshot.new instead')
  static TreeEntrySnapshot create() => TreeEntrySnapshot._();
  static $pb.GeneratedMessage $_createMessage() => TreeEntrySnapshot._();
  @$core.override
  TreeEntrySnapshot createEmptyInstance() => TreeEntrySnapshot._();
  @$core.pragma('dart2js:noInline')
  static TreeEntrySnapshot getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TreeEntrySnapshot>(
          TreeEntrySnapshot.$_createMessage);
  static TreeEntrySnapshot? _defaultInstance;

  @$pb.TagNumber(1)
  LogicalName get name => $_getN(0);
  @$pb.TagNumber(1)
  set name(LogicalName value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasName() => $_has(0);
  @$pb.TagNumber(1)
  void clearName() => $_clearField(1);
  @$pb.TagNumber(1)
  LogicalName ensureName() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get fileId => $_getN(1);
  @$pb.TagNumber(2)
  set fileId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasFileId() => $_has(1);
  @$pb.TagNumber(2)
  void clearFileId() => $_clearField(2);

  @$pb.TagNumber(3)
  FileKind get fileKind => $_getN(2);
  @$pb.TagNumber(3)
  set fileKind(FileKind value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasFileKind() => $_has(2);
  @$pb.TagNumber(3)
  void clearFileKind() => $_clearField(3);
}

class DirectoryBindingChange extends $pb.GeneratedMessage {
  factory DirectoryBindingChange({
    $core.List<$core.int>? directoryId,
    LogicalName? name,
    TreeEntrySnapshot? before,
    TreeEntrySnapshot? after,
  }) {
    final result = DirectoryBindingChange._();
    if (directoryId != null) result.directoryId = directoryId;
    if (name != null) result.name = name;
    if (before != null) result.before = before;
    if (after != null) result.after = after;
    return result;
  }

  DirectoryBindingChange._();

  factory DirectoryBindingChange.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryBindingChange()..mergeFromBuffer(data, registry);
  factory DirectoryBindingChange.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryBindingChange()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DirectoryBindingChange',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: DirectoryBindingChange.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'directoryId', $pb.PbFieldType.OY)
    ..aOM<LogicalName>(2, _omitFieldNames ? '' : 'name',
        subBuilder: LogicalName.$_createMessage)
    ..aOM<TreeEntrySnapshot>(3, _omitFieldNames ? '' : 'before',
        subBuilder: TreeEntrySnapshot.$_createMessage)
    ..aOM<TreeEntrySnapshot>(4, _omitFieldNames ? '' : 'after',
        subBuilder: TreeEntrySnapshot.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryBindingChange clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryBindingChange copyWith(
          void Function(DirectoryBindingChange) updates) =>
      super.copyWith((message) => updates(message as DirectoryBindingChange))
          as DirectoryBindingChange;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use DirectoryBindingChange() / DirectoryBindingChange.new instead')
  static DirectoryBindingChange create() => DirectoryBindingChange._();
  static $pb.GeneratedMessage $_createMessage() => DirectoryBindingChange._();
  @$core.override
  DirectoryBindingChange createEmptyInstance() => DirectoryBindingChange._();
  @$core.pragma('dart2js:noInline')
  static DirectoryBindingChange getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DirectoryBindingChange>(
          DirectoryBindingChange.$_createMessage);
  static DirectoryBindingChange? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get directoryId => $_getN(0);
  @$pb.TagNumber(1)
  set directoryId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasDirectoryId() => $_has(0);
  @$pb.TagNumber(1)
  void clearDirectoryId() => $_clearField(1);

  @$pb.TagNumber(2)
  LogicalName get name => $_getN(1);
  @$pb.TagNumber(2)
  set name(LogicalName value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => $_clearField(2);
  @$pb.TagNumber(2)
  LogicalName ensureName() => $_ensure(1);

  @$pb.TagNumber(3)
  TreeEntrySnapshot get before => $_getN(2);
  @$pb.TagNumber(3)
  set before(TreeEntrySnapshot value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasBefore() => $_has(2);
  @$pb.TagNumber(3)
  void clearBefore() => $_clearField(3);
  @$pb.TagNumber(3)
  TreeEntrySnapshot ensureBefore() => $_ensure(2);

  @$pb.TagNumber(4)
  TreeEntrySnapshot get after => $_getN(3);
  @$pb.TagNumber(4)
  set after(TreeEntrySnapshot value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasAfter() => $_has(3);
  @$pb.TagNumber(4)
  void clearAfter() => $_clearField(4);
  @$pb.TagNumber(4)
  TreeEntrySnapshot ensureAfter() => $_ensure(3);
}

class WorkCounters extends $pb.GeneratedMessage {
  factory WorkCounters({
    $fixnum.Int64? authorityRecordsRead,
    $fixnum.Int64? authorityRecordsAppended,
    $fixnum.Int64? authorityBytesRead,
    $fixnum.Int64? authorityBytesWritten,
    $fixnum.Int64? objectProbes,
    $fixnum.Int64? backendReadOperations,
    $fixnum.Int64? backendWriteOperations,
    $fixnum.Int64? durabilityOperations,
    $fixnum.Int64? pageReads,
    $fixnum.Int64? pageWrites,
    $fixnum.Int64? objectBytesRead,
    $fixnum.Int64? objectBytesWritten,
    $fixnum.Int64? bytesHashed,
    $fixnum.Int64? bytesCopied,
    $fixnum.Int64? bytesEncoded,
    $fixnum.Int64? sourceBytesRead,
    $fixnum.Int64? outputBytes,
    $fixnum.Int64? itemsExamined,
    $fixnum.Int64? itemsReturned,
    $fixnum.Int64? allocationOperations,
    $fixnum.Int64? peakAllocationBytes,
    $fixnum.Int64? materializations,
    $fixnum.Int64? sourcePathComponents,
    $fixnum.Int64? sourceEntriesVisited,
  }) {
    final result = WorkCounters._();
    if (authorityRecordsRead != null)
      result.authorityRecordsRead = authorityRecordsRead;
    if (authorityRecordsAppended != null)
      result.authorityRecordsAppended = authorityRecordsAppended;
    if (authorityBytesRead != null)
      result.authorityBytesRead = authorityBytesRead;
    if (authorityBytesWritten != null)
      result.authorityBytesWritten = authorityBytesWritten;
    if (objectProbes != null) result.objectProbes = objectProbes;
    if (backendReadOperations != null)
      result.backendReadOperations = backendReadOperations;
    if (backendWriteOperations != null)
      result.backendWriteOperations = backendWriteOperations;
    if (durabilityOperations != null)
      result.durabilityOperations = durabilityOperations;
    if (pageReads != null) result.pageReads = pageReads;
    if (pageWrites != null) result.pageWrites = pageWrites;
    if (objectBytesRead != null) result.objectBytesRead = objectBytesRead;
    if (objectBytesWritten != null)
      result.objectBytesWritten = objectBytesWritten;
    if (bytesHashed != null) result.bytesHashed = bytesHashed;
    if (bytesCopied != null) result.bytesCopied = bytesCopied;
    if (bytesEncoded != null) result.bytesEncoded = bytesEncoded;
    if (sourceBytesRead != null) result.sourceBytesRead = sourceBytesRead;
    if (outputBytes != null) result.outputBytes = outputBytes;
    if (itemsExamined != null) result.itemsExamined = itemsExamined;
    if (itemsReturned != null) result.itemsReturned = itemsReturned;
    if (allocationOperations != null)
      result.allocationOperations = allocationOperations;
    if (peakAllocationBytes != null)
      result.peakAllocationBytes = peakAllocationBytes;
    if (materializations != null) result.materializations = materializations;
    if (sourcePathComponents != null)
      result.sourcePathComponents = sourcePathComponents;
    if (sourceEntriesVisited != null)
      result.sourceEntriesVisited = sourceEntriesVisited;
    return result;
  }

  WorkCounters._();

  factory WorkCounters.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkCounters()..mergeFromBuffer(data, registry);
  factory WorkCounters.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      WorkCounters()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'WorkCounters',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: WorkCounters.$_createMessage)
    ..a<$fixnum.Int64>(
        1, _omitFieldNames ? '' : 'authorityRecordsRead', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'authorityRecordsAppended',
        $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        3, _omitFieldNames ? '' : 'authorityBytesRead', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'authorityBytesWritten', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        5, _omitFieldNames ? '' : 'objectProbes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        6, _omitFieldNames ? '' : 'backendReadOperations', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        7, _omitFieldNames ? '' : 'backendWriteOperations', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        8, _omitFieldNames ? '' : 'durabilityOperations', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        9, _omitFieldNames ? '' : 'pageReads', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        10, _omitFieldNames ? '' : 'pageWrites', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        11, _omitFieldNames ? '' : 'objectBytesRead', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        12, _omitFieldNames ? '' : 'objectBytesWritten', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        13, _omitFieldNames ? '' : 'bytesHashed', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        14, _omitFieldNames ? '' : 'bytesCopied', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        15, _omitFieldNames ? '' : 'bytesEncoded', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        16, _omitFieldNames ? '' : 'sourceBytesRead', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        17, _omitFieldNames ? '' : 'outputBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        18, _omitFieldNames ? '' : 'itemsExamined', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        19, _omitFieldNames ? '' : 'itemsReturned', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        20, _omitFieldNames ? '' : 'allocationOperations', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        21, _omitFieldNames ? '' : 'peakAllocationBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        22, _omitFieldNames ? '' : 'materializations', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        23, _omitFieldNames ? '' : 'sourcePathComponents', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..a<$fixnum.Int64>(
        24, _omitFieldNames ? '' : 'sourceEntriesVisited', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkCounters clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  WorkCounters copyWith(void Function(WorkCounters) updates) =>
      super.copyWith((message) => updates(message as WorkCounters))
          as WorkCounters;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use WorkCounters() / WorkCounters.new instead')
  static WorkCounters create() => WorkCounters._();
  static $pb.GeneratedMessage $_createMessage() => WorkCounters._();
  @$core.override
  WorkCounters createEmptyInstance() => WorkCounters._();
  @$core.pragma('dart2js:noInline')
  static WorkCounters getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<WorkCounters>(
          WorkCounters.$_createMessage);
  static WorkCounters? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get authorityRecordsRead => $_getI64(0);
  @$pb.TagNumber(1)
  set authorityRecordsRead($fixnum.Int64 value) => $_setInt64(0, value);
  @$pb.TagNumber(1)
  $core.bool hasAuthorityRecordsRead() => $_has(0);
  @$pb.TagNumber(1)
  void clearAuthorityRecordsRead() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get authorityRecordsAppended => $_getI64(1);
  @$pb.TagNumber(2)
  set authorityRecordsAppended($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAuthorityRecordsAppended() => $_has(1);
  @$pb.TagNumber(2)
  void clearAuthorityRecordsAppended() => $_clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get authorityBytesRead => $_getI64(2);
  @$pb.TagNumber(3)
  set authorityBytesRead($fixnum.Int64 value) => $_setInt64(2, value);
  @$pb.TagNumber(3)
  $core.bool hasAuthorityBytesRead() => $_has(2);
  @$pb.TagNumber(3)
  void clearAuthorityBytesRead() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get authorityBytesWritten => $_getI64(3);
  @$pb.TagNumber(4)
  set authorityBytesWritten($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasAuthorityBytesWritten() => $_has(3);
  @$pb.TagNumber(4)
  void clearAuthorityBytesWritten() => $_clearField(4);

  @$pb.TagNumber(5)
  $fixnum.Int64 get objectProbes => $_getI64(4);
  @$pb.TagNumber(5)
  set objectProbes($fixnum.Int64 value) => $_setInt64(4, value);
  @$pb.TagNumber(5)
  $core.bool hasObjectProbes() => $_has(4);
  @$pb.TagNumber(5)
  void clearObjectProbes() => $_clearField(5);

  @$pb.TagNumber(6)
  $fixnum.Int64 get backendReadOperations => $_getI64(5);
  @$pb.TagNumber(6)
  set backendReadOperations($fixnum.Int64 value) => $_setInt64(5, value);
  @$pb.TagNumber(6)
  $core.bool hasBackendReadOperations() => $_has(5);
  @$pb.TagNumber(6)
  void clearBackendReadOperations() => $_clearField(6);

  @$pb.TagNumber(7)
  $fixnum.Int64 get backendWriteOperations => $_getI64(6);
  @$pb.TagNumber(7)
  set backendWriteOperations($fixnum.Int64 value) => $_setInt64(6, value);
  @$pb.TagNumber(7)
  $core.bool hasBackendWriteOperations() => $_has(6);
  @$pb.TagNumber(7)
  void clearBackendWriteOperations() => $_clearField(7);

  @$pb.TagNumber(8)
  $fixnum.Int64 get durabilityOperations => $_getI64(7);
  @$pb.TagNumber(8)
  set durabilityOperations($fixnum.Int64 value) => $_setInt64(7, value);
  @$pb.TagNumber(8)
  $core.bool hasDurabilityOperations() => $_has(7);
  @$pb.TagNumber(8)
  void clearDurabilityOperations() => $_clearField(8);

  @$pb.TagNumber(9)
  $fixnum.Int64 get pageReads => $_getI64(8);
  @$pb.TagNumber(9)
  set pageReads($fixnum.Int64 value) => $_setInt64(8, value);
  @$pb.TagNumber(9)
  $core.bool hasPageReads() => $_has(8);
  @$pb.TagNumber(9)
  void clearPageReads() => $_clearField(9);

  @$pb.TagNumber(10)
  $fixnum.Int64 get pageWrites => $_getI64(9);
  @$pb.TagNumber(10)
  set pageWrites($fixnum.Int64 value) => $_setInt64(9, value);
  @$pb.TagNumber(10)
  $core.bool hasPageWrites() => $_has(9);
  @$pb.TagNumber(10)
  void clearPageWrites() => $_clearField(10);

  @$pb.TagNumber(11)
  $fixnum.Int64 get objectBytesRead => $_getI64(10);
  @$pb.TagNumber(11)
  set objectBytesRead($fixnum.Int64 value) => $_setInt64(10, value);
  @$pb.TagNumber(11)
  $core.bool hasObjectBytesRead() => $_has(10);
  @$pb.TagNumber(11)
  void clearObjectBytesRead() => $_clearField(11);

  @$pb.TagNumber(12)
  $fixnum.Int64 get objectBytesWritten => $_getI64(11);
  @$pb.TagNumber(12)
  set objectBytesWritten($fixnum.Int64 value) => $_setInt64(11, value);
  @$pb.TagNumber(12)
  $core.bool hasObjectBytesWritten() => $_has(11);
  @$pb.TagNumber(12)
  void clearObjectBytesWritten() => $_clearField(12);

  @$pb.TagNumber(13)
  $fixnum.Int64 get bytesHashed => $_getI64(12);
  @$pb.TagNumber(13)
  set bytesHashed($fixnum.Int64 value) => $_setInt64(12, value);
  @$pb.TagNumber(13)
  $core.bool hasBytesHashed() => $_has(12);
  @$pb.TagNumber(13)
  void clearBytesHashed() => $_clearField(13);

  @$pb.TagNumber(14)
  $fixnum.Int64 get bytesCopied => $_getI64(13);
  @$pb.TagNumber(14)
  set bytesCopied($fixnum.Int64 value) => $_setInt64(13, value);
  @$pb.TagNumber(14)
  $core.bool hasBytesCopied() => $_has(13);
  @$pb.TagNumber(14)
  void clearBytesCopied() => $_clearField(14);

  @$pb.TagNumber(15)
  $fixnum.Int64 get bytesEncoded => $_getI64(14);
  @$pb.TagNumber(15)
  set bytesEncoded($fixnum.Int64 value) => $_setInt64(14, value);
  @$pb.TagNumber(15)
  $core.bool hasBytesEncoded() => $_has(14);
  @$pb.TagNumber(15)
  void clearBytesEncoded() => $_clearField(15);

  @$pb.TagNumber(16)
  $fixnum.Int64 get sourceBytesRead => $_getI64(15);
  @$pb.TagNumber(16)
  set sourceBytesRead($fixnum.Int64 value) => $_setInt64(15, value);
  @$pb.TagNumber(16)
  $core.bool hasSourceBytesRead() => $_has(15);
  @$pb.TagNumber(16)
  void clearSourceBytesRead() => $_clearField(16);

  @$pb.TagNumber(17)
  $fixnum.Int64 get outputBytes => $_getI64(16);
  @$pb.TagNumber(17)
  set outputBytes($fixnum.Int64 value) => $_setInt64(16, value);
  @$pb.TagNumber(17)
  $core.bool hasOutputBytes() => $_has(16);
  @$pb.TagNumber(17)
  void clearOutputBytes() => $_clearField(17);

  @$pb.TagNumber(18)
  $fixnum.Int64 get itemsExamined => $_getI64(17);
  @$pb.TagNumber(18)
  set itemsExamined($fixnum.Int64 value) => $_setInt64(17, value);
  @$pb.TagNumber(18)
  $core.bool hasItemsExamined() => $_has(17);
  @$pb.TagNumber(18)
  void clearItemsExamined() => $_clearField(18);

  @$pb.TagNumber(19)
  $fixnum.Int64 get itemsReturned => $_getI64(18);
  @$pb.TagNumber(19)
  set itemsReturned($fixnum.Int64 value) => $_setInt64(18, value);
  @$pb.TagNumber(19)
  $core.bool hasItemsReturned() => $_has(18);
  @$pb.TagNumber(19)
  void clearItemsReturned() => $_clearField(19);

  @$pb.TagNumber(20)
  $fixnum.Int64 get allocationOperations => $_getI64(19);
  @$pb.TagNumber(20)
  set allocationOperations($fixnum.Int64 value) => $_setInt64(19, value);
  @$pb.TagNumber(20)
  $core.bool hasAllocationOperations() => $_has(19);
  @$pb.TagNumber(20)
  void clearAllocationOperations() => $_clearField(20);

  @$pb.TagNumber(21)
  $fixnum.Int64 get peakAllocationBytes => $_getI64(20);
  @$pb.TagNumber(21)
  set peakAllocationBytes($fixnum.Int64 value) => $_setInt64(20, value);
  @$pb.TagNumber(21)
  $core.bool hasPeakAllocationBytes() => $_has(20);
  @$pb.TagNumber(21)
  void clearPeakAllocationBytes() => $_clearField(21);

  @$pb.TagNumber(22)
  $fixnum.Int64 get materializations => $_getI64(21);
  @$pb.TagNumber(22)
  set materializations($fixnum.Int64 value) => $_setInt64(21, value);
  @$pb.TagNumber(22)
  $core.bool hasMaterializations() => $_has(21);
  @$pb.TagNumber(22)
  void clearMaterializations() => $_clearField(22);

  @$pb.TagNumber(23)
  $fixnum.Int64 get sourcePathComponents => $_getI64(22);
  @$pb.TagNumber(23)
  set sourcePathComponents($fixnum.Int64 value) => $_setInt64(22, value);
  @$pb.TagNumber(23)
  $core.bool hasSourcePathComponents() => $_has(22);
  @$pb.TagNumber(23)
  void clearSourcePathComponents() => $_clearField(23);

  @$pb.TagNumber(24)
  $fixnum.Int64 get sourceEntriesVisited => $_getI64(23);
  @$pb.TagNumber(24)
  set sourceEntriesVisited($fixnum.Int64 value) => $_setInt64(23, value);
  @$pb.TagNumber(24)
  $core.bool hasSourceEntriesVisited() => $_has(23);
  @$pb.TagNumber(24)
  void clearSourceEntriesVisited() => $_clearField(24);
}

class DiffResponse extends $pb.GeneratedMessage {
  factory DiffResponse({
    GenerationRef? from,
    GenerationRef? to,
    $core.Iterable<FileRecordChange>? files,
    $core.Iterable<DirectoryBindingChange>? bindings,
    $core.bool? truncated,
    WorkCounters? work,
  }) {
    final result = DiffResponse._();
    if (from != null) result.from = from;
    if (to != null) result.to = to;
    if (files != null) result.files.addAll(files);
    if (bindings != null) result.bindings.addAll(bindings);
    if (truncated != null) result.truncated = truncated;
    if (work != null) result.work = work;
    return result;
  }

  DiffResponse._();

  factory DiffResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DiffResponse()..mergeFromBuffer(data, registry);
  factory DiffResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DiffResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DiffResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: DiffResponse.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'from',
        subBuilder: GenerationRef.$_createMessage)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'to',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<FileRecordChange>(3, _omitFieldNames ? '' : 'files',
        subBuilder: FileRecordChange.$_createMessage)
    ..pPM<DirectoryBindingChange>(4, _omitFieldNames ? '' : 'bindings',
        subBuilder: DirectoryBindingChange.$_createMessage)
    ..aOB(5, _omitFieldNames ? '' : 'truncated')
    ..aOM<WorkCounters>(6, _omitFieldNames ? '' : 'work',
        subBuilder: WorkCounters.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DiffResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DiffResponse copyWith(void Function(DiffResponse) updates) =>
      super.copyWith((message) => updates(message as DiffResponse))
          as DiffResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use DiffResponse() / DiffResponse.new instead')
  static DiffResponse create() => DiffResponse._();
  static $pb.GeneratedMessage $_createMessage() => DiffResponse._();
  @$core.override
  DiffResponse createEmptyInstance() => DiffResponse._();
  @$core.pragma('dart2js:noInline')
  static DiffResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<DiffResponse>(
          DiffResponse.$_createMessage);
  static DiffResponse? _defaultInstance;

  @$pb.TagNumber(1)
  GenerationRef get from => $_getN(0);
  @$pb.TagNumber(1)
  set from(GenerationRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasFrom() => $_has(0);
  @$pb.TagNumber(1)
  void clearFrom() => $_clearField(1);
  @$pb.TagNumber(1)
  GenerationRef ensureFrom() => $_ensure(0);

  @$pb.TagNumber(2)
  GenerationRef get to => $_getN(1);
  @$pb.TagNumber(2)
  set to(GenerationRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasTo() => $_has(1);
  @$pb.TagNumber(2)
  void clearTo() => $_clearField(2);
  @$pb.TagNumber(2)
  GenerationRef ensureTo() => $_ensure(1);

  @$pb.TagNumber(3)
  $pb.PbList<FileRecordChange> get files => $_getList(2);

  @$pb.TagNumber(4)
  $pb.PbList<DirectoryBindingChange> get bindings => $_getList(3);

  @$pb.TagNumber(5)
  $core.bool get truncated => $_getBF(4);
  @$pb.TagNumber(5)
  set truncated($core.bool value) => $_setBool(4, value);
  @$pb.TagNumber(5)
  $core.bool hasTruncated() => $_has(4);
  @$pb.TagNumber(5)
  void clearTruncated() => $_clearField(5);

  @$pb.TagNumber(6)
  WorkCounters get work => $_getN(5);
  @$pb.TagNumber(6)
  set work(WorkCounters value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasWork() => $_has(5);
  @$pb.TagNumber(6)
  void clearWork() => $_clearField(6);
  @$pb.TagNumber(6)
  WorkCounters ensureWork() => $_ensure(5);
}

class RebaseRequest extends $pb.GeneratedMessage {
  factory RebaseRequest({
    WorkspaceRef? workspace,
    $core.int? maximumConflicts,
    OperationOptions? operation,
    $core.int? maximumGenerations,
    $core.int? maximumChanges,
  }) {
    final result = RebaseRequest._();
    if (workspace != null) result.workspace = workspace;
    if (maximumConflicts != null) result.maximumConflicts = maximumConflicts;
    if (operation != null) result.operation = operation;
    if (maximumGenerations != null)
      result.maximumGenerations = maximumGenerations;
    if (maximumChanges != null) result.maximumChanges = maximumChanges;
    return result;
  }

  RebaseRequest._();

  factory RebaseRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RebaseRequest()..mergeFromBuffer(data, registry);
  factory RebaseRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RebaseRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RebaseRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: RebaseRequest.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..aI(3, _omitFieldNames ? '' : 'maximumConflicts',
        fieldType: $pb.PbFieldType.OU3)
    ..aOM<OperationOptions>(4, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..aI(5, _omitFieldNames ? '' : 'maximumGenerations',
        fieldType: $pb.PbFieldType.OU3)
    ..aI(6, _omitFieldNames ? '' : 'maximumChanges',
        fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RebaseRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RebaseRequest copyWith(void Function(RebaseRequest) updates) =>
      super.copyWith((message) => updates(message as RebaseRequest))
          as RebaseRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RebaseRequest() / RebaseRequest.new instead')
  static RebaseRequest create() => RebaseRequest._();
  static $pb.GeneratedMessage $_createMessage() => RebaseRequest._();
  @$core.override
  RebaseRequest createEmptyInstance() => RebaseRequest._();
  @$core.pragma('dart2js:noInline')
  static RebaseRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RebaseRequest>(
          RebaseRequest.$_createMessage);
  static RebaseRequest? _defaultInstance;

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(3)
  $core.int get maximumConflicts => $_getIZ(1);
  @$pb.TagNumber(3)
  set maximumConflicts($core.int value) => $_setUnsignedInt32(1, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumConflicts() => $_has(1);
  @$pb.TagNumber(3)
  void clearMaximumConflicts() => $_clearField(3);

  @$pb.TagNumber(4)
  OperationOptions get operation => $_getN(2);
  @$pb.TagNumber(4)
  set operation(OperationOptions value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(4)
  void clearOperation() => $_clearField(4);
  @$pb.TagNumber(4)
  OperationOptions ensureOperation() => $_ensure(2);

  @$pb.TagNumber(5)
  $core.int get maximumGenerations => $_getIZ(3);
  @$pb.TagNumber(5)
  set maximumGenerations($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(5)
  $core.bool hasMaximumGenerations() => $_has(3);
  @$pb.TagNumber(5)
  void clearMaximumGenerations() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.int get maximumChanges => $_getIZ(4);
  @$pb.TagNumber(6)
  set maximumChanges($core.int value) => $_setUnsignedInt32(4, value);
  @$pb.TagNumber(6)
  $core.bool hasMaximumChanges() => $_has(4);
  @$pb.TagNumber(6)
  void clearMaximumChanges() => $_clearField(6);
}

class FileConflict extends $pb.GeneratedMessage {
  factory FileConflict({
    $core.List<$core.int>? fileId,
  }) {
    final result = FileConflict._();
    if (fileId != null) result.fileId = fileId;
    return result;
  }

  FileConflict._();

  factory FileConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileConflict()..mergeFromBuffer(data, registry);
  factory FileConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      FileConflict()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'FileConflict',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: FileConflict.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'fileId', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  FileConflict copyWith(void Function(FileConflict) updates) =>
      super.copyWith((message) => updates(message as FileConflict))
          as FileConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use FileConflict() / FileConflict.new instead')
  static FileConflict create() => FileConflict._();
  static $pb.GeneratedMessage $_createMessage() => FileConflict._();
  @$core.override
  FileConflict createEmptyInstance() => FileConflict._();
  @$core.pragma('dart2js:noInline')
  static FileConflict getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<FileConflict>(
          FileConflict.$_createMessage);
  static FileConflict? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get fileId => $_getN(0);
  @$pb.TagNumber(1)
  set fileId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasFileId() => $_has(0);
  @$pb.TagNumber(1)
  void clearFileId() => $_clearField(1);
}

class ContentConflict extends $pb.GeneratedMessage {
  factory ContentConflict({
    $core.List<$core.int>? fileId,
    ByteRange? range,
  }) {
    final result = ContentConflict._();
    if (fileId != null) result.fileId = fileId;
    if (range != null) result.range = range;
    return result;
  }

  ContentConflict._();

  factory ContentConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ContentConflict()..mergeFromBuffer(data, registry);
  factory ContentConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ContentConflict()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ContentConflict',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ContentConflict.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'fileId', $pb.PbFieldType.OY)
    ..aOM<ByteRange>(2, _omitFieldNames ? '' : 'range',
        subBuilder: ByteRange.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ContentConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ContentConflict copyWith(void Function(ContentConflict) updates) =>
      super.copyWith((message) => updates(message as ContentConflict))
          as ContentConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ContentConflict() / ContentConflict.new instead')
  static ContentConflict create() => ContentConflict._();
  static $pb.GeneratedMessage $_createMessage() => ContentConflict._();
  @$core.override
  ContentConflict createEmptyInstance() => ContentConflict._();
  @$core.pragma('dart2js:noInline')
  static ContentConflict getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ContentConflict>(
          ContentConflict.$_createMessage);
  static ContentConflict? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get fileId => $_getN(0);
  @$pb.TagNumber(1)
  set fileId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasFileId() => $_has(0);
  @$pb.TagNumber(1)
  void clearFileId() => $_clearField(1);

  @$pb.TagNumber(2)
  ByteRange get range => $_getN(1);
  @$pb.TagNumber(2)
  set range(ByteRange value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasRange() => $_has(1);
  @$pb.TagNumber(2)
  void clearRange() => $_clearField(2);
  @$pb.TagNumber(2)
  ByteRange ensureRange() => $_ensure(1);
}

class SparseConflict extends $pb.GeneratedMessage {
  factory SparseConflict({
    $core.List<$core.int>? fileId,
    $fixnum.Int64? offset,
    SparseTarget? target,
  }) {
    final result = SparseConflict._();
    if (fileId != null) result.fileId = fileId;
    if (offset != null) result.offset = offset;
    if (target != null) result.target = target;
    return result;
  }

  SparseConflict._();

  factory SparseConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SparseConflict()..mergeFromBuffer(data, registry);
  factory SparseConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      SparseConflict()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'SparseConflict',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: SparseConflict.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'fileId', $pb.PbFieldType.OY)
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'offset', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aE<SparseTarget>(3, _omitFieldNames ? '' : 'target',
        enumValues: SparseTarget.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SparseConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  SparseConflict copyWith(void Function(SparseConflict) updates) =>
      super.copyWith((message) => updates(message as SparseConflict))
          as SparseConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use SparseConflict() / SparseConflict.new instead')
  static SparseConflict create() => SparseConflict._();
  static $pb.GeneratedMessage $_createMessage() => SparseConflict._();
  @$core.override
  SparseConflict createEmptyInstance() => SparseConflict._();
  @$core.pragma('dart2js:noInline')
  static SparseConflict getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<SparseConflict>(
          SparseConflict.$_createMessage);
  static SparseConflict? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get fileId => $_getN(0);
  @$pb.TagNumber(1)
  set fileId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasFileId() => $_has(0);
  @$pb.TagNumber(1)
  void clearFileId() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get offset => $_getI64(1);
  @$pb.TagNumber(2)
  set offset($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasOffset() => $_has(1);
  @$pb.TagNumber(2)
  void clearOffset() => $_clearField(2);

  @$pb.TagNumber(3)
  SparseTarget get target => $_getN(2);
  @$pb.TagNumber(3)
  set target(SparseTarget value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasTarget() => $_has(2);
  @$pb.TagNumber(3)
  void clearTarget() => $_clearField(3);
}

class DirectoryNameConflict extends $pb.GeneratedMessage {
  factory DirectoryNameConflict({
    $core.List<$core.int>? directoryId,
    LogicalName? name,
  }) {
    final result = DirectoryNameConflict._();
    if (directoryId != null) result.directoryId = directoryId;
    if (name != null) result.name = name;
    return result;
  }

  DirectoryNameConflict._();

  factory DirectoryNameConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryNameConflict()..mergeFromBuffer(data, registry);
  factory DirectoryNameConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryNameConflict()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DirectoryNameConflict',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: DirectoryNameConflict.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'directoryId', $pb.PbFieldType.OY)
    ..aOM<LogicalName>(2, _omitFieldNames ? '' : 'name',
        subBuilder: LogicalName.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryNameConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryNameConflict copyWith(
          void Function(DirectoryNameConflict) updates) =>
      super.copyWith((message) => updates(message as DirectoryNameConflict))
          as DirectoryNameConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use DirectoryNameConflict() / DirectoryNameConflict.new instead')
  static DirectoryNameConflict create() => DirectoryNameConflict._();
  static $pb.GeneratedMessage $_createMessage() => DirectoryNameConflict._();
  @$core.override
  DirectoryNameConflict createEmptyInstance() => DirectoryNameConflict._();
  @$core.pragma('dart2js:noInline')
  static DirectoryNameConflict getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DirectoryNameConflict>(
          DirectoryNameConflict.$_createMessage);
  static DirectoryNameConflict? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get directoryId => $_getN(0);
  @$pb.TagNumber(1)
  set directoryId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasDirectoryId() => $_has(0);
  @$pb.TagNumber(1)
  void clearDirectoryId() => $_clearField(1);

  @$pb.TagNumber(2)
  LogicalName get name => $_getN(1);
  @$pb.TagNumber(2)
  set name(LogicalName value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => $_clearField(2);
  @$pb.TagNumber(2)
  LogicalName ensureName() => $_ensure(1);
}

class DirectoryRangeConflict extends $pb.GeneratedMessage {
  factory DirectoryRangeConflict({
    $core.List<$core.int>? directoryId,
    LogicalName? after,
    $core.int? maximumEntries,
  }) {
    final result = DirectoryRangeConflict._();
    if (directoryId != null) result.directoryId = directoryId;
    if (after != null) result.after = after;
    if (maximumEntries != null) result.maximumEntries = maximumEntries;
    return result;
  }

  DirectoryRangeConflict._();

  factory DirectoryRangeConflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryRangeConflict()..mergeFromBuffer(data, registry);
  factory DirectoryRangeConflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      DirectoryRangeConflict()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'DirectoryRangeConflict',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: DirectoryRangeConflict.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'directoryId', $pb.PbFieldType.OY)
    ..aOM<LogicalName>(2, _omitFieldNames ? '' : 'after',
        subBuilder: LogicalName.$_createMessage)
    ..aI(3, _omitFieldNames ? '' : 'maximumEntries',
        fieldType: $pb.PbFieldType.OU3)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryRangeConflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  DirectoryRangeConflict copyWith(
          void Function(DirectoryRangeConflict) updates) =>
      super.copyWith((message) => updates(message as DirectoryRangeConflict))
          as DirectoryRangeConflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use DirectoryRangeConflict() / DirectoryRangeConflict.new instead')
  static DirectoryRangeConflict create() => DirectoryRangeConflict._();
  static $pb.GeneratedMessage $_createMessage() => DirectoryRangeConflict._();
  @$core.override
  DirectoryRangeConflict createEmptyInstance() => DirectoryRangeConflict._();
  @$core.pragma('dart2js:noInline')
  static DirectoryRangeConflict getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<DirectoryRangeConflict>(
          DirectoryRangeConflict.$_createMessage);
  static DirectoryRangeConflict? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get directoryId => $_getN(0);
  @$pb.TagNumber(1)
  set directoryId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasDirectoryId() => $_has(0);
  @$pb.TagNumber(1)
  void clearDirectoryId() => $_clearField(1);

  @$pb.TagNumber(2)
  LogicalName get after => $_getN(1);
  @$pb.TagNumber(2)
  set after(LogicalName value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasAfter() => $_has(1);
  @$pb.TagNumber(2)
  void clearAfter() => $_clearField(2);
  @$pb.TagNumber(2)
  LogicalName ensureAfter() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.int get maximumEntries => $_getIZ(2);
  @$pb.TagNumber(3)
  set maximumEntries($core.int value) => $_setUnsignedInt32(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumEntries() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumEntries() => $_clearField(3);
}

enum Conflict_Region {
  fileRecord,
  metadata,
  fileLength,
  contentRange,
  sparseSeek,
  directoryName,
  directoryRange,
  notSet
}

class Conflict extends $pb.GeneratedMessage {
  factory Conflict({
    FileConflict? fileRecord,
    FileConflict? metadata,
    FileConflict? fileLength,
    ContentConflict? contentRange,
    SparseConflict? sparseSeek,
    DirectoryNameConflict? directoryName,
    DirectoryRangeConflict? directoryRange,
    ConflictUse? use,
    $core.List<$core.int>? expectedDigest,
    $core.List<$core.int>? actualDigest,
  }) {
    final result = Conflict._();
    if (fileRecord != null) result.fileRecord = fileRecord;
    if (metadata != null) result.metadata = metadata;
    if (fileLength != null) result.fileLength = fileLength;
    if (contentRange != null) result.contentRange = contentRange;
    if (sparseSeek != null) result.sparseSeek = sparseSeek;
    if (directoryName != null) result.directoryName = directoryName;
    if (directoryRange != null) result.directoryRange = directoryRange;
    if (use != null) result.use = use;
    if (expectedDigest != null) result.expectedDigest = expectedDigest;
    if (actualDigest != null) result.actualDigest = actualDigest;
    return result;
  }

  Conflict._();

  factory Conflict.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Conflict()..mergeFromBuffer(data, registry);
  factory Conflict.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      Conflict()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, Conflict_Region> _Conflict_RegionByTag = {
    1: Conflict_Region.fileRecord,
    2: Conflict_Region.metadata,
    3: Conflict_Region.fileLength,
    4: Conflict_Region.contentRange,
    5: Conflict_Region.sparseSeek,
    6: Conflict_Region.directoryName,
    7: Conflict_Region.directoryRange,
    0: Conflict_Region.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'Conflict',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: Conflict.$_createMessage)
    ..oo(0, [1, 2, 3, 4, 5, 6, 7])
    ..aOM<FileConflict>(1, _omitFieldNames ? '' : 'fileRecord',
        subBuilder: FileConflict.$_createMessage)
    ..aOM<FileConflict>(2, _omitFieldNames ? '' : 'metadata',
        subBuilder: FileConflict.$_createMessage)
    ..aOM<FileConflict>(3, _omitFieldNames ? '' : 'fileLength',
        subBuilder: FileConflict.$_createMessage)
    ..aOM<ContentConflict>(4, _omitFieldNames ? '' : 'contentRange',
        subBuilder: ContentConflict.$_createMessage)
    ..aOM<SparseConflict>(5, _omitFieldNames ? '' : 'sparseSeek',
        subBuilder: SparseConflict.$_createMessage)
    ..aOM<DirectoryNameConflict>(6, _omitFieldNames ? '' : 'directoryName',
        subBuilder: DirectoryNameConflict.$_createMessage)
    ..aOM<DirectoryRangeConflict>(7, _omitFieldNames ? '' : 'directoryRange',
        subBuilder: DirectoryRangeConflict.$_createMessage)
    ..aE<ConflictUse>(8, _omitFieldNames ? '' : 'use',
        enumValues: ConflictUse.values)
    ..a<$core.List<$core.int>>(
        9, _omitFieldNames ? '' : 'expectedDigest', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        10, _omitFieldNames ? '' : 'actualDigest', $pb.PbFieldType.OY)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Conflict clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  Conflict copyWith(void Function(Conflict) updates) =>
      super.copyWith((message) => updates(message as Conflict)) as Conflict;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use Conflict() / Conflict.new instead')
  static Conflict create() => Conflict._();
  static $pb.GeneratedMessage $_createMessage() => Conflict._();
  @$core.override
  Conflict createEmptyInstance() => Conflict._();
  @$core.pragma('dart2js:noInline')
  static Conflict getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<Conflict>(Conflict.$_createMessage);
  static Conflict? _defaultInstance;

  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  Conflict_Region whichRegion() => _Conflict_RegionByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(1)
  @$pb.TagNumber(2)
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  @$pb.TagNumber(5)
  @$pb.TagNumber(6)
  @$pb.TagNumber(7)
  void clearRegion() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  FileConflict get fileRecord => $_getN(0);
  @$pb.TagNumber(1)
  set fileRecord(FileConflict value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasFileRecord() => $_has(0);
  @$pb.TagNumber(1)
  void clearFileRecord() => $_clearField(1);
  @$pb.TagNumber(1)
  FileConflict ensureFileRecord() => $_ensure(0);

  @$pb.TagNumber(2)
  FileConflict get metadata => $_getN(1);
  @$pb.TagNumber(2)
  set metadata(FileConflict value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasMetadata() => $_has(1);
  @$pb.TagNumber(2)
  void clearMetadata() => $_clearField(2);
  @$pb.TagNumber(2)
  FileConflict ensureMetadata() => $_ensure(1);

  @$pb.TagNumber(3)
  FileConflict get fileLength => $_getN(2);
  @$pb.TagNumber(3)
  set fileLength(FileConflict value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasFileLength() => $_has(2);
  @$pb.TagNumber(3)
  void clearFileLength() => $_clearField(3);
  @$pb.TagNumber(3)
  FileConflict ensureFileLength() => $_ensure(2);

  @$pb.TagNumber(4)
  ContentConflict get contentRange => $_getN(3);
  @$pb.TagNumber(4)
  set contentRange(ContentConflict value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasContentRange() => $_has(3);
  @$pb.TagNumber(4)
  void clearContentRange() => $_clearField(4);
  @$pb.TagNumber(4)
  ContentConflict ensureContentRange() => $_ensure(3);

  @$pb.TagNumber(5)
  SparseConflict get sparseSeek => $_getN(4);
  @$pb.TagNumber(5)
  set sparseSeek(SparseConflict value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasSparseSeek() => $_has(4);
  @$pb.TagNumber(5)
  void clearSparseSeek() => $_clearField(5);
  @$pb.TagNumber(5)
  SparseConflict ensureSparseSeek() => $_ensure(4);

  @$pb.TagNumber(6)
  DirectoryNameConflict get directoryName => $_getN(5);
  @$pb.TagNumber(6)
  set directoryName(DirectoryNameConflict value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasDirectoryName() => $_has(5);
  @$pb.TagNumber(6)
  void clearDirectoryName() => $_clearField(6);
  @$pb.TagNumber(6)
  DirectoryNameConflict ensureDirectoryName() => $_ensure(5);

  @$pb.TagNumber(7)
  DirectoryRangeConflict get directoryRange => $_getN(6);
  @$pb.TagNumber(7)
  set directoryRange(DirectoryRangeConflict value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasDirectoryRange() => $_has(6);
  @$pb.TagNumber(7)
  void clearDirectoryRange() => $_clearField(7);
  @$pb.TagNumber(7)
  DirectoryRangeConflict ensureDirectoryRange() => $_ensure(6);

  @$pb.TagNumber(8)
  ConflictUse get use => $_getN(7);
  @$pb.TagNumber(8)
  set use(ConflictUse value) => $_setField(8, value);
  @$pb.TagNumber(8)
  $core.bool hasUse() => $_has(7);
  @$pb.TagNumber(8)
  void clearUse() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.List<$core.int> get expectedDigest => $_getN(8);
  @$pb.TagNumber(9)
  set expectedDigest($core.List<$core.int> value) => $_setBytes(8, value);
  @$pb.TagNumber(9)
  $core.bool hasExpectedDigest() => $_has(8);
  @$pb.TagNumber(9)
  void clearExpectedDigest() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.List<$core.int> get actualDigest => $_getN(9);
  @$pb.TagNumber(10)
  set actualDigest($core.List<$core.int> value) => $_setBytes(9, value);
  @$pb.TagNumber(10)
  $core.bool hasActualDigest() => $_has(9);
  @$pb.TagNumber(10)
  void clearActualDigest() => $_clearField(10);
}

class RebaseResponse extends $pb.GeneratedMessage {
  factory RebaseResponse({
    RebaseStatus? status,
    GenerationRef? generation,
    $core.Iterable<Conflict>? conflicts,
    $core.bool? truncated,
  }) {
    final result = RebaseResponse._();
    if (status != null) result.status = status;
    if (generation != null) result.generation = generation;
    if (conflicts != null) result.conflicts.addAll(conflicts);
    if (truncated != null) result.truncated = truncated;
    return result;
  }

  RebaseResponse._();

  factory RebaseResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RebaseResponse()..mergeFromBuffer(data, registry);
  factory RebaseResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RebaseResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RebaseResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: RebaseResponse.$_createMessage)
    ..aE<RebaseStatus>(1, _omitFieldNames ? '' : 'status',
        enumValues: RebaseStatus.values)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<Conflict>(3, _omitFieldNames ? '' : 'conflicts',
        subBuilder: Conflict.$_createMessage)
    ..aOB(4, _omitFieldNames ? '' : 'truncated')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RebaseResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RebaseResponse copyWith(void Function(RebaseResponse) updates) =>
      super.copyWith((message) => updates(message as RebaseResponse))
          as RebaseResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use RebaseResponse() / RebaseResponse.new instead')
  static RebaseResponse create() => RebaseResponse._();
  static $pb.GeneratedMessage $_createMessage() => RebaseResponse._();
  @$core.override
  RebaseResponse createEmptyInstance() => RebaseResponse._();
  @$core.pragma('dart2js:noInline')
  static RebaseResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<RebaseResponse>(
          RebaseResponse.$_createMessage);
  static RebaseResponse? _defaultInstance;

  @$pb.TagNumber(1)
  RebaseStatus get status => $_getN(0);
  @$pb.TagNumber(1)
  set status(RebaseStatus value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStatus() => $_has(0);
  @$pb.TagNumber(1)
  void clearStatus() => $_clearField(1);

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

  @$pb.TagNumber(3)
  $pb.PbList<Conflict> get conflicts => $_getList(2);

  @$pb.TagNumber(4)
  $core.bool get truncated => $_getBF(3);
  @$pb.TagNumber(4)
  set truncated($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasTruncated() => $_has(3);
  @$pb.TagNumber(4)
  void clearTruncated() => $_clearField(4);
}

class PlanJoinRequest extends $pb.GeneratedMessage {
  factory PlanJoinRequest({
    GenerationRef? source,
    GenerationRef? target,
    $core.int? maximumChanges,
    $core.int? maximumConflicts,
    $core.int? maximumGenerations,
    JoinHistory? history,
  }) {
    final result = PlanJoinRequest._();
    if (source != null) result.source = source;
    if (target != null) result.target = target;
    if (maximumChanges != null) result.maximumChanges = maximumChanges;
    if (maximumConflicts != null) result.maximumConflicts = maximumConflicts;
    if (maximumGenerations != null)
      result.maximumGenerations = maximumGenerations;
    if (history != null) result.history = history;
    return result;
  }

  PlanJoinRequest._();

  factory PlanJoinRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PlanJoinRequest()..mergeFromBuffer(data, registry);
  factory PlanJoinRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      PlanJoinRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'PlanJoinRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: PlanJoinRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'source',
        subBuilder: GenerationRef.$_createMessage)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'target',
        subBuilder: GenerationRef.$_createMessage)
    ..aI(3, _omitFieldNames ? '' : 'maximumChanges',
        fieldType: $pb.PbFieldType.OU3)
    ..aI(4, _omitFieldNames ? '' : 'maximumConflicts',
        fieldType: $pb.PbFieldType.OU3)
    ..aI(5, _omitFieldNames ? '' : 'maximumGenerations',
        fieldType: $pb.PbFieldType.OU3)
    ..aE<JoinHistory>(6, _omitFieldNames ? '' : 'history',
        enumValues: JoinHistory.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PlanJoinRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  PlanJoinRequest copyWith(void Function(PlanJoinRequest) updates) =>
      super.copyWith((message) => updates(message as PlanJoinRequest))
          as PlanJoinRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use PlanJoinRequest() / PlanJoinRequest.new instead')
  static PlanJoinRequest create() => PlanJoinRequest._();
  static $pb.GeneratedMessage $_createMessage() => PlanJoinRequest._();
  @$core.override
  PlanJoinRequest createEmptyInstance() => PlanJoinRequest._();
  @$core.pragma('dart2js:noInline')
  static PlanJoinRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<PlanJoinRequest>(
          PlanJoinRequest.$_createMessage);
  static PlanJoinRequest? _defaultInstance;

  @$pb.TagNumber(1)
  GenerationRef get source => $_getN(0);
  @$pb.TagNumber(1)
  set source(GenerationRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasSource() => $_has(0);
  @$pb.TagNumber(1)
  void clearSource() => $_clearField(1);
  @$pb.TagNumber(1)
  GenerationRef ensureSource() => $_ensure(0);

  @$pb.TagNumber(2)
  GenerationRef get target => $_getN(1);
  @$pb.TagNumber(2)
  set target(GenerationRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasTarget() => $_has(1);
  @$pb.TagNumber(2)
  void clearTarget() => $_clearField(2);
  @$pb.TagNumber(2)
  GenerationRef ensureTarget() => $_ensure(1);

  @$pb.TagNumber(3)
  $core.int get maximumChanges => $_getIZ(2);
  @$pb.TagNumber(3)
  set maximumChanges($core.int value) => $_setUnsignedInt32(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumChanges() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumChanges() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.int get maximumConflicts => $_getIZ(3);
  @$pb.TagNumber(4)
  set maximumConflicts($core.int value) => $_setUnsignedInt32(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumConflicts() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumConflicts() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.int get maximumGenerations => $_getIZ(4);
  @$pb.TagNumber(5)
  set maximumGenerations($core.int value) => $_setUnsignedInt32(4, value);
  @$pb.TagNumber(5)
  $core.bool hasMaximumGenerations() => $_has(4);
  @$pb.TagNumber(5)
  void clearMaximumGenerations() => $_clearField(5);

  @$pb.TagNumber(6)
  JoinHistory get history => $_getN(5);
  @$pb.TagNumber(6)
  set history(JoinHistory value) => $_setField(6, value);
  @$pb.TagNumber(6)
  $core.bool hasHistory() => $_has(5);
  @$pb.TagNumber(6)
  void clearHistory() => $_clearField(6);
}

class JoinPlan extends $pb.GeneratedMessage {
  factory JoinPlan({
    $core.List<$core.int>? planId,
    GenerationRef? source,
    GenerationRef? expectedTarget,
    $core.Iterable<FileRecordChange>? fileChanges,
    $core.Iterable<Conflict>? conflicts,
    $core.bool? truncated,
    GenerationRef? commonAncestor,
    $core.int? maximumGenerations,
    $core.int? maximumChanges,
    $core.int? maximumConflicts,
    JoinHistory? history,
    $core.Iterable<DirectoryBindingChange>? bindingChanges,
  }) {
    final result = JoinPlan._();
    if (planId != null) result.planId = planId;
    if (source != null) result.source = source;
    if (expectedTarget != null) result.expectedTarget = expectedTarget;
    if (fileChanges != null) result.fileChanges.addAll(fileChanges);
    if (conflicts != null) result.conflicts.addAll(conflicts);
    if (truncated != null) result.truncated = truncated;
    if (commonAncestor != null) result.commonAncestor = commonAncestor;
    if (maximumGenerations != null)
      result.maximumGenerations = maximumGenerations;
    if (maximumChanges != null) result.maximumChanges = maximumChanges;
    if (maximumConflicts != null) result.maximumConflicts = maximumConflicts;
    if (history != null) result.history = history;
    if (bindingChanges != null) result.bindingChanges.addAll(bindingChanges);
    return result;
  }

  JoinPlan._();

  factory JoinPlan.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JoinPlan()..mergeFromBuffer(data, registry);
  factory JoinPlan.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JoinPlan()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'JoinPlan',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: JoinPlan.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'planId', $pb.PbFieldType.OY)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'source',
        subBuilder: GenerationRef.$_createMessage)
    ..aOM<GenerationRef>(3, _omitFieldNames ? '' : 'expectedTarget',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<FileRecordChange>(4, _omitFieldNames ? '' : 'fileChanges',
        subBuilder: FileRecordChange.$_createMessage)
    ..pPM<Conflict>(5, _omitFieldNames ? '' : 'conflicts',
        subBuilder: Conflict.$_createMessage)
    ..aOB(6, _omitFieldNames ? '' : 'truncated')
    ..aOM<GenerationRef>(7, _omitFieldNames ? '' : 'commonAncestor',
        subBuilder: GenerationRef.$_createMessage)
    ..aI(8, _omitFieldNames ? '' : 'maximumGenerations',
        fieldType: $pb.PbFieldType.OU3)
    ..aI(9, _omitFieldNames ? '' : 'maximumChanges',
        fieldType: $pb.PbFieldType.OU3)
    ..aI(10, _omitFieldNames ? '' : 'maximumConflicts',
        fieldType: $pb.PbFieldType.OU3)
    ..aE<JoinHistory>(11, _omitFieldNames ? '' : 'history',
        enumValues: JoinHistory.values)
    ..pPM<DirectoryBindingChange>(12, _omitFieldNames ? '' : 'bindingChanges',
        subBuilder: DirectoryBindingChange.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JoinPlan clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JoinPlan copyWith(void Function(JoinPlan) updates) =>
      super.copyWith((message) => updates(message as JoinPlan)) as JoinPlan;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use JoinPlan() / JoinPlan.new instead')
  static JoinPlan create() => JoinPlan._();
  static $pb.GeneratedMessage $_createMessage() => JoinPlan._();
  @$core.override
  JoinPlan createEmptyInstance() => JoinPlan._();
  @$core.pragma('dart2js:noInline')
  static JoinPlan getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<JoinPlan>(JoinPlan.$_createMessage);
  static JoinPlan? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get planId => $_getN(0);
  @$pb.TagNumber(1)
  set planId($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasPlanId() => $_has(0);
  @$pb.TagNumber(1)
  void clearPlanId() => $_clearField(1);

  @$pb.TagNumber(2)
  GenerationRef get source => $_getN(1);
  @$pb.TagNumber(2)
  set source(GenerationRef value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasSource() => $_has(1);
  @$pb.TagNumber(2)
  void clearSource() => $_clearField(2);
  @$pb.TagNumber(2)
  GenerationRef ensureSource() => $_ensure(1);

  @$pb.TagNumber(3)
  GenerationRef get expectedTarget => $_getN(2);
  @$pb.TagNumber(3)
  set expectedTarget(GenerationRef value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasExpectedTarget() => $_has(2);
  @$pb.TagNumber(3)
  void clearExpectedTarget() => $_clearField(3);
  @$pb.TagNumber(3)
  GenerationRef ensureExpectedTarget() => $_ensure(2);

  @$pb.TagNumber(4)
  $pb.PbList<FileRecordChange> get fileChanges => $_getList(3);

  @$pb.TagNumber(5)
  $pb.PbList<Conflict> get conflicts => $_getList(4);

  @$pb.TagNumber(6)
  $core.bool get truncated => $_getBF(5);
  @$pb.TagNumber(6)
  set truncated($core.bool value) => $_setBool(5, value);
  @$pb.TagNumber(6)
  $core.bool hasTruncated() => $_has(5);
  @$pb.TagNumber(6)
  void clearTruncated() => $_clearField(6);

  @$pb.TagNumber(7)
  GenerationRef get commonAncestor => $_getN(6);
  @$pb.TagNumber(7)
  set commonAncestor(GenerationRef value) => $_setField(7, value);
  @$pb.TagNumber(7)
  $core.bool hasCommonAncestor() => $_has(6);
  @$pb.TagNumber(7)
  void clearCommonAncestor() => $_clearField(7);
  @$pb.TagNumber(7)
  GenerationRef ensureCommonAncestor() => $_ensure(6);

  @$pb.TagNumber(8)
  $core.int get maximumGenerations => $_getIZ(7);
  @$pb.TagNumber(8)
  set maximumGenerations($core.int value) => $_setUnsignedInt32(7, value);
  @$pb.TagNumber(8)
  $core.bool hasMaximumGenerations() => $_has(7);
  @$pb.TagNumber(8)
  void clearMaximumGenerations() => $_clearField(8);

  @$pb.TagNumber(9)
  $core.int get maximumChanges => $_getIZ(8);
  @$pb.TagNumber(9)
  set maximumChanges($core.int value) => $_setUnsignedInt32(8, value);
  @$pb.TagNumber(9)
  $core.bool hasMaximumChanges() => $_has(8);
  @$pb.TagNumber(9)
  void clearMaximumChanges() => $_clearField(9);

  @$pb.TagNumber(10)
  $core.int get maximumConflicts => $_getIZ(9);
  @$pb.TagNumber(10)
  set maximumConflicts($core.int value) => $_setUnsignedInt32(9, value);
  @$pb.TagNumber(10)
  $core.bool hasMaximumConflicts() => $_has(9);
  @$pb.TagNumber(10)
  void clearMaximumConflicts() => $_clearField(10);

  @$pb.TagNumber(11)
  JoinHistory get history => $_getN(10);
  @$pb.TagNumber(11)
  set history(JoinHistory value) => $_setField(11, value);
  @$pb.TagNumber(11)
  $core.bool hasHistory() => $_has(10);
  @$pb.TagNumber(11)
  void clearHistory() => $_clearField(11);

  @$pb.TagNumber(12)
  $pb.PbList<DirectoryBindingChange> get bindingChanges => $_getList(11);
}

class ApplyJoinRequest extends $pb.GeneratedMessage {
  factory ApplyJoinRequest({
    JoinPlan? plan,
    OperationOptions? operation,
  }) {
    final result = ApplyJoinRequest._();
    if (plan != null) result.plan = plan;
    if (operation != null) result.operation = operation;
    return result;
  }

  ApplyJoinRequest._();

  factory ApplyJoinRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ApplyJoinRequest()..mergeFromBuffer(data, registry);
  factory ApplyJoinRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ApplyJoinRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ApplyJoinRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ApplyJoinRequest.$_createMessage)
    ..aOM<JoinPlan>(1, _omitFieldNames ? '' : 'plan',
        subBuilder: JoinPlan.$_createMessage)
    ..aOM<OperationOptions>(2, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ApplyJoinRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ApplyJoinRequest copyWith(void Function(ApplyJoinRequest) updates) =>
      super.copyWith((message) => updates(message as ApplyJoinRequest))
          as ApplyJoinRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ApplyJoinRequest() / ApplyJoinRequest.new instead')
  static ApplyJoinRequest create() => ApplyJoinRequest._();
  static $pb.GeneratedMessage $_createMessage() => ApplyJoinRequest._();
  @$core.override
  ApplyJoinRequest createEmptyInstance() => ApplyJoinRequest._();
  @$core.pragma('dart2js:noInline')
  static ApplyJoinRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ApplyJoinRequest>(
          ApplyJoinRequest.$_createMessage);
  static ApplyJoinRequest? _defaultInstance;

  @$pb.TagNumber(1)
  JoinPlan get plan => $_getN(0);
  @$pb.TagNumber(1)
  set plan(JoinPlan value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasPlan() => $_has(0);
  @$pb.TagNumber(1)
  void clearPlan() => $_clearField(1);
  @$pb.TagNumber(1)
  JoinPlan ensurePlan() => $_ensure(0);

  @$pb.TagNumber(2)
  OperationOptions get operation => $_getN(1);
  @$pb.TagNumber(2)
  set operation(OperationOptions value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOperation() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperation() => $_clearField(2);
  @$pb.TagNumber(2)
  OperationOptions ensureOperation() => $_ensure(1);
}

class JoinResponse extends $pb.GeneratedMessage {
  factory JoinResponse({
    JoinStatus? status,
    GenerationRef? generation,
    $core.Iterable<Conflict>? conflicts,
    $core.bool? truncated,
  }) {
    final result = JoinResponse._();
    if (status != null) result.status = status;
    if (generation != null) result.generation = generation;
    if (conflicts != null) result.conflicts.addAll(conflicts);
    if (truncated != null) result.truncated = truncated;
    return result;
  }

  JoinResponse._();

  factory JoinResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JoinResponse()..mergeFromBuffer(data, registry);
  factory JoinResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      JoinResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'JoinResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: JoinResponse.$_createMessage)
    ..aE<JoinStatus>(1, _omitFieldNames ? '' : 'status',
        enumValues: JoinStatus.values)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..pPM<Conflict>(3, _omitFieldNames ? '' : 'conflicts',
        subBuilder: Conflict.$_createMessage)
    ..aOB(4, _omitFieldNames ? '' : 'truncated')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JoinResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  JoinResponse copyWith(void Function(JoinResponse) updates) =>
      super.copyWith((message) => updates(message as JoinResponse))
          as JoinResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use JoinResponse() / JoinResponse.new instead')
  static JoinResponse create() => JoinResponse._();
  static $pb.GeneratedMessage $_createMessage() => JoinResponse._();
  @$core.override
  JoinResponse createEmptyInstance() => JoinResponse._();
  @$core.pragma('dart2js:noInline')
  static JoinResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<JoinResponse>(
          JoinResponse.$_createMessage);
  static JoinResponse? _defaultInstance;

  @$pb.TagNumber(1)
  JoinStatus get status => $_getN(0);
  @$pb.TagNumber(1)
  set status(JoinStatus value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasStatus() => $_has(0);
  @$pb.TagNumber(1)
  void clearStatus() => $_clearField(1);

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

  @$pb.TagNumber(3)
  $pb.PbList<Conflict> get conflicts => $_getList(2);

  @$pb.TagNumber(4)
  $core.bool get truncated => $_getBF(3);
  @$pb.TagNumber(4)
  set truncated($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasTruncated() => $_has(3);
  @$pb.TagNumber(4)
  void clearTruncated() => $_clearField(4);
}

class RetainGenerationRequest extends $pb.GeneratedMessage {
  factory RetainGenerationRequest({
    GenerationRef? generation,
    $core.String? identity,
    OperationOptions? operation,
  }) {
    final result = RetainGenerationRequest._();
    if (generation != null) result.generation = generation;
    if (identity != null) result.identity = identity;
    if (operation != null) result.operation = operation;
    return result;
  }

  RetainGenerationRequest._();

  factory RetainGenerationRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetainGenerationRequest()..mergeFromBuffer(data, registry);
  factory RetainGenerationRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetainGenerationRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RetainGenerationRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: RetainGenerationRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'identity')
    ..aOM<OperationOptions>(3, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetainGenerationRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetainGenerationRequest copyWith(
          void Function(RetainGenerationRequest) updates) =>
      super.copyWith((message) => updates(message as RetainGenerationRequest))
          as RetainGenerationRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use RetainGenerationRequest() / RetainGenerationRequest.new instead')
  static RetainGenerationRequest create() => RetainGenerationRequest._();
  static $pb.GeneratedMessage $_createMessage() => RetainGenerationRequest._();
  @$core.override
  RetainGenerationRequest createEmptyInstance() => RetainGenerationRequest._();
  @$core.pragma('dart2js:noInline')
  static RetainGenerationRequest getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RetainGenerationRequest>(
          RetainGenerationRequest.$_createMessage);
  static RetainGenerationRequest? _defaultInstance;

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
  $core.String get identity => $_getSZ(1);
  @$pb.TagNumber(2)
  set identity($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdentity() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdentity() => $_clearField(2);

  @$pb.TagNumber(3)
  OperationOptions get operation => $_getN(2);
  @$pb.TagNumber(3)
  set operation(OperationOptions value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasOperation() => $_has(2);
  @$pb.TagNumber(3)
  void clearOperation() => $_clearField(3);
  @$pb.TagNumber(3)
  OperationOptions ensureOperation() => $_ensure(2);
}

class RetainGenerationResponse extends $pb.GeneratedMessage {
  factory RetainGenerationResponse({
    GenerationRef? generation,
    $core.String? identity,
    MutationStatus? status,
  }) {
    final result = RetainGenerationResponse._();
    if (generation != null) result.generation = generation;
    if (identity != null) result.identity = identity;
    if (status != null) result.status = status;
    return result;
  }

  RetainGenerationResponse._();

  factory RetainGenerationResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetainGenerationResponse()..mergeFromBuffer(data, registry);
  factory RetainGenerationResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      RetainGenerationResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'RetainGenerationResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: RetainGenerationResponse.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOS(2, _omitFieldNames ? '' : 'identity')
    ..aE<MutationStatus>(3, _omitFieldNames ? '' : 'status',
        enumValues: MutationStatus.values)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetainGenerationResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  RetainGenerationResponse copyWith(
          void Function(RetainGenerationResponse) updates) =>
      super.copyWith((message) => updates(message as RetainGenerationResponse))
          as RetainGenerationResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated(
      'Use RetainGenerationResponse() / RetainGenerationResponse.new instead')
  static RetainGenerationResponse create() => RetainGenerationResponse._();
  static $pb.GeneratedMessage $_createMessage() => RetainGenerationResponse._();
  @$core.override
  RetainGenerationResponse createEmptyInstance() =>
      RetainGenerationResponse._();
  @$core.pragma('dart2js:noInline')
  static RetainGenerationResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<RetainGenerationResponse>(
          RetainGenerationResponse.$_createMessage);
  static RetainGenerationResponse? _defaultInstance;

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
  $core.String get identity => $_getSZ(1);
  @$pb.TagNumber(2)
  set identity($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasIdentity() => $_has(1);
  @$pb.TagNumber(2)
  void clearIdentity() => $_clearField(2);

  @$pb.TagNumber(3)
  MutationStatus get status => $_getN(2);
  @$pb.TagNumber(3)
  set status(MutationStatus value) => $_setField(3, value);
  @$pb.TagNumber(3)
  $core.bool hasStatus() => $_has(2);
  @$pb.TagNumber(3)
  void clearStatus() => $_clearField(3);
}

class ExportRequest extends $pb.GeneratedMessage {
  factory ExportRequest({
    GenerationRef? generation,
    $core.List<$core.int>? after,
    $core.int? maximumObjects,
    $fixnum.Int64? maximumBytes,
  }) {
    final result = ExportRequest._();
    if (generation != null) result.generation = generation;
    if (after != null) result.after = after;
    if (maximumObjects != null) result.maximumObjects = maximumObjects;
    if (maximumBytes != null) result.maximumBytes = maximumBytes;
    return result;
  }

  ExportRequest._();

  factory ExportRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExportRequest()..mergeFromBuffer(data, registry);
  factory ExportRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExportRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExportRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ExportRequest.$_createMessage)
    ..aOM<GenerationRef>(1, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'after', $pb.PbFieldType.OY)
    ..aI(3, _omitFieldNames ? '' : 'maximumObjects',
        fieldType: $pb.PbFieldType.OU3)
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'maximumBytes', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExportRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExportRequest copyWith(void Function(ExportRequest) updates) =>
      super.copyWith((message) => updates(message as ExportRequest))
          as ExportRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExportRequest() / ExportRequest.new instead')
  static ExportRequest create() => ExportRequest._();
  static $pb.GeneratedMessage $_createMessage() => ExportRequest._();
  @$core.override
  ExportRequest createEmptyInstance() => ExportRequest._();
  @$core.pragma('dart2js:noInline')
  static ExportRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ExportRequest>(
          ExportRequest.$_createMessage);
  static ExportRequest? _defaultInstance;

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
  $core.List<$core.int> get after => $_getN(1);
  @$pb.TagNumber(2)
  set after($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasAfter() => $_has(1);
  @$pb.TagNumber(2)
  void clearAfter() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.int get maximumObjects => $_getIZ(2);
  @$pb.TagNumber(3)
  set maximumObjects($core.int value) => $_setUnsignedInt32(2, value);
  @$pb.TagNumber(3)
  $core.bool hasMaximumObjects() => $_has(2);
  @$pb.TagNumber(3)
  void clearMaximumObjects() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get maximumBytes => $_getI64(3);
  @$pb.TagNumber(4)
  set maximumBytes($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasMaximumBytes() => $_has(3);
  @$pb.TagNumber(4)
  void clearMaximumBytes() => $_clearField(4);
}

class ExportChunk extends $pb.GeneratedMessage {
  factory ExportChunk({
    $core.List<$core.int>? cursor,
    $core.List<$core.int>? objectId,
    $core.List<$core.int>? contents,
    $core.bool? terminal,
  }) {
    final result = ExportChunk._();
    if (cursor != null) result.cursor = cursor;
    if (objectId != null) result.objectId = objectId;
    if (contents != null) result.contents = contents;
    if (terminal != null) result.terminal = terminal;
    return result;
  }

  ExportChunk._();

  factory ExportChunk.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExportChunk()..mergeFromBuffer(data, registry);
  factory ExportChunk.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ExportChunk()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ExportChunk',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ExportChunk.$_createMessage)
    ..a<$core.List<$core.int>>(
        1, _omitFieldNames ? '' : 'cursor', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'objectId', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'contents', $pb.PbFieldType.OY)
    ..aOB(4, _omitFieldNames ? '' : 'terminal')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExportChunk clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ExportChunk copyWith(void Function(ExportChunk) updates) =>
      super.copyWith((message) => updates(message as ExportChunk))
          as ExportChunk;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ExportChunk() / ExportChunk.new instead')
  static ExportChunk create() => ExportChunk._();
  static $pb.GeneratedMessage $_createMessage() => ExportChunk._();
  @$core.override
  ExportChunk createEmptyInstance() => ExportChunk._();
  @$core.pragma('dart2js:noInline')
  static ExportChunk getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ExportChunk>(
          ExportChunk.$_createMessage);
  static ExportChunk? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<$core.int> get cursor => $_getN(0);
  @$pb.TagNumber(1)
  set cursor($core.List<$core.int> value) => $_setBytes(0, value);
  @$pb.TagNumber(1)
  $core.bool hasCursor() => $_has(0);
  @$pb.TagNumber(1)
  void clearCursor() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.int> get objectId => $_getN(1);
  @$pb.TagNumber(2)
  set objectId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasObjectId() => $_has(1);
  @$pb.TagNumber(2)
  void clearObjectId() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get contents => $_getN(2);
  @$pb.TagNumber(3)
  set contents($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasContents() => $_has(2);
  @$pb.TagNumber(3)
  void clearContents() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.bool get terminal => $_getBF(3);
  @$pb.TagNumber(4)
  set terminal($core.bool value) => $_setBool(3, value);
  @$pb.TagNumber(4)
  $core.bool hasTerminal() => $_has(3);
  @$pb.TagNumber(4)
  void clearTerminal() => $_clearField(4);
}

class ImportChunk extends $pb.GeneratedMessage {
  factory ImportChunk({
    WorkspaceRef? workspace,
    $core.List<$core.int>? operationId,
    $core.List<$core.int>? cursor,
    $core.List<$core.int>? objectId,
    $core.List<$core.int>? contents,
    $core.bool? terminal,
  }) {
    final result = ImportChunk._();
    if (workspace != null) result.workspace = workspace;
    if (operationId != null) result.operationId = operationId;
    if (cursor != null) result.cursor = cursor;
    if (objectId != null) result.objectId = objectId;
    if (contents != null) result.contents = contents;
    if (terminal != null) result.terminal = terminal;
    return result;
  }

  ImportChunk._();

  factory ImportChunk.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ImportChunk()..mergeFromBuffer(data, registry);
  factory ImportChunk.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ImportChunk()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ImportChunk',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ImportChunk.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'operationId', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        3, _omitFieldNames ? '' : 'cursor', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        4, _omitFieldNames ? '' : 'objectId', $pb.PbFieldType.OY)
    ..a<$core.List<$core.int>>(
        5, _omitFieldNames ? '' : 'contents', $pb.PbFieldType.OY)
    ..aOB(6, _omitFieldNames ? '' : 'terminal')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ImportChunk clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ImportChunk copyWith(void Function(ImportChunk) updates) =>
      super.copyWith((message) => updates(message as ImportChunk))
          as ImportChunk;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ImportChunk() / ImportChunk.new instead')
  static ImportChunk create() => ImportChunk._();
  static $pb.GeneratedMessage $_createMessage() => ImportChunk._();
  @$core.override
  ImportChunk createEmptyInstance() => ImportChunk._();
  @$core.pragma('dart2js:noInline')
  static ImportChunk getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ImportChunk>(
          ImportChunk.$_createMessage);
  static ImportChunk? _defaultInstance;

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get operationId => $_getN(1);
  @$pb.TagNumber(2)
  set operationId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasOperationId() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperationId() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.int> get cursor => $_getN(2);
  @$pb.TagNumber(3)
  set cursor($core.List<$core.int> value) => $_setBytes(2, value);
  @$pb.TagNumber(3)
  $core.bool hasCursor() => $_has(2);
  @$pb.TagNumber(3)
  void clearCursor() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.List<$core.int> get objectId => $_getN(3);
  @$pb.TagNumber(4)
  set objectId($core.List<$core.int> value) => $_setBytes(3, value);
  @$pb.TagNumber(4)
  $core.bool hasObjectId() => $_has(3);
  @$pb.TagNumber(4)
  void clearObjectId() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.List<$core.int> get contents => $_getN(4);
  @$pb.TagNumber(5)
  set contents($core.List<$core.int> value) => $_setBytes(4, value);
  @$pb.TagNumber(5)
  $core.bool hasContents() => $_has(4);
  @$pb.TagNumber(5)
  void clearContents() => $_clearField(5);

  @$pb.TagNumber(6)
  $core.bool get terminal => $_getBF(5);
  @$pb.TagNumber(6)
  set terminal($core.bool value) => $_setBool(5, value);
  @$pb.TagNumber(6)
  $core.bool hasTerminal() => $_has(5);
  @$pb.TagNumber(6)
  void clearTerminal() => $_clearField(6);
}

class ImportResponse extends $pb.GeneratedMessage {
  factory ImportResponse({
    MutationResponse? outcome,
  }) {
    final result = ImportResponse._();
    if (outcome != null) result.outcome = outcome;
    return result;
  }

  ImportResponse._();

  factory ImportResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ImportResponse()..mergeFromBuffer(data, registry);
  factory ImportResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ImportResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ImportResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ImportResponse.$_createMessage)
    ..aOM<MutationResponse>(1, _omitFieldNames ? '' : 'outcome',
        subBuilder: MutationResponse.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ImportResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ImportResponse copyWith(void Function(ImportResponse) updates) =>
      super.copyWith((message) => updates(message as ImportResponse))
          as ImportResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ImportResponse() / ImportResponse.new instead')
  static ImportResponse create() => ImportResponse._();
  static $pb.GeneratedMessage $_createMessage() => ImportResponse._();
  @$core.override
  ImportResponse createEmptyInstance() => ImportResponse._();
  @$core.pragma('dart2js:noInline')
  static ImportResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ImportResponse>(
          ImportResponse.$_createMessage);
  static ImportResponse? _defaultInstance;

  @$pb.TagNumber(1)
  MutationResponse get outcome => $_getN(0);
  @$pb.TagNumber(1)
  set outcome(MutationResponse value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOutcome() => $_has(0);
  @$pb.TagNumber(1)
  void clearOutcome() => $_clearField(1);
  @$pb.TagNumber(1)
  MutationResponse ensureOutcome() => $_ensure(0);
}

class CredentialRequest extends $pb.GeneratedMessage {
  factory CredentialRequest({
    WorkspaceRef? workspace,
    GenerationRef? generation,
    $core.bool? writable,
    $fixnum.Int64? expiresAfterSeconds,
    OperationOptions? operation,
  }) {
    final result = CredentialRequest._();
    if (workspace != null) result.workspace = workspace;
    if (generation != null) result.generation = generation;
    if (writable != null) result.writable = writable;
    if (expiresAfterSeconds != null)
      result.expiresAfterSeconds = expiresAfterSeconds;
    if (operation != null) result.operation = operation;
    return result;
  }

  CredentialRequest._();

  factory CredentialRequest.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CredentialRequest()..mergeFromBuffer(data, registry);
  factory CredentialRequest.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CredentialRequest()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CredentialRequest',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CredentialRequest.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..aOM<GenerationRef>(2, _omitFieldNames ? '' : 'generation',
        subBuilder: GenerationRef.$_createMessage)
    ..aOB(3, _omitFieldNames ? '' : 'writable')
    ..a<$fixnum.Int64>(
        4, _omitFieldNames ? '' : 'expiresAfterSeconds', $pb.PbFieldType.OU6,
        defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOM<OperationOptions>(5, _omitFieldNames ? '' : 'operation',
        subBuilder: OperationOptions.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CredentialRequest clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CredentialRequest copyWith(void Function(CredentialRequest) updates) =>
      super.copyWith((message) => updates(message as CredentialRequest))
          as CredentialRequest;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CredentialRequest() / CredentialRequest.new instead')
  static CredentialRequest create() => CredentialRequest._();
  static $pb.GeneratedMessage $_createMessage() => CredentialRequest._();
  @$core.override
  CredentialRequest createEmptyInstance() => CredentialRequest._();
  @$core.pragma('dart2js:noInline')
  static CredentialRequest getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<CredentialRequest>(
          CredentialRequest.$_createMessage);
  static CredentialRequest? _defaultInstance;

  @$pb.TagNumber(1)
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

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

  @$pb.TagNumber(3)
  $core.bool get writable => $_getBF(2);
  @$pb.TagNumber(3)
  set writable($core.bool value) => $_setBool(2, value);
  @$pb.TagNumber(3)
  $core.bool hasWritable() => $_has(2);
  @$pb.TagNumber(3)
  void clearWritable() => $_clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get expiresAfterSeconds => $_getI64(3);
  @$pb.TagNumber(4)
  set expiresAfterSeconds($fixnum.Int64 value) => $_setInt64(3, value);
  @$pb.TagNumber(4)
  $core.bool hasExpiresAfterSeconds() => $_has(3);
  @$pb.TagNumber(4)
  void clearExpiresAfterSeconds() => $_clearField(4);

  @$pb.TagNumber(5)
  OperationOptions get operation => $_getN(4);
  @$pb.TagNumber(5)
  set operation(OperationOptions value) => $_setField(5, value);
  @$pb.TagNumber(5)
  $core.bool hasOperation() => $_has(4);
  @$pb.TagNumber(5)
  void clearOperation() => $_clearField(5);
  @$pb.TagNumber(5)
  OperationOptions ensureOperation() => $_ensure(4);
}

enum CredentialResponse_Credential { bearerToken, s3, notSet }

class CredentialResponse extends $pb.GeneratedMessage {
  factory CredentialResponse({
    $core.String? endpoint,
    $fixnum.Int64? expiresAtUnixSeconds,
    $core.String? bearerToken,
    S3Credential? s3,
  }) {
    final result = CredentialResponse._();
    if (endpoint != null) result.endpoint = endpoint;
    if (expiresAtUnixSeconds != null)
      result.expiresAtUnixSeconds = expiresAtUnixSeconds;
    if (bearerToken != null) result.bearerToken = bearerToken;
    if (s3 != null) result.s3 = s3;
    return result;
  }

  CredentialResponse._();

  factory CredentialResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CredentialResponse()..mergeFromBuffer(data, registry);
  factory CredentialResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      CredentialResponse()..mergeFromJson(json, registry);

  static const $core.Map<$core.int, CredentialResponse_Credential>
      _CredentialResponse_CredentialByTag = {
    3: CredentialResponse_Credential.bearerToken,
    4: CredentialResponse_Credential.s3,
    0: CredentialResponse_Credential.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'CredentialResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CredentialResponse.$_createMessage)
    ..oo(0, [3, 4])
    ..aOS(1, _omitFieldNames ? '' : 'endpoint')
    ..aInt64(2, _omitFieldNames ? '' : 'expiresAtUnixSeconds')
    ..aOS(3, _omitFieldNames ? '' : 'bearerToken')
    ..aOM<S3Credential>(4, _omitFieldNames ? '' : 's3',
        subBuilder: S3Credential.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CredentialResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  CredentialResponse copyWith(void Function(CredentialResponse) updates) =>
      super.copyWith((message) => updates(message as CredentialResponse))
          as CredentialResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use CredentialResponse() / CredentialResponse.new instead')
  static CredentialResponse create() => CredentialResponse._();
  static $pb.GeneratedMessage $_createMessage() => CredentialResponse._();
  @$core.override
  CredentialResponse createEmptyInstance() => CredentialResponse._();
  @$core.pragma('dart2js:noInline')
  static CredentialResponse getDefault() => _defaultInstance ??=
      $pb.GeneratedMessage.$_defaultFor<CredentialResponse>(
          CredentialResponse.$_createMessage);
  static CredentialResponse? _defaultInstance;

  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  CredentialResponse_Credential whichCredential() =>
      _CredentialResponse_CredentialByTag[$_whichOneof(0)]!;
  @$pb.TagNumber(3)
  @$pb.TagNumber(4)
  void clearCredential() => $_clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.String get endpoint => $_getSZ(0);
  @$pb.TagNumber(1)
  set endpoint($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasEndpoint() => $_has(0);
  @$pb.TagNumber(1)
  void clearEndpoint() => $_clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get expiresAtUnixSeconds => $_getI64(1);
  @$pb.TagNumber(2)
  set expiresAtUnixSeconds($fixnum.Int64 value) => $_setInt64(1, value);
  @$pb.TagNumber(2)
  $core.bool hasExpiresAtUnixSeconds() => $_has(1);
  @$pb.TagNumber(2)
  void clearExpiresAtUnixSeconds() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get bearerToken => $_getSZ(2);
  @$pb.TagNumber(3)
  set bearerToken($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasBearerToken() => $_has(2);
  @$pb.TagNumber(3)
  void clearBearerToken() => $_clearField(3);

  @$pb.TagNumber(4)
  S3Credential get s3 => $_getN(3);
  @$pb.TagNumber(4)
  set s3(S3Credential value) => $_setField(4, value);
  @$pb.TagNumber(4)
  $core.bool hasS3() => $_has(3);
  @$pb.TagNumber(4)
  void clearS3() => $_clearField(4);
  @$pb.TagNumber(4)
  S3Credential ensureS3() => $_ensure(3);
}

class S3Credential extends $pb.GeneratedMessage {
  factory S3Credential({
    $core.String? bucket,
    $core.String? region,
    $core.String? accessKeyId,
    $core.String? secretAccessKey,
    $core.String? sessionToken,
  }) {
    final result = S3Credential._();
    if (bucket != null) result.bucket = bucket;
    if (region != null) result.region = region;
    if (accessKeyId != null) result.accessKeyId = accessKeyId;
    if (secretAccessKey != null) result.secretAccessKey = secretAccessKey;
    if (sessionToken != null) result.sessionToken = sessionToken;
    return result;
  }

  S3Credential._();

  factory S3Credential.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      S3Credential()..mergeFromBuffer(data, registry);
  factory S3Credential.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      S3Credential()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'S3Credential',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: S3Credential.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'bucket')
    ..aOS(2, _omitFieldNames ? '' : 'region')
    ..aOS(3, _omitFieldNames ? '' : 'accessKeyId')
    ..aOS(4, _omitFieldNames ? '' : 'secretAccessKey')
    ..aOS(5, _omitFieldNames ? '' : 'sessionToken')
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  S3Credential clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  S3Credential copyWith(void Function(S3Credential) updates) =>
      super.copyWith((message) => updates(message as S3Credential))
          as S3Credential;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use S3Credential() / S3Credential.new instead')
  static S3Credential create() => S3Credential._();
  static $pb.GeneratedMessage $_createMessage() => S3Credential._();
  @$core.override
  S3Credential createEmptyInstance() => S3Credential._();
  @$core.pragma('dart2js:noInline')
  static S3Credential getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<S3Credential>(
          S3Credential.$_createMessage);
  static S3Credential? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get bucket => $_getSZ(0);
  @$pb.TagNumber(1)
  set bucket($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasBucket() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucket() => $_clearField(1);

  @$pb.TagNumber(2)
  $core.String get region => $_getSZ(1);
  @$pb.TagNumber(2)
  set region($core.String value) => $_setString(1, value);
  @$pb.TagNumber(2)
  $core.bool hasRegion() => $_has(1);
  @$pb.TagNumber(2)
  void clearRegion() => $_clearField(2);

  @$pb.TagNumber(3)
  $core.String get accessKeyId => $_getSZ(2);
  @$pb.TagNumber(3)
  set accessKeyId($core.String value) => $_setString(2, value);
  @$pb.TagNumber(3)
  $core.bool hasAccessKeyId() => $_has(2);
  @$pb.TagNumber(3)
  void clearAccessKeyId() => $_clearField(3);

  @$pb.TagNumber(4)
  $core.String get secretAccessKey => $_getSZ(3);
  @$pb.TagNumber(4)
  set secretAccessKey($core.String value) => $_setString(3, value);
  @$pb.TagNumber(4)
  $core.bool hasSecretAccessKey() => $_has(3);
  @$pb.TagNumber(4)
  void clearSecretAccessKey() => $_clearField(4);

  @$pb.TagNumber(5)
  $core.String get sessionToken => $_getSZ(4);
  @$pb.TagNumber(5)
  set sessionToken($core.String value) => $_setString(4, value);
  @$pb.TagNumber(5)
  $core.bool hasSessionToken() => $_has(4);
  @$pb.TagNumber(5)
  void clearSessionToken() => $_clearField(5);
}

class ObserveRequest extends $pb.GeneratedMessage {
  factory ObserveRequest({
    WorkspaceRef? workspace,
    $core.List<$core.int>? operationId,
  }) {
    final result = ObserveRequest._();
    if (workspace != null) result.workspace = workspace;
    if (operationId != null) result.operationId = operationId;
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
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ObserveRequest.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'operationId', $pb.PbFieldType.OY)
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
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get operationId => $_getN(1);
  @$pb.TagNumber(2)
  set operationId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasOperationId() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperationId() => $_clearField(2);
}

class ObserveResponse extends $pb.GeneratedMessage {
  factory ObserveResponse({
    $core.String? state,
    MutationResponse? outcome,
  }) {
    final result = ObserveResponse._();
    if (state != null) result.state = state;
    if (outcome != null) result.outcome = outcome;
    return result;
  }

  ObserveResponse._();

  factory ObserveResponse.fromBuffer($core.List<$core.int> data,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObserveResponse()..mergeFromBuffer(data, registry);
  factory ObserveResponse.fromJson($core.String json,
          [$pb.ExtensionRegistry registry = $pb.ExtensionRegistry.EMPTY]) =>
      ObserveResponse()..mergeFromJson(json, registry);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(
      _omitMessageNames ? '' : 'ObserveResponse',
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: ObserveResponse.$_createMessage)
    ..aOS(1, _omitFieldNames ? '' : 'state')
    ..aOM<MutationResponse>(2, _omitFieldNames ? '' : 'outcome',
        subBuilder: MutationResponse.$_createMessage)
    ..hasRequiredFields = false;

  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObserveResponse clone() => deepCopy();
  @$core.Deprecated('See https://github.com/google/protobuf.dart/issues/998.')
  ObserveResponse copyWith(void Function(ObserveResponse) updates) =>
      super.copyWith((message) => updates(message as ObserveResponse))
          as ObserveResponse;

  @$core.override
  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  @$core.Deprecated('Use ObserveResponse() / ObserveResponse.new instead')
  static ObserveResponse create() => ObserveResponse._();
  static $pb.GeneratedMessage $_createMessage() => ObserveResponse._();
  @$core.override
  ObserveResponse createEmptyInstance() => ObserveResponse._();
  @$core.pragma('dart2js:noInline')
  static ObserveResponse getDefault() =>
      _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<ObserveResponse>(
          ObserveResponse.$_createMessage);
  static ObserveResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get state => $_getSZ(0);
  @$pb.TagNumber(1)
  set state($core.String value) => $_setString(0, value);
  @$pb.TagNumber(1)
  $core.bool hasState() => $_has(0);
  @$pb.TagNumber(1)
  void clearState() => $_clearField(1);

  @$pb.TagNumber(2)
  MutationResponse get outcome => $_getN(1);
  @$pb.TagNumber(2)
  set outcome(MutationResponse value) => $_setField(2, value);
  @$pb.TagNumber(2)
  $core.bool hasOutcome() => $_has(1);
  @$pb.TagNumber(2)
  void clearOutcome() => $_clearField(2);
  @$pb.TagNumber(2)
  MutationResponse ensureOutcome() => $_ensure(1);
}

class CancelRequest extends $pb.GeneratedMessage {
  factory CancelRequest({
    WorkspaceRef? workspace,
    $core.List<$core.int>? operationId,
  }) {
    final result = CancelRequest._();
    if (workspace != null) result.workspace = workspace;
    if (operationId != null) result.operationId = operationId;
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
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CancelRequest.$_createMessage)
    ..aOM<WorkspaceRef>(1, _omitFieldNames ? '' : 'workspace',
        subBuilder: WorkspaceRef.$_createMessage)
    ..a<$core.List<$core.int>>(
        2, _omitFieldNames ? '' : 'operationId', $pb.PbFieldType.OY)
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
  WorkspaceRef get workspace => $_getN(0);
  @$pb.TagNumber(1)
  set workspace(WorkspaceRef value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasWorkspace() => $_has(0);
  @$pb.TagNumber(1)
  void clearWorkspace() => $_clearField(1);
  @$pb.TagNumber(1)
  WorkspaceRef ensureWorkspace() => $_ensure(0);

  @$pb.TagNumber(2)
  $core.List<$core.int> get operationId => $_getN(1);
  @$pb.TagNumber(2)
  set operationId($core.List<$core.int> value) => $_setBytes(1, value);
  @$pb.TagNumber(2)
  $core.bool hasOperationId() => $_has(1);
  @$pb.TagNumber(2)
  void clearOperationId() => $_clearField(2);
}

class CancelResponse extends $pb.GeneratedMessage {
  factory CancelResponse({
    ObserveResponse? operation,
  }) {
    final result = CancelResponse._();
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
      package: const $pb.PackageName(
          _omitMessageNames ? '' : 'acyclic.filesystem.v2'),
      createEmptyInstance: CancelResponse.$_createMessage)
    ..aOM<ObserveResponse>(1, _omitFieldNames ? '' : 'operation',
        subBuilder: ObserveResponse.$_createMessage)
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
  ObserveResponse get operation => $_getN(0);
  @$pb.TagNumber(1)
  set operation(ObserveResponse value) => $_setField(1, value);
  @$pb.TagNumber(1)
  $core.bool hasOperation() => $_has(0);
  @$pb.TagNumber(1)
  void clearOperation() => $_clearField(1);
  @$pb.TagNumber(1)
  ObserveResponse ensureOperation() => $_ensure(0);
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
