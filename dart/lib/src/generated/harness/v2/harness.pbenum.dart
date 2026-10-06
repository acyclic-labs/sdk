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

import 'package:protobuf/protobuf.dart' as $pb;

class ErrorCode extends $pb.ProtobufEnum {
  static const ErrorCode ERROR_CODE_UNSPECIFIED =
      ErrorCode._(0, _omitEnumNames ? '' : 'ERROR_CODE_UNSPECIFIED');
  static const ErrorCode ERROR_CODE_NOT_FOUND =
      ErrorCode._(1, _omitEnumNames ? '' : 'ERROR_CODE_NOT_FOUND');
  static const ErrorCode ERROR_CODE_CONFLICT =
      ErrorCode._(2, _omitEnumNames ? '' : 'ERROR_CODE_CONFLICT');
  static const ErrorCode ERROR_CODE_UNSUPPORTED =
      ErrorCode._(3, _omitEnumNames ? '' : 'ERROR_CODE_UNSUPPORTED');
  static const ErrorCode ERROR_CODE_INVALID =
      ErrorCode._(4, _omitEnumNames ? '' : 'ERROR_CODE_INVALID');
  static const ErrorCode ERROR_CODE_UNAUTHORIZED =
      ErrorCode._(5, _omitEnumNames ? '' : 'ERROR_CODE_UNAUTHORIZED');
  static const ErrorCode ERROR_CODE_STORAGE =
      ErrorCode._(6, _omitEnumNames ? '' : 'ERROR_CODE_STORAGE');
  static const ErrorCode ERROR_CODE_INDETERMINATE =
      ErrorCode._(7, _omitEnumNames ? '' : 'ERROR_CODE_INDETERMINATE');
  static const ErrorCode ERROR_CODE_INTERACTION_DECLINED =
      ErrorCode._(8, _omitEnumNames ? '' : 'ERROR_CODE_INTERACTION_DECLINED');
  static const ErrorCode ERROR_CODE_INTERACTION_CANCELLED =
      ErrorCode._(9, _omitEnumNames ? '' : 'ERROR_CODE_INTERACTION_CANCELLED');
  static const ErrorCode ERROR_CODE_INTERACTION_EXPIRED =
      ErrorCode._(10, _omitEnumNames ? '' : 'ERROR_CODE_INTERACTION_EXPIRED');
  static const ErrorCode ERROR_CODE_INTERACTION_DENIED =
      ErrorCode._(11, _omitEnumNames ? '' : 'ERROR_CODE_INTERACTION_DENIED');

  static const $core.List<ErrorCode> values = <ErrorCode>[
    ERROR_CODE_UNSPECIFIED,
    ERROR_CODE_NOT_FOUND,
    ERROR_CODE_CONFLICT,
    ERROR_CODE_UNSUPPORTED,
    ERROR_CODE_INVALID,
    ERROR_CODE_UNAUTHORIZED,
    ERROR_CODE_STORAGE,
    ERROR_CODE_INDETERMINATE,
    ERROR_CODE_INTERACTION_DECLINED,
    ERROR_CODE_INTERACTION_CANCELLED,
    ERROR_CODE_INTERACTION_EXPIRED,
    ERROR_CODE_INTERACTION_DENIED,
  ];

  static final $core.List<ErrorCode?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 11);
  static ErrorCode? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ErrorCode._(super.value, super.name);
}

class AdmissionState extends $pb.ProtobufEnum {
  static const AdmissionState ADMISSION_STATE_UNSPECIFIED =
      AdmissionState._(0, _omitEnumNames ? '' : 'ADMISSION_STATE_UNSPECIFIED');
  static const AdmissionState ADMISSION_STATE_ACCEPTED =
      AdmissionState._(1, _omitEnumNames ? '' : 'ADMISSION_STATE_ACCEPTED');
  static const AdmissionState ADMISSION_STATE_REJECTED =
      AdmissionState._(2, _omitEnumNames ? '' : 'ADMISSION_STATE_REJECTED');
  static const AdmissionState ADMISSION_STATE_INDETERMINATE = AdmissionState._(
      3, _omitEnumNames ? '' : 'ADMISSION_STATE_INDETERMINATE');

  static const $core.List<AdmissionState> values = <AdmissionState>[
    ADMISSION_STATE_UNSPECIFIED,
    ADMISSION_STATE_ACCEPTED,
    ADMISSION_STATE_REJECTED,
    ADMISSION_STATE_INDETERMINATE,
  ];

  static final $core.List<AdmissionState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static AdmissionState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const AdmissionState._(super.value, super.name);
}

class CompletionState extends $pb.ProtobufEnum {
  static const CompletionState COMPLETION_STATE_UNSPECIFIED = CompletionState._(
      0, _omitEnumNames ? '' : 'COMPLETION_STATE_UNSPECIFIED');
  static const CompletionState COMPLETION_STATE_RUNNING =
      CompletionState._(1, _omitEnumNames ? '' : 'COMPLETION_STATE_RUNNING');
  static const CompletionState COMPLETION_STATE_SUCCEEDED =
      CompletionState._(2, _omitEnumNames ? '' : 'COMPLETION_STATE_SUCCEEDED');
  static const CompletionState COMPLETION_STATE_FAILED =
      CompletionState._(3, _omitEnumNames ? '' : 'COMPLETION_STATE_FAILED');
  static const CompletionState COMPLETION_STATE_CANCELLED =
      CompletionState._(4, _omitEnumNames ? '' : 'COMPLETION_STATE_CANCELLED');
  static const CompletionState COMPLETION_STATE_INDETERMINATE =
      CompletionState._(
          5, _omitEnumNames ? '' : 'COMPLETION_STATE_INDETERMINATE');

  static const $core.List<CompletionState> values = <CompletionState>[
    COMPLETION_STATE_UNSPECIFIED,
    COMPLETION_STATE_RUNNING,
    COMPLETION_STATE_SUCCEEDED,
    COMPLETION_STATE_FAILED,
    COMPLETION_STATE_CANCELLED,
    COMPLETION_STATE_INDETERMINATE,
  ];

  static final $core.List<CompletionState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static CompletionState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const CompletionState._(super.value, super.name);
}

/// Kind of independently ordered durable aggregate.
class AggregateKind extends $pb.ProtobufEnum {
  static const AggregateKind AGGREGATE_KIND_UNSPECIFIED =
      AggregateKind._(0, _omitEnumNames ? '' : 'AGGREGATE_KIND_UNSPECIFIED');
  static const AggregateKind AGGREGATE_KIND_AGENT =
      AggregateKind._(1, _omitEnumNames ? '' : 'AGGREGATE_KIND_AGENT');
  static const AggregateKind AGGREGATE_KIND_CONVERSATION =
      AggregateKind._(2, _omitEnumNames ? '' : 'AGGREGATE_KIND_CONVERSATION');
  static const AggregateKind AGGREGATE_KIND_SESSION =
      AggregateKind._(3, _omitEnumNames ? '' : 'AGGREGATE_KIND_SESSION');
  static const AggregateKind AGGREGATE_KIND_TURN =
      AggregateKind._(4, _omitEnumNames ? '' : 'AGGREGATE_KIND_TURN');
  static const AggregateKind AGGREGATE_KIND_TASK =
      AggregateKind._(5, _omitEnumNames ? '' : 'AGGREGATE_KIND_TASK');

  static const $core.List<AggregateKind> values = <AggregateKind>[
    AGGREGATE_KIND_UNSPECIFIED,
    AGGREGATE_KIND_AGENT,
    AGGREGATE_KIND_CONVERSATION,
    AGGREGATE_KIND_SESSION,
    AGGREGATE_KIND_TURN,
    AGGREGATE_KIND_TASK,
  ];

  static final $core.List<AggregateKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static AggregateKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const AggregateKind._(super.value, super.name);
}

class ApplyState extends $pb.ProtobufEnum {
  static const ApplyState APPLY_STATE_UNSPECIFIED =
      ApplyState._(0, _omitEnumNames ? '' : 'APPLY_STATE_UNSPECIFIED');
  static const ApplyState APPLY_STATE_APPLIED =
      ApplyState._(1, _omitEnumNames ? '' : 'APPLY_STATE_APPLIED');
  static const ApplyState APPLY_STATE_REPLAYED =
      ApplyState._(2, _omitEnumNames ? '' : 'APPLY_STATE_REPLAYED');

  static const $core.List<ApplyState> values = <ApplyState>[
    APPLY_STATE_UNSPECIFIED,
    APPLY_STATE_APPLIED,
    APPLY_STATE_REPLAYED,
  ];

  static final $core.List<ApplyState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static ApplyState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ApplyState._(super.value, super.name);
}

class VolumeClass extends $pb.ProtobufEnum {
  static const VolumeClass VOLUME_CLASS_UNSPECIFIED =
      VolumeClass._(0, _omitEnumNames ? '' : 'VOLUME_CLASS_UNSPECIFIED');
  static const VolumeClass VOLUME_CLASS_PROJECT =
      VolumeClass._(1, _omitEnumNames ? '' : 'VOLUME_CLASS_PROJECT');
  static const VolumeClass VOLUME_CLASS_AGENT_PRIVATE =
      VolumeClass._(2, _omitEnumNames ? '' : 'VOLUME_CLASS_AGENT_PRIVATE');
  static const VolumeClass VOLUME_CLASS_SESSION_SHARED =
      VolumeClass._(3, _omitEnumNames ? '' : 'VOLUME_CLASS_SESSION_SHARED');

  static const $core.List<VolumeClass> values = <VolumeClass>[
    VOLUME_CLASS_UNSPECIFIED,
    VOLUME_CLASS_PROJECT,
    VOLUME_CLASS_AGENT_PRIVATE,
    VOLUME_CLASS_SESSION_SHARED,
  ];

  static final $core.List<VolumeClass?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static VolumeClass? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const VolumeClass._(super.value, super.name);
}

class ExtensionForkPolicy extends $pb.ProtobufEnum {
  static const ExtensionForkPolicy EXTENSION_FORK_POLICY_UNSPECIFIED =
      ExtensionForkPolicy._(
          0, _omitEnumNames ? '' : 'EXTENSION_FORK_POLICY_UNSPECIFIED');
  static const ExtensionForkPolicy EXTENSION_FORK_POLICY_INHERIT =
      ExtensionForkPolicy._(
          1, _omitEnumNames ? '' : 'EXTENSION_FORK_POLICY_INHERIT');
  static const ExtensionForkPolicy EXTENSION_FORK_POLICY_RESET =
      ExtensionForkPolicy._(
          2, _omitEnumNames ? '' : 'EXTENSION_FORK_POLICY_RESET');
  static const ExtensionForkPolicy EXTENSION_FORK_POLICY_REJECT =
      ExtensionForkPolicy._(
          3, _omitEnumNames ? '' : 'EXTENSION_FORK_POLICY_REJECT');

  static const $core.List<ExtensionForkPolicy> values = <ExtensionForkPolicy>[
    EXTENSION_FORK_POLICY_UNSPECIFIED,
    EXTENSION_FORK_POLICY_INHERIT,
    EXTENSION_FORK_POLICY_RESET,
    EXTENSION_FORK_POLICY_REJECT,
  ];

  static final $core.List<ExtensionForkPolicy?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static ExtensionForkPolicy? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ExtensionForkPolicy._(super.value, super.name);
}

class InteractionKind extends $pb.ProtobufEnum {
  static const InteractionKind INTERACTION_KIND_UNSPECIFIED = InteractionKind._(
      0, _omitEnumNames ? '' : 'INTERACTION_KIND_UNSPECIFIED');
  static const InteractionKind INTERACTION_KIND_QUESTION =
      InteractionKind._(1, _omitEnumNames ? '' : 'INTERACTION_KIND_QUESTION');
  static const InteractionKind INTERACTION_KIND_CHOICE =
      InteractionKind._(2, _omitEnumNames ? '' : 'INTERACTION_KIND_CHOICE');
  static const InteractionKind INTERACTION_KIND_FORM =
      InteractionKind._(3, _omitEnumNames ? '' : 'INTERACTION_KIND_FORM');
  static const InteractionKind INTERACTION_KIND_APPROVAL =
      InteractionKind._(4, _omitEnumNames ? '' : 'INTERACTION_KIND_APPROVAL');

  static const $core.List<InteractionKind> values = <InteractionKind>[
    INTERACTION_KIND_UNSPECIFIED,
    INTERACTION_KIND_QUESTION,
    INTERACTION_KIND_CHOICE,
    INTERACTION_KIND_FORM,
    INTERACTION_KIND_APPROVAL,
  ];

  static final $core.List<InteractionKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 4);
  static InteractionKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const InteractionKind._(super.value, super.name);
}

class ConversationKind extends $pb.ProtobufEnum {
  static const ConversationKind CONVERSATION_KIND_UNSPECIFIED =
      ConversationKind._(
          0, _omitEnumNames ? '' : 'CONVERSATION_KIND_UNSPECIFIED');
  static const ConversationKind CONVERSATION_KIND_USER =
      ConversationKind._(1, _omitEnumNames ? '' : 'CONVERSATION_KIND_USER');
  static const ConversationKind CONVERSATION_KIND_ASSISTANT =
      ConversationKind._(
          2, _omitEnumNames ? '' : 'CONVERSATION_KIND_ASSISTANT');
  static const ConversationKind CONVERSATION_KIND_SYSTEM =
      ConversationKind._(3, _omitEnumNames ? '' : 'CONVERSATION_KIND_SYSTEM');
  static const ConversationKind CONVERSATION_KIND_TOOL_CALL =
      ConversationKind._(
          4, _omitEnumNames ? '' : 'CONVERSATION_KIND_TOOL_CALL');
  static const ConversationKind CONVERSATION_KIND_TOOL_RESULT =
      ConversationKind._(
          5, _omitEnumNames ? '' : 'CONVERSATION_KIND_TOOL_RESULT');
  static const ConversationKind CONVERSATION_KIND_INTERACTION =
      ConversationKind._(
          6, _omitEnumNames ? '' : 'CONVERSATION_KIND_INTERACTION');
  static const ConversationKind CONVERSATION_KIND_PERMISSION =
      ConversationKind._(
          7, _omitEnumNames ? '' : 'CONVERSATION_KIND_PERMISSION');
  static const ConversationKind CONVERSATION_KIND_FORK =
      ConversationKind._(8, _omitEnumNames ? '' : 'CONVERSATION_KIND_FORK');
  static const ConversationKind CONVERSATION_KIND_MERGE =
      ConversationKind._(9, _omitEnumNames ? '' : 'CONVERSATION_KIND_MERGE');

  static const $core.List<ConversationKind> values = <ConversationKind>[
    CONVERSATION_KIND_UNSPECIFIED,
    CONVERSATION_KIND_USER,
    CONVERSATION_KIND_ASSISTANT,
    CONVERSATION_KIND_SYSTEM,
    CONVERSATION_KIND_TOOL_CALL,
    CONVERSATION_KIND_TOOL_RESULT,
    CONVERSATION_KIND_INTERACTION,
    CONVERSATION_KIND_PERMISSION,
    CONVERSATION_KIND_FORK,
    CONVERSATION_KIND_MERGE,
  ];

  static final $core.List<ConversationKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 9);
  static ConversationKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ConversationKind._(super.value, super.name);
}

class ResourceKind extends $pb.ProtobufEnum {
  static const ResourceKind RESOURCE_KIND_UNSPECIFIED =
      ResourceKind._(0, _omitEnumNames ? '' : 'RESOURCE_KIND_UNSPECIFIED');
  static const ResourceKind RESOURCE_KIND_WORKSPACE =
      ResourceKind._(1, _omitEnumNames ? '' : 'RESOURCE_KIND_WORKSPACE');
  static const ResourceKind RESOURCE_KIND_GENERATION =
      ResourceKind._(2, _omitEnumNames ? '' : 'RESOURCE_KIND_GENERATION');
  static const ResourceKind RESOURCE_KIND_ARTIFACT =
      ResourceKind._(3, _omitEnumNames ? '' : 'RESOURCE_KIND_ARTIFACT');
  static const ResourceKind RESOURCE_KIND_SANDBOX =
      ResourceKind._(4, _omitEnumNames ? '' : 'RESOURCE_KIND_SANDBOX');
  static const ResourceKind RESOURCE_KIND_CHECKPOINT =
      ResourceKind._(5, _omitEnumNames ? '' : 'RESOURCE_KIND_CHECKPOINT');
  static const ResourceKind RESOURCE_KIND_STREAM =
      ResourceKind._(6, _omitEnumNames ? '' : 'RESOURCE_KIND_STREAM');
  static const ResourceKind RESOURCE_KIND_CONTEXT =
      ResourceKind._(7, _omitEnumNames ? '' : 'RESOURCE_KIND_CONTEXT');
  static const ResourceKind RESOURCE_KIND_RUN =
      ResourceKind._(8, _omitEnumNames ? '' : 'RESOURCE_KIND_RUN');

  static const $core.List<ResourceKind> values = <ResourceKind>[
    RESOURCE_KIND_UNSPECIFIED,
    RESOURCE_KIND_WORKSPACE,
    RESOURCE_KIND_GENERATION,
    RESOURCE_KIND_ARTIFACT,
    RESOURCE_KIND_SANDBOX,
    RESOURCE_KIND_CHECKPOINT,
    RESOURCE_KIND_STREAM,
    RESOURCE_KIND_CONTEXT,
    RESOURCE_KIND_RUN,
  ];

  static final $core.List<ResourceKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 8);
  static ResourceKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ResourceKind._(super.value, super.name);
}

class BatchGroupPolicy extends $pb.ProtobufEnum {
  static const BatchGroupPolicy BATCH_GROUP_POLICY_UNSPECIFIED =
      BatchGroupPolicy._(
          0, _omitEnumNames ? '' : 'BATCH_GROUP_POLICY_UNSPECIFIED');
  static const BatchGroupPolicy BATCH_GROUP_POLICY_COLLECT_ALL =
      BatchGroupPolicy._(
          1, _omitEnumNames ? '' : 'BATCH_GROUP_POLICY_COLLECT_ALL');
  static const BatchGroupPolicy BATCH_GROUP_POLICY_CANCEL_ON_FAILURE =
      BatchGroupPolicy._(
          2, _omitEnumNames ? '' : 'BATCH_GROUP_POLICY_CANCEL_ON_FAILURE');

  static const $core.List<BatchGroupPolicy> values = <BatchGroupPolicy>[
    BATCH_GROUP_POLICY_UNSPECIFIED,
    BATCH_GROUP_POLICY_COLLECT_ALL,
    BATCH_GROUP_POLICY_CANCEL_ON_FAILURE,
  ];

  static final $core.List<BatchGroupPolicy?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static BatchGroupPolicy? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const BatchGroupPolicy._(super.value, super.name);
}

class SharedVolumeOperation extends $pb.ProtobufEnum {
  static const SharedVolumeOperation SHARED_VOLUME_OPERATION_UNSPECIFIED =
      SharedVolumeOperation._(
          0, _omitEnumNames ? '' : 'SHARED_VOLUME_OPERATION_UNSPECIFIED');
  static const SharedVolumeOperation SHARED_VOLUME_OPERATION_READ =
      SharedVolumeOperation._(
          1, _omitEnumNames ? '' : 'SHARED_VOLUME_OPERATION_READ');
  static const SharedVolumeOperation SHARED_VOLUME_OPERATION_WRITE =
      SharedVolumeOperation._(
          2, _omitEnumNames ? '' : 'SHARED_VOLUME_OPERATION_WRITE');

  static const $core.List<SharedVolumeOperation> values =
      <SharedVolumeOperation>[
    SHARED_VOLUME_OPERATION_UNSPECIFIED,
    SHARED_VOLUME_OPERATION_READ,
    SHARED_VOLUME_OPERATION_WRITE,
  ];

  static final $core.List<SharedVolumeOperation?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static SharedVolumeOperation? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const SharedVolumeOperation._(super.value, super.name);
}

class PrivateDirectoryEntry_Kind extends $pb.ProtobufEnum {
  static const PrivateDirectoryEntry_Kind KIND_UNSPECIFIED =
      PrivateDirectoryEntry_Kind._(0, _omitEnumNames ? '' : 'KIND_UNSPECIFIED');
  static const PrivateDirectoryEntry_Kind KIND_FILE =
      PrivateDirectoryEntry_Kind._(1, _omitEnumNames ? '' : 'KIND_FILE');
  static const PrivateDirectoryEntry_Kind KIND_DIRECTORY =
      PrivateDirectoryEntry_Kind._(2, _omitEnumNames ? '' : 'KIND_DIRECTORY');

  static const $core.List<PrivateDirectoryEntry_Kind> values =
      <PrivateDirectoryEntry_Kind>[
    KIND_UNSPECIFIED,
    KIND_FILE,
    KIND_DIRECTORY,
  ];

  static final $core.List<PrivateDirectoryEntry_Kind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 2);
  static PrivateDirectoryEntry_Kind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const PrivateDirectoryEntry_Kind._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
