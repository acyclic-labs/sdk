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

import 'package:protobuf/protobuf.dart' as $pb;

class WarmState extends $pb.ProtobufEnum {
  static const WarmState WARM_STATE_UNSPECIFIED =
      WarmState._(0, _omitEnumNames ? '' : 'WARM_STATE_UNSPECIFIED');
  static const WarmState WARM_STATE_ACTIVE =
      WarmState._(1, _omitEnumNames ? '' : 'WARM_STATE_ACTIVE');
  static const WarmState WARM_STATE_EXPIRED =
      WarmState._(2, _omitEnumNames ? '' : 'WARM_STATE_EXPIRED');
  static const WarmState WARM_STATE_BREACHED =
      WarmState._(3, _omitEnumNames ? '' : 'WARM_STATE_BREACHED');
  static const WarmState WARM_STATE_RELEASED =
      WarmState._(4, _omitEnumNames ? '' : 'WARM_STATE_RELEASED');

  static const $core.List<WarmState> values = <WarmState>[
    WARM_STATE_UNSPECIFIED,
    WARM_STATE_ACTIVE,
    WARM_STATE_EXPIRED,
    WARM_STATE_BREACHED,
    WARM_STATE_RELEASED,
  ];

  static final $core.List<WarmState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 4);
  static WarmState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const WarmState._(super.value, super.name);
}

class EvaluationAggregation extends $pb.ProtobufEnum {
  static const EvaluationAggregation EVALUATION_AGGREGATION_UNSPECIFIED =
      EvaluationAggregation._(
          0, _omitEnumNames ? '' : 'EVALUATION_AGGREGATION_UNSPECIFIED');
  static const EvaluationAggregation EVALUATION_AGGREGATION_MEAN =
      EvaluationAggregation._(
          1, _omitEnumNames ? '' : 'EVALUATION_AGGREGATION_MEAN');
  static const EvaluationAggregation EVALUATION_AGGREGATION_SUM =
      EvaluationAggregation._(
          2, _omitEnumNames ? '' : 'EVALUATION_AGGREGATION_SUM');
  static const EvaluationAggregation EVALUATION_AGGREGATION_MINIMUM =
      EvaluationAggregation._(
          3, _omitEnumNames ? '' : 'EVALUATION_AGGREGATION_MINIMUM');
  static const EvaluationAggregation EVALUATION_AGGREGATION_MAXIMUM =
      EvaluationAggregation._(
          4, _omitEnumNames ? '' : 'EVALUATION_AGGREGATION_MAXIMUM');

  static const $core.List<EvaluationAggregation> values =
      <EvaluationAggregation>[
    EVALUATION_AGGREGATION_UNSPECIFIED,
    EVALUATION_AGGREGATION_MEAN,
    EVALUATION_AGGREGATION_SUM,
    EVALUATION_AGGREGATION_MINIMUM,
    EVALUATION_AGGREGATION_MAXIMUM,
  ];

  static final $core.List<EvaluationAggregation?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 4);
  static EvaluationAggregation? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const EvaluationAggregation._(super.value, super.name);
}

class EvaluationCaseOutcome extends $pb.ProtobufEnum {
  static const EvaluationCaseOutcome EVALUATION_CASE_OUTCOME_UNSPECIFIED =
      EvaluationCaseOutcome._(
          0, _omitEnumNames ? '' : 'EVALUATION_CASE_OUTCOME_UNSPECIFIED');
  static const EvaluationCaseOutcome EVALUATION_CASE_OUTCOME_SCORED =
      EvaluationCaseOutcome._(
          1, _omitEnumNames ? '' : 'EVALUATION_CASE_OUTCOME_SCORED');
  static const EvaluationCaseOutcome EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED =
      EvaluationCaseOutcome._(
          2, _omitEnumNames ? '' : 'EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED');
  static const EvaluationCaseOutcome EVALUATION_CASE_OUTCOME_GRADER_FAILED =
      EvaluationCaseOutcome._(
          3, _omitEnumNames ? '' : 'EVALUATION_CASE_OUTCOME_GRADER_FAILED');

  static const $core.List<EvaluationCaseOutcome> values =
      <EvaluationCaseOutcome>[
    EVALUATION_CASE_OUTCOME_UNSPECIFIED,
    EVALUATION_CASE_OUTCOME_SCORED,
    EVALUATION_CASE_OUTCOME_CANDIDATE_FAILED,
    EVALUATION_CASE_OUTCOME_GRADER_FAILED,
  ];

  static final $core.List<EvaluationCaseOutcome?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 3);
  static EvaluationCaseOutcome? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const EvaluationCaseOutcome._(super.value, super.name);
}

class EvaluationState extends $pb.ProtobufEnum {
  static const EvaluationState EVALUATION_STATE_UNSPECIFIED = EvaluationState._(
      0, _omitEnumNames ? '' : 'EVALUATION_STATE_UNSPECIFIED');
  static const EvaluationState EVALUATION_STATE_ADMITTED =
      EvaluationState._(1, _omitEnumNames ? '' : 'EVALUATION_STATE_ADMITTED');
  static const EvaluationState EVALUATION_STATE_RUNNING =
      EvaluationState._(2, _omitEnumNames ? '' : 'EVALUATION_STATE_RUNNING');
  static const EvaluationState EVALUATION_STATE_COMPLETED =
      EvaluationState._(3, _omitEnumNames ? '' : 'EVALUATION_STATE_COMPLETED');
  static const EvaluationState EVALUATION_STATE_FAILED =
      EvaluationState._(4, _omitEnumNames ? '' : 'EVALUATION_STATE_FAILED');
  static const EvaluationState EVALUATION_STATE_CANCELLED =
      EvaluationState._(5, _omitEnumNames ? '' : 'EVALUATION_STATE_CANCELLED');

  static const $core.List<EvaluationState> values = <EvaluationState>[
    EVALUATION_STATE_UNSPECIFIED,
    EVALUATION_STATE_ADMITTED,
    EVALUATION_STATE_RUNNING,
    EVALUATION_STATE_COMPLETED,
    EVALUATION_STATE_FAILED,
    EVALUATION_STATE_CANCELLED,
  ];

  static final $core.List<EvaluationState?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 5);
  static EvaluationState? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const EvaluationState._(super.value, super.name);
}

class ItemKind extends $pb.ProtobufEnum {
  static const ItemKind ITEM_KIND_UNSPECIFIED =
      ItemKind._(0, _omitEnumNames ? '' : 'ITEM_KIND_UNSPECIFIED');
  static const ItemKind ITEM_KIND_INSTRUCTION =
      ItemKind._(1, _omitEnumNames ? '' : 'ITEM_KIND_INSTRUCTION');
  static const ItemKind ITEM_KIND_SYSTEM =
      ItemKind._(2, _omitEnumNames ? '' : 'ITEM_KIND_SYSTEM');
  static const ItemKind ITEM_KIND_DEVELOPER =
      ItemKind._(3, _omitEnumNames ? '' : 'ITEM_KIND_DEVELOPER');
  static const ItemKind ITEM_KIND_USER =
      ItemKind._(4, _omitEnumNames ? '' : 'ITEM_KIND_USER');
  static const ItemKind ITEM_KIND_ASSISTANT =
      ItemKind._(5, _omitEnumNames ? '' : 'ITEM_KIND_ASSISTANT');
  static const ItemKind ITEM_KIND_TOOL_DEFINITION =
      ItemKind._(6, _omitEnumNames ? '' : 'ITEM_KIND_TOOL_DEFINITION');
  static const ItemKind ITEM_KIND_TOOL_CALL =
      ItemKind._(7, _omitEnumNames ? '' : 'ITEM_KIND_TOOL_CALL');
  static const ItemKind ITEM_KIND_TOOL_RESULT =
      ItemKind._(8, _omitEnumNames ? '' : 'ITEM_KIND_TOOL_RESULT');
  static const ItemKind ITEM_KIND_IMAGE =
      ItemKind._(9, _omitEnumNames ? '' : 'ITEM_KIND_IMAGE');
  static const ItemKind ITEM_KIND_AUDIO =
      ItemKind._(10, _omitEnumNames ? '' : 'ITEM_KIND_AUDIO');
  static const ItemKind ITEM_KIND_FILE =
      ItemKind._(11, _omitEnumNames ? '' : 'ITEM_KIND_FILE');
  static const ItemKind ITEM_KIND_CONTINUATION =
      ItemKind._(12, _omitEnumNames ? '' : 'ITEM_KIND_CONTINUATION');

  static const $core.List<ItemKind> values = <ItemKind>[
    ITEM_KIND_UNSPECIFIED,
    ITEM_KIND_INSTRUCTION,
    ITEM_KIND_SYSTEM,
    ITEM_KIND_DEVELOPER,
    ITEM_KIND_USER,
    ITEM_KIND_ASSISTANT,
    ITEM_KIND_TOOL_DEFINITION,
    ITEM_KIND_TOOL_CALL,
    ITEM_KIND_TOOL_RESULT,
    ITEM_KIND_IMAGE,
    ITEM_KIND_AUDIO,
    ITEM_KIND_FILE,
    ITEM_KIND_CONTINUATION,
  ];

  static final $core.List<ItemKind?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 12);
  static ItemKind? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const ItemKind._(super.value, super.name);
}

class RunTerminal extends $pb.ProtobufEnum {
  static const RunTerminal RUN_TERMINAL_UNSPECIFIED =
      RunTerminal._(0, _omitEnumNames ? '' : 'RUN_TERMINAL_UNSPECIFIED');
  static const RunTerminal RUN_TERMINAL_COMPLETED =
      RunTerminal._(1, _omitEnumNames ? '' : 'RUN_TERMINAL_COMPLETED');
  static const RunTerminal RUN_TERMINAL_OUTPUT_LIMITED =
      RunTerminal._(2, _omitEnumNames ? '' : 'RUN_TERMINAL_OUTPUT_LIMITED');
  static const RunTerminal RUN_TERMINAL_TOOL_CALL =
      RunTerminal._(3, _omitEnumNames ? '' : 'RUN_TERMINAL_TOOL_CALL');
  static const RunTerminal RUN_TERMINAL_REFUSAL =
      RunTerminal._(4, _omitEnumNames ? '' : 'RUN_TERMINAL_REFUSAL');
  static const RunTerminal RUN_TERMINAL_CANCELLED =
      RunTerminal._(5, _omitEnumNames ? '' : 'RUN_TERMINAL_CANCELLED');
  static const RunTerminal RUN_TERMINAL_FAILED =
      RunTerminal._(6, _omitEnumNames ? '' : 'RUN_TERMINAL_FAILED');
  static const RunTerminal RUN_TERMINAL_INDETERMINATE =
      RunTerminal._(7, _omitEnumNames ? '' : 'RUN_TERMINAL_INDETERMINATE');

  static const $core.List<RunTerminal> values = <RunTerminal>[
    RUN_TERMINAL_UNSPECIFIED,
    RUN_TERMINAL_COMPLETED,
    RUN_TERMINAL_OUTPUT_LIMITED,
    RUN_TERMINAL_TOOL_CALL,
    RUN_TERMINAL_REFUSAL,
    RUN_TERMINAL_CANCELLED,
    RUN_TERMINAL_FAILED,
    RUN_TERMINAL_INDETERMINATE,
  ];

  static final $core.List<RunTerminal?> _byValue =
      $pb.ProtobufEnum.$_initByValueList(values, 7);
  static RunTerminal? valueOf($core.int value) =>
      value < 0 || value >= _byValue.length ? null : _byValue[value];

  const RunTerminal._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
