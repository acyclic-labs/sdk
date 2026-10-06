// This is a generated file - do not edit.
//
// Generated from validation/v1/options.proto.

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

class Options {
  static final nonzeroFixedBytes = $pb.Extension<$core.int>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'nonzeroFixedBytes',
      51001,
      $pb.PbFieldType.OU3);
  static final requiredMessage = $pb.Extension<$core.bool>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'requiredMessage',
      51002,
      $pb.PbFieldType.OB);
  static final positiveUint64 = $pb.Extension<$core.bool>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'positiveUint64',
      51003,
      $pb.PbFieldType.OB);
  static final minItems = $pb.Extension<$core.int>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'minItems',
      51005,
      $pb.PbFieldType.OU3);
  static final maxItems = $pb.Extension<$core.int>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'maxItems',
      51006,
      $pb.PbFieldType.OU3);
  static final nonemptyMaxBytes = $pb.Extension<$core.int>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'nonemptyMaxBytes',
      51007,
      $pb.PbFieldType.OU3);
  static final maxUint64 = $pb.Extension<$fixnum.Int64>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'maxUint64',
      51008,
      $pb.PbFieldType.OU6,
      defaultOrMaker: $fixnum.Int64.ZERO);
  static final knownNonzeroEnum = $pb.Extension<$core.bool>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'knownNonzeroEnum',
      51009,
      $pb.PbFieldType.OB);
  static final nonemptyMaxItemBytes = $pb.Extension<$core.int>(
      _omitMessageNames ? '' : 'google.protobuf.FieldOptions',
      _omitFieldNames ? '' : 'nonemptyMaxItemBytes',
      51010,
      $pb.PbFieldType.OU3);
  static final partialTerminal = $pb.Extension<$core.bool>(
      _omitMessageNames ? '' : 'google.protobuf.EnumValueOptions',
      _omitFieldNames ? '' : 'partialTerminal',
      51011,
      $pb.PbFieldType.OB);
  static final requiredOneof = $pb.Extension<$core.bool>(
      _omitMessageNames ? '' : 'google.protobuf.OneofOptions',
      _omitFieldNames ? '' : 'requiredOneof',
      51004,
      $pb.PbFieldType.OB);
  static final httpPath = $pb.Extension<$core.String>(
      _omitMessageNames ? '' : 'google.protobuf.MethodOptions',
      _omitFieldNames ? '' : 'httpPath',
      51012,
      $pb.PbFieldType.OS);
  static void registerAllExtensions($pb.ExtensionRegistry registry) {
    registry.add(nonzeroFixedBytes);
    registry.add(requiredMessage);
    registry.add(positiveUint64);
    registry.add(minItems);
    registry.add(maxItems);
    registry.add(nonemptyMaxBytes);
    registry.add(maxUint64);
    registry.add(knownNonzeroEnum);
    registry.add(nonemptyMaxItemBytes);
    registry.add(partialTerminal);
    registry.add(requiredOneof);
    registry.add(httpPath);
  }
}

const $core.bool _omitFieldNames =
    $core.bool.fromEnvironment('protobuf.omit_field_names');
const $core.bool _omitMessageNames =
    $core.bool.fromEnvironment('protobuf.omit_message_names');
