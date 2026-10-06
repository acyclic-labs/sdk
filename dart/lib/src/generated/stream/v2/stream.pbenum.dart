// This is a generated file - do not edit.
//
// Generated from stream/v2/stream.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

/// Canonical bounds shared by Rust and generated TypeScript clients.
class StreamLimit extends $pb.ProtobufEnum {
  static const StreamLimit STREAM_LIMIT_UNSPECIFIED =
      StreamLimit._(0, _omitEnumNames ? '' : 'STREAM_LIMIT_UNSPECIFIED');
  static const StreamLimit STREAM_LIMIT_MAX_RECORD_BYTES = StreamLimit._(
      65536, _omitEnumNames ? '' : 'STREAM_LIMIT_MAX_RECORD_BYTES');
  static const StreamLimit STREAM_LIMIT_MAX_ITEMS =
      StreamLimit._(1024, _omitEnumNames ? '' : 'STREAM_LIMIT_MAX_ITEMS');
  static const StreamLimit STREAM_LIMIT_MAX_COMMAND_BYTES = StreamLimit._(
      1056768, _omitEnumNames ? '' : 'STREAM_LIMIT_MAX_COMMAND_BYTES');
  static const StreamLimit STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES =
      StreamLimit._(
          256, _omitEnumNames ? '' : 'STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES');
  static const StreamLimit STREAM_LIMIT_MAX_PATH_BYTES =
      StreamLimit._(65535, _omitEnumNames ? '' : 'STREAM_LIMIT_MAX_PATH_BYTES');

  static const $core.List<StreamLimit> values = <StreamLimit>[
    STREAM_LIMIT_UNSPECIFIED,
    STREAM_LIMIT_MAX_RECORD_BYTES,
    STREAM_LIMIT_MAX_ITEMS,
    STREAM_LIMIT_MAX_COMMAND_BYTES,
    STREAM_LIMIT_MAX_IDEMPOTENCY_KEY_BYTES,
    STREAM_LIMIT_MAX_PATH_BYTES,
  ];

  static final $core.Map<$core.int, StreamLimit> _byValue =
      $pb.ProtobufEnum.initByValue(values);
  static StreamLimit? valueOf($core.int value) => _byValue[value];

  const StreamLimit._(super.value, super.name);
}

const $core.bool _omitEnumNames =
    $core.bool.fromEnvironment('protobuf.omit_enum_names');
