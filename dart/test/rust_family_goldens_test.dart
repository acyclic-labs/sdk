import 'dart:convert';
import 'dart:io';

import 'package:acyclic_sdk/src/generated/actors/v1/actors.pb.dart' as actors;
import 'package:acyclic_sdk/src/generated/filesystem/v2/filesystem.pb.dart'
    as filesystem;
import 'package:acyclic_sdk/src/generated/harness/v2/harness.pb.dart' as harness;
import 'package:acyclic_sdk/src/generated/inference/v1/inference.pb.dart'
    as inference;
import 'package:acyclic_sdk/src/generated/machines/v1/machines.pb.dart'
    as machines;
import 'package:acyclic_sdk/src/generated/objects/v2/objects.pb.dart' as objects;
import 'package:acyclic_sdk/src/generated/protocol/v1/protocol.pb.dart'
    as protocol;
import 'package:acyclic_sdk/src/generated/stream/v2/stream.pb.dart' as stream;
import 'package:acyclic_sdk/src/generated/workers/v1/workers.pb.dart' as workers;
import 'package:protobuf/protobuf.dart';
import 'package:test/test.dart';

void main() {
  test('all nine Rust family goldens round trip exactly', () {
    final path = Platform.environment['RUST_FAMILY_GOLDENS'] ??
        '../../../php/tests/fixtures/rust-family-goldens.json';
    final fixtures = (jsonDecode(File(path).readAsStringSync()) as List)
        .cast<Map<String, dynamic>>();
    expect(fixtures, hasLength(9));
    expect(fixtures.map((fixture) => fixture['family']).toSet(), hasLength(9));

    final factories = <String, GeneratedMessage Function(List<int>)>{
      'actors': actors.ActorLimits.fromBuffer,
      'stream': stream.Record.fromBuffer,
      'objects': objects.ObjectInfo.fromBuffer,
      'workers': workers.CodeVersion.fromBuffer,
      'filesystem': filesystem.WorkspaceContextSnapshot.fromBuffer,
      'harness': harness.OperationStatus.fromBuffer,
      'inference': inference.ModelCapability.fromBuffer,
      'machines': machines.SuspensionPolicy.fromBuffer,
      'protocol': protocol.ProtocolIdentity.fromBuffer,
    };

    for (final fixture in fixtures) {
      final family = fixture['family'] as String;
      final message = factories[family]!(_decodeHex(fixture['wire_hex'] as String));
      _assertGolden(message, fixture);
    }
  });
}

void _assertGolden(GeneratedMessage message, Map<String, dynamic> fixture) {
  final wire = _decodeHex(fixture['wire_hex'] as String);
  expect(message.writeToBuffer(), wire, reason: fixture['family'] as String);
  expect(jsonEncode(message.toProto3Json()), fixture['json'],
      reason: fixture['family'] as String);
}

List<int> _decodeHex(String value) => [
      for (var index = 0; index < value.length; index += 2)
        int.parse(value.substring(index, index + 2), radix: 16),
    ];
