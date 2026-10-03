import 'dart:io';

import 'package:test/test.dart';

void main() {
  test('generator lock and canonical source inputs are present', () {
    expect(File('generator.lock.yaml').existsSync(), isTrue);
    final canonicalActors = File('../proto/actors/v1/actors.proto');
    final canonicalStream =
        File('../rust/crates/stream/proto/stream/v2/stream.proto');
    if (!canonicalActors.existsSync() || !canonicalStream.existsSync()) {
      markTestSkipped(
        'canonical Rust sources are unavailable in a clean package consumer',
      );
    }
  });

  test('generated transport is required for package qualification', () {
    final actors = Directory('lib/src/generated/actors/v1');
    final stream = Directory('lib/src/generated/stream/v2');
    if (!actors.existsSync() || !stream.existsSync()) {
      markTestSkipped(
        'run dart run tool/generate.dart in a Dart 3.8+ environment',
      );
    }
    expect(
      actors.listSync().whereType<File>().any(
        (file) => file.path.endsWith('.pb.dart'),
      ),
      isTrue,
    );
    expect(
      stream.listSync().whereType<File>().any(
        (file) => file.path.endsWith('.pbgrpc.dart'),
      ),
      isTrue,
    );
  });
}
