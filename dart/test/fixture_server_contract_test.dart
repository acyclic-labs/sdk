import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:acyclic_sdk/src/generated/stream/v2/stream.pb.dart' as stream;
import 'package:acyclic_sdk/src/generated/stream/v2/stream.pbgrpc.dart' as stream_rpc;
import 'package:fixnum/fixnum.dart';
import 'package:grpc/grpc.dart';
import 'package:test/test.dart';

/// Exercises the Rust-owned loopback fixture when FIXTURE_SERVER_URL points at
/// `cargo run --bin fixture-server`. The fixture intentionally speaks HTTP
/// with protobuf request bodies and JSON receipts; it is not a hosted-service
/// or gRPC availability claim.
void main() {
  final base = Platform.environment['FIXTURE_SERVER_URL'];

  test('append and bounded read use Rust fixture protobuf bytes', () async {
    if (base == null || base.isEmpty) {
      markTestSkipped('set FIXTURE_SERVER_URL to the Rust fixture server');
      return;
    }

    final append = stream.AppendRequest()
      ..path = 'dart/fixture'
      ..records.addAll([
        utf8.encode('hello'),
        utf8.encode('world'),
      ])
      ..ifTail = Int64.ZERO
      ..idempotencyKey = utf8.encode('dart-fixture-append');
    final appendReceipt = await _post(base, '/v1/stream/append', append.writeToBuffer());
    expect(appendReceipt['status'], 'committed');
    expect(appendReceipt['start'], 0);
    expect(appendReceipt['end'], 2);

    final read = stream.ReadRequest()
      ..path = 'dart/fixture'
      ..from = Int64.ZERO
      ..limit = 2;
    final readReceipt = await _post(base, '/v1/stream/read', read.writeToBuffer());
    expect(readReceipt['status'], 'ok');
    expect(readReceipt['records'], [
      {'sequence': 0, 'value_hex': '68656c6c6f'},
      {'sequence': 1, 'value_hex': '776f726c64'},
    ]);
  });

  test('Dart response subscription can be cancelled against fixture', () async {
    if (base == null || base.isEmpty) {
      markTestSkipped('set FIXTURE_SERVER_URL to the Rust fixture server');
      return;
    }

    final request = stream.ReadRequest()
      ..path = 'dart/cancellation'
      ..from = Int64.ZERO
      ..limit = 1;
    final client = HttpClient();
    final uri = Uri.parse('$base/v1/stream/read');
    final outgoing = await client.postUrl(uri);
    final bytes = request.writeToBuffer();
    outgoing.headers.contentType = ContentType('application', 'protobuf');
    outgoing.contentLength = bytes.length;
    outgoing.add(bytes);
    final response = await outgoing.close();
    final done = Completer<void>();
    late StreamSubscription<List<int>> subscription;
    subscription = response.listen((_) async {
      await subscription.cancel();
      if (!done.isCompleted) done.complete();
    }, onError: done.completeError, onDone: () {
      if (!done.isCompleted) done.complete();
    });
    await done.future.timeout(const Duration(seconds: 5));
    client.close(force: true);
  });

  test('generated gRPC stream cancellation is wired to ClientCall', () async {
    final address = Platform.environment['FIXTURE_GRPC_ADDRESS'];
    if (address == null || address.isEmpty) {
      markTestSkipped('set FIXTURE_GRPC_ADDRESS to a Rust gRPC fixture endpoint');
      return;
    }
    final parts = address.split(':');
    if (parts.length != 2) {
      throw FormatException('FIXTURE_GRPC_ADDRESS must be host:port');
    }
    final channel = ClientChannel(
      parts[0],
      port: int.parse(parts[1]),
      options: const ChannelOptions(credentials: ChannelCredentials.insecure()),
    );
    final rpc = stream_rpc.StreamServiceClient(channel);
    final request = stream.ReadRequest()
      ..path = 'dart/cancellation'
      ..from = Int64.ZERO
      ..limit = 1024;
    final subscription = rpc.read(request).listen((_) {});
    await subscription.cancel();
    await channel.shutdown();
  });
}

Future<Map<String, dynamic>> _post(String base, String path, List<int> body) async {
  final client = HttpClient();
  try {
    final request = await client.postUrl(Uri.parse('$base$path'));
    request.headers.contentType = ContentType('application', 'protobuf');
    request.contentLength = body.length;
    request.add(body);
    final response = await request.close();
    final text = await utf8.decoder.bind(response).join();
    if (response.statusCode != HttpStatus.ok) {
      throw StateError('fixture returned HTTP ${response.statusCode}: $text');
    }
    return jsonDecode(text) as Map<String, dynamic>;
  } finally {
    client.close(force: true);
  }
}
