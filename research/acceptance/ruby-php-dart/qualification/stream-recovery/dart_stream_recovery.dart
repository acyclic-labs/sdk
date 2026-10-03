import 'dart:async';
import 'dart:io';
import 'package:acyclic_sdk/src/generated/stream/v2/stream.pb.dart' as stream;
import 'package:acyclic_sdk/src/generated/stream/v2/stream.pbgrpc.dart' as rpc;
import 'package:fixnum/fixnum.dart';
import 'package:grpc/grpc.dart';

Future<void> main() async {
  final endpoint = Platform.environment['FIXTURE_GRPC_ADDRESS']!;
  final parts = endpoint.split(':');
  final path = Platform.environment['RECOVERY_PATH']!;
  final id = Platform.environment['RECOVERY_ID']!;
  final channel = ClientChannel(parts[0], port: int.parse(parts[1]), options: const ChannelOptions(credentials: ChannelCredentials.insecure()));
  try {
    final client = rpc.StreamServiceClient(channel);
    final appended = await client.append(stream.AppendRequest()
      ..path = path
      ..records.addAll(['dart-recovery-0'.codeUnits, 'dart-recovery-1'.codeUnits])
      ..ifTail = Int64.ZERO
      ..idempotencyKey = ('dart-recovery-append-' + id).codeUnits);
    if (appended.committed.end != Int64(2)) throw StateError('append end mismatch: ${appended.committed.end}');
    final items = <List<Object>>[];
    await for (final item in client.read(stream.ReadRequest()..path = path..from = Int64.ZERO..limit = 2)) {
      items.add([item.record.sequence.toInt(), String.fromCharCodes(item.record.value)]);
    }
    if (items.toString() != '[[0, dart-recovery-0], [1, dart-recovery-1]]') throw StateError('page mismatch: $items');
    final resumed = <List<Object>>[];
    await for (final item in client.read(stream.ReadRequest()..path = path..from = Int64(1)..limit = 1)) {
      resumed.add([item.record.sequence.toInt(), String.fromCharCodes(item.record.value)]);
    }
    if (resumed.toString() != '[[1, dart-recovery-1]]') throw StateError('resume mismatch: $resumed');
    final first = Completer<int>();
    late StreamSubscription<stream.ReadResponse> subscription;
    subscription = client.follow(stream.FollowRequest()..path = path..from = Int64.ZERO).listen((item) async {
      if (!first.isCompleted) {
        first.complete(item.record.sequence.toInt());
        await subscription.cancel();
      }
    }, onError: (Object error, StackTrace stack) {
      if (!first.isCompleted) first.completeError(error, stack);
    });
    final firstSequence = await first.future.timeout(const Duration(seconds: 5));
    if (firstSequence != 0) throw StateError('cancel first mismatch: $firstSequence');
    await subscription.cancel();
    print('append_end=${appended.committed.end}');
    print('page=${items.map((item) => item[0]).join(',')}');
    print('resume=${resumed.map((item) => item[0]).join(',')}');
    print('cancel_first=$firstSequence');
    print('cancelled=subscription.cancel');
  } finally {
    await channel.shutdown();
  }
}
