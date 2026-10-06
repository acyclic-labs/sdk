import 'package:acyclic_sdk/src/generated/actors/v1/actors.pb.dart' as actors;
import 'package:acyclic_sdk/src/generated/stream/v2/stream.pb.dart' as stream;
import 'package:acyclic_sdk/src/generated/stream/v2/stream.pbgrpc.dart'
    as stream_rpc;
import 'package:acyclic_sdk/src/remote_policy.dart';
import 'package:fixnum/fixnum.dart';
import 'package:grpc/grpc.dart';
import 'package:test/test.dart';

void main() {
  test('uint64 and bytes round trip through generated messages', () {
    final limits =
        actors.ActorLimits()
          ..memoryBytes = Int64.parseInt('9223372036854775807');
    expect(limits.memoryBytes.toString(), '9223372036854775807');

    final observation = actors.ActorObservation()..codeSha256 = [0, 255];
    expect(observation.codeSha256, [0, 255]);
  });

  test('optional presence and oneof branches are retained', () {
    final request = stream.AppendRequest();
    expect(request.hasIfTail(), isFalse);
    request.ifTail = Int64.parseInt('9007199254740992');
    expect(request.hasIfTail(), isTrue);
    expect(request.ifTail.toString(), '9007199254740992');

    final response =
        stream.AppendResponse()
          ..committed = (stream.AppendReceipt()..tail = Int64(3));
    expect(response.whichOutcome(), stream.AppendResponse_Outcome.committed);
  });

  test('server-streaming methods are generated as Dart streams', () async {
    final channel = ClientChannel(
      'localhost',
      port: 1,
      options: const ChannelOptions(credentials: ChannelCredentials.insecure()),
    );
    final client = stream_rpc.StreamServiceClient(channel);
    expect(client.read, isNotNull);
    expect(client.follow, isNotNull);
    expect(client.children, isNotNull);
    await channel.shutdown();
  });

  test('public client round trips unknown enum and oneof wire values', () async {
    final enumWire = <int>[0x0A, 0x02, 0x20, 0x63];
    final enumResponse = actors.InspectActorResponse.fromBuffer(enumWire);
    final enumClient = RemoteClient(
      family: 'actors',
      installed: const {RemoteTransport.grpc: false, RemoteTransport.httpJson: true},
      endpoint: const {RemoteTransport.grpc: false, RemoteTransport.httpJson: true},
      invoker: (_, __, ___) => enumResponse,
    );
    final returnedEnum = await enumClient.call('InspectActor', {'actor_id': 'actor-1'})
        as actors.InspectActorResponse;
    expect(returnedEnum.writeToBuffer(), enumWire);

    final oneofWire = <int>[0x0A, 0x00, 0x9A, 0x06, 0x01, 0x7F];
    final oneofResponse = stream.AppendResponse.fromBuffer(oneofWire);
    final oneofClient = RemoteClient(
      family: 'stream',
      installed: const {RemoteTransport.grpc: false, RemoteTransport.httpJson: true},
      endpoint: const {RemoteTransport.grpc: false, RemoteTransport.httpJson: true},
      invoker: (_, __, ___) => oneofResponse,
    );
    final returnedOneof = await oneofClient.call('Append', {'path': 'events'})
        as stream.AppendResponse;
    expect(returnedOneof.writeToBuffer(), oneofWire);
  });
}
