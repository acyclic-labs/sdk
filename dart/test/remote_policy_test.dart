import 'package:acyclic_sdk/src/remote_policy.dart';
import 'package:test/test.dart';

void main() {
  test('native defaults to gRPC and invokes once', () async {
    final calls = <List<Object?>>[];
    final client = RemoteClient(
      family: 'stream',
      invoker: (operation, request, transport) {
        calls.add([operation, request, transport]);
        return 'ok';
      },
    );
    expect(client.transport, RemoteTransport.grpc);
    expect(await client.call('append', {'path': 'events'}), 'ok');
    expect(calls, [
      ['append', {'path': 'events'}, RemoteTransport.grpc],
    ]);
  });

  test('streaming HTTP override is supported but unary-only override is rejected', () {
    final client = RemoteClient(
      family: 'stream',
      streaming: true,
      transport: RemoteTransport.httpJson,
      invoker: (_, __, ___) => null,
    );
    expect(client.transport, RemoteTransport.httpJson);
    expect(
      () => RemoteClient(
        family: 'actors',
        streaming: true,
        transport: RemoteTransport.httpJson,
        invoker: (_, __, ___) => null,
      ),
      throwsArgumentError,
    );
  });

  test('bearer policy rejects empty and header injection', () {
    expect(RemotePolicy.validateBearer(' token '), ' token ');
    expect(() => RemotePolicy.validateBearer('  '), throwsArgumentError);
    expect(() => RemotePolicy.validateBearer('token\r\nInjected: yes'), throwsArgumentError);
  });
}
