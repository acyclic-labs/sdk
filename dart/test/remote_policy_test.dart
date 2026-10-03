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
    expect(client.runtime, ClientRuntime.native);
    expect(await client.call('append', {'path': 'events'}), 'ok');
    expect(calls, [
      ['append', {'path': 'events'}, RemoteTransport.grpc],
    ]);
  });

  test('automatic runtime resolver selects the current embedded platform', () {
    expect(RemotePolicy.resolveRuntime(), ClientRuntime.native);
    expect(RemotePolicy.resolveRuntime(ClientRuntime.browser), ClientRuntime.browser);
    expect(
      RemotePolicy.select(
        family: 'stream',
        runtime: ClientRuntime.browser,
        streaming: true,
      ),
      RemoteTransport.httpJson,
    );
  });

  test('streaming HTTP override is supported but unary-only override is rejected', () {
    final client = RemoteClient(
      family: 'stream',
      streaming: true,
      transport: RemoteTransport.httpJson,
      invoker: (_, __, ___) => null,
    );
    expect(client.transport, RemoteTransport.httpJson);

    final https = RemoteClient(
      family: 'actors',
      endpoint: 'https://api.example',
      bearer: ' token ',
      invoker: (_, __, ___) => null,
    );
    expect(https.transport, RemoteTransport.httpJson);
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

  test('installed and endpoint capabilities are forwarded to generated policy', () {
    final client = RemoteClient(
      family: 'actors',
      installed: {RemoteTransport.grpc: false, RemoteTransport.httpJson: true},
      endpoint: {RemoteTransport.grpc: false, RemoteTransport.httpJson: true},
      invoker: (_, __, ___) => null,
    );
    expect(client.transport, RemoteTransport.httpJson);

    expect(
      () => RemoteClient(
        family: 'actors',
        endpoint: {RemoteTransport.grpc: false, RemoteTransport.httpJson: false},
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
