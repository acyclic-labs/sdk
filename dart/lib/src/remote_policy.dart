import 'dart:async';

import 'generated_remote_policy.dart' as generated;

enum RemoteTransport { grpc, grpcWeb, httpJson }

enum ClientRuntime { native, browser }

class RemotePolicy {
  static Map<ClientRuntime, Map<String, List<(RemoteTransport, bool)>>> get options => {
        ClientRuntime.native: _options(generated.GeneratedClientRuntime.native),
        ClientRuntime.browser: _options(generated.GeneratedClientRuntime.browser),
      };

  static Map<String, List<(RemoteTransport, bool)>> _options(
    generated.GeneratedClientRuntime runtime,
  ) => generated.generatedRemotePolicyOptions[runtime]!.map(
        (family, values) => MapEntry(
          family,
          values.map((value) => (_transport(value.$1), value.$2)).toList(),
        ),
      );

  static RemoteTransport _transport(generated.GeneratedRemoteTransport value) => switch (value) {
        generated.GeneratedRemoteTransport.grpc => RemoteTransport.grpc,
        generated.GeneratedRemoteTransport.grpcWeb => RemoteTransport.grpcWeb,
        generated.GeneratedRemoteTransport.httpJson => RemoteTransport.httpJson,
      };

  static generated.GeneratedRemoteTransport _generatedTransport(RemoteTransport value) => switch (value) {
        RemoteTransport.grpc => generated.GeneratedRemoteTransport.grpc,
        RemoteTransport.grpcWeb => generated.GeneratedRemoteTransport.grpcWeb,
        RemoteTransport.httpJson => generated.GeneratedRemoteTransport.httpJson,
      };

  static RemoteTransport select({
    required String family,
    ClientRuntime runtime = ClientRuntime.native,
    bool streaming = false,
    RemoteTransport? override,
  }) {
    final selected = generated.GeneratedRemotePolicy.select(
      family: family,
      runtime: runtime == ClientRuntime.native
          ? generated.GeneratedClientRuntime.native
          : generated.GeneratedClientRuntime.browser,
      streaming: streaming,
      transportOverride: override == null ? null : _generatedTransport(override),
    );
    return _transport(selected);
  }

  static String validateBearer(String token) {
    return generated.GeneratedRemotePolicy.validateBearer(token);
  }
}

typedef RemoteInvoker = FutureOr<Object?> Function(
  String operation,
  Object? request,
  RemoteTransport transport,
);

class RemoteClient {
  RemoteClient({
    required this.family,
    required RemoteInvoker invoker,
    ClientRuntime runtime = ClientRuntime.native,
    bool streaming = false,
    RemoteTransport? transport,
    String? bearer,
  })  : _invoker = invoker,
        runtime = runtime,
        transport = RemotePolicy.select(
          family: family,
          runtime: runtime,
          streaming: streaming,
          override: transport,
        ) {
    if (bearer != null) RemotePolicy.validateBearer(bearer);
  }

  final String family;
  final ClientRuntime runtime;
  final RemoteTransport transport;
  final RemoteInvoker _invoker;

  FutureOr<Object?> call(String operation, Object? request) =>
      _invoker(operation, request, transport);
}
