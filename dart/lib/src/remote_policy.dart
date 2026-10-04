import 'dart:async';

import 'generated_remote_policy.dart' as generated;
import 'type_policy.dart';

enum RemoteTransport { grpc, grpcWeb, httpJson }

enum ClientRuntime { auto, native, browser }

const _isBrowserRuntime = bool.fromEnvironment('dart.library.js_interop') ||
    bool.fromEnvironment('dart.library.html');

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
    ClientRuntime runtime = ClientRuntime.auto,
    bool streaming = false,
    bool bearerAuth = true,
    Map<RemoteTransport, bool>? installed,
    Object? endpoint,
    RemoteTransport? override,
  }) {
    final selected = generated.GeneratedRemotePolicy.select(
      family: family,
      runtime: resolveRuntime(runtime) == ClientRuntime.native
          ? generated.GeneratedClientRuntime.native
          : generated.GeneratedClientRuntime.browser,
      streaming: streaming,
      bearerAuth: bearerAuth,
      installed: _generatedAvailability(installed ?? installedAvailability(resolveRuntime(runtime))),
      endpoint: _generatedAvailability(_normalizeEndpoint(endpoint)),
      transportOverride: override == null ? null : _generatedTransport(override),
    );
    return _transport(selected);
  }

  static Map<generated.GeneratedRemoteTransport, bool>? _generatedAvailability(
    Map<RemoteTransport, bool>? value,
  ) => value?.map((key, available) => MapEntry(_generatedTransport(key), available));

  static Map<RemoteTransport, bool> installedAvailability(ClientRuntime runtime) =>
      runtime == ClientRuntime.browser
          ? const {
              RemoteTransport.grpc: false,
              RemoteTransport.grpcWeb: true,
              RemoteTransport.httpJson: true,
            }
          : const {
              RemoteTransport.grpc: true,
              RemoteTransport.grpcWeb: false,
              RemoteTransport.httpJson: true,
            };

  static Map<RemoteTransport, bool> get endpointAvailability {
    const configured = String.fromEnvironment('ACYCLIC_ENDPOINT_TRANSPORTS');
    if (configured.trim().isEmpty) {
      return const {
        RemoteTransport.grpc: true,
        RemoteTransport.grpcWeb: true,
        RemoteTransport.httpJson: true,
      };
    }
    final kinds = configured.split(',').map((kind) => kind.trim().toLowerCase()).toSet();
    return {
      RemoteTransport.grpc: kinds.contains('grpc'),
      RemoteTransport.grpcWeb: kinds.contains('grpc_web'),
      RemoteTransport.httpJson: kinds.contains('http_json'),
    };
  }

  static Map<RemoteTransport, bool> _normalizeEndpoint(Object? endpoint) {
    if (endpoint is Map<RemoteTransport, bool>) return endpoint;
    if (endpoint is String) {
      final scheme = endpoint.split(':').first.toLowerCase();
      if (scheme == 'grpc') {
        return const {
          RemoteTransport.grpc: true,
          RemoteTransport.grpcWeb: false,
          RemoteTransport.httpJson: false,
        };
      }
      if (scheme == 'grpc-web') {
        return const {
          RemoteTransport.grpc: false,
          RemoteTransport.grpcWeb: true,
          RemoteTransport.httpJson: false,
        };
      }
      if (scheme == 'http' || scheme == 'https') {
        return const {
          RemoteTransport.grpc: false,
          RemoteTransport.grpcWeb: false,
          RemoteTransport.httpJson: true,
        };
      }
    }
    return endpointAvailability;
  }

  static ClientRuntime resolveRuntime([ClientRuntime runtime = ClientRuntime.auto]) {
    if (runtime != ClientRuntime.auto) return runtime;
    return _isBrowserRuntime ? ClientRuntime.browser : ClientRuntime.native;
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
    ClientRuntime runtime = ClientRuntime.auto,
    bool streaming = false,
    RemoteTransport? transport,
    String? bearer,
    bool bearerAuth = true,
    Map<RemoteTransport, bool>? installed,
    Object? endpoint,
  })  : _invoker = invoker,
        runtime = RemotePolicy.resolveRuntime(runtime),
        transport = RemotePolicy.select(
          family: family,
          runtime: runtime,
          streaming: streaming,
          bearerAuth: bearerAuth,
          installed: installed,
          endpoint: endpoint,
          override: transport,
        ) {
    if (bearer != null) RemotePolicy.validateBearer(bearer);
  }

  final String family;
  final ClientRuntime runtime;
  final RemoteTransport transport;
  final RemoteInvoker _invoker;

  FutureOr<Object?> call(String operation, Object? request) =>
      _invoker(operation, TypePolicyWire.normalizeRequest(family, request), transport);

  Object? typedField(Object? response, String field) {
    if (response is! Map || response[field] == null) return response;
    return TypePolicyWire.typedField(family: family, field: field, value: response[field]!);
  }
}
