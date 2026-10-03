import 'dart:async';

// PROTOTYPE ONLY: handwritten adapter pending Rust generator emission. The
// generated Rust policy must replace this table before package qualification.

// PROTOTYPE ONLY: handwritten adapter pending Rust generator emission. The
// generated Rust policy must replace this table before package qualification.

enum RemoteTransport { grpc, grpcWeb, httpJson }

enum ClientRuntime { native, browser }

class RemotePolicy {
  static const Map<ClientRuntime, Map<String, List<(RemoteTransport, bool)>>> options = {
    ClientRuntime.native: {
      'actors': [(RemoteTransport.grpc, false), (RemoteTransport.httpJson, false)],
      'workers': [(RemoteTransport.grpc, false), (RemoteTransport.httpJson, false)],
      'objects': [(RemoteTransport.grpc, true), (RemoteTransport.httpJson, true)],
      'stream': [(RemoteTransport.grpc, true), (RemoteTransport.httpJson, true)],
      'inference': [(RemoteTransport.grpc, true)],
      'machines': [(RemoteTransport.grpc, true)],
      'filesystem': [(RemoteTransport.grpc, true)],
      'harness': [(RemoteTransport.grpc, true)],
    },
    ClientRuntime.browser: {
      'actors': [(RemoteTransport.httpJson, false)],
      'workers': [(RemoteTransport.httpJson, false)],
      'objects': [(RemoteTransport.httpJson, true)],
      'stream': [(RemoteTransport.httpJson, true)],
      'inference': [(RemoteTransport.httpJson, true)],
      'machines': [],
      'filesystem': [(RemoteTransport.grpcWeb, true)],
      'harness': [],
    },
  };

  static RemoteTransport select({
    required String family,
    ClientRuntime runtime = ClientRuntime.native,
    bool streaming = false,
    RemoteTransport? override,
  }) {
    final familyOptions = options[runtime]?[family];
    if (familyOptions == null) {
      throw ArgumentError('unknown transport family or runtime: $family/$runtime');
    }
    final compatible = familyOptions.where((option) => !streaming || option.$2).toList();
    if (override != null) {
      if (compatible.any((option) => option.$1 == override)) return override;
      throw ArgumentError('unsupported transport override for $family/$runtime');
    }
    if (compatible.isEmpty) throw ArgumentError('no compatible transport for $family/$runtime');
    return compatible.first.$1;
  }

  static String validateBearer(String token) {
    if (token.trim().isEmpty || token.contains(RegExp(r'[\r\n]'))) {
      throw ArgumentError('invalid bearer credential');
    }
    return token;
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
