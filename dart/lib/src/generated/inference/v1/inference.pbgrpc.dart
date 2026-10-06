// This is a generated file - do not edit.
//
// Generated from inference/v1/inference.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:async' as $async;
import 'dart:core' as $core;

import 'package:grpc/service_api.dart' as $grpc;
import 'package:protobuf/protobuf.dart' as $pb;

import 'inference.pb.dart' as $0;

export 'inference.pb.dart';

@$pb.GrpcServiceName('inference.customer.v1.ModelsService')
class ModelsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  ModelsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.ListModelsResponse> list(
    $0.ListModelsRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$list, request, options: options);
  }

  // method descriptors

  static final _$list =
      $grpc.ClientMethod<$0.ListModelsRequest, $0.ListModelsResponse>(
          '/inference.customer.v1.ModelsService/List',
          ($0.ListModelsRequest value) => value.writeToBuffer(),
          $0.ListModelsResponse.fromBuffer);
}

@$pb.GrpcServiceName('inference.customer.v1.ModelsService')
abstract class ModelsServiceBase extends $grpc.Service {
  $core.String get $name => 'inference.customer.v1.ModelsService';

  ModelsServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.ListModelsRequest, $0.ListModelsResponse>(
        'List',
        list_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ListModelsRequest.fromBuffer(value),
        ($0.ListModelsResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.ListModelsResponse> list_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ListModelsRequest> $request) async {
    return list($call, await $request);
  }

  $async.Future<$0.ListModelsResponse> list(
      $grpc.ServiceCall call, $0.ListModelsRequest request);
}

/// Immutable canonical content operations. Execution/retention guarantees are
/// advertised separately; these methods do not admit a Run or a warm promise.
@$pb.GrpcServiceName('inference.customer.v1.ContextsService')
class ContextsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  ContextsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.MutationReceipt> create(
    $0.CreateContextRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$create, request, options: options);
  }

  $grpc.ResponseFuture<$0.ContextView> inspect(
    $0.InspectContextRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspect, request, options: options);
  }

  $grpc.ResponseFuture<$0.MutationReceipt> mutate(
    $0.MutateContextRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$mutate, request, options: options);
  }

  // method descriptors

  static final _$create =
      $grpc.ClientMethod<$0.CreateContextRequest, $0.MutationReceipt>(
          '/inference.customer.v1.ContextsService/Create',
          ($0.CreateContextRequest value) => value.writeToBuffer(),
          $0.MutationReceipt.fromBuffer);
  static final _$inspect =
      $grpc.ClientMethod<$0.InspectContextRequest, $0.ContextView>(
          '/inference.customer.v1.ContextsService/Inspect',
          ($0.InspectContextRequest value) => value.writeToBuffer(),
          $0.ContextView.fromBuffer);
  static final _$mutate =
      $grpc.ClientMethod<$0.MutateContextRequest, $0.MutationReceipt>(
          '/inference.customer.v1.ContextsService/Mutate',
          ($0.MutateContextRequest value) => value.writeToBuffer(),
          $0.MutationReceipt.fromBuffer);
}

@$pb.GrpcServiceName('inference.customer.v1.ContextsService')
abstract class ContextsServiceBase extends $grpc.Service {
  $core.String get $name => 'inference.customer.v1.ContextsService';

  ContextsServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.CreateContextRequest, $0.MutationReceipt>(
        'Create',
        create_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.CreateContextRequest.fromBuffer(value),
        ($0.MutationReceipt value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.InspectContextRequest, $0.ContextView>(
        'Inspect',
        inspect_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.InspectContextRequest.fromBuffer(value),
        ($0.ContextView value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.MutateContextRequest, $0.MutationReceipt>(
        'Mutate',
        mutate_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.MutateContextRequest.fromBuffer(value),
        ($0.MutationReceipt value) => value.writeToBuffer()));
  }

  $async.Future<$0.MutationReceipt> create_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CreateContextRequest> $request) async {
    return create($call, await $request);
  }

  $async.Future<$0.MutationReceipt> create(
      $grpc.ServiceCall call, $0.CreateContextRequest request);

  $async.Future<$0.ContextView> inspect_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InspectContextRequest> $request) async {
    return inspect($call, await $request);
  }

  $async.Future<$0.ContextView> inspect(
      $grpc.ServiceCall call, $0.InspectContextRequest request);

  $async.Future<$0.MutationReceipt> mutate_Pre($grpc.ServiceCall $call,
      $async.Future<$0.MutateContextRequest> $request) async {
    return mutate($call, await $request);
  }

  $async.Future<$0.MutationReceipt> mutate(
      $grpc.ServiceCall call, $0.MutateContextRequest request);
}

/// Customer warm-retention commitments. Placement, workers, allocations,
/// migration, rebalancing, and cleanup mechanics remain private.
@$pb.GrpcServiceName('inference.customer.v1.WarmContextsService')
class WarmContextsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  WarmContextsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.WarmView> retain(
    $0.RetainWarmRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$retain, request, options: options);
  }

  $grpc.ResponseFuture<$0.WarmView> inspect(
    $0.InspectWarmRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspect, request, options: options);
  }

  $grpc.ResponseFuture<$0.WarmView> renew(
    $0.RenewWarmRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$renew, request, options: options);
  }

  $grpc.ResponseFuture<$0.WarmView> release(
    $0.ReleaseWarmRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$release, request, options: options);
  }

  // method descriptors

  static final _$retain = $grpc.ClientMethod<$0.RetainWarmRequest, $0.WarmView>(
      '/inference.customer.v1.WarmContextsService/Retain',
      ($0.RetainWarmRequest value) => value.writeToBuffer(),
      $0.WarmView.fromBuffer);
  static final _$inspect =
      $grpc.ClientMethod<$0.InspectWarmRequest, $0.WarmView>(
          '/inference.customer.v1.WarmContextsService/Inspect',
          ($0.InspectWarmRequest value) => value.writeToBuffer(),
          $0.WarmView.fromBuffer);
  static final _$renew = $grpc.ClientMethod<$0.RenewWarmRequest, $0.WarmView>(
      '/inference.customer.v1.WarmContextsService/Renew',
      ($0.RenewWarmRequest value) => value.writeToBuffer(),
      $0.WarmView.fromBuffer);
  static final _$release =
      $grpc.ClientMethod<$0.ReleaseWarmRequest, $0.WarmView>(
          '/inference.customer.v1.WarmContextsService/Release',
          ($0.ReleaseWarmRequest value) => value.writeToBuffer(),
          $0.WarmView.fromBuffer);
}

@$pb.GrpcServiceName('inference.customer.v1.WarmContextsService')
abstract class WarmContextsServiceBase extends $grpc.Service {
  $core.String get $name => 'inference.customer.v1.WarmContextsService';

  WarmContextsServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.RetainWarmRequest, $0.WarmView>(
        'Retain',
        retain_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.RetainWarmRequest.fromBuffer(value),
        ($0.WarmView value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.InspectWarmRequest, $0.WarmView>(
        'Inspect',
        inspect_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.InspectWarmRequest.fromBuffer(value),
        ($0.WarmView value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.RenewWarmRequest, $0.WarmView>(
        'Renew',
        renew_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.RenewWarmRequest.fromBuffer(value),
        ($0.WarmView value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ReleaseWarmRequest, $0.WarmView>(
        'Release',
        release_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.ReleaseWarmRequest.fromBuffer(value),
        ($0.WarmView value) => value.writeToBuffer()));
  }

  $async.Future<$0.WarmView> retain_Pre($grpc.ServiceCall $call,
      $async.Future<$0.RetainWarmRequest> $request) async {
    return retain($call, await $request);
  }

  $async.Future<$0.WarmView> retain(
      $grpc.ServiceCall call, $0.RetainWarmRequest request);

  $async.Future<$0.WarmView> inspect_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InspectWarmRequest> $request) async {
    return inspect($call, await $request);
  }

  $async.Future<$0.WarmView> inspect(
      $grpc.ServiceCall call, $0.InspectWarmRequest request);

  $async.Future<$0.WarmView> renew_Pre($grpc.ServiceCall $call,
      $async.Future<$0.RenewWarmRequest> $request) async {
    return renew($call, await $request);
  }

  $async.Future<$0.WarmView> renew(
      $grpc.ServiceCall call, $0.RenewWarmRequest request);

  $async.Future<$0.WarmView> release_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ReleaseWarmRequest> $request) async {
    return release($call, await $request);
  }

  $async.Future<$0.WarmView> release(
      $grpc.ServiceCall call, $0.ReleaseWarmRequest request);
}

/// Recoverable logical generation. Distribution, placement, migration, cache,
/// and worker identities are intentionally absent.
@$pb.GrpcServiceName('inference.customer.v1.RunsService')
class RunsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  RunsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.GenerateRunResponse> generate(
    $0.GenerateRunRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$generate, request, options: options);
  }

  $grpc.ResponseFuture<$0.RunView> inspect(
    $0.InspectRunRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspect, request, options: options);
  }

  $grpc.ResponseStream<$0.RunEvent> watch(
    $0.WatchRunRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$watch, $async.Stream.fromIterable([request]),
        options: options);
  }

  $grpc.ResponseFuture<$0.RunView> cancel(
    $0.InspectRunRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$cancel, request, options: options);
  }

  // method descriptors

  static final _$generate =
      $grpc.ClientMethod<$0.GenerateRunRequest, $0.GenerateRunResponse>(
          '/inference.customer.v1.RunsService/Generate',
          ($0.GenerateRunRequest value) => value.writeToBuffer(),
          $0.GenerateRunResponse.fromBuffer);
  static final _$inspect = $grpc.ClientMethod<$0.InspectRunRequest, $0.RunView>(
      '/inference.customer.v1.RunsService/Inspect',
      ($0.InspectRunRequest value) => value.writeToBuffer(),
      $0.RunView.fromBuffer);
  static final _$watch = $grpc.ClientMethod<$0.WatchRunRequest, $0.RunEvent>(
      '/inference.customer.v1.RunsService/Watch',
      ($0.WatchRunRequest value) => value.writeToBuffer(),
      $0.RunEvent.fromBuffer);
  static final _$cancel = $grpc.ClientMethod<$0.InspectRunRequest, $0.RunView>(
      '/inference.customer.v1.RunsService/Cancel',
      ($0.InspectRunRequest value) => value.writeToBuffer(),
      $0.RunView.fromBuffer);
}

@$pb.GrpcServiceName('inference.customer.v1.RunsService')
abstract class RunsServiceBase extends $grpc.Service {
  $core.String get $name => 'inference.customer.v1.RunsService';

  RunsServiceBase() {
    $addMethod(
        $grpc.ServiceMethod<$0.GenerateRunRequest, $0.GenerateRunResponse>(
            'Generate',
            generate_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.GenerateRunRequest.fromBuffer(value),
            ($0.GenerateRunResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.InspectRunRequest, $0.RunView>(
        'Inspect',
        inspect_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.InspectRunRequest.fromBuffer(value),
        ($0.RunView value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.WatchRunRequest, $0.RunEvent>(
        'Watch',
        watch_Pre,
        false,
        true,
        ($core.List<$core.int> value) => $0.WatchRunRequest.fromBuffer(value),
        ($0.RunEvent value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.InspectRunRequest, $0.RunView>(
        'Cancel',
        cancel_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.InspectRunRequest.fromBuffer(value),
        ($0.RunView value) => value.writeToBuffer()));
  }

  $async.Future<$0.GenerateRunResponse> generate_Pre($grpc.ServiceCall $call,
      $async.Future<$0.GenerateRunRequest> $request) async {
    return generate($call, await $request);
  }

  $async.Future<$0.GenerateRunResponse> generate(
      $grpc.ServiceCall call, $0.GenerateRunRequest request);

  $async.Future<$0.RunView> inspect_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InspectRunRequest> $request) async {
    return inspect($call, await $request);
  }

  $async.Future<$0.RunView> inspect(
      $grpc.ServiceCall call, $0.InspectRunRequest request);

  $async.Stream<$0.RunEvent> watch_Pre($grpc.ServiceCall $call,
      $async.Future<$0.WatchRunRequest> $request) async* {
    yield* watch($call, await $request);
  }

  $async.Stream<$0.RunEvent> watch(
      $grpc.ServiceCall call, $0.WatchRunRequest request);

  $async.Future<$0.RunView> cancel_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InspectRunRequest> $request) async {
    return cancel($call, await $request);
  }

  $async.Future<$0.RunView> cancel(
      $grpc.ServiceCall call, $0.InspectRunRequest request);
}

/// Immutable, recoverable evaluation admissions. Candidate execution and grader
/// placement remain private; the customer contract contains only exact artifacts,
/// bounded suite inputs, metric semantics, and content-addressed observations.
@$pb.GrpcServiceName('inference.customer.v1.EvaluationsService')
class EvaluationsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  EvaluationsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.EvaluationView> create(
    $0.CreateEvaluationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$create, request, options: options);
  }

  $grpc.ResponseFuture<$0.EvaluationView> inspect(
    $0.InspectEvaluationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspect, request, options: options);
  }

  // method descriptors

  static final _$create =
      $grpc.ClientMethod<$0.CreateEvaluationRequest, $0.EvaluationView>(
          '/inference.customer.v1.EvaluationsService/Create',
          ($0.CreateEvaluationRequest value) => value.writeToBuffer(),
          $0.EvaluationView.fromBuffer);
  static final _$inspect =
      $grpc.ClientMethod<$0.InspectEvaluationRequest, $0.EvaluationView>(
          '/inference.customer.v1.EvaluationsService/Inspect',
          ($0.InspectEvaluationRequest value) => value.writeToBuffer(),
          $0.EvaluationView.fromBuffer);
}

@$pb.GrpcServiceName('inference.customer.v1.EvaluationsService')
abstract class EvaluationsServiceBase extends $grpc.Service {
  $core.String get $name => 'inference.customer.v1.EvaluationsService';

  EvaluationsServiceBase() {
    $addMethod(
        $grpc.ServiceMethod<$0.CreateEvaluationRequest, $0.EvaluationView>(
            'Create',
            create_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.CreateEvaluationRequest.fromBuffer(value),
            ($0.EvaluationView value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.InspectEvaluationRequest, $0.EvaluationView>(
            'Inspect',
            inspect_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.InspectEvaluationRequest.fromBuffer(value),
            ($0.EvaluationView value) => value.writeToBuffer()));
  }

  $async.Future<$0.EvaluationView> create_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CreateEvaluationRequest> $request) async {
    return create($call, await $request);
  }

  $async.Future<$0.EvaluationView> create(
      $grpc.ServiceCall call, $0.CreateEvaluationRequest request);

  $async.Future<$0.EvaluationView> inspect_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InspectEvaluationRequest> $request) async {
    return inspect($call, await $request);
  }

  $async.Future<$0.EvaluationView> inspect(
      $grpc.ServiceCall call, $0.InspectEvaluationRequest request);
}
