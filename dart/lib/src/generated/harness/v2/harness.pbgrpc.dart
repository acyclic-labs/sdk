// This is a generated file - do not edit.
//
// Generated from harness/v2/harness.proto.

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

import '../../protocol/v1/protocol.pb.dart' as $0;
import 'harness.pb.dart' as $1;

export 'harness.pb.dart';

@$pb.GrpcServiceName('acyclic.harness.v2.HarnessService')
class HarnessServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  HarnessServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.HandshakeResponse> handshake(
    $0.HandshakeRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$handshake, request, options: options);
  }

  $grpc.ResponseFuture<$1.Admission> submit(
    $1.CommandEnvelope request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$submit, request, options: options);
  }

  $grpc.ResponseStream<$1.Delivery> replay(
    $1.ResumeRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$replay, $async.Stream.fromIterable([request]),
        options: options);
  }

  $grpc.ResponseFuture<$1.OperationStatus> observe(
    $1.ObserveRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$observe, request, options: options);
  }

  $grpc.ResponseFuture<$1.CancelResponse> cancel(
    $1.CancelRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$cancel, request, options: options);
  }

  // method descriptors

  static final _$handshake =
      $grpc.ClientMethod<$0.HandshakeRequest, $0.HandshakeResponse>(
          '/acyclic.harness.v2.HarnessService/Handshake',
          ($0.HandshakeRequest value) => value.writeToBuffer(),
          $0.HandshakeResponse.fromBuffer);
  static final _$submit = $grpc.ClientMethod<$1.CommandEnvelope, $1.Admission>(
      '/acyclic.harness.v2.HarnessService/Submit',
      ($1.CommandEnvelope value) => value.writeToBuffer(),
      $1.Admission.fromBuffer);
  static final _$replay = $grpc.ClientMethod<$1.ResumeRequest, $1.Delivery>(
      '/acyclic.harness.v2.HarnessService/Replay',
      ($1.ResumeRequest value) => value.writeToBuffer(),
      $1.Delivery.fromBuffer);
  static final _$observe =
      $grpc.ClientMethod<$1.ObserveRequest, $1.OperationStatus>(
          '/acyclic.harness.v2.HarnessService/Observe',
          ($1.ObserveRequest value) => value.writeToBuffer(),
          $1.OperationStatus.fromBuffer);
  static final _$cancel =
      $grpc.ClientMethod<$1.CancelRequest, $1.CancelResponse>(
          '/acyclic.harness.v2.HarnessService/Cancel',
          ($1.CancelRequest value) => value.writeToBuffer(),
          $1.CancelResponse.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.harness.v2.HarnessService')
abstract class HarnessServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.harness.v2.HarnessService';

  HarnessServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.HandshakeRequest, $0.HandshakeResponse>(
        'Handshake',
        handshake_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.HandshakeRequest.fromBuffer(value),
        ($0.HandshakeResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$1.CommandEnvelope, $1.Admission>(
        'Submit',
        submit_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $1.CommandEnvelope.fromBuffer(value),
        ($1.Admission value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$1.ResumeRequest, $1.Delivery>(
        'Replay',
        replay_Pre,
        false,
        true,
        ($core.List<$core.int> value) => $1.ResumeRequest.fromBuffer(value),
        ($1.Delivery value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$1.ObserveRequest, $1.OperationStatus>(
        'Observe',
        observe_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $1.ObserveRequest.fromBuffer(value),
        ($1.OperationStatus value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$1.CancelRequest, $1.CancelResponse>(
        'Cancel',
        cancel_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $1.CancelRequest.fromBuffer(value),
        ($1.CancelResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.HandshakeResponse> handshake_Pre($grpc.ServiceCall $call,
      $async.Future<$0.HandshakeRequest> $request) async {
    return handshake($call, await $request);
  }

  $async.Future<$0.HandshakeResponse> handshake(
      $grpc.ServiceCall call, $0.HandshakeRequest request);

  $async.Future<$1.Admission> submit_Pre($grpc.ServiceCall $call,
      $async.Future<$1.CommandEnvelope> $request) async {
    return submit($call, await $request);
  }

  $async.Future<$1.Admission> submit(
      $grpc.ServiceCall call, $1.CommandEnvelope request);

  $async.Stream<$1.Delivery> replay_Pre($grpc.ServiceCall $call,
      $async.Future<$1.ResumeRequest> $request) async* {
    yield* replay($call, await $request);
  }

  $async.Stream<$1.Delivery> replay(
      $grpc.ServiceCall call, $1.ResumeRequest request);

  $async.Future<$1.OperationStatus> observe_Pre($grpc.ServiceCall $call,
      $async.Future<$1.ObserveRequest> $request) async {
    return observe($call, await $request);
  }

  $async.Future<$1.OperationStatus> observe(
      $grpc.ServiceCall call, $1.ObserveRequest request);

  $async.Future<$1.CancelResponse> cancel_Pre(
      $grpc.ServiceCall $call, $async.Future<$1.CancelRequest> $request) async {
    return cancel($call, await $request);
  }

  $async.Future<$1.CancelResponse> cancel(
      $grpc.ServiceCall call, $1.CancelRequest request);
}
