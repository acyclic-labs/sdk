// This is a generated file - do not edit.
//
// Generated from workers/v1/workers.proto.

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

import 'workers.pb.dart' as $0;

export 'workers.pb.dart';

@$pb.GrpcServiceName('acyclic.workers.v1.WorkersService')
class WorkersServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  WorkersServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.PublishVersionResponse> publishVersion(
    $0.PublishVersionRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$publishVersion, request, options: options);
  }

  $grpc.ResponseFuture<$0.SelectDeploymentResponse> selectDeployment(
    $0.SelectDeploymentRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$selectDeployment, request, options: options);
  }

  $grpc.ResponseFuture<$0.SubmitJobResponse> submitJob(
    $0.SubmitJobRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$submitJob, request, options: options);
  }

  $grpc.ResponseFuture<$0.InspectJobResponse> inspectJob(
    $0.InspectJobRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspectJob, request, options: options);
  }

  $grpc.ResponseFuture<$0.CancelJobResponse> cancelJob(
    $0.CancelJobRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$cancelJob, request, options: options);
  }

  $grpc.ResponseFuture<$0.InvokeResponse> invokeVersion(
    $0.InvokeVersionRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$invokeVersion, request, options: options);
  }

  $grpc.ResponseFuture<$0.InvokeResponse> invokeDeployment(
    $0.InvokeDeploymentRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$invokeDeployment, request, options: options);
  }

  // method descriptors

  static final _$publishVersion =
      $grpc.ClientMethod<$0.PublishVersionRequest, $0.PublishVersionResponse>(
          '/acyclic.workers.v1.WorkersService/PublishVersion',
          ($0.PublishVersionRequest value) => value.writeToBuffer(),
          $0.PublishVersionResponse.fromBuffer);
  static final _$selectDeployment = $grpc.ClientMethod<
          $0.SelectDeploymentRequest, $0.SelectDeploymentResponse>(
      '/acyclic.workers.v1.WorkersService/SelectDeployment',
      ($0.SelectDeploymentRequest value) => value.writeToBuffer(),
      $0.SelectDeploymentResponse.fromBuffer);
  static final _$submitJob =
      $grpc.ClientMethod<$0.SubmitJobRequest, $0.SubmitJobResponse>(
          '/acyclic.workers.v1.WorkersService/SubmitJob',
          ($0.SubmitJobRequest value) => value.writeToBuffer(),
          $0.SubmitJobResponse.fromBuffer);
  static final _$inspectJob =
      $grpc.ClientMethod<$0.InspectJobRequest, $0.InspectJobResponse>(
          '/acyclic.workers.v1.WorkersService/InspectJob',
          ($0.InspectJobRequest value) => value.writeToBuffer(),
          $0.InspectJobResponse.fromBuffer);
  static final _$cancelJob =
      $grpc.ClientMethod<$0.CancelJobRequest, $0.CancelJobResponse>(
          '/acyclic.workers.v1.WorkersService/CancelJob',
          ($0.CancelJobRequest value) => value.writeToBuffer(),
          $0.CancelJobResponse.fromBuffer);
  static final _$invokeVersion =
      $grpc.ClientMethod<$0.InvokeVersionRequest, $0.InvokeResponse>(
          '/acyclic.workers.v1.WorkersService/InvokeVersion',
          ($0.InvokeVersionRequest value) => value.writeToBuffer(),
          $0.InvokeResponse.fromBuffer);
  static final _$invokeDeployment =
      $grpc.ClientMethod<$0.InvokeDeploymentRequest, $0.InvokeResponse>(
          '/acyclic.workers.v1.WorkersService/InvokeDeployment',
          ($0.InvokeDeploymentRequest value) => value.writeToBuffer(),
          $0.InvokeResponse.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.workers.v1.WorkersService')
abstract class WorkersServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.workers.v1.WorkersService';

  WorkersServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.PublishVersionRequest,
            $0.PublishVersionResponse>(
        'PublishVersion',
        publishVersion_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.PublishVersionRequest.fromBuffer(value),
        ($0.PublishVersionResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.SelectDeploymentRequest,
            $0.SelectDeploymentResponse>(
        'SelectDeployment',
        selectDeployment_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.SelectDeploymentRequest.fromBuffer(value),
        ($0.SelectDeploymentResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.SubmitJobRequest, $0.SubmitJobResponse>(
        'SubmitJob',
        submitJob_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.SubmitJobRequest.fromBuffer(value),
        ($0.SubmitJobResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.InspectJobRequest, $0.InspectJobResponse>(
        'InspectJob',
        inspectJob_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.InspectJobRequest.fromBuffer(value),
        ($0.InspectJobResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.CancelJobRequest, $0.CancelJobResponse>(
        'CancelJob',
        cancelJob_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.CancelJobRequest.fromBuffer(value),
        ($0.CancelJobResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.InvokeVersionRequest, $0.InvokeResponse>(
        'InvokeVersion',
        invokeVersion_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.InvokeVersionRequest.fromBuffer(value),
        ($0.InvokeResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.InvokeDeploymentRequest, $0.InvokeResponse>(
            'InvokeDeployment',
            invokeDeployment_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.InvokeDeploymentRequest.fromBuffer(value),
            ($0.InvokeResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.PublishVersionResponse> publishVersion_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.PublishVersionRequest> $request) async {
    return publishVersion($call, await $request);
  }

  $async.Future<$0.PublishVersionResponse> publishVersion(
      $grpc.ServiceCall call, $0.PublishVersionRequest request);

  $async.Future<$0.SelectDeploymentResponse> selectDeployment_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.SelectDeploymentRequest> $request) async {
    return selectDeployment($call, await $request);
  }

  $async.Future<$0.SelectDeploymentResponse> selectDeployment(
      $grpc.ServiceCall call, $0.SelectDeploymentRequest request);

  $async.Future<$0.SubmitJobResponse> submitJob_Pre($grpc.ServiceCall $call,
      $async.Future<$0.SubmitJobRequest> $request) async {
    return submitJob($call, await $request);
  }

  $async.Future<$0.SubmitJobResponse> submitJob(
      $grpc.ServiceCall call, $0.SubmitJobRequest request);

  $async.Future<$0.InspectJobResponse> inspectJob_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InspectJobRequest> $request) async {
    return inspectJob($call, await $request);
  }

  $async.Future<$0.InspectJobResponse> inspectJob(
      $grpc.ServiceCall call, $0.InspectJobRequest request);

  $async.Future<$0.CancelJobResponse> cancelJob_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CancelJobRequest> $request) async {
    return cancelJob($call, await $request);
  }

  $async.Future<$0.CancelJobResponse> cancelJob(
      $grpc.ServiceCall call, $0.CancelJobRequest request);

  $async.Future<$0.InvokeResponse> invokeVersion_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InvokeVersionRequest> $request) async {
    return invokeVersion($call, await $request);
  }

  $async.Future<$0.InvokeResponse> invokeVersion(
      $grpc.ServiceCall call, $0.InvokeVersionRequest request);

  $async.Future<$0.InvokeResponse> invokeDeployment_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InvokeDeploymentRequest> $request) async {
    return invokeDeployment($call, await $request);
  }

  $async.Future<$0.InvokeResponse> invokeDeployment(
      $grpc.ServiceCall call, $0.InvokeDeploymentRequest request);
}
