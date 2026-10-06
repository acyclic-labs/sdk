// This is a generated file - do not edit.
//
// Generated from actors/v1/actors.proto.

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

import 'actors.pb.dart' as $0;

export 'actors.pb.dart';

@$pb.GrpcServiceName('acyclic.actors.v1.ActorsService')
class ActorsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  ActorsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.CreateActorResponse> createActor(
    $0.CreateActorRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$createActor, request, options: options);
  }

  $grpc.ResponseFuture<$0.UpdateActorResponse> updateActor(
    $0.UpdateActorRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$updateActor, request, options: options);
  }

  $grpc.ResponseFuture<$0.InspectActorResponse> inspectActor(
    $0.InspectActorRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspectActor, request, options: options);
  }

  $grpc.ResponseFuture<$0.AddSubscriptionResponse> addSubscription(
    $0.AddSubscriptionRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$addSubscription, request, options: options);
  }

  $grpc.ResponseFuture<$0.RemoveSubscriptionResponse> removeSubscription(
    $0.RemoveSubscriptionRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$removeSubscription, request, options: options);
  }

  $grpc.ResponseFuture<$0.ResumeSubscriptionResponse> resumeSubscription(
    $0.ResumeSubscriptionRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$resumeSubscription, request, options: options);
  }

  $grpc.ResponseFuture<$0.CheckpointActorResponse> checkpointActor(
    $0.CheckpointActorRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$checkpointActor, request, options: options);
  }

  $grpc.ResponseFuture<$0.InvokeActorResponse> invokeActor(
    $0.InvokeActorRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$invokeActor, request, options: options);
  }

  // method descriptors

  static final _$createActor =
      $grpc.ClientMethod<$0.CreateActorRequest, $0.CreateActorResponse>(
          '/acyclic.actors.v1.ActorsService/CreateActor',
          ($0.CreateActorRequest value) => value.writeToBuffer(),
          $0.CreateActorResponse.fromBuffer);
  static final _$updateActor =
      $grpc.ClientMethod<$0.UpdateActorRequest, $0.UpdateActorResponse>(
          '/acyclic.actors.v1.ActorsService/UpdateActor',
          ($0.UpdateActorRequest value) => value.writeToBuffer(),
          $0.UpdateActorResponse.fromBuffer);
  static final _$inspectActor =
      $grpc.ClientMethod<$0.InspectActorRequest, $0.InspectActorResponse>(
          '/acyclic.actors.v1.ActorsService/InspectActor',
          ($0.InspectActorRequest value) => value.writeToBuffer(),
          $0.InspectActorResponse.fromBuffer);
  static final _$addSubscription =
      $grpc.ClientMethod<$0.AddSubscriptionRequest, $0.AddSubscriptionResponse>(
          '/acyclic.actors.v1.ActorsService/AddSubscription',
          ($0.AddSubscriptionRequest value) => value.writeToBuffer(),
          $0.AddSubscriptionResponse.fromBuffer);
  static final _$removeSubscription = $grpc.ClientMethod<
          $0.RemoveSubscriptionRequest, $0.RemoveSubscriptionResponse>(
      '/acyclic.actors.v1.ActorsService/RemoveSubscription',
      ($0.RemoveSubscriptionRequest value) => value.writeToBuffer(),
      $0.RemoveSubscriptionResponse.fromBuffer);
  static final _$resumeSubscription = $grpc.ClientMethod<
          $0.ResumeSubscriptionRequest, $0.ResumeSubscriptionResponse>(
      '/acyclic.actors.v1.ActorsService/ResumeSubscription',
      ($0.ResumeSubscriptionRequest value) => value.writeToBuffer(),
      $0.ResumeSubscriptionResponse.fromBuffer);
  static final _$checkpointActor =
      $grpc.ClientMethod<$0.CheckpointActorRequest, $0.CheckpointActorResponse>(
          '/acyclic.actors.v1.ActorsService/CheckpointActor',
          ($0.CheckpointActorRequest value) => value.writeToBuffer(),
          $0.CheckpointActorResponse.fromBuffer);
  static final _$invokeActor =
      $grpc.ClientMethod<$0.InvokeActorRequest, $0.InvokeActorResponse>(
          '/acyclic.actors.v1.ActorsService/InvokeActor',
          ($0.InvokeActorRequest value) => value.writeToBuffer(),
          $0.InvokeActorResponse.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.actors.v1.ActorsService')
abstract class ActorsServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.actors.v1.ActorsService';

  ActorsServiceBase() {
    $addMethod(
        $grpc.ServiceMethod<$0.CreateActorRequest, $0.CreateActorResponse>(
            'CreateActor',
            createActor_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.CreateActorRequest.fromBuffer(value),
            ($0.CreateActorResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.UpdateActorRequest, $0.UpdateActorResponse>(
            'UpdateActor',
            updateActor_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.UpdateActorRequest.fromBuffer(value),
            ($0.UpdateActorResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.InspectActorRequest, $0.InspectActorResponse>(
            'InspectActor',
            inspectActor_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.InspectActorRequest.fromBuffer(value),
            ($0.InspectActorResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.AddSubscriptionRequest,
            $0.AddSubscriptionResponse>(
        'AddSubscription',
        addSubscription_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.AddSubscriptionRequest.fromBuffer(value),
        ($0.AddSubscriptionResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.RemoveSubscriptionRequest,
            $0.RemoveSubscriptionResponse>(
        'RemoveSubscription',
        removeSubscription_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.RemoveSubscriptionRequest.fromBuffer(value),
        ($0.RemoveSubscriptionResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ResumeSubscriptionRequest,
            $0.ResumeSubscriptionResponse>(
        'ResumeSubscription',
        resumeSubscription_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.ResumeSubscriptionRequest.fromBuffer(value),
        ($0.ResumeSubscriptionResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.CheckpointActorRequest,
            $0.CheckpointActorResponse>(
        'CheckpointActor',
        checkpointActor_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.CheckpointActorRequest.fromBuffer(value),
        ($0.CheckpointActorResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.InvokeActorRequest, $0.InvokeActorResponse>(
            'InvokeActor',
            invokeActor_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.InvokeActorRequest.fromBuffer(value),
            ($0.InvokeActorResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.CreateActorResponse> createActor_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CreateActorRequest> $request) async {
    return createActor($call, await $request);
  }

  $async.Future<$0.CreateActorResponse> createActor(
      $grpc.ServiceCall call, $0.CreateActorRequest request);

  $async.Future<$0.UpdateActorResponse> updateActor_Pre($grpc.ServiceCall $call,
      $async.Future<$0.UpdateActorRequest> $request) async {
    return updateActor($call, await $request);
  }

  $async.Future<$0.UpdateActorResponse> updateActor(
      $grpc.ServiceCall call, $0.UpdateActorRequest request);

  $async.Future<$0.InspectActorResponse> inspectActor_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.InspectActorRequest> $request) async {
    return inspectActor($call, await $request);
  }

  $async.Future<$0.InspectActorResponse> inspectActor(
      $grpc.ServiceCall call, $0.InspectActorRequest request);

  $async.Future<$0.AddSubscriptionResponse> addSubscription_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.AddSubscriptionRequest> $request) async {
    return addSubscription($call, await $request);
  }

  $async.Future<$0.AddSubscriptionResponse> addSubscription(
      $grpc.ServiceCall call, $0.AddSubscriptionRequest request);

  $async.Future<$0.RemoveSubscriptionResponse> removeSubscription_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.RemoveSubscriptionRequest> $request) async {
    return removeSubscription($call, await $request);
  }

  $async.Future<$0.RemoveSubscriptionResponse> removeSubscription(
      $grpc.ServiceCall call, $0.RemoveSubscriptionRequest request);

  $async.Future<$0.ResumeSubscriptionResponse> resumeSubscription_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.ResumeSubscriptionRequest> $request) async {
    return resumeSubscription($call, await $request);
  }

  $async.Future<$0.ResumeSubscriptionResponse> resumeSubscription(
      $grpc.ServiceCall call, $0.ResumeSubscriptionRequest request);

  $async.Future<$0.CheckpointActorResponse> checkpointActor_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.CheckpointActorRequest> $request) async {
    return checkpointActor($call, await $request);
  }

  $async.Future<$0.CheckpointActorResponse> checkpointActor(
      $grpc.ServiceCall call, $0.CheckpointActorRequest request);

  $async.Future<$0.InvokeActorResponse> invokeActor_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InvokeActorRequest> $request) async {
    return invokeActor($call, await $request);
  }

  $async.Future<$0.InvokeActorResponse> invokeActor(
      $grpc.ServiceCall call, $0.InvokeActorRequest request);
}
