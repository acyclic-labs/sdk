// This is a generated file - do not edit.
//
// Generated from stream/v2/stream.proto.

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

import 'stream.pb.dart' as $0;

export 'stream.pb.dart';

@$pb.GrpcServiceName('acyclic.stream.v2.StreamService')
class StreamServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  StreamServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.InspectIdempotencyResponse> inspectIdempotency(
    $0.InspectIdempotencyRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspectIdempotency, request, options: options);
  }

  $grpc.ResponseFuture<$0.AppendResponse> append(
    $0.AppendRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$append, request, options: options);
  }

  $grpc.ResponseFuture<$0.TailResponse> tail(
    $0.TailRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$tail, request, options: options);
  }

  $grpc.ResponseFuture<$0.ForkReceipt> fork(
    $0.ForkRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$fork, request, options: options);
  }

  $grpc.ResponseStream<$0.ReadResponse> read(
    $0.ReadRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$read, $async.Stream.fromIterable([request]),
        options: options);
  }

  $grpc.ResponseStream<$0.ReadResponse> follow(
    $0.FollowRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$follow, $async.Stream.fromIterable([request]),
        options: options);
  }

  $grpc.ResponseStream<$0.ChildrenResponse> children(
    $0.ChildrenRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(
        _$children, $async.Stream.fromIterable([request]),
        options: options);
  }

  $grpc.ResponseFuture<$0.ChildrenPageResponse> childrenPage(
    $0.ChildrenPageRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$childrenPage, request, options: options);
  }

  $grpc.ResponseFuture<$0.CommitResponse> commit(
    $0.CommitRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$commit, request, options: options);
  }

  $grpc.ResponseFuture<$0.CommittedEnvelope> readCommit(
    $0.ReadCommitRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$readCommit, request, options: options);
  }

  // method descriptors

  static final _$inspectIdempotency = $grpc.ClientMethod<
          $0.InspectIdempotencyRequest, $0.InspectIdempotencyResponse>(
      '/acyclic.stream.v2.StreamService/InspectIdempotency',
      ($0.InspectIdempotencyRequest value) => value.writeToBuffer(),
      $0.InspectIdempotencyResponse.fromBuffer);
  static final _$append =
      $grpc.ClientMethod<$0.AppendRequest, $0.AppendResponse>(
          '/acyclic.stream.v2.StreamService/Append',
          ($0.AppendRequest value) => value.writeToBuffer(),
          $0.AppendResponse.fromBuffer);
  static final _$tail = $grpc.ClientMethod<$0.TailRequest, $0.TailResponse>(
      '/acyclic.stream.v2.StreamService/Tail',
      ($0.TailRequest value) => value.writeToBuffer(),
      $0.TailResponse.fromBuffer);
  static final _$fork = $grpc.ClientMethod<$0.ForkRequest, $0.ForkReceipt>(
      '/acyclic.stream.v2.StreamService/Fork',
      ($0.ForkRequest value) => value.writeToBuffer(),
      $0.ForkReceipt.fromBuffer);
  static final _$read = $grpc.ClientMethod<$0.ReadRequest, $0.ReadResponse>(
      '/acyclic.stream.v2.StreamService/Read',
      ($0.ReadRequest value) => value.writeToBuffer(),
      $0.ReadResponse.fromBuffer);
  static final _$follow = $grpc.ClientMethod<$0.FollowRequest, $0.ReadResponse>(
      '/acyclic.stream.v2.StreamService/Follow',
      ($0.FollowRequest value) => value.writeToBuffer(),
      $0.ReadResponse.fromBuffer);
  static final _$children =
      $grpc.ClientMethod<$0.ChildrenRequest, $0.ChildrenResponse>(
          '/acyclic.stream.v2.StreamService/Children',
          ($0.ChildrenRequest value) => value.writeToBuffer(),
          $0.ChildrenResponse.fromBuffer);
  static final _$childrenPage =
      $grpc.ClientMethod<$0.ChildrenPageRequest, $0.ChildrenPageResponse>(
          '/acyclic.stream.v2.StreamService/ChildrenPage',
          ($0.ChildrenPageRequest value) => value.writeToBuffer(),
          $0.ChildrenPageResponse.fromBuffer);
  static final _$commit =
      $grpc.ClientMethod<$0.CommitRequest, $0.CommitResponse>(
          '/acyclic.stream.v2.StreamService/Commit',
          ($0.CommitRequest value) => value.writeToBuffer(),
          $0.CommitResponse.fromBuffer);
  static final _$readCommit =
      $grpc.ClientMethod<$0.ReadCommitRequest, $0.CommittedEnvelope>(
          '/acyclic.stream.v2.StreamService/ReadCommit',
          ($0.ReadCommitRequest value) => value.writeToBuffer(),
          $0.CommittedEnvelope.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.stream.v2.StreamService')
abstract class StreamServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.stream.v2.StreamService';

  StreamServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.InspectIdempotencyRequest,
            $0.InspectIdempotencyResponse>(
        'InspectIdempotency',
        inspectIdempotency_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.InspectIdempotencyRequest.fromBuffer(value),
        ($0.InspectIdempotencyResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.AppendRequest, $0.AppendResponse>(
        'Append',
        append_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.AppendRequest.fromBuffer(value),
        ($0.AppendResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.TailRequest, $0.TailResponse>(
        'Tail',
        tail_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.TailRequest.fromBuffer(value),
        ($0.TailResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ForkRequest, $0.ForkReceipt>(
        'Fork',
        fork_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ForkRequest.fromBuffer(value),
        ($0.ForkReceipt value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ReadRequest, $0.ReadResponse>(
        'Read',
        read_Pre,
        false,
        true,
        ($core.List<$core.int> value) => $0.ReadRequest.fromBuffer(value),
        ($0.ReadResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.FollowRequest, $0.ReadResponse>(
        'Follow',
        follow_Pre,
        false,
        true,
        ($core.List<$core.int> value) => $0.FollowRequest.fromBuffer(value),
        ($0.ReadResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ChildrenRequest, $0.ChildrenResponse>(
        'Children',
        children_Pre,
        false,
        true,
        ($core.List<$core.int> value) => $0.ChildrenRequest.fromBuffer(value),
        ($0.ChildrenResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.ChildrenPageRequest, $0.ChildrenPageResponse>(
            'ChildrenPage',
            childrenPage_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.ChildrenPageRequest.fromBuffer(value),
            ($0.ChildrenPageResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.CommitRequest, $0.CommitResponse>(
        'Commit',
        commit_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.CommitRequest.fromBuffer(value),
        ($0.CommitResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ReadCommitRequest, $0.CommittedEnvelope>(
        'ReadCommit',
        readCommit_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ReadCommitRequest.fromBuffer(value),
        ($0.CommittedEnvelope value) => value.writeToBuffer()));
  }

  $async.Future<$0.InspectIdempotencyResponse> inspectIdempotency_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.InspectIdempotencyRequest> $request) async {
    return inspectIdempotency($call, await $request);
  }

  $async.Future<$0.InspectIdempotencyResponse> inspectIdempotency(
      $grpc.ServiceCall call, $0.InspectIdempotencyRequest request);

  $async.Future<$0.AppendResponse> append_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.AppendRequest> $request) async {
    return append($call, await $request);
  }

  $async.Future<$0.AppendResponse> append(
      $grpc.ServiceCall call, $0.AppendRequest request);

  $async.Future<$0.TailResponse> tail_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.TailRequest> $request) async {
    return tail($call, await $request);
  }

  $async.Future<$0.TailResponse> tail(
      $grpc.ServiceCall call, $0.TailRequest request);

  $async.Future<$0.ForkReceipt> fork_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.ForkRequest> $request) async {
    return fork($call, await $request);
  }

  $async.Future<$0.ForkReceipt> fork(
      $grpc.ServiceCall call, $0.ForkRequest request);

  $async.Stream<$0.ReadResponse> read_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.ReadRequest> $request) async* {
    yield* read($call, await $request);
  }

  $async.Stream<$0.ReadResponse> read(
      $grpc.ServiceCall call, $0.ReadRequest request);

  $async.Stream<$0.ReadResponse> follow_Pre($grpc.ServiceCall $call,
      $async.Future<$0.FollowRequest> $request) async* {
    yield* follow($call, await $request);
  }

  $async.Stream<$0.ReadResponse> follow(
      $grpc.ServiceCall call, $0.FollowRequest request);

  $async.Stream<$0.ChildrenResponse> children_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ChildrenRequest> $request) async* {
    yield* children($call, await $request);
  }

  $async.Stream<$0.ChildrenResponse> children(
      $grpc.ServiceCall call, $0.ChildrenRequest request);

  $async.Future<$0.ChildrenPageResponse> childrenPage_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.ChildrenPageRequest> $request) async {
    return childrenPage($call, await $request);
  }

  $async.Future<$0.ChildrenPageResponse> childrenPage(
      $grpc.ServiceCall call, $0.ChildrenPageRequest request);

  $async.Future<$0.CommitResponse> commit_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.CommitRequest> $request) async {
    return commit($call, await $request);
  }

  $async.Future<$0.CommitResponse> commit(
      $grpc.ServiceCall call, $0.CommitRequest request);

  $async.Future<$0.CommittedEnvelope> readCommit_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ReadCommitRequest> $request) async {
    return readCommit($call, await $request);
  }

  $async.Future<$0.CommittedEnvelope> readCommit(
      $grpc.ServiceCall call, $0.ReadCommitRequest request);
}
