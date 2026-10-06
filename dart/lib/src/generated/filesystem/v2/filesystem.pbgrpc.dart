// This is a generated file - do not edit.
//
// Generated from filesystem/v2/filesystem.proto.

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

import 'filesystem.pb.dart' as $0;

export 'filesystem.pb.dart';

@$pb.GrpcServiceName('acyclic.filesystem.v2.FilesystemService')
class FilesystemServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  FilesystemServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.HandshakeResponse> handshake(
    $0.HandshakeRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$handshake, request, options: options);
  }

  $grpc.ResponseFuture<$0.WorkspaceResponse> createWorkspace(
    $0.CreateWorkspaceRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$createWorkspace, request, options: options);
  }

  $grpc.ResponseFuture<$0.WorkspaceResponse> openWorkspace(
    $0.OpenWorkspaceRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$openWorkspace, request, options: options);
  }

  $grpc.ResponseFuture<$0.MutationResponse> deleteWorkspace(
    $0.DeleteWorkspaceRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$deleteWorkspace, request, options: options);
  }

  $grpc.ResponseFuture<$0.GenerationResponse> getHead(
    $0.GetHeadRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$getHead, request, options: options);
  }

  $grpc.ResponseFuture<$0.GenerationResponse> getGeneration(
    $0.GetGenerationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$getGeneration, request, options: options);
  }

  $grpc.ResponseFuture<$0.ReadResponse> read(
    $0.ReadRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$read, request, options: options);
  }

  $grpc.ResponseFuture<$0.StatResponse> stat(
    $0.StatRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$stat, request, options: options);
  }

  $grpc.ResponseFuture<$0.ListDirectoryResponse> listDirectory(
    $0.ListDirectoryRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$listDirectory, request, options: options);
  }

  $grpc.ResponseFuture<$0.ReadResponse> readLink(
    $0.ReadLinkRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$readLink, request, options: options);
  }

  $grpc.ResponseFuture<$0.PlanExtentsResponse> planExtents(
    $0.PlanExtentsRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$planExtents, request, options: options);
  }

  $grpc.ResponseFuture<$0.MutationResponse> applyTransaction(
    $0.ApplyTransactionRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$applyTransaction, request, options: options);
  }

  $grpc.ResponseFuture<$0.RebaseTransactionResponse> rebaseTransaction(
    $0.RebaseTransactionRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$rebaseTransaction, request, options: options);
  }

  $grpc.ResponseFuture<$0.WorkspaceResponse> forkWorkspace(
    $0.ForkWorkspaceRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$forkWorkspace, request, options: options);
  }

  $grpc.ResponseFuture<$0.DiffResponse> diff(
    $0.DiffRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$diff, request, options: options);
  }

  $grpc.ResponseFuture<$0.RebaseResponse> rebase(
    $0.RebaseRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$rebase, request, options: options);
  }

  $grpc.ResponseFuture<$0.JoinPlan> planJoin(
    $0.PlanJoinRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$planJoin, request, options: options);
  }

  $grpc.ResponseFuture<$0.JoinResponse> applyJoin(
    $0.ApplyJoinRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$applyJoin, request, options: options);
  }

  $grpc.ResponseFuture<$0.RetainGenerationResponse> checkpoint(
    $0.RetainGenerationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$checkpoint, request, options: options);
  }

  $grpc.ResponseFuture<$0.RetainGenerationResponse> pin(
    $0.RetainGenerationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$pin, request, options: options);
  }

  $grpc.ResponseStream<$0.ExportChunk> export(
    $0.ExportRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$export, $async.Stream.fromIterable([request]),
        options: options);
  }

  $grpc.ResponseFuture<$0.ImportResponse> import(
    $async.Stream<$0.ImportChunk> request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$import, request, options: options).single;
  }

  $grpc.ResponseFuture<$0.CredentialResponse> issueMountCredential(
    $0.CredentialRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$issueMountCredential, request, options: options);
  }

  $grpc.ResponseFuture<$0.CredentialResponse> issueS3Credential(
    $0.CredentialRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$issueS3Credential, request, options: options);
  }

  $grpc.ResponseFuture<$0.SourceResponse> getSourceState(
    $0.SourceStateRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$getSourceState, request, options: options);
  }

  $grpc.ResponseFuture<$0.SourceResponse> reconcileSource(
    $0.SourceOperationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$reconcileSource, request, options: options);
  }

  $grpc.ResponseFuture<$0.SourceResponse> rescanSource(
    $0.SourceOperationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$rescanSource, request, options: options);
  }

  $grpc.ResponseFuture<$0.SourceResponse> sealSource(
    $0.SourceOperationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$sealSource, request, options: options);
  }

  $grpc.ResponseFuture<$0.ObserveResponse> observe(
    $0.ObserveRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$observe, request, options: options);
  }

  $grpc.ResponseFuture<$0.CancelResponse> cancel(
    $0.CancelRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$cancel, request, options: options);
  }

  // method descriptors

  static final _$handshake =
      $grpc.ClientMethod<$0.HandshakeRequest, $0.HandshakeResponse>(
          '/acyclic.filesystem.v2.FilesystemService/Handshake',
          ($0.HandshakeRequest value) => value.writeToBuffer(),
          $0.HandshakeResponse.fromBuffer);
  static final _$createWorkspace =
      $grpc.ClientMethod<$0.CreateWorkspaceRequest, $0.WorkspaceResponse>(
          '/acyclic.filesystem.v2.FilesystemService/CreateWorkspace',
          ($0.CreateWorkspaceRequest value) => value.writeToBuffer(),
          $0.WorkspaceResponse.fromBuffer);
  static final _$openWorkspace =
      $grpc.ClientMethod<$0.OpenWorkspaceRequest, $0.WorkspaceResponse>(
          '/acyclic.filesystem.v2.FilesystemService/OpenWorkspace',
          ($0.OpenWorkspaceRequest value) => value.writeToBuffer(),
          $0.WorkspaceResponse.fromBuffer);
  static final _$deleteWorkspace =
      $grpc.ClientMethod<$0.DeleteWorkspaceRequest, $0.MutationResponse>(
          '/acyclic.filesystem.v2.FilesystemService/DeleteWorkspace',
          ($0.DeleteWorkspaceRequest value) => value.writeToBuffer(),
          $0.MutationResponse.fromBuffer);
  static final _$getHead =
      $grpc.ClientMethod<$0.GetHeadRequest, $0.GenerationResponse>(
          '/acyclic.filesystem.v2.FilesystemService/GetHead',
          ($0.GetHeadRequest value) => value.writeToBuffer(),
          $0.GenerationResponse.fromBuffer);
  static final _$getGeneration =
      $grpc.ClientMethod<$0.GetGenerationRequest, $0.GenerationResponse>(
          '/acyclic.filesystem.v2.FilesystemService/GetGeneration',
          ($0.GetGenerationRequest value) => value.writeToBuffer(),
          $0.GenerationResponse.fromBuffer);
  static final _$read = $grpc.ClientMethod<$0.ReadRequest, $0.ReadResponse>(
      '/acyclic.filesystem.v2.FilesystemService/Read',
      ($0.ReadRequest value) => value.writeToBuffer(),
      $0.ReadResponse.fromBuffer);
  static final _$stat = $grpc.ClientMethod<$0.StatRequest, $0.StatResponse>(
      '/acyclic.filesystem.v2.FilesystemService/Stat',
      ($0.StatRequest value) => value.writeToBuffer(),
      $0.StatResponse.fromBuffer);
  static final _$listDirectory =
      $grpc.ClientMethod<$0.ListDirectoryRequest, $0.ListDirectoryResponse>(
          '/acyclic.filesystem.v2.FilesystemService/ListDirectory',
          ($0.ListDirectoryRequest value) => value.writeToBuffer(),
          $0.ListDirectoryResponse.fromBuffer);
  static final _$readLink =
      $grpc.ClientMethod<$0.ReadLinkRequest, $0.ReadResponse>(
          '/acyclic.filesystem.v2.FilesystemService/ReadLink',
          ($0.ReadLinkRequest value) => value.writeToBuffer(),
          $0.ReadResponse.fromBuffer);
  static final _$planExtents =
      $grpc.ClientMethod<$0.PlanExtentsRequest, $0.PlanExtentsResponse>(
          '/acyclic.filesystem.v2.FilesystemService/PlanExtents',
          ($0.PlanExtentsRequest value) => value.writeToBuffer(),
          $0.PlanExtentsResponse.fromBuffer);
  static final _$applyTransaction =
      $grpc.ClientMethod<$0.ApplyTransactionRequest, $0.MutationResponse>(
          '/acyclic.filesystem.v2.FilesystemService/ApplyTransaction',
          ($0.ApplyTransactionRequest value) => value.writeToBuffer(),
          $0.MutationResponse.fromBuffer);
  static final _$rebaseTransaction = $grpc.ClientMethod<
          $0.RebaseTransactionRequest, $0.RebaseTransactionResponse>(
      '/acyclic.filesystem.v2.FilesystemService/RebaseTransaction',
      ($0.RebaseTransactionRequest value) => value.writeToBuffer(),
      $0.RebaseTransactionResponse.fromBuffer);
  static final _$forkWorkspace =
      $grpc.ClientMethod<$0.ForkWorkspaceRequest, $0.WorkspaceResponse>(
          '/acyclic.filesystem.v2.FilesystemService/ForkWorkspace',
          ($0.ForkWorkspaceRequest value) => value.writeToBuffer(),
          $0.WorkspaceResponse.fromBuffer);
  static final _$diff = $grpc.ClientMethod<$0.DiffRequest, $0.DiffResponse>(
      '/acyclic.filesystem.v2.FilesystemService/Diff',
      ($0.DiffRequest value) => value.writeToBuffer(),
      $0.DiffResponse.fromBuffer);
  static final _$rebase =
      $grpc.ClientMethod<$0.RebaseRequest, $0.RebaseResponse>(
          '/acyclic.filesystem.v2.FilesystemService/Rebase',
          ($0.RebaseRequest value) => value.writeToBuffer(),
          $0.RebaseResponse.fromBuffer);
  static final _$planJoin = $grpc.ClientMethod<$0.PlanJoinRequest, $0.JoinPlan>(
      '/acyclic.filesystem.v2.FilesystemService/PlanJoin',
      ($0.PlanJoinRequest value) => value.writeToBuffer(),
      $0.JoinPlan.fromBuffer);
  static final _$applyJoin =
      $grpc.ClientMethod<$0.ApplyJoinRequest, $0.JoinResponse>(
          '/acyclic.filesystem.v2.FilesystemService/ApplyJoin',
          ($0.ApplyJoinRequest value) => value.writeToBuffer(),
          $0.JoinResponse.fromBuffer);
  static final _$checkpoint = $grpc.ClientMethod<$0.RetainGenerationRequest,
          $0.RetainGenerationResponse>(
      '/acyclic.filesystem.v2.FilesystemService/Checkpoint',
      ($0.RetainGenerationRequest value) => value.writeToBuffer(),
      $0.RetainGenerationResponse.fromBuffer);
  static final _$pin = $grpc.ClientMethod<$0.RetainGenerationRequest,
          $0.RetainGenerationResponse>(
      '/acyclic.filesystem.v2.FilesystemService/Pin',
      ($0.RetainGenerationRequest value) => value.writeToBuffer(),
      $0.RetainGenerationResponse.fromBuffer);
  static final _$export = $grpc.ClientMethod<$0.ExportRequest, $0.ExportChunk>(
      '/acyclic.filesystem.v2.FilesystemService/Export',
      ($0.ExportRequest value) => value.writeToBuffer(),
      $0.ExportChunk.fromBuffer);
  static final _$import = $grpc.ClientMethod<$0.ImportChunk, $0.ImportResponse>(
      '/acyclic.filesystem.v2.FilesystemService/Import',
      ($0.ImportChunk value) => value.writeToBuffer(),
      $0.ImportResponse.fromBuffer);
  static final _$issueMountCredential =
      $grpc.ClientMethod<$0.CredentialRequest, $0.CredentialResponse>(
          '/acyclic.filesystem.v2.FilesystemService/IssueMountCredential',
          ($0.CredentialRequest value) => value.writeToBuffer(),
          $0.CredentialResponse.fromBuffer);
  static final _$issueS3Credential =
      $grpc.ClientMethod<$0.CredentialRequest, $0.CredentialResponse>(
          '/acyclic.filesystem.v2.FilesystemService/IssueS3Credential',
          ($0.CredentialRequest value) => value.writeToBuffer(),
          $0.CredentialResponse.fromBuffer);
  static final _$getSourceState =
      $grpc.ClientMethod<$0.SourceStateRequest, $0.SourceResponse>(
          '/acyclic.filesystem.v2.FilesystemService/GetSourceState',
          ($0.SourceStateRequest value) => value.writeToBuffer(),
          $0.SourceResponse.fromBuffer);
  static final _$reconcileSource =
      $grpc.ClientMethod<$0.SourceOperationRequest, $0.SourceResponse>(
          '/acyclic.filesystem.v2.FilesystemService/ReconcileSource',
          ($0.SourceOperationRequest value) => value.writeToBuffer(),
          $0.SourceResponse.fromBuffer);
  static final _$rescanSource =
      $grpc.ClientMethod<$0.SourceOperationRequest, $0.SourceResponse>(
          '/acyclic.filesystem.v2.FilesystemService/RescanSource',
          ($0.SourceOperationRequest value) => value.writeToBuffer(),
          $0.SourceResponse.fromBuffer);
  static final _$sealSource =
      $grpc.ClientMethod<$0.SourceOperationRequest, $0.SourceResponse>(
          '/acyclic.filesystem.v2.FilesystemService/SealSource',
          ($0.SourceOperationRequest value) => value.writeToBuffer(),
          $0.SourceResponse.fromBuffer);
  static final _$observe =
      $grpc.ClientMethod<$0.ObserveRequest, $0.ObserveResponse>(
          '/acyclic.filesystem.v2.FilesystemService/Observe',
          ($0.ObserveRequest value) => value.writeToBuffer(),
          $0.ObserveResponse.fromBuffer);
  static final _$cancel =
      $grpc.ClientMethod<$0.CancelRequest, $0.CancelResponse>(
          '/acyclic.filesystem.v2.FilesystemService/Cancel',
          ($0.CancelRequest value) => value.writeToBuffer(),
          $0.CancelResponse.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.filesystem.v2.FilesystemService')
abstract class FilesystemServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.filesystem.v2.FilesystemService';

  FilesystemServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.HandshakeRequest, $0.HandshakeResponse>(
        'Handshake',
        handshake_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.HandshakeRequest.fromBuffer(value),
        ($0.HandshakeResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.CreateWorkspaceRequest, $0.WorkspaceResponse>(
            'CreateWorkspace',
            createWorkspace_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.CreateWorkspaceRequest.fromBuffer(value),
            ($0.WorkspaceResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.OpenWorkspaceRequest, $0.WorkspaceResponse>(
            'OpenWorkspace',
            openWorkspace_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.OpenWorkspaceRequest.fromBuffer(value),
            ($0.WorkspaceResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.DeleteWorkspaceRequest, $0.MutationResponse>(
            'DeleteWorkspace',
            deleteWorkspace_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.DeleteWorkspaceRequest.fromBuffer(value),
            ($0.MutationResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetHeadRequest, $0.GenerationResponse>(
        'GetHead',
        getHead_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.GetHeadRequest.fromBuffer(value),
        ($0.GenerationResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.GetGenerationRequest, $0.GenerationResponse>(
            'GetGeneration',
            getGeneration_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.GetGenerationRequest.fromBuffer(value),
            ($0.GenerationResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ReadRequest, $0.ReadResponse>(
        'Read',
        read_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ReadRequest.fromBuffer(value),
        ($0.ReadResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.StatRequest, $0.StatResponse>(
        'Stat',
        stat_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.StatRequest.fromBuffer(value),
        ($0.StatResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.ListDirectoryRequest, $0.ListDirectoryResponse>(
            'ListDirectory',
            listDirectory_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.ListDirectoryRequest.fromBuffer(value),
            ($0.ListDirectoryResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ReadLinkRequest, $0.ReadResponse>(
        'ReadLink',
        readLink_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ReadLinkRequest.fromBuffer(value),
        ($0.ReadResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.PlanExtentsRequest, $0.PlanExtentsResponse>(
            'PlanExtents',
            planExtents_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.PlanExtentsRequest.fromBuffer(value),
            ($0.PlanExtentsResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.ApplyTransactionRequest, $0.MutationResponse>(
            'ApplyTransaction',
            applyTransaction_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.ApplyTransactionRequest.fromBuffer(value),
            ($0.MutationResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.RebaseTransactionRequest,
            $0.RebaseTransactionResponse>(
        'RebaseTransaction',
        rebaseTransaction_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.RebaseTransactionRequest.fromBuffer(value),
        ($0.RebaseTransactionResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.ForkWorkspaceRequest, $0.WorkspaceResponse>(
            'ForkWorkspace',
            forkWorkspace_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.ForkWorkspaceRequest.fromBuffer(value),
            ($0.WorkspaceResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.DiffRequest, $0.DiffResponse>(
        'Diff',
        diff_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.DiffRequest.fromBuffer(value),
        ($0.DiffResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.RebaseRequest, $0.RebaseResponse>(
        'Rebase',
        rebase_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.RebaseRequest.fromBuffer(value),
        ($0.RebaseResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.PlanJoinRequest, $0.JoinPlan>(
        'PlanJoin',
        planJoin_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.PlanJoinRequest.fromBuffer(value),
        ($0.JoinPlan value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ApplyJoinRequest, $0.JoinResponse>(
        'ApplyJoin',
        applyJoin_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ApplyJoinRequest.fromBuffer(value),
        ($0.JoinResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.RetainGenerationRequest,
            $0.RetainGenerationResponse>(
        'Checkpoint',
        checkpoint_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.RetainGenerationRequest.fromBuffer(value),
        ($0.RetainGenerationResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.RetainGenerationRequest,
            $0.RetainGenerationResponse>(
        'Pin',
        pin_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.RetainGenerationRequest.fromBuffer(value),
        ($0.RetainGenerationResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ExportRequest, $0.ExportChunk>(
        'Export',
        export_Pre,
        false,
        true,
        ($core.List<$core.int> value) => $0.ExportRequest.fromBuffer(value),
        ($0.ExportChunk value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ImportChunk, $0.ImportResponse>(
        'Import',
        import,
        true,
        false,
        ($core.List<$core.int> value) => $0.ImportChunk.fromBuffer(value),
        ($0.ImportResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.CredentialRequest, $0.CredentialResponse>(
        'IssueMountCredential',
        issueMountCredential_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.CredentialRequest.fromBuffer(value),
        ($0.CredentialResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.CredentialRequest, $0.CredentialResponse>(
        'IssueS3Credential',
        issueS3Credential_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.CredentialRequest.fromBuffer(value),
        ($0.CredentialResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.SourceStateRequest, $0.SourceResponse>(
        'GetSourceState',
        getSourceState_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.SourceStateRequest.fromBuffer(value),
        ($0.SourceResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.SourceOperationRequest, $0.SourceResponse>(
            'ReconcileSource',
            reconcileSource_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.SourceOperationRequest.fromBuffer(value),
            ($0.SourceResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.SourceOperationRequest, $0.SourceResponse>(
            'RescanSource',
            rescanSource_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.SourceOperationRequest.fromBuffer(value),
            ($0.SourceResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.SourceOperationRequest, $0.SourceResponse>(
            'SealSource',
            sealSource_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.SourceOperationRequest.fromBuffer(value),
            ($0.SourceResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ObserveRequest, $0.ObserveResponse>(
        'Observe',
        observe_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ObserveRequest.fromBuffer(value),
        ($0.ObserveResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.CancelRequest, $0.CancelResponse>(
        'Cancel',
        cancel_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.CancelRequest.fromBuffer(value),
        ($0.CancelResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.HandshakeResponse> handshake_Pre($grpc.ServiceCall $call,
      $async.Future<$0.HandshakeRequest> $request) async {
    return handshake($call, await $request);
  }

  $async.Future<$0.HandshakeResponse> handshake(
      $grpc.ServiceCall call, $0.HandshakeRequest request);

  $async.Future<$0.WorkspaceResponse> createWorkspace_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.CreateWorkspaceRequest> $request) async {
    return createWorkspace($call, await $request);
  }

  $async.Future<$0.WorkspaceResponse> createWorkspace(
      $grpc.ServiceCall call, $0.CreateWorkspaceRequest request);

  $async.Future<$0.WorkspaceResponse> openWorkspace_Pre($grpc.ServiceCall $call,
      $async.Future<$0.OpenWorkspaceRequest> $request) async {
    return openWorkspace($call, await $request);
  }

  $async.Future<$0.WorkspaceResponse> openWorkspace(
      $grpc.ServiceCall call, $0.OpenWorkspaceRequest request);

  $async.Future<$0.MutationResponse> deleteWorkspace_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.DeleteWorkspaceRequest> $request) async {
    return deleteWorkspace($call, await $request);
  }

  $async.Future<$0.MutationResponse> deleteWorkspace(
      $grpc.ServiceCall call, $0.DeleteWorkspaceRequest request);

  $async.Future<$0.GenerationResponse> getHead_Pre($grpc.ServiceCall $call,
      $async.Future<$0.GetHeadRequest> $request) async {
    return getHead($call, await $request);
  }

  $async.Future<$0.GenerationResponse> getHead(
      $grpc.ServiceCall call, $0.GetHeadRequest request);

  $async.Future<$0.GenerationResponse> getGeneration_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.GetGenerationRequest> $request) async {
    return getGeneration($call, await $request);
  }

  $async.Future<$0.GenerationResponse> getGeneration(
      $grpc.ServiceCall call, $0.GetGenerationRequest request);

  $async.Future<$0.ReadResponse> read_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.ReadRequest> $request) async {
    return read($call, await $request);
  }

  $async.Future<$0.ReadResponse> read(
      $grpc.ServiceCall call, $0.ReadRequest request);

  $async.Future<$0.StatResponse> stat_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.StatRequest> $request) async {
    return stat($call, await $request);
  }

  $async.Future<$0.StatResponse> stat(
      $grpc.ServiceCall call, $0.StatRequest request);

  $async.Future<$0.ListDirectoryResponse> listDirectory_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.ListDirectoryRequest> $request) async {
    return listDirectory($call, await $request);
  }

  $async.Future<$0.ListDirectoryResponse> listDirectory(
      $grpc.ServiceCall call, $0.ListDirectoryRequest request);

  $async.Future<$0.ReadResponse> readLink_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ReadLinkRequest> $request) async {
    return readLink($call, await $request);
  }

  $async.Future<$0.ReadResponse> readLink(
      $grpc.ServiceCall call, $0.ReadLinkRequest request);

  $async.Future<$0.PlanExtentsResponse> planExtents_Pre($grpc.ServiceCall $call,
      $async.Future<$0.PlanExtentsRequest> $request) async {
    return planExtents($call, await $request);
  }

  $async.Future<$0.PlanExtentsResponse> planExtents(
      $grpc.ServiceCall call, $0.PlanExtentsRequest request);

  $async.Future<$0.MutationResponse> applyTransaction_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.ApplyTransactionRequest> $request) async {
    return applyTransaction($call, await $request);
  }

  $async.Future<$0.MutationResponse> applyTransaction(
      $grpc.ServiceCall call, $0.ApplyTransactionRequest request);

  $async.Future<$0.RebaseTransactionResponse> rebaseTransaction_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.RebaseTransactionRequest> $request) async {
    return rebaseTransaction($call, await $request);
  }

  $async.Future<$0.RebaseTransactionResponse> rebaseTransaction(
      $grpc.ServiceCall call, $0.RebaseTransactionRequest request);

  $async.Future<$0.WorkspaceResponse> forkWorkspace_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ForkWorkspaceRequest> $request) async {
    return forkWorkspace($call, await $request);
  }

  $async.Future<$0.WorkspaceResponse> forkWorkspace(
      $grpc.ServiceCall call, $0.ForkWorkspaceRequest request);

  $async.Future<$0.DiffResponse> diff_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.DiffRequest> $request) async {
    return diff($call, await $request);
  }

  $async.Future<$0.DiffResponse> diff(
      $grpc.ServiceCall call, $0.DiffRequest request);

  $async.Future<$0.RebaseResponse> rebase_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.RebaseRequest> $request) async {
    return rebase($call, await $request);
  }

  $async.Future<$0.RebaseResponse> rebase(
      $grpc.ServiceCall call, $0.RebaseRequest request);

  $async.Future<$0.JoinPlan> planJoin_Pre($grpc.ServiceCall $call,
      $async.Future<$0.PlanJoinRequest> $request) async {
    return planJoin($call, await $request);
  }

  $async.Future<$0.JoinPlan> planJoin(
      $grpc.ServiceCall call, $0.PlanJoinRequest request);

  $async.Future<$0.JoinResponse> applyJoin_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ApplyJoinRequest> $request) async {
    return applyJoin($call, await $request);
  }

  $async.Future<$0.JoinResponse> applyJoin(
      $grpc.ServiceCall call, $0.ApplyJoinRequest request);

  $async.Future<$0.RetainGenerationResponse> checkpoint_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.RetainGenerationRequest> $request) async {
    return checkpoint($call, await $request);
  }

  $async.Future<$0.RetainGenerationResponse> checkpoint(
      $grpc.ServiceCall call, $0.RetainGenerationRequest request);

  $async.Future<$0.RetainGenerationResponse> pin_Pre($grpc.ServiceCall $call,
      $async.Future<$0.RetainGenerationRequest> $request) async {
    return pin($call, await $request);
  }

  $async.Future<$0.RetainGenerationResponse> pin(
      $grpc.ServiceCall call, $0.RetainGenerationRequest request);

  $async.Stream<$0.ExportChunk> export_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ExportRequest> $request) async* {
    yield* export($call, await $request);
  }

  $async.Stream<$0.ExportChunk> export(
      $grpc.ServiceCall call, $0.ExportRequest request);

  $async.Future<$0.ImportResponse> import(
      $grpc.ServiceCall call, $async.Stream<$0.ImportChunk> request);

  $async.Future<$0.CredentialResponse> issueMountCredential_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.CredentialRequest> $request) async {
    return issueMountCredential($call, await $request);
  }

  $async.Future<$0.CredentialResponse> issueMountCredential(
      $grpc.ServiceCall call, $0.CredentialRequest request);

  $async.Future<$0.CredentialResponse> issueS3Credential_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.CredentialRequest> $request) async {
    return issueS3Credential($call, await $request);
  }

  $async.Future<$0.CredentialResponse> issueS3Credential(
      $grpc.ServiceCall call, $0.CredentialRequest request);

  $async.Future<$0.SourceResponse> getSourceState_Pre($grpc.ServiceCall $call,
      $async.Future<$0.SourceStateRequest> $request) async {
    return getSourceState($call, await $request);
  }

  $async.Future<$0.SourceResponse> getSourceState(
      $grpc.ServiceCall call, $0.SourceStateRequest request);

  $async.Future<$0.SourceResponse> reconcileSource_Pre($grpc.ServiceCall $call,
      $async.Future<$0.SourceOperationRequest> $request) async {
    return reconcileSource($call, await $request);
  }

  $async.Future<$0.SourceResponse> reconcileSource(
      $grpc.ServiceCall call, $0.SourceOperationRequest request);

  $async.Future<$0.SourceResponse> rescanSource_Pre($grpc.ServiceCall $call,
      $async.Future<$0.SourceOperationRequest> $request) async {
    return rescanSource($call, await $request);
  }

  $async.Future<$0.SourceResponse> rescanSource(
      $grpc.ServiceCall call, $0.SourceOperationRequest request);

  $async.Future<$0.SourceResponse> sealSource_Pre($grpc.ServiceCall $call,
      $async.Future<$0.SourceOperationRequest> $request) async {
    return sealSource($call, await $request);
  }

  $async.Future<$0.SourceResponse> sealSource(
      $grpc.ServiceCall call, $0.SourceOperationRequest request);

  $async.Future<$0.ObserveResponse> observe_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ObserveRequest> $request) async {
    return observe($call, await $request);
  }

  $async.Future<$0.ObserveResponse> observe(
      $grpc.ServiceCall call, $0.ObserveRequest request);

  $async.Future<$0.CancelResponse> cancel_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.CancelRequest> $request) async {
    return cancel($call, await $request);
  }

  $async.Future<$0.CancelResponse> cancel(
      $grpc.ServiceCall call, $0.CancelRequest request);
}
