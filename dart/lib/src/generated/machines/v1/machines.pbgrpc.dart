// This is a generated file - do not edit.
//
// Generated from machines/v1/machines.proto.

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

import 'machines.pb.dart' as $0;

export 'machines.pb.dart';

@$pb.GrpcServiceName('acyclic.machines.v1.MachinesService')
class MachinesServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  MachinesServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.ImageQualification> qualifyImage(
    $0.QualifyImageRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$qualifyImage, request, options: options);
  }

  $grpc.ResponseFuture<$0.MachineAdmission> create(
    $0.CreateMachineRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$create, request, options: options);
  }

  $grpc.ResponseFuture<$0.CheckpointAdmission> checkpoint(
    $0.CheckpointMachineRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$checkpoint, request, options: options);
  }

  $grpc.ResponseFuture<$0.ForkAdmission> fork(
    $0.ForkCheckpointRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$fork, request, options: options);
  }

  $grpc.ResponseFuture<$0.ForkMachineAdmission> forkMachine(
    $0.ForkMachineRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$forkMachine, request, options: options);
  }

  $grpc.ResponseFuture<$0.MutationAdmission> suspend(
    $0.MachineMutationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$suspend, request, options: options);
  }

  $grpc.ResponseFuture<$0.MutationAdmission> wake(
    $0.MachineMutationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$wake, request, options: options);
  }

  $grpc.ResponseFuture<$0.PolicyAdmission> setSuspensionPolicy(
    $0.SetSuspensionPolicyRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$setSuspensionPolicy, request, options: options);
  }

  $grpc.ResponseFuture<$0.MutationAdmission> destroyMachine(
    $0.MachineMutationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$destroyMachine, request, options: options);
  }

  $grpc.ResponseFuture<$0.MutationAdmission> destroyCheckpoint(
    $0.CheckpointMutationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$destroyCheckpoint, request, options: options);
  }

  $grpc.ResponseFuture<$0.RecoveredAdmission> recover(
    $0.RecoverRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$recover, request, options: options);
  }

  $grpc.ResponseFuture<$0.MachineState> inspectMachine(
    $0.InspectMachineRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspectMachine, request, options: options);
  }

  $grpc.ResponseFuture<$0.CheckpointState> inspectCheckpoint(
    $0.InspectCheckpointRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspectCheckpoint, request, options: options);
  }

  $grpc.ResponseFuture<$0.MachinePage> listMachines(
    $0.ListMachinesRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$listMachines, request, options: options);
  }

  $grpc.ResponseFuture<$0.EventPage> events(
    $0.EventsRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$events, request, options: options);
  }

  $grpc.ResponseFuture<$0.UsageReceipt> usage(
    $0.UsageRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$usage, request, options: options);
  }

  $grpc.ResponseFuture<$0.OperationState> cancel(
    $0.OperationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$cancel, request, options: options);
  }

  $grpc.ResponseFuture<$0.OperationState> inspectOperation(
    $0.OperationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$inspectOperation, request, options: options);
  }

  $grpc.ResponseStream<$0.OperationState> watchOperation(
    $0.OperationRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(
        _$watchOperation, $async.Stream.fromIterable([request]),
        options: options);
  }

  // method descriptors

  static final _$qualifyImage =
      $grpc.ClientMethod<$0.QualifyImageRequest, $0.ImageQualification>(
          '/acyclic.machines.v1.MachinesService/QualifyImage',
          ($0.QualifyImageRequest value) => value.writeToBuffer(),
          $0.ImageQualification.fromBuffer);
  static final _$create =
      $grpc.ClientMethod<$0.CreateMachineRequest, $0.MachineAdmission>(
          '/acyclic.machines.v1.MachinesService/Create',
          ($0.CreateMachineRequest value) => value.writeToBuffer(),
          $0.MachineAdmission.fromBuffer);
  static final _$checkpoint =
      $grpc.ClientMethod<$0.CheckpointMachineRequest, $0.CheckpointAdmission>(
          '/acyclic.machines.v1.MachinesService/Checkpoint',
          ($0.CheckpointMachineRequest value) => value.writeToBuffer(),
          $0.CheckpointAdmission.fromBuffer);
  static final _$fork =
      $grpc.ClientMethod<$0.ForkCheckpointRequest, $0.ForkAdmission>(
          '/acyclic.machines.v1.MachinesService/Fork',
          ($0.ForkCheckpointRequest value) => value.writeToBuffer(),
          $0.ForkAdmission.fromBuffer);
  static final _$forkMachine =
      $grpc.ClientMethod<$0.ForkMachineRequest, $0.ForkMachineAdmission>(
          '/acyclic.machines.v1.MachinesService/ForkMachine',
          ($0.ForkMachineRequest value) => value.writeToBuffer(),
          $0.ForkMachineAdmission.fromBuffer);
  static final _$suspend =
      $grpc.ClientMethod<$0.MachineMutationRequest, $0.MutationAdmission>(
          '/acyclic.machines.v1.MachinesService/Suspend',
          ($0.MachineMutationRequest value) => value.writeToBuffer(),
          $0.MutationAdmission.fromBuffer);
  static final _$wake =
      $grpc.ClientMethod<$0.MachineMutationRequest, $0.MutationAdmission>(
          '/acyclic.machines.v1.MachinesService/Wake',
          ($0.MachineMutationRequest value) => value.writeToBuffer(),
          $0.MutationAdmission.fromBuffer);
  static final _$setSuspensionPolicy =
      $grpc.ClientMethod<$0.SetSuspensionPolicyRequest, $0.PolicyAdmission>(
          '/acyclic.machines.v1.MachinesService/SetSuspensionPolicy',
          ($0.SetSuspensionPolicyRequest value) => value.writeToBuffer(),
          $0.PolicyAdmission.fromBuffer);
  static final _$destroyMachine =
      $grpc.ClientMethod<$0.MachineMutationRequest, $0.MutationAdmission>(
          '/acyclic.machines.v1.MachinesService/DestroyMachine',
          ($0.MachineMutationRequest value) => value.writeToBuffer(),
          $0.MutationAdmission.fromBuffer);
  static final _$destroyCheckpoint =
      $grpc.ClientMethod<$0.CheckpointMutationRequest, $0.MutationAdmission>(
          '/acyclic.machines.v1.MachinesService/DestroyCheckpoint',
          ($0.CheckpointMutationRequest value) => value.writeToBuffer(),
          $0.MutationAdmission.fromBuffer);
  static final _$recover =
      $grpc.ClientMethod<$0.RecoverRequest, $0.RecoveredAdmission>(
          '/acyclic.machines.v1.MachinesService/Recover',
          ($0.RecoverRequest value) => value.writeToBuffer(),
          $0.RecoveredAdmission.fromBuffer);
  static final _$inspectMachine =
      $grpc.ClientMethod<$0.InspectMachineRequest, $0.MachineState>(
          '/acyclic.machines.v1.MachinesService/InspectMachine',
          ($0.InspectMachineRequest value) => value.writeToBuffer(),
          $0.MachineState.fromBuffer);
  static final _$inspectCheckpoint =
      $grpc.ClientMethod<$0.InspectCheckpointRequest, $0.CheckpointState>(
          '/acyclic.machines.v1.MachinesService/InspectCheckpoint',
          ($0.InspectCheckpointRequest value) => value.writeToBuffer(),
          $0.CheckpointState.fromBuffer);
  static final _$listMachines =
      $grpc.ClientMethod<$0.ListMachinesRequest, $0.MachinePage>(
          '/acyclic.machines.v1.MachinesService/ListMachines',
          ($0.ListMachinesRequest value) => value.writeToBuffer(),
          $0.MachinePage.fromBuffer);
  static final _$events = $grpc.ClientMethod<$0.EventsRequest, $0.EventPage>(
      '/acyclic.machines.v1.MachinesService/Events',
      ($0.EventsRequest value) => value.writeToBuffer(),
      $0.EventPage.fromBuffer);
  static final _$usage = $grpc.ClientMethod<$0.UsageRequest, $0.UsageReceipt>(
      '/acyclic.machines.v1.MachinesService/Usage',
      ($0.UsageRequest value) => value.writeToBuffer(),
      $0.UsageReceipt.fromBuffer);
  static final _$cancel =
      $grpc.ClientMethod<$0.OperationRequest, $0.OperationState>(
          '/acyclic.machines.v1.MachinesService/Cancel',
          ($0.OperationRequest value) => value.writeToBuffer(),
          $0.OperationState.fromBuffer);
  static final _$inspectOperation =
      $grpc.ClientMethod<$0.OperationRequest, $0.OperationState>(
          '/acyclic.machines.v1.MachinesService/InspectOperation',
          ($0.OperationRequest value) => value.writeToBuffer(),
          $0.OperationState.fromBuffer);
  static final _$watchOperation =
      $grpc.ClientMethod<$0.OperationRequest, $0.OperationState>(
          '/acyclic.machines.v1.MachinesService/WatchOperation',
          ($0.OperationRequest value) => value.writeToBuffer(),
          $0.OperationState.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.machines.v1.MachinesService')
abstract class MachinesServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.machines.v1.MachinesService';

  MachinesServiceBase() {
    $addMethod(
        $grpc.ServiceMethod<$0.QualifyImageRequest, $0.ImageQualification>(
            'QualifyImage',
            qualifyImage_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.QualifyImageRequest.fromBuffer(value),
            ($0.ImageQualification value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.CreateMachineRequest, $0.MachineAdmission>(
            'Create',
            create_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.CreateMachineRequest.fromBuffer(value),
            ($0.MachineAdmission value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.CheckpointMachineRequest,
            $0.CheckpointAdmission>(
        'Checkpoint',
        checkpoint_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.CheckpointMachineRequest.fromBuffer(value),
        ($0.CheckpointAdmission value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ForkCheckpointRequest, $0.ForkAdmission>(
        'Fork',
        fork_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.ForkCheckpointRequest.fromBuffer(value),
        ($0.ForkAdmission value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.ForkMachineRequest, $0.ForkMachineAdmission>(
            'ForkMachine',
            forkMachine_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.ForkMachineRequest.fromBuffer(value),
            ($0.ForkMachineAdmission value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.MachineMutationRequest, $0.MutationAdmission>(
            'Suspend',
            suspend_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.MachineMutationRequest.fromBuffer(value),
            ($0.MutationAdmission value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.MachineMutationRequest, $0.MutationAdmission>(
            'Wake',
            wake_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.MachineMutationRequest.fromBuffer(value),
            ($0.MutationAdmission value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.SetSuspensionPolicyRequest, $0.PolicyAdmission>(
            'SetSuspensionPolicy',
            setSuspensionPolicy_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.SetSuspensionPolicyRequest.fromBuffer(value),
            ($0.PolicyAdmission value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.MachineMutationRequest, $0.MutationAdmission>(
            'DestroyMachine',
            destroyMachine_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.MachineMutationRequest.fromBuffer(value),
            ($0.MutationAdmission value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.CheckpointMutationRequest, $0.MutationAdmission>(
            'DestroyCheckpoint',
            destroyCheckpoint_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.CheckpointMutationRequest.fromBuffer(value),
            ($0.MutationAdmission value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.RecoverRequest, $0.RecoveredAdmission>(
        'Recover',
        recover_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.RecoverRequest.fromBuffer(value),
        ($0.RecoveredAdmission value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.InspectMachineRequest, $0.MachineState>(
        'InspectMachine',
        inspectMachine_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.InspectMachineRequest.fromBuffer(value),
        ($0.MachineState value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.InspectCheckpointRequest, $0.CheckpointState>(
            'InspectCheckpoint',
            inspectCheckpoint_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.InspectCheckpointRequest.fromBuffer(value),
            ($0.CheckpointState value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ListMachinesRequest, $0.MachinePage>(
        'ListMachines',
        listMachines_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.ListMachinesRequest.fromBuffer(value),
        ($0.MachinePage value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.EventsRequest, $0.EventPage>(
        'Events',
        events_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.EventsRequest.fromBuffer(value),
        ($0.EventPage value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.UsageRequest, $0.UsageReceipt>(
        'Usage',
        usage_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.UsageRequest.fromBuffer(value),
        ($0.UsageReceipt value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.OperationRequest, $0.OperationState>(
        'Cancel',
        cancel_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.OperationRequest.fromBuffer(value),
        ($0.OperationState value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.OperationRequest, $0.OperationState>(
        'InspectOperation',
        inspectOperation_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.OperationRequest.fromBuffer(value),
        ($0.OperationState value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.OperationRequest, $0.OperationState>(
        'WatchOperation',
        watchOperation_Pre,
        false,
        true,
        ($core.List<$core.int> value) => $0.OperationRequest.fromBuffer(value),
        ($0.OperationState value) => value.writeToBuffer()));
  }

  $async.Future<$0.ImageQualification> qualifyImage_Pre($grpc.ServiceCall $call,
      $async.Future<$0.QualifyImageRequest> $request) async {
    return qualifyImage($call, await $request);
  }

  $async.Future<$0.ImageQualification> qualifyImage(
      $grpc.ServiceCall call, $0.QualifyImageRequest request);

  $async.Future<$0.MachineAdmission> create_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CreateMachineRequest> $request) async {
    return create($call, await $request);
  }

  $async.Future<$0.MachineAdmission> create(
      $grpc.ServiceCall call, $0.CreateMachineRequest request);

  $async.Future<$0.CheckpointAdmission> checkpoint_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CheckpointMachineRequest> $request) async {
    return checkpoint($call, await $request);
  }

  $async.Future<$0.CheckpointAdmission> checkpoint(
      $grpc.ServiceCall call, $0.CheckpointMachineRequest request);

  $async.Future<$0.ForkAdmission> fork_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ForkCheckpointRequest> $request) async {
    return fork($call, await $request);
  }

  $async.Future<$0.ForkAdmission> fork(
      $grpc.ServiceCall call, $0.ForkCheckpointRequest request);

  $async.Future<$0.ForkMachineAdmission> forkMachine_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.ForkMachineRequest> $request) async {
    return forkMachine($call, await $request);
  }

  $async.Future<$0.ForkMachineAdmission> forkMachine(
      $grpc.ServiceCall call, $0.ForkMachineRequest request);

  $async.Future<$0.MutationAdmission> suspend_Pre($grpc.ServiceCall $call,
      $async.Future<$0.MachineMutationRequest> $request) async {
    return suspend($call, await $request);
  }

  $async.Future<$0.MutationAdmission> suspend(
      $grpc.ServiceCall call, $0.MachineMutationRequest request);

  $async.Future<$0.MutationAdmission> wake_Pre($grpc.ServiceCall $call,
      $async.Future<$0.MachineMutationRequest> $request) async {
    return wake($call, await $request);
  }

  $async.Future<$0.MutationAdmission> wake(
      $grpc.ServiceCall call, $0.MachineMutationRequest request);

  $async.Future<$0.PolicyAdmission> setSuspensionPolicy_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.SetSuspensionPolicyRequest> $request) async {
    return setSuspensionPolicy($call, await $request);
  }

  $async.Future<$0.PolicyAdmission> setSuspensionPolicy(
      $grpc.ServiceCall call, $0.SetSuspensionPolicyRequest request);

  $async.Future<$0.MutationAdmission> destroyMachine_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.MachineMutationRequest> $request) async {
    return destroyMachine($call, await $request);
  }

  $async.Future<$0.MutationAdmission> destroyMachine(
      $grpc.ServiceCall call, $0.MachineMutationRequest request);

  $async.Future<$0.MutationAdmission> destroyCheckpoint_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.CheckpointMutationRequest> $request) async {
    return destroyCheckpoint($call, await $request);
  }

  $async.Future<$0.MutationAdmission> destroyCheckpoint(
      $grpc.ServiceCall call, $0.CheckpointMutationRequest request);

  $async.Future<$0.RecoveredAdmission> recover_Pre($grpc.ServiceCall $call,
      $async.Future<$0.RecoverRequest> $request) async {
    return recover($call, await $request);
  }

  $async.Future<$0.RecoveredAdmission> recover(
      $grpc.ServiceCall call, $0.RecoverRequest request);

  $async.Future<$0.MachineState> inspectMachine_Pre($grpc.ServiceCall $call,
      $async.Future<$0.InspectMachineRequest> $request) async {
    return inspectMachine($call, await $request);
  }

  $async.Future<$0.MachineState> inspectMachine(
      $grpc.ServiceCall call, $0.InspectMachineRequest request);

  $async.Future<$0.CheckpointState> inspectCheckpoint_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.InspectCheckpointRequest> $request) async {
    return inspectCheckpoint($call, await $request);
  }

  $async.Future<$0.CheckpointState> inspectCheckpoint(
      $grpc.ServiceCall call, $0.InspectCheckpointRequest request);

  $async.Future<$0.MachinePage> listMachines_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ListMachinesRequest> $request) async {
    return listMachines($call, await $request);
  }

  $async.Future<$0.MachinePage> listMachines(
      $grpc.ServiceCall call, $0.ListMachinesRequest request);

  $async.Future<$0.EventPage> events_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.EventsRequest> $request) async {
    return events($call, await $request);
  }

  $async.Future<$0.EventPage> events(
      $grpc.ServiceCall call, $0.EventsRequest request);

  $async.Future<$0.UsageReceipt> usage_Pre(
      $grpc.ServiceCall $call, $async.Future<$0.UsageRequest> $request) async {
    return usage($call, await $request);
  }

  $async.Future<$0.UsageReceipt> usage(
      $grpc.ServiceCall call, $0.UsageRequest request);

  $async.Future<$0.OperationState> cancel_Pre($grpc.ServiceCall $call,
      $async.Future<$0.OperationRequest> $request) async {
    return cancel($call, await $request);
  }

  $async.Future<$0.OperationState> cancel(
      $grpc.ServiceCall call, $0.OperationRequest request);

  $async.Future<$0.OperationState> inspectOperation_Pre($grpc.ServiceCall $call,
      $async.Future<$0.OperationRequest> $request) async {
    return inspectOperation($call, await $request);
  }

  $async.Future<$0.OperationState> inspectOperation(
      $grpc.ServiceCall call, $0.OperationRequest request);

  $async.Stream<$0.OperationState> watchOperation_Pre($grpc.ServiceCall $call,
      $async.Future<$0.OperationRequest> $request) async* {
    yield* watchOperation($call, await $request);
  }

  $async.Stream<$0.OperationState> watchOperation(
      $grpc.ServiceCall call, $0.OperationRequest request);
}
