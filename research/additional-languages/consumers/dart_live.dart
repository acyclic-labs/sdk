#!/usr/bin/env dart
// Generated source-bound live consumer. Run from an installed acyclic_sdk package.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:crypto/crypto.dart' as crypto;
import 'package:grpc/grpc.dart';
import 'package:acyclic_sdk/src/generated/actors/v1/actors.pbgrpc.dart' as actors;
import 'package:acyclic_sdk/src/generated/protocol/v1/protocol.pb.dart' as protocol;
import 'package:acyclic_sdk/src/generated/filesystem/v2/filesystem.pbgrpc.dart' as filesystem;
import 'package:acyclic_sdk/src/generated/harness/v2/harness.pbgrpc.dart' as harness;
import 'package:acyclic_sdk/src/generated/inference/v1/inference.pbgrpc.dart' as inference;
import 'package:acyclic_sdk/src/generated/machines/v1/machines.pbgrpc.dart' as machines;
import 'package:acyclic_sdk/src/generated/objects/v2/objects.pbgrpc.dart' as objects;
import 'package:acyclic_sdk/src/generated/stream/v2/stream.pbgrpc.dart' as stream;
import 'package:acyclic_sdk/src/generated/workers/v1/workers.pbgrpc.dart' as workers;

String wireHex(dynamic value) => value.writeToBuffer().map((byte) => byte.toRadixString(16).padLeft(2, '0')).join();
String wireSha256(String hex) => crypto.sha256.convert([for (var i = 0; i < hex.length; i += 2) int.parse(hex.substring(i, i + 2), radix: 16)]).toString();
dynamic decodeMessage(dynamic value) {
  try { return jsonDecode(value.writeToJson()); } catch (_) { return value.toString(); }
}

String arg(List<String> args, String name, String fallback) {
  final prefix = '$name=';
  for (var i = 0; i < args.length; i++) {
    if (args[i] == name && i + 1 < args.length) return args[i + 1];
    if (args[i].startsWith(prefix)) return args[i].substring(prefix.length);
  }
  return fallback;
}

Future<Object?> invoke(String key, ClientChannel channel, List<List<int>> frameBytes, Duration timeout) async {
  final bytes = frameBytes.first;
  switch (key) {
    case "acyclic.actors.v1.ActorsService/CreateActor":
      final request = actors.CreateActorRequest.fromBuffer(bytes);
      final client = actors.ActorsServiceClient(channel);
      final response = await client.createActor(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.actors.v1.ActorsService/UpdateActor":
      final request = actors.UpdateActorRequest.fromBuffer(bytes);
      final client = actors.ActorsServiceClient(channel);
      final response = await client.updateActor(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.actors.v1.ActorsService/InspectActor":
      final request = actors.InspectActorRequest.fromBuffer(bytes);
      final client = actors.ActorsServiceClient(channel);
      final response = await client.inspectActor(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.actors.v1.ActorsService/AddSubscription":
      final request = actors.AddSubscriptionRequest.fromBuffer(bytes);
      final client = actors.ActorsServiceClient(channel);
      final response = await client.addSubscription(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.actors.v1.ActorsService/RemoveSubscription":
      final request = actors.RemoveSubscriptionRequest.fromBuffer(bytes);
      final client = actors.ActorsServiceClient(channel);
      final response = await client.removeSubscription(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.actors.v1.ActorsService/ResumeSubscription":
      final request = actors.ResumeSubscriptionRequest.fromBuffer(bytes);
      final client = actors.ActorsServiceClient(channel);
      final response = await client.resumeSubscription(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.actors.v1.ActorsService/CheckpointActor":
      final request = actors.CheckpointActorRequest.fromBuffer(bytes);
      final client = actors.ActorsServiceClient(channel);
      final response = await client.checkpointActor(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.actors.v1.ActorsService/InvokeActor":
      final request = actors.InvokeActorRequest.fromBuffer(bytes);
      final client = actors.ActorsServiceClient(channel);
      final response = await client.invokeActor(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Handshake":
      final request = filesystem.HandshakeRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.handshake(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/CreateWorkspace":
      final request = filesystem.CreateWorkspaceRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.createWorkspace(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/OpenWorkspace":
      final request = filesystem.OpenWorkspaceRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.openWorkspace(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/DeleteWorkspace":
      final request = filesystem.DeleteWorkspaceRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.deleteWorkspace(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/GetHead":
      final request = filesystem.GetHeadRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.getHead(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/GetGeneration":
      final request = filesystem.GetGenerationRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.getGeneration(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Read":
      final request = filesystem.ReadRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.read(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Stat":
      final request = filesystem.StatRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.stat(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/ListDirectory":
      final request = filesystem.ListDirectoryRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.listDirectory(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/ReadLink":
      final request = filesystem.ReadLinkRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.readLink(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/PlanExtents":
      final request = filesystem.PlanExtentsRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.planExtents(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/ApplyTransaction":
      final request = filesystem.ApplyTransactionRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.applyTransaction(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/RebaseTransaction":
      final request = filesystem.RebaseTransactionRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.rebaseTransaction(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/ForkWorkspace":
      final request = filesystem.ForkWorkspaceRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.forkWorkspace(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Diff":
      final request = filesystem.DiffRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.diff(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Rebase":
      final request = filesystem.RebaseRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.rebase(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/PlanJoin":
      final request = filesystem.PlanJoinRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.planJoin(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/ApplyJoin":
      final request = filesystem.ApplyJoinRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.applyJoin(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Checkpoint":
      final request = filesystem.RetainGenerationRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.checkpoint(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Pin":
      final request = filesystem.RetainGenerationRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.pin(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Export":
      final request = filesystem.ExportRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final frames = <Object?>[];
      final frameHex = <String>[];
      final frameTypes = <String>[];
      await for (final frame in client.export(request, options: CallOptions(timeout: timeout))) { frames.add(decodeMessage(frame)); frameHex.add(wireHex(frame)); frameTypes.add(frame.runtimeType.toString()); }
      return <String, dynamic>{'frames': frames, 'frames_hex': frameHex, 'frame_types': frameTypes};
    case "acyclic.filesystem.v2.FilesystemService/Import":
      final request = filesystem.ImportChunk.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.import(frameBytes.map(filesystem.ImportChunk.fromBuffer), options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/IssueMountCredential":
      final request = filesystem.CredentialRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.issueMountCredential(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/IssueS3Credential":
      final request = filesystem.CredentialRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.issueS3Credential(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/GetSourceState":
      final request = filesystem.SourceStateRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.getSourceState(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/ReconcileSource":
      final request = filesystem.SourceOperationRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.reconcileSource(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/RescanSource":
      final request = filesystem.SourceOperationRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.rescanSource(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/SealSource":
      final request = filesystem.SourceOperationRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.sealSource(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Observe":
      final request = filesystem.ObserveRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.observe(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.filesystem.v2.FilesystemService/Cancel":
      final request = filesystem.CancelRequest.fromBuffer(bytes);
      final client = filesystem.FilesystemServiceClient(channel);
      final response = await client.cancel(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.harness.v2.HarnessService/Handshake":
      final request = harness.HandshakeRequest.fromBuffer(bytes);
      final client = harness.HarnessServiceClient(channel);
      final response = await client.handshake(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.harness.v2.HarnessService/Submit":
      final request = harness.CommandEnvelope.fromBuffer(bytes);
      final client = harness.HarnessServiceClient(channel);
      final response = await client.submit(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.harness.v2.HarnessService/Replay":
      final request = harness.ResumeRequest.fromBuffer(bytes);
      final client = harness.HarnessServiceClient(channel);
      final frames = <Object?>[];
      final frameHex = <String>[];
      final frameTypes = <String>[];
      await for (final frame in client.replay(request, options: CallOptions(timeout: timeout))) { frames.add(decodeMessage(frame)); frameHex.add(wireHex(frame)); frameTypes.add(frame.runtimeType.toString()); }
      return <String, dynamic>{'frames': frames, 'frames_hex': frameHex, 'frame_types': frameTypes};
    case "acyclic.harness.v2.HarnessService/Observe":
      final request = harness.ObserveRequest.fromBuffer(bytes);
      final client = harness.HarnessServiceClient(channel);
      final response = await client.observe(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.harness.v2.HarnessService/Cancel":
      final request = harness.CancelRequest.fromBuffer(bytes);
      final client = harness.HarnessServiceClient(channel);
      final response = await client.cancel(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.ModelsService/List":
      final request = inference.ListModelsRequest.fromBuffer(bytes);
      final client = inference.ModelsServiceClient(channel);
      final response = await client.list(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.ContextsService/Create":
      final request = inference.CreateContextRequest.fromBuffer(bytes);
      final client = inference.ContextsServiceClient(channel);
      final response = await client.create(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.ContextsService/Inspect":
      final request = inference.InspectContextRequest.fromBuffer(bytes);
      final client = inference.ContextsServiceClient(channel);
      final response = await client.inspect(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.ContextsService/Mutate":
      final request = inference.MutateContextRequest.fromBuffer(bytes);
      final client = inference.ContextsServiceClient(channel);
      final response = await client.mutate(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.WarmContextsService/Retain":
      final request = inference.RetainWarmRequest.fromBuffer(bytes);
      final client = inference.WarmContextsServiceClient(channel);
      final response = await client.retain(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.WarmContextsService/Inspect":
      final request = inference.InspectWarmRequest.fromBuffer(bytes);
      final client = inference.WarmContextsServiceClient(channel);
      final response = await client.inspect(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.WarmContextsService/Renew":
      final request = inference.RenewWarmRequest.fromBuffer(bytes);
      final client = inference.WarmContextsServiceClient(channel);
      final response = await client.renew(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.WarmContextsService/Release":
      final request = inference.ReleaseWarmRequest.fromBuffer(bytes);
      final client = inference.WarmContextsServiceClient(channel);
      final response = await client.release(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.RunsService/Generate":
      final request = inference.GenerateRunRequest.fromBuffer(bytes);
      final client = inference.RunsServiceClient(channel);
      final response = await client.generate(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.RunsService/Inspect":
      final request = inference.InspectRunRequest.fromBuffer(bytes);
      final client = inference.RunsServiceClient(channel);
      final response = await client.inspect(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.RunsService/Watch":
      final request = inference.WatchRunRequest.fromBuffer(bytes);
      final client = inference.RunsServiceClient(channel);
      final frames = <Object?>[];
      final frameHex = <String>[];
      final frameTypes = <String>[];
      await for (final frame in client.watch(request, options: CallOptions(timeout: timeout))) { frames.add(decodeMessage(frame)); frameHex.add(wireHex(frame)); frameTypes.add(frame.runtimeType.toString()); }
      return <String, dynamic>{'frames': frames, 'frames_hex': frameHex, 'frame_types': frameTypes};
    case "inference.customer.v1.RunsService/Cancel":
      final request = inference.InspectRunRequest.fromBuffer(bytes);
      final client = inference.RunsServiceClient(channel);
      final response = await client.cancel(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.EvaluationsService/Create":
      final request = inference.CreateEvaluationRequest.fromBuffer(bytes);
      final client = inference.EvaluationsServiceClient(channel);
      final response = await client.create(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "inference.customer.v1.EvaluationsService/Inspect":
      final request = inference.InspectEvaluationRequest.fromBuffer(bytes);
      final client = inference.EvaluationsServiceClient(channel);
      final response = await client.inspect(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/QualifyImage":
      final request = machines.QualifyImageRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.qualifyImage(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Create":
      final request = machines.CreateMachineRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.create(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Checkpoint":
      final request = machines.CheckpointMachineRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.checkpoint(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Fork":
      final request = machines.ForkCheckpointRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.fork(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/ForkMachine":
      final request = machines.ForkMachineRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.forkMachine(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Suspend":
      final request = machines.MachineMutationRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.suspend(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Wake":
      final request = machines.MachineMutationRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.wake(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/SetSuspensionPolicy":
      final request = machines.SetSuspensionPolicyRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.setSuspensionPolicy(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/DestroyMachine":
      final request = machines.MachineMutationRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.destroyMachine(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/DestroyCheckpoint":
      final request = machines.CheckpointMutationRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.destroyCheckpoint(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Recover":
      final request = machines.RecoverRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.recover(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/InspectMachine":
      final request = machines.InspectMachineRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.inspectMachine(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/InspectCheckpoint":
      final request = machines.InspectCheckpointRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.inspectCheckpoint(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/ListMachines":
      final request = machines.ListMachinesRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.listMachines(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Events":
      final request = machines.EventsRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.events(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Usage":
      final request = machines.UsageRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.usage(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/Cancel":
      final request = machines.OperationRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.cancel(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/InspectOperation":
      final request = machines.OperationRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final response = await client.inspectOperation(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.machines.v1.MachinesService/WatchOperation":
      final request = machines.OperationRequest.fromBuffer(bytes);
      final client = machines.MachinesServiceClient(channel);
      final frames = <Object?>[];
      final frameHex = <String>[];
      final frameTypes = <String>[];
      await for (final frame in client.watchOperation(request, options: CallOptions(timeout: timeout))) { frames.add(decodeMessage(frame)); frameHex.add(wireHex(frame)); frameTypes.add(frame.runtimeType.toString()); }
      return <String, dynamic>{'frames': frames, 'frames_hex': frameHex, 'frame_types': frameTypes};
    case "acyclic.objects.v2.BucketsService/CreateBucket":
      final request = objects.CreateBucketRequest.fromBuffer(bytes);
      final client = objects.BucketsServiceClient(channel);
      final response = await client.createBucket(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.BucketsService/HeadBucket":
      final request = objects.HeadBucketRequest.fromBuffer(bytes);
      final client = objects.BucketsServiceClient(channel);
      final response = await client.headBucket(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.BucketsService/DeleteBucket":
      final request = objects.DeleteBucketRequest.fromBuffer(bytes);
      final client = objects.BucketsServiceClient(channel);
      final response = await client.deleteBucket(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.ObjectsService/PutObject":
      final request = objects.PutObjectRequest.fromBuffer(bytes);
      final client = objects.ObjectsServiceClient(channel);
      final response = await client.putObject(frameBytes.map(objects.PutObjectRequest.fromBuffer), options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.ObjectsService/GetObject":
      final request = objects.GetObjectRequest.fromBuffer(bytes);
      final client = objects.ObjectsServiceClient(channel);
      final frames = <Object?>[];
      final frameHex = <String>[];
      final frameTypes = <String>[];
      await for (final frame in client.getObject(request, options: CallOptions(timeout: timeout))) { frames.add(decodeMessage(frame)); frameHex.add(wireHex(frame)); frameTypes.add(frame.runtimeType.toString()); }
      return <String, dynamic>{'frames': frames, 'frames_hex': frameHex, 'frame_types': frameTypes};
    case "acyclic.objects.v2.ObjectsService/HeadObject":
      final request = objects.HeadObjectRequest.fromBuffer(bytes);
      final client = objects.ObjectsServiceClient(channel);
      final response = await client.headObject(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.ObjectsService/DeleteObject":
      final request = objects.DeleteObjectRequest.fromBuffer(bytes);
      final client = objects.ObjectsServiceClient(channel);
      final response = await client.deleteObject(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.ObjectsService/ListObjects":
      final request = objects.ListObjectsRequest.fromBuffer(bytes);
      final client = objects.ObjectsServiceClient(channel);
      final response = await client.listObjects(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.MultipartService/CreateMultipart":
      final request = objects.CreateMultipartRequest.fromBuffer(bytes);
      final client = objects.MultipartServiceClient(channel);
      final response = await client.createMultipart(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.MultipartService/UploadPart":
      final request = objects.UploadPartRequest.fromBuffer(bytes);
      final client = objects.MultipartServiceClient(channel);
      final response = await client.uploadPart(frameBytes.map(objects.UploadPartRequest.fromBuffer), options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.MultipartService/ListParts":
      final request = objects.ListPartsRequest.fromBuffer(bytes);
      final client = objects.MultipartServiceClient(channel);
      final response = await client.listParts(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.MultipartService/CompleteMultipart":
      final request = objects.CompleteMultipartRequest.fromBuffer(bytes);
      final client = objects.MultipartServiceClient(channel);
      final response = await client.completeMultipart(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.objects.v2.MultipartService/AbortMultipart":
      final request = objects.AbortMultipartRequest.fromBuffer(bytes);
      final client = objects.MultipartServiceClient(channel);
      final response = await client.abortMultipart(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.stream.v2.StreamService/InspectIdempotency":
      final request = stream.InspectIdempotencyRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final response = await client.inspectIdempotency(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.stream.v2.StreamService/Append":
      final request = stream.AppendRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final response = await client.append(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.stream.v2.StreamService/Tail":
      final request = stream.TailRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final response = await client.tail(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.stream.v2.StreamService/Fork":
      final request = stream.ForkRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final response = await client.fork(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.stream.v2.StreamService/Read":
      final request = stream.ReadRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final frames = <Object?>[];
      final frameHex = <String>[];
      final frameTypes = <String>[];
      await for (final frame in client.read(request, options: CallOptions(timeout: timeout))) { frames.add(decodeMessage(frame)); frameHex.add(wireHex(frame)); frameTypes.add(frame.runtimeType.toString()); }
      return <String, dynamic>{'frames': frames, 'frames_hex': frameHex, 'frame_types': frameTypes};
    case "acyclic.stream.v2.StreamService/Follow":
      final request = stream.FollowRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final frames = <Object?>[];
      final frameHex = <String>[];
      final frameTypes = <String>[];
      await for (final frame in client.follow(request, options: CallOptions(timeout: timeout))) { frames.add(decodeMessage(frame)); frameHex.add(wireHex(frame)); frameTypes.add(frame.runtimeType.toString()); }
      return <String, dynamic>{'frames': frames, 'frames_hex': frameHex, 'frame_types': frameTypes};
    case "acyclic.stream.v2.StreamService/Children":
      final request = stream.ChildrenRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final frames = <Object?>[];
      final frameHex = <String>[];
      final frameTypes = <String>[];
      await for (final frame in client.children(request, options: CallOptions(timeout: timeout))) { frames.add(decodeMessage(frame)); frameHex.add(wireHex(frame)); frameTypes.add(frame.runtimeType.toString()); }
      return <String, dynamic>{'frames': frames, 'frames_hex': frameHex, 'frame_types': frameTypes};
    case "acyclic.stream.v2.StreamService/ChildrenPage":
      final request = stream.ChildrenPageRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final response = await client.childrenPage(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.stream.v2.StreamService/Commit":
      final request = stream.CommitRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final response = await client.commit(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.stream.v2.StreamService/ReadCommit":
      final request = stream.ReadCommitRequest.fromBuffer(bytes);
      final client = stream.StreamServiceClient(channel);
      final response = await client.readCommit(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.workers.v1.WorkersService/PublishVersion":
      final request = workers.PublishVersionRequest.fromBuffer(bytes);
      final client = workers.WorkersServiceClient(channel);
      final response = await client.publishVersion(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.workers.v1.WorkersService/SelectDeployment":
      final request = workers.SelectDeploymentRequest.fromBuffer(bytes);
      final client = workers.WorkersServiceClient(channel);
      final response = await client.selectDeployment(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.workers.v1.WorkersService/SubmitJob":
      final request = workers.SubmitJobRequest.fromBuffer(bytes);
      final client = workers.WorkersServiceClient(channel);
      final response = await client.submitJob(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.workers.v1.WorkersService/InspectJob":
      final request = workers.InspectJobRequest.fromBuffer(bytes);
      final client = workers.WorkersServiceClient(channel);
      final response = await client.inspectJob(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.workers.v1.WorkersService/CancelJob":
      final request = workers.CancelJobRequest.fromBuffer(bytes);
      final client = workers.WorkersServiceClient(channel);
      final response = await client.cancelJob(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.workers.v1.WorkersService/InvokeVersion":
      final request = workers.InvokeVersionRequest.fromBuffer(bytes);
      final client = workers.WorkersServiceClient(channel);
      final response = await client.invokeVersion(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    case "acyclic.workers.v1.WorkersService/InvokeDeployment":
      final request = workers.InvokeDeploymentRequest.fromBuffer(bytes);
      final client = workers.WorkersServiceClient(channel);
      final response = await client.invokeDeployment(request, options: CallOptions(timeout: timeout));
      return <String, dynamic>{'value': decodeMessage(response), 'bytes_hex': wireHex(response), 'type_name': response.runtimeType.toString()};
    default:
      throw ArgumentError('unknown Rust RPC: $key');
  }
}

Future<void> main(List<String> args) async {
  final manifestPath = arg(args, '--manifest', 'rust-typed-request-manifest.json');
  final endpoint = arg(args, '--endpoint', '127.0.0.1:50051');
  final output = arg(args, '--output', 'dart-live-receipt.json');
  final timeoutMs = int.parse(arg(args, '--timeout-ms', '5000'));
  final manifest = jsonDecode(await File(manifestPath).readAsString()) as Map<String, dynamic>;
  final split = endpoint.split(':');
  final channel = ClientChannel(split.first, port: int.parse(split.last), options: const ChannelOptions(credentials: ChannelCredentials.insecure()));
  final results = <Map<String, dynamic>>[];
  for (final raw in (manifest['methods'] as List)) {
    final entry = Map<String, dynamic>.from(raw as Map);
    final result = <String, dynamic>{};
    for (final key in ['family', 'package', 'service', 'method', 'path', 'request_type', 'response_type', 'client_streaming', 'server_streaming']) result[key] = entry[key];
    final rawTyped = entry['typed_request'];
    if (rawTyped is! Map) {
      result['status'] = 'pending_missing_typed_request';
      results.add(result);
      continue;
    }
    final typed = Map<String, dynamic>.from(rawTyped);
    final rawHex = typed['serialized_hex'];
    final rawExpectedRequestSha = typed['serialized_sha256'];
    if (!typed.containsKey('serialized_hex') || rawHex is! String || rawExpectedRequestSha is! String) {
      result['status'] = 'pending_missing_typed_request';
      results.add(result);
      continue;
    }
    final hex = rawHex;
    final expectedRequestSha = rawExpectedRequestSha;
    try {
      if (!RegExp(r'^(?:[0-9a-fA-F]{2})*$').hasMatch(hex)) {
        throw FormatException('invalid serialized request hex');
      }
      final bytes = [for (var i = 0; i < hex.length; i += 2) int.parse(hex.substring(i, i + 2), radix: 16)];
      final requestDigest = crypto.sha256.convert(bytes).toString();
      final expectedDigest = expectedRequestSha.replaceFirst(RegExp(r'^sha256:', caseSensitive: false), '').toLowerCase();
      if (expectedDigest != requestDigest) throw FormatException('serialized request hash does not match Rust authority');
      final rawFrames = typed['serialized_frames'];
      final frameHexes = rawFrames is List && rawFrames.isNotEmpty
          ? rawFrames.map((rawFrame) => rawFrame is Map ? rawFrame['serialized_hex'] as String : rawFrame as String).toList()
          : <String>[hex];
      final frameBytes = <List<int>>[];
      final frameDigests = <String>[];
      for (var frameIndex = 0; frameIndex < frameHexes.length; frameIndex++) {
        final frameHex = frameHexes[frameIndex];
        if (!RegExp(r'^(?:[0-9a-fA-F]{2})*$').hasMatch(frameHex)) throw FormatException('invalid serialized request frame hex');
        final frame = [for (var i = 0; i < frameHex.length; i += 2) int.parse(frameHex.substring(i, i + 2), radix: 16)];
        final digest = crypto.sha256.convert(frame).toString();
        if (rawFrames is List && rawFrames.isNotEmpty && rawFrames[frameIndex] is Map) {
          final declared = (rawFrames[frameIndex] as Map)['serialized_sha256'];
          if (declared is String && declared.replaceFirst(RegExp(r'^sha256:', caseSensitive: false), '').toLowerCase() != digest) throw FormatException('serialized request frame hash does not match Rust authority');
        }
        frameBytes.add(frame);
        frameDigests.add(digest);
      }
      result['request_frames_hex'] = frameHexes;
      result['request_frames_sha256'] = frameDigests;
      result['request_frame_type_ids'] = rawFrames is List && rawFrames.isNotEmpty
          ? rawFrames.map((rawFrame) => rawFrame is Map ? rawFrame['type'] : null).toList()
          : <String?>[null];
      final key = '${entry['package']}.${entry['service']}/${entry['method']}';
      final invocation = await invoke(key, channel, frameBytes, Duration(milliseconds: timeoutMs));
      result['request_sha256'] = requestDigest;
      if (invocation is Map && invocation.containsKey('frames')) {
        result['response_frames'] = invocation['frames'];
        result['response_frames_hex'] = invocation['frames_hex'];
        result['response_frame_types'] = invocation['frame_types'];
        result['response_frame_type_ids'] = List<String>.filled((invocation['frames'] as List).length, entry['response_type'] as String);
        result['response_frames_sha256'] = (invocation['frames_hex'] as List).map((value) => wireSha256(value as String)).toList();
      } else {
        result['response'] = invocation['value'];
        result['response_bytes_hex'] = invocation['bytes_hex'];
        result['response_type_observed'] = invocation['type_name'];
        result['response_type_id_observed'] = entry['response_type'];
        result['response_sha256'] = wireSha256(invocation['bytes_hex'] as String);
      }
      result['terminal_status'] = 'ok';
      result['terminal_code'] = 0;
      result['status'] = 'semantic_passed';
    } on GrpcError catch (error) {
      if (error.code == StatusCode.cancelled) {
        result['status'] = 'semantic_passed';
        result['terminal_status'] = 'canceled';
        result['terminal_code'] = error.code.value;
      } else {
        result['status'] = 'error';
        result['terminal_status'] = 'error';
        result['error'] = {'code': error.code, 'message': error.message};
        result['terminal_code'] = error.code.value;
      }
    } catch (error) {
      result['status'] = 'error';
      result['terminal_status'] = 'error';
      result['terminal_code'] = 2;
      result['error'] = {'class': error.runtimeType.toString(), 'message': error.toString()};
    }
    results.add(result);
  }
  await channel.shutdown();
  final receipt = <String, dynamic>{'schema': 'acyclic.sdk.rpd.dart-live-receipt.v1', 'authority': manifest['authority'], 'endpoint': endpoint, 'method_count': results.length, 'passed': results.where((r) => r['status'] == 'semantic_passed').length, 'transport_succeeded': results.where((r) => r['status'] == 'transport_success_pending_semantics').length, 'pending': results.where((r) => r['status'] == 'pending_missing_typed_request').length, 'methods': results};
  await File(output).writeAsString(const JsonEncoder.withIndent('  ').convert(receipt) + '\n');
  stdout.writeln(jsonEncode({'method_count': results.length, 'passed': receipt['passed'], 'transport_succeeded': receipt['transport_succeeded'], 'pending': receipt['pending']}));
}





