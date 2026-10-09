import 'dart:io';
import 'dart:isolate';
import 'package:acyclic_sdk_transport/actors/v1/actors.pb.dart' as actors;
import 'package:acyclic_sdk_transport/workers/v1/workers.pb.dart' as workers;
import 'package:acyclic_sdk_transport/stream/v2/stream.pb.dart' as stream;
import 'package:acyclic_sdk_transport/actors/v1/actors.pbgrpc.dart' as actorsRpc;
import 'package:acyclic_sdk_transport/workers/v1/workers.pbgrpc.dart' as workersRpc;
import 'package:acyclic_sdk_transport/stream/v2/stream.pbgrpc.dart' as streamRpc;
import 'package:fixnum/fixnum.dart';
import 'package:grpc/service_api.dart' as grpc;
import 'package:protobuf/protobuf.dart';
import 'package:protoc_plugin/src/gen/google/protobuf/descriptor.pb.dart';
import 'reflection.dart';

void check(bool value, String message) { if (!value) throw StateError(message); }
bool sameBytes(List<int> a,List<int> b) => a.length==b.length && List.generate(a.length,(i)=>a[i]==b[i]).every((x)=>x);
mixin Capture on grpc.Service {
  final registered=<String,grpc.ServiceMethod>{};
  @override void $addMethod(grpc.ServiceMethod method) {
    check(!registered.containsKey(method.name),'duplicate RPC registration');
    registered[method.name]=method;
    super.$addMethod(method);
  }
}
class ProbeActors extends actorsRpc.ActorsServiceBase with Capture {
  @override dynamic noSuchMethod(Invocation invocation) => throw UnimplementedError();
}
class ProbeWorkers extends workersRpc.WorkersServiceBase with Capture {
  @override dynamic noSuchMethod(Invocation invocation) => throw UnimplementedError();
}
class ProbeStream extends streamRpc.StreamServiceBase with Capture {
  @override dynamic noSuchMethod(Invocation invocation) => throw UnimplementedError();
}

Future<void> main(List<String> paths) async {
  check(paths.length==3,'pass three verified Rust descriptor files');
  final installed=Directory(Platform.environment['SDK_QUALIFIED_PACKAGE']!).resolveSymbolicLinksSync();
  for(final source in sourceNames) {
    for(final suffix in ['pb.dart','pbenum.dart','pbjson.dart','pbgrpc.dart']) {
      final name=source.substring(0,source.length-6);
      final uri=await Isolate.resolvePackageUri(Uri.parse('package:acyclic_sdk_transport/$name.$suffix'));
      final resolved=uri==null?'':File.fromUri(uri).resolveSymbolicLinksSync();
      final expected=installed+Platform.pathSeparator;
      check(Platform.isWindows?resolved.toLowerCase().startsWith(expected.toLowerCase()):resolved.startsWith(expected),'SDK source does not resolve to fresh installed package: $resolved, expected $expected');
    }
  }
  final services=<Capture>[ProbeActors(),ProbeWorkers(),ProbeStream()];
  for (var i=0;i<paths.length;i++) {
    final files=FileDescriptorSet.fromBuffer(File(paths[i]).readAsBytesSync()).file;
    final file=files.singleWhere((f)=>f.name==sourceNames[i]);
    final actual=descriptors[i];
    final expected=<String,List<int>>{};
    for(final message in file.messageType) expected[message.name]=message.writeToBuffer();
    for(final enumeration in file.enumType) expected[enumeration.name]=enumeration.writeToBuffer();
    check(actual.length==expected.length,'descriptor count differs');
    for(final entry in expected.entries) check(actual.containsKey(entry.key)&&sameBytes(actual[entry.key]!,entry.value),'descriptor differs: ${file.name}:${entry.key}');
    final service=services[i];
    final schema=file.service.single;
    check((service as grpc.Service).$name=='${file.package}.${schema.name}','RPC service name differs');
    check(service.registered.length==schema.method.length,'RPC method count differs');
    for(final method in schema.method) {
      final rpc=service.registered[method.name];
      check(rpc!=null,'RPC missing: ${method.name}');
      check(rpc!.streamingRequest==method.clientStreaming&&rpc.streamingResponse==method.serverStreaming,'RPC streaming shape differs');
      final input=rpc.deserialize(<int>[]) as GeneratedMessage;
      check('.${input.info_.qualifiedMessageName}'==method.inputType,'RPC input type differs');
      final output=messageFactories[method.outputType]!();
      check(sameBytes(rpc.serialize(output),output.writeToBuffer()),'RPC output serializer differs');
    }
  }
  final bytes=<int>[0,255];
  final actor=actors.CreateActorRequest(codeSha256:bytes,homeRegion:'test',idempotencyKey:'test');
  final worker=workers.PublishVersionRequest(javascriptModule:bytes,expectedSha256:bytes,idempotencyKey:'test');
  final append=stream.AppendRequest(path:'test/path',records:[bytes,<int>[]],ifTail:Int64(-1),idempotencyKey:bytes);
  final read=stream.ReadRequest(path:'test/path',from:Int64(-1),limit:4294967295);
  check(sameBytes(actor.codeSha256,bytes),'actor bytes changed');
  check(sameBytes(worker.javascriptModule,bytes)&&sameBytes(worker.expectedSha256,bytes),'worker bytes changed');
  check(append.records.length==2&&sameBytes(append.records[0],bytes)&&append.records[1].isEmpty&&sameBytes(append.idempotencyKey,bytes),'stream bytes changed');
  check(append.ifTail.toStringUnsigned()=='18446744073709551615'&&read.from.toStringUnsigned()=='18446744073709551615','uint64 maximum bits changed');
  check(read.limit==4294967295,'uint32 maximum changed');
  check(actors.CreateActorRequest.fromBuffer(actor.writeToBuffer())==actor,'actor round trip differs');
  check(workers.PublishVersionRequest.fromBuffer(worker.writeToBuffer())==worker,'worker round trip differs');
  check(stream.AppendRequest.fromBuffer(append.writeToBuffer())==append,'append round trip differs');
  check(stream.ReadRequest.fromBuffer(read.writeToBuffer())==read,'read round trip differs');
  final absent=stream.AppendRequest(),explicit=stream.AppendRequest(ifTail:Int64.ZERO);
  check(!absent.hasIfTail()&&explicit.hasIfTail(),'optional presence lost');
  check(absent.writeToBuffer().isEmpty&&sameBytes(explicit.writeToBuffer(),[24,0]),'optional zero wire encoding lost');
  check(sameBytes(stream.AppendRequest(ifTail:Int64(-1)).writeToBuffer(),[24,255,255,255,255,255,255,255,255,255,1]),'uint64 wire encoding changed');
  final target=workers.JobTarget(versionSha256:bytes);
  check(target.whichTarget()==workers.JobTarget_Target.versionSha256&&target.deploymentAlias.isEmpty,'version branch lost');
  target.deploymentAlias='test-alias';
  check(target.whichTarget()==workers.JobTarget_Target.deploymentAlias&&target.versionSha256.isEmpty,'alias did not clear version');
  check(workers.JobTarget.fromBuffer(target.writeToBuffer())==target,'alias wire branch lost');
  target.versionSha256=bytes;
  check(target.whichTarget()==workers.JobTarget_Target.versionSha256&&target.deploymentAlias.isEmpty,'version did not clear alias');
  check(workers.JobTarget.fromBuffer(target.writeToBuffer())==target,'version wire branch lost');
  target.clearTarget();
  check(target.whichTarget()==workers.JobTarget_Target.notSet&&target.versionSha256.isEmpty&&target.deploymentAlias.isEmpty,'oneof clear lost');
  print('PASS: installed Dart descriptors, bytes, unsigned bits, optional presence, oneof and gRPC shapes');
}
