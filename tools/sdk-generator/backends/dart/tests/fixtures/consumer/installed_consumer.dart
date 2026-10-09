import 'dart:io';
import 'dart:isolate';
import 'package:acyclic_sdk_transport/actors/v1/actors.pb.dart' as actors;
import 'package:acyclic_sdk_transport/workers/v1/workers.pb.dart' as workers;
import 'package:acyclic_sdk_transport/stream/v1/stream.pb.dart' as stream;
import 'package:acyclic_sdk_transport/actors/v1/actors.pbgrpc.dart' as actorsRpc;
import 'package:acyclic_sdk_transport/workers/v1/workers.pbgrpc.dart' as workersRpc;
import 'package:acyclic_sdk_transport/stream/v1/stream.pbgrpc.dart' as streamRpc;
import 'package:fixnum/fixnum.dart';
import 'package:grpc/service_api.dart' as grpc;
import 'package:grpc/grpc.dart' as clientApi;
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

// Use maintained reflection to populate bounded known fields. The unknown-field
// marker also makes genuinely empty message types carry a nonempty wire sample.
GeneratedMessage populated(GeneratedMessage message, [int depth=0]) {
  for(final field in message.info_.fieldInfo.values) {
    if(PbFieldType.isMapField(field.type)) continue;
    Object value;
    final type=PbFieldType.baseType(field.type);
    if(PbFieldType.isGroupOrMessage(type)) {
      if(depth>=2) continue;
      value=populated(field.subBuilder!(),depth+1);
    } else if(PbFieldType.isEnum(type)) {
      value=field.enumValues!.last;
    } else if(PbFieldType.isBytes(type)) {
      value=<int>[0,255];
    } else if(type==PbFieldType.OPTIONAL_STRING) {
      value='probe';
    } else if(type==PbFieldType.OPTIONAL_BOOL) {
      value=true;
    } else if(type==PbFieldType.OPTIONAL_FLOAT||type==PbFieldType.OPTIONAL_DOUBLE) {
      value=1.25;
    } else if([PbFieldType.OPTIONAL_INT64,PbFieldType.OPTIONAL_SINT64,PbFieldType.OPTIONAL_UINT64,PbFieldType.OPTIONAL_FIXED64,PbFieldType.OPTIONAL_SFIXED64].contains(type)) {
      value=Int64(17);
    } else {
      value=17;
    }
    if(PbFieldType.isRepeated(field.type)) {
      (message.getField(field.tagNumber) as List).add(value);
    } else {
      message.setField(field.tagNumber,value);
    }
  }
  message.unknownFields.mergeVarintField(536870000,Int64(23));
  check(message.writeToBuffer().isNotEmpty,'RPC sample is empty');
  return message;
}

class ProbeCall<Q,R> implements clientApi.ClientCall<Q,R> {
  @override final Stream<R> response;
  ProbeCall(List<R> values):response=Stream.fromIterable(values);
  @override Future<Map<String,String>> get headers async => {};
  @override Future<Map<String,String>> get trailers async => {};
  @override Future<void> cancel() async {}
  @override dynamic noSuchMethod(Invocation invocation) => throw StateError('unexpected client-call API: $invocation');
}

class ProbeChannel implements grpc.ClientChannel {
  final String path;
  final GeneratedMessage request,response;
  final bool serverStreaming;
  int calls=0;
  late Future<void> requestsChecked;
  ProbeChannel(this.path,this.request,this.response,this.serverStreaming);
  @override clientApi.ClientCall<Q,R> createCall<Q,R>(grpc.ClientMethod<Q,R> method,Stream<Q> requests,grpc.CallOptions options) {
    calls++;
    check(method.path==path,'client RPC path differs');
    requestsChecked=requests.toList().then((values) {
      check(values.length==1,'client request sample count differs');
      final value=values.single as GeneratedMessage;
      check(value.info_.qualifiedMessageName==request.info_.qualifiedMessageName,'client request type differs');
      check(sameBytes(method.requestSerializer(values.single),request.writeToBuffer()),'client request serializer discarded or changed content');
    });
    final decoded=method.responseDeserializer(response.writeToBuffer()) as GeneratedMessage;
    check(decoded.info_.qualifiedMessageName==response.info_.qualifiedMessageName,'client response type differs');
    check(sameBytes(decoded.writeToBuffer(),response.writeToBuffer()),'client response decoder discarded or changed content');
    return ProbeCall<Q,R>([decoded as R,if(serverStreaming)decoded as R]);
  }
  @override Future<void> shutdown() async {}
  @override Future<void> terminate() async {}
  @override Stream<clientApi.ConnectionState> get onConnectionStateChanged => const Stream.empty();
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
    check(clientProbes[i].length==schema.method.length,'client RPC method count differs');
    for(final method in schema.method) {
      final rpc=service.registered[method.name];
      check(rpc!=null,'RPC missing: ${method.name}');
      check(rpc!.streamingRequest==method.clientStreaming&&rpc.streamingResponse==method.serverStreaming,'RPC streaming shape differs');
      final request=populated(messageFactories[method.inputType]!());
      final input=rpc.deserialize(request.writeToBuffer()) as GeneratedMessage;
      check('.${input.info_.qualifiedMessageName}'==method.inputType,'RPC input type differs');
      check(sameBytes(input.writeToBuffer(),request.writeToBuffer()),'RPC input decoder discarded or changed content');
      final output=populated(messageFactories[method.outputType]!());
      check(sameBytes(rpc.serialize(output),output.writeToBuffer()),'RPC output serializer discarded or changed content');
      final path='/${file.package}.${schema.name}/${method.name}';
      final client=clientProbes[i][path];
      check(client!=null,'client method path missing');
      check(client!.clientStreaming==method.clientStreaming&&client.serverStreaming==method.serverStreaming,'client streaming signature differs');
      final channel=ProbeChannel(path,request,output,method.serverStreaming);
      final result=client.invoke(channel,request);
      check(method.serverStreaming?result is grpc.ResponseStream:result is grpc.ResponseFuture,'client response shape differs');
      await channel.requestsChecked;
      final responses=method.serverStreaming?await (result as grpc.ResponseStream).toList():[await (result as grpc.ResponseFuture)];
      check(responses.length==(method.serverStreaming?2:1),'client response sample count differs');
      for(final value in responses) check(sameBytes((value as GeneratedMessage).writeToBuffer(),output.writeToBuffer()),'client response content differs');
      check(channel.calls==1,'client did not make exactly one call');
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
