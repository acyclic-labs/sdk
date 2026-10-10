import Foundation
import SwiftProtobuf
import GRPCCore
import GRPCInProcessTransport
import AcyclicTransport

enum RPCControlFailure: Error { case failed(String) }
func rpcRequire(_ condition: Bool, _ message: String) throws {
  if !condition { throw RPCControlFailure.failed(message) }
}
struct RPCSchema: Sendable {
  let messages: [String: Google_Protobuf_DescriptorProto]
  let enums: [String: Google_Protobuf_EnumDescriptorProto]
  let methods: [String: Google_Protobuf_MethodDescriptorProto]
  init(_ paths: [String]) throws {
    var messages: [String: Google_Protobuf_DescriptorProto] = [:]
    var enums: [String: Google_Protobuf_EnumDescriptorProto] = [:]
    var methods: [String: Google_Protobuf_MethodDescriptorProto] = [:]
    func visit(_ message: Google_Protobuf_DescriptorProto, prefix: String) {
      let name = prefix + "." + message.name
      messages[name] = message
      for value in message.enumType { enums[name + "." + value.name] = value }
      for child in message.nestedType { visit(child, prefix: name) }
    }
    for path in paths {
      let set = try Google_Protobuf_FileDescriptorSet(serializedBytes: Data(contentsOf: URL(fileURLWithPath: path)))
      for file in set.file {
        for message in file.messageType { visit(message, prefix: "." + file.package) }
        for value in file.enumType { enums["." + file.package + "." + value.name] = value }
        for service in file.service {
          for method in service.method { methods[file.package + "." + service.name + "/" + method.name] = method }
        }
      }
    }
    self.messages = messages; self.enums = enums; self.methods = methods
    try rpcRequire(methods.count == 25, "Rust descriptor method inventory differs")
  }
  func check<I: SwiftProtobuf.Message, O: SwiftProtobuf.Message>(
    _ path: String, input: I.Type, output: O.Type, streaming: Bool, descriptor: MethodDescriptor
  ) throws {
    guard let method = methods[path] else { throw RPCControlFailure.failed("method absent from Rust descriptors: " + path) }
    try rpcRequire(method.inputType == "." + I.protoMessageName && method.outputType == "." + O.protoMessageName,
      "RPC message types differ from Rust descriptors: " + path)
    try rpcRequire(!method.clientStreaming && method.serverStreaming == streaming, "RPC streaming shape differs: " + path)
    try rpcRequire(descriptor.fullyQualifiedMethod == path && descriptor.type == (streaming ? .serverStreaming : .unary),
      "generated RPC metadata differs: " + path)
  }
  func seeded<M: SwiftProtobuf.Message>(_ type: M.Type, tag: UInt64) throws -> M {
    let (wire, json) = try value("." + M.protoMessageName, tag: tag, depth: 0)
    let message = try M(serializedBytes: wire)
    let actual = try JSONSerialization.jsonObject(with: message.jsonUTF8Data()) as? NSDictionary
    try rpcRequire(actual == json as NSDictionary, "generated fields/JSON differ from Rust descriptor values: " + M.protoMessageName)
    try rpcRequire(message.unknownFields.data.isEmpty, "generated message lacks Rust fields: " + M.protoMessageName)
    return message
  }
  private func value(_ name: String, tag: UInt64, depth: Int) throws -> (Data, [String: Any]) {
    guard let message = messages[name] else { throw RPCControlFailure.failed("Rust message absent: " + name) }
    var wire = Data(), json: [String: Any] = [:], oneofs = Set<Int32>()
    for field in message.field {
      if field.hasOneofIndex && !oneofs.insert(field.oneofIndex).inserted { continue }
      if field.type == .message && depth >= 4 { continue }
      var body = Data(), jsonValue: Any, kind: UInt64
      switch field.type {
      case .string:
        let text = "probe-\(tag)-\(field.name)"; body = Data(text.utf8); jsonValue = text; kind = 2
      case .bytes:
        body = Data([0, 255, UInt8(tag)]); jsonValue = body.base64EncodedString(); kind = 2
      case .uint64:
        let number = UInt64.max - tag; body = varint(number); jsonValue = String(number); kind = 0
      case .uint32:
        let number = UInt32.max - UInt32(tag); body = varint(UInt64(number)); jsonValue = number; kind = 0
      case .bool:
        body = varint(1); jsonValue = true; kind = 0
      case .enum:
        guard let descriptor = enums[field.typeName], let selected = descriptor.value.first(where: { $0.number != 0 && $0.number > 0 }) else {
          throw RPCControlFailure.failed("Rust enum lacks populated probe: " + field.typeName)
        }
        body = varint(UInt64(selected.number)); jsonValue = selected.name; kind = 0
      case .message:
        let nested = try value(field.typeName, tag: tag, depth: depth + 1)
        body = nested.0; jsonValue = nested.1; kind = 2
      default:
        throw RPCControlFailure.failed("unimplemented Rust field probe: " + field.name + " " + String(describing: field.type))
      }
      let count = field.label == .repeated ? 2 : 1
      for _ in 0..<count {
        wire.append(varint(UInt64(field.number) << 3 | kind))
        if kind == 2 { wire.append(varint(UInt64(body.count))) }
        wire.append(body)
      }
      json[field.jsonName] = count == 2 ? [jsonValue, jsonValue] : jsonValue
    }
    return (wire, json)
  }
}
func varint(_ input: UInt64) -> Data {
  var number = input, bytes = Data()
  while number > 127 { bytes.append(UInt8(number & 127) | 128); number >>= 7 }
  bytes.append(UInt8(number)); return bytes
}

struct Probe0: Acyclic_Actors_V1_ActorsService.SimpleServiceProtocol {
 let schema: RPCSchema
 func createActor(request: Acyclic_Actors_V1_CreateActorRequest, context: ServerContext) async throws -> Acyclic_Actors_V1_CreateActorResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/CreateActor", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Actors_V1_CreateActorRequest.self, tag: 1), "native request payload differs: acyclic.actors.v1.ActorsService/CreateActor")
 return try schema.seeded(Acyclic_Actors_V1_CreateActorResponse.self, tag: 2)
 }
 func updateActor(request: Acyclic_Actors_V1_UpdateActorRequest, context: ServerContext) async throws -> Acyclic_Actors_V1_UpdateActorResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/UpdateActor", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Actors_V1_UpdateActorRequest.self, tag: 1), "native request payload differs: acyclic.actors.v1.ActorsService/UpdateActor")
 return try schema.seeded(Acyclic_Actors_V1_UpdateActorResponse.self, tag: 2)
 }
 func inspectActor(request: Acyclic_Actors_V1_InspectActorRequest, context: ServerContext) async throws -> Acyclic_Actors_V1_InspectActorResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/InspectActor", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Actors_V1_InspectActorRequest.self, tag: 1), "native request payload differs: acyclic.actors.v1.ActorsService/InspectActor")
 return try schema.seeded(Acyclic_Actors_V1_InspectActorResponse.self, tag: 2)
 }
 func addSubscription(request: Acyclic_Actors_V1_AddSubscriptionRequest, context: ServerContext) async throws -> Acyclic_Actors_V1_AddSubscriptionResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/AddSubscription", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Actors_V1_AddSubscriptionRequest.self, tag: 1), "native request payload differs: acyclic.actors.v1.ActorsService/AddSubscription")
 return try schema.seeded(Acyclic_Actors_V1_AddSubscriptionResponse.self, tag: 2)
 }
 func removeSubscription(request: Acyclic_Actors_V1_RemoveSubscriptionRequest, context: ServerContext) async throws -> Acyclic_Actors_V1_RemoveSubscriptionResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/RemoveSubscription", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Actors_V1_RemoveSubscriptionRequest.self, tag: 1), "native request payload differs: acyclic.actors.v1.ActorsService/RemoveSubscription")
 return try schema.seeded(Acyclic_Actors_V1_RemoveSubscriptionResponse.self, tag: 2)
 }
 func resumeSubscription(request: Acyclic_Actors_V1_ResumeSubscriptionRequest, context: ServerContext) async throws -> Acyclic_Actors_V1_ResumeSubscriptionResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/ResumeSubscription", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Actors_V1_ResumeSubscriptionRequest.self, tag: 1), "native request payload differs: acyclic.actors.v1.ActorsService/ResumeSubscription")
 return try schema.seeded(Acyclic_Actors_V1_ResumeSubscriptionResponse.self, tag: 2)
 }
 func checkpointActor(request: Acyclic_Actors_V1_CheckpointActorRequest, context: ServerContext) async throws -> Acyclic_Actors_V1_CheckpointActorResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/CheckpointActor", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Actors_V1_CheckpointActorRequest.self, tag: 1), "native request payload differs: acyclic.actors.v1.ActorsService/CheckpointActor")
 return try schema.seeded(Acyclic_Actors_V1_CheckpointActorResponse.self, tag: 2)
 }
 func invokeActor(request: Acyclic_Actors_V1_InvokeActorRequest, context: ServerContext) async throws -> Acyclic_Actors_V1_InvokeActorResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/InvokeActor", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Actors_V1_InvokeActorRequest.self, tag: 1), "native request payload differs: acyclic.actors.v1.ActorsService/InvokeActor")
 return try schema.seeded(Acyclic_Actors_V1_InvokeActorResponse.self, tag: 2)
 }
}

struct Probe1: Acyclic_Workers_V1_WorkersService.SimpleServiceProtocol {
 let schema: RPCSchema
 func publishVersion(request: Acyclic_Workers_V1_PublishVersionRequest, context: ServerContext) async throws -> Acyclic_Workers_V1_PublishVersionResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.workers.v1.WorkersService/PublishVersion", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Workers_V1_PublishVersionRequest.self, tag: 1), "native request payload differs: acyclic.workers.v1.WorkersService/PublishVersion")
 return try schema.seeded(Acyclic_Workers_V1_PublishVersionResponse.self, tag: 2)
 }
 func selectDeployment(request: Acyclic_Workers_V1_SelectDeploymentRequest, context: ServerContext) async throws -> Acyclic_Workers_V1_SelectDeploymentResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.workers.v1.WorkersService/SelectDeployment", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Workers_V1_SelectDeploymentRequest.self, tag: 1), "native request payload differs: acyclic.workers.v1.WorkersService/SelectDeployment")
 return try schema.seeded(Acyclic_Workers_V1_SelectDeploymentResponse.self, tag: 2)
 }
 func submitJob(request: Acyclic_Workers_V1_SubmitJobRequest, context: ServerContext) async throws -> Acyclic_Workers_V1_SubmitJobResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.workers.v1.WorkersService/SubmitJob", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Workers_V1_SubmitJobRequest.self, tag: 1), "native request payload differs: acyclic.workers.v1.WorkersService/SubmitJob")
 return try schema.seeded(Acyclic_Workers_V1_SubmitJobResponse.self, tag: 2)
 }
 func inspectJob(request: Acyclic_Workers_V1_InspectJobRequest, context: ServerContext) async throws -> Acyclic_Workers_V1_InspectJobResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.workers.v1.WorkersService/InspectJob", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Workers_V1_InspectJobRequest.self, tag: 1), "native request payload differs: acyclic.workers.v1.WorkersService/InspectJob")
 return try schema.seeded(Acyclic_Workers_V1_InspectJobResponse.self, tag: 2)
 }
 func cancelJob(request: Acyclic_Workers_V1_CancelJobRequest, context: ServerContext) async throws -> Acyclic_Workers_V1_CancelJobResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.workers.v1.WorkersService/CancelJob", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Workers_V1_CancelJobRequest.self, tag: 1), "native request payload differs: acyclic.workers.v1.WorkersService/CancelJob")
 return try schema.seeded(Acyclic_Workers_V1_CancelJobResponse.self, tag: 2)
 }
 func invokeVersion(request: Acyclic_Workers_V1_InvokeVersionRequest, context: ServerContext) async throws -> Acyclic_Workers_V1_InvokeResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.workers.v1.WorkersService/InvokeVersion", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Workers_V1_InvokeVersionRequest.self, tag: 1), "native request payload differs: acyclic.workers.v1.WorkersService/InvokeVersion")
 return try schema.seeded(Acyclic_Workers_V1_InvokeResponse.self, tag: 2)
 }
 func invokeDeployment(request: Acyclic_Workers_V1_InvokeDeploymentRequest, context: ServerContext) async throws -> Acyclic_Workers_V1_InvokeResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.workers.v1.WorkersService/InvokeDeployment", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Workers_V1_InvokeDeploymentRequest.self, tag: 1), "native request payload differs: acyclic.workers.v1.WorkersService/InvokeDeployment")
 return try schema.seeded(Acyclic_Workers_V1_InvokeResponse.self, tag: 2)
 }
}

struct Probe2: Acyclic_Stream_V1_StreamService.SimpleServiceProtocol {
 let schema: RPCSchema
 func inspectIdempotency(request: Acyclic_Stream_V1_InspectIdempotencyRequest, context: ServerContext) async throws -> Acyclic_Stream_V1_InspectIdempotencyResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/InspectIdempotency", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_InspectIdempotencyRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/InspectIdempotency")
 return try schema.seeded(Acyclic_Stream_V1_InspectIdempotencyResponse.self, tag: 2)
 }
 func append(request: Acyclic_Stream_V1_AppendRequest, context: ServerContext) async throws -> Acyclic_Stream_V1_AppendResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/Append", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_AppendRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/Append")
 return try schema.seeded(Acyclic_Stream_V1_AppendResponse.self, tag: 2)
 }
 func tail(request: Acyclic_Stream_V1_TailRequest, context: ServerContext) async throws -> Acyclic_Stream_V1_TailResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/Tail", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_TailRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/Tail")
 return try schema.seeded(Acyclic_Stream_V1_TailResponse.self, tag: 2)
 }
 func fork(request: Acyclic_Stream_V1_ForkRequest, context: ServerContext) async throws -> Acyclic_Stream_V1_ForkReceipt {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/Fork", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_ForkRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/Fork")
 return try schema.seeded(Acyclic_Stream_V1_ForkReceipt.self, tag: 2)
 }
 func read(request: Acyclic_Stream_V1_ReadRequest, response: RPCWriter<Acyclic_Stream_V1_ReadResponse>, context: ServerContext) async throws {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/Read", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_ReadRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/Read")
 try await response.write(schema.seeded(Acyclic_Stream_V1_ReadResponse.self, tag: 2))
 try await response.write(schema.seeded(Acyclic_Stream_V1_ReadResponse.self, tag: 3))
 }
 func follow(request: Acyclic_Stream_V1_FollowRequest, response: RPCWriter<Acyclic_Stream_V1_ReadResponse>, context: ServerContext) async throws {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/Follow", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_FollowRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/Follow")
 try await response.write(schema.seeded(Acyclic_Stream_V1_ReadResponse.self, tag: 2))
 try await response.write(schema.seeded(Acyclic_Stream_V1_ReadResponse.self, tag: 3))
 }
 func children(request: Acyclic_Stream_V1_ChildrenRequest, response: RPCWriter<Acyclic_Stream_V1_ChildrenResponse>, context: ServerContext) async throws {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/Children", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_ChildrenRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/Children")
 try await response.write(schema.seeded(Acyclic_Stream_V1_ChildrenResponse.self, tag: 2))
 try await response.write(schema.seeded(Acyclic_Stream_V1_ChildrenResponse.self, tag: 3))
 }
 func childrenPage(request: Acyclic_Stream_V1_ChildrenPageRequest, context: ServerContext) async throws -> Acyclic_Stream_V1_ChildrenPageResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/ChildrenPage", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_ChildrenPageRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/ChildrenPage")
 return try schema.seeded(Acyclic_Stream_V1_ChildrenPageResponse.self, tag: 2)
 }
 func commit(request: Acyclic_Stream_V1_CommitRequest, context: ServerContext) async throws -> Acyclic_Stream_V1_CommitResponse {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/Commit", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_CommitRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/Commit")
 return try schema.seeded(Acyclic_Stream_V1_CommitResponse.self, tag: 2)
 }
 func readCommit(request: Acyclic_Stream_V1_ReadCommitRequest, context: ServerContext) async throws -> Acyclic_Stream_V1_CommittedEnvelope {
 try rpcRequire(context.descriptor.fullyQualifiedMethod == "acyclic.stream.v1.StreamService/ReadCommit", "actual server method path differs")
 try rpcRequire(request == schema.seeded(Acyclic_Stream_V1_ReadCommitRequest.self, tag: 1), "native request payload differs: acyclic.stream.v1.StreamService/ReadCommit")
 return try schema.seeded(Acyclic_Stream_V1_CommittedEnvelope.self, tag: 2)
 }
}

func runRPCControls(_ schema: RPCSchema) async throws {
try schema.check("acyclic.actors.v1.ActorsService/CreateActor", input: Acyclic_Actors_V1_CreateActorRequest.self, output: Acyclic_Actors_V1_CreateActorResponse.self, streaming: false, descriptor: Acyclic_Actors_V1_ActorsService.Method.CreateActor.descriptor)
try schema.check("acyclic.actors.v1.ActorsService/UpdateActor", input: Acyclic_Actors_V1_UpdateActorRequest.self, output: Acyclic_Actors_V1_UpdateActorResponse.self, streaming: false, descriptor: Acyclic_Actors_V1_ActorsService.Method.UpdateActor.descriptor)
try schema.check("acyclic.actors.v1.ActorsService/InspectActor", input: Acyclic_Actors_V1_InspectActorRequest.self, output: Acyclic_Actors_V1_InspectActorResponse.self, streaming: false, descriptor: Acyclic_Actors_V1_ActorsService.Method.InspectActor.descriptor)
try schema.check("acyclic.actors.v1.ActorsService/AddSubscription", input: Acyclic_Actors_V1_AddSubscriptionRequest.self, output: Acyclic_Actors_V1_AddSubscriptionResponse.self, streaming: false, descriptor: Acyclic_Actors_V1_ActorsService.Method.AddSubscription.descriptor)
try schema.check("acyclic.actors.v1.ActorsService/RemoveSubscription", input: Acyclic_Actors_V1_RemoveSubscriptionRequest.self, output: Acyclic_Actors_V1_RemoveSubscriptionResponse.self, streaming: false, descriptor: Acyclic_Actors_V1_ActorsService.Method.RemoveSubscription.descriptor)
try schema.check("acyclic.actors.v1.ActorsService/ResumeSubscription", input: Acyclic_Actors_V1_ResumeSubscriptionRequest.self, output: Acyclic_Actors_V1_ResumeSubscriptionResponse.self, streaming: false, descriptor: Acyclic_Actors_V1_ActorsService.Method.ResumeSubscription.descriptor)
try schema.check("acyclic.actors.v1.ActorsService/CheckpointActor", input: Acyclic_Actors_V1_CheckpointActorRequest.self, output: Acyclic_Actors_V1_CheckpointActorResponse.self, streaming: false, descriptor: Acyclic_Actors_V1_ActorsService.Method.CheckpointActor.descriptor)
try schema.check("acyclic.actors.v1.ActorsService/InvokeActor", input: Acyclic_Actors_V1_InvokeActorRequest.self, output: Acyclic_Actors_V1_InvokeActorResponse.self, streaming: false, descriptor: Acyclic_Actors_V1_ActorsService.Method.InvokeActor.descriptor)
try rpcRequire(Set(Acyclic_Actors_V1_ActorsService.Method.descriptors.map(\.fullyQualifiedMethod)) == Set(["acyclic.actors.v1.ActorsService/CreateActor", "acyclic.actors.v1.ActorsService/UpdateActor", "acyclic.actors.v1.ActorsService/InspectActor", "acyclic.actors.v1.ActorsService/AddSubscription", "acyclic.actors.v1.ActorsService/RemoveSubscription", "acyclic.actors.v1.ActorsService/ResumeSubscription", "acyclic.actors.v1.ActorsService/CheckpointActor", "acyclic.actors.v1.ActorsService/InvokeActor"]), "generated service method inventory differs")
try schema.check("acyclic.workers.v1.WorkersService/PublishVersion", input: Acyclic_Workers_V1_PublishVersionRequest.self, output: Acyclic_Workers_V1_PublishVersionResponse.self, streaming: false, descriptor: Acyclic_Workers_V1_WorkersService.Method.PublishVersion.descriptor)
try schema.check("acyclic.workers.v1.WorkersService/SelectDeployment", input: Acyclic_Workers_V1_SelectDeploymentRequest.self, output: Acyclic_Workers_V1_SelectDeploymentResponse.self, streaming: false, descriptor: Acyclic_Workers_V1_WorkersService.Method.SelectDeployment.descriptor)
try schema.check("acyclic.workers.v1.WorkersService/SubmitJob", input: Acyclic_Workers_V1_SubmitJobRequest.self, output: Acyclic_Workers_V1_SubmitJobResponse.self, streaming: false, descriptor: Acyclic_Workers_V1_WorkersService.Method.SubmitJob.descriptor)
try schema.check("acyclic.workers.v1.WorkersService/InspectJob", input: Acyclic_Workers_V1_InspectJobRequest.self, output: Acyclic_Workers_V1_InspectJobResponse.self, streaming: false, descriptor: Acyclic_Workers_V1_WorkersService.Method.InspectJob.descriptor)
try schema.check("acyclic.workers.v1.WorkersService/CancelJob", input: Acyclic_Workers_V1_CancelJobRequest.self, output: Acyclic_Workers_V1_CancelJobResponse.self, streaming: false, descriptor: Acyclic_Workers_V1_WorkersService.Method.CancelJob.descriptor)
try schema.check("acyclic.workers.v1.WorkersService/InvokeVersion", input: Acyclic_Workers_V1_InvokeVersionRequest.self, output: Acyclic_Workers_V1_InvokeResponse.self, streaming: false, descriptor: Acyclic_Workers_V1_WorkersService.Method.InvokeVersion.descriptor)
try schema.check("acyclic.workers.v1.WorkersService/InvokeDeployment", input: Acyclic_Workers_V1_InvokeDeploymentRequest.self, output: Acyclic_Workers_V1_InvokeResponse.self, streaming: false, descriptor: Acyclic_Workers_V1_WorkersService.Method.InvokeDeployment.descriptor)
try rpcRequire(Set(Acyclic_Workers_V1_WorkersService.Method.descriptors.map(\.fullyQualifiedMethod)) == Set(["acyclic.workers.v1.WorkersService/PublishVersion", "acyclic.workers.v1.WorkersService/SelectDeployment", "acyclic.workers.v1.WorkersService/SubmitJob", "acyclic.workers.v1.WorkersService/InspectJob", "acyclic.workers.v1.WorkersService/CancelJob", "acyclic.workers.v1.WorkersService/InvokeVersion", "acyclic.workers.v1.WorkersService/InvokeDeployment"]), "generated service method inventory differs")
try schema.check("acyclic.stream.v1.StreamService/InspectIdempotency", input: Acyclic_Stream_V1_InspectIdempotencyRequest.self, output: Acyclic_Stream_V1_InspectIdempotencyResponse.self, streaming: false, descriptor: Acyclic_Stream_V1_StreamService.Method.InspectIdempotency.descriptor)
try schema.check("acyclic.stream.v1.StreamService/Append", input: Acyclic_Stream_V1_AppendRequest.self, output: Acyclic_Stream_V1_AppendResponse.self, streaming: false, descriptor: Acyclic_Stream_V1_StreamService.Method.Append.descriptor)
try schema.check("acyclic.stream.v1.StreamService/Tail", input: Acyclic_Stream_V1_TailRequest.self, output: Acyclic_Stream_V1_TailResponse.self, streaming: false, descriptor: Acyclic_Stream_V1_StreamService.Method.Tail.descriptor)
try schema.check("acyclic.stream.v1.StreamService/Fork", input: Acyclic_Stream_V1_ForkRequest.self, output: Acyclic_Stream_V1_ForkReceipt.self, streaming: false, descriptor: Acyclic_Stream_V1_StreamService.Method.Fork.descriptor)
try schema.check("acyclic.stream.v1.StreamService/Read", input: Acyclic_Stream_V1_ReadRequest.self, output: Acyclic_Stream_V1_ReadResponse.self, streaming: true, descriptor: Acyclic_Stream_V1_StreamService.Method.Read.descriptor)
try schema.check("acyclic.stream.v1.StreamService/Follow", input: Acyclic_Stream_V1_FollowRequest.self, output: Acyclic_Stream_V1_ReadResponse.self, streaming: true, descriptor: Acyclic_Stream_V1_StreamService.Method.Follow.descriptor)
try schema.check("acyclic.stream.v1.StreamService/Children", input: Acyclic_Stream_V1_ChildrenRequest.self, output: Acyclic_Stream_V1_ChildrenResponse.self, streaming: true, descriptor: Acyclic_Stream_V1_StreamService.Method.Children.descriptor)
try schema.check("acyclic.stream.v1.StreamService/ChildrenPage", input: Acyclic_Stream_V1_ChildrenPageRequest.self, output: Acyclic_Stream_V1_ChildrenPageResponse.self, streaming: false, descriptor: Acyclic_Stream_V1_StreamService.Method.ChildrenPage.descriptor)
try schema.check("acyclic.stream.v1.StreamService/Commit", input: Acyclic_Stream_V1_CommitRequest.self, output: Acyclic_Stream_V1_CommitResponse.self, streaming: false, descriptor: Acyclic_Stream_V1_StreamService.Method.Commit.descriptor)
try schema.check("acyclic.stream.v1.StreamService/ReadCommit", input: Acyclic_Stream_V1_ReadCommitRequest.self, output: Acyclic_Stream_V1_CommittedEnvelope.self, streaming: false, descriptor: Acyclic_Stream_V1_StreamService.Method.ReadCommit.descriptor)
try rpcRequire(Set(Acyclic_Stream_V1_StreamService.Method.descriptors.map(\.fullyQualifiedMethod)) == Set(["acyclic.stream.v1.StreamService/InspectIdempotency", "acyclic.stream.v1.StreamService/Append", "acyclic.stream.v1.StreamService/Tail", "acyclic.stream.v1.StreamService/Fork", "acyclic.stream.v1.StreamService/Read", "acyclic.stream.v1.StreamService/Follow", "acyclic.stream.v1.StreamService/Children", "acyclic.stream.v1.StreamService/ChildrenPage", "acyclic.stream.v1.StreamService/Commit", "acyclic.stream.v1.StreamService/ReadCommit"]), "generated service method inventory differs")
let transport = InProcessTransport()
try await withGRPCServer(transport: transport.server, services: [Probe0(schema: schema), Probe1(schema: schema), Probe2(schema: schema)]) { _ in
try await withGRPCClient(transport: transport.client) { client in
let api0 = Acyclic_Actors_V1_ActorsService.Client(wrapping: client)
let result1: Acyclic_Actors_V1_CreateActorResponse = try await api0.createActor(request: .init(message: schema.seeded(Acyclic_Actors_V1_CreateActorRequest.self, tag: 1)))
try rpcRequire(result1 == schema.seeded(Acyclic_Actors_V1_CreateActorResponse.self, tag: 2), "native response payload differs: acyclic.actors.v1.ActorsService/CreateActor")
let result2: Acyclic_Actors_V1_UpdateActorResponse = try await api0.updateActor(request: .init(message: schema.seeded(Acyclic_Actors_V1_UpdateActorRequest.self, tag: 1)))
try rpcRequire(result2 == schema.seeded(Acyclic_Actors_V1_UpdateActorResponse.self, tag: 2), "native response payload differs: acyclic.actors.v1.ActorsService/UpdateActor")
let result3: Acyclic_Actors_V1_InspectActorResponse = try await api0.inspectActor(request: .init(message: schema.seeded(Acyclic_Actors_V1_InspectActorRequest.self, tag: 1)))
try rpcRequire(result3 == schema.seeded(Acyclic_Actors_V1_InspectActorResponse.self, tag: 2), "native response payload differs: acyclic.actors.v1.ActorsService/InspectActor")
let result4: Acyclic_Actors_V1_AddSubscriptionResponse = try await api0.addSubscription(request: .init(message: schema.seeded(Acyclic_Actors_V1_AddSubscriptionRequest.self, tag: 1)))
try rpcRequire(result4 == schema.seeded(Acyclic_Actors_V1_AddSubscriptionResponse.self, tag: 2), "native response payload differs: acyclic.actors.v1.ActorsService/AddSubscription")
let result5: Acyclic_Actors_V1_RemoveSubscriptionResponse = try await api0.removeSubscription(request: .init(message: schema.seeded(Acyclic_Actors_V1_RemoveSubscriptionRequest.self, tag: 1)))
try rpcRequire(result5 == schema.seeded(Acyclic_Actors_V1_RemoveSubscriptionResponse.self, tag: 2), "native response payload differs: acyclic.actors.v1.ActorsService/RemoveSubscription")
let result6: Acyclic_Actors_V1_ResumeSubscriptionResponse = try await api0.resumeSubscription(request: .init(message: schema.seeded(Acyclic_Actors_V1_ResumeSubscriptionRequest.self, tag: 1)))
try rpcRequire(result6 == schema.seeded(Acyclic_Actors_V1_ResumeSubscriptionResponse.self, tag: 2), "native response payload differs: acyclic.actors.v1.ActorsService/ResumeSubscription")
let result7: Acyclic_Actors_V1_CheckpointActorResponse = try await api0.checkpointActor(request: .init(message: schema.seeded(Acyclic_Actors_V1_CheckpointActorRequest.self, tag: 1)))
try rpcRequire(result7 == schema.seeded(Acyclic_Actors_V1_CheckpointActorResponse.self, tag: 2), "native response payload differs: acyclic.actors.v1.ActorsService/CheckpointActor")
let result8: Acyclic_Actors_V1_InvokeActorResponse = try await api0.invokeActor(request: .init(message: schema.seeded(Acyclic_Actors_V1_InvokeActorRequest.self, tag: 1)))
try rpcRequire(result8 == schema.seeded(Acyclic_Actors_V1_InvokeActorResponse.self, tag: 2), "native response payload differs: acyclic.actors.v1.ActorsService/InvokeActor")
let api1 = Acyclic_Workers_V1_WorkersService.Client(wrapping: client)
let result10: Acyclic_Workers_V1_PublishVersionResponse = try await api1.publishVersion(request: .init(message: schema.seeded(Acyclic_Workers_V1_PublishVersionRequest.self, tag: 1)))
try rpcRequire(result10 == schema.seeded(Acyclic_Workers_V1_PublishVersionResponse.self, tag: 2), "native response payload differs: acyclic.workers.v1.WorkersService/PublishVersion")
let result11: Acyclic_Workers_V1_SelectDeploymentResponse = try await api1.selectDeployment(request: .init(message: schema.seeded(Acyclic_Workers_V1_SelectDeploymentRequest.self, tag: 1)))
try rpcRequire(result11 == schema.seeded(Acyclic_Workers_V1_SelectDeploymentResponse.self, tag: 2), "native response payload differs: acyclic.workers.v1.WorkersService/SelectDeployment")
let result12: Acyclic_Workers_V1_SubmitJobResponse = try await api1.submitJob(request: .init(message: schema.seeded(Acyclic_Workers_V1_SubmitJobRequest.self, tag: 1)))
try rpcRequire(result12 == schema.seeded(Acyclic_Workers_V1_SubmitJobResponse.self, tag: 2), "native response payload differs: acyclic.workers.v1.WorkersService/SubmitJob")
let result13: Acyclic_Workers_V1_InspectJobResponse = try await api1.inspectJob(request: .init(message: schema.seeded(Acyclic_Workers_V1_InspectJobRequest.self, tag: 1)))
try rpcRequire(result13 == schema.seeded(Acyclic_Workers_V1_InspectJobResponse.self, tag: 2), "native response payload differs: acyclic.workers.v1.WorkersService/InspectJob")
let result14: Acyclic_Workers_V1_CancelJobResponse = try await api1.cancelJob(request: .init(message: schema.seeded(Acyclic_Workers_V1_CancelJobRequest.self, tag: 1)))
try rpcRequire(result14 == schema.seeded(Acyclic_Workers_V1_CancelJobResponse.self, tag: 2), "native response payload differs: acyclic.workers.v1.WorkersService/CancelJob")
let result15: Acyclic_Workers_V1_InvokeResponse = try await api1.invokeVersion(request: .init(message: schema.seeded(Acyclic_Workers_V1_InvokeVersionRequest.self, tag: 1)))
try rpcRequire(result15 == schema.seeded(Acyclic_Workers_V1_InvokeResponse.self, tag: 2), "native response payload differs: acyclic.workers.v1.WorkersService/InvokeVersion")
let result16: Acyclic_Workers_V1_InvokeResponse = try await api1.invokeDeployment(request: .init(message: schema.seeded(Acyclic_Workers_V1_InvokeDeploymentRequest.self, tag: 1)))
try rpcRequire(result16 == schema.seeded(Acyclic_Workers_V1_InvokeResponse.self, tag: 2), "native response payload differs: acyclic.workers.v1.WorkersService/InvokeDeployment")
let api2 = Acyclic_Stream_V1_StreamService.Client(wrapping: client)
let result18: Acyclic_Stream_V1_InspectIdempotencyResponse = try await api2.inspectIdempotency(request: .init(message: schema.seeded(Acyclic_Stream_V1_InspectIdempotencyRequest.self, tag: 1)))
try rpcRequire(result18 == schema.seeded(Acyclic_Stream_V1_InspectIdempotencyResponse.self, tag: 2), "native response payload differs: acyclic.stream.v1.StreamService/InspectIdempotency")
let result19: Acyclic_Stream_V1_AppendResponse = try await api2.append(request: .init(message: schema.seeded(Acyclic_Stream_V1_AppendRequest.self, tag: 1)))
try rpcRequire(result19 == schema.seeded(Acyclic_Stream_V1_AppendResponse.self, tag: 2), "native response payload differs: acyclic.stream.v1.StreamService/Append")
let result20: Acyclic_Stream_V1_TailResponse = try await api2.tail(request: .init(message: schema.seeded(Acyclic_Stream_V1_TailRequest.self, tag: 1)))
try rpcRequire(result20 == schema.seeded(Acyclic_Stream_V1_TailResponse.self, tag: 2), "native response payload differs: acyclic.stream.v1.StreamService/Tail")
let result21: Acyclic_Stream_V1_ForkReceipt = try await api2.fork(request: .init(message: schema.seeded(Acyclic_Stream_V1_ForkRequest.self, tag: 1)))
try rpcRequire(result21 == schema.seeded(Acyclic_Stream_V1_ForkReceipt.self, tag: 2), "native response payload differs: acyclic.stream.v1.StreamService/Fork")
let result22 = try await api2.read(request: .init(message: schema.seeded(Acyclic_Stream_V1_ReadRequest.self, tag: 1))) { response in
 var messages: [Acyclic_Stream_V1_ReadResponse] = []
 for try await message in response.messages { messages.append(message) }
 return messages
}
try rpcRequire(result22 == [schema.seeded(Acyclic_Stream_V1_ReadResponse.self, tag: 2), schema.seeded(Acyclic_Stream_V1_ReadResponse.self, tag: 3)], "native streaming response payload differs: acyclic.stream.v1.StreamService/Read")
let result23 = try await api2.follow(request: .init(message: schema.seeded(Acyclic_Stream_V1_FollowRequest.self, tag: 1))) { response in
 var messages: [Acyclic_Stream_V1_ReadResponse] = []
 for try await message in response.messages { messages.append(message) }
 return messages
}
try rpcRequire(result23 == [schema.seeded(Acyclic_Stream_V1_ReadResponse.self, tag: 2), schema.seeded(Acyclic_Stream_V1_ReadResponse.self, tag: 3)], "native streaming response payload differs: acyclic.stream.v1.StreamService/Follow")
let result24 = try await api2.children(request: .init(message: schema.seeded(Acyclic_Stream_V1_ChildrenRequest.self, tag: 1))) { response in
 var messages: [Acyclic_Stream_V1_ChildrenResponse] = []
 for try await message in response.messages { messages.append(message) }
 return messages
}
try rpcRequire(result24 == [schema.seeded(Acyclic_Stream_V1_ChildrenResponse.self, tag: 2), schema.seeded(Acyclic_Stream_V1_ChildrenResponse.self, tag: 3)], "native streaming response payload differs: acyclic.stream.v1.StreamService/Children")
let result25: Acyclic_Stream_V1_ChildrenPageResponse = try await api2.childrenPage(request: .init(message: schema.seeded(Acyclic_Stream_V1_ChildrenPageRequest.self, tag: 1)))
try rpcRequire(result25 == schema.seeded(Acyclic_Stream_V1_ChildrenPageResponse.self, tag: 2), "native response payload differs: acyclic.stream.v1.StreamService/ChildrenPage")
let result26: Acyclic_Stream_V1_CommitResponse = try await api2.commit(request: .init(message: schema.seeded(Acyclic_Stream_V1_CommitRequest.self, tag: 1)))
try rpcRequire(result26 == schema.seeded(Acyclic_Stream_V1_CommitResponse.self, tag: 2), "native response payload differs: acyclic.stream.v1.StreamService/Commit")
let result27: Acyclic_Stream_V1_CommittedEnvelope = try await api2.readCommit(request: .init(message: schema.seeded(Acyclic_Stream_V1_ReadCommitRequest.self, tag: 1)))
try rpcRequire(result27 == schema.seeded(Acyclic_Stream_V1_CommittedEnvelope.self, tag: 2), "native response payload differs: acyclic.stream.v1.StreamService/ReadCommit")
}
}
}
let schema = try RPCSchema(Array(CommandLine.arguments.dropFirst()))
try await runRPCControls(schema)
print("PASS Swift 25 generated client/server calls, Rust descriptor message/field/path/stream checks, populated native payloads")
