import Foundation
import SwiftProtobuf
import GRPCCore
import GRPCProtobuf
import AcyclicTransport

enum ControlFailure: Error { case failed(String) }
func require(_ condition: Bool, _ message: String) throws {
  if !condition { throw ControlFailure.failed(message) }
}
func roundtrip<M: SwiftProtobuf.Message & Equatable>(_ message: M) throws {
  let bytes: [UInt8] = try message.serializedBytes()
  try require(try M(serializedBytes: bytes) == message, "message roundtrip differs")
  let grpcBytes: [UInt8] = try ProtobufSerializer<M>().serialize(message)
  try require(grpcBytes == bytes, "gRPC serializer differs")
  try require(try ProtobufDeserializer<M>().deserialize(grpcBytes) == message, "gRPC decoder differs")
}
let bytes = Data([0, 255])
var actor = Acyclic_Actors_V1_CreateActorRequest()
actor.codeSha256 = bytes; actor.homeRegion = "test"; actor.idempotencyKey = "probe"
try require(actor.codeSha256 == bytes, "Actor bytes changed"); try roundtrip(actor)
var worker = Acyclic_Workers_V1_PublishVersionRequest()
worker.javascriptModule = bytes; worker.expectedSha256 = bytes; worker.idempotencyKey = "probe"
try require(worker.javascriptModule == bytes && worker.expectedSha256 == bytes, "Worker bytes changed"); try roundtrip(worker)
var append = Acyclic_Stream_V2_AppendRequest()
append.path = "test/path"; append.records = [bytes, Data()]; append.ifTail = UInt64.max; append.idempotencyKey = bytes
try require(append.records == [bytes, Data()] && append.ifTail == UInt64.max && append.hasIfTail, "Stream bytes or unsigned bounds changed"); try roundtrip(append)
var zero = Acyclic_Stream_V2_AppendRequest(); zero.ifTail = 0
try require(zero.hasIfTail && (try zero.serializedData()) == Data([0x18, 0]), "optional zero differs"); try roundtrip(zero)
zero.clearIfTail(); try require(!zero.hasIfTail, "optional clear differs")
var read = Acyclic_Stream_V2_ReadRequest(); read.from = UInt64.max; read.limit = UInt32.max
let maxWire: [UInt8] = [0x10] + Array(repeating: 0xff, count: 9) + [1, 0x18] + Array(repeating: 0xff, count: 4) + [0x0f]
try require(try read.serializedData() == Data(maxWire), "unsigned maximum wire bits differ"); try roundtrip(read)
var response = Acyclic_Stream_V2_AppendResponse(); response.committed.tail = 123; try roundtrip(response)
response.conflict.actualTail = 456; try require(response.committed.tail == 0 && response.conflict.actualTail == 456, "oneof switch differs"); try roundtrip(response)
response.outcome = nil; try require(response.outcome == nil, "oneof clear differs")
var mutation = Acyclic_Stream_V2_CommitMutation(); mutation.append.path = "test/path"; mutation.append.records = [bytes]; try roundtrip(mutation)
mutation.fork.source = "source"; mutation.fork.destination = "destination"; mutation.fork.atTail = UInt64.max; mutation.fork.records = [bytes]
try require(mutation.append.path.isEmpty && mutation.fork.source == "source", "fork oneof switch differs"); try roundtrip(mutation)
mutation.mutation = nil; try require(mutation.mutation == nil, "fork oneof clear differs")
try require(Acyclic_Actors_V1_ActorsService.Method.CreateActor.descriptor.fullyQualifiedMethod == "acyclic.actors.v1.ActorsService/CreateActor" && Acyclic_Actors_V1_ActorsService.Method.CreateActor.descriptor.type == .unary, "Actor RPC metadata differs")
try require(Acyclic_Workers_V1_WorkersService.Method.PublishVersion.descriptor.fullyQualifiedMethod == "acyclic.workers.v1.WorkersService/PublishVersion", "Worker RPC metadata differs")
try require(Acyclic_Stream_V2_StreamService.Method.Read.descriptor.fullyQualifiedMethod == "acyclic.stream.v2.StreamService/Read" && Acyclic_Stream_V2_StreamService.Method.Read.descriptor.type == .serverStreaming, "Stream RPC metadata differs")
print("PASS Swift bytes, unsigned bounds, optional zero, populated oneofs, gRPC codecs and bounded RPC metadata")
