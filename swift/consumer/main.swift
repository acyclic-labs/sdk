import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import AcyclicSDKGenerated

@main
struct AcyclicFixtureConsumer {
  static func main() async throws {
    guard let rawAddress = ProcessInfo.processInfo.environment["FIXTURE_GRPC_ADDRESS"] else {
      throw NSError(domain: "AcyclicFixtureConsumer", code: 2, userInfo: [NSLocalizedDescriptionKey: "FIXTURE_GRPC_ADDRESS is required"])
    }
    let pieces = rawAddress.split(separator: ":", maxSplits: 1).map(String.init)
    guard pieces.count == 2, let port = Int(pieces[1]) else {
      throw NSError(domain: "AcyclicFixtureConsumer", code: 3, userInfo: [NSLocalizedDescriptionKey: "FIXTURE_GRPC_ADDRESS must be host:port"])
    }
    try await withGRPCClient(
      transport: .http2NIOPosix(
        target: .dns(host: pieces[0], port: port),
        transportSecurity: .plaintext
      )
    ) { client in
      let actors = Acyclic_Actors_V1_ActorsService.Client(wrapping: client)
      let request = Acyclic_Actors_V1_CreateActorRequest.with {
        $0.codeSha256 = Data([0, 1, 255])
        $0.homeRegion = "eu-west"
        $0.idempotencyKey = "swift-consumer"
      }
      let response = try await actors.createActor(request)
      guard response.actor.actorId == "fixture-actor",
            response.actor.homeRegion == "eu-west" else {
        throw NSError(domain: "AcyclicFixtureConsumer", code: 4, userInfo: [NSLocalizedDescriptionKey: "fixture CreateActor response mismatch"])
      }
      print("swift-rpc=passed actor=\(response.actor.actorId)")

      let streams = Acyclic_Stream_V2_StreamService.Client(wrapping: client)
      let appendRequest = Acyclic_Stream_V2_AppendRequest.with {
        $0.path = "swift-consumer/events"
        $0.records = [Data("one".utf8), Data("two".utf8)]
        $0.idempotencyKey = Data("swift-stream".utf8)
      }
      _ = try await streams.append(appendRequest)
      let readRequest = Acyclic_Stream_V2_ReadRequest.with {
        $0.path = "swift-consumer/events"
        $0.from = 0
        $0.limit = 2
      }
      let readCount = try await streams.read(readRequest) { response in
        var count = 0
        for try await _ in response.messages {
          count += 1
        }
        return count
      }
      guard readCount == 2 else {
        throw NSError(domain: "AcyclicFixtureConsumer", code: 5, userInfo: [NSLocalizedDescriptionKey: "fixture Stream.Read response count mismatch"])
      }
      print("swift-stream=passed records=\(readCount)")

      let cancellation = Task {
        try await streams.follow(
          Acyclic_Stream_V2_FollowRequest.with {
            $0.path = "swift-consumer/events"
            $0.from = 2
          }
        ) { response in
          for try await _ in response.messages {
            try Task.checkCancellation()
          }
          return true
        }
      }
      cancellation.cancel()
      var cancellationObserved = false
      do {
        let completed = try await cancellation.value
        cancellationObserved = !completed
      } catch {
        cancellationObserved = cancellation.isCancelled
      }
      guard cancellationObserved else {
        throw NSError(domain: "AcyclicFixtureConsumer", code: 6, userInfo: [NSLocalizedDescriptionKey: "fixture Stream.Follow did not observe cancellation"])
      }
      if cancellationObserved {
        print("swift-cancel=passed")
      }
    }
  }
}
