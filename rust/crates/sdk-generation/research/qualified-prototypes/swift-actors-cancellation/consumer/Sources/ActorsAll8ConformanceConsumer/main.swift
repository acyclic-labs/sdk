import Foundation
import AcyclicActors

private struct FixtureOptions: Decodable {
    let endpoint: String
    let token: String
    let caCertificate: String
    let actorId: String
}

private func requireObservation(_ value: ActorObservation?, label: String, actorID: String) throws -> ActorObservation {
    guard let observation = value else {
        throw NSError(domain: "SwiftPostPatchAll8", code: 10,
                      userInfo: [NSLocalizedDescriptionKey: "\(label) returned nil"])
    }
    precondition(observation.actorId.value() == actorID, "\(label) actor id")
    precondition(observation.codeSha256.value() == Data(repeating: 1, count: 32), "\(label) digest")
    precondition(observation.homeRegion == "eu", "\(label) region")
    precondition(observation.state == .active, "\(label) state")
    print("\(label): typed ActorObservation revision=\(observation.configurationRevision)")
    return observation
}

@main
struct SwiftPostPatchAll8Consumer {
    static func main() async throws {
        guard let raw = ProcessInfo.processInfo.environment["ACTORS_FIXTURE_OPTIONS"] else {
            throw NSError(domain: "SwiftPostPatchAll8", code: 1,
                          userInfo: [NSLocalizedDescriptionKey: "ACTORS_FIXTURE_OPTIONS is required"])
        }
        let data: Data
        if FileManager.default.fileExists(atPath: raw) {
            data = try Data(contentsOf: URL(fileURLWithPath: raw))
        } else if let inline = raw.data(using: .utf8) {
            data = inline
        } else {
            throw NSError(domain: "SwiftPostPatchAll8", code: 2,
                          userInfo: [NSLocalizedDescriptionKey: "ACTORS_FIXTURE_OPTIONS is not UTF-8"])
        }
        let options = try JSONDecoder().decode(FixtureOptions.self, from: data)
        let client = try await connectActorsWithCa(
            endpoint: options.endpoint,
            token: options.token,
            caCertificate: Data(options.caCertificate.utf8),
            cancellation: nil
        )
        let actorID = try ActorId(value: options.actorId)
        let digest = try CodeSha256(value: Data(repeating: 1, count: 32))
        let limits = try ActorLimits(
            handlerTimeoutMillis: PositiveU64(value: 1000),
            memoryBytes: PositiveU64(value: 4096),
            checkpointBytes: PositiveU64(value: 8192)
        )

        _ = try requireObservation(await client.inspectActor(actorId: actorID, cancellation: nil), label: "ordinary-inspect", actorID: options.actorId)
        let spec = try SubscriptionSpec(
            subscriptionId: "swift-post-patch-subscription",
            streamPath: "events/input",
            start: .cursor(cursor: 9007199254740993),
            placementAnchor: false
        )
        let create = try CreateActorRequest(codeSha256: digest, homeRegion: "eu", bindings: [], limits: limits, subscriptions: [], idempotencyKey: "post-create")
        _ = try requireObservation(await client.createActor(request: create, cancellation: nil), label: "create", actorID: options.actorId)
        let update = try UpdateActorRequest(actorId: actorID, codeSha256: digest, bindings: [], limits: limits, expectedConfigurationRevision: 0, idempotencyKey: "post-update")
        _ = try requireObservation(await client.updateActor(request: update, cancellation: nil), label: "update", actorID: options.actorId)
        let add = try AddSubscriptionRequest(actorId: actorID, subscription: spec, idempotencyKey: "post-add")
        _ = try requireObservation(await client.addSubscription(request: add, cancellation: nil), label: "add", actorID: options.actorId)
        let resume = ResumeSubscriptionRequest(actorId: actorID, subscriptionId: "swift-post-patch-subscription", idempotencyKey: "post-resume")
        _ = try requireObservation(await client.resumeSubscription(request: resume, cancellation: nil), label: "resume", actorID: options.actorId)
        let checkpoint = CheckpointActorRequest(actorId: actorID, idempotencyKey: "post-checkpoint")
        let checkpointObservation = try requireObservation(await client.checkpointActor(request: checkpoint, cancellation: nil), label: "checkpoint", actorID: options.actorId)
        precondition(checkpointObservation.checkpointEpoch == 9, "checkpoint epoch")
        let invoke = InvokeActorRequest(actorId: actorID, method: "POST", url: "/result", body: Data("{}".utf8), headers: [Header(name: "content-type", value: "application/json")])
        let response = try await client.invokeActor(request: invoke, cancellation: nil)
        precondition(response.status == 201, "invoke status")
        print("invoke: typed InvokeActorResponse status=\(response.status) headers=\(response.headers.count)")
        let remove = RemoveSubscriptionRequest(actorId: actorID, subscriptionId: "swift-post-patch-subscription", idempotencyKey: "post-remove")
        _ = try requireObservation(await client.removeSubscription(request: remove, cancellation: nil), label: "remove", actorID: options.actorId)

        let wrongClient = try await connectActorsWithCa(endpoint: options.endpoint, token: "wrong-token", caCertificate: Data(options.caCertificate.utf8), cancellation: nil)
        do {
            _ = try await wrongClient.inspectActor(actorId: actorID, cancellation: nil)
            throw NSError(domain: "SwiftPostPatchAll8", code: 11,
                          userInfo: [NSLocalizedDescriptionKey: "wrong token unexpectedly succeeded"])
        } catch let error as BindingError {
            guard case let .Service(grpcCode, serviceCode, detailMessage) = error else {
                throw NSError(domain: "SwiftPostPatchAll8", code: 12,
                              userInfo: [NSLocalizedDescriptionKey: "unexpected typed error \(error)"])
            }
            print("typed-service-error grpc=\(grpcCode) service=\(String(describing: serviceCode)) detail=\(detailMessage)")
        }
        print("PASS post-patch ordinary completion + all eight typed operations/u64 cursor + typed service error")
    }
}
