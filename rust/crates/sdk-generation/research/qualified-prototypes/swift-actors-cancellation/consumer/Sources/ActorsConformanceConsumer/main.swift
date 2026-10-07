import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
import AcyclicActors

private struct FixtureOptions: Decodable {
    let endpoint: String
    let token: String
    let caCertificate: String
    let controlEndpoint: String
}

private struct FixtureState: Decodable {
    let started: Int
    let aborted: Int
    let active: Int
}

private func readFixtureState(_ endpoint: String) async throws -> FixtureState {
    let (data, response) = try await URLSession.shared.data(from: URL(string: endpoint)!)
    guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
        throw NSError(domain: "SwiftCancellationProbe", code: 10,
                      userInfo: [NSLocalizedDescriptionKey: "fixture state request failed"])
    }
    return try JSONDecoder().decode(FixtureState.self, from: data)
}

private func waitForActive(_ endpoint: String, target: Int) async throws -> FixtureState {
    for _ in 0..<100 {
        let state = try await readFixtureState(endpoint)
        if state.active == target { return state }
        try await Task.sleep(nanoseconds: 50_000_000)
    }
    return try await readFixtureState(endpoint)
}

private func waitForAbort(_ endpoint: String, targetActive: Int, minimumAborted: Int) async throws -> FixtureState {
    for _ in 0..<100 {
        let state = try await readFixtureState(endpoint)
        if state.active == targetActive && state.aborted >= minimumAborted { return state }
        try await Task.sleep(nanoseconds: 50_000_000)
    }
    return try await readFixtureState(endpoint)
}

@main
struct SwiftGeneratedCancellationProbe {
    static func main() async throws {
        guard let optionsPath = ProcessInfo.processInfo.environment["ACTORS_FIXTURE_OPTIONS"] else {
            throw NSError(domain: "SwiftCancellationProbe", code: 1,
                          userInfo: [NSLocalizedDescriptionKey: "ACTORS_FIXTURE_OPTIONS is required"])
        }
        let optionsData = try Data(contentsOf: URL(fileURLWithPath: optionsPath))
        let options = try JSONDecoder().decode(FixtureOptions.self, from: optionsData)
        let endpoint = options.endpoint
        let token = options.token
        let caValue = options.caCertificate
        let control = ProcessInfo.processInfo.environment["ACTORS_FIXTURE_CONTROL"] ?? options.controlEndpoint
        let baseline = try await readFixtureState(control + "/state")
        print("baseline started=\(baseline.started) aborted=\(baseline.aborted) active=\(baseline.active)")

        let ca: Data
        if caValue.contains("BEGIN CERTIFICATE") {
            ca = Data(caValue.utf8)
        } else {
            ca = try Data(contentsOf: URL(fileURLWithPath: caValue))
        }
        let client = try await connectActorsWithCa(
            endpoint: endpoint,
            token: token,
            caCertificate: ca,
            cancellation: nil
        )
        let actorID = try ActorId(value: "pending-swift-generator")
        let task = Task {
            do {
                _ = try await client.inspectActor(request: InspectActorRequest(actorId: actorID), cancellation: nil)
                return "completed"
            } catch {
                return "error=\(error)"
            }
        }

        let active = try await waitForActive(control + "/state", target: baseline.active + 1)
        guard active.active == baseline.active + 1 else {
            throw NSError(domain: "SwiftCancellationProbe", code: 4,
                          userInfo: [NSLocalizedDescriptionKey: "pending call never became active"])
        }
        print("active-before-cancel started=\(active.started) aborted=\(active.aborted) active=\(active.active)")
        task.cancel()
        let outcome = await task.value
        let final = try await waitForAbort(
            control + "/state",
            targetActive: baseline.active,
            minimumAborted: baseline.aborted + 1
        )
        guard final.active == baseline.active, final.aborted == baseline.aborted + 1 else {
            throw NSError(domain: "SwiftCancellationProbe", code: 5,
                          userInfo: [NSLocalizedDescriptionKey: "fixture did not observe exactly one client abort"])
        }
        print("task-cancel-outcome=\(outcome)")
        print("final started=\(final.started) aborted=\(final.aborted) active=\(final.active)")
        print("PASS generated Swift Task.cancel -> Rust future cancel; cancellation argument=nil; no manual handle")
    }
}
