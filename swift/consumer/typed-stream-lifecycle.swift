import Foundation

private final class FixtureCounters: @unchecked Sendable {
  private let lock = NSLock()
  private(set) var active = 0
  private(set) var maximumActive = 0
  private(set) var cancellations = 0

  func enter() {
    lock.lock()
    active += 1
    maximumActive = max(maximumActive, active)
    lock.unlock()
  }

  func leave() {
    lock.lock()
    active -= 1
    lock.unlock()
  }

  func cancelled() {
    lock.lock()
    cancellations += 1
    lock.unlock()
  }

  func snapshot() -> (active: Int, maximumActive: Int, cancellations: Int) {
    lock.lock()
    defer { lock.unlock() }
    return (active, maximumActive, cancellations)
  }
}

private struct FixtureRequest: RustWireRequest, Sendable {
  typealias Wire = RustWireMessage
  func toWire() -> RustWireMessage { RustWireMessage(wire: Data()) }
}

private struct FixtureResponse: RustWireResponse, Sendable {
  typealias Wire = RustWireMessage
  static func fromWire(_ wire: RustWireMessage) throws -> FixtureResponse { _ = wire; return FixtureResponse() }
}

@main
struct TypedStreamLifecycleFixture {
  static func main() async throws {
    let counters = FixtureCounters()
    let stream = RustTypedClientStream<FixtureRequest, FixtureResponse>(
      send: { _ in
        counters.enter()
        await Task.yield()
        counters.leave()
      },
      finish: {
        counters.enter()
        await Task.yield()
        counters.leave()
        return FixtureResponse()
      },
      cancel: { counters.cancelled() }
    )

    async let first: Void = stream.send(FixtureRequest())
    async let second: Void = stream.send(FixtureRequest())
    try await first
    try await second
    let afterSends = counters.snapshot()
    precondition(afterSends.active == 0 && afterSends.maximumActive == 1)

    stream.cancel()
    stream.cancel()
    precondition(counters.snapshot().cancellations == 1)

    do {
      let dropped = RustTypedStream<FixtureResponse>(
        next: { nil },
        cancel: { counters.cancelled() }
      )
      _ = dropped.state
    }
    precondition(counters.snapshot().cancellations == 2)
  }
}
