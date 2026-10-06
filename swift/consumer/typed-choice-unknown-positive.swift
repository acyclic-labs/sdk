import Foundation

func checkUnknownChoice() {
  let choice: InferenceCustomerRunResultContextChoice =
    .unknown(rawTag: 902, payload: Data([0, 1, 255]))
  switch choice {
  case let .unknown(rawTag, payload):
    precondition(rawTag == 902 && payload == Data([0, 1, 255]))
  default:
    fatalError("expected unknown raw arm")
  }
}
