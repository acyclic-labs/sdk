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

@main
struct TypedChoiceUnknownFixture {
  static func main() {
    checkUnknownChoice()

    guard let digest = Sha256Digest(Data(repeating: 7, count: 32)) else {
      fatalError("valid digest was rejected")
    }
    precondition(
      Image(ImmutableReferenceChoice: .ManagedDigest(digest)) != nil
    )
    precondition(Image() == nil)
  }
}
