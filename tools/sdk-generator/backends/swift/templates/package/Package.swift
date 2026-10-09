// swift-tools-version:6.2
import PackageDescription
let package = Package(name: "AcyclicTransport", products: [.library(name: "AcyclicTransport", targets: ["AcyclicTransport"])], dependencies: [
 .package(url: "https://github.com/apple/swift-protobuf.git", exact: "1.38.1"),
 .package(url: "https://github.com/grpc/grpc-swift-2.git", exact: "2.4.3"),
 .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", exact: "2.4.1")
], targets: [
 .target(name: "AcyclicTransport", dependencies: [.product(name: "SwiftProtobuf", package: "swift-protobuf"), .product(name: "GRPCCore", package: "grpc-swift-2"), .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf")])
])
