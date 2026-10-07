// swift-tools-version: 6.4
// Source-only SwiftPM consumer layout.
// runner.ps1 stages generated Swift/header/modulemap files into Generated/ before building.
import PackageDescription

let package = Package(
    name: "SwiftActorsCancellationConsumer",
    products: [
        .executable(name: "ActorsConformanceConsumer", targets: ["ActorsConformanceConsumer"]),
        .executable(name: "ActorsAll8ConformanceConsumer", targets: ["ActorsAll8ConformanceConsumer"]),
    ],
    targets: [
        .target(
            name: "acyclic_actorsFFI",
            path: "Generated/acyclic_actorsFFI",
            publicHeadersPath: "include",
            linkerSettings: [
                .unsafeFlags(["-L", "Native/windows-x86_64"], .when(platforms: [.windows])),
                .linkedLibrary("acyclic_actors_uniffi", .when(platforms: [.windows])),
            ]
        ),
        .target(
            name: "acyclic_actors_uniffiFFI",
            path: "Generated/acyclic_actors_uniffiFFI",
            publicHeadersPath: "include",
            linkerSettings: [
                .unsafeFlags(["-L", "Native/windows-x86_64"], .when(platforms: [.windows])),
                .linkedLibrary("acyclic_actors_uniffi", .when(platforms: [.windows])),
            ]
        ),
        .target(
            name: "AcyclicActors",
            dependencies: ["acyclic_actorsFFI", "acyclic_actors_uniffiFFI"],
            path: "Generated/AcyclicActors"
        ),
        .executableTarget(
            name: "ActorsConformanceConsumer",
            dependencies: ["AcyclicActors"],
            path: "Sources/ActorsConformanceConsumer"
        ),
        .executableTarget(
            name: "ActorsAll8ConformanceConsumer",
            dependencies: ["AcyclicActors"],
            path: "Sources/ActorsAll8ConformanceConsumer"
        ),
    ]
)
