ThisBuild / scalaVersion := "2.13.16"
ThisBuild / organization := "dev.acyclic"
ThisBuild / version := "0.1.0"
name := "acyclic-sdk-scala-grpc-prototype"

// Every proto directory is generated from the Rust authority in release/manual runs.
Compile / PB.protoSources := Seq(
  file("proto/actors/v1"), file("proto/filesystem/v2"), file("proto/harness/v2"),
  file("proto/inference/v1"), file("proto/machines/v1"), file("proto/objects/v1"),
  file("proto/objects/v2"), file("proto/protocol/v1"), file("proto/validation/v1"),
  file("proto/workers/v1"), file("rust/crates/stream/proto")
)
Compile / PB.targets := Seq(scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb")
Compile / PB.protocVersion := "3.25.5"

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "compilerplugin" % "0.11.17",
  "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.17",
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "0.11.17",
  "io.grpc" % "grpc-inprocess" % "1.66.0",
  "io.grpc" % "grpc-netty-shaded" % "1.66.0",
  "com.google.protobuf" % "protobuf-java" % "3.25.1"
)
