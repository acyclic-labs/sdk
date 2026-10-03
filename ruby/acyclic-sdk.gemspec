# frozen_string_literal: true

require_relative "lib/acyclic_sdk/version"

Gem::Specification.new do |spec|
  spec.name = "acyclic-sdk"
  spec.version = AcyclicSdk::VERSION
  spec.summary = "Generated Acyclic Actors and Stream transport stubs"
  spec.description = "Transport-only Ruby stubs generated from Rust-owned protobuf contracts."
  spec.authors = ["Acyclic"]
  spec.license = "Apache-2.0"
  spec.required_ruby_version = ">= 3.2"
  spec.files = Dir["lib/**/*.rb", "generated/**/*", "README.md", "LICENSE*", "generator.lock.json"]
  spec.require_paths = ["lib"]
  spec.add_runtime_dependency "grpc", "= 1.82.0"
  spec.add_runtime_dependency "google-protobuf", "= 4.33.0"
end
