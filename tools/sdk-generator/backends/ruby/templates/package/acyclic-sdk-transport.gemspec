Gem::Specification.new do |s|
  s.name = "acyclic-sdk-transport"
  s.version = "0.2.0.alpha.1"
  s.summary = "Rust-authoritative Actors, Workers and Stream transport bindings"
  s.authors = ["Acyclic"]
  s.license = "Apache-2.0"
  s.homepage = "https://github.com/acyclic-labs/sdk"
  s.date = Time.utc(2026, 1, 1)
  s.required_ruby_version = ">= 3.2"
  s.files = (Dir["lib/**/*.rb"] + ["LICENSE", "NOTICE", "authority/rust-authority.json"]).sort
  s.require_paths = ["lib"]
  s.add_runtime_dependency "grpc", "= 1.84.0"
  s.add_runtime_dependency "google-protobuf", "= 4.36.2"
  s.metadata = { "rust_source_revision" => "@RUST_SOURCE_REVISION@" }
end
