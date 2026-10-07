Gem::Specification.new do |s|
  s.name = "acyclic_actors_uniffi_c8_frozen"
  s.version = "0.2.0.c8frozen20261007"
  s.summary = "Acyclic Actors maintained UniFFI Ruby qualification artifact"
  s.description = "Generated from the maintained UniFFI Ruby backend and the Rust-owned Actors component."
  s.authors = ["qualification"]
  s.licenses = ["MPL-2.0"]
  s.files = Dir["lib/**/*.rb", "native/*"]
  s.require_paths = ["lib"]
  s.add_runtime_dependency "ffi", "= 1.17.2"
end
