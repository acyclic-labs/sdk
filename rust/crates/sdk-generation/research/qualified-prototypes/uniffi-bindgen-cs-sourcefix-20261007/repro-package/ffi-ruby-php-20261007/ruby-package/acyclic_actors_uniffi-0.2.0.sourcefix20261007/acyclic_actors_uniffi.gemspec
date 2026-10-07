Gem::Specification.new do |s|
  s.name = "acyclic_actors_uniffi"
  s.version = "0.2.0.sourcefix20261007"
  s.summary = "Acyclic Actors UniFFI Ruby binding"
  s.authors = ["task-owned qualification"]
  s.files = ["lib/acyclic_actors_uniffi.rb", "native/libacyclic_actors_uniffi.dylib"]
  s.require_paths = ["lib"]
  s.platform = Gem::Platform::CURRENT
  s.add_runtime_dependency "ffi", "~> 1.17"
end
