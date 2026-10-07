require "json"
require "C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk/rust/crates/sdk-generation/research/qualified-prototypes/uniffi-bindgen-cs-sourcefix-20261007/repro-package/ffi-ruby-php-20261007/current-c8-generic-frozen-windows-20261007/acyclic_actors.rb"
require "C:/Users/varun/.codex/worktrees/rust-source-foundation/sdk/rust/crates/sdk-generation/research/qualified-prototypes/uniffi-bindgen-cs-sourcefix-20261007/repro-package/ffi-ruby-php-20261007/current-c8-generic-frozen-windows-20261007/acyclic_actors_uniffi.rb"
options = JSON.parse(File.read(ARGV.fetch(0)))
client = AcyclicActorsUniffi.connect_actors_with_ca(options["endpoint"], options["token"], options["caCertificate"].b, nil)
values = [9223372036854775808, 18446744073709551615]
values.each_with_index do |v, i|
  p = AcyclicActors::PositiveU64.new(v)
  raise "constructor narrowed" unless p.value == v
  limits = AcyclicActors::ActorLimits.new(handler_timeout_millis: p, memory_bytes: AcyclicActors::PositiveU64.new(2), checkpoint_bytes: AcyclicActors::PositiveU64.new(3))
  req = AcyclicActors::CreateActorRequest.new(code_sha256: AcyclicActors::CodeSha256.new(Array.new(32, 1).pack("C*")), home_region: "eu", bindings: [], limits: limits, subscriptions: [], idempotency_key: "ruby-c8-u64-#{i}")
  client.create_actor(req, nil)
  puts "u64=#{v}=PASS"
end
puts "RUBY_CURRENT_C8_GENERIC_FULL_U64_PASS"
