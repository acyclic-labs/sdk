require "json"
require "/tmp/acyclic-ruby-patched-20261007.rb"

options = JSON.parse(File.read("/tmp/mac-fixture-options-normal.json"))
actor_id = AcyclicActorsUniffi::ActorId.new("actor-a")
begin
  AcyclicActorsUniffi::ActorId.new(123)
  abort "RUBY_NEGATIVE_TYPE_FAIL=ActorId"
rescue TypeError => error
  puts "RUBY_NEGATIVE_TYPE_PASS=#{error.class}:#{error.message}"
end
begin
  AcyclicActorsUniffi::ActorLimits.new(2**64, 1, 1)
  abort "RUBY_NEGATIVE_RANGE_FAIL=u64"
rescue RangeError => error
  puts "RUBY_NEGATIVE_RANGE_PASS=#{error.class}:#{error.message}"
end
client = AcyclicActorsUniffi.connect_actors_with_ca(options["endpoint"], "wrong-token", options["caCertificate"].b, nil)
begin
  client.inspect_actor(actor_id, nil)
  abort "RUBY_TYPED_ERROR_FAIL=no exception"
rescue Exception => error
  puts "RUBY_TYPED_ERROR_PASS=#{error.class}:#{error.message}"
end
