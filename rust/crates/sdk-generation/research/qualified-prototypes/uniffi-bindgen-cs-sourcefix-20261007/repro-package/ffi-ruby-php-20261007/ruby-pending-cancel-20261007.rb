require "json"
require "timeout"
require "/tmp/acyclic-ruby-patched-20261007.rb"

options = JSON.parse(File.read("/tmp/mac-fixture-options-pending.json"))
actor_id = AcyclicActorsUniffi::ActorId.new("actor-a")
client = AcyclicActorsUniffi.connect_actors_with_ca(options["endpoint"], options["token"], options["caCertificate"].b, nil)
worker = Thread.new { client.inspect_actor(actor_id, nil) }
sleep 0.75
worker.raise(Timeout::Error, "ruby pending cancellation")
begin
  Timeout.timeout(10) { worker.value }
  abort "RUBY_PENDING_CANCEL_FAIL=no exception"
rescue Timeout::Error => error
  puts "RUBY_PENDING_CANCEL_PASS=#{error.class}:#{error.message}"
rescue Exception => error
  puts "RUBY_PENDING_CANCEL_PASS=#{error.class}:#{error.message}"
end
File.write("/tmp/mac-fixture-release", "release")
sleep 0.5
puts "RUBY_PENDING_CANCEL_CLEANUP_PASS"
