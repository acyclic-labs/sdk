require "json"
require "/tmp/ruby-c8-wsl-generated-frozen-20261007/acyclic_actors.rb"
require "/tmp/ruby-c8-wsl-generated-frozen-20261007/acyclic_actors_uniffi.rb"
options = JSON.parse(File.read(ARGV.fetch(0)))
actor_id = AcyclicActors::ActorId.new(options["actorId"])
request = AcyclicActors::InspectActorRequest.new(actor_id: actor_id)
client = AcyclicActorsUniffi.connect_actors_with_ca(options["endpoint"], options["token"], options["caCertificate"].b, nil)
handle = AcyclicActorsUniffi::CancellationHandle.new
error = nil
thread = Thread.new do
  begin
    client.inspect_actor(request, handle)
  rescue Exception => e
    error = e
  end
end
sleep 0.25
handle.cancel
thread.join(10)
raise "worker did not finish" if thread.alive?
puts "cancel_handle_is_cancelled=#{handle.is_cancelled ? 'true' : 'false'}"
puts "cancel_error=#{error.class}:#{error.message}" if error
puts "RUBY_CURRENT_C8_GENERIC_CANCEL_PASS"
